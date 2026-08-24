"""
Pipecat over chan_websocket.

Pipecat ships serializers for Twilio, Telnyx, Plivo, Vonage, Exotel and
Genesys (`pipecat/serializers/`), all of them cloud services, and none for
Asterisk. This one runs any Pipecat pipeline over the telephony you already
have.

A serializer only TRANSLATES, and it is worth saying because it is what gets
misread: a pipeline frame becomes bytes for the wire, and what comes off the
wire becomes a frame. The Pipecat transport sets the pace; flow control,
alignment and the channel limits are galcymedia's job underneath. There is
no `sleep` in this file on purpose: pacing on both sides is how choppy audio
gets made.

The parts: `serialize` (pipeline to channel), `deserialize` (channel to
pipeline), and `_on_media_start`, where the codec, the frame size and the
sample rate are learned. The rest guards the outgoing audio: alignment, the
message ceiling and split samples.

Usage. The serializer goes INSIDE `params`, not as a loose argument:

    from galcymedia.adapters.pipecat import AsteriskFrameSerializer

    params = FastAPIWebsocketParams(serializer=AsteriskFrameSerializer(), ...)
    transport = FastAPIWebsocketTransport(websocket=ws, params=params)

`f(json)` in the Dial is not optional (the whole dialplan is in
`docs/architecture.md`). The dialplan variables reach the pipeline through
`AsteriskMediaStartFrame`: read its `message`, see that class.

Which transport you pick decides how many calls you take:
`FastAPIWebsocketTransport` takes several (every telephony serializer of
Pipecat uses it, one pipeline per call); `SingleClientWebsocketServerTransport`
takes ONE and rejects the rest, so it serves to try things on your machine.
Its alias `WebsocketServerTransport` is deprecated since Pipecat 1.4.0
(`transports/websocket/server.py:699`, Pipecat's, not ours).

THE SAMPLE RATE: build the pipeline at the channel's rate and not a byte is
touched here. The `c()` of the Dial decides it, but the channel only announces
it in MEDIA_START, when the pipeline has ALREADY started, so when they differ
the audio is CONVERTED in both directions and a warning says what to write.
Neither side can renegotiate: the channel sends the event with `send_event`
(`chan_websocket.c:1178`) and the speech engine locks its rate at startup
(`stt_service.py:359`). The why is in `docs/decisions.md`.

A QUIET CHANNEL: the channel writes nothing while the caller is silent
(`chan_websocket.c:1234` drops comfort noise; measured at 0 frames/s). The
Pipecat VAD closes the turn anyway, through `audio_idle_timeout` of
`LLMUserAggregatorParams` (`llm_response_universal.py:169`, 1.0 s by
default): measured, the turn closes at +1.01 s after the last frame against
+0.20 s with continuous silence, and logs a warning each time. Set it to 0.3
in the aggregator; no silence filler is needed here. The table is in
`docs/decisions.md`.

The concurrency model is Pipecat's (one pipeline per call), not galcymedia's
(one `Session` per call inside one process). See `docs/decisions.md`.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

# The only adapter that needs the provider's SDK: Pipecat is a framework, not
# a WebSocket API like the other three. A bare ModuleNotFoundError points at
# a line in here and does not say how to fix it.
try:
    from pipecat.audio.dtmf.types import KeypadEntry
    from pipecat.audio.utils import create_stream_resampler
    from pipecat.frames.frames import (
        AudioRawFrame,
        CancelFrame,
        ControlFrame,
        EndFrame,
        Frame,
        InputAudioRawFrame,
        InputDTMFFrame,
        InputTransportMessageFrame,
        InterruptionFrame,
        LLMFullResponseEndFrame,
        LLMFullResponseStartFrame,
        OutputTransportMessageFrame,
        OutputTransportMessageUrgentFrame,
        StartFrame,
        TTSStartedFrame,
        TTSStoppedFrame,
    )
    from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
    from pipecat.serializers.base_serializer import FrameSerializer
except ImportError as exc:
    raise ImportError(
        "The Pipecat adapter needs its extra, which is not installed. "
        "Install it with:\n\n"
        '    pip install "galcymedia[pipecat]"\n\n'
        "The other adapters (deepgram, openai, elevenlabs) do not need it: "
        "they speak the provider's WebSocket directly."
    ) from exc

from ..framing import FrameAligner
from ..pcm import alaw_to_pcm, pcm_to_alaw, pcm_to_ulaw, ulaw_to_pcm
from ..protocol import (
    MAX_WEBSOCKET_MESSAGE_BYTES,
    Command,
    Event,
    MediaStart,
    build_command,
    codec_name_for,
    parse_event,
    parse_media_start,
    sample_rate_for,
)
from ..session import FINISH_MAX_WAIT_S

log = logging.getLogger(__name__)

# The G.711 formats the channel delivers compressed to 8 bits per sample,
# which Pipecat does not understand: they are converted to PCM both ways. Any
# other format (slin, slin16, slin24, ...) is already 16-bit linear PCM, what
# Pipecat moves inside, and passes as is. The channel names them with
# `ast_format_get_name` (`chan_websocket.c:236`): `alaw` and `ulaw`
# (`codec_builtin.c:168`, `:183`), never `pcma`, `pcmu` or `mulaw`. Pinned
# by `test_the_g711_names_are_the_channels_own`.
_ALAW_NAMES = frozenset({"alaw"})
_ULAW_NAMES = frozenset({"ulaw"})

# Samples per second of a G.711 trunk. Fixed by the codec, not a guess of
# ours: alaw and ulaw are always 8 kHz.
_G711_SAMPLE_RATE = 8000

# Ceiling of the audio waiting for room in a message: about 8 s of G.711,
# the same criterion as the rest of the library. Holding more only trades an
# audio problem for a memory one, and in real time what arrives late is
# already useless.
MAX_PENDING_OVERFLOW_BYTES = MAX_WEBSOCKET_MESSAGE_BYTES



@dataclass
class AsteriskMediaStartFrame(InputTransportMessageFrame):
    """The channel's MEDIA_START, delivered to the pipeline.

    It exists for ONE thing only the dialplan has: `channel_variables`, what
    the integrator set with `Set(_MY_VARIABLE=...)`. Without it a Pipecat
    pipeline cannot know whom the person called, where they come from, or
    which language the extension asked for.

    It travels as a `SystemFrame` (`InputTransportMessageFrame` is one,
    `frames.py:1267`), so it does not wait behind queued audio.

    Read `message`, the raw payload of the event: it is the contract of
    `InputTransportMessageFrame` and the only field that reaches the pipeline.
    `FastAPIWebsocketTransport` rebuilds a generic frame with just `message`
    (`fastapi.py:382-383`), so `media`, the same event already parsed, only
    survives in a transport that forwards the instance. Reading `media` works
    in a test that builds the frame by hand and fails in a real call.

    Typical use in a pipeline processor:

        if isinstance(frame, InputTransportMessageFrame):
            payload = frame.message
            if payload.get("event") == "MEDIA_START":
                language = payload["channel_variables"].get("AI_LANGUAGE", "es")

    Populated in `_on_media_start`; pinned by
    `test_the_media_start_reaches_the_pipeline_with_its_channel_variables`.
    """

    media: MediaStart | None = None


class AsteriskFrameSerializer(FrameSerializer):
    """Translates between Pipecat frames and the chan_websocket protocol.

    The audio format is NOT configured: it is learned from the MEDIA_START the
    channel sends, the only source that knows what the `Dial` asked for. A
    serializer that makes you repeat the codec in code drifts silently the
    day someone edits the dialplan.
    """

    class InputParams(FrameSerializer.InputParams):
        """Adapter settings.

        Parameters:
            sample_rate: Forces the sample rate declared to the pipeline.
                Derived from the channel codec by default, which is right.
            auto_hang_up: Hangs up the call on EndFrame or CancelFrame.
            align_frames: Delivers audio in frames of the size the channel
                asks for. What avoids the gap at every block edge.
        """

        sample_rate: int | None = None
        auto_hang_up: bool = True
        align_frames: bool = True

    def __init__(self, params: InputParams | None = None, **kwargs: Any) -> None:
        """Prepares the serializer.

        Args:
            params: Optional settings. The defaults fit a normal telephony
                call.
            **kwargs: Passed to the Pipecat base class (`name`, ...).
        """
        params = params or AsteriskFrameSerializer.InputParams()
        super().__init__(params, **kwargs)
        self._params: AsteriskFrameSerializer.InputParams = params

        # What the channel has not said yet. Until MEDIA_START the codec is
        # unknown and is not guessed: incoming audio is ignored meanwhile,
        # which beats decoding it with the wrong codec and handing noise to
        # the model. MEDIA_START usually comes first but CANNOT be assumed:
        # audio gets ahead of it on some calls, worse under CPU load
        # (`docs/protocol.md`, issue #1712).
        self._media: Any = None
        self._encode: Any = None
        self._decode: Any = None
        self._aligner: FrameAligner | None = None

        # The sample rate declared to the pipeline. The channel's rules; the
        # StartFrame one is only used until MEDIA_START arrives.
        self._sample_rate = 0

        # The rate the pipeline was built with. When it differs from the
        # channel's, the audio is converted both ways (`_setup_resamplers`).
        self._pipeline_sample_rate = 0

        # Which StartFrame started this call, to tell the second `setup` of
        # the same start from a new call. See `setup`.
        self._start_frame_id: Any = None

        # The converters, one per direction. They only exist when needed:
        # with both rates equal, the recommended setup, there is nothing here
        # and not a byte is touched.
        self._resample_in: Any = None
        self._resample_out: Any = None

        # Idempotence guard for the hang-up: EndFrame and CancelFrame can both
        # come down through the same close, and hanging up twice sends a
        # command over a socket that is already leaving.
        self._hangup_attempted = False

        # The stray byte of an odd-length PCM block, waiting for its partner.
        # See `_even_samples`: without it the audio shifts and turns to noise.
        self._odd_byte = b""

        # Audio that did not fit in one message and goes out in the next one,
        # with the tally of what had to be dropped for not fitting there
        # either.
        self._pending_overflow = b""
        self._overflow_dropped = 0

        # Warnings that only make sense once per call.
        self._warned_plain_text = False
        self._warned_early_audio = False
        self._warned_rate_mismatch = False
        self._warned_out_rate = False
        self._warned_marker_without_processor = False
        self._untranslated: set[str] = set()

        # The source rate `_resample_out` was built for. The bot audio
        # carries its own rate (`audio_out_sample_rate`), which need not be
        # the input one: see `_resample_out_at`.
        self._out_source_rate = 0

    async def setup(self, frame: StartFrame) -> None:
        """Pipecat starts the pipeline and declares the rate it works at.

        A starting value only: once MEDIA_START arrives the channel's real
        codec rules. The pipeline rate is kept apart to warn when the two
        differ, a failure that otherwise degrades in silence
        (`_setup_resamplers`).

        Clears the previous call's state, which is what makes a reused
        instance safe: without it the second call inherits the hang-up guard
        and never hangs up. But ONLY for a new call. `setup` is NOT called
        once per connection: `FastAPIWebsocketTransport` calls it TWICE on the
        same serializer, from the input side (`fastapi.py:314`) and from the
        output side (`:462`), each with its own initialized flag. If
        MEDIA_START lands between the two, a reset here wipes the codec just
        learned and the call goes mute: no audio, no transcription,
        `Framesout=0` in Asterisk. Pinned by
        `test_a_second_setup_does_not_forget_the_codec_of_a_live_call`.

        Args:
            frame: The StartFrame of the pipeline.
        """
        # The SAME StartFrame or a new one is what tells the two apart: the
        # pipeline creates ONE StartFrame and hands it to both sides of the
        # transport (`worker.py:1089`), so a repeated `id` means "the other
        # side of the same start" and a new one means "another call".
        same_call = frame.id == self._start_frame_id
        self._start_frame_id = frame.id

        if not same_call:
            self._reset_call_state()

        self._pipeline_sample_rate = frame.audio_in_sample_rate
        self._sample_rate = self._params.sample_rate or frame.audio_in_sample_rate

        # The codec already learned beats what the pipeline just declared, and
        # the conversion has to be rebuilt: this `setup` came AFTER
        # MEDIA_START, and the pipeline rate was unknown until now.
        if self._media is not None:
            self._sample_rate = (self._params.sample_rate
                                 or self._media.sample_rate)
            self._resample_in = self._resample_out = None
            self._setup_resamplers()

    def _reset_call_state(self) -> None:
        """Leaves the serializer as freshly built, settings aside."""
        self._media = None
        self._encode = self._decode = None
        self._aligner = None
        # The converters keep state of the previous stream: reusing them
        # across calls drags audio of the previous call into this one.
        self._resample_in = self._resample_out = None
        self._hangup_attempted = False
        self._overflow_dropped = 0
        self._warned_plain_text = False
        self._warned_early_audio = False
        self._warned_rate_mismatch = False
        self._warned_out_rate = False
        self._warned_marker_without_processor = False
        self._out_source_rate = 0
        self._untranslated = set()
        self._drop_stale_audio()

    def _drop_stale_audio(self) -> None:
        """Drops the half-done audio that is no longer valid.

        Those bytes are in the PREVIOUS codec: releasing them in the new one
        is noise, and the stray byte also shifts everything behind it by half
        a step.
        """
        self._pending_overflow = b""
        self._odd_byte = b""

    # ------------------------------------------------------------------
    # From the pipeline toward Asterisk
    # ------------------------------------------------------------------

    async def serialize(self, frame: Frame) -> str | bytes | None:
        """Turns a pipeline frame into something the channel understands.

        The branch order is the Pipecat contract and is not cosmetic: the
        close has to beat the audio, or the goodbye stays queued while the
        pipeline is already down.

        Args:
            frame: Any frame the transport hands over.

        Returns:
            A control command (`str`), audio (`bytes`), or None for frames
            the channel has no equivalent for.
        """
        if (
            self._params.auto_hang_up
            and not self._hangup_attempted
            and isinstance(frame, (EndFrame, CancelFrame))
        ):
            # Hanging up is a command of the channel itself: no credentials
            # and no network call that can fail.
            self._hangup_attempted = True
            return build_command(Command.HANGUP)

        if isinstance(frame, HangUpAfterTurnFrame):
            # The marker reached the transport: nobody turned it into an
            # EndFrame, so the call will not hang up. Said once, with the fix
            # (`test_the_marker_without_the_processor_warns_once`).
            if not self._warned_marker_without_processor:
                self._warned_marker_without_processor = True
                log.warning(
                    "A HangUpAfterTurnFrame reached the transport, so the call "
                    "will NOT hang up: put EndAfterBotTurn() in the pipeline "
                    "before transport.output(), which turns it into an "
                    "EndFrame once the bot's turn has played."
                )
            return None

        if isinstance(frame, InterruptionFrame):
            # Barge-in: the channel queue is flushed AND so is everything this
            # file holds half-done, which is also speech the person cut off.
            # Left alone it goes out glued in FRONT of the next reply, as a
            # hiccup of the interrupted utterance. Pinned by
            # `test_the_interruption_drops_the_alignment_remainder`.
            if self._aligner is not None:
                # The return value is DISCARDED on purpose: `flush()` is used
                # for its emptying effect, not for the padded tail it returns,
                # which is exactly what must not be sent.
                self._aligner.flush()
            self._pending_overflow = b""
            self._odd_byte = b""
            return build_command(Command.FLUSH_MEDIA)

        if isinstance(frame, AudioRawFrame):
            return await self._encode_audio(frame.audio, frame.sample_rate)

        if isinstance(frame, (OutputTransportMessageFrame,
                              OutputTransportMessageUrgentFrame)):
            # The channel only understands ITS commands and answers ERROR to
            # any other text, so these are all dropped. A cloud serializer
            # forwards them to its provider; here there is nobody to forward
            # to.
            return None

        # Everything else is not translated, OUTGOING DTMF included
        # (OutputDTMFFrame): the channel has `.send_digit_end` in its tech
        # (`chan_websocket.c:169`) but no text command to ask for it; that
        # path is the core's toward the channel, not the application's.
        #
        # The type is logged, not the frame: Pipecat adds frames every
        # release, and a silent drop leaves "the pipeline sends something and
        # nothing happens" without a single clue. Only the type, because an
        # audio frame in the log is unreadable.
        self._note_untranslated(type(frame).__name__)
        return None

    def _note_untranslated(self, name: str) -> None:
        """Records a frame type that is not translated, once.

        Once per type and not per frame: in a talkative pipeline this runs
        fifty times a second and buries the rest of the log.

        Args:
            name: The frame class name.
        """
        if name in self._untranslated:
            return
        self._untranslated.add(name)
        log.debug("Pipecat frame with no equivalent in the channel: %s", name)

    async def _encode_audio(self, samples: bytes, sample_rate: int) -> bytes | None:
        """PCM from the pipeline to the channel codec, in whole frames.

        A coroutine because it may have to CONVERT the rate, and the Pipecat
        converter is asynchronous. Without conversion (the recommended setup,
        both rates equal) not a byte is touched.

        Returns None while no whole frame is ready: the rest waits for the
        next block. That hold is what avoids the gap, because the channel
        drops the tail of any message that is not a whole number of frames
        (`chan_websocket.c:1050-1064`).

        Known limit: the last remainder of a call is lost, under one frame
        (20 ms in G.711). There is nowhere to flush it: the frame that ends
        the bot's turn never reaches this file (the transport only passes
        audio, interruptions, messages and the close, `fastapi.py:473-551`),
        and padding every block would bring back the choppiness alignment
        exists to avoid. Whoever cannot afford those milliseconds uses
        `align_frames=False` and leaves the chunking to the transport.

        Args:
            samples: 16-bit PCM, any length.
            sample_rate: The rate those samples carry, from the frame.

        Returns:
            Whole frames in the channel codec, or None while filling.
        """
        if self._encode is None:
            return None

        # Converted BEFORE splitting samples and encoding: the converter needs
        # whole PCM at the source rate, and everything after this works at
        # the channel rate. The source rate is the FRAME's, not the pipeline
        # input rate: the transport resamples the bot audio to
        # `audio_out_sample_rate` and rebuilds the frame with it
        # (`base_output.py:129`, `fastapi.py:533`), and the two pipeline rates
        # need not match. Pinned by
        # `test_the_output_rate_is_the_frame_rate_not_the_input_rate`.
        if samples and sample_rate and sample_rate != self._sample_rate:
            samples = await self._resample_out_at(sample_rate).resample(
                samples, sample_rate, self._sample_rate)

        samples = self._even_samples(samples)
        if not samples and not self._pending_overflow:
            return None

        encoded = self._encode(samples) if samples else b""

        # What did not fit in the previous message goes FIRST: audio has to
        # leave in the order it was generated.
        if self._pending_overflow:
            encoded = self._pending_overflow + encoded
            self._pending_overflow = b""

        if self._aligner is None:
            return self._fit_message(encoded)

        frames = self._aligner.push(encoded)
        if not frames:
            return None
        # The transport sends one message per returned value, and several
        # frames can be ready here: they are joined into one. The channel
        # reads the binary as a stream of samples, so two frames in one
        # message sound the same as in two, with fewer trips.
        return self._fit_message(b"".join(frames))

    def _even_samples(self, samples: bytes) -> bytes:
        """Returns an EVEN number of bytes, keeping the odd one left over.

        CAREFUL when touching this: dropping that byte does not lose one
        sample, it shifts every following one by half a step and the audio
        turns to noise. The measurement is in `docs/decisions.md`; pinned by
        `test_a_split_sample_block_does_not_corrupt_the_audio`.

        Args:
            samples: 16-bit PCM, any length.

        Returns:
            The same audio cut to whole samples.
        """
        if self._odd_byte:
            samples = self._odd_byte + samples
        if len(samples) % 2:
            self._odd_byte = samples[-1:]
            return samples[:-1]
        self._odd_byte = b""
        return samples

    def _fit_message(self, audio: bytes) -> bytes | None:
        """Caps the audio to what fits in one channel message.

        A message over the WebSocket limit is not dropped: it hangs up the
        call (`protocol.MAX_WEBSOCKET_MESSAGE_BYTES`). What does not fit is
        chunked and goes out in the following messages, milliseconds later in
        live audio.

        CAREFUL: it cannot accumulate without draining. The transport sends
        ONE message per `serialize` (`fastapi.py:558-567`), so holding the
        whole remainder keeps more audio than will ever go out, and it is
        lost at hang-up. The remainder is chunked here and leaves right
        away, and what no longer fits in the queue is dropped with a warning
        instead of growing without bound. Pinned by
        `test_the_audio_waiting_for_a_slot_does_not_grow_without_bound`.

        Known limit: the remainder only drains with the NEXT `AudioRawFrame`.
        If a turn ends with a remainder pending, the tail of utterance N goes
        out glued in front of utterance N+1. Only with blocks bigger than one
        message; the interruption clears it.

        Args:
            audio: Encoded audio, any length.

        Returns:
            At most one message worth of audio, or None if empty.
        """
        if not audio:
            return None
        if len(audio) <= MAX_WEBSOCKET_MESSAGE_BYTES:
            return audio

        head = audio[:MAX_WEBSOCKET_MESSAGE_BYTES]
        tail = audio[MAX_WEBSOCKET_MESSAGE_BYTES:]

        room = MAX_PENDING_OVERFLOW_BYTES - len(self._pending_overflow)
        if len(tail) > room:
            # Drop the NEWEST and keep the old: audio has to leave in order,
            # so what stays is what sounds first.
            dropped = len(tail) - max(room, 0)
            self._overflow_dropped += dropped
            log.warning(
                "The pipeline delivers audio faster than the channel plays "
                "it: dropping %d bytes (%d in total).",
                dropped, self._overflow_dropped,
            )
            tail = tail[:max(room, 0)]

        self._pending_overflow += tail
        return head

    def _resample_out_at(self, source_rate: int) -> Any:
        """The output converter for this source rate, warning once per call.

        Built here and not in `_setup_resamplers`, which only knows the input
        rate: with the input matching the channel and the output not, that
        method builds nothing and the voice would play at another speed.
        Pinned by `test_the_output_rate_is_the_frame_rate_not_the_input_rate`
        and `test_output_at_the_channel_rate_is_not_converted_even_if_the_input_is`.

        Args:
            source_rate: The rate of the frame being encoded.

        Returns:
            A stream resampler from `source_rate` to the channel rate.
        """
        if self._resample_out is None or source_rate != self._out_source_rate:
            # A stream resampler carries state from one rate pair: a new
            # source rate gets a new one.
            self._resample_out = create_stream_resampler()
            self._out_source_rate = source_rate

        # The input warning already covers a pipeline built at one rate for
        # both directions; this one is for an output rate of its own.
        if source_rate != self._pipeline_sample_rate and not self._warned_out_rate:
            self._warned_out_rate = True
            log.warning(
                "The channel delivers %d Hz and your pipeline plays at %d Hz, "
                "so the bot audio is converted on every frame. For %d Hz ask "
                "for c(%s) in the Dial, or build the pipeline with "
                "PipelineParams(audio_out_sample_rate=%d).",
                self._sample_rate, source_rate,
                source_rate, codec_name_for(source_rate),
                self._sample_rate,
            )
        return self._resample_out

    def _setup_resamplers(self) -> None:
        """Prepares the conversion when the pipeline and the channel differ.

        The `c()` of the Dial decides the call rate, but the channel only
        announces it in MEDIA_START, when the pipeline has ALREADY started,
        and neither side renegotiates (`chan_websocket.c:1178`,
        `stt_service.py:359`). The only place for the adaptation is here, in
        the middle.

        Matching rates are still best, hence the warning: converting costs
        CPU on every frame and in both directions. Left alone, a mismatch
        plays the voice at another speed with no exception at all. Pinned by
        `test_the_audio_is_resampled_when_the_rates_differ`.
        """
        if not self._pipeline_sample_rate:
            return
        if self._sample_rate == self._pipeline_sample_rate:
            return

        # Once per call. The transport calls `setup` TWICE (see `setup`), and
        # without this guard the same warning came out twice in a row: a log
        # line that repeats reads as if it happened twice. Pinned by
        # `test_the_rate_warning_is_logged_once_per_call`.
        if self._warned_rate_mismatch:
            self._resample_in = create_stream_resampler()
            self._resample_out = create_stream_resampler()
            self._out_source_rate = self._pipeline_sample_rate
            return
        self._warned_rate_mismatch = True

        log.warning(
            "The channel delivers %d Hz and your pipeline was built at %d Hz, "
            "so the audio is converted in both directions. It works, but it "
            "costs CPU on every frame: make them match if you can. For %d Hz "
            "ask for c(%s) in the Dial, or build the pipeline with "
            "PipelineParams(audio_in_sample_rate=%d, audio_out_sample_rate=%d).",
            self._sample_rate, self._pipeline_sample_rate,
            # The Dial hint names the PIPELINE rate: the channel already has
            # its own. Pinned by `test_the_audio_is_resampled_when_the_rates_differ`.
            self._pipeline_sample_rate, codec_name_for(self._pipeline_sample_rate),
            self._sample_rate, self._sample_rate,
        )

        self._resample_in = create_stream_resampler()
        self._resample_out = create_stream_resampler()
        self._out_source_rate = self._pipeline_sample_rate

    # ------------------------------------------------------------------
    # From Asterisk toward the pipeline
    # ------------------------------------------------------------------

    async def deserialize(self, data: str | bytes) -> Frame | None:
        """Turns what the channel sends into a pipeline frame.

        Discriminated by Python type, not by inspecting bytes: the WebSocket
        already delivers `str` for text frames (control) and `bytes` for
        binary ones (audio), and the channel never mixes them.

        Args:
            data: One WebSocket message.

        Returns:
            A frame, or None for what the pipeline has no use for.
        """
        if isinstance(data, (bytes, bytearray)):
            return await self._audio_frame(bytes(data))
        return self._control_frame(data)

    async def _audio_frame(self, chunk: bytes) -> Frame | None:
        """Caller audio toward the pipeline, already as PCM."""
        if self._decode is None:
            # Audio can arrive BEFORE MEDIA_START (`docs/protocol.md`, issue
            # #1712). Without the codec it cannot be decoded, and raw G.711
            # bytes handed to a model that expects PCM are noise. Those few
            # frames of the start are dropped.
            if not self._warned_early_audio:
                self._warned_early_audio = True
                log.warning(
                    "Audio arrived before the MEDIA_START: dropped until the "
                    "channel codec is known."
                )
            return None

        pcm = self._decode(chunk)
        if not pcm:
            return None

        # If the pipeline was built at another rate, the conversion is HERE.
        # The speech engine already connected at its own rate and does not
        # look again (`stt_service.py:359`): the channel audio as is makes it
        # read at another speed, and the transcript comes out wrong with no
        # exception.
        if self._resample_in is not None:
            pcm = await self._resample_in.resample(
                pcm, self._sample_rate, self._pipeline_sample_rate)
            if not pcm:
                return None

        return InputAudioRawFrame(
            audio=pcm,
            # The declared rate is the PIPELINE's, not the channel's: it is
            # the one the audio has after converting.
            sample_rate=self._pipeline_sample_rate or self._sample_rate,
            num_channels=1,
        )

    def _control_frame(self, raw: str) -> Frame | None:
        """Channel events. Almost none is translated, and that is right."""
        event, payload = parse_event(raw)

        if event is None:
            self._maybe_warn_plain_text(raw)
            return None

        if event is Event.MEDIA_START:
            # The format is learned AND the event goes up to the pipeline:
            # the dialplan variables live in no other event, and without
            # them whoever builds the pipeline does not know what number was
            # called.
            self._on_media_start(payload)
            return AsteriskMediaStartFrame(message=payload, media=self._media)

        if event is Event.DTMF_END:
            return self._dtmf_frame(payload.get("digit"))

        # The rest (XOFF, XON, marks, QUEUE_DRAINED, STATUS) has no frame in
        # Pipecat, and none is invented: the channel flow control is a
        # transport mechanism, and Pipecat does not model transport. Whoever
        # needs it uses galcymedia directly.
        return None

    def _on_media_start(self, payload: dict[str, Any]) -> None:
        """Learns how this call sounds."""
        media = parse_media_start(payload)
        self._media = media

        # A mid-call MEDIA_START changes the codec, and what is half-done was
        # written in the previous one.
        self._drop_stale_audio()

        fmt = media.audio_format.strip().lower()

        if fmt in _ALAW_NAMES:
            self._encode, self._decode = pcm_to_alaw, alaw_to_pcm
            self._sample_rate = _G711_SAMPLE_RATE
        elif fmt in _ULAW_NAMES:
            self._encode, self._decode = pcm_to_ulaw, ulaw_to_pcm
            self._sample_rate = _G711_SAMPLE_RATE
        else:
            # slin, slin16, slin24, ...: already 16-bit PCM, Pipecat's native
            # format. Not a byte is touched.
            self._encode = self._decode = _passthrough
            self._sample_rate = _slin_sample_rate(fmt)

        # The explicit override wins over the derived value. A zero is not
        # accepted: it reaches the Pipecat resampler and blows up there, far
        # from the cause.
        if self._params.sample_rate:
            self._sample_rate = self._params.sample_rate

        self._setup_resamplers()

        # Alignment uses the size the channel asks for. In passthrough the
        # channel sends zero and there is nothing to align (`FrameAligner`
        # treats it as "do not align"), and there is no barge-in there
        # either. The warning comes from the `media.passthrough` block below.
        self._aligner = (
            FrameAligner(media.optimal_frame_size, media.silence_byte)
            if self._params.align_frames
            else None
        )

        log.info(
            "Call started: channel=%s format=%s frame=%dB sample_rate=%d",
            media.channel_name, media.audio_format,
            media.optimal_frame_size, self._sample_rate,
        )

        if media.passthrough:
            log.warning(
                "The channel is in PASSTHROUGH (format %r): barge-in and the "
                "clean hang-up do not work, and the audio arrives compressed, "
                "so the pipeline will not understand it. Use c(slin16) or "
                "c(ulaw) in the Dial and let Asterisk transcode.",
                media.audio_format,
            )

    def _dtmf_frame(self, digit: Any) -> Frame | None:
        """A keypad digit toward the pipeline.

        Asterisk also emits A-D, part of the DTMF standard but absent from
        the Pipecat keypad (`types.py:35-47`). They are ignored instead of
        raising: a strange digit cannot bring down a call.
        """
        try:
            return InputDTMFFrame(button=KeypadEntry(str(digit)))
        except ValueError:
            log.debug("DTMF %r has no equivalent in Pipecat, ignored", digit)
            return None

    def _maybe_warn_plain_text(self, raw: str) -> None:
        """Warns once if the channel is not speaking JSON.

        The old one-line format is not a lesser option, it is a broken one:
        its MEDIA_START carries no `channel_variables`
        (`chan_websocket.c:249-257`), so the dialplan cannot tell you
        anything about the call, and the marks lose their `correlation_id`.
        """
        if self._warned_plain_text or not isinstance(raw, str):
            return
        self._warned_plain_text = True
        log.warning(
            "The channel sent a control frame that is not JSON (%r). f(json) "
            "is missing in the Dial (or control_message_format = json in "
            "chan_websocket.conf). With the old format the dialplan "
            "variables do not arrive and the marks lose their correlation.",
            raw[:60],
        )


@dataclass
class HangUpAfterTurnFrame(ControlFrame):
    """Asks `EndAfterBotTurn` to hang up once the bot's current turn played.

    A `ControlFrame` on purpose (`frames.py:127`, pipecat `072df9de`): it
    travels in order, so pushed from a tool handler it lands behind the text
    of that turn and ahead of the next one. A `SystemFrame` would overtake
    the audio. The TTS service forwards it through the same queue as its
    audio contexts (`tts_service.py:879-888`), so downstream of the TTS it
    arrives after the speech of the turn it was pushed in.

    Push it from the tool instead of an `EndFrame`:

        await params.llm.push_frame(HangUpAfterTurnFrame(),
                                    FrameDirection.DOWNSTREAM)
    """


class EndAfterBotTurn(FrameProcessor):
    """Hangs up after the bot's NEXT turn has played: `finish()` for Pipecat.

    Place it between the TTS and `transport.output()`. It forwards every
    frame untouched; after a `HangUpAfterTurnFrame` it waits for the bot's
    turn to end and then pushes the `EndFrame` that the serializer turns into
    the channel's HANGUP, with the output transport draining its audio queue
    first (`base_output.py:506-520`).

    An `EndFrame` pushed straight from the tool hangs up BEFORE the model's
    answer to the tool result: that answer is the turn where the model says
    goodbye, and it died behind the EndFrame in every real call
    (2026-08-22). The why is in `docs/decisions.md`.

    The turn is over when the LLM closed its response AND no speech is in
    flight: `TTSStartedFrame`/`TTSStoppedFrame` pairs counted since the
    marker, with `LLMFullResponseEndFrame` seen. Only an End that follows a
    `LLMFullResponseStartFrame` seen after the marker counts: the tool
    handler runs as a task (`llm_service.py:1332`) and the LLM pushes the
    End of the turn that called the tool in its `finally`
    (`openai/base_llm.py:602-604`), so that End can land behind the marker,
    and it belongs to the old turn
    (`test_the_end_of_the_turn_that_called_the_tool_is_ignored`). Pipecat's
    TTS emits the End after its own Stopped (`tts_service.py:354,1629`), so
    one or two sentences make no difference. With a TTS over HTTP the base
    class puts
    the `TTSStoppedFrame` after `stop_frame_timeout_s` of idle, 3.0 s by
    default (`tts_service.py:157,294`): the hang-up lands three seconds
    after the last sentence. Not ours, but it is felt.

    An interruption during the goodbye hangs up at once, after forwarding
    the interruption. Nothing at all within `timeout_s` hangs up anyway.

    Args:
        timeout_s: Ceiling of the wait after the marker. Defaults to the
            `finish()` one, `FINISH_MAX_WAIT_S`.
        sleep: The sleep used for the timeout, injectable for tests.
    """

    def __init__(self, *, timeout_s: float = FINISH_MAX_WAIT_S,
                 sleep: Any = asyncio.sleep) -> None:
        super().__init__()
        self._timeout_s = timeout_s
        self._sleep = sleep
        self._armed = False
        self._fired = False
        self._in_flight = 0
        self._response_ended = False
        self._saw_start = False
        self._timer: asyncio.Task | None = None

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if isinstance(frame, HangUpAfterTurnFrame):
            # Consumed here: the transport would only warn about it.
            if not self._armed and not self._fired:
                self._armed = True
                self._in_flight = 0
                self._response_ended = False
                self._saw_start = False
                self._timer = asyncio.create_task(self._expire())
            return

        if isinstance(frame, (EndFrame, CancelFrame)):
            self._disarm()

        await self.push_frame(frame, direction)

        if not self._armed:
            return
        if isinstance(frame, TTSStartedFrame):
            self._in_flight += 1
        elif isinstance(frame, TTSStoppedFrame):
            self._in_flight = max(0, self._in_flight - 1)
        elif isinstance(frame, LLMFullResponseStartFrame):
            self._saw_start = True
        elif isinstance(frame, LLMFullResponseEndFrame) and self._saw_start:
            # An End with no Start since the marker closes the turn that
            # called the tool, which may still be draining behind us.
            self._response_ended = True

        # The interruption went out first: the transport flushes on it, and
        # the EndFrame behind it drains what is left, which is nothing.
        if isinstance(frame, InterruptionFrame):
            await self._fire("the caller interrupted the goodbye")
        elif self._response_ended and self._in_flight == 0:
            await self._fire("the bot's turn played out")

    async def _expire(self) -> None:
        await self._sleep(self._timeout_s)
        if self._armed and not self._fired:
            log.warning(
                "No bot turn within %.1fs of the hang-up request, hanging up "
                "anyway", self._timeout_s,
            )
            await self._fire("timeout")

    async def _fire(self, why: str) -> None:
        if self._fired:
            return
        self._fired = True
        self._disarm()
        log.info("Hanging up after the bot's turn: %s", why)
        await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)

    def _disarm(self) -> None:
        self._armed = False
        timer, self._timer = self._timer, None
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()


def _passthrough(data: bytes) -> bytes:
    """The audio is already in the right format: untouched."""
    return data


def _slin_sample_rate(fmt: str) -> int:
    """Samples per second of an Asterisk linear PCM format.

    Delegates to `protocol.sample_rate_for`, where the table lives: it was
    duplicated here, and two copies of a channel table diverge as soon as
    someone touches one.

    What that table settles, for whoever wants to "simplify" it: the name
    lies in one case, `slin44` is 44100 Hz and not 44000
    (`codec_builtin.c:371`). An unknown format falls back to 8 kHz with a
    warning instead of zero, which would blow up inside the pipeline when
    resampling.
    """
    return sample_rate_for(fmt)
