"""
ElevenLabs Agents over chan_websocket.

The adapter moves audio and the speaking turn between the Asterisk channel
and an ElevenLabs agent. What the bot SAYS does not live here.

This provider diverges from the other two in two ways. The first is about
the channel and the adapter solves it; the second is about the provider's
API and nobody solves it:

    - It does NOT speak alaw, only ulaw. The adapter transcodes an alaw
      channel with the tables in `galcymedia.pcm`, in both directions.
    - It has NO literal phrase injection and no per-response instructions.
      What the agent says is configured in its panel, not per call. That is
      why this adapter exposes no `say`, which the other two have: faking it
      would be lying.

What it does NOT do, and is up to you: the prompt, the greeting and the
language, which go in the initiation message you build; and the tools,
whose schemas live in the provider's panel and whose execution reaches your
`on_function_call`.

BARGE-IN IS RESOLVED BY NUMBER here. This is the only adapter that never
calls `resume()`: its one door back to speaking is the `reset_discard` of
`end_turn`, hung on `user_transcript`, and that door cannot tell a new
response from the tail of the one the caller just cut. So the adapter does
what the provider's own SDK does: the `interruption` carries an `event_id`,
every `audio` event carries its own, and audio whose number does not exceed
the last interruption is dropped before it reaches the speaking turn
(`tests/test_adapters_barge_in.py`). Why the guard in `speech.py` cannot be
hardened instead is in `docs/architecture.md`.

The minimum that works:

    from functools import partial
    from galcymedia import serve
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    asyncio.run(serve(partial(ElevenLabsProvider,
                              agent_id=MY_AGENT,
                              api_key=KEY)))    # the key, only if private

The provider protocol is at elevenlabs.io/docs; our side is in
docs/protocol.md.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable
from typing import Any, NamedTuple

from ..pcm import ALAW_SILENCE, ALAW_TO_ULAW, ULAW_SILENCE, ULAW_TO_ALAW
from . import _shared

log = logging.getLogger(__name__)

WSS_URL = "wss://api.elevenlabs.io/v1/convai/conversation"

# Measured against the real service. With NO client message at all: `ping`
# every 1.7 s and close 1002 "No user message received for 60 seconds". With
# audio flowing and ZERO pongs for 100 s (60 pings unanswered): the session
# stays open and converses. The audio is what resets the counter, so this
# cut only applies when nothing goes out; see `_shared.TOOL_TIMEOUT_S` and
# `docs/decisions.md`. The SDK does not carry the number.
PROVIDER_IDLE_CUT_S = 60.0

# Envelopes that do not follow `<type>_event`. The SDK reads each one by
# name (`conversation.py:581-630`); only the caller transcript breaks the
# pattern. Measured on the real agent: `{"type": "user_transcript",
# "user_transcription_event": {"user_transcript": ..., "event_id": ...}}`.
_ENVELOPES = {"user_transcript": "user_transcription_event"}

SIGNED_URL_ENDPOINT = (
    "https://api.elevenlabs.io/v1/convai/conversation/get-signed-url"
)

InitiationBuilder = Callable[[Any], dict]
FunctionHandler = Callable[[str, dict], Awaitable[Any]]


class AudioPath(NamedTuple):
    """How audio travels between the channel and the provider.

    `silence_byte` is the CHANNEL codec's: it pads the last frame of every
    utterance after `to_channel`, which is what the caller hears.
    """

    to_provider: Callable[[bytes], bytes]
    to_channel: Callable[[bytes], bytes]
    silence_byte: int


def resolve_audio_path(media: Any) -> AudioPath:
    """Decides the conversion from the channel codec.

    Fails on anything but G.711: anything else would need a resample this
    adapter does not do, and falling back to a default kills the call later
    and without a clue.
    """
    fmt = (media.audio_format or "").lower()

    # Only the names the channel writes in MEDIA_START (codec_builtin.c:168
    # and :183 through ast_format_get_name): "mulaw", "pcmu" and "pcma" never
    # arrive.
    if fmt == "ulaw":
        return AudioPath(_same, _same, ULAW_SILENCE)

    if fmt == "alaw":
        # Direct transcoding with 256-byte tables: one translate at C speed,
        # negligible cost per frame.
        return AudioPath(
            lambda chunk: chunk.translate(ALAW_TO_ULAW),
            lambda chunk: chunk.translate(ULAW_TO_ALAW),
            ALAW_SILENCE,
        )

    raise RuntimeError(
        f"The channel delivered codec {fmt!r} and ElevenLabs only speaks "
        f"G.711. Change the Dial to c(ulaw) or c(alaw)."
    )


def _same(chunk: bytes) -> bytes:
    """The codec already matches: untouched."""
    return chunk


def _event_id(payload: Any) -> int | None:
    """The `event_id` of a provider event, or None if it cannot be read.

    ACCEPTS A STRING AS WELL AS AN INTEGER, and it is not free tolerance: the
    provider sends the id both ways. Its types declare `int`, but its own
    SDK tests feed `{"event_id": "789"}` in quotes
    (`elevenlabs-python`, `tests/test_async_convai.py:334`), which is why its
    code converts with `int(event["event_id"])` instead of comparing
    directly (`conversation.py:582`). Rejecting the string would leave the
    correlation discard INERT against a deployment that numbers that way:
    nothing raised, nothing logged, and the tail of the interrupted
    utterance playing again
    (`test_elevenlabs_correlates_when_the_id_arrives_as_a_string`).

    What is rejected is what is not a number. A `bool` is an `int` in Python
    and would pass as an identifier; a dict or a text would blow up the
    comparison. The library validates at the boundary (`docs/decisions.md`)
    and what is not understood is ignored, which here means letting the
    audio play rather than muting the bot over an odd field.
    """
    if not isinstance(payload, dict):
        return None
    raw = payload.get("event_id")
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw.strip())
        except ValueError:
            return None
    return None


class ElevenLabsProvider:
    """One Asterisk call conversing with an ElevenLabs agent."""

    def __init__(
        self,
        session: Any,
        *,
        agent_id: str,
        api_key: str = "",
        initiation: InitiationBuilder | None = None,
        on_function_call: FunctionHandler | None = None,
        on_transcript: Callable[[str, str, bool], None] | None = None,
        on_provider_event: Callable[[str, dict], None] | None = None,
        tool_timeout_s: float = _shared.TOOL_TIMEOUT_S,
    ) -> None:
        """Prepares the call.

        Args:
            session: The galcymedia session the factory already hands you.
            agent_id: The agent that answers.
            api_key: Only needed if the agent is PRIVATE; then a signed URL
                is requested before connecting.
            initiation: Function that receives `session.media` and returns
                the initiation message (prompt, greeting, language). Without
                it, whatever the agent has configured in its panel is used.
            on_function_call: Called when the agent asks for a tool. Its
                schemas live in the provider's panel, not here.
            on_transcript: Called with (role, text, final) on every
                transcript.
            on_provider_event: The peephole. Called with (kind, payload) on
                EVERY event the provider sends, also the ones this adapter
                does not translate. It replaces nothing, it only lets you
                watch: it is how you reach an event the API adds tomorrow.
            tool_timeout_s: Cap on one tool call; see
                `_shared.TOOL_TIMEOUT_S`.
        """
        self.session = session
        self.media = session.media

        self._agent_id = agent_id
        self._api_key = api_key
        self._initiation = initiation
        self._on_function_call = on_function_call
        self._tool_timeout_s = tool_timeout_s

        self.audio_path = resolve_audio_path(self.media)

        # The session's own speaking turn pads with the channel's silence
        # byte, which is what the bot's audio is in after `to_channel`: no
        # override needed (`test_the_speaking_turn_pads_with_the_channel_byte`).
        self.speech = session.speech
        self._report = _shared.TranscriptReporter(session, on_transcript)
        self._watch = _shared.ProviderEvents(on_provider_event, "ElevenLabs")

        self.ws: Any = None
        self._reader: asyncio.Task | None = None
        self._filler: asyncio.Task | None = None
        self.filler: _shared.SilenceFiller | None = None
        self._closed = False
        self._gone = False
        self._frames_in = 0

        # Up to which event number an interruption cancelled. Any audio with
        # an `event_id` that does not exceed it belongs to a response the
        # caller already cut.
        #
        # None and not zero WHILE THERE HAS BEEN NONE, and here we part from
        # the provider's SDK on purpose. Theirs starts this counter at 0 and
        # compares with `<=` (`conversation.py:498`, `:582`), which would
        # drop an `event_id` of zero. It holds because the provider numbers
        # FROM ONE: their tests never use zero and the first value is always
        # 1. Their sentinel works by a property their docs do not promise.
        #
        # Not copied because the day they number from zero the symptom would
        # be the greeting cut at its first syllable, with no log and no
        # exception. With None there is no sentinel that can collide with a
        # legitimate value: without an interruption nothing is discarded,
        # wherever they start counting
        # (`test_elevenlabs_discards_a_zero_numbered_chunk_after_an_interruption`).
        self._last_interrupt_id: int | None = None

    # ------------------------------------------------------------------
    # VoiceProvider contract
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connects to the provider and answers the call."""
        url = await self._resolve_url()
        log.info("Connecting to ElevenLabs (agent=%s)", self._agent_id)

        self.ws = await _shared.connect(
            url, {}, "ElevenLabs",
            "A private agent needs the key; check it next to the agent_id "
            "at https://elevenlabs.io/app/agents",
        )

        if self._initiation is not None:
            await self.ws.send(json.dumps(self._initiation(self.media)))

        self._reader = asyncio.create_task(self._read_loop())
        self._filler = asyncio.create_task(self._build_filler().run())
        # The audio gate, BEFORE answering (why: the docstring of
        # Session.accept_audio).
        self.session.accept_audio()
        await self.session.answer()

    async def send_audio(self, chunk: bytes) -> None:
        """Caller audio: converted to ulaw if needed, and base64 encoded."""
        if self._closed or self._gone or self.ws is None:
            return
        try:
            await self._send_frame(chunk)
            if self.filler is not None:
                self.filler.note_caller_frame()
            self._frames_in += 1
        except Exception:
            log.debug("The audio did not go out: the connection is already closed",
                      exc_info=True)

    async def on_dtmf(self, digit: str) -> None:
        """A keypad digit. What each key means is yours."""
        log.info("The caller pressed %s", digit)

    async def close(self) -> None:
        """Releases the connection. Tolerates being called twice."""
        if self._closed:
            return
        self._closed = True
        await _shared.close_quietly(self.ws, (self._reader, self._filler))

        log.info("Provider session closed (%d frames sent)",
                 self._frames_in)

    async def _send_frame(self, chunk: bytes) -> None:
        """One channel frame to the provider: transcoded and base64 wrapped.

        Shared by the caller's audio and the filler. It reports nothing: the
        gap clock and the frame counter belong to `send_audio`
        (`test_the_filler_does_not_reset_the_gap_clock`).
        """
        await self.ws.send(json.dumps({
            "user_audio_chunk": base64.b64encode(
                self.audio_path.to_provider(chunk)).decode("ascii"),
        }))

    def _build_filler(self) -> _shared.SilenceFiller:
        """The silence filler for a quiet channel (`_shared.SilenceFiller`).

        Its frames are CHANNEL silence and go through `_send_frame`, the
        same path as the caller's audio, so an alaw channel's filler reaches
        the provider as ulaw (`test_the_filler_goes_through_the_audio_path`).
        """
        self.filler = _shared.SilenceFiller(
            self._send_frame,
            silence_byte=self.audio_path.silence_byte,
            frame_size=self.media.optimal_frame_size,
            ptime_ms=self.media.ptime,
        )
        return self.filler

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def _resolve_url(self) -> str:
        """Decides how the agent is reached.

        With a key the signed URL is requested, which a private agent
        demands; without one it connects directly, which a public agent
        allows. The request runs in a thread because urllib is synchronous
        and the event loop belongs to EVERY call of the process.
        """
        agent = urllib.parse.quote(self._agent_id)
        if not self._api_key:
            return f"{WSS_URL}?agent_id={agent}"

        def fetch() -> str:
            request = urllib.request.Request(
                f"{SIGNED_URL_ENDPOINT}?agent_id={agent}",
                headers={"xi-api-key": self._api_key},
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                return str(json.load(response).get("signed_url", ""))

        try:
            url = await asyncio.to_thread(fetch)
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                raise RuntimeError(
                    "ElevenLabs rejected the key. Check it at "
                    "https://elevenlabs.io/app/settings/api-keys"
                ) from exc
            raise RuntimeError(
                f"ElevenLabs answered {exc.code} to the signed URL request"
            ) from exc
        except OSError as exc:
            # URLError (network down, DNS) is an OSError. Translated with the
            # same hint connect() gives, instead of urllib's raw error, which
            # does not say where to look.
            raise RuntimeError(
                f"Could not reach ElevenLabs for the signed URL: {exc}. "
                f"Check this machine's internet access."
            ) from exc
        except (ValueError, json.JSONDecodeError) as exc:
            # The signed response was not JSON: a proxy or a captive portal
            # returning HTML, almost always. json.JSONDecodeError is a
            # ValueError subclass; named so it is clear it is covered.
            raise RuntimeError(
                "ElevenLabs returned a response that could not be understood "
                "for the signed URL. Check the agent_id and that no proxy is "
                "in the way."
            ) from exc

        if not url:
            raise RuntimeError(
                "ElevenLabs returned no signed URL: check the agent_id."
            )
        return url

    # ------------------------------------------------------------------
    # Incoming stream
    # ------------------------------------------------------------------

    async def _read_loop(self) -> None:
        """Consumes what the provider sends until the call ends."""
        await _shared.read_until_closed(
            self.ws, self._on_event, on_gone=self._hangup_if_alive,
        )

    async def _hangup_if_alive(self) -> None:
        # A provider that drops mid-call cannot leave the line open and mute.
        # The caller's frames keep arriving until Asterisk releases the
        # channel: none of them may touch the dead socket, or the log gets a
        # traceback per frame (`test_after_the_provider_drops_send_audio_stays_quiet`).
        # The filler does not look at `_gone` on purpose: its send fails once
        # and the task ends by itself
        # (`test_the_filler_ends_cleanly_if_the_socket_is_gone`).
        self._gone = True
        if not self._closed:
            await self.session.hangup()

    async def _on_event(self, raw: str) -> None:
        """Translates a provider event into the channel's speaking turn."""
        event = _shared.parse_json(raw)
        if event is None:
            return

        kind = event.get("type", "")

        # The peephole, before translating (the rule, in _shared.ProviderEvents).
        # It gets the whole envelope, not the unwrapped content: what we do
        # not translate is understood with the envelope, not without it.
        self._watch(kind, event)

        # The content comes wrapped and the envelope's name varies: the
        # SDK's names first, then `<kind>_event`, then the whole event. The
        # caller transcript is the one that does not follow the pattern:
        # `user_transcript` arrives in `user_transcription_event`
        # (`conversation.py:619-621`); with the generic guess it fell back
        # to the whole event and the text came out empty, so no `[user]`
        # line in a real call (`test_the_caller_transcript_uses_the_sdk_envelope`).
        payload = (event.get(_ENVELOPES.get(kind, ""))
                   or event.get(f"{kind}_event") or event.get(kind) or event)

        if kind == "audio":
            # Audio of an already interrupted response is dropped HERE, by
            # identifier, before touching the speaking turn: what the
            # provider's SDK does (`conversation.py:582`). This adapter never
            # calls `resume()`, so it depends on the `reset_discard` of
            # `end_turn`, which cannot tell new audio from old; the number
            # answers that without assuming any event order, which the
            # provider does not document
            # (`test_elevenlabs_drops_the_audio_of_an_interrupted_response`).
            if self._is_stale_audio(payload):
                return
            block = base64.b64decode(payload.get("audio_base_64", "") or "")
            await self.speech.play(self.audio_path.to_channel(block))

        elif kind == "user_transcript":
            self._report(
                "user", str(payload.get("user_transcript", "")))
            # This provider sends no end of turn of its own: the caller's
            # transcript IS the signal that the bot's turn is over. The
            # caller speaking arrives through TWO events with opposite
            # effects: this one closes the turn in order (flushes the
            # aligner's tail), `interruption` below empties what is queued
            # (`test_the_caller_speaking_closes_the_bot_turn`).
            await self.speech.end_turn(reset_discard=True)

        elif kind in ("agent_response", "agent_response_correction"):
            self._report(
                "assistant",
                str(payload.get("agent_response")
                    or payload.get("corrected_agent_response", "")))

        elif kind == "interruption":
            # The interruption's number is the boundary. Recorded BEFORE
            # emptying, because old frames can arrive while the flush is in
            # flight (`test_elevenlabs_does_not_walk_the_interruption_limit_backwards`).
            self._note_interruption(payload)
            _shared.report_caller_speaking(self.session)
            await self.speech.interrupt()

        elif kind == "ping":
            # Answered as the SDK does (`conversation.py:991-997`). Measured:
            # with audio flowing the provider keeps the session open through
            # 60 unanswered pings; the pong matters when nothing else goes
            # out (`PROVIDER_IDLE_CUT_S`).
            await self._pong(payload.get("event_id"))

        elif kind == "client_tool_call":
            await self._run_function(payload)

        elif kind == "conversation_initiation_metadata":
            self._check_audio_format(payload)

        elif kind == "error":
            _shared.report_provider_error(self.session, event)

        else:
            self._watch.note_unhandled(kind)

    def _note_interruption(self, payload: dict) -> None:
        """Records up to which event number got cancelled."""
        event_id = _event_id(payload)
        if event_id is None:
            return
        if self._last_interrupt_id is None:
            self._last_interrupt_id = event_id
        else:
            self._last_interrupt_id = max(self._last_interrupt_id, event_id)

    def _is_stale_audio(self, payload: dict) -> bool:
        """True if this block belongs to a response already interrupted.

        Returns False, that is, lets the audio through, in two cases, and
        both lean the same way on purpose: this adapter already has a record
        of going mute, so when in doubt it plays.

            - no interruption yet: nothing is cancelled, and besides it is
              unknown from which number the provider starts counting
              (`test_elevenlabs_plays_the_first_chunk_of_the_call`)
            - the block carries no readable `event_id`: going mute over a
              missing field would be worse than one frame too many
              (`test_elevenlabs_plays_audio_that_carries_no_event_id`)
        """
        if self._last_interrupt_id is None:
            return False
        event_id = _event_id(payload)
        if event_id is None:
            return False
        return event_id <= self._last_interrupt_id

    async def _pong(self, event_id: Any) -> None:
        if self._closed or self.ws is None:
            return
        try:
            await self.ws.send(json.dumps({"type": "pong",
                                           "event_id": event_id}))
        except Exception:
            log.debug("Could not answer the ping", exc_info=True)

    def _check_audio_format(self, payload: dict[str, Any]) -> None:
        """Warns if the agent is not configured for ulaw at 8 kHz.

        The format belongs to the AGENT, not the call, so it cannot be
        negotiated here: if it does not match, the audio sounds like noise
        and the only place to fix it is the provider's panel.
        """
        for key in ("user_input_audio_format", "agent_output_audio_format"):
            value = str(payload.get(key, "") or "")
            if value and value != "ulaw_8000":
                log.error(
                    "The agent has %s=%r and this adapter speaks ulaw_8000. "
                    "Change it in the agent's panel or the audio will sound "
                    "wrong.", key, value,
                )

    async def _run_function(self, payload: dict[str, Any]) -> None:
        """Hands your hook the tool the agent asked for.

        Unlike the other two, the schemas are not sent on connect: they live
        in the provider's panel. The adapter only receives the call and
        returns what you decide.
        """
        name = str(payload.get("tool_name", ""))
        call_id = str(payload.get("tool_call_id", ""))
        arguments = payload.get("parameters") or {}
        if not isinstance(arguments, dict):
            arguments = {}

        result = await _shared.run_tool(
            self._on_function_call, name, arguments,
            session=self.session, tool_call_id=call_id,
            timeout_s=self._tool_timeout_s)

        # `is_error` comes from YOUR result, not from our guess: whether "no
        # slot available" is an error or a normal answer is your business's
        # call, not the transport's
        # (`test_if_your_tool_fails_it_is_marked_as_an_error`).
        is_error = isinstance(result, dict) and result.get("ok") is False

        try:
            await self.ws.send(json.dumps({
                "type": "client_tool_result",
                "tool_call_id": call_id,
                "result": json.dumps(result, ensure_ascii=False),
                "is_error": is_error,
            }))
        except Exception:
            log.exception("Could not answer tool %s", name)
