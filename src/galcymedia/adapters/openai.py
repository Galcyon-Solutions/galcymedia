"""
OpenAI Realtime over chan_websocket.

The adapter moves audio and the speaking turn between the Asterisk channel
and the Realtime API. What the bot SAYS and what it DOES do not live here:
the prompt, the tools and the utterances are yours, and they come in
through the hooks.

What this file does:

    - opens the provider WebSocket and authenticates
    - wraps the caller's audio in base64, which is all this API asks for;
      no codec conversion, because it speaks G.711 natively
    - translates its stream events into the speaking turn: talking over the
      bot (barge-in), end of turn, partial and final transcript
    - builds the audio part of `session.update` from the channel format
    - tells the model how much of an interrupted reply the caller actually
      heard (`conversation.item.truncate`), so its history matches what was
      said on the line

What it does NOT do, and is up to you:

    - the prompt (`instructions`), the tools and the voice. You pass them in
      `session_config` and they are merged with the audio part unread
    - running the tools the model asks for: they reach your
      `on_function_call`
    - the bot's utterances. See `request_response`, which is this
      provider's twist

THE TRUNCATE is the part of this adapter that costs most to understand. When
the caller interrupts, muting the bot is not enough: the model's history
keeps the WHOLE sentence even if only two seconds played, so the bot takes
as said something the caller never heard. The three fields it needs come
from two places and none is guessed: `item_id` and `content_index` from
`response.output_audio.delta`, `audio_end_ms` from `speech.played_ms_at_most`,
which is a CEILING because the server rejects a value past the real audio.
The order is measure, cut, send. The whole reasoning, the ceiling, the
order and the discarded alternative are in `docs/architecture.md`.

CAREFUL with "make the bot say a phrase". This API has no literal injection:
it is emulated by asking for a response with instructions that apply to it
alone, that is, WRITING PROMPT. So the adapter exposes the transport
(`request_response`) and writes nothing: you write the instruction, you
know which language and tone your agent speaks.

The minimum that works:

    from functools import partial
    from galcymedia import serve
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    def my_config(media):
        return {"instructions": MY_PROMPT, "audio": {"output": {"voice": "marin"}}}

    asyncio.run(serve(partial(OpenAIRealtimeProvider,
                              api_key=KEY,
                              session_config=my_config)))

The provider protocol is at developers.openai.com; our side is in
docs/protocol.md.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .. import events
from . import _shared

log = logging.getLogger(__name__)

REALTIME_URL = "wss://api.openai.com/v1/realtime"

# Channel format to the API's audio type (`realtime_audio_formats.py:25,32`
# in its SDK). It speaks G.711 natively, so no conversion: only the base64
# wrapper.
# Only the names the channel really writes in MEDIA_START
# (`ast_format_get_name`, chan_websocket.c:236; `.name` in codec_builtin.c:168
# and :183). "pcma", "pcmu" and "mulaw" never arrive on the wire.
CODECS = {
    "alaw": "audio/pcma",
    "ulaw": "audio/pcmu",
}

SessionConfig = Callable[[Any], dict]
FunctionHandler = Callable[[str, dict], Awaitable[Any]]


def resolve_codec(media: Any) -> str:
    """Translates the channel format into the type OpenAI Realtime uses."""
    return _shared.resolve_codec(media, CODECS, "OpenAI Realtime")


class OpenAIRealtimeProvider:
    """One Asterisk call conversing with OpenAI Realtime."""

    def __init__(
        self,
        session: Any,
        *,
        api_key: str,
        model: str = "gpt-realtime",
        session_config: SessionConfig | None = None,
        on_function_call: FunctionHandler | None = None,
        on_transcript: Callable[[str, str, bool], None] | None = None,
        on_provider_event: Callable[[str, dict], None] | None = None,
        tool_timeout_s: float = _shared.TOOL_TIMEOUT_S,
    ) -> None:
        """Prepares the call.

        Args:
            session: The galcymedia session the factory already hands you.
            api_key: The provider key.
            model: The realtime model that takes the call.
            session_config: Function that receives `session.media` and
                returns your part of `session.update` (instructions, tools,
                voice, language). The adapter adds the audio and the turn,
                which belong to the channel. On the FORMAT the adapter wins
                and there is no overriding it: letting you change it would
                break the call silently. On the TURN it does not: there it
                uses setdefault, so if you send your own `turn_detection`
                yours wins (a noisy call center raises the threshold that
                way). The caller's transcripts (`on_transcript` with role
                `user`) only arrive if you ask for them here, with
                `audio.input.transcription` (for example
                `{"model": "gpt-4o-mini-transcribe"}`): without it the API
                sends no `input_audio_transcription` events at all.
            on_function_call: Called when the model asks for a tool, with
                (name, arguments). Whatever you return is sent back.
            on_transcript: Called with (role, text, final) on every
                transcript.
            on_provider_event: The peephole. Called with (kind, payload) on
                EVERY event the provider sends, also the ones this adapter
                does not translate. It replaces nothing, it only lets you
                watch: it is how you reach an event the API adds tomorrow.
            tool_timeout_s: Budget for the whole batch of tools in one
                response; see `_shared.TOOL_TIMEOUT_S`.
        """
        self.session = session
        self.media = session.media
        self.speech = session.speech

        self._api_key = api_key
        self._model = model
        self._session_config = session_config
        self._on_function_call = on_function_call
        self._tool_timeout_s = tool_timeout_s
        self._report = _shared.TranscriptReporter(session, on_transcript)
        self._watch = _shared.ProviderEvents(on_provider_event,
                                             "OpenAI Realtime")

        self.ws: Any = None
        self._reader: asyncio.Task | None = None
        self._filler: asyncio.Task | None = None
        self.filler: _shared.SilenceFiller | None = None
        self._closed = False
        self._gone = False
        self._frames_in = 0

        # The caller's transcript arrives in chunks and is assembled here.
        self._partial = ""

        # Which audio item is sounding, to truncate it if the caller
        # interrupts. Both fields come in `response.output_audio.delta`
        # (`response_audio_delta_event.py:13,22`) and are two of the three
        # that `conversation.item.truncate` takes; its SDK says of
        # `content_index` "Set this to `0`"
        # (`conversation_item_truncate_event.py:34-35`), and reading it from
        # the delta gives that same 0 without assuming an order this API
        # does not promise.
        self._audio_item_id: str | None = None
        self._audio_content_index = 0

    # ------------------------------------------------------------------
    # VoiceProvider contract
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connects to the provider and answers the call."""
        codec = resolve_codec(self.media)

        log.info("Connecting to OpenAI Realtime (model=%s codec=%s)",
                 self._model, codec)

        self.ws = await _shared.connect(
            f"{REALTIME_URL}?model={self._model}",
            {"Authorization": f"Bearer {self._api_key}"},
            "OpenAI Realtime",
            "Check it at https://platform.openai.com",
        )

        await self.ws.send(json.dumps(self._build_session_update(codec)))

        self._reader = asyncio.create_task(self._read_loop())
        self._filler = asyncio.create_task(self._build_filler().run())
        # The audio gate, BEFORE answering (why: the docstring of
        # Session.accept_audio).
        self.session.accept_audio()
        await self.session.answer()

    def _build_session_update(self, codec: str) -> dict:
        """Builds the `session.update` joining the channel's part and yours.

        From the channel come the audio format and the speaking turn, which
        are what makes the call sound and interruptible. The rest
        (instructions, tools, voice, language) is yours and travels as is
        (`test_your_config_travels_untouched_in_the_session_update`,
        `test_the_channel_wins_on_the_audio_format`).
        """
        config: dict[str, Any] = {}
        if self._session_config is not None:
            config = dict(self._session_config(self.media) or {})

        audio = {k: dict(v) for k, v in (config.pop("audio", {}) or {}).items()}
        audio.setdefault("input", {})
        audio.setdefault("output", {})

        # The channel wins on format and turn: they are what cannot fail.
        audio["input"]["format"] = {"type": codec}
        audio["input"].setdefault(
            "turn_detection",
            {"type": "server_vad", "interrupt_response": True},
        )
        audio["output"]["format"] = {"type": codec}

        return {
            "type": "session.update",
            "session": {
                "type": "realtime",
                "output_modalities": ["audio"],
                **config,
                "audio": audio,
            },
        }

    async def send_audio(self, chunk: bytes) -> None:
        """Passes the caller's audio to the provider, base64 encoded.

        The transport cost of this API: a third more bytes for the wrapper.
        No codec conversion.
        """
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
        """One channel frame to the provider, base64 wrapped.

        Shared by the caller's audio and the filler. It reports nothing: the
        gap clock and the frame counter belong to `send_audio`
        (`test_the_filler_does_not_reset_the_gap_clock`).
        """
        await self.ws.send(json.dumps({
            "type": "input_audio_buffer.append",
            "audio": base64.b64encode(chunk).decode("ascii"),
        }))

    def _build_filler(self) -> _shared.SilenceFiller:
        """The silence filler for a quiet channel (`_shared.SilenceFiller`).

        The API speaks the channel's codec, so the frame is the channel's
        silence and goes through `_send_frame`, the same path as the
        caller's audio (`test_the_filler_goes_through_the_audio_path`).
        Measured: after a phrase with no more audio the server VAD never
        reports `speech_stopped` and no reply ever comes (`docs/decisions.md`).
        """
        self.filler = _shared.SilenceFiller(
            self._send_frame,
            silence_byte=self.media.silence_byte,
            frame_size=self.media.optimal_frame_size,
            ptime_ms=self.media.ptime,
        )
        return self.filler

    # ------------------------------------------------------------------
    # What can be asked of the provider
    # ------------------------------------------------------------------

    async def request_response(self, instructions: str = "") -> None:
        """Asks the model for a response now, with nobody talking to it.

        This is what to use for the greeting or a notice, because this API
        has NO literal phrase injection: the closest thing is asking for a
        response with instructions that apply to it alone.

        You write the instruction, whole. Writing it here would put prompt
        in the library, and it would come out wrong besides: these
        instructions compete with the session's, so they usually have to
        repeat the language and the tone, and only your agent knows those.
        """
        if self._closed or self.ws is None:
            return
        message: dict[str, Any] = {"type": "response.create"}
        if instructions:
            message["response"] = {"instructions": instructions}
        await self.ws.send(json.dumps(message))

    async def _truncate_played_audio(self, played_ms: int | None) -> None:
        """Tells the model how far the utterance just cut was HEARD.

        Without this the history keeps the whole sentence even if only two
        seconds played, and the bot takes as said something nobody heard. Its
        SDK: "Truncating audio will delete the server-side text transcript to
        ensure there is not text in the context that hasn't been heard by the
        user" and "This will synchronize the server's understanding of the
        audio with the client's playback"
        (`conversation_item_truncate_event.py:17-21`).

        `played_ms` comes MEASURED from outside, it is not read here: the
        caller takes it before muting the bot and sends this afterwards, so
        no network call sits ahead of the audio cut. See the barge-in site.

        It stays quiet in two cases, and quiet is right:

            - no known audio item: there was no bot audio to truncate
            - `played_ms` is None: the library does NOT KNOW how much was
              heard (no turn, no format, or a `pause()` that misaligned the
              clock). An invented number does not fix the history and goes
              past the real audio, which the server rejects with an error
              (`:30-31`) that is published to the client: a silent defect
              of ours would become an alarm on the integrator's panel.
        """
        if self._closed or self.ws is None or not self._audio_item_id:
            return

        if played_ms is None:
            log.debug("Not truncating %s: unknown how much audio was heard",
                      self._audio_item_id)
            return

        try:
            await self.ws.send(json.dumps({
                "type": "conversation.item.truncate",
                "item_id": self._audio_item_id,
                "content_index": self._audio_content_index,
                "audio_end_ms": played_ms,
            }))
        except Exception:
            # A failed truncate cannot bring down the barge-in: muting the
            # bot is what matters, this only syncs the history.
            log.exception("Could not truncate the audio of item %s",
                          self._audio_item_id)
        finally:
            # An item already truncated is not truncated again: the next
            # delta brings its own.
            self._audio_item_id = None

    # ------------------------------------------------------------------
    # Incoming stream
    # ------------------------------------------------------------------

    async def _read_loop(self) -> None:
        """Consumes what the provider sends until the call ends.

        EVERYTHING arrives as text here: the bot's audio comes base64 encoded
        inside an event, not as a binary frame.
        """
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
        self._watch(kind, event)

        # A new response closes the discard of the previous one. Relying on
        # the end of turn alone leaves the bot MUTE when the new response
        # starts before the previous one finished closing: the discard stays
        # on and the audio is dropped in silence. The caller hears a turn
        # that never sounds and speaks again thinking the line dropped
        # (`test_a_new_response_clears_the_discard`).
        if kind == "response.created":
            self.speech.resume()

        if kind == "response.output_audio.delta":
            # WHICH item this audio belongs to, to truncate it later. The
            # event says it; deducing it from the order would build on
            # something this API does not promise.
            # It has to be a STRING, and it is not converted with str(): the
            # SDK declares it `str` (`response_audio_delta_event.py:22`), and
            # `str()` on whatever comes would turn a dict into "{'a': 1}",
            # which travels as an identifier and the server rejects with an
            # error published to the client. What is not understood is
            # ignored, and then there is no truncate (`docs/decisions.md`,
            # the library is the boundary).
            item_id = event.get("item_id")
            if isinstance(item_id, str) and item_id:
                self._audio_item_id = item_id
                index = event.get("content_index")
                # A `bool` is an `int` in Python and would pass as an index.
                self._audio_content_index = (
                    index if isinstance(index, int)
                    and not isinstance(index, bool) else 0)
            await self.speech.play(base64.b64decode(event.get("delta", "")))

        elif kind == "input_audio_buffer.speech_started":
            self.speech.note_caller_activity()
            _shared.report_caller_speaking(self.session)
            # MEASURE here and SEND after `interrupt()`. The ceiling GROWS
            # with the clock, so it is read first; the cut comes next because
            # muting the bot is what matters; a `ws.send()` ahead of the cut
            # would trade history precision for latency the caller hears.
            # Measured: `interrupt()` does not touch the turn's bounds, so
            # the value is the same before and after
            # (`test_openai_cuts_the_audio_before_sending_the_truncate`). The
            # full reasoning is in `docs/architecture.md`.
            # Only while the bot is still SOUNDING. The item id survives the
            # end of the response and the turn's clock only resets on the
            # next `play`, so after a reply heard whole, a caller who simply
            # answers would get it truncated: measured, the server accepts
            # that truncate (audio_end_ms = sent - 40 ms, 0 errors) and
            # deletes the transcript of a sentence the caller heard entire
            # (`test_openai_does_not_truncate_a_reply_heard_whole`).
            sounding = self.speech.bot_audio_playing
            played_ms = self.speech.played_ms_at_most if sounding else None
            await self.speech.interrupt()
            if sounding:
                await self._truncate_played_audio(played_ms)

        elif kind == "input_audio_buffer.speech_stopped":
            self.session.emit(events.user_stopped_speaking())

        elif kind == "response.output_audio.done":
            # The end of the AUDIO, which is not the end of the response. It
            # closes the turn because it marks that no more voice is coming:
            # here the aligner's tail goes out, the last bytes that did not
            # complete a frame. Without it the utterance is clipped at the
            # end, and only the long ones: a short one is saved by chance
            # when its length falls on a frame multiple
            # (`test_the_tail_of_a_long_phrase_is_not_lost`).
            await self.speech.end_turn(reset_discard=True)

        elif kind == "response.done":
            # Tools travel INSIDE the finished response, not in an event of
            # their own. This is the SECOND end_turn of the same turn when
            # the response carried audio (the end of the audio already sent
            # one); it does not re-emit bot_stopped_speaking, `was_speaking`
            # guards that. What it is really for is the case WITHOUT audio
            # (a response that only asks for tools): there
            # `response.output_audio.done` never comes, and without this
            # close the aligner's tail would stay for the next turn
            # (`test_closing_the_turn_twice_is_harmless`).
            #
            # The discard is lifted only if the response FINISHED, not if it
            # was cancelled, and the event itself declares it: `status` is
            # `cancelled` when the provider's VAD cut the response because
            # the caller started talking, `status_details.reason`
            # `turn_detected` (`realtime_response.py:87`,
            # `realtime_response_status.py:33-37`; measured live). Lifting
            # it there would play the tail of the utterance the caller just
            # interrupted.
            status = str((event.get("response") or {}).get("status", ""))
            await self.speech.end_turn(reset_discard=status != "cancelled")
            await self._run_functions(event)

        elif kind == (
            "conversation.item.input_audio_transcription.delta"
        ):
            self._partial += str(event.get("delta", ""))
            self._report("user", self._partial, final=False)

        elif kind == (
            "conversation.item.input_audio_transcription.completed"
        ):
            text = str(event.get("transcript", "") or self._partial)
            self._partial = ""
            self._report("user", text, final=True)

        elif kind == "response.output_audio_transcript.delta":
            # The bot's text as it is pronounced: lets a panel paint the
            # utterance while it sounds, instead of waiting for the turn end.
            self._report("assistant", str(event.get("delta", "")), final=False)

        elif kind == "response.output_audio_transcript.done":
            self._report("assistant",
                         str(event.get("transcript", "")), final=True)

        elif kind == "error":
            _shared.report_provider_error(self.session, event.get("error"))

        else:
            self._watch.note_unhandled(kind)

    async def _run_functions(self, event: dict[str, Any]) -> None:
        """Hands your hook the tools the model asked for.

        They come inside the response that just finished, not in an event
        of their own. After answering them a new response has to be
        requested, or the model stays quiet waiting: that is protocol, and
        the adapter does it (`test_tools_are_handed_to_your_handler`).
        """
        outputs = (event.get("response") or {}).get("output") or []
        calls = [item for item in outputs
                 if item.get("type") == "function_call"]
        if not calls:
            return

        # ONE budget for the whole batch, not one per tool: the model can ask
        # for five at once, and with a per-call cap the worst case multiplies
        # by five. Meanwhile this loop reads nothing from the provider
        # socket. Same shape as `Session.finish()`.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._tool_timeout_s

        for call in calls:
            name = call.get("name", "")
            call_id = str(call.get("call_id") or call.get("id") or name)

            arguments = _shared.parse_arguments(call.get("arguments"))

            result = await _shared.run_tool(
                self._on_function_call, name, arguments,
                session=self.session, tool_call_id=call_id,
                timeout_s=max(deadline - loop.time(), 0.0))

            try:
                await self.ws.send(json.dumps({
                    "type": "conversation.item.create",
                    "item": {
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": json.dumps(result, ensure_ascii=False),
                    },
                }))
            except Exception:
                log.exception("Could not answer tool %s", name)

        await self.request_response()
