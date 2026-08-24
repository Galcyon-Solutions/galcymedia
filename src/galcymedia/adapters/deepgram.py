"""
Deepgram Voice Agent over chan_websocket.

The adapter moves audio and the speaking turn between the Asterisk channel
and Deepgram. What the bot SAYS and what it DOES do not live here: the
prompt, the tools and the utterances are yours, and they come in through the
hooks.

What this file does:

    - opens the provider WebSocket and authenticates
    - hands it the caller's audio untouched, because Deepgram speaks alaw
      and ulaw at 8 kHz, which is exactly what the channel delivers
    - translates its stream events into the speaking turn: the caller talked
      over the bot (barge-in), the bot finished its turn, there is a
      transcript
    - fills the line with silence, at real time, when the channel stops
      delivering frames: the provider only advances with incoming audio

What it does NOT do, and is up to you: the `Settings` message (your prompt
and your tools), what each tool does, and what the bot says.

The minimum that works:

    from functools import partial
    from galcymedia import serve
    from galcymedia.adapters.deepgram import DeepgramProvider

    def my_settings(media):
        return {"type": "Settings", "audio": {...}, "agent": {...}}

    asyncio.run(serve(partial(DeepgramProvider,
                              api_key=KEY,
                              settings=my_settings)))

The provider protocol is at developers.deepgram.com/docs/voice-agent; our
side is in docs/protocol.md. What this file shares with the other adapters
lives in `_shared.py`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from ..pcm import ALAW_SILENCE, ULAW_SILENCE
from . import _shared

log = logging.getLogger(__name__)

AGENT_URL = "wss://agent.deepgram.com/v1/agent/converse"

# Channel format to the provider's name. Deepgram speaks G.711 natively, so
# no audio is converted in either direction.
# Only the names the channel really writes in MEDIA_START
# (`ast_format_get_name`, chan_websocket.c:236; `.name` in codec_builtin.c:168
# and :183). "pcma", "pcmu" and "mulaw" never arrive on the wire.
CODECS = {
    "alaw": "alaw",
    "ulaw": "mulaw",
}

# The silence byte of each provider codec, for the filler. Zero is not
# silence in G.711: it injects a click.
SILENCE = {"alaw": ALAW_SILENCE, "mulaw": ULAW_SILENCE}

SettingsBuilder = Callable[[Any], dict]
FunctionHandler = Callable[[str, dict], Awaitable[Any]]


def resolve_codec(media: Any) -> str:
    """Translates the channel format into the name Deepgram uses."""
    return _shared.resolve_codec(media, CODECS, "Deepgram")


class DeepgramProvider:
    """One Asterisk call conversing with Deepgram Voice Agent."""

    def __init__(
        self,
        session: Any,
        *,
        api_key: str,
        settings: SettingsBuilder,
        on_function_call: FunctionHandler | None = None,
        on_transcript: Callable[[str, str, bool], None] | None = None,
        on_provider_event: Callable[[str, dict], None] | None = None,
        tool_timeout_s: float = _shared.TOOL_TIMEOUT_S,
    ) -> None:
        """Prepares the call.

        Args:
            session: The galcymedia session the factory already hands you.
            api_key: The provider key.
            settings: Function that receives `session.media` and returns the
                complete `Settings` message. Your prompt and your tools go
                there: the adapter does not look at them, it only sends them.
            on_function_call: Called when the model asks for a tool, with
                (name, arguments). Whatever you return is sent back as is.
                Without it, tools are rejected with a notice.
            on_transcript: Called with (role, text, final) on every
                transcript. Useful for a panel or your own logic.
            on_provider_event: The peephole. Called with (kind, payload) on
                EVERY event the provider sends, also the ones this adapter
                does not translate. It replaces nothing, it only lets you
                watch: it is how you reach an event the API adds tomorrow.
            tool_timeout_s: Budget for the whole batch of tools in one
                request; see `_shared.TOOL_TIMEOUT_S`.
        """
        self.session = session
        self.media = session.media
        self.speech = session.speech

        self._api_key = api_key
        self._settings = settings
        self._on_function_call = on_function_call
        self._tool_timeout_s = tool_timeout_s
        self._report = _shared.TranscriptReporter(session, on_transcript)
        self._watch = _shared.ProviderEvents(on_provider_event, "Deepgram")

        self.ws: Any = None
        self._reader: asyncio.Task | None = None
        self._filler: asyncio.Task | None = None
        self.filler: _shared.SilenceFiller | None = None
        self._closed = False
        self._gone = False
        self._frames_in = 0

    # ------------------------------------------------------------------
    # VoiceProvider contract
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connects to the provider and answers the call."""
        codec = resolve_codec(self.media)

        log.info("Connecting to Deepgram (codec=%s)", codec)
        self.ws = await _shared.connect(
            AGENT_URL,
            {"Authorization": f"Token {self._api_key}"},
            "Deepgram",
            "Check it at https://console.deepgram.com",
        )

        await self.ws.send(json.dumps(self._settings(self.media)))

        self._reader = asyncio.create_task(self._read_loop())
        self._filler = asyncio.create_task(self._build_filler().run())

        # The audio gate opens BEFORE answering: answering is what makes
        # Asterisk start sending, and whatever arrives in that gap is lost.
        self.session.accept_audio()
        await self.session.answer()

    async def send_audio(self, chunk: bytes) -> None:
        """Passes the caller's audio to the provider, untouched."""
        if self._closed or self._gone or self.ws is None:
            return
        try:
            await self.ws.send(chunk)
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

    # ------------------------------------------------------------------
    # Transport toward the provider
    # ------------------------------------------------------------------

    async def say(self, phrase: str, behavior: str = "queue") -> None:
        """Sends an utterance for the bot to speak AS IS.

        Transport, not a decision: WHEN to make the bot speak and WHAT to say
        belong to your agent. This only builds the message Deepgram expects,
        so you do not have to know its format.

        `behavior` decides what happens if the bot is already speaking:
        `queue` waits its turn, `interrupt` cuts in. The API default refuses
        the message in that case with `InjectionRefused`, which only reaches
        the log through the peephole (`agent_v1inject_agent_message.py`).
        """
        if self._closed or self.ws is None:
            return
        await self.ws.send(json.dumps({
            "type": "InjectAgentMessage",
            "message": phrase,
            "behavior": behavior,
        }))

    # ------------------------------------------------------------------
    # Incoming stream
    # ------------------------------------------------------------------

    async def _read_loop(self) -> None:
        """Binary messages are the bot's voice; text messages are events."""
        await _shared.read_until_closed(
            self.ws, self._on_event, self.speech.play,
            on_gone=self._hangup_if_alive,
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
        """Translates a provider event into the channel's speaking turn.

        This is the part we have to maintain: if Deepgram changes HOW it
        signals an interruption, it is fixed here. If it changes the format
        of its configuration message, that belongs to whoever builds the
        `settings`.
        """
        event = _shared.parse_json(raw)
        if event is None:
            return

        kind = event.get("type", "")

        # The peephole, before translating (the rule, in _shared.ProviderEvents).
        self._watch(kind, event)

        # A new response closes the discard of the previous one. Relying on
        # the end of turn alone leaves the bot mute when the interruption
        # landed on its last frames (`test_a_new_response_reopens_the_bot_voice`).
        if kind in ("ConversationText", "FunctionCallRequest") \
                and event.get("role") != "user":
            self.speech.resume()

        if kind == "ConversationText":
            # Published as final, and it is not our guess: the schema of this
            # event (type, role, content, languages, languages_hinted) has NO
            # finality field (`agent_v1conversation_text.py`), and the provider
            # sends it once per turn it considers CLOSED. A person who speaks
            # in pauses produces several, all definitive: the provider's
            # end-of-turn engine decides, and the knob (`eot_threshold`) only
            # exists in the v2 schema, the `flux` family
            # (`deepgram_listen_provider_v2.py:38`). Whoever builds the
            # `settings` can pass it; this adapter sends them as is.
            self._report(event.get("role", "?"), event.get("content", ""))

        elif kind == "UserStartedSpeaking":
            _shared.report_caller_speaking(self.session)
            # The "is the bot really silent" guard lives in speech.
            await self.speech.interrupt()

        elif kind == "AgentAudioDone":
            # The provider does send `AgentStartedSpeaking` (with its
            # latencies, `agent_v1agent_started_speaking.py`), but the turn
            # opens on the first audio block in `speech.play`: that is what
            # the caller hears, and the event falls to the peephole.
            await self.speech.end_turn(reset_discard=True)

        elif kind == "FunctionCallRequest":
            await self._run_functions(event)

        elif kind == "Error":
            _shared.report_provider_error(self.session, event)

        elif kind == "Warning":
            # The provider separates two diagnostics: `Warning` "notifies the
            # client of non-fatal errors or warnings" (`agent_v1warning.py`)
            # and the session goes on. It goes to the log and not to the
            # peephole because a warning explains why the bot behaved oddly,
            # and `note_unhandled` would print it at DEBUG once per kind: the
            # second warning of the call would appear nowhere.
            log.warning("Deepgram warning: %s (code %s)",
                        event.get("description", "?"), event.get("code", "?"))

        else:
            self._watch.note_unhandled(kind)

    async def _run_functions(self, event: dict[str, Any]) -> None:
        """Hands your hook the tools the model asks for."""
        # ONE budget for the whole batch, not one per tool: with a per-call
        # cap, five tools multiply the worst case by five, and meanwhile this
        # loop reads nothing from the provider socket (nor the bot's audio,
        # which in Deepgram arrives through the same place).
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._tool_timeout_s

        for call in event.get("functions", []) or []:
            name = call.get("name", "")
            call_id = str(call.get("id") or name)
            arguments = _shared.parse_arguments(call.get("arguments"))

            result = await _shared.run_tool(
                self._on_function_call, name, arguments,
                session=self.session, tool_call_id=call_id,
                timeout_s=max(deadline - loop.time(), 0.0))

            try:
                await self.ws.send(json.dumps({
                    "type": "FunctionCallResponse",
                    "id": call_id,
                    "name": name,
                    "content": json.dumps(result, ensure_ascii=False),
                }))
            except Exception:
                log.exception("Could not answer tool %s", name)

    def _build_filler(self) -> _shared.SilenceFiller:
        """The silence filler for a quiet channel (`_shared.SilenceFiller`).

        Deepgram speaks the channel's codec, so the filler frame is that
        codec's silence and goes to the socket as is. Only `send_audio`
        reports frames to it: a filler frame must not reset the gap clock
        (`test_the_filler_does_not_reset_the_gap_clock`).
        """
        self.filler = _shared.SilenceFiller(
            self._send_filler,
            silence_byte=SILENCE[resolve_codec(self.media)],
            frame_size=self.media.optimal_frame_size,
            ptime_ms=self.media.ptime,
        )
        return self.filler

    async def _send_filler(self, frame: bytes) -> None:
        await self.ws.send(frame)
