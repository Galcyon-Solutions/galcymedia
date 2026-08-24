"""
What every adapter does the same way: connect, read until the call ends,
resolve the codec, close, and the notices every provider shares.

What moves up here and what stays in each adapter, and the silence-clock
failure that set the rule: `docs/decisions.md`. Internal: nothing here is
public surface of the library. An outside adapter may use it, with no
stability promise.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

import websockets

from .. import events

log = logging.getLogger(__name__)

# How long the integrator's tool hook gets before it is cut off. Generous on
# purpose: a slow CRM lookup is legitimate, and the cap is not there to hurry
# it but to keep one that will NOT come back from taking the call with it.
# While the hook runs the adapter's reader is blocked: no barge-in and no
# bot audio get processed. It does NOT cost the socket: the session keeps
# sending the caller's audio from its own task, and measured against
# ElevenLabs that audio resets its 60 s cut (100 s with 60 pings
# unanswered, session open). The cut only lands when nothing goes out
# (`elevenlabs.PROVIDER_IDLE_CUT_S`), and this stays at half of it as a
# floor for that case (`test_the_tool_timeout_stays_under_the_elevenlabs_cut`).
TOOL_TIMEOUT_S = 20.0

# How long the channel can go without a caller frame before the filler
# starts. Under load the widest gap between real frames measured in the lab
# was 21 ms, so 300 ms never fires on jitter. It does not need to be exact
# because a false positive is cheap: a SIP leg losing a burst of packets can
# cross 300 ms without any VAD, and all that happens is a few frames of
# silence followed by the voice. Missing a real gap costs more: the providers
# only advance with incoming audio (see `SilenceFiller`).
GAP_BEFORE_FILL_S = 0.3


class SilenceFiller:
    """Feeds silence at real time while the channel delivers nothing.

    The channel writes nothing when the caller's leg delivers no frames: it
    drops comfort noise (`chan_websocket.c:1234`), writes nothing with media
    direction `out` (`:1230`), and never synthesizes silence (measured in
    the lab: 0 frames/s during a `Wait()`, 50 frames/s during a
    `Playback()`). A phone with silence suppression, a hold, or a dialplan
    application on the caller's side all look like that, and the providers'
    whole pipeline (transcript, reply, voice) only advances with incoming
    audio: after a phrase, a keep-alive message alone never closes the turn,
    and a gap of N seconds delays the reply by N (measured on Deepgram and
    ElevenLabs; the tables are in `docs/decisions.md`). So the filler is one
    silence frame per `ptime`, like a phone without VAD, from
    GAP_BEFORE_FILL_S after the last caller frame until the next one.

    The adapter reports every real frame with `note_caller_frame` and hands
    in a `send` that puts one frame on the wire the way its real audio goes
    (transcoded, wrapped) WITHOUT reporting it back here: a filler frame
    that reset the clock would switch the filler off and on every gap
    (`test_the_filler_does_not_reset_the_gap_clock` in each adapter). The
    last real frame is checked before every filler frame, so the overlap is
    at most one frame (`test_the_filler_stops_on_the_first_real_frame`).

    Args:
        send: Coroutine function that sends one frame to the provider.
        silence_byte: The silence of the codec `send` expects, usually the
            channel's. Zero is not silence in G.711: it injects a click.
        frame_size: Bytes per frame; `optimal_frame_size` of the channel.
        ptime_ms: Milliseconds per frame; `ptime` of the channel.
        clock: Monotonic clock; injectable so pacing is tested without a
            real sleep (on Windows `asyncio.sleep(0.02)` lasts ~31 ms).
        sleep: Coroutine function that waits; injectable for the same reason.
    """

    def __init__(self, send: Callable[[bytes], Awaitable[None]], *,
                 silence_byte: int, frame_size: int, ptime_ms: int,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 ) -> None:
        self._send = send
        self._frame = bytes([silence_byte]) * (frame_size or 160)
        self._frame_s = ptime_ms / 1000
        self._clock = clock
        self._sleep = sleep
        self._last_caller_frame = self._clock()
        self._stopped = False

    def note_caller_frame(self) -> None:
        """A real frame went to the provider: the gap starts over."""
        self._last_caller_frame = self._clock()

    def stop(self) -> None:
        """Ends `run` at its next check."""
        self._stopped = True

    async def run(self) -> None:
        """The loop: wait a gap, then fill until a real frame shows up.

        Returns when `stop` is called or when `send` fails: the connection
        is gone and the adapter's reader hangs up the call
        (`test_the_filler_ends_cleanly_if_the_socket_is_gone`).
        """
        while not self._stopped:
            await self._sleep(GAP_BEFORE_FILL_S)
            # Absolute schedule: `send` plus `sleep(frame_s)` drifts slow by
            # the cost of each send (`test_the_filler_pacing_is_absolute`).
            due = self._clock()
            while (not self._stopped
                   and self._clock() - self._last_caller_frame >= GAP_BEFORE_FILL_S):
                try:
                    await self._send(self._frame)
                except Exception:
                    log.debug("The filler did not go out", exc_info=True)
                    return
                due += self._frame_s
                await self._sleep(max(due - self._clock(), 0.0))

# Audio format names as chan_websocket writes them in MEDIA_START
# (`ast_format_get_name`, `chan_websocket.c:236`, returns `format->name`,
# `format.c:336`): the audio `.name` entries of `codec_builtin.c` (g723 :98,
# codec2 :122, ulaw :168, alaw :183, gsm :208, g726 :233, g726aal2 :248,
# adpcm :263, slin :288, lpc10 :442, g729 :466, speex :596, ilbc :655,
# g722 :669, siren7 :694, siren14 :718, g719 :742, opus :771, silk :881)
# plus the rate-suffixed names of `format_cache.c:397-407`. Only these can
# arrive on the wire, so a CODECS key outside this set is dead: a test walks
# every adapter's table against it
# (`test_every_adapter_codec_key_is_a_name_asterisk_writes`).
ASTERISK_AUDIO_FORMATS = frozenset({
    "g723", "codec2", "ulaw", "alaw", "gsm", "g726", "g726aal2", "adpcm",
    "slin", "slin12", "slin16", "slin24", "slin32", "slin44", "slin48",
    "slin96", "slin192", "lpc10", "g729", "speex", "speex16", "speex32",
    "ilbc", "g722", "siren7", "siren14", "g719", "opus", "silk8", "silk12",
    "silk16", "silk24",
})


def resolve_codec(media: Any, codecs: dict[str, str], provider: str) -> str:
    """Translates the channel format into the provider's codec name.

    Raises instead of falling back: a codec the provider rejects kills the
    call later, with an error of theirs that says nothing about the `Dial`
    that caused it (`test_an_unknown_codec_fails_naming_the_provider`).

    Args:
        media: The MEDIA_START of the call.
        codecs: Channel format to provider codec name.
        provider: How the provider is named in the error.

    Raises:
        RuntimeError: The channel format is not in `codecs`.
    """
    fmt = (media.audio_format or "").lower()
    codec = codecs.get(fmt)
    if codec is None:
        # The hint is built from `codecs`, never hardcoded: a fixed "c(ulaw)
        # or c(alaw)" contradicts the list for a provider that speaks
        # something else. The keys are trusted to be names Asterisk writes:
        # `test_every_adapter_codec_key_is_a_name_asterisk_writes` guards it
        # (`test_an_unknown_codec_fails_naming_the_provider`).
        accepted = ", ".join(f"c({name})" for name in sorted(set(codecs)))
        raise RuntimeError(
            f"The channel delivered codec {fmt!r} and {provider} only speaks "
            f"{', '.join(sorted(set(codecs)))}. Change the Dial to one of "
            f"{accepted}."
        )
    return codec


async def connect(url: str, headers: dict[str, str], provider: str,
                  auth_hint: str) -> Any:
    """Opens the provider's WebSocket, translating the usual failures.

    A rejected key and a network failure are investigated differently, and a
    401 is not fixed by retrying, so they are told apart instead of letting
    the library's raw error through.

    Args:
        url: Where to connect.
        headers: The authentication headers.
        provider: How the provider is named in the errors.
        auth_hint: What to do when the credential is rejected.

    Raises:
        RuntimeError: Rejected credential, another HTTP status, or no route.
    """
    try:
        # max_size=None: a provider sends whole audio blocks (an utterance of
        # TTS in one message) well past the 1 MiB default, and a dropped
        # block is a silent bot. ping_interval=20 is the WebSocket-level
        # keepalive of `websockets`, answered by the library itself even while
        # the adapter's reader is blocked; not ElevenLabs' JSON `ping`, which
        # the adapter answers (`ElevenLabsProvider._pong`).
        return await websockets.connect(
            url, additional_headers=headers, max_size=None, ping_interval=20,
        )
    except websockets.InvalidStatus as exc:
        if exc.response.status_code in (401, 403):
            raise RuntimeError(f"{provider} rejected the credential. "
                               f"{auth_hint}") from exc
        raise RuntimeError(
            f"{provider} answered {exc.response.status_code} on connect"
        ) from exc
    except OSError as exc:
        raise RuntimeError(
            f"Could not reach {provider}: {exc}. Check this machine's "
            f"internet access."
        ) from exc


async def read_until_closed(
    websocket: Any,
    on_text: Callable[[str], Awaitable[None]],
    on_binary: Callable[[bytes], Awaitable[None]] | None = None,
    *,
    on_gone: Callable[[], Awaitable[None]] | None = None,
) -> None:
    """Consumes what the provider sends until the call ends.

    Args:
        websocket: The provider connection.
        on_text: Called with every text message.
        on_binary: Only for providers that send the bot's voice as binary
            frames; the ones that send base64 inside an event leave it out,
            and binaries are ignored.
        on_gone: Runs when the loop ends without anyone closing on purpose.
            The three adapters hang up there: a provider that drops mid-call
            cannot leave the line open and mute
            (`test_provider_dropping_mid_call_hangs_up`).
    """
    try:
        async for message in websocket:
            if isinstance(message, str):
                await on_text(message)
            elif on_binary is not None:
                await on_binary(message)
    except asyncio.CancelledError:
        raise
    except websockets.ConnectionClosed:
        log.info("The provider closed the connection")
    except Exception:
        log.exception("The provider read loop failed")
    finally:
        if on_gone is not None:
            await on_gone()


async def close_quietly(websocket: Any, tasks: Iterable[Any]) -> None:
    """Cancels the adapter's tasks and closes the socket, without complaint.

    Cancel AND await, not just cancel: otherwise the tasks keep unwinding
    after the call is considered closed
    (`test_shutdown_cancels_and_awaits_the_tasks`).

    The ORDER matters: tasks first, socket after. The other way around the
    reader wakes with the connection closed and runs its `on_gone`, which
    hangs up in the three adapters, so closing the socket first would hang up
    during the close. What covers that today is the caller setting `_closed`
    before entering (the `_closed` guard tests of each adapter).

    Called from INSIDE one of `tasks` (an adapter closing from its own
    reader), that task is skipped: cancelling yourself and then gathering
    yourself never returns, and the socket never closes
    (`test_shutdown_from_inside_a_listed_task_does_not_deadlock`).
    """
    me = asyncio.current_task()
    pending = [task for task in tasks if task is not None and task is not me]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    if websocket is not None:
        try:
            await websocket.close()
        except Exception:
            log.debug("Closing the provider connection failed", exc_info=True)


def parse_json(raw: str) -> dict[str, Any] | None:
    """Reads a provider message, or None when it is not a JSON object.

    An unreadable message cannot kill the call: logged and skipped
    (`test_an_unreadable_message_does_not_blow_up`).
    """
    try:
        message = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        log.debug("Provider message that is not JSON: %r", str(raw)[:120])
        return None
    return message if isinstance(message, dict) else None


def parse_arguments(raw: Any) -> dict[str, Any]:
    """Reads a tool's arguments, tolerating garbage.

    A model sends them, so they may be malformed. Degrades to an empty dict
    instead of blowing up: the tool decides what to do without arguments,
    which beats losing the call (`test_tool_arguments_tolerate_garbage`).
    """
    if isinstance(raw, dict):
        return raw
    try:
        arguments = json.loads(raw or "{}")
    except (json.JSONDecodeError, TypeError, ValueError):
        log.warning("Unreadable tool arguments: %r", str(raw)[:120])
        return {}
    return arguments if isinstance(arguments, dict) else {}


async def run_tool(handler: Any, name: str, arguments: dict[str, Any],
                   *, session: Any = None, tool_call_id: str = "",
                   timeout_s: float = TOOL_TIMEOUT_S) -> Any:
    """Hands the user's hook a tool the model asked for, and returns its answer.

    The adapter does not know what any tool does: it delivers and returns
    whatever the user decides. The model has to get an answer ALWAYS, or it
    waits and the conversation stops dead: a failure and a timeout both come
    back as an error answer (`test_a_tool_that_blows_up_returns_an_error`,
    `test_a_tool_that_never_returns_is_abandoned`).

    Args:
        handler: The `on_function_call` hook, or None to reject the tool.
        name: The tool.
        arguments: What the model passed.
        session: When given, publishes the pair of events that frames the
            tool; the opening one lets a panel paint "checking the
            calendar..." and without the closing one that wait never ends
            (`test_a_tool_that_times_out_still_closes_its_event_pair`).
        tool_call_id: Pairs the two events; a model can ask for several at
            once. Falls back to `name`.
        timeout_s: Cap on the hook. This runs INSIDE the loop that reads
            the provider's socket, so while the hook does not return the
            adapter processes nothing: see `TOOL_TIMEOUT_S`.
    """
    if session is not None:
        session.emit(events.function_call_started(
            name, tool_call_id=tool_call_id or name, arguments=arguments))

    if handler is None:
        log.warning(
            "The model asked for tool %r and there is no on_function_call: "
            "rejected.", name)
        result: Any = {"ok": False, "error": "no handler"}
    else:
        try:
            result = await asyncio.wait_for(handler(name, arguments),
                                            timeout=timeout_s)
        except asyncio.TimeoutError:
            log.error(
                "Tool %s did not answer within %gs and is abandoned. The "
                "model gets an error so the conversation goes on; the hook "
                "call is cancelled.", name, timeout_s)
            result = {"ok": False, "error": f"no answer within {timeout_s:g}s"}
        except Exception as exc:
            log.exception("Tool %s failed", name)
            result = {"ok": False, "error": str(exc)}

    if session is not None:
        session.emit(events.function_call_stopped(
            name, tool_call_id=tool_call_id or name, result=result))
    return result


class ProviderEvents:
    """The peephole into the provider: lets you watch what arrives, unchanged.

    The twin of `on_event`, which does the same with the Asterisk channel,
    under the same rule: called with EVERYTHING, translated or not, and IN
    ADDITION to the internal handling, never instead of it. Receives `(kind,
    payload)` with the FULL content: an event you only know the name of is
    of no use. A failing hook cannot bring the call down
    (`test_a_broken_peephole_does_not_bring_down_the_call`).

    Args:
        hook: The integrator's `on_provider_event`, or None.
        provider: How the provider is named in the log.
    """

    def __init__(self, hook: Any, provider: str) -> None:
        self._hook = hook
        self._provider = provider
        self._seen: set[str] = set()

    def __call__(self, kind: str, payload: Any) -> None:
        if self._hook is None:
            return
        try:
            self._hook(kind, payload)
        except Exception:
            log.exception("The %s event hook failed", self._provider)

    def note_unhandled(self, kind: str) -> None:
        """Records an event type the adapter does not translate, ONCE.

        Not an error, so it goes to debug: voice APIs add events all the
        time, and an adapter that only reads the ones it needs is right. Once
        per TYPE, not per event, so a periodic one (a latency report) does
        not bury the new one that matters
        (`test_an_untranslated_type_is_logged_once_per_call`). The hook still
        gets every event; this only decides how often the log repeats.
        """
        if kind in self._seen:
            return
        self._seen.add(kind)
        log.debug("[%s] untranslated event: %s", self._provider, kind)


def report_caller_speaking(session: Any) -> None:
    """The provider detected that the caller started speaking.

    Separate from the barge-in on purpose: the caller speaking does not
    always cut the bot (nothing to cut while it is quiet), but the client
    has to know either way, since it is half of the speaking turn drawn on
    screen.
    """
    session.emit(events.user_started_speaking())


def report_provider_error(session: Any, detail: Any) -> None:
    """The provider reported an error of its own.

    Not fatal: the call goes on and the provider decides whether it
    recovers. It has to leave the log, because a client that only sees the
    conversation cannot explain why the bot went quiet
    (`test_a_provider_error_reaches_the_client_and_not_just_the_log`).
    """
    log.error("The provider reported an error: %s", detail)
    session.emit(events.error(str(detail), fatal=False))


class TranscriptReporter:
    """Publishes transcripts and stops the silence clock.

    Both halves go together on purpose, and the second is the one that gets
    done wrong: only the CALLER stops the clock, since if the bot did too,
    every utterance of its own would restart it and an abandoned line would
    never be detected (`test_only_the_person_stops_the_silence_clock`). An
    empty text does NEITHER: no event, no clock.

    Args:
        session: The call. The SESSION is kept, not its speaking turn, and
            `session.speech` is read on every report: whoever swaps the
            turn (the integrator, by assigning `session.speech`, which
            `Session` documents) is followed from that point on. Holding a
            reference taken at construction would leave the clock stopped
            for the rest of the call
            (`test_the_clock_follows_the_current_speech_turn`).
        on_transcript: The integrator's hook, `(role, text, final)`. A
            failure of its own does not bring the call down.
    """

    def __init__(self, session: Any, on_transcript: Any = None) -> None:
        self._session = session
        self._on_transcript = on_transcript

    def __call__(self, role: str, text: str, final: bool = True) -> None:
        if not text:
            return
        if final:
            log.info("[%s] %s", role, text)
        if role == "user":
            self._session.speech.note_caller_activity()

        self._session.emit(self._as_event(role, text, final))

        if self._on_transcript is None:
            return
        try:
            self._on_transcript(role, text, final)
        except Exception:
            log.exception("The transcript hook failed")

    @staticmethod
    def _as_event(role: str, text: str, final: bool) -> Any:
        """Maps (role, text, final) to its RTVI event.

        The caller and the bot do NOT share an event: the caller's carries
        `final` inside, since a partial gets corrected as the person keeps
        talking; the bot's splits into two types, the fragment as spoken and
        the closed text of the turn
        (`test_the_bot_text_splits_into_the_two_types_the_standard_has`).
        """
        if role == "user":
            return events.user_transcription(text, final=final)
        if final:
            return events.bot_output(text)
        return events.bot_tts_text(text)
