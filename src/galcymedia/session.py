"""
One call, one Session.

A Session owns the WebSocket Asterisk opened for one call. It speaks the
channel protocol, applies flow control, and hands the audio to the provider
the dialplan chose. It knows nothing about a provider beyond the small
interface of `provider.py`.

It is the big class of the package on purpose: everything about one call
orbits one state that crosses the input/output boundary, so splitting it
would cut every mechanism in half. The parts:

    - Outgoing commands (answer, hangup, flush, mark, pause, ...): wrap the
      channel protocol. The protocol details are in `docs/protocol.md`.
    - Reader loop (run, _on_audio, _on_control, _on_media_start): reads the
      socket and dispatches. It NEVER awaits anything of the provider that
      may hang; the audio goes through a queue (`_audio_in`) and the provider
      startup through its own task (`_start_provider`).
    - Close (finish, _cleanup): orderly shutdown, which always has to
      complete even if the provider hangs.

The why of the non-obvious decisions lives in `docs/decisions.md`; the
channel's traps, in `docs/protocol.md`. Only what bites while reading the
code stays here. Every `chan_websocket.c` line below is Asterisk 23.4.1,
the release the README requires.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import websockets

from . import events
from .protocol import (
    Command,
    Event,
    MediaStart,
    build_command,
    parse_event,
    parse_media_start,
)
from .provider import VoiceProvider
from .speech import SpeechState

log = logging.getLogger(__name__)

# Valid DTMF digits: the keypad plus A-D (rare, but part of the standard). A
# DTMF_END with anything else is garbage from the channel.
_DTMF_VALID = frozenset("0123456789*#ABCD")


def _dtmf_digit(value: Any) -> str | None:
    """Returns the DTMF digit if it is valid, or None if it is not.

    The value comes from the DTMF_END event and is not under our control: a
    channel with a bug or a hostile frame can bring a dict, a thousand
    characters, or control bytes. The library is the boundary: the adapter
    only gets a real digit.

    Args:
        value: Whatever came in the `digit` field.

    Returns:
        The single character, or None.
    """
    if not isinstance(value, str) or len(value) != 1:
        return None
    return value if value in _DTMF_VALID else None


class _AlreadyLogged(Exception):
    """A failure already logged with its cause where it happened.

    The main loop lets it through without dumping the same trace twice.
    Internal: it never leaves the library. Its one raiser is `_on_control`
    on a plain-text channel; `run()` catches it and its `finally` publishes
    the synthetic HANGUP and cleans up (`test_a_plain_text_channel_is_refused_once`).
    """


# Defaults of the session policies. Each one can be overridden per call with
# the kwarg of the same name on Session (and on serve()/connect(), which pass
# it through). These are OUR policies: the channel limits (128 B of control,
# queue 900/800) live in protocol.py and are not configurable.

# Cap on waiting for the XON before dropping the audio that does not fit. The
# Asterisk queue holds about 20 s (`protocol.QUEUE_LENGTH_MAX`): after 5
# without moving, the XON is not coming.
XOFF_MAX_WAIT_S = 5.0

# How long finish() waits for what was left in the queue to play out.
FINISH_MAX_WAIT_S = 5.0

# Cap on the provider connecting in start(). Generous: connecting to a
# speech engine can take a while.
START_MAX_WAIT_S = 15.0

# Cap on the provider closing in close(); past the margin it is abandoned so
# it does not block the server shutdown.
CLOSE_MAX_WAIT_S = 5.0

# Caller frames the input queue retains (at 20 ms, 50 = 1 s). When full the
# oldest is dropped, which in real time is the least useful one.
AUDIO_IN_MAX_FRAMES = 50


class Session:
    """Carries one call from MEDIA_START until it hangs up."""

    def __init__(
        self,
        websocket: Any,
        provider_factory: Any,
        emit: Any = None,
        on_event: Any = None,
        *,
        transfers: Any = None,
        forward_dtmf: bool = False,
        xoff_max_wait_s: float = XOFF_MAX_WAIT_S,
        finish_max_wait_s: float = FINISH_MAX_WAIT_S,
        provider_start_timeout_s: float = START_MAX_WAIT_S,
        provider_close_timeout_s: float = CLOSE_MAX_WAIT_S,
        audio_in_max_frames: int = AUDIO_IN_MAX_FRAMES,
    ) -> None:
        """Prepares the call. Nothing happens until `run()`.

        Args:
            websocket: The connection Asterisk opened for this call.
            provider_factory: Callable of ONE argument, the session, that
                returns the provider (`docs/decisions.md`).
            emit: Destination of the RTVI events, optional.
            on_event: The peephole on the raw channel protocol, optional.
            transfers: The `Transfers` registry, if `serve()` injected one.
            forward_dtmf: Whether keypad digits reach the provider. Off by
                default; the why is where it applies (`_on_control`).
            xoff_max_wait_s: See `XOFF_MAX_WAIT_S`.
            finish_max_wait_s: See `FINISH_MAX_WAIT_S`.
            provider_start_timeout_s: See `START_MAX_WAIT_S`.
            provider_close_timeout_s: See `CLOSE_MAX_WAIT_S`.
            audio_in_max_frames: See `AUDIO_IN_MAX_FRAMES`. Must be >= 1.

        Raises:
            ValueError: `audio_in_max_frames` under 1. With `maxsize <= 0`
                an `asyncio.Queue` becomes UNBOUNDED: the value that looks
                like "retain nothing" is the opposite, memory without a cap
                under a stuck provider (`test_audio_in_max_frames_invalid_raises`).
        """
        self._ws = websocket
        self._provider_factory = provider_factory

        # The integrator's two hooks, both optional and both for WATCHING:
        # the peephole on the raw channel protocol, and the destination of
        # the RTVI events. Neither changes what the library does, so neither
        # can bring the call down: a failure of theirs is logged and the
        # call goes on.
        self._on_event = on_event
        self._emit = emit

        # The transfers registry, if serve() injected it. Used by
        # request_escalation() to record only; without it, the escalation
        # still lands in `session.escalation` for a custom agi_handler.
        self._transfers = transfers

        self._forward_dtmf = forward_dtmf

        # Policies of this session (the defaults are the constants above).
        self._xoff_max_wait_s = xoff_max_wait_s
        self._finish_max_wait_s = finish_max_wait_s
        self._provider_start_timeout_s = provider_start_timeout_s
        self._provider_close_timeout_s = provider_close_timeout_s

        self._provider: VoiceProvider | None = None
        self._media: MediaStart | None = None

        # The channel's id, kept from the FIRST event that carries one: every
        # event does (`chan_websocket.c:204-362`), so a call that dies before
        # MEDIA_START still hangs up with its id
        # (`test_the_synthetic_hangup_carries_the_channel_id_before_media_start`).
        self._channel_id = ""

        # Passthrough the MEDIA_START cannot announce: the p() option of the
        # Dial marks the channel (`chan_websocket.c:1633`) without zeroing
        # optimal_frame_size (only `minimum_bytes <= 10` does, `:1406-1408`),
        # and the event carries no field that says so (`:231-239`). The
        # first ERROR "not supported in passthrough mode" tells (`:681`).
        # Pinned by `test_a_p_option_dial_is_detected_from_the_channel_error`.
        self._passthrough_forced = False

        # Set once the provider finished starting; until then the caller's
        # audio is dropped.
        self._provider_ready = asyncio.Event()

        # XOFF/XON flow control. Event and not Lock on purpose: see
        # `docs/decisions.md`.
        self._can_send = asyncio.Event()
        self._can_send.set()

        self._mark_counter = 0
        self._discarded_frames = 0        # dropped by a sustained XOFF

        # One Event per report_when_drained() in flight, so two do not step
        # on each other's signal (`docs/decisions.md`).
        self._drain_waiters: list[asyncio.Event] = []

        self._tasks: set[asyncio.Task] = set()   # tasks outside the reader loop

        # Caller audio on its way to the provider, through a queue apart from
        # the reader loop. When full the oldest frame is dropped;
        # _audio_dropped counts those (apart from the ones the XOFF drops).
        if audio_in_max_frames < 1:
            raise ValueError(
                f"audio_in_max_frames has to be >= 1, got "
                f"{audio_in_max_frames}. With 0 the asyncio queue is "
                f"unbounded, the opposite of a cap."
            )
        self._audio_in: asyncio.Queue[bytes] = asyncio.Queue(
            maxsize=audio_in_max_frames)
        self._audio_dropped = 0
        # Frames that arrived before the provider was ready. Another cause
        # and another reading than the full queue, so another counter.
        self._audio_before_ready = 0

        self._finishing = False           # idempotent finish() guard
        self._pending_marks: dict[str, asyncio.Event] = {}
        self._closed = asyncio.Event()
        self._escalate_to: str | None = None
        self._speech: SpeechState | None = None   # lazy: see property speech

    # ------------------------------------------------------------------
    # Events toward whoever is watching
    # ------------------------------------------------------------------

    def emit(self, event: Any) -> None:
        """Publishes an RTVI event of this call.

        Synchronous on purpose, so it can be called from anywhere. A failure
        here NEVER propagates: instrumentation accompanies the call, it does
        not command it.

        Args:
            event: The RTVI event (`events.py`).
        """
        if self._emit is None:
            return
        try:
            self._emit(event)
        except Exception:
            log.debug("An event could not be published", exc_info=True)

    async def _send_command(self, command: Command, **params: Any) -> None:
        try:
            await self._ws.send(build_command(command, **params))
        except ValueError as exc:
            # Over the 128 bytes. This is never swallowed in silence.
            log.error("A command over the limit is not sent: %s", exc)
            raise
        except Exception:
            # The socket is gone: the normal race when the caller hangs up
            # while the bot is still saying goodbye. Not an error.
            log.debug("Command %s dropped: the connection already closed",
                      command.value, exc_info=True)

    def accept_audio(self) -> None:
        """Opens the gate to the caller's audio. Call it BEFORE `answer()`.

        Answering is what makes Asterisk start sending audio, so between the
        ANSWER and this call there is a window where the person already
        hears the open line, speaks, and what they say is dropped. It does
        not look like a failure: it looks like a bot answering nonsense,
        because the provider gets the utterance already half started, its
        voice detector cuts where it should not and the model fills in the
        rest (`test_the_audio_gate_opens_before_answering`).

        Idempotent, and the Session calls it on its own when the provider
        startup ENDS WELL, so an adapter that does not use it keeps working;
        it only pays for the window. If the startup fails it is not called,
        which is right: that call is already being hung up.
        """
        if self._provider_ready.is_set():
            return
        # The queue starts draining BEFORE opening the gate: the other way
        # round, the first frame would come in with nobody to pick it up.
        self._spawn(self._drain_audio(), "drain_audio")
        self._provider_ready.set()

    async def answer(self) -> None:
        """Answers the call: Asterisk starts sending the caller's audio."""
        await self._send_command(Command.ANSWER)

    async def hangup(self) -> None:
        """Hangs up the bot's leg. `finish()` waits for the audio first."""
        await self._send_command(Command.HANGUP)

    # ------------------------------------------------------------------
    # Buffering on the Asterisk side
    # ------------------------------------------------------------------

    async def start_buffering(self) -> None:
        """Lets Asterisk assemble the frames instead of you.

        Frame alignment, but on the Asterisk side. Close it with
        `stop_buffering()`, or the last remainder never plays
        (`docs/protocol.md`).
        """
        await self._send_command(Command.START_MEDIA_BUFFERING)

    def _register_mark(self, prefix: str) -> tuple[str, asyncio.Event]:
        """Registers a correlated wait and returns (id, Event).

        Marks and the end of buffering use the same mechanism: a unique id
        that travels in the command and comes back in the ack, and an Event
        set when that ack arrives. `flush()` and `_cleanup()` release these
        waits.
        """
        self._mark_counter += 1
        correlation_id = f"{prefix}{self._mark_counter}"
        waiter = asyncio.Event()
        self._pending_marks[correlation_id] = waiter
        return correlation_id, waiter

    def _resolve_mark(self, payload: dict[str, Any]) -> None:
        """Sets the wait of the mark (or the buffering) Asterisk confirmed.

        MEDIA_MARK_PROCESSED and MEDIA_BUFFERING_COMPLETED share the
        correlation registry, so both resolve the same way.
        """
        correlation_id = str(payload.get("correlation_id", ""))
        waiter = self._pending_marks.pop(correlation_id, None)
        if waiter is not None:
            waiter.set()

    async def stop_buffering(self) -> asyncio.Event:
        """Closes the buffering and sends what was left pending.

        Returns:
            An Event set on MEDIA_BUFFERING_COMPLETED. A `flush()` in between
            gives it up for lost (`docs/protocol.md`).
        """
        correlation_id, waiter = self._register_mark("b")
        await self._send_command(Command.STOP_MEDIA_BUFFERING,
                                 correlation_id=correlation_id)
        return waiter

    # ------------------------------------------------------------------
    # Queue state and pacing
    # ------------------------------------------------------------------

    async def request_status(self) -> None:
        """Asks for the state of the Asterisk audio queue.

        The answer arrives as a STATUS event and is read from the `on_event`
        hook.
        """
        await self._send_command(Command.GET_STATUS)

    async def report_when_drained(self) -> asyncio.Event:
        """Asks for a notice when the audio queue runs empty.

        Unlike a mark, it survives a `flush()`: that is why `finish()` uses
        it to wait for the end of the audio even with a barge-in in between
        (`docs/protocol.md`).

        In passthrough the channel REJECTS this command, and this function
        does not guard against it: it sends it anyway and Asterisk answers
        an ERROR. Checking is the caller's job (`finish()` and
        `report_bot_audio_drained()` already do).

        Returns:
            An Event set on QUEUE_DRAINED.
        """
        waiter = asyncio.Event()
        self._drain_waiters.append(waiter)
        await self._send_command(Command.REPORT_QUEUE_DRAINED)
        return waiter

    async def report_bot_audio_drained(self) -> None:
        """Asks Asterisk for the queue-drained notice of the bot's turn.

        Used by `SpeechState.end_turn`: the provider finished generating,
        but its audio is still queued. When QUEUE_DRAINED arrives,
        `_on_control` tells the SpeechState (`on_queue_drained`) that the
        bot really stopped sounding. No correlation (the event carries
        none): any QUEUE_DRAINED lowers `_audio_pending`, which is exactly
        what it means.

        In passthrough the channel rejects the command, so the notice is
        given HERE, at once: `on_queue_drained` runs inside `end_turn` while
        Asterisk is still playing the queue. It means "the provider finished
        generating", NOT "it finished sounding": `played_ms_at_most` goes to
        None before the audio ends. It does not matter there, because in
        that mode there is no barge-in and no truncate worth the name; what
        it buys is `bot_audio_playing` coming down instead of staying True
        for the rest of the call (`test_passthrough_closes_the_bot_turn_without_the_notice`;
        `docs/decisions.md`).
        """
        if self._passthrough_rejects_drain():
            if self._speech is not None:
                self._speech.on_queue_drained()
            return
        await self._send_command(Command.REPORT_QUEUE_DRAINED)

    def _passthrough_rejects_drain(self) -> bool:
        # In passthrough the channel rejects REPORT_QUEUE_DRAINED (8 of the
        # 11 commands, `chan_websocket.c:740-846`): asking only produces an
        # ERROR. There is no barge-in there anyway, so it is skipped quietly.
        return self._in_passthrough()

    def _in_passthrough(self) -> bool:
        # Both forms: the small-frame codec (optimal_frame_size == 0) and the
        # p() option of the Dial, only seen in the channel's first ERROR.
        return self._passthrough_forced or (
            self._media is not None and self._media.passthrough)

    def _on_channel_error(self, payload: dict[str, Any]) -> None:
        # The channel rejects 8 of the 11 commands in passthrough with this
        # exact text (`chan_websocket.c:681`, "%s not supported in
        # passthrough mode"). With p() in the Dial it is the ONLY signal that
        # arrives: warn once, with the cause, and the following rejections
        # drop to DEBUG because they say nothing new.
        text = str(payload.get("error_text", ""))
        if "not supported in passthrough mode" not in text:
            log.error("Asterisk reported an error: %s", payload)
            return
        if self._passthrough_forced:
            log.debug("Command rejected in passthrough: %s", text)
            return
        self._passthrough_forced = True
        audio_format = self._media.audio_format if self._media else "?"
        log.warning(
            "The channel is in PASSTHROUGH through the p() option of the Dial "
            "(format %r): barge-in, marks and the clean hang-up do not work. "
            "Remove the p(): Dial(WebSocket/<connection>/c(%s)f(json)). "
            "Asterisk rejected: %s",
            audio_format, audio_format, text,
        )
        self.emit(events.error(
            f"p() option in the Dial (format {audio_format!r}): no barge-in "
            f"and no clean hang-up. Remove the p() and keep c({audio_format})f(json).",
            fatal=False,
        ))

    async def pause(self) -> None:
        """Stops playback without dropping what is queued.

        CAREFUL: with the pause active the channel emits no MEDIA_XON. If
        you pause with an XOFF in progress, the audio does not resume until
        `continue_media()` (`docs/protocol.md`).
        """
        # The speaking turn has to know: it measures what was heard with a
        # clock, and the pause stops the audio without stopping time. From
        # here it stops knowing and says so, instead of returning a number
        # too high (`test_the_pause_tells_the_speaking_turn_the_clock_broke`).
        if self._speech is not None:
            self._speech.note_playback_paused()
        await self._send_command(Command.PAUSE_MEDIA)

    async def continue_media(self) -> None:
        """Resumes playback after `pause()`."""
        await self._send_command(Command.CONTINUE_MEDIA)

    async def set_media_direction(self, direction: str) -> None:
        """Changes which half of the audio stays alive.

            "both"   the normal case: listening and speaking
            "in"     listen only. Asterisk stops accepting your audio
            "out"    speak only. It stops sending you the caller's audio

        CAREFUL with "in": it closes the channel timer, and with it goes the
        MEDIA_XON. An active XOFF is not lifted until back to "both"
        (`docs/protocol.md`).

        Args:
            direction: One of "both", "in", "out".

        Raises:
            ValueError: Any other value.
        """
        valid = ("both", "in", "out")
        if direction not in valid:
            raise ValueError(
                f"direction has to be one of {valid}, got {direction!r}"
            )
        await self._send_command(Command.SET_MEDIA_DIRECTION,
                                 direction=direction)

    async def flush(self) -> None:
        """Drops everything left in the queue.

        The barge-in piece: what the bot had queued is no longer useful.
        """
        await self._send_command(Command.FLUSH_MEDIA)

        # The flush destroys the marks in flight, so their ack never
        # arrives: whoever waits has to be released or hangs until their
        # timeout (`docs/protocol.md`;
        # `test_the_flush_releases_the_pending_marks`).
        for pending in self._pending_marks.values():
            pending.set()
        self._pending_marks.clear()

        self.emit(events.bot_interrupted())

    async def mark(self) -> asyncio.Event:
        """Puts a marker in the audio queue.

        The right way to wait for the bot to finish speaking; sleeping an
        estimated duration cuts the goodbye.

        Returns:
            An Event set when everything queued before the marker played.
        """
        correlation_id, waiter = self._register_mark("m")
        await self._send_command(Command.MARK_MEDIA, correlation_id=correlation_id)
        return waiter

    async def send_audio(self, chunk: bytes) -> bool:
        """Sends audio to the caller, honoring flow control.

        Waits while the XOFF is active, with a cap: some commands stop the
        XON clock and without a cap that wait would block the task forever.
        Dropping the frame is right in real time: arriving late is worse
        than not arriving (`docs/decisions.md`).

        Args:
            chunk: Audio in the channel codec, any size.

        Returns:
            True if the frame LEFT, False if it was dropped (sustained XOFF,
            or the socket already closed). Whoever sends a whole block needs
            it for two things: to stop pushing frames that will each pay the
            same timeout, and not to count as queued audio that never played
            (`test_send_audio_says_whether_the_frame_actually_left`).
            Ignoring the return value keeps working as before.
        """
        try:
            await asyncio.wait_for(self._can_send.wait(),
                                   timeout=self._xoff_max_wait_s)
        except asyncio.TimeoutError:
            self._discarded_frames += 1
            if self._discarded_frames == 1:
                log.warning(
                    "XOFF active for more than %.0fs: dropping the audio that "
                    "does not fit. The Asterisk queue is not draining.",
                    self._xoff_max_wait_s,
                )
            return False

        try:
            await self._ws.send(chunk)
        except Exception:
            # Normal race: the caller hung up while the provider was still
            # generating speech. The main loop is already ending the session.
            log.debug("Audio frame dropped: the connection already closed",
                      exc_info=True)
            return False
        return True

    def note_audio_dropped(self, frames: int) -> None:
        """Adds frames given up for lost without being attempted.

        Called by the speaking turn when it cuts a whole block at the first
        rejection: those frames never go through `send_audio`, so they do
        not count themselves, and without them the closing summary would say
        "1 frame dropped" when a block of twenty-five was lost. That number
        is all that is left to know how much audio did not arrive.

        Args:
            frames: How many frames were not even attempted.
        """
        if frames > 0:
            self._discarded_frames += frames

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Reads the socket until the call ends.

        Whatever the exit, the `finally` publishes the synthetic HANGUP and
        cleans up, so the end reaches the hooks through the same path as
        everything else.
        """
        try:
            async for message in self._ws:
                if isinstance(message, bytes):
                    await self._on_audio(message)
                else:
                    await self._on_control(message)
        except _AlreadyLogged:
            pass
        except websockets.ConnectionClosed:
            # Abrupt close. The close code is told by _notify_hangup().
            log.debug("The connection closed without a goodbye", exc_info=True)
        except Exception:
            log.exception("The session loop failed")
        finally:
            self._notify_hangup()
            await self._cleanup()

    def _notify_hangup(self) -> None:
        """Publishes how the call ended, as a synthetic HANGUP event.

        The channel sends no hangup event: the only notice of the end is the
        WebSocket close code. Here it is translated into an event so the end
        arrives through the same path as the rest (`docs/decisions.md`).

        Contract with `server.py`, not a casual private call: when `on_call`
        fails before the loop starts, the server closes with 1011 and calls
        this so the peephole and the RTVI client still see the end
        (`test_a_failing_on_call_drops_the_call_with_1011_and_a_hangup`, red
        if the name moves).
        """
        code = getattr(self._ws, "close_code", None)
        reason = getattr(self._ws, "close_reason", "") or ""

        # 1000 and 1005 (closed without sending a code) are normal ends.
        normal_codes = (1000, 1005, None)

        payload = {
            "event": "HANGUP",
            # The channel puts channel_id in every event it sends
            # (`chan_websocket.c:204-362`); the synthetic one keeps the
            # parity so a shared hook can tell concurrent calls apart. Empty
            # only if no event ever arrived.
            "channel_id": self._channel_id,
            "code": code,
            "reason": reason,
            "normal": code in normal_codes,
        }

        if code in normal_codes:
            log.info("Call hung up normally (code %s)", code)
        else:
            # This branch covers ANY code other than 1000/1005/None, not just
            # 1001. The common one is 1001, which is not an error: it tells
            # "the bot finished" from "the person left", which in a dialplan
            # with transfers changes the destination. The others (1003,
            # 1006, 1011) are anomalous, and that is why all of them go out
            # with normal=False.
            log.info("Call hung up by the other side (code %s%s)",
                     code, f", {reason}" if reason else "")

        self._notify("HANGUP", payload)

        # And to the RTVI client too: the standard has no hangup type, so it
        # travels through the wildcard. Without this a panel waits forever
        # for a call that already ended, because the last thing it saw was
        # the bot going quiet.
        self.emit(events.server_message({"hangup": payload}))

    async def _on_audio(self, chunk: bytes) -> None:
        """Caller audio: queued; the delivery is `_drain_audio`'s.

        With the queue full it also DEQUEUES: drops the oldest frame to fit
        the new one. It never waits for the provider, which is the point
        (`docs/decisions.md`). Before the provider is ready the audio is
        dropped: it can arrive before MEDIA_START (`docs/protocol.md`, issue
        #1712) or while start() is still running.
        """
        if self._provider is None or not self._provider_ready.is_set():
            # Counted apart from the ones the full queue drops: these are
            # STARTUP (the provider is still connecting) and do not mean it
            # is slow. Without counting them, a call that starts cut off
            # leaves no trace of why.
            self._audio_before_ready += 1
            return
        try:
            self._audio_in.put_nowait(chunk)
        except asyncio.QueueFull:
            # The provider cannot keep up with real time. The oldest frame
            # goes and the new one comes in: in live audio the latest
            # matters more than the one already behind.
            try:
                self._audio_in.get_nowait()
                self._audio_in.put_nowait(chunk)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass
            self._audio_dropped += 1
            if self._audio_dropped == 1:
                log.warning(
                    "The provider cannot keep up with the incoming audio: "
                    "dropping frames. Its send_audio() takes too long.")

    async def _drain_audio(self) -> None:
        """Delivers the queued audio to the provider, one frame at a time.

        Runs as a task apart from the reader loop. If `send_audio` hangs,
        only this task gets stuck: the reader keeps reading the socket and
        can close the call. `_cleanup` cancels it at the end.
        """
        while True:
            chunk = await self._audio_in.get()
            provider = self._provider
            if provider is None:
                # Should not happen: the audio gate opens with the provider
                # already built. But `accept_audio()` is public, so an
                # adapter can call it too early, and there the
                # AttributeError would die inside this task unseen: the call
                # would stay open and mute.
                self._audio_in.task_done()
                continue
            try:
                await provider.send_audio(chunk)
            except Exception:
                # A broken send_audio cannot kill the delivery of the rest:
                # logged, and on to the next frame.
                log.exception("The provider's send_audio failed")
            finally:
                self._audio_in.task_done()

    def _spawn(self, coro: Any, name: str) -> None:
        """Runs something of the provider outside the socket reader loop.

        Keeps the reference: asyncio only holds a weak one and the collector
        can take a task mid-run ("Important: Save a reference to the result
        of this function", the `asyncio.create_task` docs). Failures are
        logged here, because a task nobody awaits swallows its exceptions.
        """
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)

        def on_done(t: asyncio.Task) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                log.error("%s failed", name, exc_info=t.exception())

        task.add_done_callback(on_done)

    def _notify(self, name: str, payload: dict[str, Any]) -> None:
        """Hands an event to the peephole, if one is set.

        Never propagates: a failure watching the call cannot bring the call
        down.
        """
        if self._on_event is None:
            return
        try:
            self._on_event(name, payload)
        except Exception:
            log.debug("The on_event hook failed on %s", name, exc_info=True)

    async def _on_control(self, raw: str) -> None:
        # Without f(json) in the Dial the channel sends plain text, its
        # default (`chan_websocket.c:2007`): the event name and then its
        # fields, "MEDIA_START connection_id:..." (`:248-257`). Recognized
        # because the first token is a member of `Event`; hostile garbage
        # ("not json", "ATTACK foo") is not and goes on to parse_event. In
        # that mode the channel does not understand our JSON commands
        # either: it takes the whole text as the command name, logs
        # "command unknown" on its side and answers NOTHING on the socket
        # (`:715-729`, `:903-904`). So it is said ONCE, by name, and the
        # call is cut. Pinned by `test_a_plain_text_channel_is_refused_once`.
        first_token = raw.split(None, 1)[0] if raw.strip() else ""
        if first_token in Event.__members__:
            log.error("The channel is in text mode: f(json) is missing in the "
                      "Dial. The call is closed.")
            raise _AlreadyLogged

        event, payload = parse_event(raw)

        if payload.get("channel_id"):
            self._channel_id = str(payload["channel_id"])

        # The peephole sees the event BEFORE the library processes it, and
        # sees the ones it does not recognize too: a new event of a future
        # Asterisk reaches the integrator on day one.
        self._notify(
            event.value if event is not None else str(payload.get("event", "?")),
            payload,
        )

        if event is None:
            return

        if event is Event.MEDIA_START:
            await self._on_media_start(payload)

        elif event is Event.MEDIA_XOFF:
            log.warning("XOFF: the Asterisk queue filled up, sending stops")
            self._can_send.clear()

        elif event is Event.MEDIA_XON:
            log.info("XON: the queue came down enough, sending resumes")
            self._can_send.set()

        elif event is Event.MEDIA_MARK_PROCESSED:
            self._resolve_mark(payload)

        elif event is Event.DTMF_END:
            # A real DTMF is one character of 0-9*#A-D. A dict, a thousand
            # characters or a format string are a channel with a bug or a
            # hostile frame, and cannot reach the adapter raw: the library is
            # the boundary (`docs/decisions.md`).
            digit = _dtmf_digit(payload.get("digit"))
            if digit is None:
                log.warning("Unreadable DTMF (%r), ignored",
                            payload.get("digit"))
            else:
                # To the client ALWAYS: the keypad is part of the call and
                # whoever runs it has to see it.
                self.emit(events.server_message({"dtmf": {"digit": digit}}))

                # To the provider, only if asked for. What people do NOT say
                # out loud travels over the keypad: the card, the ID, the
                # PIN. Forwarding it by default would put those digits into
                # a third party's records without anyone deciding it, and
                # the provider does not need them to converse. What each key
                # means is the agent's, and the agent knows without the
                # model seeing it (`test_the_keypad_does_not_reach_the_provider_unless_asked`).
                #
                # Only with the provider ready (like the audio), and in its
                # own task: on_dtmf runs outside the reader loop because a
                # callback that waits for a channel event would block itself
                # if it ran in here (`test_the_dtmf_does_not_block_the_loop_that_reads_the_socket`).
                if (self._forward_dtmf and self._provider is not None
                        and self._provider_ready.is_set()):
                    self._spawn(self._provider.on_dtmf(digit), "on_dtmf")

        elif event is Event.ERROR:
            self._on_channel_error(payload)

        elif event is Event.STATUS:
            log.debug("STATUS: %s", payload)

        elif event is Event.MEDIA_BUFFERING_COMPLETED:
            self._resolve_mark(payload)
            log.debug("Buffering completed: %s", payload)

        elif event is Event.QUEUE_DRAINED:
            # A single QUEUE_DRAINED resolves EVERYONE waiting for the empty
            # queue: it means "the queue ran empty", which is what they asked
            # (`test_two_queue_empty_notices_are_distinct_and_one_drain_resolves_both`).
            waiters = self._drain_waiters
            self._drain_waiters = []
            for waiter in waiters:
                waiter.set()
            # The speaking turn, if any, learns the bot really stopped
            # sounding: it lowers its _audio_pending so a barge-in with the
            # queue already empty does not flush a new reply.
            if self._speech is not None:
                self._speech.on_queue_drained()
            log.debug("The Asterisk queue drained: %s", payload)

    async def _on_media_start(self, payload: dict[str, Any]) -> None:
        # A second MEDIA_START (a channel with a bug) would overwrite the
        # provider without closing it: the STT/LLM/TTS connection would be
        # orphaned and billing. The repeat is ignored.
        if self._provider is not None:
            log.warning("Repeated MEDIA_START on the same call, ignored "
                        "(the provider is already started)")
            return

        self._media = parse_media_start(payload)

        # The dialplan variables already travel whole through the on_event hook.
        if self._on_event is None:
            log.debug("channel_variables: %s", self._media.channel_variables)

        log.info(
            "Call started: channel=%s format=%s frame=%dB provider=%s",
            self._media.channel_name,
            self._media.audio_format,
            self._media.optimal_frame_size,
            self._media.provider,
        )

        # In passthrough (the codecs with `minimum_bytes <= 10`: opus, g729,
        # speex, `chan_websocket.c:1406-1408`) barge-in and the clean
        # hang-up do not work and the audio goes compressed, so the provider
        # gets bytes it cannot decode. Without this warning it looks like
        # "the bot does not listen" with no clue. Loud, with the way out
        # (`docs/decisions.md`; `test_passthrough_warns_instead_of_failing_silently`).
        if self._media.passthrough:
            log.warning(
                "The channel is in PASSTHROUGH (format %r): barge-in, marks "
                "and the clean hang-up do not work, and the audio arrives "
                "compressed. Use a full-API codec in the Dial: c(slin16) for a "
                "PCM provider, c(ulaw) for one that speaks telephony. "
                "Transcode in Asterisk, do not send opus/g729/speex to the channel.",
                self._media.audio_format,
            )
            self.emit(events.error(
                f"Codec {self._media.audio_format!r} in passthrough: no "
                f"barge-in and no clean hang-up, and the audio goes compressed. "
                f"Use c(slin16) or c(ulaw) in the Dial.",
                fatal=False,
            ))

        # Everything the dialplan sent, before a single frame plays. The
        # SIP headers travel there: a panel can draw the customer's card
        # while the phone is still ringing.
        self.emit(events.server_message({
            "call": {
                "channel": self._media.channel_name,
                "channel_id": self._media.channel_id,
                "format": self._media.audio_format,
                "frame_size": self._media.optimal_frame_size,
                "caller": self._media.caller,
            },
            "variables": self._media.channel_variables,
        }))

        # The dialplan chose the provider (Set(_AI_PROVIDER=...)). The factory
        # gets ONLY the session (`docs/decisions.md`). start() does not run
        # inline but in its own task: if it hung connecting, it would block
        # the reader loop and with it the close of the call.
        try:
            self._provider = self._provider_factory(self)
        except Exception as exc:
            # The exact point where a badly built functools.partial blows
            # up. Same contract as _start_provider: it has to EXIT through
            # emit and an explicit hang-up, or whoever watches the panel
            # sees a mute call that ended "normally" with no clue
            # (`test_a_factory_that_raises_does_not_leave_the_call_mute`).
            log.exception("The provider factory %r failed",
                          self._media.provider)
            self.emit(events.error(
                f"The provider factory {self._media.provider!r} failed: "
                f"{exc}",
                fatal=True,
            ))
            try:
                await self.hangup()
            except Exception:
                log.debug("The hang-up failed too", exc_info=True)
            return
        self._spawn(self._start_provider(), "start")

    async def _start_provider(self) -> None:
        """Starts the provider outside the reader loop, with a time cap.

        Until it ends, `_on_audio` drops the caller's audio: the provider
        cannot take it yet. A failure here (bad key, provider down, start()
        that never returns) hangs the call up through emit and HANGUP, so no
        mute leg is left billing (`test_provider_start_timeout_configurable`).
        """
        try:
            await asyncio.wait_for(self._provider.start(),
                                   timeout=self._provider_start_timeout_s)
        except Exception as exc:
            # It has to EXIT through emit: if it only goes to the server log,
            # whoever watches the panel sees a mute call with no clue.
            log.exception("The provider %r could not start",
                          self._media.provider)
            self.emit(events.error(
                f"The provider {self._media.provider!r} did not start: {exc}",
                fatal=True,
            ))
            # Hang up explicitly: if the bot's leg stays open and mute, the
            # call survives until some Asterisk timeout, holding a channel
            # and billing the provider.
            try:
                await self.hangup()
            except Exception:
                log.debug("The hang-up failed too", exc_info=True)
            return

        # Net in case the adapter did not call `accept_audio()`: without it
        # a call with an older adapter would stay mute forever
        # (`test_an_adapter_that_does_not_open_it_still_works`).
        self.accept_audio()
        self.emit(events.bot_ready(
            provider=self._media.provider,
            language=self._media.language,
            format=self._media.audio_format,
        ))

    # ------------------------------------------------------------------
    # Escalation and close
    # ------------------------------------------------------------------

    def request_escalation(self, destination: str, reason: str = "") -> None:
        """Marks this call to be handed to a person.

        The channel does not transfer: its tech has no `.transfer`
        (`chan_websocket.c:161-175`); it is a media channel, not a signaling
        one. The real handoff is the dialplan's: this leg hangs up, the
        original SIP channel survives and the dialplan goes on toward the
        queue (`docs/architecture.md`).

        If `serve()` received a `Transfers`, it is also recorded here on its
        own: the CALL_ID key the dialplan seeded (`Set(_CALL_ID=${UNIQUEID})`)
        goes to `transfers.transfer(key, reason)`. Without `Transfers`
        nothing is recorded and the escalation stays in `session.escalation`
        for whoever runs their own `agi_handler`.

        Args:
            destination: Where the dialplan should send the call.
            reason: Why, in one sentence; recorded with the transfer.
        """
        self._escalate_to = destination

        if self._transfers is None:
            log.debug("Escalation to %r without Transfers: only in "
                      "session.escalation", destination)
            return

        key = ""
        if self._media is not None:
            key = self._media.channel_variables.get("CALL_ID", "")
        if not key:
            # With Transfers wired, a missing key is a real config error:
            # the bot records but the dialplan will never be able to ask.
            log.warning(
                "The bot asked to transfer but the dialplan sent no CALL_ID: "
                "the dialplan will not find out. Add "
                "Set(_CALL_ID=${UNIQUEID}) before the Dial."
            )
            return
        self._transfers.transfer(key, reason, destination=destination)

    async def finish(self) -> None:
        """Ends the bot's leg, after what was left plays out.

        Call it as soon as you know the call ends, without waiting for the
        bot to say goodbye: this function waits for you. First for the bot
        to say what it has to say (if it is going to say anything), then for
        Asterisk to confirm that audio finished playing. Both waits fit in
        `finish_max_wait_s`, not one each, and the first is capped at half
        so the goodbye keeps something.

        Careful lowering `finish_max_wait_s`: the default 5 s is plenty, but
        with 2 s the goodbye keeps under a second if the previous turn is
        slow to drain, and a long goodbye gets cut anyway.

        That the wait lives here and not in your code is deliberate: knowing
        whether bot audio is still in play takes speaking the channel
        protocol and keeping the speaking turn, which is exactly what the
        library does. You decide WHEN the call ends; it makes sure it is not
        cut mid-utterance.

        Idempotent: two paths can ask for the end almost at once (the model
        asking to hang up and the provider crashing). If the session already
        closed it does nothing: a late `finish()` would wait in vain for a
        queue-drained notice the dead socket will never send
        (`test_finish_is_idempotent`).

        The guard is once in the session's life: `_finishing` goes True and
        never comes down, so a finish() after one that exhausted its wait
        does not retry either. Deliberate: the first one's HANGUP already
        went out.
        """
        if self._finishing or self._closed.is_set():
            return
        self._finishing = True

        # In passthrough the channel rejects marks and the queue-drained
        # notice: waiting for an ack that will not come is silence for the
        # caller (`test_finish_hangs_up_straight_away_in_passthrough`).
        if self._in_passthrough():
            await self.hangup()
            return

        # The whole close fits in ONE budget, not one per stage: two chained
        # waits with a cap each would make the worst case twice what the
        # kwarg says.
        #
        # But the split cannot be "first come eats it all". The turn's wait
        # is capped at HALF so the goodbye keeps something: if the previous
        # turn takes four and a half seconds to drain, without this cap the
        # goodbye would be cut anyway, just by another path
        # (`test_a_long_previous_turn_does_not_eat_the_goodbyes_budget`).
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._finish_max_wait_s

        try:
            try:
                already_drained = await asyncio.wait_for(
                    self._wait_for_bot_turn(),
                    timeout=self._finish_max_wait_s / 2)
            except asyncio.TimeoutError:
                # Exhausting THIS stage does not abort the close: it means
                # the previous turn takes too long, and what is left of the
                # budget is for the goodbye. On to the queue-drained notice.
                already_drained = False

            # If the bot's turn already finished SOUNDING, there is nothing
            # to wait for: asking for the notice here would ask it over a
            # queue that is already empty, and Asterisk sends no
            # QUEUE_DRAINED when nothing is left to drain. That wait would
            # eat the whole budget for nothing
            # (`test_finish_skips_the_notice_when_the_goodbye_already_played`).
            if not already_drained:
                # Only NOW is the notice asked for: asking before the bot
                # starts speaking waits for the wrong drain, and the goodbye
                # is cut or never plays.
                drained = await self.report_when_drained()
                remaining = deadline - loop.time()
                await asyncio.wait_for(drained.wait(),
                                       timeout=max(remaining, 0.0))
        except asyncio.TimeoutError:
            # Hang up anyway: the alternative is a call open indefinitely
            # waiting for audio that may never end.
            log.warning(
                "Hung up after waiting %.0fs for the queued audio to finish. "
                "More audio was left than fits in that margin.",
                self._finish_max_wait_s,
            )
        except Exception:
            log.exception("The wait for the final audio failed")

        await self.hangup()

    # How often the bot is checked for the start of its goodbye. A poll and
    # not an event on purpose: the "I started speaking" notice would come
    # from the provider, and that is three providers with three different
    # events (or none). Watching the speaking turn, which is ours, works the
    # same with all three.
    _TURN_POLL_S = 0.05

    async def _wait_for_bot_turn(self) -> bool:
        """Waits for the bot to finish its turn, if it gets to start one.

        Returns:
            True if on exit its audio is known to have ALREADY played whole,
            so nothing is left in the Asterisk queue and no notice is
            needed. False if the bot never started, and there the notice is
            needed because audio from before may remain.

        The case it solves: the model asks to hang up as a tool and the
        application calls `finish()` right away, but the goodbye does not
        exist yet. The provider takes a moment to generate it, so at that
        instant NOTHING is playing and the queue is empty. Without this wait
        the queue-drained notice is asked over that empty queue, arrives at
        once, and the call hangs up just as the goodbye started to play
        (`test_finish_waits_for_a_goodbye_that_has_not_started_yet`;
        `docs/decisions.md`).

        Two phases, and both are needed:

            1. if the bot has not started yet, it gets a grace period in
               case it is about to. Exit as soon as it starts
            2. while the bot has audio in play, wait

        If it never starts (the application hangs up without a goodbye,
        which is legitimate), phase 1 runs out and we go on: the call is not
        hung waiting for an utterance nobody will say
        (`test_finish_does_not_stall_when_the_bot_never_speaks`). The phase 1
        cap is short on purpose; the big budget is for the audio to play,
        not for waiting on a provider that will not answer.
        """
        if self._speech is None:
            return False

        loop = asyncio.get_running_loop()
        grace = min(1.5, self._finish_max_wait_s / 2)

        # Which turn was sounding on entry. It tells "the bot is still
        # speaking" from "a NEW goodbye started", which cannot be deduced
        # from whether audio is in play alone
        # (`test_finish_waits_when_the_previous_turn_is_still_playing`).
        turn_on_entry = self._speech.turn

        # Phase 2 first, if something was already sounding: wait for the
        # turn in progress to end. It may be the previous one, not the goodbye.
        while self._speech.bot_audio_playing and not self._closed.is_set():
            await asyncio.sleep(self._TURN_POLL_S)

        # Phase 1: the grace period. The cap comes from real calls: between
        # the model asking to hang up and its first audio frame about one
        # second passes (`docs/decisions.md`). Waited here both when nothing
        # was sounding on entry and when what sounded was the PREVIOUS turn:
        # in both cases the goodbye is still to come.
        deadline = loop.time() + grace
        while (self._speech.turn == turn_on_entry
               and not self._speech.bot_audio_playing
               and loop.time() < deadline
               and not self._closed.is_set()):
            await asyncio.sleep(self._TURN_POLL_S)

        if not self._speech.bot_audio_playing:
            # No goodbye started. If the entry turn got to drain, everything
            # already played and there is no notice to ask for; if there
            # never was anything, let the queue notice decide.
            return self._speech.turn != turn_on_entry or turn_on_entry > 0

        # Phase 2 of the goodbye: let what just started finish.
        while self._speech.bot_audio_playing and not self._closed.is_set():
            await asyncio.sleep(self._TURN_POLL_S)
        return not self._closed.is_set()

    async def _cleanup(self) -> None:
        # Closed BEFORE anything else: a finish() landing during the cleanup
        # (a provider whose close() calls finish()) has to cut through the
        # `_closed` guard, not wait for a notice that will not come.
        self._closed.set()

        # Cancel AND await: cancelling without awaiting leaves code running
        # after the call is given up as closed.
        if self._tasks:
            pending = list(self._tasks)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        # Empty the queued audio without delivering: its task was cancelled,
        # so those frames would only take memory.
        while True:
            try:
                self._audio_in.get_nowait()
            except asyncio.QueueEmpty:
                break

        # Release whoever waited for something that is not going to happen
        # (a mark, flow control, the queue-drained notice): otherwise it
        # outlives the call (`test_the_close_releases_whoever_was_waiting`).
        self._can_send.set()
        for waiter in self._drain_waiters:
            waiter.set()
        self._drain_waiters.clear()
        for pending_mark in self._pending_marks.values():
            pending_mark.set()
        self._pending_marks.clear()

        if self._discarded_frames:
            log.warning("Dropped %d audio frames under a sustained XOFF",
                        self._discarded_frames)

        if self._audio_dropped:
            log.warning("Dropped %d incoming frames: the provider could not "
                        "keep up with real time", self._audio_dropped)

        if self._audio_before_ready:
            # At 20 ms per frame this reads as time: 50 frames is 1 s of the
            # call in which the person spoke and nobody listened.
            log.info("Dropped %d incoming frames while the provider was "
                     "starting (%.1f s of audio)", self._audio_before_ready,
                     self._audio_before_ready * 0.02)

        if self._provider is not None:
            try:
                # With a cap: a hung close() would block the orderly shutdown
                # of the whole server. Past the margin it is given up
                # (`test_provider_close_timeout_configurable`).
                await asyncio.wait_for(self._provider.close(),
                                       timeout=self._provider_close_timeout_s)
            except asyncio.TimeoutError:
                log.warning("The provider did not close in %.0fs, abandoned",
                            self._provider_close_timeout_s)
            except Exception:
                log.exception("The provider close failed")

    @property
    def media(self) -> MediaStart | None:
        """The parsed MEDIA_START, or None before it arrives."""
        return self._media

    @property
    def escalation(self) -> str | None:
        """Where `request_escalation()` pointed, or None."""
        return self._escalate_to

    @property
    def speech(self) -> SpeechState:
        """The bot's speaking turn, created on demand.

        The first access creates it, the rest return the same one (why
        lazy: `docs/decisions.md`). An adapter that needs another alignment,
        because it transcodes like the ElevenLabs one, replaces it by
        assigning: `session.speech = SpeechState(session, frame_size, silence_byte)`.

        Raises:
            RuntimeError: Before MEDIA_START there is no format to derive
                from, and freezing the padding values would be a silent
                misalignment (`test_speech_before_media_start_raises`).
        """
        if self._speech is None:
            if self._media is None:
                raise RuntimeError(
                    "session.speech is not available before MEDIA_START: "
                    "the channel format is not known yet. Access it from the "
                    "provider's start() onward."
                )
            self._speech = SpeechState(self)
        return self._speech

    @speech.setter
    def speech(self, value: SpeechState) -> None:
        self._speech = value
