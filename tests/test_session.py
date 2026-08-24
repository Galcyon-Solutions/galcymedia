"""
Session tests.

This is where the things that actually break in production live: flow control,
audio marks and event ordering. Tested against a simulated WebSocket, so they
run in milliseconds and without Asterisk.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from galcymedia import Session


class FakeWebSocket:
    """A fake Asterisk: delivers a script and records what is sent to it."""

    def __init__(self, script=None, close_code=None, close_reason=""):
        self.script = script or []
        self.sent: list = []
        self._delivered = asyncio.Event()
        # How the connection ended. `websockets` exposes these two attributes
        # when the socket closes, and they are the only clue about why the call
        # hung up: the channel does not send any HANGUP event.
        self.close_code = close_code
        self.close_reason = close_reason

    def __aiter__(self):
        async def generate():
            for message in self.script:
                yield message
                await asyncio.sleep(0)
            self._delivered.set()
        return generate()

    async def send(self, message):
        self.sent.append(message)

    @property
    def commands(self) -> list[dict]:
        return [json.loads(m) for m in self.sent if isinstance(m, str)]

    @property
    def audio(self) -> list[bytes]:
        return [m for m in self.sent if isinstance(m, bytes)]


class EchoProvider:
    def __init__(self, session):
        self.session = session
        self.media = session.media
        self.started = False
        self.closed = 0
        self.digits: list[str] = []

    async def start(self):
        self.started = True

    async def send_audio(self, chunk):
        await self.session.send_audio(chunk)

    async def on_dtmf(self, digit):
        self.digits.append(digit)

    async def close(self):
        self.closed += 1


def media_start(**extra) -> str:
    base = {
        "event": "MEDIA_START",
        "connection_id": "conn-1",
        "channel_id": "c1",
        "channel": "PJSIP/1001-00000001",
        "format": "alaw",
        "optimal_frame_size": 160,
        "ptime": 20,
        "channel_variables": {},
    }
    base.update(extra)
    return json.dumps(base)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_provider_starts_when_media_start_arrives():
    created = []

    def factory(session):
        p = EchoProvider(session)
        created.append(p)
        return p

    ws = FakeWebSocket([media_start()])
    await Session(ws, factory).run()

    assert len(created) == 1
    assert created[0].started


async def test_a_plain_text_channel_is_refused_once(caplog):
    """Without f(json) the channel sends plain text: one error, then hang up.

    A warning per frame ("not valid JSON") never names the cause; the
    integrator needs the missing option, once, and no provider started.
    """
    created = []

    def factory(session):
        p = EchoProvider(session)
        created.append(p)
        return p

    plain_media_start = (
        "MEDIA_START connection_id:conn-1 channel:PJSIP/1001-00000001 "
        "channel_id:c1 format:alaw optimal_frame_size:160 ptime:20"
    )
    seen: list[str] = []
    ws = FakeWebSocket([plain_media_start, "MEDIA_XON", "MEDIA_XON"])
    with caplog.at_level("WARNING"):
        await Session(ws, factory, on_event=lambda n, p: seen.append(n)).run()

    errors = [r for r in caplog.records if "text mode" in r.getMessage()]
    assert len(errors) == 1, "one error for the whole call, not one per frame"
    assert errors[0].levelname == "ERROR"
    assert not [r for r in caplog.records if "not JSON" in r.getMessage()], \
        "no per-frame warning reached the log"
    assert created == [], "no provider starts on a channel nobody can read"
    # The whole cycle of _AlreadyLogged: raised in _on_control, caught by
    # run(), and its finally still publishes the end through the peephole.
    assert seen == ["HANGUP"], (
        f"peephole saw {seen}: the refused call has to end like any other")


async def test_provider_closes_even_when_the_call_ends_abruptly():
    """Without this, a connection to the provider is left open for every call."""
    created = []
    ws = FakeWebSocket([media_start()])
    await Session(ws, lambda s: created.append(EchoProvider(s)) or created[-1]).run()
    assert created[0].closed == 1


async def test_audio_arriving_before_media_start_is_discarded():
    """Asterisk issue #1712: it really happens, and more so above 60% CPU.

    Without the guard, the first frame blows up against a provider that does
    not exist yet and takes down the whole call.
    """
    ws = FakeWebSocket([b"\xd5" * 160, media_start(), b"\xd5" * 160])
    await Session(ws, EchoProvider).run()
    assert len(ws.audio) == 1, "only the frame after MEDIA_START should come back"


async def test_digits_reach_the_provider_when_forwarding_is_on():
    created = []
    ws = FakeWebSocket([
        media_start(),
        json.dumps({"event": "DTMF_END", "digit": "7"}),
    ])
    await Session(ws, lambda s: created.append(EchoProvider(s)) or created[-1],
                  forward_dtmf=True).run()
    assert created[0].digits == ["7"]


# ---------------------------------------------------------------------------
# Flow control: the part the official example gets wrong
# ---------------------------------------------------------------------------


async def test_xoff_stops_sending_and_xon_resumes_it():
    """The official example uses a Lock and blocks itself.

    It takes the lock in the SAME task that reads the socket, so the XON that
    would release it can never be read. Here it is an Event: the reader sets it,
    the writer waits on it, and they do not get in each other's way.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)

    await session._on_control(json.dumps({"event": "MEDIA_XOFF"}))

    send = asyncio.create_task(session.send_audio(b"\xd5" * 160))
    await asyncio.sleep(0.05)
    assert not send.done(), "with XOFF active nothing can have been sent"

    await session._on_control(json.dumps({"event": "MEDIA_XON"}))
    await asyncio.wait_for(send, timeout=1.0)
    assert len(ws.audio) == 1


async def test_an_xon_without_a_prior_xoff_does_not_blow_up():
    """With a Lock this raises RuntimeError: Lock is not acquired."""
    session = Session(FakeWebSocket(), EchoProvider)
    await session._on_control(json.dumps({"event": "MEDIA_XON"}))
    await asyncio.wait_for(session.send_audio(b"\x00"), timeout=1.0)


async def test_repeated_xoff_stays_a_single_state():
    """The Event is idempotent. A Lock taken twice deadlocks itself."""
    session = Session(FakeWebSocket(), EchoProvider)
    for _ in range(3):
        await session._on_control(json.dumps({"event": "MEDIA_XOFF"}))
    await session._on_control(json.dumps({"event": "MEDIA_XON"}))
    await asyncio.wait_for(session.send_audio(b"\x00"), timeout=1.0)


# ---------------------------------------------------------------------------
# Marks: how to hang up without cutting off the goodbye
# ---------------------------------------------------------------------------


async def test_the_mark_resolves_when_asterisk_reports_it():
    session = Session(FakeWebSocket(), EchoProvider)
    rang = await session.mark()
    assert not rang.is_set()

    correlation_id = session._ws.commands[-1]["correlation_id"]
    await session._on_control(json.dumps({
        "event": "MEDIA_MARK_PROCESSED",
        "correlation_id": correlation_id,
    }))
    assert rang.is_set(), "the mark had to resolve"


async def test_two_marks_do_not_get_confused_with_each_other():
    """Without correlation, the second goodbye would hang on the first."""
    session = Session(FakeWebSocket(), EchoProvider)
    first = await session.mark()
    second = await session.mark()

    ids = [c["correlation_id"] for c in session._ws.commands]
    assert ids[0] != ids[1]

    await session._on_control(json.dumps({
        "event": "MEDIA_MARK_PROCESSED", "correlation_id": ids[1],
    }))
    assert second.is_set()
    assert not first.is_set(), "only the mark Asterisk named is resolved"


async def test_an_unknown_mark_is_ignored():
    session = Session(FakeWebSocket(), EchoProvider)
    await session._on_control(json.dumps({
        "event": "MEDIA_MARK_PROCESSED", "correlation_id": "does-not-exist",
    }))


# ---------------------------------------------------------------------------
# Watching does not command what is watched
# ---------------------------------------------------------------------------
#
# There used to be a second way to watch a call, an observer factory, and it
# could bring the call down: four of its five call sites ran without a guard,
# so a bug in someone's tracing code killed the conversation. Watching now
# happens in one place, and it cannot do that.


async def test_a_broken_peephole_does_not_bring_the_call_down():
    """The one rule: whoever only watches never decides."""
    def explodes(name, payload):
        raise RuntimeError("the tracing code has a bug")

    ws = FakeWebSocket([
        media_start(),
        json.dumps({"event": "MEDIA_XOFF"}),
        json.dumps({"event": "MEDIA_XON"}),
    ])
    session = Session(ws, EchoProvider, on_event=explodes)

    await session.run()

    assert session.media is not None, "the call went through anyway"


# ---------------------------------------------------------------------------
# The protocol peephole (on_event)
# ---------------------------------------------------------------------------
#
# It exists so that whoever integrates sees EVERYTHING Asterisk sends, not just
# what the library decided to interpret. Before this, three known events
# (STATUS, MEDIA_BUFFERING_COMPLETED, QUEUE_DRAINED) and any future event fell
# into the void: no log, no notice, no way to find out.


async def test_the_peephole_sees_every_event():
    seen: list[str] = []

    ws = FakeWebSocket([
        media_start(),
        json.dumps({"event": "MEDIA_XOFF"}),
        json.dumps({"event": "MEDIA_XON"}),
    ])
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append(n)).run()

    # The HANGUP at the end is synthetic: the session fabricates it when the
    # socket closes, because the channel does not send any hangup event.
    assert seen == ["MEDIA_START", "MEDIA_XOFF", "MEDIA_XON", "HANGUP"]


async def test_the_peephole_sees_the_events_the_library_does_not_handle():
    """The three that used to vanish without a trace."""
    seen: list[str] = []

    ws = FakeWebSocket([
        media_start(),
        json.dumps({"event": "STATUS", "state": "up"}),
        json.dumps({"event": "QUEUE_DRAINED"}),
        json.dumps({"event": "MEDIA_BUFFERING_COMPLETED"}),
    ])
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append(n)).run()

    assert "STATUS" in seen
    assert "QUEUE_DRAINED" in seen
    assert "MEDIA_BUFFERING_COMPLETED" in seen


async def test_the_peephole_sees_an_event_the_library_does_not_know():
    """Asterisk 24 may add events. Whoever integrates sees them from day one."""
    seen: list[tuple[str, dict]] = []

    ws = FakeWebSocket([
        media_start(),
        json.dumps({"event": "EVENT_FROM_THE_FUTURE", "datum": 42}),
    ])
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append((n, p))).run()

    names = [n for n, _ in seen]
    assert "EVENT_FROM_THE_FUTURE" in names
    # And it arrives with its full content, not just the name.
    payload = dict(seen[names.index("EVENT_FROM_THE_FUTURE")][1])
    assert payload["datum"] == 42


async def test_a_peephole_that_blows_up_does_not_take_down_the_call():
    """Instrumentation accompanies, it does not command. Same rule as emit()."""
    def explode(name, payload):
        raise RuntimeError("failed while watching")

    created = []

    def factory(session):
        p = EchoProvider(session)
        created.append(p)
        return p

    ws = FakeWebSocket([media_start(), json.dumps({"event": "MEDIA_XON"})])
    await Session(ws, factory, on_event=explode).run()

    # The call was answered and closed normally.
    assert created and created[0].started
    assert created[0].closed == 1


async def test_the_session_works_the_same_without_a_peephole():
    ws = FakeWebSocket([media_start(), json.dumps({"event": "MEDIA_XON"})])
    await Session(ws, EchoProvider).run()


# ---------------------------------------------------------------------------
# The end of the call
# ---------------------------------------------------------------------------
#
# The channel does not send any hangup event: verified in chan_websocket.c, it
# emits nine events and none reports the end. The only thing left is the
# WebSocket close code, which Asterisk chooses on purpose. The session
# translates it into a synthetic HANGUP so the application receives the end via
# the same path as everything else.


async def test_a_normal_hangup_is_published():
    seen: list[tuple[str, dict]] = []

    ws = FakeWebSocket([media_start()], close_code=1000)
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append((n, p))).run()

    ends = [p for n, p in seen if n == "HANGUP"]
    assert len(ends) == 1
    assert ends[0]["code"] == 1000
    assert ends[0]["normal"] is True
    # Parity with the channel: all its events carry channel_id
    # (Asterisk 23.4.1, chan_websocket.c:204-362), so the synthetic one does too.
    assert ends[0]["channel_id"] == "c1"


async def test_a_hangup_from_the_other_side_is_distinguished():
    """1001 is the code Asterisk uses when the caller hangs up."""
    seen: list[tuple[str, dict]] = []

    ws = FakeWebSocket([media_start()], close_code=1001,
                       close_reason="going away")
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append((n, p))).run()

    end = next(p for n, p in seen if n == "HANGUP")
    assert end["code"] == 1001
    assert end["normal"] is False
    assert end["reason"] == "going away"


async def test_the_hangup_arrives_after_everything_else():
    """The end is last, or a trace of the call would be out of order."""
    seen: list[str] = []

    ws = FakeWebSocket([media_start(), json.dumps({"event": "MEDIA_XON"})],
                       close_code=1000)
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append(n)).run()

    assert seen[-1] == "HANGUP"
    assert seen[:-1] == ["MEDIA_START", "MEDIA_XON"]


async def test_without_a_close_code_it_is_considered_normal():
    """Closing without sending a code is the usual case in an orderly end."""
    seen: list[tuple[str, dict]] = []

    ws = FakeWebSocket([media_start()])          # close_code = None
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append((n, p))).run()

    end = next(p for n, p in seen if n == "HANGUP")
    assert end["normal"] is True


async def test_the_synthetic_hangup_carries_the_channel_id_before_media_start():
    """A call that dies before MEDIA_START still hangs up with its id.

    Every channel event carries `channel_id` (Asterisk 23.4.1,
    `chan_websocket.c:204-362`), so the first one that arrives is enough.
    Taking it from MEDIA_START alone left `""`, and a shared hook could not
    tell which of its concurrent calls just ended.
    """
    seen: list[tuple[str, dict]] = []

    ws = FakeWebSocket([json.dumps({"event": "MEDIA_XON", "channel_id": "c9"})],
                       close_code=1001)
    await Session(ws, EchoProvider,
                  on_event=lambda n, p: seen.append((n, p))).run()

    end = next(p for n, p in seen if n == "HANGUP")
    assert end["channel_id"] == "c9", (
        f"channel_id {end['channel_id']!r}: the id from the first event was not kept")


# ---------------------------------------------------------------------------
# Cases that corrupt the call if not handled
# ---------------------------------------------------------------------------


async def test_the_flush_releases_the_pending_marks():
    """FLUSH_MEDIA destroys queued marks and their ack never arrives.

    MARK_MEDIA queues a control frame alongside the audio, and FLUSH_MEDIA
    releases the whole queue without looking at the type. Whoever was waiting
    for that mark has to find out right away, not exhaust their timeout.
    """
    session = Session(FakeWebSocket(), EchoProvider)
    rang = await session.mark()
    assert not rang.is_set()

    await session.flush()

    assert rang.is_set(), "the mark had to be given up for lost"
    assert not session._pending_marks, "and no trace can remain"


async def test_two_queue_empty_notices_are_distinct_and_one_drain_resolves_both():
    """Each report_when_drained() gets ITS own Event, and a single
    QUEUE_DRAINED from the channel resolves them all.

    A single shared Event would step on itself between two callers: one's
    clear() would reset the already-fired signal of the other. And since
    finish() uses this same path, an integrator's report_when_drained() running
    at the same time would steal it. A QUEUE_DRAINED means "the queue is empty",
    which is what every pending waiter was waiting for: it resolves both.
    """
    session = Session(FakeWebSocket(), EchoProvider)

    first = await session.report_when_drained()
    second = await session.report_when_drained()

    assert first is not second, "each notice is its own Event"
    assert not first.is_set() and not second.is_set()

    await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

    assert first.is_set() and second.is_set(), \
        "a single QUEUE_DRAINED resolves both that were waiting"


async def test_finish_is_idempotent():
    """Two paths can request the end of the same call at once.

    It really happens: the model asks to hang up and at the same time the
    provider crashes.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)

    async def notify_queue_empty():
        await asyncio.sleep(0.02)
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

    await asyncio.gather(session.finish(), session.finish(),
                         notify_queue_empty())

    hung_up = [c for c in ws.commands if c.get("command") == "HANGUP"]
    assert len(hung_up) == 1, "it can only hang up once"


async def test_finish_waits_for_a_goodbye_that_has_not_started_yet():
    """The hang-up tool fires BEFORE the provider generates the goodbye.

    Found on a real call: the model asked to hang up at 17:53:39, the goodbye
    only started at 17:53:40, and the call was cut mid-sentence. At the moment
    finish() runs there is nothing playing and the queue is empty, so asking
    for the queue-empty notice right away waits for the wrong drain.

    finish() has to hold until the bot's turn is over, and only then hang up.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, finish_max_wait_s=5.0)
    await session._on_control(media_start())
    speech = session.speech

    playing_at_hangup = []

    async def provider_says_goodbye():
        # The queue empties FIRST, while nothing is playing yet: this is the
        # drain that belongs to the previous turn, and it is what used to wake
        # finish() up too early. On the real call it was the greeting's.
        await asyncio.sleep(0.05)
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

        await asyncio.sleep(0.15)             # the model takes its time
        await speech.play(b"\x01" * 160)      # only now the goodbye starts
        await asyncio.sleep(0.2)
        await speech.end_turn(reset_discard=True)
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

    async def watch_hangup():
        while not any(c["command"] == "HANGUP" for c in ws.commands):
            await asyncio.sleep(0.01)
        playing_at_hangup.append(speech.bot_audio_playing)

    await asyncio.gather(session.finish(), provider_says_goodbye(),
                         watch_hangup())

    assert playing_at_hangup == [False], (
        "hung up while the bot was still speaking: the goodbye gets cut")
    assert ws.commands[-1]["command"] == "HANGUP"


async def test_finish_waits_when_the_previous_turn_is_still_playing():
    """The mirror case: something IS playing when finish() runs, but it is the
    PREVIOUS turn, not the goodbye.

    Waiting only for "nothing playing" hangs up as soon as that older audio
    drains, which lands right before the goodbye starts. The turn counter is
    what tells the two apart.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, finish_max_wait_s=5.0)
    await session._on_control(media_start())
    speech = session.speech

    await speech.play(b"\x01" * 160)              # the previous turn,
    await speech.end_turn(reset_discard=True)     # closed but still queued
    assert speech.bot_audio_playing

    goodbye_heard = []

    async def rest_of_the_call():
        await asyncio.sleep(0.05)
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))
        await asyncio.sleep(0.15)                 # the model takes its time
        await speech.play(b"\x02" * 160)          # only now the goodbye
        goodbye_heard.append(True)
        await asyncio.sleep(0.1)
        await speech.end_turn(reset_discard=True)
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

    async def watch_hangup():
        while not any(c["command"] == "HANGUP" for c in ws.commands):
            await asyncio.sleep(0.01)
        return list(goodbye_heard)

    _, _, at_hangup = await asyncio.gather(
        session.finish(), rest_of_the_call(), watch_hangup())

    assert at_hangup == [True], "hung up before the goodbye even started"


async def test_a_long_previous_turn_does_not_eat_the_goodbyes_budget():
    """The two waits share ONE budget, so a slow previous turn could starve the
    goodbye and cut it by a different path than the original bug.

    Here the previous turn takes most of the budget to drain. What must not
    happen is hanging up while the goodbye is sounding.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, finish_max_wait_s=0.5)
    await session._on_control(media_start())
    speech = session.speech

    await speech.play(b"\x01" * 160)
    await speech.end_turn(reset_discard=True)

    playing_at_hangup = []

    async def slow_previous_turn_then_goodbye():
        await asyncio.sleep(0.35)                 # most of the budget
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))
        await asyncio.sleep(0.05)
        await speech.play(b"\x02" * 160)          # the goodbye, on a shoestring

    async def watch_hangup():
        while not any(c["command"] == "HANGUP" for c in ws.commands):
            await asyncio.sleep(0.01)
        playing_at_hangup.append(speech.bot_audio_playing)

    await asyncio.gather(session.finish(), slow_previous_turn_then_goodbye(),
                         watch_hangup())

    assert playing_at_hangup == [False], (
        "the previous turn ate the budget and the goodbye got cut anyway")


async def test_finish_does_not_stall_when_the_bot_never_speaks():
    """Hanging up without a goodbye is legitimate and must not wait.

    The grace period only exists in case a goodbye is about to start. If none
    comes, finish() must not burn its whole budget: it hangs up.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, finish_max_wait_s=1.0)
    await session._on_control(media_start())

    async def drain_notice():
        await asyncio.sleep(0.05)
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

    started = asyncio.get_running_loop().time()
    await asyncio.gather(session.finish(), drain_notice())
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 1.0, f"waited {elapsed:.2f}s for a goodbye that never came"
    assert ws.commands[-1]["command"] == "HANGUP"


async def test_finish_waits_for_the_queue_to_empty_not_for_a_mark():
    """The mark is destroyed by a flush; the queue-empty notice survives.

    It is the common case: the bot says goodbye, it is interrupted, and the
    model asks to hang up. With a mark that wait would exhaust entirely in
    silence.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)

    async def interrupt_and_flush():
        await asyncio.sleep(0.02)
        await session.flush()            # would destroy a pending mark
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

    await asyncio.gather(session.finish(), interrupt_and_flush())

    requests = [c["command"] for c in ws.commands]
    assert "REPORT_QUEUE_DRAINED" in requests
    assert "MARK_MEDIA" not in requests, "finish() no longer depends on a mark"
    assert requests[-1] == "HANGUP"


async def test_the_eleven_channel_commands_are_reachable():
    """If the channel offers something, whoever integrates has to be able to use it."""
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)

    await session.answer()
    await session.start_buffering()
    await session.stop_buffering()
    await session.mark()
    await session.flush()
    await session.request_status()
    await session.report_when_drained()
    await session.pause()
    await session.continue_media()
    await session.set_media_direction("in")
    await session.hangup()

    sent = {c["command"] for c in ws.commands}
    assert sent == {
        "ANSWER", "HANGUP", "START_MEDIA_BUFFERING", "STOP_MEDIA_BUFFERING",
        "MARK_MEDIA", "FLUSH_MEDIA", "GET_STATUS", "REPORT_QUEUE_DRAINED",
        "PAUSE_MEDIA", "CONTINUE_MEDIA", "SET_MEDIA_DIRECTION",
    }


async def test_an_invalid_media_direction_is_rejected_when_requested():
    session = Session(FakeWebSocket(), EchoProvider)
    with pytest.raises(ValueError):
        await session.set_media_direction("left")


async def test_the_end_of_buffering_resolves_its_wait():
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)

    ready = await session.stop_buffering()
    assert not ready.is_set()

    correlation_id = ws.commands[-1]["correlation_id"]
    await session._on_control(json.dumps({
        "event": "MEDIA_BUFFERING_COMPLETED",
        "correlation_id": correlation_id,
    }))
    assert ready.is_set()


async def test_a_sustained_xoff_discards_audio_instead_of_blocking():
    """Without a limit, an XOFF that is never lifted stalls the task forever.

    The limit is configurable per session (kwarg of Session and of serve());
    here it is shortened so as not to wait the default 5 s.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, xoff_max_wait_s=0.05)
    await session._on_control(json.dumps({"event": "MEDIA_XOFF"}))

    await asyncio.wait_for(session.send_audio(b"\xd5" * 160), timeout=1.0)

    assert not ws.audio, "with XOFF active nothing is sent"
    assert session._discarded_frames == 1


async def test_the_close_releases_whoever_was_waiting():
    session = Session(FakeWebSocket(), EchoProvider)
    rang = await session.mark()

    await session._cleanup()

    assert rang.is_set()
    assert not session._pending_marks


async def test_passthrough_is_detected_by_a_zero_frame_size():
    """The channel marks passthrough with optimal_frame_size=0, and there are no marks there."""
    from galcymedia import parse_media_start

    normal = parse_media_start(json.loads(media_start()))
    assert not normal.passthrough
    assert normal.optimal_frame_size == 160

    direct = parse_media_start(json.loads(media_start(optimal_frame_size=0)))
    assert direct.passthrough, "a zero means passthrough, not absence"

    without_field = parse_media_start({"event": "MEDIA_START"})
    assert without_field.optimal_frame_size == 160, "absent falls back to the default"


async def test_the_dtmf_does_not_block_the_loop_that_reads_the_socket():
    """A callback that waits for a channel event cannot run in the reader.

    If `on_dtmf` hung the call waiting for the queue-empty notice inside the
    loop, that notice would arrive at the socket and nobody could read it: the
    call blocks until the timeout expires. That is why it runs in its own task.
    """
    class HangingProvider:
        def __init__(self, session):
            self.session = session
            self.hung_up = False

        async def start(self): pass
        async def send_audio(self, chunk): pass
        async def close(self): pass

        async def on_dtmf(self, digit):
            # Waits for an event only the main loop can read.
            await self.session.finish()
            self.hung_up = True

    created = []

    def factory(session):
        p = HangingProvider(session)
        created.append(p)
        return p

    ws = FakeWebSocket([
        media_start(),
        json.dumps({"event": "DTMF_END", "digit": "0"}),
        json.dumps({"event": "QUEUE_DRAINED"}),
    ])

    # Without the fix this takes as long as finish()'s timeout.
    await asyncio.wait_for(
        Session(ws, factory, forward_dtmf=True).run(), timeout=2.0)

    assert created[0].hung_up, "the callback had to complete"
    assert any(c.get("command") == "HANGUP" for c in ws.commands)


async def test_the_keypad_does_not_reach_the_provider_unless_asked():
    """What people type is not what people say.

    Card numbers, ID numbers and PINs travel over the keypad, and a provider
    does not need any of it to hold a conversation. Forwarding it by default
    would put those digits into a third party's records because nobody thought
    about it, which is the wrong way round for a decision like that.
    """
    seen = []

    class Provider:
        def __init__(self, session): pass
        async def start(self): pass
        async def send_audio(self, chunk): pass
        async def close(self): pass
        async def on_dtmf(self, digit): seen.append(digit)

    def call(**kwargs):
        ws = FakeWebSocket([
            media_start(),
            json.dumps({"event": "DTMF_END", "digit": "4"}),
        ])
        return Session(ws, Provider, **kwargs).run()

    await call()
    assert seen == [], "silence is the default"

    await call(forward_dtmf=True)
    assert seen == ["4"], "and it is reached by asking for it"


async def test_the_keypad_always_reaches_the_integrator():
    """Turning off the forwarding hides the digit from the provider, not from
    whoever runs the call: it is their server and their business rule."""
    published = []

    ws = FakeWebSocket([
        media_start(),
        json.dumps({"event": "DTMF_END", "digit": "7"}),
    ])
    await Session(ws, EchoProvider, emit=published.append).run()

    digits = [e.data["dtmf"]["digit"] for e in published
              if isinstance(e.data, dict) and "dtmf" in e.data]
    assert digits == ["7"]


async def test_passthrough_warns_instead_of_failing_silently(caplog):
    """A codec in passthrough (opus/g729/speex) breaks barge-in and clean
    hangup, and the audio arrives compressed: the provider receives bytes it
    cannot decode. Without a warning, it looks like 'the bot does not hear'
    with no clue. The session warns loudly, with the fix (transcode to
    slin16/ulaw in the Dial), and emits an error event for the integrator's
    panel."""
    import logging

    emitted = []
    ws = FakeWebSocket([media_start(format="opus", optimal_frame_size=0)])
    session = Session(ws, EchoProvider, emit=lambda e: emitted.append(e))

    with caplog.at_level(logging.WARNING, logger="galcymedia.session"):
        await session.run()

    assert any("PASSTHROUGH" in r.message for r in caplog.records), \
        "it has to warn about passthrough in the log"
    types = [e.type.value for e in emitted]
    assert "error" in types, "and emit an error event for the panel"


async def test_a_normal_codec_does_not_trigger_the_passthrough_warning(caplog):
    """The warning is only for passthrough: a normal call (alaw, slin16) must
    not dirty the log with the advisory."""
    import logging

    ws = FakeWebSocket([media_start(format="slin16", optimal_frame_size=640)])
    with caplog.at_level(logging.WARNING, logger="galcymedia.session"):
        await Session(ws, EchoProvider).run()

    assert not any("PASSTHROUGH" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# The new shape: one-argument factory, lazy speech, injected transfers,
# policies configurable per kwarg
# ---------------------------------------------------------------------------


async def test_the_factory_accepts_functools_partial_with_config():
    """The factory is a callable of ONE argument: functools.partial loads the
    adapter config without lambdas, which is the intended use."""
    import functools

    created = []

    class WithConfig:
        def __init__(self, session, config):
            self.session = session
            self.config = config
            created.append(self)

        async def start(self): pass
        async def send_audio(self, chunk): pass
        async def on_dtmf(self, digit): pass
        async def close(self): pass

    ws = FakeWebSocket([media_start()])
    await Session(ws, functools.partial(WithConfig, config={"model": "m1"})).run()

    assert len(created) == 1
    assert created[0].config == {"model": "m1"}


async def test_speech_before_media_start_raises():
    """Without MEDIA_START there is no format to derive from: freezing the
    filler values would be a silent misalignment, so it raises."""
    session = Session(FakeWebSocket(), EchoProvider)
    with pytest.raises(RuntimeError):
        _ = session.speech


async def test_speech_is_created_on_its_own_and_is_always_the_same():
    from galcymedia import SpeechState

    ws = FakeWebSocket([media_start()])
    session = Session(ws, EchoProvider)
    await session.run()

    first = session.speech
    assert isinstance(first, SpeechState)
    assert session.speech is first, "the second access returns the same one"


async def test_speech_can_be_overridden_with_a_custom_one():
    """The adapter that transcodes (ElevenLabs) needs another aligned one:
    assigning replaces the derived one."""
    from galcymedia import SpeechState

    ws = FakeWebSocket([media_start()])
    session = Session(ws, EchoProvider)
    await session.run()

    custom = SpeechState(session, 320, 0x00)
    session.speech = custom
    assert session.speech is custom


async def test_request_escalation_records_in_transfers_with_call_id():
    from galcymedia import Transfers

    transfers = Transfers()
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, transfers=transfers)
    await session._on_control(media_start(channel_variables={"CALL_ID": "k-1"}))
    await asyncio.sleep(0)   # let the provider's start finish

    session.request_escalation("queue", reason="asked for a human")

    assert session.escalation == "queue"
    assert len(transfers) == 1, "the escalation was recorded for FastAGI"


async def test_request_escalation_without_call_id_does_not_record_but_does_not_raise(caplog):
    from galcymedia import Transfers

    transfers = Transfers()
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, transfers=transfers)
    await session._on_control(media_start())   # without CALL_ID
    await asyncio.sleep(0)

    session.request_escalation("queue", reason="asked for a human")

    assert session.escalation == "queue"
    assert len(transfers) == 0
    assert any("CALL_ID" in r.message for r in caplog.records), \
        "without the key, the warning is the only clue the dialplan will not find out"


async def test_request_escalation_without_transfers_only_marks():
    """Whoever handles their own agi_handler does not use Transfers: the
    escalation stays in session.escalation, without noise."""
    session = Session(FakeWebSocket(), EchoProvider)
    session.request_escalation("queue")
    assert session.escalation == "queue"


def test_the_policy_defaults_did_not_change():
    """The Task 2 contract: exposing the kwarg does NOT change the default."""
    session = Session(FakeWebSocket(), EchoProvider)
    assert session._xoff_max_wait_s == 5.0
    assert session._finish_max_wait_s == 5.0
    assert session._provider_start_timeout_s == 15.0
    assert session._provider_close_timeout_s == 5.0
    assert session._audio_in.maxsize == 50


async def test_finish_max_wait_configurable():
    """With the wait shortened, finish() hangs up fast when the queue-empty
    notice never arrives (with the default it would take 5 s)."""
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, finish_max_wait_s=0.05)
    await session._on_control(media_start())
    await asyncio.sleep(0)

    await asyncio.wait_for(session.finish(), timeout=1.0)

    assert any(c.get("command") == "HANGUP" for c in ws.commands)


async def test_provider_close_timeout_configurable():
    """A hung close() is abandoned at the configured limit: the whole session
    ends fast (with the default it would take 5 s)."""
    class CloseHangs:
        def __init__(self, session): pass
        async def start(self): pass
        async def send_audio(self, chunk): pass
        async def on_dtmf(self, digit): pass
        async def close(self): await asyncio.sleep(60)

    ws = FakeWebSocket([media_start()])
    session = Session(ws, CloseHangs, provider_close_timeout_s=0.05)
    await asyncio.wait_for(session.run(), timeout=2.0)


async def test_provider_start_timeout_configurable():
    """A start() that does not return is cut off at the configured limit and
    the call is hung up (with the default it would take 15 s)."""
    class Open(FakeWebSocket):
        def __aiter__(self):
            async def generate():
                for message in self.script:
                    yield message
                    await asyncio.sleep(0)
                await asyncio.sleep(60)   # the socket stays open
            return generate()

    class StartHangs:
        def __init__(self, session): pass
        async def start(self): await asyncio.sleep(60)
        async def send_audio(self, chunk): pass
        async def on_dtmf(self, digit): pass
        async def close(self): pass

    ws = Open([media_start()])
    session = Session(ws, StartHangs, provider_start_timeout_s=0.05)
    task = asyncio.create_task(session.run())
    try:
        await asyncio.sleep(0.5)
        assert any(c.get("command") == "HANGUP" for c in ws.commands), \
            "the hung start has to end in the call being hung up"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_audio_in_max_frames_configurable():
    session_default = Session(FakeWebSocket(), EchoProvider)
    session_short = Session(FakeWebSocket(), EchoProvider, audio_in_max_frames=2)
    assert session_default._audio_in.maxsize == 50
    assert session_short._audio_in.maxsize == 2


async def test_a_factory_that_raises_does_not_leave_the_call_mute():
    """The exact point where a badly built partial blows up: it has to exit via
    emit (fatal) and hang up, not die with a 'normal' HANGUP without a clue."""
    events = []

    def broken_factory(session):
        raise TypeError("badly built partial")

    ws = FakeWebSocket([media_start()])
    await Session(ws, broken_factory, emit=events.append).run()

    errors = [e for e in events
              if getattr(e, "type", "") == "error"
              and getattr(e, "data", {}).get("fatal")]
    assert errors, "the factory failure has to exit via emit"
    assert any(c.get("command") == "HANGUP" for c in ws.commands), \
        "and the call has to be hung up explicitly"


def test_audio_in_max_frames_invalid_raises():
    """With maxsize <= 0 the asyncio queue becomes UNLIMITED: the value that
    looks like 'retain nothing' would be the opposite. A config trap."""
    for invalid in (0, -5):
        with pytest.raises(ValueError):
            Session(FakeWebSocket(), EchoProvider,
                    audio_in_max_frames=invalid)


# ---------------------------------------------------------------------------
# The gap between answering and being able to listen
# ---------------------------------------------------------------------------
#
# Answering is what makes Asterisk start sending audio. If the provider is not
# ready by then, whatever the caller says in that window is dropped, and it
# does not look like a failure: it looks like a bot answering nonsense, because
# the provider gets the sentence already half started, its voice detector cuts
# where it should not, and the model fills in the rest.


async def test_the_audio_gate_opens_before_answering():
    """An adapter that opens the gate first loses nothing."""
    order: list[str] = []

    class Provider:
        def __init__(self, session):
            self.session = session

        async def start(self):
            self.session.accept_audio()
            order.append("gate")
            await self.session.answer()
            order.append("answer")

        async def send_audio(self, chunk):
            order.append("audio")

        async def on_dtmf(self, digit): pass
        async def close(self): pass

    ws = FakeWebSocket([media_start(), b"\xd5" * 160])
    await Session(ws, Provider).run()

    assert order[:2] == ["gate", "answer"]
    assert "audio" in order, "the caller's audio reached the provider"


async def test_an_adapter_that_does_not_open_it_still_works():
    """The Session opens it on its own when the startup finishes: an older
    adapter keeps working, it just pays for the window."""
    ws = FakeWebSocket([media_start(), b"\xd5" * 160])
    session = Session(ws, EchoProvider)

    await session.run()

    assert session._provider_ready.is_set()
    assert len(ws.audio) == 1, "the frame after the startup did come back"


async def test_opening_it_twice_does_not_duplicate_the_delivery():
    """It is idempotent because both call it: the adapter up front and the
    Session as a fallback. Two delivery tasks would send every frame twice."""
    ws = FakeWebSocket([media_start(), b"\xd5" * 160])
    session = Session(ws, EchoProvider)

    session._media = None
    await session.run()

    assert len(ws.audio) == 1, "one frame in, one frame out"


async def test_the_pause_tells_the_speaking_turn_the_clock_broke():
    """`pause()` stops the audio and not the time, so from there the library
    cannot say how much was heard, and it says so.

    It matters because that number feeds OpenAI's truncate, whose server rejects
    a value past the real audio and publishes an `error` to the client. A
    contaminated clock would turn a silent defect into a false alarm in the
    integrator's panel, so the pause has to reach the speaking turn.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)
    await session._on_control(media_start())
    speech = session.speech

    await speech.play(b"\xd5" * 1600)
    assert speech.played_ms_at_most is not None, "before the pause it answers"

    await session.pause()

    assert speech.played_ms_at_most is None, (
        "the pause did not reach the speaking turn: the clock keeps measuring "
        "audio that stopped playing")
    assert any(c["command"] == "PAUSE_MEDIA" for c in ws.commands)


async def test_send_audio_says_whether_the_frame_actually_left():
    """The return value is load-bearing, not decoration.

    `play()` cuts the whole block on the first False, because the XOFF budget is
    paid per frame; and `SpeechState` only raises `_audio_pending` when a frame
    really left, so it does not wait for a queue-empty notice for audio that was
    never queued. A `send_audio` that answered True after discarding would make
    the bot look like it is sounding while nothing was sent.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, xoff_max_wait_s=0.05)
    await session._on_control(media_start())

    assert await session.send_audio(b"\xd5" * 160) is True, (
        "with the queue open the frame does leave")

    await session._on_control(json.dumps({"event": "MEDIA_XOFF"}))
    assert await session.send_audio(b"\xd5" * 160) is False, (
        "it reported the discarded frame as sent: the state ends up lying "
        "about what reached Asterisk")


async def test_a_discarded_frame_does_not_leave_audio_queued_in_asterisk():
    """The two halves together: the channel discards and the turn finds out.

    `speaking` DOES stay True, and that is correct: the provider is generating
    the turn, which is a different fact from Asterisk playing it. What must not
    stay up is the "there is audio queued" half, because that one only comes
    down with a QUEUE_DRAINED, and Asterisk has no reason to send one for audio
    that never got queued. Getting it wrong leaves the state stuck for the rest
    of the call: the abandonment meter never starts and `finish()` burns its
    whole budget.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, xoff_max_wait_s=0.05)
    await session._on_control(media_start())
    speech = session.speech

    await session._on_control(json.dumps({"event": "MEDIA_XOFF"}))
    await speech.play(b"\xd5" * 1600)        # ten frames, none of them leave

    assert not ws.audio, "with XOFF active nothing reaches the channel"
    assert speech.speaking, "the provider IS generating: that half is true"
    assert not speech._audio_pending, (
        "it is waiting for a queue-empty notice for audio that was never "
        "queued, and that flag has no other way down")

    # And the close does not ask the channel for a notice that cannot arrive.
    drain_before = [c for c in ws.commands
                    if c.get("command") == "REPORT_QUEUE_DRAINED"]
    await speech.end_turn()
    drain_after = [c for c in ws.commands
                   if c.get("command") == "REPORT_QUEUE_DRAINED"]
    assert len(drain_after) == len(drain_before), (
        "it asked for a queue-empty notice with nothing queued")


async def test_finish_hangs_up_straight_away_in_passthrough():
    """In passthrough the channel REJECTS the queue-empty notice, so waiting
    for it is silence for whoever is on the line.

    The channel turns passthrough on by itself with a small-frame codec
    (opus/g729/speex), signalled by `optimal_frame_size` at zero, and there it
    rejects 8 of its 11 commands. Without this guard `finish()` would ask for a
    notice that can never arrive and burn its whole budget before hanging up,
    with the caller hearing nothing.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, finish_max_wait_s=5.0)
    await session._on_control(media_start(optimal_frame_size=0, format="opus"))

    started = asyncio.get_running_loop().time()
    await asyncio.wait_for(session.finish(), timeout=1.0)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.5, (
        f"it waited {elapsed:.2f}s for a notice the channel rejects")
    commands = [c["command"] for c in ws.commands]
    assert "REPORT_QUEUE_DRAINED" not in commands, (
        "it asked for the notice in passthrough: the channel answers ERROR "
        "and the wait runs out in silence")
    assert commands[-1] == "HANGUP"


async def test_finish_skips_the_notice_when_the_goodbye_already_played():
    """The notice is skipped only when the goodbye REALLY played out.

    Careful with the mirror case, which looks the same and is not: a turn that
    drained before `finish()` was even called does NOT count, because the
    goodbye may not exist yet. There the notice is still asked for on purpose,
    and that is the fix for a measured real call. What this pins is the other
    branch: the bot starts its goodbye after `finish()`, plays it out, and then
    there is genuinely nothing left to wait for.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider, finish_max_wait_s=2.0)
    await session._on_control(media_start())
    speech = session.speech

    async def the_goodbye():
        await asyncio.sleep(0.05)
        await speech.play(b"\xd5" * 160)          # the goodbye starts
        await asyncio.sleep(0.05)
        await speech.end_turn()
        await session._on_control(json.dumps({"event": "QUEUE_DRAINED"}))

    started = asyncio.get_running_loop().time()
    await asyncio.gather(session.finish(), the_goodbye())
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 1.5, (
        f"it burned {elapsed:.2f}s: with the goodbye already played out there "
        "is nothing left to wait for")
    assert ws.commands[-1]["command"] == "HANGUP"


# ---------------------------------------------------------------------------
# Passthrough the MEDIA_START cannot announce
# ---------------------------------------------------------------------------


async def test_a_p_option_dial_is_detected_from_the_channel_error(caplog):
    """`p()` with G.711 is invisible in MEDIA_START: `optimal_frame_size` stays
    160 (Asterisk 23.4.1, `chan_websocket.c:1633` sets the flag, only
    `:1406-1408` zeroes the size). The first rejected command names it, once,
    and the session stops asking for what the channel refuses."""
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)
    await session._on_control(media_start(format="alaw", optimal_frame_size=160))

    # The exact text the channel writes (`chan_websocket.c:681`,
    # `"%s not supported in passthrough mode"`), as the JSON event of `:359-364`.
    rejected = {"event": "ERROR", "channel_id": "c1",
                "error_text": "REPORT_QUEUE_DRAINED not supported in passthrough mode"}
    with caplog.at_level("DEBUG"):
        await session._on_control(json.dumps(rejected))
        await session._on_control(json.dumps(rejected))

    warnings = [r for r in caplog.records
                if r.levelname == "WARNING" and "p()" in r.getMessage()]
    assert len(warnings) == 1, "one warning for the call, naming the p() option"
    assert "c(alaw)" in warnings[0].getMessage(), "it tells the Dial to use"

    # And from here the session knows: no more drain requests the channel rejects.
    before = len(ws.commands)
    await session.report_bot_audio_drained()
    assert len(ws.commands) == before, "REPORT_QUEUE_DRAINED sent into passthrough"


async def test_passthrough_closes_the_bot_turn_without_the_notice():
    """With `p()` the bot's turn still comes down at the end of the turn.

    The channel rejects REPORT_QUEUE_DRAINED there, and nobody else lowers
    `_audio_pending`: `bot_audio_playing` stayed True for the rest of the
    call, so the caller-silence clock never started and OpenAI's truncate
    guard let a truncate out on every turn. Now the notice is given at once
    on `end_turn`, which means "finished generating", not "finished
    sounding"; in that mode nothing depends on the difference.
    """
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)
    await session._on_control(media_start(format="alaw", optimal_frame_size=160))
    await session._on_control(json.dumps({
        "event": "ERROR", "channel_id": "c1",
        "error_text": "REPORT_QUEUE_DRAINED not supported in passthrough mode"}))
    speech = session.speech

    await speech.play(b"\xd5" * 160)
    assert speech.bot_audio_playing
    await speech.end_turn()

    assert not speech.bot_audio_playing, (
        "bot_audio_playing stays True for the rest of the call: the channel "
        "will never send the notice it rejects")
    assert speech.caller_quiet_for is not None, "the caller-silence clock did not start"
    commands = [c["command"] for c in ws.commands]
    assert "REPORT_QUEUE_DRAINED" not in commands


async def test_an_ordinary_channel_error_is_still_an_error(caplog):
    ws = FakeWebSocket()
    session = Session(ws, EchoProvider)
    await session._on_control(media_start())

    with caplog.at_level("ERROR"):
        await session._on_control(json.dumps({
            "event": "ERROR", "channel_id": "c1", "error_text": "something else"}))

    assert [r.levelname for r in caplog.records] == ["ERROR"]
    before = len(ws.commands)
    await session.report_bot_audio_drained()
    assert len(ws.commands) == before + 1, "an unrelated error must not imply passthrough"
