"""
Hostile harness for PROVIDERS: badly written or malicious adapters.

test_hostile.py attacks the channel protocol (Asterisk -> library). This one
attacks the provider contract (library <-> adapter): an integrator who writes
their adapter badly, or a malicious one, cannot take down the process or affect
OTHER concurrent calls. It is the gateway's central guarantee: a single process
serves all calls, so the failure of one has to stay contained in that one.

Each test brings up a real `serve()`, connects one or several calls with
hostile adapters, and verifies that the rest of the system is still alive.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets

from galcymedia import serve


def media_start(**extra):
    base = {"event": "MEDIA_START", "connection_id": "c", "channel_id": "c1",
            "channel": "PJSIP/1-1", "format": "alaw",
            "optimal_frame_size": 160, "ptime": 20, "channel_variables": {}}
    base.update(extra)
    return json.dumps(base)


async def _serve_on(port, factory, **kw):
    stop = asyncio.get_running_loop().create_future()
    # serve() implements the coroutine protocol, so create_task and
    # ensure_future work the same; ensure_future is used out of habit.
    task = asyncio.ensure_future(
        serve(factory, host="127.0.0.1", port=port, stop=stop, **kw))
    await _wait_until_listening(port, task)
    return stop, task


async def _wait_until_listening(port, task, timeout=10.0):
    """Waits for the port to accept, instead of guessing how long it takes.

    Same reason as the twin in `test_hostile.py`: `asyncio.sleep(0.15)` is a
    race with a stopwatch, and on a loaded runner it loses.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if task.done():
            await task          # re-raises the bind failure with its message
            raise RuntimeError("serve() ended before accepting connections")
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
        except OSError:
            if asyncio.get_running_loop().time() > deadline:
                raise
            await asyncio.sleep(0.01)
            continue
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
        return


async def _shutdown(stop, task, timeout=6.0):
    stop.set_result(None)
    await asyncio.wait_for(task, timeout=timeout)


class Sane:
    """A sane reference provider, for the call that must NOT break."""
    live = 0

    def __init__(self, session):
        self.session = session
        self.started = False
        self.audio = 0
        Sane.live += 1

    async def start(self):
        self.started = True

    async def send_audio(self, chunk):
        self.audio += 1

    async def on_dtmf(self, digit):
        pass

    async def close(self):
        Sane.live -= 1


@pytest.fixture(autouse=True)
def _reset():
    Sane.live = 0
    yield


# ---------------------------------------------------------------------------
# hostile start()
# ---------------------------------------------------------------------------

async def test_a_start_that_raises_does_not_leave_the_call_open():
    """An adapter that blows up on connect (bad key, network down): the call
    is hung up cleanly, no mute channel is left behind."""
    class Boom:
        def __init__(self, session): self.session = session
        async def start(self): raise RuntimeError("could not connect")
        async def send_audio(self, c): pass
        async def on_dtmf(self, d): pass
        async def close(self): pass

    stop, task = await _serve_on(19001, Boom)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:19001", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            # The library sends a HANGUP to the channel when start() blows up.
            got_hangup = False
            try:
                for _ in range(10):
                    msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
                    if isinstance(msg, str) and "HANGUP" in msg:
                        got_hangup = True
                        break
            except (asyncio.TimeoutError, websockets.ConnectionClosed):
                pass
            assert got_hangup, "the library must hang up when start() blows up"
    finally:
        await _shutdown(stop, task)


async def test_a_hung_start_does_not_freeze_other_calls():
    """THE CENTRAL TEST: a start() that never returns (a provider that hangs
    while connecting) can NOT prevent another call from coming in and working.
    A single process serves them all: the hang of one stays contained in that
    one."""
    the_sane_one_came_in = asyncio.Event()

    class Hangs:
        def __init__(self, session): pass
        async def start(self):
            await asyncio.sleep(3600)          # hung forever
        async def send_audio(self, c): pass
        async def on_dtmf(self, d): pass
        async def close(self): pass

    def factory(session):
        # The first call uses the hung provider; the second, the sane one.
        if session.media.channel_id == "hung":
            return Hangs(session)
        p = Sane(session)
        the_sane_one_came_in.set()
        return p

    stop, task = await _serve_on(19002, factory)
    try:
        # Call 1: hangs in start().
        ws1 = await websockets.connect(
            "ws://127.0.0.1:19002", subprotocols=["media"])
        await ws1.send(media_start(channel_id="hung"))
        await asyncio.sleep(0.2)

        # Call 2: has to come in and start despite the hung call 1.
        async with websockets.connect(
                "ws://127.0.0.1:19002", subprotocols=["media"]) as ws2:
            await ws2.send(media_start(channel_id="sane"))
            await asyncio.wait_for(the_sane_one_came_in.wait(), timeout=2.0)
            # And the sane one processes audio while the other is still hung.
            await ws2.send(b"\xd5" * 160)
            await asyncio.sleep(0.2)

        await ws1.close()
    finally:
        # Fast shutdown despite call 1 hung in start(): the start runs in a
        # separate task, so the reader loop detects the socket close and
        # `_cleanup` cancels the hung start. Without that, shutdown waited the
        # 15 s of START_MAX_WAIT_S. The short timeout verifies it.
        await _shutdown(stop, task, timeout=3.0)


# ---------------------------------------------------------------------------
# hostile send_audio()
# ---------------------------------------------------------------------------

async def test_a_send_audio_that_raises_on_every_frame_does_not_take_down_the_call():
    """A send_audio broken on every frame: the failure is logged but the call
    and the process go on."""
    class BoomAudio(Sane):
        async def send_audio(self, chunk):
            raise RuntimeError("send_audio broken")

    stop, task = await _serve_on(19003, BoomAudio)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:19003", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.1)
            for _ in range(10):
                await ws.send(b"\xd5" * 160)
            await asyncio.sleep(0.2)
        # The process goes on: another call comes in.
        async with websockets.connect(
                "ws://127.0.0.1:19003", subprotocols=["media"]) as ws2:
            await ws2.send(media_start(channel_id="c2"))
            await asyncio.sleep(0.15)
    finally:
        await _shutdown(stop, task)


async def test_a_hung_send_audio_does_not_block_the_server_shutdown():
    """A send_audio that never returns cannot block the orderly shutdown.

    The caller's audio is delivered in a task separate from the reader loop: a
    hung send_audio jams that task, but the loop keeps reading the socket and
    `_cleanup` cancels it on close. Without that decoupling, shutdown hung
    INDEFINITELY (there is no per-frame timeout for real-time audio).
    """
    class HangsAudio(Sane):
        async def send_audio(self, chunk):
            await asyncio.sleep(3600)

    stop, task = await _serve_on(19009, HangsAudio)
    ws = await websockets.connect(
        "ws://127.0.0.1:19009", subprotocols=["media"])
    await ws.send(media_start())
    await asyncio.sleep(0.15)
    await ws.send(b"\xd5" * 160)                # hangs the send_audio
    await asyncio.sleep(0.15)
    await ws.close()
    # Shutdown has to complete fast despite the hung send_audio. The short
    # timeout of _shutdown is the proof: if shutdown waited for the send_audio,
    # it would give TimeoutError.
    await _shutdown(stop, task, timeout=3.0)


async def test_a_hung_send_audio_does_not_freeze_other_calls():
    """A send_audio that never returns freezes the READER LOOP of ITS call (it
    cannot read more frames of it), but NOT that of others."""
    sane_processed_audio = asyncio.Event()

    class HangsAudio:
        def __init__(self, session): self.session = session
        async def start(self): pass
        async def send_audio(self, chunk):
            await asyncio.sleep(3600)
        async def on_dtmf(self, d): pass
        async def close(self): pass

    class SaneSignal(Sane):
        async def send_audio(self, chunk):
            self.audio += 1
            sane_processed_audio.set()

    def factory(session):
        return HangsAudio(session) if session.media.channel_id == "hung" \
            else SaneSignal(session)

    stop, task = await _serve_on(19004, factory)
    try:
        ws1 = await websockets.connect(
            "ws://127.0.0.1:19004", subprotocols=["media"])
        await ws1.send(media_start(channel_id="hung"))
        await asyncio.sleep(0.1)
        await ws1.send(b"\xd5" * 160)              # hangs call 1's send_audio
        await asyncio.sleep(0.1)

        async with websockets.connect(
                "ws://127.0.0.1:19004", subprotocols=["media"]) as ws2:
            await ws2.send(media_start(channel_id="sane"))
            await asyncio.sleep(0.1)
            await ws2.send(b"\xd5" * 160)
            await asyncio.wait_for(sane_processed_audio.wait(), timeout=2.0)
        await ws1.close()
    finally:
        await _shutdown(stop, task)


# ---------------------------------------------------------------------------
# hostile close()
# ---------------------------------------------------------------------------

async def test_a_close_that_raises_does_not_prevent_closing_the_session():
    """A close() that blows up: the rest of the cleanup runs anyway and the
    process goes on."""
    class BoomClose(Sane):
        async def close(self):
            raise RuntimeError("close broken")

    stop, task = await _serve_on(19005, BoomClose)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:19005", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.15)
        await asyncio.sleep(0.2)
        # The process survived the broken close: another call comes in.
        async with websockets.connect(
                "ws://127.0.0.1:19005", subprotocols=["media"]) as ws2:
            await ws2.send(media_start(channel_id="c2"))
            await asyncio.sleep(0.15)
    finally:
        await _shutdown(stop, task)


async def test_a_hung_close_does_not_block_the_server_shutdown():
    """A close() that never returns cannot prevent serve() from stopping. The
    _cleanup cancels it and waits with its own handling."""
    class HangsClose(Sane):
        async def close(self):
            await asyncio.sleep(3600)

    stop, task = await _serve_on(19006, HangsClose)
    try:
        ws = await websockets.connect(
            "ws://127.0.0.1:19006", subprotocols=["media"])
        await ws.send(media_start())
        await asyncio.sleep(0.15)
        await ws.close()
        await asyncio.sleep(0.2)
    finally:
        # If a hung close() blocked shutdown, this _shutdown would time out.
        # The shorter timeout verifies it.
        await _shutdown(stop, task, timeout=8.0)


# ---------------------------------------------------------------------------
# finish() called abusively
# ---------------------------------------------------------------------------

async def test_finish_called_from_many_tasks_at_once():
    """An adapter that calls finish() from 10 concurrent tasks: it hangs up
    only once (idempotent), not ten times."""
    class SpamFinish:
        def __init__(self, session): self.session = session
        async def start(self):
            # Fires 10 concurrent finish() as soon as it starts.
            for _ in range(10):
                asyncio.ensure_future(self.session.finish())
        async def send_audio(self, c): pass
        async def on_dtmf(self, d): pass
        async def close(self): pass

    hangups = 0
    stop, task = await _serve_on(19007, SpamFinish)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:19007", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            try:
                for _ in range(30):
                    msg = await asyncio.wait_for(ws.recv(), timeout=0.3)
                    if isinstance(msg, str) and '"command":"HANGUP"' in msg:
                        hangups += 1
            except (asyncio.TimeoutError, websockets.ConnectionClosed):
                pass
        # finish() is idempotent: a single HANGUP even if called 10 times.
        assert hangups <= 1, f"it hung up {hangups} times, must be 1"
    finally:
        await _shutdown(stop, task)


# ---------------------------------------------------------------------------
# Many concurrent hostile calls: the failure of some does not affect others
# ---------------------------------------------------------------------------

async def test_many_calls_with_broken_providers_do_not_affect_the_sane_ones():
    """Half the calls have providers that blow up in start(); the other half
    are sane. The sane ones have to ALL start despite the broken ones living in
    the same process."""
    started = []

    class Boom:
        def __init__(self, session): pass
        async def start(self): raise RuntimeError("broken")
        async def send_audio(self, c): pass
        async def on_dtmf(self, d): pass
        async def close(self): pass

    class SaneCount(Sane):
        async def start(self):
            await super().start()
            started.append(self.session.media.channel_id)

    def factory(session):
        if session.media.channel_id.startswith("broken"):
            return Boom(session)
        return SaneCount(session)

    stop, task = await _serve_on(19008, factory)
    try:
        async def call(cid):
            try:
                async with websockets.connect(
                        "ws://127.0.0.1:19008", subprotocols=["media"]) as ws:
                    await ws.send(media_start(channel_id=cid))
                    await asyncio.sleep(0.3)
            except Exception:
                pass

        calls = []
        expected = set()
        for i in range(20):
            if i % 2 == 0:
                cid = f"broken{i}"
            else:
                cid = f"sane{i}"
                expected.add(cid)
            calls.append(call(cid))
        await asyncio.gather(*calls)
        await asyncio.sleep(0.3)

        # The 10 sane ones ran their start() for real, not just "did not crash".
        assert set(started) == expected
    finally:
        await _shutdown(stop, task)


# ---------------------------------------------------------------------------
# hostile DTMF: the library is the border. Only a real digit reaches the
# adapter, never garbage that its on_dtmf would index, compare or put into a
# model prompt.
# ---------------------------------------------------------------------------


def dtmf(digit):
    return json.dumps({"event": "DTMF_END", "digit": digit})


async def test_a_hostile_dtmf_does_not_reach_the_provider_but_a_valid_one_does():
    """A DTMF with a thousand characters, a dict, control bytes or an invalid
    digit ('Z') can NOT reach the adapter's on_dtmf raw: the library filters
    it. A real DTMF ('1', '*', '#', 'A') DOES reach it.
    """
    class Capturer(Sane):
        digits: list = []
        async def on_dtmf(self, digit):
            Capturer.digits.append(digit)

    Capturer.digits = []

    # The forwarding is off by default (the keypad carries what people do not
    # say out loud); this test is about the border filter, so it turns it on.
    stop, task = await _serve_on(19010, Capturer, forward_dtmf=True)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:19010", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.2)             # let the provider start

            # Hostile: none should cross the border.
            await ws.send(dtmf("x" * 1000))
            await ws.send(dtmf({"malicious": True}))
            await ws.send(dtmf("\x00"))
            await ws.send(dtmf("Z"))
            # Valid: all four should arrive.
            for good in ("1", "*", "#", "A"):
                await ws.send(dtmf(good))
            await asyncio.sleep(0.3)
    finally:
        await _shutdown(stop, task)

    assert Capturer.digits == ["1", "*", "#", "A"], \
        "only valid DTMF cross; the garbage is discarded before the adapter"


async def test_a_dtmf_during_a_slow_start_is_not_dispatched_too_early():
    """A DTMF that lands while start() has not finished yet can NOT reach the
    on_dtmf: the provider exists but is half-built (the STT/LLM/TTS client does
    not exist yet), and dispatching it the digit would blow up its on_dtmf and
    lose the digit in silence. It is held until the provider is ready, or
    discarded.
    """
    class SlowStart(Sane):
        received: list = []
        ready = False
        async def start(self):
            await asyncio.sleep(0.3)
            SlowStart.ready = True
        async def on_dtmf(self, digit):
            # If this runs before start() finishes, `ready` is False: exactly
            # the state the provider guard must prevent.
            SlowStart.received.append((digit, SlowStart.ready))

    SlowStart.received = []
    SlowStart.ready = False

    stop, task = await _serve_on(19011, SlowStart)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:19011", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.05)            # start() still in progress
            await ws.send(dtmf("5"))             # lands before it starts
            await asyncio.sleep(0.5)             # let the start finish
    finally:
        await _shutdown(stop, task)

    # The digit sent before start() finished was never dispatched mid-start:
    # there is no entry marked with the provider not ready.
    assert not any(not ready for _, ready in SlowStart.received), \
        "no DTMF was dispatched before the provider was ready"
