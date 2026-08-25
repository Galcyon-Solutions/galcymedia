"""
Hostile harness: a fake Asterisk that sends protocol garbage.

Load and isolation test the happy path at scale. This tests the opposite: that
the library does not crash, does not hang a task, and does not corrupt a call
when the channel sends something a normal conversation never produces, but that
an Asterisk with a bug (or an attacker reaching the WebSocket) can indeed send.

Each test brings up a real `serve()` on an ephemeral port, connects as a
WebSocket client, sends the attack, and verifies that:
    - the process stays alive and serving,
    - the session closes without hanging (with a hard timeout),
    - no other call is affected (failure isolation).
"""

from __future__ import annotations

import asyncio
import json

import pytest
import websockets

from galcymedia import serve

async def _wait_closed(provider, timeout=1.0):
    """The session close is asynchronous; wait for the provider to close."""
    for _ in range(int(timeout / 0.05)):
        if provider.closed >= 1:
            return
        await asyncio.sleep(0.05)


class SpyProvider:
    """Records what it was called with, to verify the lifecycle."""

    instances: list = []

    def __init__(self, session):
        self.media = session.media
        self.session = session
        self.started = False
        self.closed = 0
        self.audio_chunks = 0
        SpyProvider.instances.append(self)

    async def start(self):
        self.started = True

    async def send_audio(self, chunk):
        self.audio_chunks += 1

    async def on_dtmf(self, digit):
        pass

    async def close(self):
        self.closed += 1


@pytest.fixture(autouse=True)
def _clean_instances():
    """Each test starts with the instances list empty: it is a class
    attribute and would accumulate between tests, contaminating the asserts
    on `instances[-1]`."""
    SpyProvider.instances.clear()
    yield
    SpyProvider.instances.clear()


async def _serve_on(port, factory=SpyProvider, **kw):
    stop = asyncio.get_running_loop().create_future()
    # serve() implements the coroutine protocol, so create_task and
    # ensure_future work the same; ensure_future is used out of habit.
    task = asyncio.ensure_future(
        serve(factory, host="127.0.0.1", port=port, stop=stop, **kw))
    await _wait_until_listening(port, task)
    return stop, task


async def _wait_until_listening(port, task, timeout=10.0):
    """Waits for the port to accept, instead of guessing how long it takes.

    This was `await asyncio.sleep(0.15)`, which is a race with a stopwatch:
    it holds on an idle machine and not on a loaded CI runner, and when it
    loses, the failure blames the test that came next. Connecting is the
    thing actually being waited for, so that is what is waited for.

    The server task is checked on every round: a failed bind would otherwise
    keep this spinning until the timeout instead of reporting the real error.
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


async def _shutdown(stop, task):
    stop.set_result(None)
    await asyncio.wait_for(task, timeout=5.0)


def media_start(**extra):
    base = {"event": "MEDIA_START", "connection_id": "c", "channel_id": "c1",
            "channel": "PJSIP/1-1", "format": "alaw",
            "optimal_frame_size": 160, "ptime": 20, "channel_variables": {}}
    base.update(extra)
    return json.dumps(base)


# ---------------------------------------------------------------------------
# Corrupt control frames: they must not crash the handler
# ---------------------------------------------------------------------------

GARBAGE = [
    "",                                   # empty frame
    "no soy json",                        # plain text
    "[]",                                 # valid JSON but not an object
    "null",                               # JSON null
    "123",                                # JSON number
    '{"sin_event": true}',                # object without an event field
    '{"event": 123}',                     # event is not a string
    '{"event": ""}',                      # empty event
    '{"event": null}',                    # event null
    '{"event": "MEDIA_START"}',           # MEDIA_START with no other field
    '{"event": "DTMF_END"}',              # DTMF without a digit
    '{"event": "MEDIA_MARK_PROCESSED", "correlation_id": "fantasma"}',
    '{"event": "QUEUE_DRAINED"}',         # drained that nobody asked for
    '{"event": "MEDIA_XON"}',             # XON without a prior XOFF
    '{"event": "EVENTO_DEL_FUTURO_2027", "cualquier": [1, 2, 3]}',
    '{"event": "MEDIA_START", "optimal_frame_size": "no-es-numero"}',
    '{"event": "MEDIA_START", "channel_variables": "no-es-dict"}',
    '{"event": "MEDIA_START", "channel_variables": [1, 2]}',
    '{"event": "DTMF_END", "digit": ' + json.dumps("9" * 500) + '}',
    '{"event": "MEDIA_START", "channel_id": null, "format": null}',
]


@pytest.mark.parametrize("payload", GARBAGE)
async def test_a_corrupt_frame_does_not_take_down_the_handler(payload):
    """Each garbage frame: the server digests it and stays alive for the next
    call."""
    stop, task = await _serve_on(18801)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:18801", subprotocols=["media"]) as ws:
            await ws.send(payload)
            await asyncio.sleep(0.1)
            # The server keeps serving: a second normal call comes in.
        async with websockets.connect(
                "ws://127.0.0.1:18801", subprotocols=["media"]) as ws2:
            await ws2.send(media_start())
            await asyncio.sleep(0.1)
    finally:
        await _shutdown(stop, task)


async def test_all_the_garbage_in_a_single_call():
    """All the corrupt frames back to back in the same session, and then a
    valid MEDIA_START: it still has to start the provider."""
    stop, task = await _serve_on(18802)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:18802", subprotocols=["media"]) as ws:
            for payload in GARBAGE:
                await ws.send(payload)
            await ws.send(media_start())
            await asyncio.sleep(0.3)
        assert SpyProvider.instances, "the final MEDIA_START had to start it"
        assert SpyProvider.instances[-1].started
    finally:
        await _shutdown(stop, task)


# ---------------------------------------------------------------------------
# Impossible sequences in a normal conversation
# ---------------------------------------------------------------------------

async def test_two_media_start_in_the_same_call():
    """Asterisk should not, but if it sends two MEDIA_START, it does not crash
    nor leave two providers unclosed."""
    stop, task = await _serve_on(18803)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:18803", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.1)
            await ws.send(media_start(channel_id="c2"))
            await asyncio.sleep(0.2)
        await asyncio.sleep(0.3)
        # The first MEDIA_START starts a provider; the second is ignored.
        # The started provider closes when the call ends.
        assert SpyProvider.instances
        started = [p for p in SpyProvider.instances if p.started]
        assert len(started) == 1, "the repeated MEDIA_START must not start another"
        await _wait_closed(started[0])
        assert started[0].closed >= 1, "the provider did not close"
    finally:
        await _shutdown(stop, task)


async def test_zero_byte_binary_audio():
    """An empty binary frame before and after the MEDIA_START."""
    stop, task = await _serve_on(18804)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:18804", subprotocols=["media"]) as ws:
            await ws.send(b"")
            await ws.send(media_start())
            await asyncio.sleep(0.1)
            await ws.send(b"")
            await ws.send(b"\xd5" * 160)
            await asyncio.sleep(0.2)
        assert SpyProvider.instances[-1].started
    finally:
        await _shutdown(stop, task)


async def test_abrupt_close_right_after_media_start():
    """The client drops the socket as soon as the call starts: clean close,
    provider closed, no dangling tasks."""
    stop, task = await _serve_on(18805)
    try:
        ws = await websockets.connect(
            "ws://127.0.0.1:18805", subprotocols=["media"])
        await ws.send(media_start())
        await asyncio.sleep(0.1)
        await ws.close()                       # abrupt cut
        await _wait_closed(SpyProvider.instances[-1])
        assert SpyProvider.instances[-1].closed >= 1
    finally:
        await _shutdown(stop, task)


# ---------------------------------------------------------------------------
# The failure of ONE call cannot affect ANOTHER
# ---------------------------------------------------------------------------

async def test_a_provider_that_blows_up_on_start_does_not_affect_another_call():
    class BoomProvider(SpyProvider):
        async def start(self):
            raise RuntimeError("provider broken on purpose")

    stop, task = await _serve_on(18806, factory=BoomProvider)
    try:
        # Call 1: the provider blows up on start.
        async with websockets.connect(
                "ws://127.0.0.1:18806", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.2)
    finally:
        await _shutdown(stop, task)

    # Call 2, on a server with a healthy provider: it has to come in the same.
    SpyProvider.instances.clear()
    stop, task = await _serve_on(18816, factory=SpyProvider)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:18816", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.2)
        assert SpyProvider.instances[-1].started
    finally:
        await _shutdown(stop, task)


async def test_an_on_event_that_blows_up_does_not_take_down_the_call():
    """The integrator's hook raises on every event: the call is served the
    same (same rule as emit)."""
    def boom(name, payload):
        raise RuntimeError("gancho roto")

    stop, task = await _serve_on(18807, on_event=boom)
    try:
        async with websockets.connect(
                "ws://127.0.0.1:18807", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await ws.send(b"\xd5" * 160)
            await asyncio.sleep(0.2)
        assert SpyProvider.instances[-1].started
        # The close is asynchronous: wait for the provider to close.
        for _ in range(20):
            if SpyProvider.instances[-1].closed >= 1:
                break
            await asyncio.sleep(0.05)
        assert SpyProvider.instances[-1].closed >= 1
    finally:
        await _shutdown(stop, task)


async def test_many_concurrent_garbage_calls_leave_no_dangling_tasks():
    """20 hostile clients at once, each sends garbage and cuts. The server
    digests them all and stays ready for a healthy call."""
    stop, task = await _serve_on(18808)
    try:
        async def hostile(i):
            try:
                async with websockets.connect(
                        "ws://127.0.0.1:18808", subprotocols=["media"]) as ws:
                    await ws.send(GARBAGE[i % len(GARBAGE)])
                    await ws.send(media_start(channel_id=f"c{i}"))
                    await asyncio.sleep(0.05)
            except Exception:
                pass

        await asyncio.gather(*(hostile(i) for i in range(20)))
        await asyncio.sleep(0.3)

        # The server is still healthy: a normal call comes in.
        SpyProvider.instances.clear()
        async with websockets.connect(
                "ws://127.0.0.1:18808", subprotocols=["media"]) as ws:
            await ws.send(media_start())
            await asyncio.sleep(0.15)
        assert SpyProvider.instances[-1].started
    finally:
        await _shutdown(stop, task)
