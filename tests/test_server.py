"""
Server tests: call admission.

They bring up a real serve() on an ephemeral port and connect with the
`websockets` client, so they exercise the full handshake path.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest
import websockets

from galcymedia import serve


class NullProvider:
    def __init__(self, session):
        pass

    async def start(self): pass
    async def send_audio(self, chunk): pass
    async def on_dtmf(self, digit): pass
    async def close(self): pass


async def test_max_calls_rejects_the_excess_with_1013():
    """The admission cap closes the surplus connection immediately.

    Without a limit, the excess does not fail: it degrades the audio of every
    call at once, which is what an operator cannot diagnose.
    """
    stop = asyncio.get_running_loop().create_future()
    server = asyncio.ensure_future(serve(
        NullProvider, host="127.0.0.1", port=18765, max_calls=1, stop=stop,
    ))
    await asyncio.sleep(0.1)

    try:
        # The first call goes in and stays open.
        first = await websockets.connect(
            "ws://127.0.0.1:18765", subprotocols=["media"],
        )

        # The second has to be rejected with 1013 (try again later).
        second = await websockets.connect(
            "ws://127.0.0.1:18765", subprotocols=["media"],
        )
        with contextlib.suppress(websockets.ConnectionClosed):
            await asyncio.wait_for(second.recv(), timeout=2.0)
        assert second.close_code == 1013

        # Hanging up the first frees up room again.
        await first.close()
        await asyncio.sleep(0.1)
        third = await websockets.connect(
            "ws://127.0.0.1:18765", subprotocols=["media"],
        )
        assert third.close_code is None
        await third.close()
    finally:
        stop.set_result(None)
        await asyncio.wait_for(server, timeout=5.0)


# ---------------------------------------------------------------------------
# The chameleon class: async with, close(), and the AGI passthrough
# ---------------------------------------------------------------------------


async def test_async_with_serves_and_closes_on_exit():
    """The new form: `async with serve(...)` accepts calls inside the block and
    releases the port on exit."""
    async with serve(NullProvider, host="127.0.0.1", port=18766) as server:
        ws = await websockets.connect(
            "ws://127.0.0.1:18766", subprotocols=["media"])
        await ws.close()
        # The server handler finishes a touch after the client close: we wait
        # for the call to be decremented, with a limit.
        for _ in range(50):
            if server.active_calls == 0:
                break
            await asyncio.sleep(0.02)
        assert server.active_calls == 0

    # Outside the block the server is gone: the port rejects.
    with pytest.raises(OSError):
        await websockets.connect(
            "ws://127.0.0.1:18766", subprotocols=["media"], open_timeout=2)


async def test_close_wakes_the_in_progress_await():
    """`await serve(...)` runs forever, unless someone holding the reference
    closes it: close() wakes it and it finishes cleanly."""
    server = serve(NullProvider, host="127.0.0.1", port=18767)
    task = asyncio.ensure_future(server)
    await asyncio.sleep(0.2)

    await server.close()
    await asyncio.wait_for(task, timeout=2.0)


async def test_await_serve_still_respects_stop():
    """The usual form does not change: the `stop` future ends the server."""
    stop = asyncio.get_running_loop().create_future()
    task = asyncio.ensure_future(
        serve(NullProvider, host="127.0.0.1", port=18768, stop=stop))
    await asyncio.sleep(0.2)

    stop.set_result(None)
    await asyncio.wait_for(task, timeout=2.0)


async def test_serve_passes_the_timeouts_to_the_agi():
    """The agi_* kwargs reach the internal AgiServer: without this, tuning the
    AGI meant not using serve()."""
    async def handler(request):
        pass

    async with serve(NullProvider, host="127.0.0.1", port=18769,
                     agi_handler=handler, agi_port=45731,
                     agi_handler_timeout_s=1.5,
                     agi_env_timeout_s=0.7) as server:
        assert server._agi is not None
        assert server._agi._handler_timeout_s == 1.5
        assert server._agi._env_timeout_s == 0.7


async def test_transfers_mounts_after_dial_and_hangup():
    """`serve(transfers=...)` mounts the two AGI routes under the names the
    integrator writes in extensions.conf: `/after-dial` for the priority
    after the Dial, `/hangup` for the hangup handler. The names say when
    each one runs; a rename here is a breaking change for every dialplan."""
    from galcymedia import Transfers

    transfers = Transfers()
    async with serve(NullProvider, host="127.0.0.1", port=18770,
                     transfers=transfers, agi_port=45734) as server:
        routes = server._agi._handler._routes
        assert routes == {"/after-dial": transfers.handler,
                          "/hangup": transfers.closing_handler}


# ---------------------------------------------------------------------------
# Audit regressions: the chameleon's state cannot lie
# ---------------------------------------------------------------------------


async def test_a_closed_serve_does_not_rearm():
    """close() before starting left a zombie: a later async with would bring up
    a server that no future close() could reach."""
    server = serve(NullProvider, host="127.0.0.1", port=18770)
    await server.close()

    with pytest.raises(RuntimeError):
        async with server:
            pass


async def test_reusing_the_async_with_after_closing_raises():
    """The second async with used to enter 'successfully' with a dead server:
    the worst failure for a call server is the silent one."""
    server = serve(NullProvider, host="127.0.0.1", port=18771)
    async with server:
        pass

    with pytest.raises(RuntimeError):
        async with server:
            pass

    # And a late await does not hang either: it raises just as loudly.
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(server, timeout=2.0)


async def test_a_failed_bind_does_not_leave_the_agi_orphaned(caplog):
    """If the WebSocket port is busy, the just-opened AgiServer has to close:
    it used to stay listening in a process that thought serve() failed
    cleanly. And the log has to say HOW to find the one holding the port, not
    just the raw traceback."""
    async def handler(request):
        pass

    async with serve(NullProvider, host="127.0.0.1", port=18772):
        # Port 18772 is already taken: this second serve has to fail on bind
        # and clean up its AGI on port 45732.
        with caplog.at_level("ERROR"), pytest.raises(OSError):
            await serve(NullProvider, host="127.0.0.1", port=18772,
                        agi_handler=handler, agi_port=45732)
        assert any("ANOTHER" in r.message and "netstat" in r.message
                   for r in caplog.records), \
            "the failed bind has to surface the diagnostic hint"

    reader = None
    try:
        with pytest.raises(OSError):
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", 45732)
    finally:
        if reader is not None:
            writer.close()


async def test_a_failed_agi_bind_kills_the_instance_and_names_the_port(caplog):
    """The AGI bind gets the same treatment as the WebSocket one.

    Measured before: the failure left `_started` True with no server behind
    it, so a second `async with` entered in silence and the process thought
    it was serving. Now the instance is dead (a second start raises) and the
    log names the AGI port with the hint to find who holds it.
    """
    async def handler(request):
        pass

    blocker = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 45733)
    try:
        server = serve(NullProvider, host="127.0.0.1", port=18776,
                       agi_handler=handler, agi_port=45733)
        with caplog.at_level("ERROR"), pytest.raises(OSError):
            async with server:
                pass
        assert any("agi://127.0.0.1:45733" in r.message and "netstat" in r.message
                   for r in caplog.records), "the AGI bind has to name its port"

        with pytest.raises(RuntimeError):
            async with server:
                pass
    finally:
        blocker.close()
        await blocker.wait_closed()

    # The WebSocket port was never taken: nothing is left listening.
    with pytest.raises(OSError):
        await websockets.connect(
            "ws://127.0.0.1:18776", subprotocols=["media"], open_timeout=2)


async def test_cancelling_the_stop_future_closes_without_cancelling_the_caller():
    """stop.cancel() is another way to ask for the close: the await finishes
    normally, without a CancelledError toward a caller nobody cancelled."""
    stop = asyncio.get_running_loop().create_future()
    task = asyncio.ensure_future(
        serve(NullProvider, host="127.0.0.1", port=18773, stop=stop))
    await asyncio.sleep(0.2)

    stop.cancel()
    await asyncio.wait_for(task, timeout=2.0)   # no exception


async def test_close_is_synchronous_for_the_coroutine_protocol():
    """asyncio calls `close()` without awaiting on a coroutine it discards
    before starting it (`tasks.py:751`, `taskgroups.py:187-193`). Measured
    before: that returned a coroutine nobody awaited (RuntimeWarning) and the
    inner `_run()` stayed in CORO_CREATED. Now the inner coroutine closes and
    nothing warns; `await server.close()` keeps working (the other tests).
    """
    import gc
    import inspect
    import warnings

    server = serve(NullProvider, host="127.0.0.1", port=18777)
    inner = server._main()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        server.close()                        # as asyncio does: no await
        gc.collect()

    assert inspect.getcoroutinestate(inner) == inspect.CORO_CLOSED
    assert not [w for w in caught if issubclass(w.category, RuntimeWarning)], \
        [str(w.message) for w in caught]


async def test_the_close_awaitable_is_harmless_once_awaited_or_already_closed():
    """`close()` returns a wrapper whose `__del__` closes the inner coroutine
    when nobody awaited it. Two things have to hold: after `await` the
    wrapper owns nothing and the GC does nothing; and if the coroutine was
    closed by another path, `__del__` does not raise.
    """
    import gc
    import warnings

    from galcymedia.server import _Close

    server = serve(NullProvider, host="127.0.0.1", port=18779)
    async with server:
        pass                                  # __aexit__ awaited close()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        closer = server.close()               # idempotent path
        await closer                          # wrapper hands the coro over
        del closer
        gc.collect()
    assert not caught, [str(w.message) for w in caught]

    async def noop():
        pass

    coro = noop()
    wrapper = _Close(coro)
    coro.close()                              # closed by another path
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        del wrapper                           # __del__ on a closed coroutine
        gc.collect()
    assert not caught, [str(w.message) for w in caught]


async def test_serve_is_a_coroutine_for_asyncio():
    """`asyncio.run(serve(MyAgent))` is the README hello-world, and
    asyncio.run/create_task only accept coroutines: the class implements the
    structural protocol (send/throw/close + __await__) via the same mechanism
    asyncio uses to run Cython coroutines."""
    import collections.abc

    server = serve(NullProvider, host="127.0.0.1", port=18774)
    assert isinstance(server, collections.abc.Coroutine)
    assert asyncio.iscoroutine(server)

    # And it really runs as a native Task: create_task + cancel closes cleanly.
    task = asyncio.create_task(server)
    await asyncio.sleep(0.2)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", 18774)


async def test_a_peer_that_stops_answering_pings_is_hung_up_with_1011():
    """A dead peer (Asterisk killed, packets dropped) sends nothing and
    answers nothing. Measured in the lab without pings: the Session lived
    until the TCP retransmit timeout. With the keepalive, a missed PONG sends
    1011 "keepalive ping timeout" on the wire; since the dead peer never
    answers the close, the Session sees 1006 (abnormal) and reports it as an
    abnormal hangup.

    The client is hand-rolled because `websockets` answers PINGs on its own.
    """
    import base64

    seen = []
    async with serve(NullProvider, host="127.0.0.1", port=18778,
                     ping_interval_s=0.1, ping_timeout_s=0.1,
                     on_event=lambda name, payload: seen.append((name, payload)),
                     ) as server:
        reader, writer = await asyncio.open_connection("127.0.0.1", 18778)
        key = base64.b64encode(b"0123456789abcdef").decode()
        writer.write(
            f"GET / HTTP/1.1\r\nHost: 127.0.0.1:18778\r\n"
            f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
            f"Sec-WebSocket-Protocol: media\r\n\r\n".encode())
        await writer.drain()
        status = await reader.readline()
        assert status.startswith(b"HTTP/1.1 101"), status

        # Never answer the PING: the server has to give up on its own.
        wire = await asyncio.wait_for(reader.read(), timeout=3.0)
        writer.close()
        # Close frame, code 1011 = 0x03F3, with websockets' reason.
        assert b"\x03\xf3keepalive ping timeout" in wire, wire
        for _ in range(50):
            if server.active_calls == 0:
                break
            await asyncio.sleep(0.02)
        assert server.active_calls == 0

    hangups = [p for n, p in seen if n == "HANGUP"]
    assert hangups and hangups[0]["code"] == 1006, hangups
    assert hangups[0]["normal"] is False


# ---------------------------------------------------------------------------
# on_call: the hook that prepares the session before anything runs
# ---------------------------------------------------------------------------
#
# It is a hook that DECIDES, not one that watches, so it is allowed to bring
# the call down: a session that could not be set up should not answer. What
# has to hold is that the end is reported like any other, not swallowed.


async def test_a_failing_on_call_drops_the_call_with_1011_and_a_hangup(caplog):
    """It runs BEFORE the reading loop, so no provider exists to close.

    Measured before: the exception propagated to `websockets`, which logged
    it and closed with 1011, but neither the peephole nor `emit` saw a HANGUP:
    a panel kept waiting for a call that was already gone. Now the server
    logs it, closes with 1011 and sends the synthetic HANGUP.
    """
    created = []
    seen = []

    class Provider:
        def __init__(self, session):
            created.append(self)

        async def start(self): pass
        async def send_audio(self, chunk): pass
        async def on_dtmf(self, digit): pass
        async def close(self): pass

    async def explodes(session):
        raise RuntimeError("the integrator's setup has a bug")

    async with serve(Provider, host="127.0.0.1", port=18775,
                     on_call=explodes,
                     on_event=lambda name, payload: seen.append((name, payload)),
                     emit=lambda event: seen.append(("emit", event.type.value)),
                     ) as server:
        with caplog.at_level("ERROR"):
            ws = await websockets.connect(
                "ws://127.0.0.1:18775", subprotocols=["media"])
            with contextlib.suppress(websockets.ConnectionClosed):
                await asyncio.wait_for(ws.recv(), timeout=2.0)
            await asyncio.sleep(0.1)

        assert ws.close_code == 1011
        assert created == [], "no provider was created, so none was left open"
        assert server.active_calls == 0, "the counter goes back down"

    hangups = [p for n, p in seen if n == "HANGUP"]
    assert hangups and hangups[0]["code"] == 1011 and hangups[0]["normal"] is False
    assert ("emit", "server-message") in seen, "the RTVI client sees the end too"
    assert any("setup has a bug" in r.message or "setup has a bug" in str(r.exc_info)
               for r in caplog.records), "the failure is logged with its traceback"


async def test_connect_refuses_transfers_because_it_has_no_agi():
    """A combination that looks like it works and cannot.

    `connect()` does not bring up the FastAGI, so the bot would note a transfer
    that no dialplan can ever ask for: a call that says it transfers and hangs
    up instead. Failing at startup beats failing in production.
    """
    from galcymedia import Transfers, connect

    with pytest.raises(ValueError, match="does not open the FastAGI"):
        await connect("ws://127.0.0.1:1/media/x", NullProvider,
                      transfers=Transfers())
