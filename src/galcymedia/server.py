"""
The media server.

`serve()` is what almost everybody uses: it brings up a WebSocket server,
waits for Asterisk to dial, and creates one Session per call. Four lines in
your program, nothing underneath to understand.

On the direction of the connection, which confuses most people at first:

    Asterisk is the CLIENT and your application is the SERVER.

That is what simplifies deployment: no inbound port toward Asterisk, and
Asterisk brings its own configurable reconnection
(`websocket_client.conf.sample:18-21`, `reconnect_interval` and
`reconnect_attempts`).

The channel also supports the opposite direction through the special
INCOMING connection, where your application connects to Asterisk's HTTP
server: that is `connect()` below, for when Asterisk cannot reach your
machine (developing behind a NAT).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import websockets

from .session import (
    AUDIO_IN_MAX_FRAMES,
    CLOSE_MAX_WAIT_S,
    FINISH_MAX_WAIT_S,
    START_MAX_WAIT_S,
    XOFF_MAX_WAIT_S,
    Session,
)

log = logging.getLogger(__name__)

# The subprotocol `websocket_client.conf` announces with `protocols = media`;
# the channel registers it (`chan_websocket.c:2100`). Measured: a client
# that offers none, or another one, gets HTTP 400 from `websockets`.
SUBPROTOCOL = "media"

# Loopback on purpose: neither the WebSocket nor the FastAGI authenticate,
# so the safe default is not exposing them. Asterisk on the same machine
# works as is; for another interface (Docker, another host) pass
# host="0.0.0.0" explicitly.
#
# 9000 is THE galcymedia port: the library, every example and the reference
# dialplan use it, so any example-dialplan pair connects without editing.
# FastAGI uses 4573, the Asterisk convention.
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000

# Incoming message cap declared to the WebSocket server. The protocol cap is
# 65,500 bytes (`MAX_WEBSOCKET_MESSAGE_BYTES`, `protocol.py`); this is the
# next natural limit, 64 KiB exactly, so a valid message at the edge is not
# cut.
MAX_INCOMING_MESSAGE_BYTES = 65536

# Keepalive toward Asterisk, the `websockets` defaults. Asterisk answers
# every PING with a PONG (`res_http_websocket.c:877-878`), and its own pings
# are opt-in (`enable_pingpongs`, default no: `res_websocket_client.c:799`),
# so without these nothing detects a dead peer: measured in the lab with the
# packets dropped, the Session lived 15 min 56 s, the TCP retransmit timeout.
# A missed PONG sends 1011 "keepalive ping timeout"; the dead peer never
# answers the close, so the Session reports 1006, abnormal
# (`test_a_peer_that_stops_answering_pings_is_hung_up_with_1011`).
#
# Healthy calls survive load: measured with 6 cores saturated and 80 calls
# echoing, PONG latency peaks at 5 ms; with this event loop stalled 3 s
# (15x the timeout) with a PING in flight, the PONG already buffered is
# processed before the timer. The numbers and the alternative considered
# (inbound audio silence) are in `docs/decisions.md`.
PING_INTERVAL_S = 20.0
PING_TIMEOUT_S = 20.0


ProviderFactory = Callable[..., Any]
SessionHook = Callable[[Session], Awaitable[None]]


class _Close:
    """What `serve.close()` returns: awaitable, and silent if nobody awaits.

    A plain coroutine left unawaited warns; asyncio drops the result of
    `close()` on the coroutine-protocol path, so this wrapper closes the
    inner coroutine instead of warning when it is collected.
    """

    def __init__(self, coro: Any) -> None:
        self._coro = coro

    def __await__(self) -> Any:
        coro, self._coro = self._coro, None
        return coro.__await__()

    def __del__(self) -> None:
        if self._coro is not None:
            self._coro.close()


class serve:
    """Serves calls until asked to stop.

    Awaitable or context manager, like `websockets.serve` (why both:
    `docs/decisions.md`):

        asyncio.run(serve(MyAgent))              # runs forever

        async with serve(MyAgent) as server:     # embedded in your app
            await the_rest_of_your_app()

    Args:
        provider_factory: The only required argument. Receives the `Session`
            and returns the object that handles the call; the MEDIA_START is
            in `session.media`. One argument on purpose (`docs/decisions.md`),
            so adapter config goes through `functools.partial`:
            `serve(partial(MyAgent, config=config))`.
        host: Interface for the WebSocket. Loopback by default, see
            `DEFAULT_HOST`.
        port: 9000, see `DEFAULT_PORT`.
        emit: Destination of the call's RTVI events.
        on_event: The peephole into the channel protocol: synchronous,
            receives (name, payload) of EVERY event, including the ones the
            library does not know (`docs/decisions.md`).
        on_call: Runs with the Session just created, before the reading
            loop and before the provider exists. If it raises, the call is
            dropped: the exception is logged, Asterisk gets 1011, and the
            synthetic HANGUP goes out as on any other end.
        agi_handler: Opens a FastAGI on `agi_port`, the only way to talk to
            the dialplan. Without it no extra port is opened.
        transfers: The shortcut for the common AGI case, handing the call
            to a person. Mounts only `/after-dial` and `/hangup`
            (`docs/architecture.md`). Together with `agi_handler` raises
            ValueError: mount `transfers.handler` on your own `AgiRouter`
            and pass only `agi_handler`.
        forward_dtmf: Forwards keypad digits to the provider. Off by
            default: that is where people type what they do not say out
            loud (card, id, PIN), and sending it to a third party has to be
            a decision, not what happens by doing nothing. The digit reaches
            your application through the events either way.
        agi_host: Where the AGI listens, 127.0.0.1 by default and NOT the
            WebSocket host. AGI has no authentication and `EXEC` is part of
            the protocol, which in telephony ends in fraud; Asterisk invokes
            it against its own loopback. Only pass another interface when
            Asterisk is on another machine, and firewall the port.
        agi_port: 4573, the Asterisk convention.
        agi_handler_timeout_s: Overrides `agi.HANDLER_TIMEOUT_S`.
        agi_env_timeout_s: Overrides `agi.ENV_TIMEOUT_S`.
        max_calls: Cap on simultaneous calls; the surplus closes with 1013
            (`docs/decisions.md`).
        stop: A future that closes the server when resolved, in the `await`
            form; the `async with` form closes on block exit.
        subprotocol: Has to match `protocols` in your
            `websocket_client.conf` (both default to `media`).
        ping_interval_s: Seconds between keepalive PINGs to Asterisk, None
            to disable. See `PING_INTERVAL_S`.
        ping_timeout_s: Seconds to wait for the PONG before closing with
            1011. See `PING_TIMEOUT_S`.
        xoff_max_wait_s: Session policy, default from `session.py`.
        finish_max_wait_s: Same.
        provider_start_timeout_s: Same.
        provider_close_timeout_s: Same.
        audio_in_max_frames: Same. Why policies are kwargs and channel
            limits are not: `docs/decisions.md`.
    """

    def __init__(
        self,
        provider_factory: ProviderFactory,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        *,
        emit: Any = None,
        on_event: Any = None,
        on_call: SessionHook | None = None,
        agi_handler: Any = None,
        transfers: Any = None,
        forward_dtmf: bool = False,
        agi_host: str = "127.0.0.1",
        agi_port: int = 4573,
        agi_handler_timeout_s: float | None = None,
        agi_env_timeout_s: float | None = None,
        max_calls: int | None = None,
        stop: asyncio.Future | None = None,
        subprotocol: str = SUBPROTOCOL,
        ping_interval_s: float | None = PING_INTERVAL_S,
        ping_timeout_s: float | None = PING_TIMEOUT_S,
        xoff_max_wait_s: float = XOFF_MAX_WAIT_S,
        finish_max_wait_s: float = FINISH_MAX_WAIT_S,
        provider_start_timeout_s: float = START_MAX_WAIT_S,
        provider_close_timeout_s: float = CLOSE_MAX_WAIT_S,
        audio_in_max_frames: int = AUDIO_IN_MAX_FRAMES,
    ) -> None:
        if transfers is not None and agi_handler is not None:
            raise ValueError(
                "transfers and agi_handler together: mount transfers.handler "
                "on your AgiRouter and pass only agi_handler."
            )

        self._provider_factory = provider_factory
        self._host = host
        self._port = port
        self._emit = emit
        self._on_event = on_event
        self._on_call = on_call
        self._agi_handler = agi_handler
        self._transfers = transfers
        self._agi_host = agi_host
        self._agi_port = agi_port
        self._agi_handler_timeout_s = agi_handler_timeout_s
        self._agi_env_timeout_s = agi_env_timeout_s
        self._max_calls = max_calls
        self._stop = stop
        self._subprotocol = subprotocol
        self._ping_interval_s = ping_interval_s
        self._ping_timeout_s = ping_timeout_s
        self._session_kwargs = {
            "transfers": transfers,
            "forward_dtmf": forward_dtmf,
            "xoff_max_wait_s": xoff_max_wait_s,
            "finish_max_wait_s": finish_max_wait_s,
            "provider_start_timeout_s": provider_start_timeout_s,
            "provider_close_timeout_s": provider_close_timeout_s,
            "audio_in_max_frames": audio_in_max_frames,
        }

        self._agi: Any = None
        self._ws_server: Any = None
        self._active_calls = 0
        self._started = False
        self._closed = False
        self._coro: Any = None    # the _run() coroutine, one per object

    async def _handle_connection(self, websocket: Any) -> None:
        peer = getattr(websocket, "remote_address", None)

        if self._max_calls is not None and self._active_calls >= self._max_calls:
            log.warning(
                "Call from %s rejected: %d calls already active "
                "(max_calls=%d)", peer, self._active_calls, self._max_calls,
            )
            await websocket.close(code=1013, reason="at capacity")
            return

        self._active_calls += 1
        log.info("Asterisk connected from %s (%d active calls)",
                 peer, self._active_calls)

        session = Session(websocket, self._provider_factory,
                          self._emit, self._on_event,
                          **self._session_kwargs)
        try:
            if self._on_call is not None:
                try:
                    await self._on_call(session)
                except Exception:
                    # Nothing is half open yet (no provider, no reading
                    # loop), so the call is dropped here: 1011 to Asterisk,
                    # and the synthetic HANGUP so the peephole and the RTVI
                    # client see the end like any other
                    # (`test_a_failing_on_call_drops_the_call_with_1011_and_a_hangup`).
                    log.exception("on_call failed for %s: the call is "
                                  "dropped", peer)
                    await websocket.close(code=1011, reason="on_call failed")
                    # Contract with session.py, not a casual private call:
                    # the test above goes red if the name moves.
                    session._notify_hangup()
                    return
            await session.run()
        finally:
            self._active_calls -= 1
            log.info("Call ended (%s)", peer)

    async def _start(self) -> None:
        # A closed serve() does not rearm: starting it would leave a server
        # no future close() reaches (they all return on _closed), a zombie
        # listening forever. Loud error, new instance
        # (`test_a_closed_serve_does_not_rearm`).
        if self._closed:
            raise RuntimeError(
                "This serve() is already closed and cannot be rearmed: "
                "create a new instance."
            )
        if self._started:
            return
        self._started = True

        # The FastAGI only exists on request: the return to the dialplan the
        # media channel lacks, see `agi.py`.
        agi_handler = self._agi_handler
        if self._transfers is not None:
            # Both paths mount on their own so hooking the close is one
            # dialplan line and no code (`docs/architecture.md`).
            from .agi import AgiRouter

            router = AgiRouter()
            router.route("/after-dial")(self._transfers.handler)
            router.route("/hangup")(self._transfers.closing_handler)
            agi_handler = router
        if agi_handler is not None:
            from .agi import AgiServer

            # Does NOT inherit the WebSocket host: the AGI stays on loopback
            # unless asked, since the protocol has no authentication.
            self._agi = AgiServer(
                agi_handler, host=self._agi_host, port=self._agi_port,
                handler_timeout_s=self._agi_handler_timeout_s,
                env_timeout_s=self._agi_env_timeout_s,
            )
            try:
                await self._agi.start()
            except Exception as exc:
                # Same treatment as the WebSocket bind below: the instance
                # is dead, and the port hint goes to the log. Without this
                # `_started` stayed True with no server behind it and a
                # second `async with` entered in silence
                # (`test_a_failed_agi_bind_kills_the_instance_and_names_the_port`).
                self._closed = True
                self._agi = None
                if isinstance(exc, OSError):
                    self._log_bind_failure(
                        "agi", self._agi_host, self._agi_port, exc,
                        "AGI_PORT / serve(agi_port=...)")
                raise

        try:
            self._ws_server = await websockets.serve(
                self._handle_connection,
                self._host,
                self._port,
                subprotocols=[self._subprotocol],
                max_size=MAX_INCOMING_MESSAGE_BYTES,
                ping_interval=self._ping_interval_s,
                ping_timeout=self._ping_timeout_s,
            )
        except Exception as exc:
            # A failed bind (port taken) cannot leave the just-opened AGI
            # listening as an orphan. The instance stays dead: a later start
            # raises the RuntimeError above
            # (`test_a_failed_bind_does_not_leave_the_agi_orphaned`).
            self._closed = True
            if self._agi is not None:
                await self._agi.close()
                self._agi = None
            if isinstance(exc, OSError):
                self._log_bind_failure(
                    "ws", self._host, self._port, exc,
                    "MEDIA_PORT / serve(port=...)")
            raise
        log.info("Media WebSocket on ws://%s:%d (the uri in the "
                 "websocket_client.conf your Dial uses points here)",
                 self._host, self._port)
        if self._agi is not None:
            log.info("FastAGI on agi://%s:%d (the AGI() line of the dialplan "
                     "points here)", self._agi_host, self._agi_port)
        log.info("Waiting for Asterisk to dial WebSocket/<connection> ...")

    @staticmethod
    def _log_bind_failure(scheme: str, host: str, port: int, exc: OSError,
                          knob: str) -> None:
        # Almost always "address already in use": another agent (or an
        # earlier one that did not die) holds the port. The raw traceback
        # does not say how to find it; this log does.
        log.error(
            "Could not open %s://%s:%d: %s. Almost always ANOTHER agent is "
            "listening on that port (an earlier one still alive). Find it "
            "with `netstat -ano | findstr :%d` (Windows) or "
            "`ss -tlnp | grep :%d` (Linux), or start this one on another "
            "port (%s).",
            scheme, host, port, exc, port, port, knob,
        )

    def close(self) -> Awaitable[None]:
        """Stops accepting calls and closes in order. Idempotent.

        `await server.close()` waits for the calls in flight to finish
        cleaning up (`websockets` closes its connections and each Session
        sees the close and collects), and wakes a running `await serve(...)`.

        Not `async def` on purpose: asyncio calls `close()` synchronously on
        a coroutine it discards before starting it (`tasks.py:751`,
        `taskgroups.py:187-193`), and an `async def` there returned a
        coroutine nobody awaited while the inner `_run()` stayed open.
        The inner coroutine is closed here; the rest needs the loop and is
        returned for `await` (`test_close_is_synchronous_for_the_coroutine_protocol`).
        """
        if self._coro is not None \
                and inspect.getcoroutinestate(self._coro) == inspect.CORO_CREATED:
            self._coro.close()
        return _Close(self._aclose())

    async def _aclose(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._ws_server is not None:
            self._ws_server.close()
            await self._ws_server.wait_closed()
        if self._agi is not None:
            await self._agi.close()

        if self._stop is not None and not self._stop.done():
            self._stop.set_result(None)

        log.info("Server stopped")

    # `typing.Self` is 3.11+ and the package supports 3.10.
    async def __aenter__(self) -> serve:  # noqa: PYI034
        await self._start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def _run(self) -> None:
        # The `await serve(...)` form: runs until `stop` resolves, or
        # forever. The close runs on task cancellation too.
        if self._stop is None:
            self._stop = asyncio.get_running_loop().create_future()
        await self._start()
        try:
            await self._stop
        except asyncio.CancelledError:
            # Cancelling the stop FUTURE is another way to ask for the
            # close, and must not reach the caller as if THEIR task had been
            # cancelled. A task cancellation does propagate
            # (`test_cancelling_the_stop_future_closes_without_cancelling_the_caller`).
            if not self._stop.cancelled():
                raise
        finally:
            await self.close()

    # ------------------------------------------------------------------
    # Coroutine protocol (collections.abc.Coroutine)
    #
    # asyncio.run() and create_task() only accept coroutines, and this class
    # is not one by birth. Implementing the STRUCTURAL protocol (send/throw
    # + __await__ + close, which already exists) makes it a coroutine for
    # asyncio, through the same mechanism it uses for Cython coroutines, so
    # the hello world `asyncio.run(serve(MyAgent))` works as is
    # (`test_serve_is_a_coroutine_for_asyncio`).
    #
    # The inner coroutine is ONE per object: whether a Task drives it
    # (send/throw) or an await does, it is the same _run(). Reusing an
    # already-run object raises, consistent with "a closed serve does not
    # rearm".
    # ------------------------------------------------------------------

    def _main(self) -> Any:
        if self._coro is None:
            self._coro = self._run()
        return self._coro

    def __await__(self) -> Any:
        return self._main().__await__()

    def send(self, value: Any) -> Any:
        return self._main().send(value)

    def throw(self, *exc_info: Any) -> Any:
        return self._main().throw(*exc_info)

    @property
    def active_calls(self) -> int:
        return self._active_calls


async def connect(
    uri: str,
    provider_factory: ProviderFactory,
    *,
    emit: Any = None,
    on_event: Any = None,
    transfers: Any = None,
    forward_dtmf: bool = False,
    subprotocol: str = SUBPROTOCOL,
    ping_interval_s: float | None = PING_INTERVAL_S,
    ping_timeout_s: float | None = PING_TIMEOUT_S,
    xoff_max_wait_s: float = XOFF_MAX_WAIT_S,
    finish_max_wait_s: float = FINISH_MAX_WAIT_S,
    provider_start_timeout_s: float = START_MAX_WAIT_S,
    provider_close_timeout_s: float = CLOSE_MAX_WAIT_S,
    audio_in_max_frames: int = AUDIO_IN_MAX_FRAMES,
) -> None:
    """Serves ONE call by connecting to Asterisk (INCOMING mode).

    The opposite of `serve()`: here your application is the client and
    Asterisk the server, for when Asterisk cannot reach your machine
    (developing behind a NAT). The dialplan dials `WebSocket/INCOMING`, the
    channel is born at once and publishes an ephemeral id in
    MEDIA_WEBSOCKET_CONNECTION_ID, which goes in the PATH of the URI:

        ws://asterisk:8088/media/<ephemeral id>

    The HTTP server strips the `/media` prefix and looks the instance up by
    the rest of the path (`chan_websocket.c:1894-1897`): a wrong id gets
    404 (`:1904`), an id already connected gets 409 (`:1914`). Since the id
    changes per call, something has to read it off the channel and hand it
    to you, so this mode almost always comes with ARI, and `serve()` is the
    recommended path for production.

    Args:
        uri: `ws://host:8088/media/<id>`.
        provider_factory: As in `serve()`.
        emit: As in `serve()`.
        on_event: As in `serve()`.
        transfers: NOT usable here, raises ValueError: this function does
            not bring up the FastAGI (only `serve()` does), so the bot
            would record a decision no dialplan can ask for. For the return
            to the dialplan in this mode, mount the `AgiServer` yourself
            with both `Transfers` handlers.
        forward_dtmf: As in `serve()`.
        subprotocol: As in `serve()`.
        ping_interval_s: As in `serve()`.
        ping_timeout_s: As in `serve()`.
        xoff_max_wait_s: Session policy, as in `serve()`.
        finish_max_wait_s: Same.
        provider_start_timeout_s: Same.
        provider_close_timeout_s: Same.
        audio_in_max_frames: Same.

    `on_call`, `agi_handler` and `max_calls` do not exist here: they belong
    to `serve()`, which manages several calls.
    """
    if transfers is not None:
        # Accepted for symmetry with serve(), but with no FastAGI to mount
        # its paths the bot would record a decision nobody asks for: a call
        # that looks like it transfers and hangs up. Failing at startup
        # beats failing in production
        # (`test_connect_refuses_transfers_because_it_has_no_agi`).
        raise ValueError(
            "connect() does not open the FastAGI, so `transfers` has nobody "
            "to answer the dialplan: the bot would record and nobody would "
            "ask. Mount your own AgiServer with transfers.handler on "
            "/after-dial and transfers.closing_handler on /hangup, or use "
            "serve()."
        )

    async with websockets.connect(
        uri,
        subprotocols=[subprotocol],
        max_size=MAX_INCOMING_MESSAGE_BYTES,
        ping_interval=ping_interval_s,
        ping_timeout=ping_timeout_s,
    ) as websocket:
        log.info("Connected to %s", uri)
        session = Session(
            websocket, provider_factory, emit, on_event,
            forward_dtmf=forward_dtmf,
            xoff_max_wait_s=xoff_max_wait_s,
            finish_max_wait_s=finish_max_wait_s,
            provider_start_timeout_s=provider_start_timeout_s,
            provider_close_timeout_s=provider_close_timeout_s,
            audio_in_max_frames=audio_in_max_frames,
        )
        await session.run()
