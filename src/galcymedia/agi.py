"""
FastAGI server: the return to the dialplan.

`chan_websocket` is a MEDIA channel: none of its eleven commands
(`chan_websocket.c:753-856`) writes a variable or runs anything in the
dialplan. An application that decides something mid-call (transfer, mark the
outcome) has no native way to tell the dialplan; the only signal is hanging
up, one bit with no data. FastAGI closes that gap: the dialplan runs
`AGI(agi://127.0.0.1:4573/after-dial)` and your application answers
`SET VARIABLE`.
Why FastAGI and not func_curl, AMI or ARI: `docs/decisions.md`.

The protocol (`res_agi.c:2455-2491` for the environment):

    1. Asterisk opens one TCP connection per AGI() in the dialplan
    2. It sends its environment, one `key: value` line each, then an empty line
    3. You answer zero or more commands, one per line
    4. Asterisk replies `200 result=...` to each, and closes when you do

With nobody listening, Asterisk logs the failure, sets `AGISTATUS=FAILURE`
and the dialplan CONTINUES at the next priority (`res_agi.c:2229`, `:4718`):
the call does not drop.

FastAGI has no timeout of its own: `run_agi` waits with `ms = -1`
(`res_agi.c:4409`), so a server that accepts and never answers leaves the
call stalled on that dialplan line. The handler runs under `HANDLER_TIMEOUT_S`
for that reason. Keep it fast: consult what you already know, do not go out
to the network.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger(__name__)

# While the handler runs, the call is stalled on that dialplan line.
HANDLER_TIMEOUT_S = 5.0

# Asterisk sends the environment in one go on connect: a delay here means
# whoever connected is not an AGI.
ENV_TIMEOUT_S = 2.0

# Environment caps. It is about twenty short lines; anything bigger is not an
# AGI.
MAX_ENV_LINES = 200
MAX_LINE_BYTES = 4096

# GET VARIABLE reply: `200 result=<0|1>` plus ` (value)` when it exists
# (`res_agi.c:2574`, `:2603`). `(.*)` is greedy on purpose: it runs to the
# LAST `)`, so a value with parentheses inside comes out whole
# (`test_get_variable_returns_the_value_in_parentheses`).
_GET_VARIABLE_RE = re.compile(r"^200 result=(-?\d+)(?: \((.*)\))?")


# A channel variable name and nothing else. The leading inheritance
# underscores (`_`, `__`) are legitimate Asterisk syntax.
#
# An allow-list, not a deny-list, and that is the point: a name ending in `)`
# is not a variable, it is a function call. `pbx_builtin_setvar_helper()`
# hands it to `ast_func_write()` (`pbx_variables.c:1241-1244`), so
# `FILE(/etc/asterisk/extensions.conf,0,0)` writes a file on the server. No
# spaces, no newlines: no per-character escaping catches it, the whole shape
# has to be demanded. And `live_dangerously` does not cover this path:
# `res_agi.c` never calls `ast_thread_inhibit_escalations`, so
# `is_write_allowed` (`pbx_functions.c:590-606`) lets it through.
_VARIABLE_NAME_RE = re.compile(r"^_{0,2}[A-Za-z][A-Za-z0-9_]*$")


def _check_variable_name(name: str) -> str:
    """Returns the name if it is a variable name, raises otherwise.

    REFUSED rather than sanitized, on purpose: sanitizing turns an attack into
    a valid write to some other variable, which is worse because nobody
    notices. An error leaves a trace. See `_VARIABLE_NAME_RE`.

    Raises:
        ValueError: The name is not a plain channel variable name.
    """
    if not _VARIABLE_NAME_RE.match(name or ""):
        raise ValueError(
            f"Invalid variable name: {name!r}. Only letters, digits and "
            f"underscore, optionally starting with _ or __ to inherit. A name "
            f"with parentheses is not a variable: Asterisk would run it as a "
            f"function call."
        )
    return name


def _safe(value: Any) -> str:
    """Sanitizes a VALUE before it goes into an AGI command.

    AGI is one command per LINE: a `\\n` or `\\r` in the value splits the
    command and what follows runs as a new one (`EXEC System ...` runs a
    shell). The double quote closes the quoting, and the backslash is the
    escape of Asterisk's parser (`res_agi.c:4156-4162`): a value ending in
    `\\` eats the closing quote this very function adds.

    Values only. A NAME is not sanitized, it is checked whole: see
    `_check_variable_name`.
    """
    text = str(value)
    return (text.replace("\r", " ").replace("\n", " ")
            .replace("\\", "/").replace('"', "'"))


class AgiRequest:
    """One AGI() invocation from the dialplan.

    Carries the environment Asterisk sent and the commands sent back. The
    public methods cover what a voice application needs; `command()` is the
    door for anything else in the protocol.

    Args:
        environment: The `key: value` block Asterisk sent.
        write: Coroutine that sends one line and returns the reply.
    """

    def __init__(self, environment: dict[str, str], write: Any) -> None:
        self.environment = environment
        self._write = write
        # True once Asterisk wrote `HANGUP` on the socket: the caller hung up
        # while this handler was running (`res_agi.c:4360`, `:4335`). Only
        # dead-safe commands answer from then on.
        self.hung_up = False

    # -- What Asterisk sent --------------------------------------------

    @property
    def channel_id(self) -> str:
        """Unique id of the channel that invoked the AGI.

        CAREFUL: it is the CALLER's channel, not the WebSocket channel. A
        call has two; the Dial creates the second with its own id, and that
        one does NOT match the `channel_id` of MEDIA_START. To correlate the
        two halves, seed a key in the dialplan (`Set(_CALL_ID=${UNIQUEID})`
        before the Dial) and pass it as an AGI argument. See
        `docs/architecture.md`.
        """
        return self.environment.get("agi_uniqueid", "")

    @property
    def channel(self) -> str:
        """Channel name, like `PJSIP/1001-00000001`."""
        return self.environment.get("agi_channel", "")

    @property
    def caller(self) -> str:
        return self.environment.get("agi_callerid", "")

    @property
    def extension(self) -> str:
        return self.environment.get("agi_extension", "")

    @property
    def context(self) -> str:
        return self.environment.get("agi_context", "")

    @property
    def path(self) -> str:
        """The tail of the URI: `agi://host:port/PATH`, `/` when absent.

        Tells invocations of one dialplan apart, such as `/start` before the
        Dial and `/end` after it.
        """
        request = self.environment.get("agi_request", "")
        _, _, rest = request.partition("://")
        _, _, path = rest.partition("/")
        return "/" + path if path else "/"

    @property
    def args(self) -> list[str]:
        """The extra arguments of AGI(agi://.../path,one,two)."""
        values = []
        index = 1
        while f"agi_arg_{index}" in self.environment:
            values.append(self.environment[f"agi_arg_{index}"])
            index += 1
        return values

    # -- What is asked of Asterisk -------------------------------------

    async def command(self, line: str) -> str:
        """Sends a raw AGI command and returns the reply.

        CAREFUL: the line goes AS IS, unsanitized, and Asterisk has no safety
        net on this path (see `_VARIABLE_NAME_RE`). A command built from text
        you do not control is command execution on your server, and text
        that comes from a call is never under control: the caller says it, an
        ASR transcribes it, a model repeats it. Build the line from YOUR data;
        anything from the model or the caller is checked against an
        allow-list before it gets here, or it does not get here.

        Args:
            line: The command, without the trailing newline.

        Returns:
            The first line of the reply, stripped.
        """
        return await self._write(line)

    async def set_variable(self, name: str, value: str) -> None:
        """Writes a variable on the channel that invoked the AGI.

        The only way an application connected over WebSocket tells the
        dialplan anything. The channel is the caller's, the one still alive
        after the Dial. The name is CHECKED and the value is SANITIZED, two
        defenses for two different attacks: see `_check_variable_name` and
        `_safe`.

        Raises:
            ValueError: `name` is not a plain channel variable name.
        """
        await self.command(
            f'SET VARIABLE {_check_variable_name(name)} "{_safe(value)}"')

    async def get_variable(self, name: str) -> str:
        """Reads a channel variable. Empty string when it does not exist.

        Raises:
            ValueError: `name` is not a plain channel variable name.
        """
        response = await self.command(
            f"GET VARIABLE {_check_variable_name(name)}")
        match = _GET_VARIABLE_RE.match(response)
        if match and match.group(1) == "1" and match.group(2) is not None:
            return match.group(2)
        return ""

    async def verbose(self, message: str, level: int = 1) -> None:
        """Writes to the Asterisk console, useful to debug the dialplan."""
        await self.command(f'VERBOSE "{_safe(message)}" {int(level)}')


Handler = Callable[[AgiRequest], Awaitable[None]]


class AgiRouter:
    """Dispatches AGI() invocations by the path of the URI.

    AGI is access to the Asterisk channel from the process, at any point of
    the dialplan, with or without a WebSocket channel involved. BEFORE the
    Dial it seeds data the WebSocket channel inherits (look up the CRM with
    the incoming number before the bot greets); AFTER it collects decisions.
    The path tells those apart:

        router = AgiRouter()

        @router.route("/start")
        async def before_dial(request):
            record = crm.lookup(request.caller)
            await request.set_variable("_CUSTOMER_ID", record.id)

        @router.route("/end")
        async def after_dial(request):
            ...

        serve(factory, agi_handler=router)

    Direct registration mounts ready-made handlers:

        router.route("/end")(transfers.handler)

    An unknown path is logged and gets no reply: the dialplan continues,
    which is normal FastAGI behavior.
    """

    def __init__(self) -> None:
        self._routes: dict[str, Handler] = {}

    def route(self, path: str) -> Callable[[Handler], Handler]:
        """Registers a handler for a path. Usable as a decorator."""
        def register(handler: Handler) -> Handler:
            self._routes[path] = handler
            return handler
        return register

    async def __call__(self, request: AgiRequest) -> None:
        handler = self._routes.get(request.path)
        if handler is None:
            log.warning(
                "AGI to path %s, which has no handler. Paths: %s",
                request.path, ", ".join(sorted(self._routes)) or "none",
            )
            return
        await handler(request)


class AgiServer:
    """Serves the AGI() invocations of the dialplan.

    One per process. Each invocation is a short connection: the environment
    arrives, the handler runs, the connection closes.

    Listens on loopback by default, and that is the safe value: AGI has no
    authentication, and whoever reaches the port can run `SET VARIABLE` or
    `EXEC <application>` on live channels. Asterisk invokes
    `AGI(agi://127.0.0.1:...)` against its own loopback, so the default
    covers the usual deployment; for another interface, firewall the port.

    Args:
        handler: Called once per invocation, under `HANDLER_TIMEOUT_S`.
        host: Interface to listen on.
        port: 4573 is the Asterisk convention.
        handler_timeout_s: Overrides `HANDLER_TIMEOUT_S`.
        env_timeout_s: Overrides `ENV_TIMEOUT_S`.
        max_line_bytes: Overrides `MAX_LINE_BYTES`. It also caps Asterisk's
            REPLIES: a long channel variable (a CRM JSON) can exceed 4096 in
            the `200 result=1 (value)` of GET VARIABLE. Raise it for that;
            the default protects against junk traffic.
    """

    def __init__(self, handler: Handler, host: str = "127.0.0.1",
                 port: int = 4573, *,
                 handler_timeout_s: float | None = None,
                 env_timeout_s: float | None = None,
                 max_line_bytes: int | None = None) -> None:
        self._handler = handler
        self._host = host
        self._port = port
        # None resolves to the module constant AT USE, not at construction,
        # so a test can patch the constant
        # (`test_a_slow_handler_is_cut_off_instead_of_hanging_the_call`).
        self._handler_timeout_s = handler_timeout_s
        self._env_timeout_s = env_timeout_s
        self._max_line_bytes = max_line_bytes
        self._limit = MAX_LINE_BYTES
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        # limit= makes the per-line cap REAL. Without it the StreamReader
        # keeps its 64 KiB default and the cap is never reached: a line with
        # no newline blows up at 64 KiB, not at the documented 4096
        # (`test_max_line_bytes_configurable`).
        limit = self._max_line_bytes \
            if self._max_line_bytes is not None else MAX_LINE_BYTES
        self._limit = limit
        self._server = await asyncio.start_server(
            self._handle, self._host, self._port, limit=limit,
        )
        log.info("FastAGI listening on agi://%s:%d", self._host, self._port)

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        """One AGI() invocation. Lives as long as the handler."""
        peer = writer.get_extra_info("peername")

        try:
            # Under a timeout: whoever opens the socket and never completes
            # the environment would hold a descriptor forever. A port scan is
            # enough to cause it.
            env_timeout = self._env_timeout_s \
                if self._env_timeout_s is not None else ENV_TIMEOUT_S
            try:
                environment = await asyncio.wait_for(
                    self._read_environment(reader), timeout=env_timeout,
                )
            except asyncio.TimeoutError:
                log.debug("Connection from %s without a complete AGI "
                          "environment", peer)
                return

            if not environment:
                return

            async def write(line: str) -> str:
                writer.write((line + "\n").encode("utf-8"))
                await writer.drain()

                # Asterisk usually replies ONE line (`200 result=...`), but a
                # malformed command gets a multiline reply: `NNN-` continues,
                # `NNN<space>` ends (`res_agi.c:4240-4242`). It has to be
                # consumed WHOLE or the tail stays in the buffer and the next
                # command reads someone else's reply
                # (`test_a_multiline_response_does_not_desync_the_dialogue`).
                first = ""
                for _ in range(MAX_ENV_LINES):
                    try:
                        raw = await reader.readline()
                    except (ValueError, asyncio.LimitOverrunError):
                        # A reply longer than the cap (a CRM JSON in a
                        # channel variable). The StreamReader drops the line
                        # whole, so the command reads as "no value"; without
                        # this warning the loss is silent. Measured: the next
                        # command still reads its own reply
                        # (`test_a_reply_over_max_line_bytes_warns_with_the_cap`).
                        log.warning(
                            "AGI reply to %r is longer than max_line_bytes=%d "
                            "and was dropped; raise it with "
                            "AgiServer(max_line_bytes=...)", line, self._limit,
                        )
                        break
                    if not raw:
                        break
                    text = raw.decode("utf-8", "replace").rstrip("\r\n")
                    # Unsolicited: Asterisk writes `HANGUP` when the caller
                    # hangs up mid-AGI (`res_agi.c:4335`). It is not the
                    # reply; taking it as one shifts the whole dialogue by
                    # one line (`test_an_unsolicited_hangup_line_does_not_
                    # desync_the_dialogue`).
                    if text == "HANGUP":
                        request.hung_up = True
                        continue
                    is_code = len(text) >= 4 and text[:3].isdigit()
                    if not first:
                        first = text
                        if not (is_code and text[3] == "-"):
                            break
                        continue
                    # Inside a multiline reply, stop at the closing line
                    # (`NNN<space>`), not at the usage text without prefix.
                    if is_code and text[3] == " ":
                        break
                return first.strip()

            request = AgiRequest(environment, write)
            log.debug("AGI %s from %s (channel %s)",
                      request.path, peer, request.channel_id)

            handler_timeout = self._handler_timeout_s \
                if self._handler_timeout_s is not None else HANDLER_TIMEOUT_S
            try:
                await asyncio.wait_for(self._handler(request),
                                       timeout=handler_timeout)
            except asyncio.TimeoutError:
                log.warning(
                    "AGI handler %s took more than %.0fs and was cut off. "
                    "The call went on without a reply.",
                    request.path, handler_timeout,
                )
            except Exception:
                # A failure here cannot take the call down: Asterisk
                # continues at the next priority either way.
                log.exception("AGI handler %s failed", request.path)

        except (ConnectionResetError, asyncio.IncompleteReadError):
            log.debug("Asterisk closed the AGI connection early")
        except Exception:
            log.exception("Failed serving an AGI invocation")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                log.debug("AGI socket was already gone on close")

    async def _read_environment(
            self, reader: asyncio.StreamReader) -> dict[str, str]:
        """Reads the `key: value` block every invocation starts with.

        Ends at an empty line (`res_agi.c:2491`). The caps above keep a
        connection that is not an AGI from consuming memory without end.

        Returns:
            The environment, or `{}` when the connection is not an AGI.
        """
        environment: dict[str, str] = {}

        for _ in range(MAX_ENV_LINES):
            try:
                raw = await reader.readline()
            except (ValueError, asyncio.LimitOverrunError):
                # With limit= on the StreamReader, a line past MAX_LINE_BYTES
                # with no newline blows up here: a client that does not speak
                # AGI (a port scan, junk). Dropped clean, no traceback.
                log.warning("AGI environment line too long, dropped")
                return {}
            if not raw:
                return {}                      # closed before finishing

            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            if not line:
                return environment             # empty line: end of environment

            name, _, value = line.partition(":")
            environment[name.strip()] = value.strip()

        log.warning("AGI environment did not end within %d lines",
                    MAX_ENV_LINES)
        return {}
