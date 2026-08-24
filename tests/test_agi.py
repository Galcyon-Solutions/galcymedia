"""
Tests of the FastAGI server.

The protocol is plain text over TCP, so it can be tested end to end without
Asterisk: a socket is opened, the environment is sent exactly as Asterisk sends
it, and whatever the handler replies is read back.

The sample environment is copied from the source (res_agi.c:2455-2491),
including the ordering and the trailing empty line.
"""

from __future__ import annotations

import asyncio

from galcymedia import AgiServer

# How Asterisk presents itself on connect. Ends with an empty line.
ENVIRONMENT = (
    "agi_request: agi://127.0.0.1:4573/after-dial\n"
    "agi_channel: PJSIP/1001-00000001\n"
    "agi_language: es\n"
    "agi_type: PJSIP\n"
    "agi_uniqueid: 1786243494.65\n"
    "agi_version: 23.4.1\n"
    "agi_callerid: 1001\n"
    "agi_calleridname: Caller\n"
    "agi_context: voicebot-in\n"
    "agi_extension: 1234\n"
    "agi_priority: 8\n"
    "agi_arg_1: first\n"
    "agi_arg_2: second\n"
    "\n"
)


async def talk_to(handler, environment: str = ENVIRONMENT,
                  responses: list[str] | None = None) -> list[str]:
    """Bring up the server, play Asterisk, and return what it requested.

    `responses` are the lines the fake Asterisk replies with for each command;
    by default, the `200 result=1` of a successful operation.
    """
    server = AgiServer(handler, host="127.0.0.1", port=0)
    await server.start()
    port = server._server.sockets[0].getsockname()[1]

    received: list[str] = []
    pending = list(responses or [])

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(environment.encode())
        await writer.drain()

        # Read until the server closes, answering each command.
        while True:
            line = await reader.readline()
            if not line:
                break
            received.append(line.decode().strip())
            response = pending.pop(0) if pending else "200 result=1"
            writer.write((response + "\n").encode())
            await writer.drain()
    finally:
        writer.close()
        await server.close()

    return received


async def test_the_environment_arrives_parsed():
    seen = {}

    async def handler(request):
        seen["channel_id"] = request.channel_id
        seen["channel"] = request.channel
        seen["caller"] = request.caller
        seen["extension"] = request.extension
        seen["context"] = request.context
        seen["path"] = request.path
        seen["args"] = request.args

    await talk_to(handler)

    # The uniqueid identifies the CALLER's channel, which is not the WebSocket
    # channel of the MEDIA_START: the real correlation is seeded by the dialplan.
    assert seen["channel_id"] == "1786243494.65"
    assert seen["channel"] == "PJSIP/1001-00000001"
    assert seen["caller"] == "1001"
    assert seen["extension"] == "1234"
    assert seen["context"] == "voicebot-in"
    assert seen["path"] == "/after-dial"
    assert seen["args"] == ["first", "second"]


async def test_set_variable_sends_the_protocol_command():
    async def handler(request):
        await request.set_variable("BOT_ACTION", "transfer")

    commands = await talk_to(handler)

    assert commands == ['SET VARIABLE BOT_ACTION "transfer"']


async def test_get_variable_returns_the_value_in_parentheses():
    """Asterisk replies `200 result=1 (value)`, or `200 result=0` if missing."""
    read = {}

    async def handler(request):
        read["exists"] = await request.get_variable("TRANSFER_TO")
        read["missing"] = await request.get_variable("MISSING")

    await talk_to(handler, responses=[
        "200 result=1 (30632)",
        "200 result=0",
    ])

    assert read["exists"] == "30632"
    assert read["missing"] == ""


async def test_several_commands_in_order():
    async def handler(request):
        await request.set_variable("BOT_ACTION", "transfer")
        await request.set_variable("BOT_REASON", "caller asked")
        await request.verbose("decision recorded")

    commands = await talk_to(handler)

    assert commands == [
        'SET VARIABLE BOT_ACTION "transfer"',
        'SET VARIABLE BOT_REASON "caller asked"',
        'VERBOSE "decision recorded" 1',
    ]


async def test_a_handler_that_does_nothing_is_valid():
    """The normal case when there was no transfer: no variable is written."""
    async def handler(request):
        pass

    assert await talk_to(handler) == []


async def test_a_handler_that_blows_up_does_not_take_down_the_server():
    """Asterisk continues at the next priority regardless."""
    async def handler(request):
        raise RuntimeError("the decision failed")

    # It does not propagate: the connection closes and the call runs its course.
    assert await talk_to(handler) == []


async def test_a_slow_handler_is_cut_off_instead_of_hanging_the_call():
    """FastAGI has no timeout of its own: without this limit the call stalls."""
    import galcymedia.agi as module

    original = module.HANDLER_TIMEOUT_S
    module.HANDLER_TIMEOUT_S = 0.05
    try:
        async def handler(request):
            await asyncio.sleep(5)
            await request.set_variable("LATE", "yes")

        commands = await asyncio.wait_for(talk_to(handler), timeout=2.0)
        assert commands == [], "it had no time to send anything"
    finally:
        module.HANDLER_TIMEOUT_S = original


async def test_a_connection_that_is_not_agi_closes_itself():
    """A port scan, or anyone who opens the socket and stays silent.

    Without a limit on reading the environment, that connection waits forever
    and holds a descriptor. On an exposed port a single scan is enough to
    exhaust them.
    """
    import galcymedia.agi as module

    original = module.ENV_TIMEOUT_S
    module.ENV_TIMEOUT_S = 0.05
    try:
        called = []

        async def handler(request):
            called.append(request)

        server = AgiServer(handler, host="127.0.0.1", port=0)
        await server.start()
        port = server._server.sockets[0].getsockname()[1]

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            # Partial environment: the empty line that closes it is missing.
            writer.write(b"agi_request: agi://x/y\n")
            await writer.drain()

            # The server has to close on its own.
            closed = await asyncio.wait_for(reader.read(), timeout=2.0)
            assert closed == b"", "the server had to close the connection"
        finally:
            writer.close()
            await server.close()

        assert not called, "without a complete environment the handler is not invoked"
    finally:
        module.ENV_TIMEOUT_S = original


async def test_the_server_can_be_closed_and_frees_the_port():
    async def handler(request):
        pass

    server = AgiServer(handler, host="127.0.0.1", port=0)
    await server.start()
    port = server._server.sockets[0].getsockname()[1]
    await server.close()

    # If the port had not been freed, this would raise.
    other = AgiServer(handler, host="127.0.0.1", port=port)
    await other.start()
    await other.close()


async def test_the_agi_listens_on_loopback_by_default():
    """The AGI protocol has no authentication: anyone who reaches the port can
    write variables and run applications on active channels. The default value
    has to be the safe one."""
    async def handler(request):
        pass

    server = AgiServer(handler)
    assert server._host == "127.0.0.1"


async def test_the_router_dispatches_by_path():
    """Routing is first class: /start and /after-dial are distinct invocations
    of the same dialplan, without an if in the user's handler."""
    from galcymedia import AgiRouter

    visits = []
    router = AgiRouter()

    @router.route("/after-dial")
    async def after_dial(request):
        visits.append(("after-dial", request.args))

    # The sample ENVIRONMENT points to /after-dial, so the router has to land there.
    await talk_to(router)
    assert visits == [("after-dial", ["first", "second"])]


async def test_a_route_without_a_handler_does_not_respond_and_does_not_blow_up():
    """FastAGI without a response = the dialplan continues. An AGI to an
    unregistered route is logged and nothing more."""
    from galcymedia import AgiRouter

    router = AgiRouter()

    @router.route("/other")
    async def other(request):
        raise AssertionError("it should not have arrived here")

    assert await talk_to(router) == []


async def test_the_router_accepts_direct_registration():
    """To mount ready-made handlers, such as transfers.handler."""
    from galcymedia import AgiRouter, Transfers

    transfers = Transfers()
    transfers.transfer("first", "reason")

    router = AgiRouter()
    router.route("/after-dial")(transfers.handler)

    # The sample environment sends agi_arg_1=first: that is the key.
    commands = await talk_to(router)
    assert 'SET VARIABLE BOT_ACTION "transfer"' in commands


async def test_set_variable_blocks_command_injection():
    """A value with a newline would inject a second AGI command
    (EXEC System = shell). The newline is neutralized."""
    async def handler(request):
        # The value comes from untrusted data (the model decided it).
        await request.set_variable("BOT_REASON",
                                   'transfer"\nEXEC System "rm -rf /')

    sent = await talk_to(handler)
    # A single command, without a newline that injects an EXEC.
    assert len(sent) == 1, "there cannot be a second injected command"
    assert "\n" not in sent[0]
    assert "EXEC System" in sent[0], "the text stays, but as inert data"
    assert sent[0].count("SET VARIABLE") == 1


async def test_safe_neutralizes_quotes_newlines_and_the_escape():
    from galcymedia.agi import _safe
    assert _safe('a"b') == "a'b"
    assert _safe("a\nb") == "a b"
    assert _safe("a\r\nb") == "a  b"
    assert _safe(None) == "None"
    # The backslash is the parser's escape character: a value ending in one
    # eats the closing quote this very function put there.
    assert "\\" not in _safe("ok\\")


async def test_a_name_that_is_really_a_function_call_is_refused():
    """The dangerous half of SET VARIABLE is the NAME, not the value.

    A name ending in `)` goes to `ast_func_write()`
    (`pbx_variables.c:1241-1244`), so `FILE(...)` writes a file on the
    server with no spaces, quotes or newlines: the whole shape is demanded.
    """
    import pytest

    from galcymedia.agi import _check_variable_name

    for hostile in ("FILE(/etc/asterisk/extensions.conf,0,0)",
                    "SHELL(rm -rf /)",
                    "DB(family/key)",
                    "BOT ACTION",
                    "BOT\nACTION",
                    "",
                    "1BOT"):
        with pytest.raises(ValueError, match="Invalid variable name"):
            _check_variable_name(hostile)


async def test_the_names_a_dialplan_really_uses_are_accepted():
    """Refusing is only worth it if it does not get in the way of the real
    thing: the inheritance underscores are legitimate Asterisk syntax."""
    from galcymedia.agi import _check_variable_name

    for good in ("BOT_ACTION", "_CALL_ID", "__TRANSFER_CONTEXT",
                 "CDR", "x", "A1"):
        assert _check_variable_name(good) == good


async def test_a_multiline_response_does_not_desync_the_dialogue():
    """A malformed command gets SEVERAL lines back (`res_agi.c:4240-4242`):
    `NNN-` continuation, usage text with no prefix, `NNN ` closing. Reading
    one line leaves the rest in the buffer and the next command reads
    someone else's reply, for the WHOLE AGI dialogue of the call.
    """
    results = {}

    async def handler(request):
        # Raw command that Asterisk rejects with a multiline response.
        results["rejected"] = await request.command("EXEC Malformed")
        # The next command has to read ITS response, not the tail of the previous one.
        results["variable"] = await request.get_variable("BOT_ACTION")

    server = AgiServer(handler, host="127.0.0.1", port=0)
    await server.start()
    port = server._server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"agi_request: agi://x/after-dial\n\n")
        await writer.drain()

        # Read the first command and answer with the REAL multiline response
        # from res_agi.c: three lines, the middle one without a numeric prefix.
        await reader.readline()
        writer.write(
            b"520-Invalid command syntax.  Proper usage follows:\n"
            b"Executes the given application.\n"
            b"520 End of proper usage.\n")
        await writer.drain()

        # Read the second command and answer with its single-line response.
        await reader.readline()
        writer.write(b"200 result=1 (transfer)\n")
        await writer.drain()
        await asyncio.sleep(0.1)
    finally:
        writer.close()
        await server.close()

    # The variable arrives correct: the dialogue did NOT shift by one position.
    assert results.get("variable") == "transfer", \
        "the second command read someone else's response: the dialogue desynced"


async def test_an_unsolicited_hangup_line_does_not_desync_the_dialogue():
    """When the caller hangs up mid-AGI, Asterisk writes `HANGUP` on the
    socket without being asked (`res_agi.c:4360`, `:4335`). Taken as a reply,
    it shifts every later command by one line. It is skipped, and the request
    remembers it in `hung_up`.
    """
    got = {}

    async def handler(request):
        got["before"] = request.hung_up
        got["first"] = await request.get_variable("A")
        got["hung_up"] = request.hung_up
        got["second"] = await request.get_variable("B")

    server = AgiServer(handler, host="127.0.0.1", port=0)
    await server.start()
    port = server._server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"agi_request: agi://x/after-dial\n\n")
        await writer.drain()

        await reader.readline()                       # GET VARIABLE A
        # The caller hung up while we were asking: HANGUP lands first.
        writer.write(b"HANGUP\n200 result=1 (value-of-A)\n")
        await writer.drain()

        await reader.readline()                       # GET VARIABLE B
        writer.write(b"200 result=1 (value-of-B)\n")
        await writer.drain()
        await asyncio.sleep(0.1)
    finally:
        writer.close()
        await server.close()

    assert got["before"] is False
    assert got["first"] == "value-of-A", "HANGUP was taken as the reply"
    assert got["hung_up"] is True
    assert got["second"] == "value-of-B", "the dialogue shifted by one"


async def test_a_reply_over_max_line_bytes_warns_with_the_cap(caplog):
    """A GET VARIABLE reply longer than the cap is dropped by the
    StreamReader and reads as "no value". The loss is logged with the cap in
    force, so the integrator knows which knob to turn; the dialogue does not
    shift (the reader clears the line whole).
    """
    got = {}

    async def handler(request):
        got["long"] = await request.get_variable("A")
        got["second"] = await request.get_variable("B")

    server = AgiServer(handler, host="127.0.0.1", port=0)
    await server.start()
    port = server._server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"agi_request: agi://x/after-dial\n\n")
        await writer.drain()
        await reader.readline()                       # GET VARIABLE A
        with caplog.at_level("WARNING"):
            writer.write(b"200 result=1 (" + b"X" * 5000 + b")\n")
            await writer.drain()
            await reader.readline()                   # GET VARIABLE B
        writer.write(b"200 result=1 (value-of-B)\n")
        await writer.drain()
        await asyncio.sleep(0.1)
    finally:
        writer.close()
        await server.close()

    assert got["long"] == ""
    assert got["second"] == "value-of-B", "the dialogue shifted"
    assert any("max_line_bytes=4096" in r.message for r in caplog.records), \
        "the drop has to be logged with the cap in force"


async def test_max_line_bytes_configurable():
    """A short limit drops the long environment; the same environment passes with
    the default. It is the kwarg for dialplans with big variables (CRM JSON)."""
    seen = []

    async def handler(request):
        seen.append(request.environment)

    server = AgiServer(handler, host="127.0.0.1", port=0, max_line_bytes=32)
    await server.start()
    port = server._server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        # An environment line longer than the 32-byte limit.
        writer.write(b"agi_channel: " + b"X" * 100 + b"\n\n")
        await writer.drain()
        await reader.read()          # the server drops it and closes
    finally:
        writer.close()
        await server.close()

    assert seen == [], "with the short limit the long environment does not fit"
