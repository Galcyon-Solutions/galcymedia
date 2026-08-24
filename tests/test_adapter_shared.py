"""
What every adapter does the same way.

Worth testing on its own, because now a failure here breaks all three at once,
which is exactly the price of not having it copied around.
"""

from __future__ import annotations

import asyncio

import pytest
import websockets
from conftest import FakeSession, FakeSocket, FakeTime

from galcymedia.adapters import _shared


class FakeMediaFormat:
    def __init__(self, audio_format: str) -> None:
        self.audio_format = audio_format


CODECS = {"alaw": "alaw", "ulaw": "mulaw"}


# ---------------------------------------------------------------------------
# The channel codec
# ---------------------------------------------------------------------------


def test_the_codec_maps_to_the_provider_name():
    assert _shared.resolve_codec(FakeMediaFormat("alaw"), CODECS, "X") == "alaw"
    assert _shared.resolve_codec(FakeMediaFormat("ulaw"), CODECS, "X") == "mulaw"


def test_an_unknown_codec_fails_naming_the_provider():
    """The message must say whose limitation it is and how to get out of it,
    and the way out comes from `codecs`: a hardcoded "c(ulaw) or c(alaw)"
    contradicts the list for a provider that speaks something else."""
    with pytest.raises(RuntimeError) as failure:
        _shared.resolve_codec(FakeMediaFormat("slin16"), CODECS, "Deepgram")

    assert "Deepgram" in str(failure.value)
    assert "c(ulaw)" in str(failure.value)

    with pytest.raises(RuntimeError) as failure:
        _shared.resolve_codec(FakeMediaFormat("alaw"), {"slin16": "pcm16"}, "X")

    assert "c(slin16)" in str(failure.value)
    assert "ulaw" not in str(failure.value)


def test_every_adapter_codec_key_is_a_name_asterisk_writes():
    """The Dial hint trusts the CODECS keys, so each one has to be a name
    `ast_format_get_name` can write (`codec_builtin.c`). An alias like
    "pcma" never arrives and would suggest a Dial that does not exist.
    ElevenLabs resolves with literals, so it is probed name by name.
    """
    from galcymedia.adapters import deepgram, elevenlabs, openai

    for module in (deepgram, openai):
        dead = set(module.CODECS) - _shared.ASTERISK_AUDIO_FORMATS
        assert not dead, f"{module.__name__}: keys Asterisk never writes {dead}"

    aliases = {"pcma", "pcmu", "mulaw", "g711", "PCMA"}
    for name in aliases | _shared.ASTERISK_AUDIO_FORMATS:
        try:
            elevenlabs.resolve_audio_path(FakeMediaFormat(name))
        except RuntimeError:
            continue
        assert name.lower() in _shared.ASTERISK_AUDIO_FORMATS, (
            f"elevenlabs accepts {name!r}, which Asterisk never writes")


# ---------------------------------------------------------------------------
# Provider messages, which are not trusted
# ---------------------------------------------------------------------------


def test_an_unreadable_message_does_not_blow_up():
    assert _shared.parse_json("{broken") is None
    assert _shared.parse_json("[1, 2]") is None, "an array is not an event"
    assert _shared.parse_json('{"type": "x"}') == {"type": "x"}


def test_tool_arguments_tolerate_garbage():
    """A model sends them: they may come malformed."""
    assert _shared.parse_arguments('{"area": "Lima"}') == {"area": "Lima"}
    assert _shared.parse_arguments("{broken") == {}
    assert _shared.parse_arguments(None) == {}
    assert _shared.parse_arguments("[1,2]") == {}, "an array is not arguments"
    assert _shared.parse_arguments({"already": "a dict"}) == {"already": "a dict"}


# ---------------------------------------------------------------------------
# The tools belong to the user
# ---------------------------------------------------------------------------


async def test_without_a_hook_the_tool_is_rejected():
    result = await _shared.run_tool(None, "whatever", {})

    assert result == {"ok": False, "error": "no handler"}


async def test_a_tool_that_blows_up_returns_an_error():
    """The model must ALWAYS get a response or it keeps waiting."""
    async def broken(name, arguments):
        raise RuntimeError("the database does not answer")

    result = await _shared.run_tool(broken, "lookup", {})

    assert result["ok"] is False
    assert "database" in result["error"]


async def test_a_tool_that_never_returns_is_abandoned():
    """A hook that hangs must not take the call with it.

    This runs INSIDE the loop that reads the provider's socket, so while the
    hook is stuck the adapter processes nothing: no barge-in, no bot audio, and
    on ElevenLabs not even the `pong` the provider needs
    (`ElevenLabsProvider._pong`).
    """
    started = asyncio.Event()

    async def never_returns(name, arguments):
        started.set()
        await asyncio.sleep(3600)

    result = await asyncio.wait_for(
        _shared.run_tool(never_returns, "lookup", {}, timeout_s=0.05),
        timeout=1.0)

    assert started.is_set(), "the hook never even ran"
    assert result["ok"] is False, "a hung tool has to answer with an error"


def test_the_tool_timeout_stays_under_the_elevenlabs_cut():
    """The floor for the case where NOTHING goes out. Measured against the
    real service: ElevenLabs closes with 1002 after 60 s without any client
    message, and stays open with audio flowing and 60 pings unanswered. The
    session sends the caller's audio from its own task and the filler covers
    a quiet channel, so this case only happens if the filler task died; the
    test keeps the default at half the cut so that it still does not bite.
    """
    from galcymedia.adapters import elevenlabs

    assert _shared.TOOL_TIMEOUT_S <= elevenlabs.PROVIDER_IDLE_CUT_S / 2, (
        f"TOOL_TIMEOUT_S={_shared.TOOL_TIMEOUT_S} is too close to the "
        f"{elevenlabs.PROVIDER_IDLE_CUT_S}s cut")


async def test_a_tool_that_times_out_still_closes_its_event_pair():
    """Otherwise the client's "consulting the agenda..." spinner never stops."""
    session = FakeSession()

    async def never_returns(name, arguments):
        await asyncio.sleep(3600)

    await _shared.run_tool(never_returns, "lookup", {},
                           session=session, tool_call_id="t1", timeout_s=0.05)

    assert session.emitted("llm-function-call-started")
    assert session.emitted("llm-function-call-stopped"), (
        "the pair was left open, so the spinner never stops")


# ---------------------------------------------------------------------------
# Transcripts and the silence clock
# ---------------------------------------------------------------------------


def test_only_the_person_stops_the_silence_clock():
    """If the bot stopped it, an abandoned line would never be detected."""
    session = FakeSession()
    report = _shared.TranscriptReporter(session)

    report("assistant", "hello")
    assert "note_caller_activity" not in session.speech.calls

    report("user", "hi there")
    assert "note_caller_activity" in session.speech.calls


def test_the_clock_follows_the_current_speech_turn():
    """The reporter reads `session.speech` on every report, not once.

    No adapter in this library swaps it (the three keep
    `self.speech = session.speech`), but `Session` documents the swap as the
    way to install a different speaking turn, and the reporter has to follow
    it. Holding a reference taken at construction would leave the clock
    stopped for the rest of the call, without anyone noticing.
    """
    session = FakeSession()
    report = _shared.TranscriptReporter(session)

    fresh = type(session.speech)()
    session.speech = fresh

    report("user", "hi there")

    assert "note_caller_activity" in fresh.calls


def test_a_broken_transcript_hook_does_not_bring_down_the_call():
    """Instrumentation accompanies the call, it does not rule over it."""
    def broken(role, text, final):
        raise RuntimeError("the panel went down")

    report = _shared.TranscriptReporter(FakeSession(), broken)

    report("user", "hi there")     # does not raise


def test_an_empty_transcript_is_not_published():
    transcripts = []
    report = _shared.TranscriptReporter(
        FakeSession(), lambda role, text, final: transcripts.append(text))

    report("user", "")

    assert transcripts == []


# ---------------------------------------------------------------------------
# Transcripts also travel as standard events
# ---------------------------------------------------------------------------
#
# The hook is for whoever writes code. The event is for whoever wrote a client
# against the standard and expects to receive it: we announce the protocol, so
# a call that publishes nothing is a promise we are not keeping.


def test_what_the_caller_says_travels_as_a_standard_event():
    session = FakeSession()
    report = _shared.TranscriptReporter(session)

    report("user", "I want an appointment", final=False)
    report("user", "I want an appointment on Tuesday", final=True)

    published = session.emitted("user-transcription")
    assert [e.data["text"] for e in published] == [
        "I want an appointment", "I want an appointment on Tuesday"]
    assert [e.data["final"] for e in published] == [False, True], (
        "a partial gets corrected as the person keeps talking, and a client "
        "that treats it as closed paints the same phrase twice")


def test_the_bot_text_splits_into_the_two_types_the_standard_has():
    """One type for the fragment as it is pronounced, another for the closed
    turn. Publishing both under the same name makes a panel unable to tell
    what it can still replace from what is already final."""
    session = FakeSession()
    report = _shared.TranscriptReporter(session)

    report("assistant", "Hello, ", final=False)
    report("assistant", "Hello, thanks for calling", final=True)

    assert [e.data["text"] for e in session.emitted("bot-tts-text")] == ["Hello, "]
    assert [e.data["text"] for e in session.emitted("bot-output")] == [
        "Hello, thanks for calling"]


async def test_a_tool_is_framed_by_a_pair_of_events():
    """The opening one is what lets a panel paint 'checking the calendar...'
    while it waits. Without the closing one that wait never ends on screen."""
    session = FakeSession()

    async def handler(name, arguments):
        return {"ok": True, "time": "10:30"}

    result = await _shared.run_tool(
        handler, "check_availability", {"day": "tuesday"},
        session=session, tool_call_id="call-1")

    assert result == {"ok": True, "time": "10:30"}

    started = session.emitted("llm-function-call-started")
    stopped = session.emitted("llm-function-call-stopped")
    assert len(started) == len(stopped) == 1
    assert started[0].data["function_name"] == "check_availability"
    assert started[0].data["arguments"] == {"day": "tuesday"}
    assert stopped[0].data["result"] == {"ok": True, "time": "10:30"}
    assert started[0].data["tool_call_id"] == stopped[0].data["tool_call_id"], (
        "the pair is matched by id, because a model can ask for several at once")


async def test_a_tool_that_fails_still_closes_its_pair():
    """Otherwise the panel is left showing a spinner for a tool that already
    came back, and the call looks stuck when it is not."""
    session = FakeSession()

    async def explodes(name, arguments):
        raise RuntimeError("the CRM is down")

    result = await _shared.run_tool(
        explodes, "schedule", {}, session=session, tool_call_id="call-2")

    assert result["ok"] is False
    assert len(session.emitted("llm-function-call-stopped")) == 1


def test_a_provider_error_reaches_the_client_and_not_just_the_log():
    """A client that only sees the conversation cannot explain why the bot
    went quiet. It is not fatal: the call goes on."""
    session = FakeSession()

    _shared.report_provider_error(session, {"code": "rate_limit"})

    published = session.emitted("error")
    assert len(published) == 1
    assert published[0].data["fatal"] is False
    assert "rate_limit" in published[0].data["error"]


# ---------------------------------------------------------------------------
# The provider peephole
# ---------------------------------------------------------------------------


def test_the_peephole_gets_the_whole_event_not_just_its_name():
    """An event you only know the name of is of no use: whoever wants to reach
    something the adapter does not translate needs its contents."""
    seen = []
    watch = _shared.ProviderEvents(lambda kind, payload: seen.append((kind, payload)),
                                   "Provider")

    watch("something.brand.new", {"type": "something.brand.new", "value": 42})

    assert seen == [("something.brand.new",
                     {"type": "something.brand.new", "value": 42})]


def test_a_broken_peephole_does_not_bring_down_the_call():
    """Watching does not rule over what is watched."""
    def explodes(kind, payload):
        raise RuntimeError("the integrator's code has a bug")

    watch = _shared.ProviderEvents(explodes, "Provider")

    watch("anything", {})        # does not raise


def test_without_a_peephole_nothing_happens():
    _shared.ProviderEvents(None, "Provider")("anything", {})


def test_an_untranslated_type_is_logged_once_per_call(caplog):
    """A periodic event and a brand new one look the same and are not.

    A new one shows up once and you want to see it; a periodic one (a latency
    report, a heartbeat) arrives every second and you only need to know it
    exists. Deepgram sends `LatencyReport` about once a second, so without this
    guard a one-minute call buries every other line under sixty copies of it,
    and the line that mattered is the one that gets buried.
    """
    watch = _shared.ProviderEvents(None, "Provider")

    with caplog.at_level("DEBUG"):
        for _ in range(60):
            watch.note_unhandled("LatencyReport")
        watch.note_unhandled("SomethingBrandNew")

    logged = [r for r in caplog.records if "untranslated event" in r.getMessage()]
    assert len(logged) == 2, "one line per type, not per event"
    assert "SomethingBrandNew" in logged[-1].getMessage(), (
        "the new one is not buried by the repeated one")


def test_the_hook_still_gets_every_single_event(caplog):
    """Quieting the log does not quiet the hook: whoever integrates decides
    what to do with a repeated event, and needs all of them to count or
    average."""
    seen = []
    watch = _shared.ProviderEvents(lambda kind, payload: seen.append(payload),
                                   "Provider")

    for i in range(5):
        watch("LatencyReport", {"stt_latency": i})
        watch.note_unhandled("LatencyReport")

    assert len(seen) == 5, "the hook receives all of them"


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


async def test_shutdown_cancels_and_awaits_the_tasks():
    """Cancelling without awaiting leaves code running after the call was
    considered closed."""
    async def forever():
        await asyncio.sleep(3600)

    task = asyncio.create_task(forever())
    socket = FakeSocket()

    await _shared.close_quietly(socket, (task, None))

    assert task.cancelled()
    assert socket.closed


async def test_shutdown_from_inside_a_listed_task_does_not_deadlock():
    """An adapter that closes from its own reader lists that reader in
    `tasks`. Measured before the fix: the task cancelled itself, then
    gathered itself, and never returned; the socket stayed open. Today no
    adapter does this from `on_gone` (`session.hangup()` only sends the
    command), so this guards the trap, not a live path.
    """
    socket = FakeSocket()
    state = {}

    async def closes_itself():
        await _shared.close_quietly(socket, (state["me"], None))
        state["finished"] = True

    state["me"] = asyncio.create_task(closes_itself())
    await asyncio.wait_for(state["me"], timeout=2.0)

    assert state.get("finished") is True
    assert socket.closed


async def test_shutdown_tolerates_a_socket_that_is_already_gone():
    class BrokenSocket(FakeSocket):
        async def close(self):
            raise RuntimeError("already gone")

    await _shared.close_quietly(BrokenSocket(), ())     # does not raise


# ---------------------------------------------------------------------------
# The connection and its failures: shared by all three adapters
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


async def test_a_rejected_credential_gives_a_clear_error(monkeypatch):
    """A 401/403 is not fixed by retrying: it is translated into a message that
    points at the credential, not the raw websockets traceback."""
    import websockets

    async def rejects(*a, **k):
        raise websockets.InvalidStatus(_FakeResponse(401))

    monkeypatch.setattr(_shared.websockets, "connect", rejects)

    with pytest.raises(RuntimeError) as failure:
        await _shared.connect("wss://x", {}, "Deepgram", "Check your key.")

    assert "Deepgram" in str(failure.value)
    assert "credential" in str(failure.value)
    assert "Check your key." in str(failure.value)


async def test_another_http_status_is_reported_with_its_code(monkeypatch):
    import websockets

    async def responds_500(*a, **k):
        raise websockets.InvalidStatus(_FakeResponse(500))

    monkeypatch.setattr(_shared.websockets, "connect", responds_500)

    with pytest.raises(RuntimeError, match="500"):
        await _shared.connect("wss://x", {}, "OpenAI", "hint")


async def test_a_network_failure_says_what_to_check(monkeypatch):
    async def network_down(*a, **k):
        raise OSError("Name or service not known")

    monkeypatch.setattr(_shared.websockets, "connect", network_down)

    with pytest.raises(RuntimeError, match="internet access"):
        await _shared.connect("wss://x", {}, "ElevenLabs", "hint")


# ---------------------------------------------------------------------------
# The provider that drops mid-call
# ---------------------------------------------------------------------------


class _ClosingIter:
    """A socket that cuts the connection as soon as you start reading it."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise self._exc


async def test_provider_dropping_mid_call_hangs_up():
    """A provider that closes mid-call cannot leave the line open and mute:
    read_until_closed fires on_gone, which is where the call is hung up."""
    import websockets

    hung_up = []

    async def on_text(msg):
        pass

    async def on_gone():
        hung_up.append(True)

    await _shared.read_until_closed(
        _ClosingIter(websockets.ConnectionClosed(None, None)),
        on_text, on_gone=on_gone,
    )

    assert hung_up == [True]


async def test_an_unexpected_read_error_still_hangs_up(caplog):
    """Any failure of the read loop ends in on_gone, so the call is not left
    hung up in silence."""
    hung_up = []

    async def on_text(msg):
        pass

    async def on_gone():
        hung_up.append(True)

    await _shared.read_until_closed(
        _ClosingIter(RuntimeError("something odd")),
        on_text, on_gone=on_gone,
    )

    assert hung_up == [True]


# ---------------------------------------------------------------------------
# The silence filler for a quiet channel
# ---------------------------------------------------------------------------


def _filler(sent: list, on_send=None) -> _shared.SilenceFiller:
    async def send(frame: bytes) -> None:
        if on_send is not None:
            on_send()
        sent.append(frame)

    return _shared.SilenceFiller(send, silence_byte=0xFF, frame_size=160,
                                 ptime_ms=20)


async def test_the_filler_is_silence_paced_at_ptime():
    """The providers' pipelines only advance with incoming audio (measured:
    after a phrase, no reply ever comes without more audio), so a quiet
    channel gets one silence frame per ptime, like a phone without VAD."""
    sent: list = []
    filler = _filler(sent)
    fake = FakeTime(filler, steps=4)
    fake.quiet_for(1.0)

    await filler.run()

    assert sent == [b"\xff" * 160] * 3
    # One frame, then one ptime of wait, never a burst.
    assert [round(s, 3) for s in fake.sleeps] == [0.3, 0.02, 0.02, 0.02]


async def test_the_filler_pacing_is_absolute():
    """Each frame is due one ptime after the previous one, not one ptime
    after the send returned: a send that costs 5 ms leaves 15 ms to sleep,
    so the filler never drifts behind real time."""
    sent: list = []
    fake = None

    def slow_send():
        fake.now += 0.005

    filler = _filler(sent, on_send=slow_send)
    fake = FakeTime(filler, steps=4)
    fake.quiet_for(1.0)

    await filler.run()

    assert [round(s, 3) for s in fake.sleeps] == [0.3, 0.015, 0.015, 0.015]


async def test_the_filler_stops_on_the_first_real_frame():
    """The last real frame is checked before EVERY filler frame: once the
    caller's audio is back, at most one filler frame was already in flight."""
    sent: list = []
    filler = _filler(sent)

    async def channel(n):
        if n == 1:                      # right after the first filler frame
            sent.append("real")
            filler.note_caller_frame()

    fake = FakeTime(filler, steps=3, on_sleep=channel)
    fake.quiet_for(1.0)

    await filler.run()

    assert sent[sent.index("real") + 1:] == [], \
        "filler kept going after the caller's audio came back"


async def test_no_filler_while_the_channel_delivers():
    """A channel that keeps delivering frames never sees a filler frame."""
    sent: list = []
    filler = _filler(sent)

    async def channel(n):
        filler.note_caller_frame()      # a real frame before every sleep

    FakeTime(filler, steps=5, on_sleep=channel)

    await filler.run()

    assert sent == []


async def test_the_filler_ends_cleanly_if_the_socket_is_gone():
    """If the send fails, the loop exits without breaking: it is the
    adapter's reader that hangs up the call."""
    async def dead(frame: bytes) -> None:
        raise websockets.ConnectionClosed(None, None)

    filler = _shared.SilenceFiller(dead, silence_byte=0xFF, frame_size=160,
                                   ptime_ms=20)
    fake = FakeTime(filler, steps=10)
    fake.quiet_for(1.0)

    await filler.run()                  # does not raise

    assert fake.sleeps == [0.3], "it should return on the first failed send"
