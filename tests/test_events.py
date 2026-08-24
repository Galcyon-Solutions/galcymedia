"""
Tests of the RTVI events protocol.

A malformed event does not raise anything: the client simply ignores it and the
information never appears on screen. That is why these tests compare against the
real specification: the `models.py` of the RTVI module in the installed
`pipecat-ai` package (extra `[pipecat]`).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from galcymedia import events
from galcymedia.events import LABEL, PROTOCOL_VERSION, EventType


def data(event) -> dict:
    return json.loads(event.to_json())


# ---------------------------------------------------------------------------
# The envelope
# ---------------------------------------------------------------------------


def test_the_envelope_carries_the_four_fields():
    """label, type, id and data. An RTVI client discards anything missing them."""
    d = data(events.bot_interrupted())
    assert set(d) == {"label", "type", "id", "data"}


def test_the_label_is_always_rtvi_ai():
    """It is not configurable. It is what identifies the protocol."""
    assert LABEL == "rtvi-ai"
    for event in (events.bot_ready("x"), events.error("x"),
                  events.user_started_speaking()):
        assert data(event)["label"] == "rtvi-ai"


def test_each_event_carries_its_own_id():
    """It serves to correlate and to discard duplicates."""
    ids = {data(events.bot_interrupted())["id"] for _ in range(20)}
    assert len(ids) == 20


def test_the_json_does_not_escape_accents():
    """The audience is LATAM. A `\\u00f3` on screen looks bad."""
    raw = events.bot_output("¿Como estas? Mañana te llamo").to_json()
    assert "Mañana" in raw
    assert "\\u" not in raw


# ---------------------------------------------------------------------------
# The fields of each message, against the specification
# ---------------------------------------------------------------------------


def test_bot_ready_announces_the_protocol_version():
    d = data(events.bot_ready("deepgram", language="es"))
    assert d["data"]["version"] == PROTOCOL_VERSION == "2.1.0"
    assert d["data"]["about"]["provider"] == "deepgram"
    assert d["data"]["about"]["language"] == "es"


def test_user_transcription_carries_the_four_standard_fields():
    """UserTranscriptionMessageData: text, user_id, timestamp, final."""
    d = data(events.user_transcription("hello"))["data"]
    assert set(d) == {"text", "user_id", "timestamp", "final"}
    assert d["final"] is True


def test_a_partial_transcription_is_marked_as_not_final():
    """A panel paints them in gray and replaces them.

    Treating them as final duplicates the text on screen.
    """
    assert data(events.user_transcription("ho", final=False))["data"]["final"] is False


def test_the_timestamp_is_iso_8601_utc():
    mark = data(events.user_transcription("x"))["data"]["timestamp"]
    assert mark.endswith("Z")
    assert len(mark) == 20            # 2026-08-08T18:15:36Z


def test_the_error_uses_the_error_field_and_not_message():
    """ErrorData from the source: `error: str`, `fatal: bool`.

    The web documentation says `message`. The source rules, and it says `error`.
    """
    d = data(events.error("the provider went down", fatal=True))["data"]
    assert d == {"error": "the provider went down", "fatal": True}
    assert "message" not in d


def test_a_non_fatal_error_is_the_normal_case():
    assert data(events.error("retrying"))["data"]["fatal"] is False


def test_bot_output_distinguishes_what_is_said_from_what_is_not():
    """`spoken=False` is text the bot produces and does NOT say out loud.

    It is the gateway to send the panel notes, labels or data for the CRM
    without them sounding on the call.
    """
    assert data(events.bot_output("hello"))["data"]["spoken"] is True
    internal = data(events.bot_output("angry customer", spoken=False))["data"]
    assert internal["spoken"] is False


def test_tool_calls_carry_tool_call_id():
    """Both ends carry `tool_call_id`, so a client can pair them.

    The standard makes it mandatory in `stopped` (`models.py:322`); `started`
    only defines `function_name` (`:257-264`) and ours adds it as an extra.
    `args` belongs to the deprecated model (`:190-202`), not here.
    """
    started = data(events.function_call_started(
        "check_balance", tool_call_id="call_1", arguments={"id": "44718822"},
    ))["data"]
    assert started["tool_call_id"] == "call_1"
    assert started["arguments"] == {"id": "44718822"}
    assert "args" not in started, "the field is called arguments, not args"

    stopped = data(events.function_call_stopped(
        "check_balance", tool_call_id="call_1", result={"balance": 120},
    ))["data"]
    assert stopped["tool_call_id"] == "call_1"
    assert stopped["cancelled"] is False


def test_metrics_admit_custom_measurements():
    """ttfb is from the standard; the rest is from the channel and a foreign client ignores it."""
    d = data(events.metrics(
        ttfb=[{"processor": "deepgram", "value": 1.562, "model": "aura"}],
        frames_in=705, silence_fills=20,
    ))["data"]
    assert d["ttfb"][0]["value"] == 1.562
    assert d["frames_in"] == 705


def test_server_message_accepts_anything():
    """It is the standard's wildcard: whatever is specific to each integration.

    Here travels what arrived in the SIP headers.
    """
    d = data(events.server_message({
        "variables": {"CUSTOMER_ID": "A-4471", "CAMPAIGN": "collections"},
    }))["data"]
    assert d["variables"]["CUSTOMER_ID"] == "A-4471"


@pytest.mark.parametrize("constructor", [
    events.user_started_speaking, events.user_stopped_speaking,
    events.bot_started_speaking, events.bot_stopped_speaking,
    events.bot_interrupted,
])
def test_the_turn_events_carry_no_data(constructor):
    """They mark an instant. The when is all the information."""
    assert data(constructor())["data"] == {}


# ---------------------------------------------------------------------------
# Against the specification of the installed Pipecat
# ---------------------------------------------------------------------------


def _rtvi_models_source() -> str:
    """The `models.py` of Pipecat's RTVI module, as installed.

    Skips the test when the extra is not installed: the comparison is optional,
    and it is also a trap, because a skip checks nothing without saying so.
    """
    try:
        from pipecat.processors.frameworks import rtvi
    except ImportError:
        pytest.skip("pipecat-ai is not installed (extra [pipecat])")

    models = Path(rtvi.__file__).parent / "models.py"
    if not models.exists():
        pytest.skip("the installed pipecat-ai has no rtvi/models.py")
    return models.read_text(encoding="utf-8")


def test_all_the_types_exist_in_the_specification():
    """Comparison against the real Pipecat source."""
    source = _rtvi_models_source()

    invented = [t.value for t in EventType if f'"{t.value}"' not in source]
    assert not invented, f"types that do not exist in the standard: {invented}"


def test_the_constants_match_the_specification():
    source = _rtvi_models_source()
    assert f'MESSAGE_LABEL = "{LABEL}"' in source
    assert f'PROTOCOL_VERSION = "{PROTOCOL_VERSION}"' in source


def test_bot_tts_text_is_the_bots_partial():
    """"Sent when text is being processed by TTS" (`models.py:474`): the
    counterpart of the user's final=False."""
    event = events.bot_tts_text("hello, tha")
    parsed = json.loads(event.to_json())
    assert parsed["type"] == "bot-tts-text"
    assert parsed["data"] == {"text": "hello, tha"}


def test_bot_transcription_still_exists_for_compatibility():
    """Pipecat still emits it for old clients, with its removal announced
    (`rtvi/observer.py:773`): removing it here would break those clients."""
    event = events.bot_transcription("hello")
    parsed = json.loads(event.to_json())
    assert parsed["type"] == "bot-transcription"


# ---------------------------------------------------------------------------
# Content-proof serialization: the `data` carries data we do not control, and
# since emit() swallows failures, a to_json() that blows up loses the event
# silently.
# ---------------------------------------------------------------------------


def test_the_bytes_of_a_sip_header_serialize_without_blowing_up():
    """Asterisk delivers channel variables as bytes, and `json.dumps` rejects
    them with TypeError. Since emit() swallows the failure, the event would be
    lost entirely. They are decoded to text and the JSON comes out valid.
    """
    raw = events.server_message({"raw": b"\x01\x02"}).to_json()
    d = json.loads(raw)          # does not raise: the JSON is valid
    assert d["data"]["raw"] == "\x01\x02"


def test_a_nan_metric_produces_json_the_strict_parser_accepts():
    """A non-finite float (NaN, inf) emits a literal that is NOT valid JSON per
    RFC 8259, and the client rejects it on receipt. `to_json` forces
    `allow_nan=False` and maps the non-finite to null, so Python's STRICT parser
    (which rejects NaN) accepts it.
    """
    raw = events.metrics(
        ttfb=[{"processor": "x", "value": float("nan"), "model": "m"}]).to_json()
    d = json.loads(raw)          # strict parser: would blow up with raw NaN
    assert d["data"]["ttfb"][0]["value"] is None, "the NaN became null"


def test_a_lone_surrogate_does_not_blow_up_when_encoding_the_frame():
    """An STT engine with broken unicode can insert a lone surrogate (\\ud800).
    That character passes `json.dumps` but blows up when encoding the frame in
    UTF-8, which is how it is sent to the socket. It is sanitized beforehand, so
    encoding the frame does not raise.
    """
    raw = events.user_transcription("bad" + chr(0xD800) + "text").to_json()
    raw.encode("utf-8")          # this is what used to blow up; now it does not raise


def test_a_none_transcription_produces_empty_text_not_null():
    """The RTVI schema requires `text: str`. An adapter that reports "no text" by
    passing None would produce `{"text": null}`, which a strict client discards
    and a lax one breaks on `text.trim()`. None maps to an empty string.
    """
    d = json.loads(events.user_transcription(None).to_json())["data"]
    assert d["text"] == "", "None is an empty string, not null"


def test_a_cyclic_payload_does_not_hang_serialization():
    """A dict that contains itself must not cause infinite recursion while
    sanitizing it. Since `emit()` swallows failures, a RecursionError here would
    lose the event silently, exactly what the sanitizing wants to avoid. It is
    cut off by depth."""
    cyclic = {"a": 1}
    cyclic["self"] = cyclic
    # Must not hang or blow up: it produces valid JSON with the cycle cut off.
    result = json.loads(events.server_message(cyclic).to_json())
    assert result["data"]["a"] == 1


def test_a_deeply_nested_payload_does_not_overflow_the_stack():
    """A hostile nesting (thousands of levels) must not overflow the stack while
    sanitizing it. It fits comfortably under a message limit, so an adapter with
    a bug could produce it."""
    deep = current = {}
    for _ in range(5000):
        current["x"] = {}
        current = current["x"]
    # Must not raise RecursionError.
    events.server_message(deep).to_json()


# ---------------------------------------------------------------------------
# The discoverable surface: describe_events()
#
# It answers "what events will reach me" without reading the source. Its risk is
# drifting away from reality: documentation that lies is worse than no
# documentation, so these tests tie the list to the module itself.
# ---------------------------------------------------------------------------


def _emitted_today() -> set[str]:
    """The events the library really emits, read off the source.

    Derived and not written by hand on purpose: a hand-kept list only catches
    the drift somebody remembers to update it for. This one greps the package
    for `events.<factory>(` calls outside the events module itself, so wiring
    up an event that nobody documented, or documenting one that nobody emits,
    fails the test on its own.
    """
    root = Path(events.__file__).parent
    factories = {
        name: getattr(events, name)
        for name in dir(events)
        if not name.startswith("_") and callable(getattr(events, name))
    }

    emitted = set()
    for path in root.rglob("*.py"):
        if path.name == "events.py":
            continue
        source = path.read_text(encoding="utf-8")
        for name in factories:
            if f"events.{name}(" in source:
                emitted.add(name)

    # From the factory name to the type it really produces, so a rename of
    # either side shows up here instead of quietly passing.
    produced = set()
    for name in emitted:
        for sample in (_SAMPLE_ARGS.get(name, ((), {})),):
            args, kwargs = sample
            produced.add(factories[name](*args, **kwargs).type.value)
    return produced


# The minimum arguments each factory needs to be called. Only the ones that
# take something: the rest are called with no arguments.
_SAMPLE_ARGS = {
    "bot_ready": (("provider",), {}),
    "user_transcription": (("x",), {}),
    "bot_transcription": (("x",), {}),
    "bot_output": (("x",), {}),
    "bot_tts_text": (("x",), {}),
    "function_call_started": (("f",), {"tool_call_id": "1"}),
    "function_call_stopped": (("f",), {"tool_call_id": "1"}),
    "error": (("x",), {}),
    "server_message": (({},), {}),
}


def test_every_entry_has_the_three_keys_filled_in():
    """An empty `when` or `data` is a row that documents nothing."""
    rows = events.describe_events()
    assert rows, "the list must not be empty"
    for row in rows:
        assert set(row) == {"type", "when", "data"}, row
        for key, value in row.items():
            assert isinstance(value, str), f"{key} must be a str"
            assert value.strip(), f"{key} is empty in {row}"


def test_every_documented_type_exists_in_the_enum():
    """Guards against a typo in the wire string: a documented type that is not
    in EventType is a type nobody will ever receive."""
    valid = {member.value for member in EventType}
    for row in events.describe_events():
        assert row["type"] in valid, f"{row['type']} is not an EventType"


def test_no_type_is_documented_twice():
    """A duplicate row means two contradicting descriptions of one event."""
    types = [row["type"] for row in events.describe_events()]
    assert len(types) == len(set(types)), types


def test_the_list_is_synchronized_with_what_the_library_emits():
    """The important one: the documented list must match reality.

    Two ways to lie, and both are checked. Documenting a type that is never
    emitted makes an integrator wait forever for an event that will not arrive;
    omitting one that is emitted sends them back to reading the source, which is
    exactly what this function exists to avoid.
    """
    documented = {row["type"] for row in events.describe_events()}
    emitted = _emitted_today()
    assert documented == emitted, (
        f"documented without being emitted: {documented - emitted}, "
        f"emitted without being documented: {emitted - documented}"
    )


def test_every_documented_type_is_produced_by_a_factory_that_works():
    """Each row must be backed by a factory that really builds that type.

    Every factory is called and the type it produces is read back, so renaming
    one or changing the type it builds breaks this test instead of silently
    leaving the documentation behind.
    """
    produced = _emitted_today()
    documented = {row["type"] for row in events.describe_events()}
    assert documented == produced, (
        f"documented without a factory: {documented - produced}, "
        f"factory without a row: {produced - documented}"
    )


def test_every_row_lists_exactly_the_fields_the_factory_emits():
    """The `data` column of a row names the keys the real event carries, no
    more and no less. A row that lists three of five fields (bot-output did)
    hides the other two from whoever reads the catalog instead of the source.

    `server-message` is skipped: its payload is free-form and the row says so
    in prose.
    """
    factories = {factory().type.value: factory for factory in (
        lambda: events.bot_ready("p"),
        lambda: events.user_transcription("x"),
        lambda: events.bot_output("x"),
        lambda: events.bot_tts_text("x"),
        lambda: events.function_call_started("f", tool_call_id="1"),
        lambda: events.function_call_stopped("f", tool_call_id="1"),
        lambda: events.metrics(ttfb=[{"processor": "p", "value": 0.1}]),
        lambda: events.error("x"),
        events.user_started_speaking, events.user_stopped_speaking,
        events.bot_started_speaking, events.bot_stopped_speaking,
        events.bot_interrupted,
    )}
    for row in events.describe_events():
        if row["type"] == "server-message":
            continue
        listed = set() if row["data"] == "no fields" else {
            name.strip() for name in row["data"].split(",")}
        # Keys as they go on the wire: `to_json` keeps nulls, so a `result`
        # of None is still a field the client receives.
        real = set(data(factories[row["type"]]())["data"])
        assert listed == real, f"{row['type']}: row {listed} vs event {real}"


def test_the_type_with_no_emission_site_stays_undocumented():
    """`bot-transcription` has a factory that nobody calls.

    It stays in the module because the standard still sends it to old clients,
    but documenting it would promise an event that never arrives. For anything
    new the standard points at `bot-output`, which we do emit.
    """
    documented = {row["type"] for row in events.describe_events()}
    assert "bot-transcription" not in documented


def test_describe_events_is_exported_from_the_package():
    """It is a discovery entry point: it has to be reachable as
    `from galcymedia import describe_events`."""
    import galcymedia

    assert "describe_events" in galcymedia.__all__
    assert galcymedia.describe_events() == events.describe_events()


def test_the_returned_list_is_not_shared_between_calls():
    """A caller that sorts or filters the result must not corrupt the next
    caller's copy."""
    first = events.describe_events()
    first.clear()
    assert events.describe_events(), "the second call came back empty"
