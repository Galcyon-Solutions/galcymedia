"""Protocol tests.

Each test pins a real error, not a line of coverage: what it fixes is said
in its docstring, with the source line where the channel behaves that way.
"""

from __future__ import annotations

import json

import pytest

from galcymedia import (
    MAX_CONTROL_MESSAGE_BYTES,
    Command,
    Event,
    FrameAligner,
    build_command,
    parse_event,
    parse_media_start,
)
from galcymedia.protocol import codec_name_for, sample_rate_for, silence_byte_for

# ---------------------------------------------------------------------------
# Event parsing
# ---------------------------------------------------------------------------


def test_parses_a_normal_event():
    event, payload = parse_event('{"event":"MEDIA_XOFF"}')
    assert event is Event.MEDIA_XOFF
    assert payload["event"] == "MEDIA_XOFF"


def test_a_channel_variable_does_not_trigger_another_event():
    """The `event` field decides, not a substring of the whole frame.

    MEDIA_START carries ALL the channel variables; one that mentions another
    event makes a substring comparison take the wrong branch. See
    `docs/decisions.md`.
    """
    raw = json.dumps({
        "event": "MEDIA_START",
        "channel_variables": {"REASON": "the customer sent MEDIA_XOFF and HANGUP"},
    })
    event, _ = parse_event(raw)
    assert event is Event.MEDIA_START, "a variable cannot change the event"


def test_an_unknown_event_does_not_blow_up():
    """A newer channel can add events; an old application keeps working."""
    event, payload = parse_event('{"event":"SOMETHING_NOT_YET_INVENTED"}')
    assert event is None
    assert payload["event"] == "SOMETHING_NOT_YET_INVENTED"


@pytest.mark.parametrize("garbage", [
    "not json",
    "",
    "[1,2,3]",              # valid JSON but not an object
    '{"no_event_field":1}',
])
def test_garbage_returns_none_without_raising(garbage):
    """Nothing arriving over the socket can take down the process."""
    event, _ = parse_event(garbage)
    assert event is None


# ---------------------------------------------------------------------------
# MEDIA_START
# ---------------------------------------------------------------------------


def test_complete_media_start():
    media = parse_media_start({
        "connection_id": "abc-123",
        "channel_id": "c1",
        "channel": "PJSIP/1001-00000001",
        "format": "alaw",
        "optimal_frame_size": 160,
        "ptime": 20,
        "channel_variables": {"AI_PROVIDER": "deepgram", "AI_LANGUAGE": "es"},
    })
    assert media.audio_format == "alaw"
    assert media.optimal_frame_size == 160
    assert media.provider == "deepgram"
    assert media.language == "es"


def test_a_variable_the_library_does_not_know_arrives_untouched():
    """The dialplan owns which variables exist; the library only forwards them.

    Asterisk already gives inheritance with the leading underscore, so the
    library does not need to invent named accessors: whatever the dialplan
    seeds arrives in channel_variables, known or not. Adding a property for
    each one would be a second, narrower list that has to be kept in sync
    with a dialplan nobody controls.
    """
    media = parse_media_start({"channel_variables": {
        "AI_PROVIDER": "deepgram",
        "CRM_TIER": "gold",          # nothing in the library knows this one
        "CAMPAIGN": "black-friday",
    }})
    assert media.provider == "deepgram"
    assert media.channel_variables["CRM_TIER"] == "gold"
    assert media.channel_variables["CAMPAIGN"] == "black-friday"


def test_media_start_without_variables_falls_back_to_echo():
    """The underscore gotcha: `Set(AI_PROVIDER=x)` without the inheritance
    prefix never arrives (`channel.c:6809`), and the default is ECHO.

    Echo needs no keys and no internet, so an incomplete extension still
    answers. A real provider as default dies with an authentication error
    blaming a key when what was missing was a dialplan line.
    """
    media = parse_media_start({"channel": "PJSIP/1001-1", "format": "alaw"})
    assert media.provider == "echo"
    assert media.channel_variables == {}


def test_a_missing_frame_size_falls_back_to_160():
    """A missing field is different from a value of zero."""
    for value in (None, "", "not-a-number"):
        media = parse_media_start({"optimal_frame_size": value})
        assert media.optimal_frame_size == 160
        assert not media.passthrough


def test_zero_means_passthrough_and_is_preserved():
    """The channel signals passthrough with optimal_frame_size=0
    (`chan_websocket.c:1428`).

    Masking it with 160 loses the only signal that marks, flushing and
    buffering will be rejected. The aligner accepts the zero: nothing to
    align in that mode.
    """
    media = parse_media_start({"optimal_frame_size": 0})
    assert media.optimal_frame_size == 0
    assert media.passthrough

    aligner = FrameAligner(media.optimal_frame_size, media.silence_byte)
    assert aligner.push(b"\xd5" * 37) == [b"\xd5" * 37], "passes through as-is"


def test_the_provider_is_normalized():
    media = parse_media_start({"channel_variables": {"AI_PROVIDER": "  DeepGram  "}})
    assert media.provider == "deepgram"


def test_the_silence_byte_is_derived_from_the_codec():
    """Silence depends on the codec: zeros are a click in G.711 (`pcm.py`).
    alaw=0xD5, ulaw=0xFF, every slin (linear PCM)=0x00, derived from the
    format so alignment pads right without the adapter knowing."""
    expected = {
        "alaw": 0xD5,
        "ulaw": 0xFF,
        "slin": 0x00, "slin16": 0x00, "slin24": 0x00, "slin48": 0x00,
    }
    for audio_format, silence in expected.items():
        media = parse_media_start({"format": audio_format})
        assert media.silence_byte == silence, \
            f"{audio_format} should give 0x{silence:02X}"


def test_an_unknown_format_falls_back_to_pcm_silence():
    """A codec we do not recognize falls back to 0x00, real PCM silence: at
    worst a tick, not a continuous tone."""
    media = parse_media_start({"format": "some-future-codec"})
    assert media.silence_byte == 0x00


def test_the_format_tables_only_hold_names_asterisk_writes():
    """The channel writes `format` with `ast_format_get_name`
    (`chan_websocket.c:236`): `ulaw`, `alaw`, `slin*`. An alias like `pcma`
    never arrives, so a table entry for it is dead and documents a format
    that does not exist on this wire. The adapters already dropped them."""
    from galcymedia import protocol
    from galcymedia.adapters import _shared

    for table in (protocol._SILENCE_BY_FORMAT, protocol._SAMPLE_RATE_BY_FORMAT):
        dead = set(table) - _shared.ASTERISK_AUDIO_FORMATS
        assert not dead, f"keys Asterisk never writes: {dead}"


def test_silence_is_case_insensitive():
    assert parse_media_start({"format": "SLIN16"}).silence_byte == 0x00
    assert parse_media_start({"format": "ALAW"}).silence_byte == 0xD5


def test_the_sample_rate_is_derived_from_the_codec():
    """Anyone wiring their own speech engine needs this number.

    A speech engine reads its rate once at startup and never checks again, so
    a wrong value raises nothing: it just plays the voice at the wrong speed.
    Measured: a 16 kHz pipeline over an 8 kHz channel plays one second of
    speech in two.
    """
    expected = {
        "alaw": 8000,
        "ulaw": 8000,
        "slin": 8000, "slin12": 12000, "slin16": 16000, "slin24": 24000,
        "slin32": 32000, "slin48": 48000, "slin96": 96000,
        "slin192": 192000,
    }
    for audio_format, rate in expected.items():
        media = parse_media_start({"format": audio_format})
        assert media.sample_rate == rate, f"{audio_format} should be {rate} Hz"


def test_slin44_is_44100_hz_and_not_44000():
    """The one format whose name lies: `slin44` is 44100 Hz
    (`main/codec_builtin.c:371`).

    Multiplying the number in the name by 1000 works for every other slin
    and fails here without raising. A published project does exactly that
    (`docs/decisions.md`).
    """
    assert parse_media_start({"format": "slin44"}).sample_rate == 44100


def test_the_sample_rate_of_an_unknown_format_falls_back_to_telephony():
    """A codec we do not know falls back to 8000, never to zero: zero travels
    into the pipeline and blows up at resampling time, far from the cause."""
    assert parse_media_start({"format": "some-future-codec"}).sample_rate == 8000
    assert parse_media_start({"format": "SLIN16"}).sample_rate == 16000


# ---------------------------------------------------------------------------
# Every fallback warns: a guess about the audio is never silent
# ---------------------------------------------------------------------------
#
# Each default replaces a fact the channel should have sent with a guess, and
# a wrong guess "works" sounding bad. The warning carries what arrived and
# what was assumed.


def test_an_unknown_format_warns_about_the_rate_it_assumes(caplog):
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        assert sample_rate_for("some-future-codec") == 8000
    assert "'some-future-codec'" in caplog.text and "8000" in caplog.text


def test_an_unknown_format_warns_about_the_silence_it_assumes(caplog):
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        assert silence_byte_for("some-future-codec") == 0x00
    assert "'some-future-codec'" in caplog.text and "0x00" in caplog.text


def test_a_known_format_does_not_warn(caplog):
    """The warning is for the guess, not for every call."""
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        sample_rate_for("SLIN16")
        silence_byte_for("ulaw")
        silence_byte_for("slin48")
    assert caplog.text == ""


def test_a_rate_without_a_codec_warns_about_slin16(caplog):
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        assert codec_name_for(11025) == "slin16"
    assert "11025" in caplog.text and "slin16" in caplog.text


@pytest.mark.parametrize("value", [None, ""])
def test_a_missing_frame_size_warns_with_what_arrived(caplog, value):
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        assert parse_media_start({"format": "alaw", "ptime": 20,
                                  "optimal_frame_size": value}).optimal_frame_size == 160
    assert f"({value!r})" in caplog.text and "160" in caplog.text


@pytest.mark.parametrize("value", [None, "", 0, -3])
def test_a_missing_or_non_positive_ptime_warns_with_what_arrived(caplog, value):
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        assert parse_media_start({"format": "alaw", "optimal_frame_size": 160,
                                  "ptime": value}).ptime == 20
    assert f"({value!r})" in caplog.text and "20" in caplog.text


def test_a_missing_format_warns_and_assumes_alaw(caplog):
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        media = parse_media_start({"optimal_frame_size": 160, "ptime": 20})
    assert media.audio_format == "alaw"
    assert "(None)" in caplog.text and "alaw" in caplog.text


def test_a_complete_media_start_does_not_warn(caplog):
    with caplog.at_level("WARNING", logger="galcymedia.protocol"):
        parse_media_start({"format": "ulaw", "optimal_frame_size": 160, "ptime": 20})
    assert caplog.text == ""


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def test_simple_command():
    assert json.loads(build_command(Command.HANGUP)) == {"command": "HANGUP"}


def test_command_with_correlation_id():
    output = json.loads(build_command(Command.MARK_MEDIA, correlation_id="m1"))
    assert output == {"command": "MARK_MEDIA", "correlation_id": "m1"}


def test_an_overlong_command_raises_instead_of_vanishing():
    """The 128-byte limit (`chan_websocket.c:144`).

    Asterisk drops a longer control message with a warning in its own log
    and nothing on the socket (`:942-945`): a command that does nothing and
    no clue why. Failing here is louder.
    """
    with pytest.raises(ValueError, match="128"):
        build_command(Command.MARK_MEDIA, correlation_id="x" * 200)


def test_the_limit_is_measured_in_BYTES_not_characters():
    """An accented character takes two bytes in UTF-8.

    Measuring with len() of the string instead of len() of the bytes lets
    through messages that Asterisk later discards.
    """
    # 60 accented characters = 120 bytes, plus the JSON wrapper, over the limit.
    with pytest.raises(ValueError):
        build_command(Command.MARK_MEDIA, correlation_id="á" * 60)


def test_null_parameters_are_not_serialized():
    output = json.loads(build_command(Command.MARK_MEDIA, correlation_id=None))
    assert "correlation_id" not in output


def test_every_channel_command_fits_within_the_limit():
    """None of the 11 commands can exceed the limit on its own."""
    for command in Command:
        encoded = build_command(command)
        assert len(encoded.encode("utf-8")) <= MAX_CONTROL_MESSAGE_BYTES


# ---------------------------------------------------------------------------
# Non-finite numbers: json.loads accepts them, int() rejects them
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("non_finite", [float("inf"), float("-inf"), float("nan")])
def test_a_non_finite_ptime_does_not_blow_up_and_falls_back_to_20(non_finite):
    """`json.loads` accepts Infinity/-Infinity/NaN (a non-standard extension
    Python enables), so a valid MEDIA_START can carry a non-finite `ptime`.
    `int(float('inf'))` raises OverflowError, not a ValueError: without the
    prior check one text frame kills the call. It falls back to 20 ms.
    """
    media = parse_media_start({"ptime": non_finite})
    assert media.ptime == 20


@pytest.mark.parametrize("non_finite", [float("inf"), float("-inf"), float("nan")])
def test_a_non_finite_frame_size_does_not_blow_up_and_falls_back_to_160(non_finite):
    """Same case as ptime but via `optimal_frame_size`: a non-finite value
    would take down the call in `int(inf)`. It is clamped to the usual size of
    160 bytes (alaw/ulaw at 20 ms) instead of blowing up.
    """
    media = parse_media_start({"optimal_frame_size": non_finite})
    assert media.optimal_frame_size == 160
    assert not media.passthrough, "the default is not passthrough"


def test_a_media_start_with_infinity_from_the_socket_does_not_blow_up():
    """The same attack entering through the real text-frame parsing.

    `json.loads` accepts the literal `Infinity` unquoted, so this JSON
    arrives as-is from the socket; both fields fall back to their default
    and the call survives.
    """
    raw = '{"event":"MEDIA_START","ptime":Infinity,"optimal_frame_size":Infinity}'
    _, payload = parse_event(raw)
    media = parse_media_start(payload)
    assert media.ptime == 20
    assert media.optimal_frame_size == 160


# ---------------------------------------------------------------------------
# parse_event against non-str types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("non_str", [None, 123, {"event": "x"}, [1, 2, 3], 4.5])
def test_parse_event_tolerates_non_text_input(non_str):
    """Any type returns (None, {}) without raising.

    The error handler logs `repr(raw)[:120]`, not a slice of raw: a slice
    over None, an int or a dict blows up INSIDE the except and turns an
    ignorable frame into a call crash.
    """
    event, payload = parse_event(non_str)
    assert event is None
    assert payload == {}


def test_a_deeply_nested_json_does_not_trigger_recursionerror():
    """The stdlib recursive parser runs out of stack with thousands of `[`.

    Ten thousand open brackets are a few KB, well under the message limit,
    so an attacker can send them. Without RecursionError in the except that
    frame kills the call; here it is ignored.
    """
    bomb = '{"event":"STATUS","x":' + "[" * 10000 + "]" * 10000 + "}"
    event, payload = parse_event(bomb)
    assert event is None
    assert payload == {}
