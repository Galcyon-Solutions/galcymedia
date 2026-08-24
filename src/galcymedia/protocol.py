"""The chan_websocket protocol.

This module speaks the channel's protocol and nothing else. It has no idea
voice providers exist, and keeping it that way is what lets a provider change
without touching anything here.

It speaks JSON, not the one-line legacy format, and the legacy format is not
a lesser option but a broken one: without `correlation_id` a MARK_MEDIA
cannot be paired with its MEDIA_MARK_PROCESSED (`chan_websocket.c:275,305`),
and that pairing is what lets the call hang up AFTER the goodbye played.
JSON is `control_message_format = json` in chan_websocket.conf or `f(json)`
in the dial string (`chan_websocket.c:64`). The whole protocol:
`docs/protocol.md`.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

log = logging.getLogger(__name__)


# Hard limit of the channel for control frames, the TEXT ones
# (`chan_websocket.c:144`). A longer one gets a warning in Asterisk's log and
# nothing on the socket (`:922-924`): the command just "does nothing", which
# is miserable to debug, so it is checked before sending.
MAX_CONTROL_MESSAGE_BYTES = 128

# Hard limit of any WebSocket message, audio included. Asterisk's receive
# buffer is 65535 (`http_websocket.h:105`) and a frame that does not fit
# closes the socket with 1009 (`res_http_websocket.c:672,693,721`), which
# hangs up the call (`chan_websocket.c:1089`). 65500 leaves room for the
# frame header.
MAX_WEBSOCKET_MESSAGE_BYTES = 65500

# Flow control thresholds compiled into the channel, not configurable
# (`chan_websocket.c:141-143`). They set the real time budget: the queue holds
# 1000 frames, 20 s of audio at 20 ms, which is why a garbage-collected
# language is fine for this job.
QUEUE_LENGTH_MAX = 1000
QUEUE_XOFF_LEVEL = 900
QUEUE_XON_LEVEL = 800

# The silence byte depends on the codec: padding blindly with zeros is an
# audible click in G.711 (`pcm.py`). Derived from the format so alignment
# pads right without the adapter knowing. Keys are the names the channel
# writes (`ast_format_get_name`, `chan_websocket.c:236`): never `pcma`,
# `pcmu` or `mulaw` (`test_the_format_tables_only_hold_names_asterisk_writes`).
_SILENCE_BY_FORMAT = {
    "alaw": 0xD5,
    "ulaw": 0xFF,
}


def silence_byte_for(audio_format: str) -> int:
    """The codec's silence byte, for padding incomplete frames.

    Args:
        audio_format: The channel format, any case.

    Returns:
        0xD5 for alaw, 0xFF for ulaw, 0x00 for every slin (linear PCM) and
        for a format this module does not know
        (`test_an_unknown_format_falls_back_to_pcm_silence`).
    """
    key = audio_format.strip().lower()
    if key not in _SILENCE_BY_FORMAT and not key.startswith("slin"):
        # A fallback is a guess about the audio: it is never silent
        # (`test_an_unknown_format_warns_about_the_silence_it_assumes`).
        log.warning("Unknown format %r, assuming PCM silence 0x00", audio_format)
    return _SILENCE_BY_FORMAT.get(key, 0x00)


# Samples per second of each channel format. G.711 is 8 kHz by definition;
# the linear PCM rates come from the Asterisk source (`main/format_cache.c`
# for the names, `main/codec_builtin.c:288-419` for the rates).
#
# The table is EXPLICIT on purpose: `slin44` is 44100 Hz
# (`codec_builtin.c:371`), not 44000, so multiplying the number in the name
# by 1000 fails exactly there and raises nothing. `docs/decisions.md` has a
# published project that does it that way.
_SAMPLE_RATE_BY_FORMAT = {
    "alaw": 8000,
    "ulaw": 8000,
    "slin": 8000,
    "slin12": 12000,
    "slin16": 16000,
    "slin24": 24000,
    "slin32": 32000,
    "slin44": 44100,
    "slin48": 48000,
    "slin96": 96000,
    "slin192": 192000,
}


def sample_rate_for(audio_format: str) -> int:
    """The samples per second the channel delivers with that format.

    Any speech engine needs this number: it reads its rate once and never
    looks again, so a wrong value raises nothing, the voice just comes out
    fast or slow. Measured: a 16 kHz pipeline on an 8 kHz channel plays one
    second of speech in two.

    Args:
        audio_format: The channel format, any case.

    Returns:
        The rate in Hz. An unknown format falls back to 8000, plain
        telephony
        (`test_the_sample_rate_of_an_unknown_format_falls_back_to_telephony`).
    """
    key = audio_format.strip().lower()
    if key not in _SAMPLE_RATE_BY_FORMAT:
        log.warning("Unknown format %r, assuming 8000 Hz", audio_format)
    return _SAMPLE_RATE_BY_FORMAT.get(key, 8000)


def codec_name_for(sample_rate: int) -> str:
    """The `Dial` codec that delivers that rate.

    The inverse of `sample_rate_for`, so a warning can say WHAT to write in
    the dialplan instead of leaving the integrator to look it up. Linear PCM
    wins: 8000 gives `slin`, not `alaw`, because a speech engine takes PCM.

    Args:
        sample_rate: Hz.

    Returns:
        The `slin*` name, or `slin16`, the one recommended for a pipeline,
        when no codec delivers that rate.
    """
    for name, rate in _SAMPLE_RATE_BY_FORMAT.items():
        if rate == sample_rate and name.startswith("slin"):
            return name
    log.warning("No channel codec delivers %r Hz, suggesting slin16", sample_rate)
    return "slin16"


class Command(str, Enum):
    """Commands the application sends to Asterisk. Case-sensitive: the
    channel matches with `strcmp` (`ast_strings_equal`).

    In passthrough (opus/g729/speex) the channel rejects 8 of the 11 with an
    ERROR event (`chan_websocket.c:678-687,740-846`); only ANSWER, HANGUP and
    GET_STATUS remain. See `docs/protocol.md`.
    """

    # Always available:
    ANSWER = "ANSWER"
    HANGUP = "HANGUP"
    GET_STATUS = "GET_STATUS"

    # Rejected in passthrough mode:
    REPORT_QUEUE_DRAINED = "REPORT_QUEUE_DRAINED"
    SET_MEDIA_DIRECTION = "SET_MEDIA_DIRECTION"
    START_MEDIA_BUFFERING = "START_MEDIA_BUFFERING"
    STOP_MEDIA_BUFFERING = "STOP_MEDIA_BUFFERING"
    MARK_MEDIA = "MARK_MEDIA"
    FLUSH_MEDIA = "FLUSH_MEDIA"
    PAUSE_MEDIA = "PAUSE_MEDIA"
    CONTINUE_MEDIA = "CONTINUE_MEDIA"


class Event(str, Enum):
    """Events Asterisk sends to the application: the channel's 9
    `create_event_*`, no more (`chan_websocket.c:198-430`)."""

    MEDIA_START = "MEDIA_START"
    DTMF_END = "DTMF_END"
    MEDIA_XOFF = "MEDIA_XOFF"
    MEDIA_XON = "MEDIA_XON"
    STATUS = "STATUS"
    MEDIA_BUFFERING_COMPLETED = "MEDIA_BUFFERING_COMPLETED"
    MEDIA_MARK_PROCESSED = "MEDIA_MARK_PROCESSED"
    QUEUE_DRAINED = "QUEUE_DRAINED"
    ERROR = "ERROR"


@dataclass(frozen=True)
class MediaStart:
    """The MEDIA_START event, parsed.

    Arrives once per call with everything the provider side needs
    (`chan_websocket.c:231-239`). `channel_variables` is the interesting
    part: the variables the dialplan set with the inheritance prefix
    (`channel.c:6809`), so `Set(_AI_PROVIDER=deepgram)` gets here and one
    process serves several providers without restarting.

    Audio can arrive BEFORE this event; whoever consumes this class has to
    tolerate it (`docs/protocol.md`).
    """

    connection_id: str
    channel_id: str
    channel_name: str
    audio_format: str
    optimal_frame_size: int
    ptime: int
    channel_variables: dict[str, str] = field(default_factory=dict)

    @property
    def passthrough(self) -> bool:
        """Whether the channel is in passthrough: opaque audio, 8 of 11
        commands rejected.

        This covers HALF of the case. A small-frame codec (the seven with
        `minimum_bytes <= 10` in `codec_builtin.c`: codec2, lpc10, g729,
        speex, speex16, speex32, opus) turns it on with nobody writing
        `p()` in the Dial, and sets `optimal_frame_size` to zero
        (`chan_websocket.c:1406-1408`). The `p()` option (`:1536`,
        `:1632-1634`) also turns it on but leaves the frame size alone, and
        MEDIA_START carries nothing else that says so (`:231-239`): the
        session learns that half from the channel's first `ERROR "... not
        supported in passthrough mode"` (`:681`,
        `test_a_p_option_dial_is_detected_from_the_channel_error`). Check
        before using marks: their ack never comes
        (`test_zero_means_passthrough_and_is_preserved`).
        """
        return self.optimal_frame_size == 0

    @property
    def silence_byte(self) -> int:
        """The silence byte of this call's codec: `silence_byte_for`."""
        return silence_byte_for(self.audio_format)

    @property
    def sample_rate(self) -> int:
        """Samples per second this call delivers: `sample_rate_for`.

        A speech engine of your own is configured with THIS number, not a
        constant.
        """
        return sample_rate_for(self.audio_format)

    @property
    def provider(self) -> str:
        """The provider the dialplan chose, `AI_PROVIDER`, normalized.

        The default is the echo, on purpose: it needs no keys and no
        internet, so an extension that forgot `Set(_AI_PROVIDER=...)`
        answers something instead of failing. A real provider as default
        looks more useful and is worse: the call dies with an authentication
        error blaming a key, when what was missing was a dialplan line
        (`test_media_start_without_variables_falls_back_to_echo`).
        """
        return self.channel_variables.get("AI_PROVIDER", "echo").strip().lower()

    @property
    def language(self) -> str:
        """`AI_LANGUAGE` from the dialplan, `es` when unset. `Session` hands
        it to the provider."""
        return self.channel_variables.get("AI_LANGUAGE", "es").strip()

    @property
    def caller(self) -> str:
        """`CALL_ORIGIN` from the dialplan, empty when unset. The channel does
        not send the caller id; the dialplan seeds it if it wants it here."""
        return self.channel_variables.get("CALL_ORIGIN", "")


def parse_event(raw: str) -> tuple[Event | None, dict[str, Any]]:
    """Parses a control frame.

    It parses the JSON and reads the `event` field instead of searching the
    name as a substring, which takes the wrong branch when a channel
    variable mentions another event (`docs/decisions.md`,
    `test_a_channel_variable_does_not_trigger_another_event`).

    Args:
        raw: The text frame. Any type is tolerated
            (`test_parse_event_tolerates_non_text_input`).

    Returns:
        The event and its payload. `(None, {})` when the frame is not a JSON
        object with an `event` field; `(None, payload)` for an event this
        version does not know.
    """
    # A log-safe fragment: repr() on ANY type, not a slice. If raw is not
    # sliceable (None, int, a dict), `raw[:120]` would blow up INSIDE the
    # error handler and turn an ignorable frame into a call crash.
    fragment = repr(raw)[:120]

    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError, RecursionError):
        # RecursionError on purpose: a few KB of deeply nested JSON overflow
        # the stdlib parser (`test_a_deeply_nested_json_does_not_trigger_recursionerror`).
        log.warning("Control frame is not valid JSON: %s", fragment)
        return None, {}

    if not isinstance(payload, dict):
        log.warning("Control frame is not a JSON object: %s", fragment)
        return None, {}

    name = payload.get("event")
    if name is None:
        log.warning("Control frame without an 'event' field: %s", fragment)
        return None, {}

    try:
        return Event(name), payload
    except ValueError:
        # Not fatal: the channel may add events and an old application has
        # to keep working (`test_an_unknown_event_does_not_blow_up`).
        log.info("Unknown event %r, ignored", name)
        return None, payload


# Clamp for `optimal_frame_size`: a frame never exceeds one WebSocket message.
# `FrameAligner` cuts passthrough chunks at this size, so an absurd value
# from a buggy channel or a hostile frame would pass oversized messages that
# close the socket (`framing.py`).
MAX_FRAME_SIZE = MAX_WEBSOCKET_MESSAGE_BYTES


def _frame_size(value: Any) -> int:
    """Reads `optimal_frame_size`, telling absent from zero.

    Zero is information: the channel marks passthrough with it. An absent
    field is another thing, and there the usual 20 ms telephony size
    applies, because assuming zero would switch frame alignment off
    (`test_a_missing_frame_size_falls_back_to_160`,
    `test_zero_means_passthrough_and_is_preserved`).

    Args:
        value: Whatever the payload carries.

    Returns:
        The size in bytes, clamped to `MAX_FRAME_SIZE`; 160 when absent or
        unreadable.
    """
    if value is None or value == "":
        log.warning("optimal_frame_size is missing (%r), using 160", value)
        return 160
    # json.loads accepts Infinity/-Infinity/NaN (a non-standard extension
    # Python enables), so a valid text frame can carry a non-finite float.
    # int(inf) raises OverflowError, a sibling of ValueError outside the
    # tuple: without this check it kills the call
    # (`test_a_non_finite_frame_size_does_not_blow_up_and_falls_back_to_160`).
    if isinstance(value, float) and not math.isfinite(value):
        log.warning("optimal_frame_size is not finite (%r), using 160", value)
        return 160
    try:
        size = int(value)
    except (TypeError, ValueError, OverflowError):
        log.warning("optimal_frame_size is unreadable (%r), using 160", value)
        return 160
    if size < 0:
        log.warning("optimal_frame_size is negative (%r), using 160", value)
        return 160
    if size > MAX_FRAME_SIZE:
        log.warning("optimal_frame_size is absurd (%r), clamped to %d",
                    value, MAX_FRAME_SIZE)
        return MAX_FRAME_SIZE
    return size


def _ptime(value: Any) -> int:
    """Reads `ptime` in milliseconds, tolerating garbage.

    As with the frame size: an unreadable `ptime` is a buggy channel or a
    hostile frame, and an unguarded `int("abc")` kills the whole call with
    one text frame.

    Args:
        value: Whatever the payload carries.

    Returns:
        Milliseconds; 20 when absent, unreadable or not positive.
    """
    if value is None or value == "":
        log.warning("ptime is missing (%r), using 20", value)
        return 20
    # Same as optimal_frame_size: int(inf) raises OverflowError
    # (`test_a_non_finite_ptime_does_not_blow_up_and_falls_back_to_20`).
    if isinstance(value, float) and not math.isfinite(value):
        log.warning("ptime is not finite (%r), using 20", value)
        return 20
    try:
        ms = int(value)
    except (TypeError, ValueError, OverflowError):
        log.warning("ptime is unreadable (%r), using 20", value)
        return 20
    if ms <= 0:
        log.warning("ptime is not positive (%r), using 20", value)
        return 20
    return ms


def parse_media_start(payload: dict[str, Any]) -> MediaStart:
    """Builds a `MediaStart` from a MEDIA_START payload.

    Args:
        payload: The parsed event. Missing fields get the telephony
            defaults: alaw, 160 bytes, 20 ms
            (`test_media_start_without_variables_falls_back_to_echo`).

    Returns:
        The frozen `MediaStart`.
    """
    variables = payload.get("channel_variables") or {}
    if not isinstance(variables, dict):
        variables = {}

    audio_format = payload.get("format")
    if audio_format is None or audio_format == "":
        # Every fallback here replaces a fact the channel should have sent
        # with a guess about the audio, and a wrong guess "works" sounding
        # bad. So each one warns with what arrived and what was assumed
        # (`test_a_missing_format_warns_and_assumes_alaw`).
        log.warning("MEDIA_START without format (%r), assuming alaw", audio_format)
        audio_format = "alaw"

    return MediaStart(
        connection_id=str(payload.get("connection_id", "")),
        channel_id=str(payload.get("channel_id", "")),
        channel_name=str(payload.get("channel", "")),
        audio_format=str(audio_format),
        # A zero HERE means passthrough (the channel sets it on purpose), NOT
        # an absent field: _frame_size tells the two apart.
        optimal_frame_size=_frame_size(payload.get("optimal_frame_size")),
        ptime=_ptime(payload.get("ptime")),
        channel_variables={str(k): str(v) for k, v in variables.items()},
    )


def build_command(command: Command, **params: Any) -> str:
    """Serializes a command to a JSON control frame.

    Args:
        command: The command.
        **params: Extra fields; `None` values are left out
            (`test_null_parameters_are_not_serialized`).

    Returns:
        Compact JSON.

    Raises:
        ValueError: If the result exceeds 128 bytes. Asterisk would drop it
            with nothing on the socket and the symptom would be a command
            that "does nothing" (`test_an_overlong_command_raises_instead_of_vanishing`).
    """
    message: dict[str, Any] = {"command": command.value}
    message.update({k: v for k, v in params.items() if v is not None})

    encoded = json.dumps(message, separators=(",", ":"))
    size = len(encoded.encode("utf-8"))

    if size > MAX_CONTROL_MESSAGE_BYTES:
        raise ValueError(
            f"Control message is {size} bytes and the limit is "
            f"{MAX_CONTROL_MESSAGE_BYTES}. Asterisk would drop it with no "
            f"reply. Command: {command.value}"
        )

    return encoded
