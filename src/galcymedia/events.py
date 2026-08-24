"""
Call events, in RTVI format.

A voice agent produces information that matters outside the call: what each
side said, how long the bot took to answer, when it got interrupted, what the
SIP signaling carried. Without a shared format every integrator invents a way
to get it out, so this module speaks RTVI (Real-Time Voice Inference), the
open standard Pipecat publishes. Protocol 2.1.0, and the envelope is always
the same (Pipecat, `processors/frameworks/rtvi/models.py:45-49`):

    {"label": "rtvi-ai", "type": "<type>", "id": "<id>", "data": {...}}

`label` is what lets a client tell a protocol message from any other traffic.
`id` names one event; it is random, so it says nothing about order.

The CORE emits these events, not the provider. An adapter fills in what its
provider knows and stays silent about the rest: a provider without
transcription simply never emits that event, and the client leaves that part
blank. See `docs/architecture.md`.

Reference: https://docs.pipecat.ai/client/rtvi-standard
"""

from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# MESSAGE_LABEL in the standard (`models.py:38`). An RTVI client discards a
# message without it.
LABEL = "rtvi-ai"

# Announced in bot-ready so the client knows which field set to expect.
# Matches the source (`models.py:32`).
PROTOCOL_VERSION = "2.1.0"


class EventType(str, Enum):
    """The event types this project emits.

    A subset of the standard: RTVI also defines stages a phone call does not
    have (a separate LLM and TTS, for one). Adding a type later breaks nothing,
    since an RTVI client ignores the types it does not know.
    """

    # Lifecycle
    BOT_READY = "bot-ready"

    # Who speaks and when. These draw the speaking turn.
    USER_STARTED_SPEAKING = "user-started-speaking"
    USER_STOPPED_SPEAKING = "user-stopped-speaking"
    BOT_STARTED_SPEAKING = "bot-started-speaking"
    BOT_STOPPED_SPEAKING = "bot-stopped-speaking"
    BOT_INTERRUPTED = "bot-interrupted"

    # Text. `final` tells a partial from a closed result.
    USER_TRANSCRIPTION = "user-transcription"
    # On its way out, not gone: Pipecat still emits it, with the removal
    # announced in its own source (`rtvi/observer.py:773`). New code uses
    # bot-output.
    BOT_TRANSCRIPTION = "bot-transcription"
    BOT_OUTPUT = "bot-output"
    # "Sent when text is being processed by TTS" (`models.py:474`): the bot's
    # partial, the counterpart of the user's `final: false`.
    BOT_TTS_TEXT = "bot-tts-text"

    # Tools the model decides to call
    FUNCTION_CALL_STARTED = "llm-function-call-started"
    FUNCTION_CALL_STOPPED = "llm-function-call-stopped"

    # Measurement and failures
    METRICS = "metrics"
    ERROR = "error"

    # The standard's wildcard: `data: Any` (`models.py:635`). Whatever is
    # specific to your integration and fits no other type. Here it carries
    # the SIP headers.
    SERVER_MESSAGE = "server-message"


@dataclass
class Event:
    """One RTVI message, ready to send.

    Args:
        type: The wire type.
        data: The payload; anything goes, `to_json` sanitizes it.
        id: 12 hex from uuid4. Pipecat only requires `str` (`models.py:47`).
    """

    type: EventType
    data: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def to_json(self) -> str:
        """Serializes the event, and never raises because of the content.

        `data` carries values this library does not control, and `emit()`
        swallows a failure, so a raise here loses the event in silence. Losing
        fidelity in one odd field is the lesser evil; see `docs/decisions.md`
        and the serialization tests in `tests/test_events.py`.
        """
        return json.dumps(
            {
                "label": LABEL,
                "type": self.type.value,
                "id": self.id,
                "data": _sanitize(self.data),
            },
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )


def _now() -> str:
    """ISO 8601 timestamp in UTC, whole seconds.

    The standard only types it as `str` (`models.py:514`). Pipecat itself
    emits `isoformat(timespec="milliseconds")` in UTC
    (Pipecat `acead05`, `utils/time.py:23`); this shape parses the same.
    """
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"


def _text(value: Any) -> str:
    """Coerces a transcription into the `text: str` the schema requires.

    An STT adapter reports "no text" as None, and `{"text": null}` is an
    invalid event: a strict client discards it, a lax one breaks on
    `text.trim()`. None becomes the empty string
    (`test_a_none_transcription_produces_empty_text_not_null`).
    """
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


# Where `_sanitize` stops descending. A cyclic or very deep structure would
# overflow the stack, and that RecursionError, swallowed by `emit()`, is one
# more lost event. Measured: 5,000 levels come out as 64 markers.
_SANITIZE_MAX_DEPTH = 64


def _sanitize(value: Any, _depth: int = 0) -> Any:
    """Returns a value `json.dumps` cannot choke on.

    Non-finite float -> None, bytes -> UTF-8 with replacement, lone
    surrogates replaced, deep or cyclic nesting cut, any other type to its
    str. Each case has a test in `tests/test_events.py`.
    """
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        # A lone surrogate survives dumps but fails to encode to UTF-8 when
        # the frame is sent.
        return value.encode("utf-8", "replace").decode("utf-8")
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")

    if isinstance(value, (dict, list, tuple)) \
            and _depth >= _SANITIZE_MAX_DEPTH:
        # Not repr()/str() here: both walk the container and overflow with
        # the same nesting this cut exists to avoid.
        return "<...>"

    if isinstance(value, dict):
        return {str(_sanitize(k, _depth + 1)): _sanitize(v, _depth + 1)
                for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v, _depth + 1) for v in value]
    return str(value)


# ---------------------------------------------------------------------------
# Factories
#
# One per type, so an adapter does not have to remember each `data` shape. A
# misspelled field produces a message the client ignores in silence, and
# nothing in the logs says so.
# ---------------------------------------------------------------------------


def bot_ready(provider: str, **about: Any) -> Event:
    """The provider is connected and the line is open.

    NOT the first event of the call, even though the standard presents it as
    the handshake: `server_message` with the dialplan data goes out first, on
    MEDIA_START, so a panel can show the caller's record while the provider
    is still connecting (`Session._on_media_start` before
    `Session._start_provider`).

    Args:
        provider: The provider name, placed in `about`.
        **about: Anything else worth announcing (`models.py:171-172`).
    """
    return Event(
        EventType.BOT_READY,
        {"version": PROTOCOL_VERSION, "about": {"provider": provider, **about}},
    )


def user_transcription(text: str, *, final: bool = True,
                       user_id: str = "caller") -> Event:
    """What the caller said (`models.py:512-515`).

    Args:
        text: The transcription; None becomes "".
        final: False marks a partial that the provider will revise. A panel
            paints those in gray and replaces them; treating them as final
            duplicates the text.
        user_id: Who spoke.
    """
    return Event(
        EventType.USER_TRANSCRIPTION,
        {"text": _text(text), "user_id": user_id, "timestamp": _now(),
         "final": final},
    )


def bot_transcription(text: str) -> Event:
    """What the bot said, closed. Kept for old clients; new code uses
    `bot_output()`.

    Pipecat still emits it, with its removal announced
    (`rtvi/observer.py:773`). Nothing in this library calls it
    (`test_the_type_with_no_emission_site_stays_undocumented`).
    """
    return Event(EventType.BOT_TRANSCRIPTION, {"text": _text(text)})


def bot_output(text: str, *, spoken: bool = True,
               aggregated_by: str = "turn",
               spoken_status: str = "completed") -> Event:
    """Text the bot produced for this turn.

    Carries the fields of BOTH protocol versions: `spoken` is v1,
    `will_be_spoken` and `spoken_status` are v2+ (`models.py:419-421`). The
    standard's own serializer sends both and drops nulls per client version
    (`processor.py:172-175`); announcing 2.1.0 in `bot_ready` while sending
    only v1 fields would promise one shape and deliver another.

    Args:
        text: The closed text of the turn; the partial travels in
            `bot_tts_text()`.
        spoken: False for text the bot produces but does not say: a note for
            the human agent, a CRM field, a classification label.
        aggregated_by: Granularity of `text`, `turn` by default.
        spoken_status: `completed` by default, since the text is closed.
    """
    return Event(
        EventType.BOT_OUTPUT,
        {
            "text": _text(text),
            "aggregated_by": aggregated_by,
            # v1
            "spoken": spoken,
            # v2
            "will_be_spoken": spoken,
            "spoken_status": spoken_status,
        },
    )


def bot_tts_text(text: str) -> Event:
    """The bot's text as it is spoken, one fragment at a time.

    The bot's partial, the counterpart of the user's `final=False`: a panel
    paints the utterance while it plays instead of waiting for the turn to
    close. The closed text travels in `bot_output()`.
    """
    return Event(EventType.BOT_TTS_TEXT, {"text": _text(text)})


def user_started_speaking() -> Event:
    return Event(EventType.USER_STARTED_SPEAKING)


def user_stopped_speaking() -> Event:
    return Event(EventType.USER_STOPPED_SPEAKING)


def bot_started_speaking() -> Event:
    return Event(EventType.BOT_STARTED_SPEAKING)


def bot_stopped_speaking() -> Event:
    return Event(EventType.BOT_STOPPED_SPEAKING)


def bot_interrupted() -> Event:
    """The caller spoke over the bot and its pending audio was discarded."""
    return Event(EventType.BOT_INTERRUPTED)


def function_call_started(name: str, *, tool_call_id: str,
                          arguments: dict[str, Any] | None = None) -> Event:
    """The model decided to call a tool.

    The standard only defines `function_name` for this message
    (`models.py:257-264`); `tool_call_id` is mandatory in `stopped`
    (`:322`). It is sent here too so a client can pair the start with its
    result when the model requests several tools at once.

    Args:
        name: The tool.
        tool_call_id: The id the result will carry.
        arguments: What the model passed. The field is `arguments`, not
            `args`: `args` belongs to the deprecated model (`:190-202`).
    """
    return Event(
        EventType.FUNCTION_CALL_STARTED,
        {
            "tool_call_id": tool_call_id,
            "function_name": name,
            "arguments": arguments or {},
        },
    )


def function_call_stopped(name: str, *, tool_call_id: str, result: Any = None,
                          cancelled: bool = False) -> Event:
    """The tool finished, or was cancelled (`models.py:322-325`)."""
    return Event(
        EventType.FUNCTION_CALL_STOPPED,
        {
            "tool_call_id": tool_call_id,
            "function_name": name,
            "cancelled": cancelled,
            "result": result,
        },
    )


def metrics(*, ttfb: list[dict[str, Any]] | None = None,
            processing: list[dict[str, Any]] | None = None,
            **extra: Any) -> Event:
    """Measurements.

    Args:
        ttfb: Time to first byte of audio, THE metric of a voice agent: above
            1.5 s the call feels slow. Entries are `{processor, value}` plus
            an optional `model`, with `value` in SECONDS
            (Pipecat `acead05`, `metrics/metrics.py:27-38`).
        processing: Same shape, for processing time.
        **extra: The channel's own counters (frames, silence fills, queue
            fills). A client that does not know them ignores them.
    """
    data: dict[str, Any] = {}
    if ttfb:
        data["ttfb"] = ttfb
    if processing:
        data["processing"] = processing
    data.update(extra)
    return Event(EventType.METRICS, data)


def error(message: str, *, fatal: bool = False) -> Event:
    """Something failed.

    The field is `error`, not `message`: ErrorData in the source is
    `error: str`, `fatal: bool` (`models.py:147-148`); the web doc says
    otherwise and the source wins.

    Args:
        message: What happened.
        fatal: True when the session ends because of it.
    """
    return Event(EventType.ERROR, {"error": message, "fatal": fatal})


def server_message(payload: Any) -> Event:
    """Whatever the protocol cannot foresee; here, the SIP signaling data.

    Customer id, campaign, origin queue: all of it arrives BEFORE the first
    audio, so a panel can show the full record while the phone is still
    ringing.
    """
    return Event(EventType.SERVER_MESSAGE, payload)


# ---------------------------------------------------------------------------
# The surface, as data
# ---------------------------------------------------------------------------

# Each entry: (type, when it is emitted, the fields its `data` carries). It
# feeds `describe_events()`. A factory that gets wired up goes here too: the
# tests compare this list against the package and against each factory
# (`test_the_list_is_synchronized_with_what_the_library_emits`,
# `test_every_row_lists_exactly_the_fields_the_factory_emits`).
_EVENT_CATALOG: tuple[tuple[EventType, str, str], ...] = (
    (
        EventType.SERVER_MESSAGE,
        "dialplan data at start, keypad presses, and the hangup",
        "call, dtmf or hangup, depending on the case",
    ),
    (
        EventType.BOT_READY,
        "the provider connected and the call is ready",
        "version, about",
    ),
    (
        EventType.USER_STARTED_SPEAKING,
        "the provider detected that the caller started speaking",
        "no fields",
    ),
    (
        EventType.USER_STOPPED_SPEAKING,
        "the provider detected that the caller stopped speaking",
        "no fields",
    ),
    (
        EventType.USER_TRANSCRIPTION,
        "the provider transcribed the caller; `final` false is a partial",
        "text, user_id, timestamp, final",
    ),
    (
        EventType.BOT_STARTED_SPEAKING,
        "the bot's audio starts flowing into the call",
        "no fields",
    ),
    (
        EventType.BOT_STOPPED_SPEAKING,
        "the bot closed its turn after speaking",
        "no fields",
    ),
    (
        EventType.BOT_TTS_TEXT,
        "a fragment of the bot's text as it is spoken",
        "text",
    ),
    (
        EventType.BOT_OUTPUT,
        "the closed text of the bot's turn",
        "text, aggregated_by, spoken, will_be_spoken, spoken_status",
    ),
    (
        EventType.BOT_INTERRUPTED,
        "the caller spoke over the bot and its pending audio was discarded",
        "no fields",
    ),
    (
        EventType.FUNCTION_CALL_STARTED,
        "the model requested a tool and it starts running",
        "tool_call_id, function_name, arguments",
    ),
    (
        EventType.FUNCTION_CALL_STOPPED,
        "the tool returned a result, failed, or was cancelled",
        "tool_call_id, function_name, cancelled, result",
    ),
    (
        EventType.METRICS,
        (
            "the bot started answering: how long since the caller spoke "
            "(ttfb, in seconds, the standard's unit). Above 1.5 the call "
            "feels slow. Not emitted for the opening greeting, which answers "
            "nobody"
        ),
        "ttfb",
    ),
    (
        EventType.ERROR,
        (
            "the provider reported a failure, did not start, or the codec "
            "stayed in passthrough; `fatal` true ends the call"
        ),
        "error, fatal",
    ),
)


def describe_events() -> list[dict[str, str]]:
    """Lists the events this library publishes, as data.

    Answers "which events will reach me" without opening the source: in a
    REPL, or to build the table of a panel or a README. Only what is emitted
    TODAY, not the whole standard; a type the enum has but nobody emits is
    left out (`test_the_type_with_no_emission_site_stays_undocumented`).

    Returns:
        One dict per event, in the order of a typical call (dialplan data,
        greeting, speaking turn, tools, failures), with three keys: `type`
        (the wire string), `when` (the moment it is emitted) and `data` (the
        fields it carries). A fresh list on every call.
    """
    return [
        {"type": event_type.value, "when": when, "data": fields}
        for event_type, when, fields in _EVENT_CATALOG
    ]
