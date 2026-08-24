"""Tests for ChannelObserver: the per-call trace of the on_event hook."""

from __future__ import annotations

import pytest

from galcymedia import ChannelObserver
from galcymedia.observer import _MAX_CALLS_RETAINED


def test_it_counts_events_per_call_without_mixing_them():
    """Two concurrent calls share the hook, not the counters."""
    observer = ChannelObserver()

    observer("MEDIA_START", {"channel_id": "a"})
    observer("MEDIA_XOFF", {"channel_id": "a"})
    observer("MEDIA_START", {"channel_id": "b"})

    assert observer.counts["a"] == {"MEDIA_START": 1, "MEDIA_XOFF": 1}
    assert observer.counts["b"] == {"MEDIA_START": 1}


def test_hangup_discards_only_its_own_call():
    """A call's state dies with its HANGUP; the others carry on."""
    observer = ChannelObserver()

    observer("MEDIA_START", {"channel_id": "a"})
    observer("MEDIA_START", {"channel_id": "b"})
    observer("HANGUP", {"channel_id": "a", "code": 1000, "normal": True})

    assert "a" not in observer.counts
    assert "b" in observer.counts


def test_an_event_without_channel_id_does_not_blow_up():
    """An event without identity lands in the shared "?" bucket."""
    observer = ChannelObserver()
    observer("FUTURE_EVENT", {"data": 42})
    assert observer.counts["?"]["FUTURE_EVENT"] == 1


def test_the_summary_of_an_unknown_call_is_empty():
    """`summary()` of a call never seen says so instead of raising."""
    assert ChannelObserver().summary("no-such-call") == "none"


def test_a_hangup_with_a_non_dict_payload_does_not_blow_up():
    """A non-dict payload degrades to empty and never raises.

    The hook is ONE for the whole process: one odd payload must not stop the
    counting of the other calls. The HANGUPs land in the shared "?" bucket
    and release it, which is the shared-bucket cost named in the class doc.
    """
    observer = ChannelObserver()
    observer("MEDIA_START", {"channel_id": "a"})
    observer("HANGUP", None)
    observer("HANGUP", 42)
    observer("HANGUP", "closed")
    assert observer.counts["a"] == {"MEDIA_START": 1}, "the other call is intact"
    assert "?" not in observer.counts, "the HANGUP released its own bucket"


def test_an_empty_channel_id_shares_the_anonymous_bucket():
    """`channel_id: ""` and no `channel_id` are the same "?" bucket.

    The synthetic HANGUP carries "" when the call died before MEDIA_START;
    treated as its own identity, it released nothing and "?" leaked for good.
    """
    observer = ChannelObserver()
    observer("MEDIA_XOFF", {})
    observer("HANGUP", {"channel_id": "", "code": 1006, "normal": False})

    assert observer.counts == {}, "the HANGUP with an empty id released '?'"


def test_no_more_calls_are_retained_than_the_cap():
    """Retention never exceeds the cap, even with no HANGUP at all.

    State is released on HANGUP; the cap is the backstop for one that never
    arrives, without which one leaked entry per call kills a nonstop process.
    """
    observer = ChannelObserver()
    for i in range(_MAX_CALLS_RETAINED + 50):
        observer("MEDIA_START", {"channel_id": f"c{i}"})

    assert len(observer.counts) <= _MAX_CALLS_RETAINED


def test_eviction_drops_the_least_recently_active_call():
    """Past the cap, the call that went quiet goes, not the one still talking.

    Insertion order alone evicts the first call in, which is usually the long
    call still alive; its next event re-creates it empty and its final tally
    lies.
    """
    observer = ChannelObserver(max_calls_retained=3)
    observer("MEDIA_START", {"channel_id": "long"})
    observer("MEDIA_START", {"channel_id": "quiet"})
    observer("MEDIA_START", {"channel_id": "c"})
    for _ in range(5):
        observer("MEDIA_XOFF", {"channel_id": "long"})

    observer("MEDIA_START", {"channel_id": "new"})

    assert "quiet" not in observer.counts, "the quiet call is the one evicted"
    assert observer.counts["long"] == {"MEDIA_START": 1, "MEDIA_XOFF": 5}


def test_the_eviction_warning_fires_once_then_every_1000(caplog):
    """One warning per leak, not per call: the first eviction, then every 1,000.

    A warning per evicted call floods a 24/7 log; a warning once per
    episode goes silent for good under a persistent leak.
    """
    observer = ChannelObserver(max_calls_retained=2)
    with caplog.at_level("WARNING", logger="galcymedia.observer"):
        for i in range(2 + 2_000):
            observer("MEDIA_START", {"channel_id": f"c{i}"})

    warnings = [r.getMessage() for r in caplog.records]
    assert len(warnings) == 3, "evictions 1, 1000 and 2000"
    assert "1 evicted so far" in warnings[0]
    assert "1000 evicted so far" in warnings[1]
    assert "2000 evicted so far" in warnings[2]


def test_observer_caps_are_configurable():
    """The caps are kwargs; the defaults do not move."""
    short = ChannelObserver(max_calls_retained=2)
    for cid in ("a", "b", "c"):
        short("MEDIA_XON", {"channel_id": cid})
    assert len(short.counts) == 2, "past the cap one call is evicted"

    limited = ChannelObserver(max_event_names_per_call=1)
    limited("ONE", {"channel_id": "z"})
    limited("TWO", {"channel_id": "z"})
    limited("ONE", {"channel_id": "z"})
    assert limited.counts["z"] == {"ONE": 2}, \
        "past the cap no new names enter but the seen ones keep counting"

    normal = ChannelObserver()
    assert normal._max_calls_retained == 10_000
    assert normal._max_event_names_per_call == 100


def test_invalid_observer_caps_raise():
    """A cap of 0 raises at construction, not silently on the first event.

    With 0 the eviction runs over an empty dict and blows up, and since the
    hook swallows exceptions the observer would die without a trace.
    """
    with pytest.raises(ValueError):
        ChannelObserver(max_calls_retained=0)
    with pytest.raises(ValueError):
        ChannelObserver(max_event_names_per_call=-1)
