"""A ready-made consumer for the `on_event` hook: the channel trace.

`serve(..., on_event=ChannelObserver())` logs every event the channel sends
and keeps a per-call tally. It knows nothing about any provider, only about
the channel, which is exactly what the library knows.

The hook runs synchronously inside the call loop (`Session._notify`), so the
work here is short: log and count. Anything that blocks delays the audio.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# Cap on calls retained at once. State is released on HANGUP; this cap is the
# backstop for a HANGUP that never arrives, far above any real concurrency.
# Fixed by test_no_more_calls_are_retained_than_the_cap.
_MAX_CALLS_RETAINED = 10_000

# Cap on distinct event names counted per call. The channel emits a fixed
# handful (the 9 in `protocol.Event` plus the synthetic HANGUP); more than
# that is a channel with variable names.
_MAX_EVENT_NAMES_PER_CALL = 100

# Events the library turns into behavior of its own.
INTERPRETED = {
    "MEDIA_START",                # call start
    "MEDIA_XOFF",                 # flow control: stop
    "MEDIA_XON",                  # flow control: resume
    "MEDIA_MARK_PROCESSED",       # resolves mark()
    "MEDIA_BUFFERING_COMPLETED",  # resolves stop_buffering()
    "QUEUE_DRAINED",              # resolves report_when_drained(); finish() uses it
    "STATUS",                     # reply to request_status()
    "DTMF_END",                   # keypad digits
    "ERROR",                      # the channel rejected a command
    "HANGUP",                     # synthetic: the library fabricates it on close
}


class ChannelObserver:
    """Traces channel events and keeps a per-call tally.

    It is callable, so it goes straight in as `on_event`. The hook is ONE for
    the whole process and calls overlap, so state is keyed by `channel_id`:
    without that, two concurrent calls share counters and the final tally
    lies (`test_it_counts_events_per_call_without_mixing_them`).

    Requires `f(json)` in the dial string. In plain text, the channel default
    (`chan_websocket.c:2027`), XON/XOFF/QUEUE_DRAINED carry no `channel_id`
    (line 212) and every event lands in the shared `"?"` bucket.
    """

    def __init__(self, *, max_calls_retained: int = _MAX_CALLS_RETAINED,
                 max_event_names_per_call: int = _MAX_EVENT_NAMES_PER_CALL,
                 ) -> None:
        """Builds an observer with the retention caps.

        Args:
            max_calls_retained: Calls kept at once; the least recently active
                is evicted past it. The default covers any real concurrency.
            max_event_names_per_call: Distinct event names counted per call.

        Raises:
            ValueError: If either cap is below 1. With 0 the eviction runs over
                an empty dict and blows up on the first event, and since the
                hook swallows exceptions the observer would die silently
                (`test_invalid_observer_caps_raise`).
        """
        if max_calls_retained < 1 or max_event_names_per_call < 1:
            raise ValueError(
                f"Observer caps must be >= 1, got "
                f"max_calls_retained={max_calls_retained}, "
                f"max_event_names_per_call={max_event_names_per_call}."
            )
        self._max_calls_retained = max_calls_retained
        self._max_event_names_per_call = max_event_names_per_call
        # Evictions since start. Never reset: a persistent leak keeps warning
        # every 1,000, instead of once and then silence.
        self._evicted = 0
        # channel_id -> {event name -> how many arrived}
        self.counts: dict[str, dict[str, int]] = {}

    def __call__(self, name: str, payload: dict) -> None:
        """Receives a channel event, logs it and counts it.

        Args:
            name: Event name as the channel sends it, or `HANGUP`.
            payload: The event's fields. Anything that is not a dict is
                treated as empty: the hook is shared, and one odd payload
                must not stop the counting of the other calls.
        """
        if not isinstance(payload, dict):
            payload = {}

        # No channel_id, or an empty one, is the shared "?" bucket. The
        # synthetic HANGUP sends "" when the call dies before MEDIA_START
        # (session.py, _notify_hangup); `channel_id` is always a string
        # (chan_websocket.c packs it with s:s), so a falsy check is exact.
        # Fixed by test_an_empty_channel_id_shares_the_anonymous_bucket.
        cid = payload.get("channel_id")
        channel_id = str(cid) if cid else "?"

        # Leak backstop: past the cap some HANGUP never released its call.
        # Evict the least recently active, not the first inserted: the first
        # inserted is usually the long call still alive, and evicting it makes
        # its final tally lie (test_eviction_drops_the_least_recently_active_call).
        if channel_id in self.counts:
            # Re-insert at the end so dict order tracks activity, not arrival.
            self.counts[channel_id] = self.counts.pop(channel_id)
        elif len(self.counts) >= self._max_calls_retained:
            stale = next(iter(self.counts))
            self.counts.pop(stale, None)
            self._evicted += 1
            # One warning per leak, not per call: the first eviction, then
            # every 1,000 (test_the_eviction_warning_fires_once_then_every_1000).
            if self._evicted == 1 or self._evicted % 1_000 == 0:
                log.warning("Observer: %d calls retained without closing, "
                            "evicting the least recently active (%s); "
                            "%d evicted so far. A HANGUP went missing.",
                            self._max_calls_retained, stale, self._evicted)

        per_call = self.counts.setdefault(channel_id, {})
        # Past the cap on distinct names, new names stop entering (a channel
        # with variable names) but the ones already seen keep counting.
        if name in per_call or len(per_call) < self._max_event_names_per_call:
            per_call[name] = per_call.get(name, 0) + 1

        if name == "MEDIA_START":
            self._media_start(payload)
        elif name == "HANGUP":
            self._hangup(channel_id, payload)
        elif name in INTERPRETED:
            log.debug("%s %s", name, payload)
        else:
            # An event the library does not know yet: logged so a future
            # channel version reaches whoever integrates it on day one.
            log.info("%s (the library does not use it) %s", name, payload)

    def _media_start(self, payload: dict) -> None:
        """Logs the call start: the data the dialplan sent.

        Usually the first event, but not guaranteed: audio can arrive before
        it (issue #1712, see `docs/protocol.md`).
        """
        variables = payload.get("channel_variables") or {}
        if variables:
            log.info("Dialplan variables: %s", variables)
        else:
            # No variables is usually a missing underscore in Set(_VAR=...):
            # the call works, but the data never reaches the channel.
            log.info("The dialplan sent no channel variables")

    def _hangup(self, channel_id: str, payload: dict) -> None:
        """Logs the call end with the tally of what went through its channel.

        The synthetic HANGUP is always the last event of a call, so this is
        where the tally is complete. Only THIS call's state is released; the
        others keep counting (`test_hangup_discards_only_its_own_call`).
        """
        if not payload.get("normal"):
            reason = payload.get("reason")
            log.info("The other side hung up (code %s%s)",
                     payload.get("code"),
                     f", {reason}" if reason else "")

        log.debug("Events on channel %s: %s",
                  channel_id, self.summary(channel_id))
        self.counts.pop(channel_id, None)

    def summary(self, channel_id: str) -> str:
        """Returns the event tally of one call, sorted by name.

        Args:
            channel_id: The call, as the channel identifies it.

        Returns:
            `"NAME=count, ..."`, or `"none"` for an unknown call. An XOFF here
            means Asterisk's queue reached 900 frames, about 18 s of audio at
            20 ms (`chan_websocket.c:142`), which no single event tells you.
        """
        per_call = self.counts.get(channel_id, {})
        return ", ".join(f"{name}={total}"
                         for name, total in sorted(per_call.items())) \
            or "none"
