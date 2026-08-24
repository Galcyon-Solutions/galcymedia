"""Decisions that cross from the WebSocket channel to the dialplan.

The channel forces this shape: `chan_websocket` has no command to transfer
or to write a variable (`chan_websocket.c:129-139` is the whole set), so
whatever the application decides is recorded in the process and delivered
later, when the dialplan asks over FastAGI. The two halves share neither
time nor channel, only the key the dialplan seeded
(`Set(_CALL_ID=${UNIQUEID})`).

Two pieces, general to concrete:

    CallDecisions   the per-key registry, for ANY data that has to cross: a
                    transfer destination, a classification label, the call
                    outcome for the CRM.
    Transfers       the first use, solved end to end: the bot records, the
                    dialplan receives BOT_ACTION and BOT_REASON.

A dict in memory is enough because the process is one: `serve()` runs the
WebSocket and the FastAGI together (`server.py`). Spread calls over several
processes and this is the one thing to replace with shared storage.
"""

from __future__ import annotations

import logging
import time
from typing import Any

log = logging.getLogger(__name__)

# How long an entry nobody collects lives. The dialplan asks seconds after
# the hangup; if it never asks (no AGI line), the entry must expire anyway.
# A default, not a policy: `ttl_s` changes it.
DEFAULT_TTL_S = 300.0


class CallDecisions:
    """Per-key registry of what the process decided about each call.

    The purge is lazy: it runs on every write and read, so the worst case is
    a handful of expired entries waiting for the next call
    (`test_entries_expire_with_the_configured_ttl`).

    Meant for the asyncio loop thread, where the WebSocket and the FastAGI
    run: there its methods are atomic with respect to each other (no await
    inside). From ANOTHER thread (a `run_in_executor`), synchronize yourself
    or come in through `loop.call_soon_threadsafe`.
    """

    def __init__(self, ttl_s: float = DEFAULT_TTL_S) -> None:
        """Builds an empty registry.

        Args:
            ttl_s: Seconds an entry lives uncollected.

        Raises:
            ValueError: If `ttl_s` is not positive. With 0 the clock moves
                between the put and the take and every entry expires before
                it can be read: transfers stop working with nothing in the
                log (`test_a_non_positive_ttl_is_a_config_trap`).
        """
        if ttl_s <= 0:
            raise ValueError(
                f"ttl_s must be positive, got {ttl_s}. A zero or negative TTL "
                f"expires every decision before it can be read."
            )
        self._ttl_s = ttl_s
        self._by_key: dict[str, tuple[float, Any]] = {}

    def put(self, key: str, value: Any) -> None:
        """Records a value for a call, replacing any previous one.

        Args:
            key: The call key. Empty is logged and ignored: without a key
                there is no way to find the entry later
                (`test_an_empty_key_is_not_recorded`).
            value: Anything; the registry does not look inside.
        """
        if not key:
            log.warning("A decision was recorded without a call key")
            return
        self._expire()
        self._by_key[key] = (time.monotonic(), value)

    def take(self, key: str, default: Any = None) -> Any:
        """Returns the entry and CONSUMES it: each decision is delivered once.

        Args:
            key: The call key.
            default: Returned when there is no entry, including the second
                ask for the same call
                (`test_what_is_recorded_is_consumed_only_once`).

        Returns:
            The recorded value, or `default`. To read without consuming, `peek()`.
        """
        self._expire()
        _, value = self._by_key.pop(key, (0.0, default))
        return value

    def peek(self, key: str, default: Any = None) -> Any:
        """Reads without consuming (`test_peek_reads_without_consuming`)."""
        self._expire()
        entry = self._by_key.get(key)
        return entry[1] if entry is not None else default

    def forget(self, key: str) -> None:
        """Discards the entry for a call, if there is one."""
        self._by_key.pop(key, None)

    def _expire(self) -> None:
        limit = time.monotonic() - self._ttl_s
        # Over a copy of the items. Nothing here can re-enter the dict; the
        # copy is a cheap belt for the unsupported cross-thread case, so a
        # misuse fails as a lost entry and not as "dictionary changed size".
        stale = [k for k, (created, _) in list(self._by_key.items())
                 if created < limit]
        for key in stale:
            self._by_key.pop(key, None)
        if stale:
            log.debug("%d decisions expired unasked", len(stale))

    def __len__(self) -> int:
        return len(self._by_key)


class Transfers:
    """The transfer to a person, solved end to end.

    The bot records (`transfer()`) and the handler answers the dialplan by
    writing `BOT_ACTION` (`transfer` or `hangup`) and `BOT_REASON` on the
    caller's channel, the one still alive after the Dial. The dialplan lines
    it needs are in `docs/architecture.md`.

        transfers = Transfers()
        serve(factory, transfers=transfers)
        # and in the adapter: transfers.transfer(call_id, "asked for it")

    Optional: whoever does not instantiate it pays nothing, and AMI or ARI
    users simply skip it. It goes over AGI because that is what survives
    after the Dial (`docs/decisions.md`). `handler` is an ordinary AGI
    handler, mountable on an `AgiRouter` route when the process serves more.
    """

    def __init__(self, ttl_s: float = DEFAULT_TTL_S, *,
                 action_var: str = "BOT_ACTION",
                 reason_var: str = "BOT_REASON",
                 destination_var: str = "BOT_DESTINATION",
                 default_action: str = "hangup") -> None:
        """Builds the registry and the variable names the dialplan reads.

        Args:
            ttl_s: Seconds a recorded transfer waits for the dialplan.
            action_var: Variable that gets `transfer` or `default_action`.
            reason_var: Variable that gets the reason, when there is one.
            destination_var: Variable that gets the destination, when set.
            default_action: What `action_var` says when nothing is recorded
                (`test_the_variable_names_and_the_default_are_parameters`).
        """
        self._decisions = CallDecisions(ttl_s=ttl_s)
        # What `handler` already wrote on a call, by key, kept for the same
        # TTL. Both AGIs of a call write on ONE channel: the default is
        # written on the first ask only, and an applied decision is never
        # overwritten (`test_each_decision_is_delivered_once`).
        self._applied = CallDecisions(ttl_s=ttl_s)
        self._action_var = action_var
        self._reason_var = reason_var
        self._destination_var = destination_var
        self._default_action = default_action

    def transfer(self, call_id: str, reason: str = "",
                 destination: str = "") -> None:
        """Records that this call has to go to a person.

        Args:
            call_id: The key the dialplan seeded. Empty is logged and ignored.
            reason: Free text for the dialplan and the CDR.
            destination: Optional. When set, the dialplan gets it in
                BOT_DESTINATION and can route by it: two bots escalating to
                different queues stop being indistinguishable
                (`test_the_escalation_destination_reaches_the_dialplan`).
        """
        if not call_id:
            log.warning("Transfer decision without a call key")
            return
        self._decisions.put(call_id, (reason, destination))
        log.info("Recorded for transfer: %s (%s)", call_id, reason)

    def forget(self, call_id: str) -> None:
        """Discards what was recorded for a call."""
        self._decisions.forget(call_id)

    async def handler(self, request: Any) -> None:
        """Answers the dialplan's AGI() after the Dial.

        The key comes as an AGI argument, not from the environment:
        `agi_uniqueid` names the CALLER's channel (`res_agi.c:2459`) and the
        MEDIA_START `channel_id` names the WebSocket channel
        (`chan_websocket.c:235`), two channels of the same call. So the
        dialplan seeds its own key before the Dial and hands it to both.

        Args:
            request: The AGI request; `args[0]` is the key.
        """
        key = request.args[0] if request.args else ""

        if not key:
            log.warning(
                "The AGI arrived without a call key. Pass it as an argument: "
                "AGI(agi://host:4573/after-dial,${CALL_ID})"
            )

        noted = self._decisions.take(key)
        if noted is None:
            # A second ask for the same call (one handler mounted on every
            # AGI path runs here from the hangup handler too) must not
            # answer the default over what the first ask wrote: the CDR of
            # a transferred call read hangup (seen 2026-08-22).
            if key and self._applied.peek(key) is not None:
                log.info("Already applied for %s, leaving the channel as is",
                         key)
                return
            action, reason, destination = self._default_action, "", ""
        else:
            reason, destination = noted
            action = "transfer"

        await request.set_variable(self._action_var, action)
        if reason:
            await request.set_variable(self._reason_var, reason)
        if destination:
            await request.set_variable(self._destination_var, destination)
        if key:
            self._applied.put(key, action)

        if action == "transfer":
            log.info("The dialplan transfers the call from %s (%s)",
                     request.caller, reason)
        else:
            log.debug("No transfer for %s", key or "(no key)")

    async def closing_handler(self, request: Any) -> None:
        """Answers the AGI of the hangup handler, the one that ALWAYS runs.

        `handler` runs after the Dial, so only when the call got that far.
        When the caller hangs up first the dialplan loop stops at the Dial
        (`pbx.c:2940`) and the bot's decision leaves no trace in the CDR. A
        hangup handler "follows the channel" and runs "regardless of where
        in the dialplan a channel is executing", transfers and pickups
        included (docs.asterisk.org, "Hangup Handlers", read 2026-08-21); the
        `h` extension runs only from the dialplan loop (`pbx.c:3150` vs
        `channel.c:2572`).

        Here it only WRITES VARIABLES, and that is the engine's limit, not a
        convention: Asterisk marks the channel hung up before running the
        handlers (`pbx_hangup_handler.c:74`, `ast_softhangup_nolock`), AGI
        then runs in dead mode (`res_agi.c:4737`) and accepts only the 11
        commands flagged dead-safe (`res_agi.c:3879-3914`: set/get variable,
        exec, database, noop, verbose). A transfer from here cannot work.

        The handler runs as a Gosub (`pbx_hangup_handler.c:86`), so the
        dialplan side must end in `Return()` (same page). The context to
        push is in `docs/architecture.md`.

        Args:
            request: The AGI request; `args[0]` is the key.
        """
        key = request.args[0] if request.args else ""

        # `take`, not `peek`: if the call reached the AGI after the Dial that
        # decision was consumed there and nothing is left here, which is
        # right. What remains is what the bot decided on calls that did NOT
        # get there, and consuming it frees the entry instead of leaving it
        # to expire by TTL.
        noted = self._decisions.take(key) if key else None

        # Nothing recorded, nothing written. The hangup handler runs on EVERY
        # call, after `handler` too, on the same channel: writing the default
        # here would overwrite the transfer `handler` already wrote and the
        # CDR would read hangup for a call that was transferred
        # (`test_the_closing_handler_keeps_what_the_dial_agi_wrote`).
        if noted is None:
            log.debug("Closing %s: nothing recorded", key or "(no key)")
            return

        reason, destination = noted
        await request.set_variable(self._action_var, "transfer")
        if reason:
            await request.set_variable(self._reason_var, reason)
        # The destination too, as in `handler`: without it the CDR knows the
        # call was escalating but not where, and two bots escalating to
        # different queues look the same, which is what this route is for
        # (`test_the_destination_survives_a_caller_who_hangs_up_first`).
        if destination:
            await request.set_variable(self._destination_var, destination)

        log.debug("Closing %s: transfer", key)

    def __len__(self) -> int:
        return len(self._decisions)
