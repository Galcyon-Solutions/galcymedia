"""Tests of the decisions that cross from the WebSocket channel to the dialplan.

`FakeRequest` stands in for the AGI request: the handlers only read `args`
and `caller` and write through `set_variable`.
"""

from __future__ import annotations

import time

import pytest

from galcymedia import CallDecisions, Transfers


class FakeRequest:
    """A fake AGI() invocation: records the variables written."""

    def __init__(self, args=None, caller="1001"):
        self.args = args or []
        self.caller = caller
        self.variables: dict[str, str] = {}

    async def set_variable(self, name, value):
        self.variables[name] = value


# ---------------------------------------------------------------------------
# CallDecisions: the generic registry
# ---------------------------------------------------------------------------


def test_what_is_recorded_is_consumed_only_once():
    """If the dialplan asks twice, the second time gets the default."""
    decisions = CallDecisions()
    decisions.put("c1", {"outcome": "sale"})

    assert decisions.take("c1") == {"outcome": "sale"}
    assert decisions.take("c1", "none") == "none"


def test_peek_reads_without_consuming():
    decisions = CallDecisions()
    decisions.put("c1", "value")

    assert decisions.peek("c1") == "value"
    assert decisions.take("c1") == "value", "peek consumed it"


def test_an_empty_key_is_not_recorded():
    """Without a key there is no way to find it later: warn, do not store."""
    decisions = CallDecisions()
    decisions.put("", "value")
    assert len(decisions) == 0


def test_entries_expire_with_the_configured_ttl():
    """The TTL is a parameter, not a policy: an omitted AGI line cannot grow
    memory forever. The purge is lazy, so it shows on the next operation."""
    decisions = CallDecisions(ttl_s=0.01)
    decisions.put("c1", "value")
    time.sleep(0.03)

    decisions.put("c2", "other")
    assert decisions.take("c1", "expired") == "expired"
    assert decisions.take("c2") == "other"


def test_forget_discards_without_claiming():
    decisions = CallDecisions()
    decisions.put("c1", "value")
    decisions.forget("c1")
    assert decisions.take("c1", "none") == "none"


@pytest.mark.parametrize("invalid_ttl", [0, -5])
def test_a_non_positive_ttl_is_a_config_trap(invalid_ttl):
    """A ttl_s <= 0 expires every entry before it can be read (the clock moves
    between the put and the take): transfers stop with nothing in the log.
    The constructor rejects it loudly.
    """
    with pytest.raises(ValueError):
        CallDecisions(ttl_s=invalid_ttl)


def test_transfers_inherits_the_ttl_validation():
    """Transfers builds a CallDecisions inside, so the same trap fires here."""
    with pytest.raises(ValueError):
        Transfers(ttl_s=0)


# ---------------------------------------------------------------------------
# Transfers: the first use, end to end
# ---------------------------------------------------------------------------


async def test_the_transfer_travels_from_the_bot_to_the_dialplan():
    transfers = Transfers()
    transfers.transfer("ast-123", "the person asked")

    request = FakeRequest(args=["ast-123"])
    await transfers.handler(request)

    assert request.variables["BOT_ACTION"] == "transfer"
    assert request.variables["BOT_REASON"] == "the person asked"


async def test_without_a_decision_the_dialplan_gets_the_default():
    request = FakeRequest(args=["ast-999"])
    await Transfers().handler(request)

    assert request.variables["BOT_ACTION"] == "hangup"
    assert "BOT_REASON" not in request.variables


async def test_each_decision_is_delivered_once():
    """The second AGI of the same call writes NOTHING: already applied.

    Both AGIs write on the caller's channel, one variable space. With a
    single handler mounted on every path (`AgiServer(transfers.handler)`,
    the shape of the Pipecat example), the hangup handler's AGI runs
    `handler` again after the Dial's AGI consumed the transfer: answering
    the default there overwrote BOT_ACTION=transfer with hangup and the CDR
    of a transferred call read hangup (seen in a real call, 2026-08-22).
    """
    transfers = Transfers()
    transfers.transfer("ast-123", "reason")

    first = FakeRequest(args=["ast-123"])
    second = FakeRequest(args=["ast-123"])
    await transfers.handler(first)
    await transfers.handler(second)

    assert first.variables["BOT_ACTION"] == "transfer"
    assert second.variables == {}, (
        "the second ask overwrote an applied decision with the default")


async def test_a_default_already_answered_is_not_answered_again():
    """The same rule for the default: the first ask writes it, the rest leave
    the channel alone. A later writer on the same call could be the dialplan
    itself."""
    transfers = Transfers()

    first = FakeRequest(args=["ast-7"])
    second = FakeRequest(args=["ast-7"])
    await transfers.handler(first)
    await transfers.handler(second)

    assert first.variables["BOT_ACTION"] == "hangup"
    assert second.variables == {}


async def test_an_applied_decision_expires_with_the_same_ttl(monkeypatch):
    """What was applied is remembered only as long as a decision waits:
    the same TTL, so the registry cannot grow with one entry per call."""
    import galcymedia.decisions as module

    now = [1000.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])
    transfers = Transfers(ttl_s=5.0)
    transfers.transfer("ast-9", "reason")

    await transfers.handler(FakeRequest(args=["ast-9"]))
    now[0] += 6.0
    late = FakeRequest(args=["ast-9"])
    await transfers.handler(late)

    assert late.variables["BOT_ACTION"] == "hangup"


async def test_the_variable_names_and_the_default_are_parameters():
    """An existing dialplan may have its own names: the policy is a parameter."""
    transfers = Transfers(action_var="NEXT", reason_var="WHY",
                          default_action="drop")
    transfers.transfer("c1", "a reason")

    request = FakeRequest(args=["c1"])
    await transfers.handler(request)
    assert request.variables == {"NEXT": "transfer", "WHY": "a reason"}

    other = FakeRequest(args=["c2"])
    await transfers.handler(other)
    assert other.variables == {"NEXT": "drop"}


async def test_an_agi_without_a_key_answers_the_default_and_does_not_blow_up():
    request = FakeRequest(args=[])
    await Transfers().handler(request)
    assert request.variables["BOT_ACTION"] == "hangup"


async def test_the_escalation_destination_reaches_the_dialplan():
    """Two bots escalating to different queues stay distinguishable: the
    recorded destination travels in BOT_DESTINATION."""
    transfers = Transfers()
    transfers.transfer("c9", "asked", destination="support_queue")

    request = FakeRequest(args=["c9"])
    await transfers.handler(request)

    assert request.variables["BOT_ACTION"] == "transfer"
    assert request.variables["BOT_REASON"] == "asked"
    assert request.variables["BOT_DESTINATION"] == "support_queue"


async def test_without_a_destination_the_variable_is_not_written():
    """The usual transfer(call_id, reason): BOT_DESTINATION does not appear."""
    transfers = Transfers()
    transfers.transfer("c10", "asked")

    request = FakeRequest(args=["c10"])
    await transfers.handler(request)

    assert request.variables["BOT_ACTION"] == "transfer"
    assert "BOT_DESTINATION" not in request.variables


# ---------------------------------------------------------------------------
# The closing handler: the one that runs even when nothing else does
# ---------------------------------------------------------------------------
#
# The AGI after the Dial runs only if the call got that far: when the caller
# hangs up first the dialplan loop stops at the Dial (`pbx.c:2940`). A hangup
# handler "follows the channel" (docs.asterisk.org, "Hangup Handlers").


async def test_a_decision_survives_a_caller_who_hangs_up_first():
    transfers = Transfers()
    transfers.transfer("c20", "the person asked")

    # The Dial ended on hangup: `/after-dial` never ran, only the hangup handler.
    request = FakeRequest(args=["c20"])
    await transfers.closing_handler(request)

    assert request.variables["BOT_ACTION"] == "transfer"
    assert request.variables["BOT_REASON"] == "the person asked"


async def test_the_destination_survives_a_caller_who_hangs_up_first():
    """The hangup route carries the destination, not only the reason.

    With BOT_ACTION and BOT_REASON alone, two bots escalating to different
    queues look the same in the CDR, and telling them apart is what this
    route is for.
    """
    transfers = Transfers()
    transfers.transfer("c21", "out of stock", destination="sales_queue")

    request = FakeRequest(args=["c21"])
    await transfers.closing_handler(request)

    assert request.variables["BOT_ACTION"] == "transfer"
    assert request.variables["BOT_REASON"] == "out of stock"
    assert request.variables["BOT_DESTINATION"] == "sales_queue"


async def test_a_normal_call_closes_without_writing_anything():
    """Nothing recorded, nothing written: the default is `handler`'s job.

    The hangup handler runs on every call, including the ones `handler`
    already answered, so a default here would overwrite that answer.
    """
    request = FakeRequest(args=["c21"])
    await Transfers().closing_handler(request)

    assert request.variables == {}


async def test_the_closing_handler_keeps_what_the_dial_agi_wrote():
    """Both AGIs write on the caller's channel: one variable space.

    `handler` consumes the entry and writes transfer; the hangup handler then
    finds nothing and must leave BOT_ACTION alone, or the CDR of a correctly
    transferred call reads hangup.
    """
    transfers = Transfers()
    transfers.transfer("c22", "asked", destination="sales_queue")

    channel = FakeRequest(args=["c22"])
    await transfers.handler(channel)
    closing = FakeRequest(args=["c22"])
    closing.variables = channel.variables           # same channel
    await transfers.closing_handler(closing)

    assert channel.variables["BOT_ACTION"] == "transfer"
    assert channel.variables["BOT_DESTINATION"] == "sales_queue"


async def test_the_closing_handler_without_a_key_does_not_blow_up():
    """A dialplan that forgot the argument gets silence, not an exception:
    a handler that raises leaves the hangup sequence half done."""
    request = FakeRequest(args=[])
    await Transfers().closing_handler(request)
    assert request.variables == {}
