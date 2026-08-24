"""Provider registry: the dialplan's `AI_PROVIDER` value maps to the adapter
that serves the call.

`VoiceProvider` is a Protocol with no logic, so there is nothing to test there.
"""

from __future__ import annotations

import pytest

from galcymedia import ProviderRegistry
from galcymedia.provider import UnknownProvider


def _factory(brand):
    """A fake provider factory: returns whatever it was called with."""
    def make(session):
        return {"brand": brand, "session": session}
    return make


def test_creates_the_registered_provider_passing_it_the_session():
    """An adapter registered by name is built with that call's session.

    The session is the only argument, and it carries the call's format in
    `session.media`.
    """
    registry = ProviderRegistry()
    registry.register("openai", _factory("openai"))

    provider = registry.create("openai", "SESSION")

    assert provider["brand"] == "openai"
    assert provider["session"] == "SESSION"


def test_the_name_is_normalized_on_register_and_on_create():
    """The name is normalized on both register and create.

    The dialplan sends whatever was typed (`Set(_AI_PROVIDER=OpenAI )`), and a
    trailing space there would otherwise be an unregistered provider.
    """
    registry = ProviderRegistry()
    registry.register("  OpenAI ", _factory("openai"))

    # Different spellings of the same name all land on the same adapter.
    for spelling in ("openai", "OPENAI", "  OpenAI  ", "OpenAi"):
        assert registry.create(spelling, "S")["brand"] == "openai"


def test_an_unknown_name_raises_with_the_list_of_available_ones():
    """An unknown `AI_PROVIDER` raises with the names that do exist.

    The typo is in the dialplan, so the list is the hint that resolves it. A
    `LookupError` and not `KeyError`: the latter reprs its argument and reaches
    the log wrapped in quotes.
    """
    registry = ProviderRegistry()
    registry.register("openai", _factory("openai"))
    registry.register("deepgram", _factory("deepgram"))

    with pytest.raises(UnknownProvider) as exc:
        registry.create("gemini", "S")

    message = str(exc.value)
    assert not message.startswith('"'), "KeyError-style quoting is back"
    assert "gemini" in message
    assert "deepgram" in message and "openai" in message


def test_an_empty_registry_says_so_in_the_error():
    """An empty registry says so instead of listing nothing.

    Otherwise the message ends in a colon and the reader cannot tell an empty
    registry from a truncated one.
    """
    with pytest.raises(UnknownProvider) as exc:
        ProviderRegistry().create("openai", "S")

    assert "none registered" in str(exc.value)


def test_names_returns_the_registered_ones_sorted():
    """`names` comes normalized and sorted.

    It is what an error message and a startup log list, so registration order
    must not change what the reader sees.
    """
    registry = ProviderRegistry()
    registry.register("openai", _factory("openai"))
    registry.register("  Deepgram ", _factory("deepgram"))
    registry.register("elevenlabs", _factory("elevenlabs"))

    # Normalized (lowercase, no spaces) and sorted alphabetically.
    assert registry.names == ["deepgram", "elevenlabs", "openai"]


def test_registering_the_same_name_twice_raises():
    """A name is taken once.

    Overwriting silently hides which adapter a call gets, and the registration
    that loses is the one written first. A test that needs a double builds its
    own registry.
    """
    registry = ProviderRegistry()
    registry.register("openai", _factory("first"))

    with pytest.raises(ValueError) as exc:
        registry.register("  OpenAI ", _factory("second"))

    assert "openai" in str(exc.value), "the message does not name the clash"
    assert registry.create("openai", "S")["brand"] == "first", \
        "the first registration lost anyway"
