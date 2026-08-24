"""The voice provider contract.

The seam that keeps this project from aging. On one side of this contract is
Asterisk, on the other one file per provider, so a provider appearing or
disappearing costs exactly one file.

It is a `typing.Protocol`, not a base class: an adapter needs these methods,
not an inheritance chain. That keeps each adapter readable on its own, which
matters more here than usual because these files are teaching material.

A provider must never appear as a concrete type inside the core; one struct
field like `tts *deepgram.Client` and the swappable-agent argument is gone. See
`docs/decisions.md`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable


class UnknownProvider(LookupError):
    """The dialplan asked for a provider that is not registered.

    A `LookupError` and not a `KeyError` on purpose: `KeyError` reprs its
    argument, so the message reaches the log wrapped in quotes, and this is a
    dialplan typo rather than a missing dict key.
    """


@runtime_checkable
class VoiceProvider(Protocol):
    """What a voice provider adapter has to do.

    Four methods. An adapter needing more surface than this is usually holding
    something that belongs to the adapter, not to the contract.
    """

    async def start(self) -> None:
        """Opens the provider connection and starts the conversation.

        Called once, right after MEDIA_START. The adapter authenticates here
        because Asterisk cannot: `websocket_client.conf` takes a username, a
        password, TLS and a proxy, but no HTTP header, and every provider
        authenticates with `Authorization`. That gap is a large part of why
        this process exists.
        """
        ...

    async def send_audio(self, chunk: bytes) -> None:
        """Hands the caller's audio to the provider.

        `chunk` arrives in the format the dialplan picked in the `Dial`: G.711
        (alaw/ulaw) at 8 kHz, or linear PCM for `slin*`. It is in
        `media.audio_format`. Converting or resampling belongs to the adapter:
        in the core, every provider would pay for the worst case.
        """
        ...

    async def on_dtmf(self, digit: str) -> None:
        """Handles one keypad digit.

        Digits arrive out of band as `DTMF_END` events, never as tones inside
        the audio, so the provider cannot hear them. Only called when the
        session was built with `forward_dtmf=True`, which is not the default:
        the keypad carries what people do not say out loud.
        """
        ...

    async def close(self) -> None:
        """Releases the connection. Has to tolerate being called twice."""
        ...


# What `ProviderRegistry` stores. The argument is the `Session`, typed as `Any`
# because `session.py` imports this module and the annotation would be circular.
ProviderFactory = Callable[[Any], VoiceProvider]


class ProviderRegistry:
    """Maps the dialplan's `AI_PROVIDER` value to its adapter.

    Registration is explicit, not discovered: auto-discovery would be shorter
    to write and worse to read, and reading is the point here.
    """

    def __init__(self) -> None:
        self._factories: dict[str, ProviderFactory] = {}

    def register(self, name: str, factory: ProviderFactory) -> None:
        """Registers an adapter factory under a dialplan name.

        Args:
            name: The `AI_PROVIDER` value, matched case-insensitively.
            factory: Called with the `Session` when a call arrives.

        Raises:
            ValueError: If the name is already taken. Overwriting silently
                hides which adapter a call is going to get; a test that needs
                a double builds its own registry.
        """
        key = name.strip().lower()
        if key in self._factories:
            raise ValueError(
                f"Provider {key!r} is already registered. Use another name, "
                f"or build a separate ProviderRegistry."
            )
        self._factories[key] = factory

    def create(self, name: str, session: Any) -> VoiceProvider:
        """Builds the adapter the dialplan asked for.

        Args:
            name: The `AI_PROVIDER` value that came in the MEDIA_START.
            session: Handed straight to the factory.

        Raises:
            UnknownProvider: If nothing is registered under that name. The
                message lists what is, because the typo is usually in the
                dialplan.
        """
        key = name.strip().lower()
        factory = self._factories.get(key)
        if factory is None:
            known = ", ".join(sorted(self._factories)) or "none registered"
            raise UnknownProvider(
                f"Unknown provider {name!r}. Available: {known}"
            )
        return factory(session)

    @property
    def names(self) -> list[str]:
        """The registered names, sorted."""
        return sorted(self._factories)
