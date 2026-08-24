"""
The doubles shared by the provider adapter tests.

The three adapters (Deepgram, OpenAI, ElevenLabs) talk to their provider over a
WebSocket and to the channel through the session. Here live the two fake
halves, so that no test opens a socket or spends a key.
"""

from __future__ import annotations

import json
from typing import Any


class FakeMedia:
    """The MEDIA_START of a normal telephony call."""

    def __init__(self, audio_format: str = "alaw") -> None:
        self.audio_format = audio_format
        self.optimal_frame_size = 160
        self.ptime = 20
        self.passthrough = False
        self.silence_byte = 0xD5


class FakeSpeech:
    """The speaking turn, recording what was asked of it."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.played: list[bytes] = []

        # How many ms of the bot's audio the caller heard, at most. None means
        # "the library does not know" (no format, or a pause broke the clock),
        # and it is the answer that makes an adapter NOT truncate. A double
        # that returned a number here would let a bug through: sending an
        # invented value is exactly what the provider's server rejects. The
        # real one keeps answering a number after the turn ended, until the
        # next `play`: that is why adapters check `bot_audio_playing` first.
        self.played_ms_at_most: int | None = None
        self.bot_audio_playing = True

    async def play(self, block: bytes) -> None:
        self.played.append(block)
        self.calls.append("play")

    async def interrupt(self) -> None:
        self.calls.append("interrupt")

    async def end_turn(self, reset_discard: bool = False) -> None:
        self.calls.append("end_turn")

    def resume(self) -> None:
        self.calls.append("resume")

    def note_caller_activity(self) -> None:
        self.calls.append("note_caller_activity")


class FakeSession:
    """The galcymedia session, with no channel behind it."""

    def __init__(self, audio_format: str = "alaw") -> None:
        self.media = FakeMedia(audio_format)
        self.speech = FakeSpeech()
        self.answered = False
        self.hung_up = False
        self.events: list = []

    def accept_audio(self) -> None:
        self.accepting_audio = True

    async def answer(self) -> None:
        # Answering before the door to the audio is open leaves a gap where
        # the person is already talking and nobody is listening.
        assert getattr(self, "accepting_audio", False), (
            "accept_audio() has to come BEFORE answer()")
        self.answered = True

    async def hangup(self) -> None:
        self.hung_up = True

    def emit(self, event: Any) -> None:
        self.events.append(event)

    def emitted(self, event_type: str) -> list:
        """The published events of one type, in order.

        Takes the type as a string on purpose: what a client reads off the wire
        is the string, so asserting on it also checks the name we publish.
        """
        return [e for e in self.events if e.type.value == event_type]


class FakeSocket:
    """A WebSocket that only remembers what was sent to it."""

    def __init__(self) -> None:
        self.sent: list = []
        self.closed = False

    async def send(self, message: Any) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True

    def json_sent(self) -> list[dict]:
        return [json.loads(m) for m in self.sent if isinstance(m, str)]


class RaisingSocket:
    """A WebSocket that blows up on send, like the real one when the caller hangs up.

    The normal double (FakeSocket) never fails, so the adapters' `except`
    branches never ran in tests: a bug there (a reference to an unimported
    symbol, for example) would slip by invisibly. This double exercises that
    path. By default it raises the same error the `websockets` library raises
    when the other side closes the connection.
    """

    def __init__(self, exc: Any = None) -> None:
        if exc is None:
            import websockets
            exc = websockets.ConnectionClosed(None, None)
        self._exc = exc
        self.closed = False

    async def send(self, message: Any) -> None:
        raise self._exc

    async def close(self) -> None:
        self.closed = True


class FakeTime:
    """A clock a `SilenceFiller` reads and a `sleep` that advances it.

    No real sleep anywhere: on Windows `asyncio.sleep(0.02)` lasts ~31 ms and
    a pacing assertion drifts. `on_sleep(n)` runs before each sleep, to play
    the channel (a real frame arriving) at a chosen moment; after `steps`
    sleeps the filler is stopped.
    """

    def __init__(self, filler: Any, steps: int, on_sleep: Any = None) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []
        self._filler = filler
        self._steps = steps
        self._on_sleep = on_sleep
        filler._clock = lambda: self.now
        filler._sleep = self.sleep
        filler._last_caller_frame = self.now

    def quiet_for(self, seconds: float) -> None:
        """The channel has been silent for this long already."""
        self._filler._last_caller_frame = self.now - seconds

    async def sleep(self, seconds: float) -> None:
        n = len(self.sleeps)
        if self._on_sleep is not None:
            await self._on_sleep(n)
        self.sleeps.append(seconds)
        self.now += seconds
        if len(self.sleeps) >= self._steps:
            self._filler.stop()
