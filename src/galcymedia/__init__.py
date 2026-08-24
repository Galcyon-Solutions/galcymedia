"""
galcymedia: the media server for Asterisk's chan_websocket.

Asterisk 23 ships `chan_websocket` (in since 23.0.0), a channel that sends
and receives raw audio over a WebSocket: no RTP, no headers, no UDP, no NAT.
What is missing on the other end is someone who speaks the protocol, honors
the flow control, aligns the frames and hands you clean audio. That is
galcymedia. It does not know what a voice provider is, on purpose: that is
what makes it serve a conversational agent, a transcriber or a recorder
alike. The module map is in `docs/architecture.md`.

The minimum that works:

    import asyncio
    from galcymedia import serve

    class MyAgent:
        def __init__(self, session):
            self.session = session

        async def start(self):
            ...                                   # connect to your provider
            self.session.accept_audio()           # open the door to audio
            await self.session.answer()           # and only then answer

        async def send_audio(self, chunk):
            await self.session.send_audio(chunk)  # echo it back

        async def on_dtmf(self, digit):
            ...

        async def close(self):
            ...

    asyncio.run(serve(MyAgent))

And in the Asterisk dialplan:

    exten => 9999,1,Answer()
     same => n,Dial(WebSocket/voicebot/c(ulaw)f(json),3600,g)

That is all. Complete examples live in the galcymedia-examples repository.
"""

from __future__ import annotations

from . import events, pcm
from .agi import AgiRequest, AgiRouter, AgiServer
from .decisions import CallDecisions, Transfers
from .events import Event as RTVIEvent
from .events import EventType, describe_events
from .framing import FrameAligner
from .observer import ChannelObserver
from .protocol import (
    MAX_CONTROL_MESSAGE_BYTES,
    MAX_WEBSOCKET_MESSAGE_BYTES,
    Command,
    Event,
    MediaStart,
    build_command,
    parse_event,
    parse_media_start,
)
from .provider import ProviderRegistry, UnknownProvider, VoiceProvider
from .server import connect, serve
from .session import Session
from .speech import SpeechState

__version__ = "0.1.0"

# Grouped by who uses each name, on purpose; alphabetical order would erase
# that, so the sort rule is waived here.
__all__ = [  # noqa: RUF022
    # What almost everybody uses
    "serve",
    "connect",
    "Session",
    "VoiceProvider",
    "FrameAligner",
    # The bot's speaking turn: barge-in solved once
    "SpeechState",
    # Call events, in RTVI format
    "events",
    "RTVIEvent",
    "EventType",
    # The event surface, discoverable without reading the source
    "describe_events",
    # The channel protocol trace, ready to pass as on_event
    "ChannelObserver",
    # The return to the dialplan, optional
    "AgiRequest",
    "AgiRouter",
    "AgiServer",
    "CallDecisions",
    "Transfers",
    # The PCM bridge and the G.711 tables: three adapters use it
    "pcm",
    # For whoever serves several providers in one process
    "ProviderRegistry",
    "UnknownProvider",
    # The protocol, for whoever wants to go one level down
    "MediaStart",
    "Command",
    "Event",
    "build_command",
    "parse_event",
    "parse_media_start",
    "MAX_CONTROL_MESSAGE_BYTES",
    "MAX_WEBSOCKET_MESSAGE_BYTES",
    "__version__",
]
