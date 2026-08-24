"""
The ElevenLabs adapter.

This provider is the one that diverges the most: it transcodes and has no
phrase injection. Both differences are channel-related, so most of these tests
check exactly that.
"""

from __future__ import annotations

import base64
import json
import urllib.error

import pytest
from conftest import FakeMedia, FakeSession, FakeSocket, FakeTime, RaisingSocket

from galcymedia import pcm
from galcymedia.adapters.elevenlabs import ElevenLabsProvider, resolve_audio_path


def new_provider(session=None, **kwargs):
    """An adapter with the socket already swapped, connected to no one."""
    session = session or FakeSession("ulaw")
    kwargs.setdefault("agent_id", "ag-1")
    provider = ElevenLabsProvider(session, **kwargs)
    provider.ws = FakeSocket()
    return provider


# ---------------------------------------------------------------------------
# The audio path: this provider's divergence
# ---------------------------------------------------------------------------


def test_a_ulaw_channel_does_not_transcode():
    """The provider speaks native ulaw: there is nothing to convert."""
    path = resolve_audio_path(FakeMedia("ulaw"))

    assert path.to_provider(b"\x01\x02") == b"\x01\x02"
    assert path.to_channel(b"\x01\x02") == b"\x01\x02"
    assert path.silence_byte == pcm.ULAW_SILENCE


def test_an_alaw_channel_transcodes_in_both_directions():
    """The provider does NOT speak alaw: convert on the way in and on the way out."""
    path = resolve_audio_path(FakeMedia("alaw"))
    block = bytes(range(256))

    assert path.to_provider(block) == block.translate(pcm.ALAW_TO_ULAW)
    assert path.to_channel(block) == block.translate(pcm.ULAW_TO_ALAW)


def test_silence_matches_the_channel_codec():
    """Padding with the wrong byte inserts a click at the end of every phrase."""
    assert resolve_audio_path(FakeMedia("alaw")).silence_byte == pcm.ALAW_SILENCE
    assert resolve_audio_path(FakeMedia("ulaw")).silence_byte == pcm.ULAW_SILENCE


def test_a_non_g711_codec_fails_early():
    with pytest.raises(RuntimeError, match="only speaks G.711"):
        resolve_audio_path(FakeMedia("slin16"))


def test_the_speaking_turn_pads_with_the_channel_byte():
    """The padding byte is the CHANNEL's: the bot's audio is converted with
    `to_channel` before it is padded, so an alaw channel pads with alaw. That
    is the byte the session derives on its own, so the adapter keeps the
    session's speaking turn instead of building a second one."""
    session = FakeSession("alaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")

    assert provider.speech is session.speech, "a second SpeechState was built"
    assert provider.audio_path.silence_byte == session.media.silence_byte
    assert provider.audio_path.silence_byte == pcm.ALAW_SILENCE


# ---------------------------------------------------------------------------
# Event translation
# ---------------------------------------------------------------------------


async def test_the_bot_audio_arrives_converted_to_the_channel_codec():
    session = FakeSession("alaw")
    provider = new_provider(session)
    ulaw_bytes = bytes(range(64))

    await provider._on_event(json.dumps({
        "type": "audio",
        "audio_event": {"audio_base_64":
                        base64.b64encode(ulaw_bytes).decode("ascii")},
    }))

    assert session.speech.played == [ulaw_bytes.translate(pcm.ULAW_TO_ALAW)]


async def test_the_interruption_reaches_the_speaking_turn():
    provider = new_provider()

    await provider._on_event(json.dumps({"type": "interruption"}))

    assert "interrupt" in provider.session.speech.calls


async def test_the_caller_speaking_closes_the_bot_turn():
    """This provider does not send an end-of-turn of its own.

    Its signal is the caller's transcript, so the adapter translates it into an
    end of turn. It is a channel compensation the user would have no reason to
    discover.
    """
    provider = new_provider()

    await provider._on_event(json.dumps({
        "type": "user_transcript",
        "user_transcription_event": {"user_transcript": "hi there"},
    }))

    assert "end_turn" in provider.session.speech.calls
    assert "note_caller_activity" in provider.session.speech.calls


async def test_the_caller_transcript_uses_the_sdk_envelope():
    """The caller's text is read from `user_transcription_event`, the SDK's name.

    It is the one envelope that does not follow `<type>_event`
    (`conversation.py:619-621`). A generic `user_transcript_event` guess falls
    back to the whole event, the text comes out empty, and a real call shows
    every `[assistant]` line and not a single `[user]` one (measured against
    the real agent on 2026-08-21).
    """
    heard = []
    provider = new_provider(on_transcript=lambda role, text, final: heard.append((role, text)))

    await provider._on_event(json.dumps({
        "type": "user_transcript",
        "user_transcription_event": {"user_transcript": "hi there", "event_id": 58},
    }))

    assert heard == [("user", "hi there")], (
        f"published {heard}: the caller's words never reach the transcript")


async def test_the_ping_is_answered_with_its_event_id():
    """As the SDK does (`conversation.py:991-997`). Measured: the pong is not
    what keeps the session open while audio flows, so no "or it hangs up"."""
    provider = new_provider()

    await provider._on_event(json.dumps({
        "type": "ping", "ping_event": {"event_id": 42},
    }))

    assert provider.ws.json_sent()[-1] == {"type": "pong", "event_id": 42}


async def test_it_warns_if_the_agent_is_not_in_ulaw(caplog):
    """The format belongs to the AGENT, not the call: it is only fixed in its panel."""
    provider = new_provider()

    with caplog.at_level("ERROR"):
        await provider._on_event(json.dumps({
            "type": "conversation_initiation_metadata",
            "conversation_initiation_metadata_event": {
                "user_input_audio_format": "pcm_16000",
            },
        }))

    assert "pcm_16000" in caplog.text


async def test_an_unreadable_message_does_not_break_the_call():
    provider = new_provider()

    await provider._on_event("{no es json")

    assert provider.session.speech.calls == []


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


async def test_tools_are_passed_to_your_hook():
    """Their schemas live in the provider's panel; the execution is yours."""
    received = []

    async def my_hook(name, arguments):
        received.append((name, arguments))
        return {"ok": True, "time": "10:30"}

    provider = new_provider(on_function_call=my_hook)

    await provider._on_event(json.dumps({
        "type": "client_tool_call",
        "client_tool_call": {"tool_name": "tell_the_time",
                             "tool_call_id": "t1",
                             "parameters": {"zone": "Lima"}},
    }))

    assert received == [("tell_the_time", {"zone": "Lima"})]

    response = provider.ws.json_sent()[-1]
    assert response["type"] == "client_tool_result"
    assert response["tool_call_id"] == "t1"
    assert response["is_error"] is False


async def test_if_your_tool_fails_it_is_marked_as_an_error():
    """`is_error` comes from YOUR result, not from an adapter assumption."""
    async def my_hook(name, arguments):
        return {"ok": False, "error": "no slot available"}

    provider = new_provider(on_function_call=my_hook)

    await provider._on_event(json.dumps({
        "type": "client_tool_call",
        "client_tool_call": {"tool_name": "x", "tool_call_id": "t1",
                             "parameters": {}},
    }))

    assert provider.ws.json_sent()[-1]["is_error"] is True


async def test_closing_tolerates_being_called_twice():
    provider = new_provider()

    await provider.close()
    await provider.close()

    assert provider.ws.closed


async def test_a_hangup_mid_sentence_does_not_crash(caplog):
    """If the caller hangs up while the bot sends audio, it is swallowed silently.

    Here the audio is also transcoded before being sent; the socket close has to
    be swallowed all the same, without an ERROR in the log.
    """
    provider = new_provider()
    provider.ws = RaisingSocket()

    with caplog.at_level("ERROR"):
        await provider.send_audio(b"\xff" * 160)

    assert not any(r.levelname == "ERROR" for r in caplog.records)


async def test_after_the_provider_drops_send_audio_stays_quiet():
    """Once the provider closed on its own, the caller's frames keep arriving
    until Asterisk releases the channel: none of them may touch the socket.

    Seen in a real call (`03-multi`/elevenlabs, 2026-08-22): a DEBUG traceback
    per frame between the provider's close (1000) and the hangup.
    """
    provider = new_provider()

    await provider._hangup_if_alive()
    await provider.send_audio(b"\xff" * 160)

    assert provider.session.hung_up
    assert provider.ws.sent == []


# ---------------------------------------------------------------------------
# The signed URL for private agents and everything that can go wrong there
# ---------------------------------------------------------------------------


class _FakeResponse:
    """What urlopen returns: a context manager with a `read()`."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self._body


def _patch_urlopen(monkeypatch, behavior):
    import galcymedia.adapters.elevenlabs as mod
    monkeypatch.setattr(mod.urllib.request, "urlopen", behavior)


async def test_without_a_key_it_connects_directly_without_fetching_a_signed_url(monkeypatch):
    """A public agent needs no signed URL: it connects directly.

    Touching the network here would be a bug, so urlopen is broken on purpose.
    """
    def must_not_be_called(*a, **k):
        raise AssertionError("should not fetch a signed URL without a key")

    _patch_urlopen(monkeypatch, must_not_be_called)
    provider = ElevenLabsProvider(FakeSession("ulaw"), agent_id="ag-1")

    url = await provider._resolve_url()

    assert url.endswith("agent_id=ag-1")


async def test_with_a_key_it_fetches_and_returns_the_signed_url(monkeypatch):
    _patch_urlopen(monkeypatch, lambda *a, **k: _FakeResponse(
        json.dumps({"signed_url": "wss://signed/x"}).encode()))
    provider = ElevenLabsProvider(FakeSession("ulaw"), agent_id="ag-1",
                                  api_key="key-x")

    url = await provider._resolve_url()

    assert url == "wss://signed/x"


async def test_a_rejected_key_gives_a_clear_error(monkeypatch):
    def rejects(*a, **k):
        raise urllib.error.HTTPError("u", 401, "no", {}, None)

    _patch_urlopen(monkeypatch, rejects)
    provider = ElevenLabsProvider(FakeSession("ulaw"), agent_id="ag-1",
                                  api_key="bad-key")

    with pytest.raises(RuntimeError, match="rejected the key"):
        await provider._resolve_url()


async def test_another_status_fetching_the_url_is_reported(monkeypatch):
    def error_500(*a, **k):
        raise urllib.error.HTTPError("u", 500, "boom", {}, None)

    _patch_urlopen(monkeypatch, error_500)
    provider = ElevenLabsProvider(FakeSession("ulaw"), agent_id="ag-1",
                                  api_key="key-x")

    with pytest.raises(RuntimeError, match="500"):
        await provider._resolve_url()


async def test_a_response_without_a_signed_url_warns(monkeypatch):
    _patch_urlopen(monkeypatch, lambda *a, **k: _FakeResponse(
        json.dumps({"otra_cosa": 1}).encode()))
    provider = ElevenLabsProvider(FakeSession("ulaw"), agent_id="ag-1",
                                  api_key="key-x")

    with pytest.raises(RuntimeError, match="agent_id"):
        await provider._resolve_url()


async def test_the_network_being_down_fetching_the_url_gives_a_clear_error(monkeypatch):
    """Regression: with the key set and no network, the operator has to get a
    hint, not urllib's raw URLError.

    It is the same translation connect() does; before, this path leaked it."""
    def no_network(*a, **k):
        raise urllib.error.URLError("dns down")

    _patch_urlopen(monkeypatch, no_network)
    provider = ElevenLabsProvider(FakeSession("ulaw"), agent_id="ag-1",
                                  api_key="key-x")

    with pytest.raises(RuntimeError, match="internet access"):
        await provider._resolve_url()


async def test_a_non_json_response_gives_a_clear_error(monkeypatch):
    """A proxy or captive portal returns HTML; it cannot surface a bare
    JSONDecodeError."""
    _patch_urlopen(monkeypatch, lambda *a, **k: _FakeResponse(
        b"<html>captive portal</html>"))
    provider = ElevenLabsProvider(FakeSession("ulaw"), agent_id="ag-1",
                                  api_key="key-x")

    with pytest.raises(RuntimeError, match="could not be understood"):
        await provider._resolve_url()


# ---------------------------------------------------------------------------
# The silence filler goes through the audio path, not through send_audio
# ---------------------------------------------------------------------------


def _provider_audio(provider) -> list[bytes]:
    return [base64.b64decode(m["user_audio_chunk"])
            for m in provider.ws.json_sent() if "user_audio_chunk" in m]


async def test_the_filler_goes_through_the_audio_path():
    """The filler frame is CHANNEL silence and takes the same path as the
    caller's audio: an alaw channel's filler reaches the provider transcoded
    through the same table as the phone's own silence (alaw 0xD5 lands on
    ulaw 0xFE, amplitude 8 of 32,767), base64 wrapped. A raw alaw byte
    would be a click in the provider's ears."""
    provider = new_provider(session=FakeSession("alaw"))
    filler = provider._build_filler()
    fake = FakeTime(filler, steps=2)
    fake.quiet_for(1.0)

    await filler.run()

    channel_silence = bytes([pcm.ALAW_SILENCE]) * 160
    assert _provider_audio(provider) == [channel_silence.translate(pcm.ALAW_TO_ULAW)]
    assert _provider_audio(provider) != [channel_silence], "sent raw alaw to a ulaw provider"


async def test_the_filler_does_not_reset_the_gap_clock():
    """Only `send_audio` reports a frame: a filler frame that did would
    switch the filler off and on every 300 ms, and it must not count as a
    caller frame either."""
    provider = new_provider(session=FakeSession("ulaw"))
    filler = provider._build_filler()
    fake = FakeTime(filler, steps=3)
    fake.quiet_for(1.0)
    quiet_since = filler._last_caller_frame

    await filler.run()

    assert len(_provider_audio(provider)) == 2, "two filler frames were expected"
    assert filler._last_caller_frame == quiet_since, "a filler frame reset the clock"
    assert provider._frames_in == 0, "filler frames counted as caller frames"

    await provider.send_audio(b"\xff" * 160)
    assert filler._last_caller_frame == fake.now, "a real frame must reset it"
    assert provider._frames_in == 1
