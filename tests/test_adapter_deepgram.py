"""
The Deepgram adapter.

No socket is opened and no key is spent: the provider's WebSocket is replaced
by a double that records what gets sent to it. What is tested is the part we
have to maintain, which is the translation between its events and the channel's
turn-taking.

What this file does NOT test, on purpose, is the prompt, the tools or the
phrases: they don't live here. They live in the agent of whoever uses the
library.
"""

from __future__ import annotations

import json

import pytest
from conftest import FakeMedia, FakeSession, FakeSocket, FakeTime, RaisingSocket

from galcymedia.adapters.deepgram import DeepgramProvider, resolve_codec


def new_provider(session=None, **kwargs):
    """An adapter with the socket already replaced, without connecting to anyone."""
    session = session or FakeSession()
    kwargs.setdefault("api_key", "unused")
    kwargs.setdefault("settings", lambda media: {"type": "Settings"})
    provider = DeepgramProvider(session, **kwargs)
    provider.ws = FakeSocket()
    return provider


# ---------------------------------------------------------------------------
# The codec comes from the channel
# ---------------------------------------------------------------------------


def test_the_codec_is_translated_to_the_provider_name():
    assert resolve_codec(FakeMedia("alaw")) == "alaw"
    assert resolve_codec(FakeMedia("ALAW")) == "alaw"
    assert resolve_codec(FakeMedia("ulaw")) == "mulaw"


def test_a_codec_the_provider_does_not_speak_fails_early():
    """It fails at startup and not mid-conversation.

    Falling back to a default value kills the call later, with a provider error
    that says nothing about the dialplan that caused it.
    """
    with pytest.raises(RuntimeError, match="only speaks"):
        resolve_codec(FakeMedia("slin16"))


# ---------------------------------------------------------------------------
# Event translation: this is the part we have to maintain
# ---------------------------------------------------------------------------


async def test_the_bot_audio_goes_to_the_turn_taking():
    provider = new_provider()

    await provider.speech.play(b"\x01" * 160)

    assert provider.session.speech.played == [b"\x01" * 160]


async def test_the_caller_talking_over_interrupts():
    """This is the most important translation: its speech event into barge-in."""
    provider = new_provider()

    await provider._on_event(json.dumps({"type": "UserStartedSpeaking"}))

    assert "interrupt" in provider.session.speech.calls


async def test_the_end_of_audio_closes_the_turn():
    provider = new_provider()

    await provider._on_event(json.dumps({"type": "AgentAudioDone"}))

    assert "end_turn" in provider.session.speech.calls


async def test_a_new_response_reopens_the_bot_voice():
    """Without this, an interruption over the end of a turn leaves the bot mute."""
    provider = new_provider()

    await provider._on_event(json.dumps({
        "type": "ConversationText", "role": "assistant", "content": "hello",
    }))

    assert "resume" in provider.session.speech.calls


async def test_only_the_person_resets_the_silence_clock():
    """If the bot reset it, an abandoned line would never be detected."""
    provider = new_provider()

    await provider._on_event(json.dumps({
        "type": "ConversationText", "role": "assistant", "content": "hello",
    }))
    assert "note_caller_activity" not in provider.session.speech.calls

    await provider._on_event(json.dumps({
        "type": "ConversationText", "role": "user", "content": "hi there",
    }))
    assert "note_caller_activity" in provider.session.speech.calls


async def test_an_unreadable_message_does_not_break_the_call():
    provider = new_provider()

    await provider._on_event("{no es json")

    assert provider.session.speech.calls == []


# ---------------------------------------------------------------------------
# The boundary: the tools belong to the user
# ---------------------------------------------------------------------------


async def test_the_tools_are_handed_to_your_handler():
    """The adapter does not know what any tool does: it just delivers them."""
    received = []

    async def my_handler(name, arguments):
        received.append((name, arguments))
        return {"ok": True, "time": "10:30"}

    provider = new_provider(on_function_call=my_handler)

    await provider._on_event(json.dumps({
        "type": "FunctionCallRequest",
        "functions": [{"id": "1", "name": "tell_the_time",
                       "arguments": '{"zone": "Lima"}'}],
    }))

    assert received == [("tell_the_time", {"zone": "Lima"})]

    # And what you returned travels back to the model unchanged.
    response = provider.ws.json_sent()[-1]
    assert response["type"] == "FunctionCallResponse"
    assert response["name"] == "tell_the_time"
    assert json.loads(response["content"]) == {"ok": True, "time": "10:30"}


async def test_a_tool_that_blows_up_still_gets_a_response():
    """Without a response, the model keeps waiting and the call stalls."""
    async def broken_handler(name, arguments):
        raise RuntimeError("the database does not answer")

    provider = new_provider(on_function_call=broken_handler)

    await provider._on_event(json.dumps({
        "type": "FunctionCallRequest",
        "functions": [{"id": "1", "name": "lookup", "arguments": "{}"}],
    }))

    response = provider.ws.json_sent()[-1]
    assert response["type"] == "FunctionCallResponse"
    assert json.loads(response["content"])["ok"] is False


async def test_without_a_handler_the_tools_are_rejected_with_a_notice():
    """The adapter does not invent a response: it says there is no one to handle it."""
    provider = new_provider()

    await provider._on_event(json.dumps({
        "type": "FunctionCallRequest",
        "functions": [{"id": "1", "name": "whatever", "arguments": "{}"}],
    }))

    response = provider.ws.json_sent()[-1]
    assert json.loads(response["content"])["ok"] is False


async def test_unreadable_arguments_do_not_kill_the_call():
    received = []

    async def my_handler(name, arguments):
        received.append(arguments)
        return {"ok": True}

    provider = new_provider(on_function_call=my_handler)

    await provider._on_event(json.dumps({
        "type": "FunctionCallRequest",
        "functions": [{"id": "1", "name": "x", "arguments": "{broken"}],
    }))

    assert received == [{}]


# ---------------------------------------------------------------------------
# What can be asked of the provider
# ---------------------------------------------------------------------------


async def test_a_literal_phrase_can_be_requested():
    """The mechanism belongs to the channel; the phrase is set by you."""
    provider = new_provider()

    await provider.say("Are you still there?", behavior="interrupt")

    message = provider.ws.json_sent()[-1]
    assert message["type"] == "InjectAgentMessage"
    assert message["message"] == "Are you still there?"
    assert message["behavior"] == "interrupt"


async def test_closing_tolerates_being_called_twice():
    provider = new_provider()

    await provider.close()
    await provider.close()

    assert provider.ws.closed


async def test_a_hangup_mid_sentence_does_not_crash(caplog):
    """If the caller hangs up while the bot is sending audio, it is swallowed silently.

    A mid-call hangup is the most normal thing in the world in telephony: it
    must not raise an exception nor dirty the log with an ERROR.
    """
    provider = new_provider()
    provider.ws = RaisingSocket()

    with caplog.at_level("ERROR"):
        await provider.send_audio(b"\x00\x00")

    assert not any(r.levelname == "ERROR" for r in caplog.records)


async def test_after_the_provider_drops_send_audio_stays_quiet():
    """Once the provider closed on its own, the caller's frames keep arriving
    until Asterisk releases the channel: none of them may touch the socket.

    Seen in a real call: a DEBUG traceback per frame between the provider's
    close and the hangup.
    """
    provider = new_provider()

    await provider._hangup_if_alive()
    await provider.send_audio(b"\x00" * 160)

    assert provider.session.hung_up
    assert provider.ws.sent == []


# ---------------------------------------------------------------------------
# The call startup
# ---------------------------------------------------------------------------


async def test_start_connects_sends_settings_and_answers(monkeypatch):
    """Starting up has to connect, send the user's Settings and answer the
    call, in that order. The real connection is replaced by a double."""
    import galcymedia.adapters.deepgram as mod

    socket = FakeSocket()

    async def fake_connect(url, headers, provider, hint):
        # The auth header is the only bit of transport we have to care about.
        assert headers["Authorization"].startswith("Token ")
        return socket

    monkeypatch.setattr(mod._shared, "connect", fake_connect)

    session = FakeSession("ulaw")
    provider = DeepgramProvider(
        session, api_key="key-x",
        settings=lambda media: {"type": "Settings", "codec": media.audio_format},
    )

    await provider.start()

    # I send the user's Settings unchanged, and I answered the call.
    assert socket.json_sent()[0] == {"type": "Settings", "codec": "ulaw"}
    assert session.answered

    await provider.close()


async def test_start_fails_early_with_a_codec_that_is_not_g711():
    """The codec is resolved before touching the network: if it does not work,
    it fails here and not mid-conversation."""
    provider = DeepgramProvider(
        FakeSession("slin16"), api_key="x",
        settings=lambda media: {},
    )

    with pytest.raises(RuntimeError, match="only speaks"):
        await provider.start()


# ---------------------------------------------------------------------------
# The silence keepalive
# ---------------------------------------------------------------------------


REAL = b"\x01" * 160


async def test_the_filler_is_the_provider_codec_silence_sent_as_is():
    """Deepgram speaks the channel's codec: the filler frame is that codec's
    silence and reaches the socket untouched (the loop itself is tested in
    `test_adapter_shared.py`)."""
    provider = new_provider(session=FakeSession("ulaw"))
    filler = provider._build_filler()
    fake = FakeTime(filler, steps=2)
    fake.quiet_for(1.0)

    await filler.run()

    from galcymedia import pcm
    assert provider.ws.sent == [bytes([pcm.ULAW_SILENCE]) * 160]


async def test_the_filler_does_not_reset_the_gap_clock():
    """Only a real caller frame (`send_audio`) restarts the gap. A filler
    frame that did would switch the filler off and on every 300 ms."""
    provider = new_provider(session=FakeSession("ulaw"))
    filler = provider._build_filler()
    fake = FakeTime(filler, steps=3)
    fake.quiet_for(1.0)
    quiet_since = filler._last_caller_frame

    await filler.run()

    assert len(provider.ws.sent) == 2, "two filler frames were expected"
    assert filler._last_caller_frame == quiet_since, "a filler frame reset the clock"

    await provider.send_audio(REAL)
    assert filler._last_caller_frame == fake.now, "a real frame must reset it"
