"""
The OpenAI Realtime adapter.

Same approach as the Deepgram one: the provider WebSocket is swapped for a
double, and the translation between its events and the turn-taking is tested.

What is NOT tested here, because it does not live here: the prompt, the tools
and the bot phrases.
"""

from __future__ import annotations

import base64
import json

import pytest
from conftest import FakeMedia, FakeSession, FakeSocket, FakeTime, RaisingSocket

from galcymedia.adapters.openai import OpenAIRealtimeProvider, resolve_codec


def new_provider(session=None, **kwargs):
    """An adapter with the socket already swapped, connected to nobody."""
    session = session or FakeSession()
    kwargs.setdefault("api_key", "unused")
    provider = OpenAIRealtimeProvider(session, **kwargs)
    provider.ws = FakeSocket()
    return provider


# ---------------------------------------------------------------------------
# The codec comes from the channel
# ---------------------------------------------------------------------------


def test_codec_translates_to_provider_type():
    assert resolve_codec(FakeMedia("alaw")) == "audio/pcma"
    assert resolve_codec(FakeMedia("ulaw")) == "audio/pcmu"
    assert resolve_codec(FakeMedia("ULAW")) == "audio/pcmu"


def test_a_codec_the_provider_does_not_speak_fails_early():
    with pytest.raises(RuntimeError, match="only speaks"):
        resolve_codec(FakeMedia("slin16"))


# ---------------------------------------------------------------------------
# The boundary: your configuration and the channel's
# ---------------------------------------------------------------------------


def test_your_config_travels_untouched_in_the_session_update():
    """The adapter neither reads nor touches your prompt or your tools."""
    provider = new_provider(session_config=lambda media: {
        "instructions": "MY PROMPT",
        "tools": [{"name": "tell_the_time"}],
    })

    update = provider._build_session_update("audio/pcma")

    assert update["session"]["instructions"] == "MY PROMPT"
    assert update["session"]["tools"] == [{"name": "tell_the_time"}]


def test_the_channel_wins_on_the_audio_format():
    """Letting you override it would break the call silently."""
    provider = new_provider(session_config=lambda media: {
        "audio": {"input": {"format": {"type": "audio/pcmu"}},
                  "output": {"voice": "marin"}},
    })

    audio = provider._build_session_update("audio/pcma")["session"]["audio"]

    assert audio["input"]["format"] == {"type": "audio/pcma"}
    assert audio["output"]["format"] == {"type": "audio/pcma"}
    assert audio["output"]["voice"] == "marin", "your value is kept"


def test_turn_taking_comes_preset_and_can_be_adjusted():
    """The server barge-in belongs to the channel, but is not forced on you."""
    provider = new_provider()
    audio = provider._build_session_update("audio/pcma")["session"]["audio"]
    assert audio["input"]["turn_detection"]["interrupt_response"] is True

    other = new_provider(session_config=lambda media: {
        "audio": {"input": {"turn_detection": {"type": "semantic_vad"}}},
    })
    audio = other._build_session_update("audio/pcma")["session"]["audio"]
    assert audio["input"]["turn_detection"] == {"type": "semantic_vad"}


def test_it_still_starts_without_any_config_from_you():
    provider = new_provider()

    update = provider._build_session_update("audio/pcma")

    assert update["type"] == "session.update"
    assert update["session"]["type"] == "realtime"


# ---------------------------------------------------------------------------
# Event translation
# ---------------------------------------------------------------------------


async def test_the_bot_audio_arrives_decoded():
    provider = new_provider()
    block = b"\x01" * 160

    await provider._on_event(json.dumps({
        "type": "response.output_audio.delta",
        "delta": base64.b64encode(block).decode("ascii"),
    }))

    assert provider.session.speech.played == [block]


async def test_the_caller_talking_over_interrupts():
    provider = new_provider()

    await provider._on_event(json.dumps({
        "type": "input_audio_buffer.speech_started",
    }))

    assert "interrupt" in provider.session.speech.calls
    assert "note_caller_activity" in provider.session.speech.calls


async def test_the_end_of_the_response_closes_the_turn():
    provider = new_provider()

    await provider._on_event(json.dumps({
        "type": "response.done", "response": {"output": []},
    }))

    assert "end_turn" in provider.session.speech.calls


async def test_the_caller_transcript_is_assembled_from_chunks():
    transcripts = []
    provider = new_provider(
        on_transcript=lambda role, text, final: transcripts.append((role, text, final))
    )

    for chunk in ("bue", "nas ", "tardes"):
        await provider._on_event(json.dumps({
            "type": "conversation.item.input_audio_transcription.delta",
            "delta": chunk,
        }))
    await provider._on_event(json.dumps({
        "type": "conversation.item.input_audio_transcription.completed",
        "transcript": "good afternoon",
    }))

    assert transcripts[-1] == ("user", "good afternoon", True)
    assert transcripts[0][2] is False, "the intermediate ones are partial"


async def test_the_bookkeeping_events_make_no_noise():
    provider = new_provider()

    for kind in ("session.created", "rate_limits.updated",
                 "response.output_item.added"):
        await provider._on_event(json.dumps({"type": kind}))

    assert provider.session.speech.calls == []


# ---------------------------------------------------------------------------
# Tools and requested responses
# ---------------------------------------------------------------------------


async def test_tools_are_handed_to_your_handler():
    received = []

    async def my_handler(name, arguments):
        received.append((name, arguments))
        return {"ok": True}

    provider = new_provider(on_function_call=my_handler)

    await provider._on_event(json.dumps({
        "type": "response.done",
        "response": {"output": [{
            "type": "function_call", "call_id": "c1",
            "name": "tell_the_time", "arguments": '{"zone": "Lima"}',
        }]},
    }))

    assert received == [("tell_the_time", {"zone": "Lima"})]

    sent = provider.ws.json_sent()
    created = next(m for m in sent if m["type"] == "conversation.item.create")
    assert created["item"]["call_id"] == "c1"
    # And then a response is requested, or the model stays silent.
    assert sent[-1]["type"] == "response.create"


async def test_you_write_the_bot_phrase_yourself():
    """This API has no literal injection: the instruction is entirely yours."""
    provider = new_provider()

    await provider.request_response("Greet in Spanish and introduce yourself.")

    message = provider.ws.json_sent()[-1]
    assert message["type"] == "response.create"
    assert message["response"]["instructions"] == (
        "Greet in Spanish and introduce yourself.")


async def test_closing_tolerates_being_called_twice():
    provider = new_provider()

    await provider.close()
    await provider.close()

    assert provider.ws.closed


async def test_a_hangup_mid_sentence_does_not_crash(caplog):
    """Regression: if the caller hangs up while the bot is sending audio, the
    adapter must swallow the close silently.

    This is the path that a badly written `except` let through: openai
    referenced `websockets.ConnectionClosed` without importing the module, so
    instead of swallowing the close it raised NameError and polluted the log
    with a false traceback on every hangup.
    """
    provider = new_provider()
    provider.ws = RaisingSocket()

    with caplog.at_level("ERROR"):
        await provider.send_audio(b"\x00\x00")

    # No exception bubbling up, and no ERROR in the log: a hangup is normal.
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
# The bot text, and the event that used to be noise
# ---------------------------------------------------------------------------


async def test_the_bot_text_arrives_while_it_is_being_pronounced():
    """`response.output_audio_transcript.delta` used to fall into the branch
    for events we do not translate, and it printed ninety lines of noise per
    call. It was never an unknown event: it is the bot's partial transcript,
    the streaming half of the `.done` we did handle.
    """
    session = FakeSession()
    provider = new_provider(session=session)

    for piece in ("Hola, ", "gracias por ", "llamar"):
        await provider._on_event(json.dumps({
            "type": "response.output_audio_transcript.delta",
            "delta": piece,
        }))
    await provider._on_event(json.dumps({
        "type": "response.output_audio_transcript.done",
        "transcript": "Hola, gracias por llamar",
    }))

    assert [e.data["text"] for e in session.emitted("bot-tts-text")] == [
        "Hola, ", "gracias por ", "llamar"]
    assert [e.data["text"] for e in session.emitted("bot-output")] == [
        "Hola, gracias por llamar"]


async def test_the_two_halves_of_the_caller_turn_are_published():
    session = FakeSession()
    provider = new_provider(session=session)

    await provider._on_event(json.dumps({
        "type": "input_audio_buffer.speech_started"}))
    await provider._on_event(json.dumps({
        "type": "input_audio_buffer.speech_stopped"}))

    assert len(session.emitted("user-started-speaking")) == 1
    assert len(session.emitted("user-stopped-speaking")) == 1


async def test_a_provider_error_reaches_the_client():
    session = FakeSession()
    provider = new_provider(session=session)

    await provider._on_event(json.dumps({
        "type": "error", "error": {"code": "invalid_request"}}))

    published = session.emitted("error")
    assert len(published) == 1
    assert "invalid_request" in published[0].data["error"]


# ---------------------------------------------------------------------------
# The peephole: reaching an event the adapter does not translate
# ---------------------------------------------------------------------------
#
# There used to be a hardcoded list of "events I know about and ignore". That
# list was a contract with somebody else's release calendar: every event the
# API added left it out of date, and then either the log filled up or
# something that mattered went quiet. The peephole replaces it.


async def test_the_peephole_sees_translated_and_untranslated_events_alike():
    seen = []
    provider = new_provider(
        on_provider_event=lambda kind, payload: seen.append(kind))

    await provider._on_event(json.dumps({"type": "session.updated"}))
    await provider._on_event(json.dumps({
        "type": "response.output_audio_transcript.done", "transcript": "hello"}))
    await provider._on_event(json.dumps({"type": "an.event.from.2027"}))

    assert seen == ["session.updated",
                    "response.output_audio_transcript.done",
                    "an.event.from.2027"]


async def test_an_event_from_the_future_arrives_whole():
    """This is the point of the whole thing: the day the API ships something
    new, whoever integrates has it without waiting for a release of ours."""
    seen = []
    provider = new_provider(
        on_provider_event=lambda kind, payload: seen.append(payload))

    await provider._on_event(json.dumps({
        "type": "an.event.from.2027", "detail": {"nested": [1, 2, 3]}}))

    assert seen == [{"type": "an.event.from.2027",
                     "detail": {"nested": [1, 2, 3]}}]


async def test_a_broken_peephole_does_not_break_the_translation():
    session = FakeSession()

    def explodes(kind, payload):
        raise RuntimeError("the integrator's code has a bug")

    provider = new_provider(session=session, on_provider_event=explodes)

    await provider._on_event(json.dumps({
        "type": "response.output_audio_transcript.done", "transcript": "hello"}))

    assert [e.data["text"] for e in session.emitted("bot-output")] == ["hello"]


# ---------------------------------------------------------------------------
# The end of the turn, and the tail of the phrase
# ---------------------------------------------------------------------------
#
# Regression from a real call: long phrases were cut off at the end. The
# adapter closed the turn on `response.done`, an event the API does not send in
# this shape, so `end_turn()` never ran and the aligner's tail never went out:
# the last bytes of the phrase, the ones that did not fill a whole frame. Short
# phrases got away with it whenever their length happened to land on a multiple
# of the frame size.


async def test_the_end_of_the_audio_closes_the_turn():
    """`response.output_audio.done` is what says no more voice is coming."""
    session = FakeSession()
    provider = new_provider(session=session)

    # Two and a half frames: half a frame is left over in the aligner.
    await provider._on_event(json.dumps({
        "type": "response.output_audio.delta",
        "delta": base64.b64encode(b"\x01" * 400).decode(),
    }))
    assert "end_turn" not in session.speech.calls, "nothing closed it yet"

    await provider._on_event(json.dumps({
        "type": "response.output_audio.done"}))

    assert "end_turn" in session.speech.calls, (
        "the end of the audio has to flush what the aligner was holding")


async def test_the_tail_of_a_long_phrase_is_not_lost():
    """The bug as the caller hears it: the phrase gets cut off at the end."""
    session = FakeSession()
    provider = new_provider(session=session)

    await provider._on_event(json.dumps({
        "type": "response.output_audio.delta",
        "delta": base64.b64encode(b"\x01" * 250).decode(),
    }))
    await provider._on_event(json.dumps({
        "type": "response.output_audio.done"}))

    assert session.speech.calls.count("end_turn") == 1, "the turn closed"


async def test_closing_the_turn_twice_is_harmless():
    """`response.done` still closes it, because the tools travel inside it and
    a provider that sends both events must not break anything."""
    session = FakeSession()
    provider = new_provider(session=session)

    await provider._on_event(json.dumps({
        "type": "response.output_audio.delta",
        "delta": base64.b64encode(b"\x01" * 160).decode(),
    }))
    await provider._on_event(json.dumps({"type": "response.output_audio.done"}))
    await provider._on_event(json.dumps({"type": "response.done",
                                         "response": {"output": []}}))

    assert session.speech.calls.count("end_turn") == 2, (
        "both close it, and it tolerates that")


# ---------------------------------------------------------------------------
# A new response lifts the previous discard
# ---------------------------------------------------------------------------
#
# Regression from a real call: two turns in a row did not sound. The provider
# started the new response before the previous turn finished closing, so the
# discard from that one was still on and the new audio was dropped silently.
# The caller hears a turn that never comes and speaks again thinking the line
# dropped.


async def test_a_new_response_clears_the_discard():
    session = FakeSession()
    provider = new_provider(session=session)

    await provider._on_event(json.dumps({"type": "response.created"}))

    assert "resume" in session.speech.calls, (
        "a new response has to reopen playback")


async def test_the_audio_of_a_new_response_is_not_dropped():
    """The bug end to end: response arrives, audio plays."""
    session = FakeSession()
    provider = new_provider(session=session)

    await provider._on_event(json.dumps({"type": "response.created"}))
    await provider._on_event(json.dumps({
        "type": "response.output_audio.delta",
        "delta": base64.b64encode(b"\x01" * 160).decode(),
    }))

    assert session.speech.played, "the new turn's audio did reach the channel"


# ---------------------------------------------------------------------------
# The silence filler goes through the audio path, not through send_audio
# ---------------------------------------------------------------------------


def _provider_audio(provider) -> list[bytes]:
    return [base64.b64decode(m["audio"]) for m in provider.ws.json_sent()
            if m.get("type") == "input_audio_buffer.append"]


async def test_the_filler_goes_through_the_audio_path():
    """The filler frame is the channel's silence, base64 wrapped in an
    `input_audio_buffer.append` like the caller's audio (the loop itself is
    tested in `test_adapter_shared.py`)."""
    provider = new_provider(session=FakeSession("alaw"))
    filler = provider._build_filler()
    fake = FakeTime(filler, steps=2)
    fake.quiet_for(1.0)

    await filler.run()

    assert _provider_audio(provider) == [bytes([0xD5]) * 160]


async def test_the_filler_does_not_reset_the_gap_clock():
    """Only `send_audio` reports a frame: a filler frame that did would
    switch the filler off and on every 300 ms, and it must not count as a
    caller frame either."""
    provider = new_provider(session=FakeSession("alaw"))
    filler = provider._build_filler()
    fake = FakeTime(filler, steps=3)
    fake.quiet_for(1.0)
    quiet_since = filler._last_caller_frame

    await filler.run()

    assert len(_provider_audio(provider)) == 2, "two filler frames were expected"
    assert filler._last_caller_frame == quiet_since, "a filler frame reset the clock"
    assert provider._frames_in == 0, "filler frames counted as caller frames"

    await provider.send_audio(b"\xd5" * 160)
    assert filler._last_caller_frame == fake.now, "a real frame must reset it"
    assert provider._frames_in == 1


async def test_the_order_is_reopen_then_play():
    """Reopening after playing would drop the first block of the turn: the
    beginning of the phrase, which is the part that gets noticed."""
    session = FakeSession()
    provider = new_provider(session=session)

    await provider._on_event(json.dumps({"type": "response.created"}))
    await provider._on_event(json.dumps({
        "type": "response.output_audio.delta",
        "delta": base64.b64encode(b"\x01" * 160).decode(),
    }))

    assert session.speech.calls.index("resume") < session.speech.calls.index("play")
