"""Barge-in and recovery, driven by each provider's REAL events.

This is the test that separates "the discard logic is right" from "one provider
goes mute after every interruption". The unit tests of `SpeechState` cannot see
it: they call `interrupt()` and `end_turn()` by hand, in an order no provider
necessarily produces.

What is checked, for the three of them, is a single sequence: the bot speaks,
the person talks over it, the provider cancels and starts a new response, and
the bot HAS TO SOUND again. Whichever event lifts the discard is each adapter's
business; that it gets lifted is not negotiable.

It uses a real `SpeechState`, not the double from conftest: the discard lives in
there, so a double that only records calls would pass no matter what.

Reference: the guard around `end_turn(reset_discard=True)` in speech.py cannot
be hardened without breaking ElevenLabs, and this file is what proves it.
"""

from __future__ import annotations

import base64
import json

import pytest
from conftest import FakeMedia, FakeSocket

from galcymedia.speech import SpeechState

pytestmark = pytest.mark.asyncio


class RecordingSession:
    """Only what an adapter touches, with the audio that reaches the channel."""

    def __init__(self, audio_format: str = "alaw") -> None:
        self.media = FakeMedia(audio_format)
        self.audio: list[bytes] = []
        self.events: list = []
        self.dropped = 0
        self.speech = SpeechState(self)

    async def send_audio(self, chunk: bytes) -> bool:
        self.audio.append(chunk)
        return True

    async def flush(self) -> None:
        pass

    async def report_bot_audio_drained(self) -> None:
        pass

    def note_audio_dropped(self, frames: int) -> None:
        self.dropped += frames

    def emit(self, event) -> None:
        self.events.append(event)


def _audio(frames: int = 1) -> bytes:
    return b"\xd5" * (160 * frames)


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


async def _feed(provider, *raw_events) -> None:
    for raw in raw_events:
        await provider._on_event(json.dumps(raw))


async def test_deepgram_speaks_again_after_a_barge_in():
    """Its door is resume(), on a ConversationText whose role is not user."""
    from galcymedia.adapters.deepgram import DeepgramProvider

    session = RecordingSession("alaw")
    provider = DeepgramProvider(session, api_key="k",
                                settings=lambda media: {"type": "Settings"})
    provider.ws = FakeSocket()

    await provider.speech.play(_audio())
    await _feed(provider,
                {"type": "UserStartedSpeaking"},          # the person cuts in
                {"type": "AgentAudioDone"},               # cancelled response
                {"type": "ConversationText",              # a new one starts
                 "role": "assistant", "content": "hola"})

    before = len(session.audio)
    await provider.speech.play(_audio())      # Deepgram sends audio as binary
    assert len(session.audio) > before, "Deepgram stays mute after a barge-in"


async def test_openai_speaks_again_after_a_barge_in():
    """Its door is resume(), on response.created."""
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = RecordingSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    await provider.speech.play(_audio())
    before = len(session.audio)
    await _feed(provider,
                {"type": "input_audio_buffer.speech_started"},
                {"type": "response.output_audio.done"},
                {"type": "response.created"},
                {"type": "response.output_audio.delta",
                 "delta": _b64(_audio())})

    assert len(session.audio) > before, "OpenAI stays mute after a barge-in"


async def test_openai_reads_the_cancellation_off_the_event_not_off_the_order():
    """A cancelled response declares it, so nothing has to be inferred.

    `response.done` carries `status`, which is "cancelled" when the provider's
    VAD cut the response because the person started talking
    (`status_details.reason` spells it out as "turn_detected"). Lifting the
    discard there would let the tail of the interrupted phrase play.

    Documented, not deduced: "For a `cancelled` Response, one of
    `turn_detected` (the server VAD detected a new start of speech) or
    `client_cancelled`". That is why this can be read off the event while the
    ordering of `response.created` cannot: the API documents its order as
    lifecycle "though some events (like the delta events) may happen
    concurrently".
    """
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = RecordingSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    await provider.speech.play(_audio())
    await _feed(provider, {"type": "input_audio_buffer.speech_started"})

    # The close of the response the person just cancelled.
    await _feed(provider, {
        "type": "response.done",
        "response": {"status": "cancelled",
                     "status_details": {"reason": "turn_detected"},
                     "output": []},
    })

    before = len(session.audio)
    await provider.speech.play(_audio())      # stale audio, still in flight
    assert len(session.audio) == before, (
        "the tail of the cancelled response played after the barge-in")


async def test_elevenlabs_speaks_again_after_a_barge_in():
    """The one that matters: this adapter never calls resume().

    Its only way back to speaking is the `reset_discard` of end_turn, hanging
    off `user_transcript`. And in this provider the interruption and the close
    are the SAME fact, the person speaking, so a guard that blocks the close
    right after an interrupt() leaves it mute for a whole response.
    """
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    await provider.speech.play(_audio())
    before = len(session.audio)
    await _feed(provider,
                {"type": "interruption"},
                {"type": "user_transcript",
                 "user_transcription_event": {"user_transcript": "espera"}},
                # The provider's text for the NEW response. Today no branch
                # acts on it, and it goes in the sequence anyway: this test has
                # to describe the conversation the provider really sends, not
                # the shortest one that makes the assert pass. The day someone
                # hangs a resume() off here, the test must not stand in the way.
                {"type": "agent_response",
                 "agent_response_event": {"agent_response": "claro, dime"}},
                {"type": "audio",
                 "audio_event": {"audio_base_64": _b64(_audio())}})

    assert len(session.audio) > before, (
        "ElevenLabs stays mute after a barge-in: it has no resume(), so the "
        "reset_discard of end_turn is its only door back to speaking")


# ---------------------------------------------------------------------------
# OpenAI: telling the model how much of its phrase was actually heard
# ---------------------------------------------------------------------------


class ClockedSession(RecordingSession):
    """A session whose channel format is complete, so ms can be derived.

    `ptime` and the frame size are what turn bytes into milliseconds: alaw is 8
    bytes per ms and slin16 is 32, so the number cannot come from a constant.
    """

    def __init__(self, audio_format: str = "alaw") -> None:
        super().__init__(audio_format)
        self.paused = False

    async def pause(self) -> None:
        self.paused = True
        self.speech.note_playback_paused()


def _truncates(socket) -> list[dict]:
    sent = [json.loads(m) for m in socket.sent]
    return [m for m in sent if m.get("type") == "conversation.item.truncate"]


async def test_openai_does_not_truncate_a_reply_heard_whole():
    """A normal turn: the bot finishes, Asterisk drains the queue, the caller
    answers. No truncate may go out. The item id survives the end of the
    response and `played_ms_at_most` keeps answering a number until the next
    `play`, so without the guard the adapter truncates a sentence the caller
    heard entire; measured, the server accepts it (0 errors) and deletes its
    transcript. While the queue is still draining it IS a barge-in, and the
    truncate must still go out (the control below)."""
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    await _feed(provider, {"type": "response.output_audio.delta",
                           "item_id": "msg_1", "content_index": 0,
                           "delta": _b64(b"\x01" * 1600)},
                          {"type": "response.output_audio.done"},
                          {"type": "response.done",
                           "response": {"status": "completed", "output": []}})
    session.speech.on_queue_drained()          # Asterisk: the queue is empty
    assert session.speech.played_ms_at_most is None, (
        "once the queue drained the bounds are gone; the adapter's guard on "
        "bot_audio_playing is the second net, tested by the control below")

    await _feed(provider, {"type": "input_audio_buffer.speech_started"})

    assert _truncates(provider.ws) == [], (
        "truncated a reply the caller heard whole")

    # Control: the same turn with the queue NOT yet drained is a barge-in.
    await _feed(provider, {"type": "response.created"},
                          {"type": "response.output_audio.delta",
                           "item_id": "msg_2", "content_index": 0,
                           "delta": _b64(b"\x01" * 1600)},
                          {"type": "response.output_audio.done"},
                          {"type": "input_audio_buffer.speech_started"})

    assert [t["item_id"] for t in _truncates(provider.ws)] == ["msg_2"]


async def test_openai_truncates_what_the_person_did_not_hear():
    """The defect this closes is not cut audio: it is the model believing it
    said a whole sentence when only two seconds played, and referring to that
    data two turns later.

    The three required fields come from two different places, and neither is
    guessed: `item_id` and `content_index` are DECLARED by
    `response.output_audio.delta`, and `audio_end_ms` comes from the speaking
    turn, which is the only one that knows the channel format.
    """
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    # The provider queues ten seconds of speech in one go, as engines do.
    await _feed(provider, {"type": "response.output_audio.delta",
                           "item_id": "msg_77", "content_index": 0,
                           "delta": _b64(b"\x01" * 80000)})

    # The person talks over it.
    await _feed(provider, {"type": "input_audio_buffer.speech_started"})

    truncates = _truncates(provider.ws)
    assert len(truncates) == 1, "the interrupted phrase was not truncated"

    sent = truncates[0]
    assert sent["item_id"] == "msg_77", "it read the item off the event"
    assert sent["content_index"] == 0
    assert sent["audio_end_ms"] <= 10000, (
        "past what was sent: the server rejects it and publishes an error")
    assert sent["audio_end_ms"] < 100, (
        "ten seconds were SENT but almost none were HEARD: Asterisk plays in "
        "real time, and billing the sent bytes is what gets rejected")


async def test_openai_cuts_the_audio_before_sending_the_truncate():
    """Cutting the audio comes FIRST. Nothing over the network goes before it.

    The truncate is a `ws.send()` towards the provider, so putting it ahead of
    the flush would make the caller's audio wait on a network round trip: a send
    that stalls 200 ms is 200 ms of bot talking over the person. That trades
    precision in the model's history for latency in what the caller hears, and
    they do not weigh the same. The mismatch is noticed by the model a turn
    later; the bot talking over you is noticed right now.

    It costs no precision: `interrupt()` does not touch the turn bounds
    (measured), so the value read before the cut is the same one that would be
    read after. Hence the three steps: measure, cut, send.

    This pins the ordering. The invariant that the value never exceeds what
    played lives in `test_speech.py`.
    """
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    order: list[str] = []

    original_flush = session.flush

    async def watched_flush() -> None:
        order.append("flush")
        await original_flush()

    session.flush = watched_flush

    original_send = provider.ws.send

    async def watched_send(raw) -> None:
        if json.loads(raw).get("type") == "conversation.item.truncate":
            order.append("truncate")
        await original_send(raw)

    provider.ws.send = watched_send

    await _feed(provider, {"type": "response.output_audio.delta",
                           "item_id": "msg_88", "content_index": 0,
                           "delta": _b64(_audio(3))})
    await _feed(provider, {"type": "input_audio_buffer.speech_started"})

    assert order == ["flush", "truncate"], (
        f"got {order}: the channel flush has to happen BEFORE the truncate is "
        "sent, or the caller keeps hearing the bot while a network send drains")


async def test_openai_does_not_truncate_what_it_cannot_measure():
    """With the clock broken by a `pause()` the library does not know how much
    was heard, and saying so is the answer. Sending an invented number would not
    fix the history AND would be rejected, publishing an error to the client:
    our silent defect turned into the integrator's false alarm.
    """
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    await _feed(provider, {"type": "response.output_audio.delta",
                           "item_id": "msg_99", "content_index": 0,
                           "delta": _b64(_audio(3))})
    await session.pause()               # the clock stops being trustworthy

    await _feed(provider, {"type": "input_audio_buffer.speech_started"})

    assert _truncates(provider.ws) == [], (
        "it truncated with a contaminated clock: better not to truncate than "
        "to send a value the server rejects")


async def test_openai_does_not_truncate_when_the_bot_never_spoke():
    """A barge-in with no bot audio has nothing to truncate, and the adapter
    must not invent an item id."""
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    await _feed(provider, {"type": "input_audio_buffer.speech_started"})

    assert _truncates(provider.ws) == []


async def test_openai_truncates_each_item_once():
    """A truncated item is not truncated again: the next delta brings its own."""
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    await _feed(provider, {"type": "response.output_audio.delta",
                           "item_id": "msg_1", "content_index": 0,
                           "delta": _b64(_audio(3))})
    await _feed(provider,
                {"type": "input_audio_buffer.speech_started"},
                {"type": "input_audio_buffer.speech_started"})

    assert len(_truncates(provider.ws)) == 1


async def test_a_slow_provider_socket_does_not_delay_cutting_the_audio():
    """The reason the ordering exists, measured instead of argued.

    The truncate travels to the provider over the network. If it went ahead of
    the cut, a stalled send would hold the caller's audio hostage: the bot keeps
    playing over the person for exactly as long as the socket takes. Here the
    socket takes a visible amount of time, and the flush still has to land
    before that delay is paid.
    """
    import asyncio

    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    flushed_at: list[float] = []
    loop = asyncio.get_running_loop()

    async def slow_send(raw) -> None:
        # A backpressured socket towards the provider.
        await asyncio.sleep(0.2)
        provider.ws.sent.append(raw)

    async def timed_flush() -> None:
        flushed_at.append(loop.time())

    session.flush = timed_flush

    await _feed(provider, {"type": "response.output_audio.delta",
                           "item_id": "msg_slow", "content_index": 0,
                           "delta": _b64(_audio(3))})

    provider.ws.send = slow_send
    started = loop.time()
    await _feed(provider, {"type": "input_audio_buffer.speech_started"})

    assert flushed_at, "the channel was never flushed"
    delay = flushed_at[0] - started
    assert delay < 0.1, (
        f"the audio cut waited {delay:.3f}s for the provider socket: the "
        "person keeps hearing the bot while a network send drains")


# ---------------------------------------------------------------------------
# ElevenLabs: correlating by event_id instead of trusting the order
# ---------------------------------------------------------------------------


async def test_elevenlabs_drops_the_audio_of_an_interrupted_response():
    """The tail of the interrupted phrase is dropped by NUMBER, not by order.

    This adapter is the one that never calls resume(), so it leans on the
    `reset_discard` of end_turn, which lifts the discard without knowing whether
    what comes next belongs to the new response or the old one. The provider
    answers that question itself: its `interruption_event` carries an `event_id`
    and so does every `audio_event`, and its own SDK discards audio whose id
    does not pass the last interruption.

    Correlating is the only solid option here: this provider does not document
    the order of its events, so anything built on "this arrives before that"
    is built on sand.
    """
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": 10,
                                           "audio_base_64": _b64(_audio())}})
    assert session.audio, "the bot was speaking"

    # The person cuts in at event 12.
    await _feed(provider, {"type": "interruption",
                           "interruption_event": {"event_id": 12}})

    # And the turn closes, which is what LIFTS the discard in this adapter: it
    # has no resume(), so `reset_discard` is its only door back to speaking.
    # From here on `_discarding` is down, so the discard inside SpeechState is
    # no longer protecting anything. Without this step the test would pass with
    # the correlation removed, because the flag alone would drop the audio.
    await _feed(provider, {"type": "user_transcript",
                           "user_transcription_event": {"user_transcript": "espera"}})

    before = len(session.audio)

    # Audio of the CANCELLED response, still in flight behind the interruption.
    # The turn is open again, so ONLY the event_id can tell it apart.
    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": 11,
                                           "audio_base_64": _b64(_audio())}})

    assert len(session.audio) == before, (
        "the tail of the interrupted phrase played: audio with an event_id "
        "below the interruption belongs to a response nobody wants to hear")


async def test_elevenlabs_still_speaks_the_response_after_the_interruption():
    """The other half, and the one that would go unnoticed: dropping too much
    leaves this adapter mute, which is its historical failure mode.

    Audio whose id is PAST the interruption is the new response and has to
    sound, even though it arrives through the same branch.
    """
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    await _feed(provider,
                {"type": "audio",
                 "audio_event": {"event_id": 10,
                                 "audio_base_64": _b64(_audio())}},
                {"type": "interruption",
                 "interruption_event": {"event_id": 12}},
                {"type": "user_transcript",
                 "user_transcription_event": {"user_transcript": "espera"}})

    before = len(session.audio)
    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": 13,
                                           "audio_base_64": _b64(_audio())}})

    assert len(session.audio) > before, (
        "ElevenLabs went mute: audio past the interruption is the NEW "
        "response and has to be heard")


async def test_elevenlabs_plays_audio_that_carries_no_event_id():
    """A missing or unreadable id lets the audio through.

    Going mute because the provider omitted a field would be worse than letting
    one extra frame play, and this adapter already has a history of going
    silent. The id arrives over the network, so anything can come through it:
    a bool is an int in Python and would sneak in, a string would blow up the
    comparison.
    """
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    await _feed(provider, {"type": "interruption",
                           "interruption_event": {"event_id": 12}})

    before = len(session.audio)
    for junk in ({"audio_base_64": _b64(_audio())},              # no id at all
                 {"event_id": "13", "audio_base_64": _b64(_audio())},
                 {"event_id": True, "audio_base_64": _b64(_audio())},
                 {"event_id": None, "audio_base_64": _b64(_audio())}):
        await _feed(provider, {"type": "audio", "audio_event": junk})

    assert len(session.audio) == before + 4, (
        "an unreadable event_id left the bot mute instead of letting the "
        "audio through")


async def test_elevenlabs_plays_the_first_chunk_of_the_call():
    """The greeting cannot be eaten by the interruption bookkeeping.

    The trap is a sentinel colliding with a real value: with `0` as the initial
    "last interrupted id" and a `<=` comparison, a provider that numbers its
    events from zero loses the first block of the greeting, because `0 <= 0`.
    The symptom would be a call that starts clipped on the first syllable,
    invisible to every other test and expensive to diagnose in production.

    The provider documents its ids as "monotonically increasing" but does NOT
    say where they start, and it could not be confirmed against its schema or
    its SDKs. So nothing is discarded until there has been a real interruption,
    which is the only thing that can be asserted without knowing the origin.
    """
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    # The very first block of the call, numbered from zero.
    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": 0,
                                           "audio_base_64": _b64(_audio())}})

    assert session.audio, (
        "the first chunk of the greeting was discarded: the call starts "
        "clipped on the first syllable")


async def test_elevenlabs_discards_a_zero_numbered_chunk_after_an_interruption():
    """The other side of the same coin: once there IS an interruption, zero is
    a number like any other and gets discarded if it does not pass it."""
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    await _feed(provider,
                {"type": "audio",
                 "audio_event": {"event_id": 0,
                                 "audio_base_64": _b64(_audio())}},
                {"type": "interruption",
                 "interruption_event": {"event_id": 0}},
                {"type": "user_transcript",
                 "user_transcription_event": {"user_transcript": "para"}})

    before = len(session.audio)
    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": 0,
                                           "audio_base_64": _b64(_audio())}})

    assert len(session.audio) == before, (
        "audio from the interrupted response played: after a real "
        "interruption the number does have to be compared")


async def test_openai_ignores_an_item_id_that_is_not_an_id():
    """The item id travels over the network, so anything can arrive in it.

    Converting with `str()` would turn a dict into the literal "{'a': 1}" and
    send it as an identifier: the server rejects it and the rejection is
    published to the client as an `error`. Same trap as elsewhere, a value that
    is not what it claims. What cannot be understood is ignored, so no truncate
    goes out, and the audio keeps playing either way.
    """
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    for junk in ("", 0, None, False, {"a": 1}, ["x"]):
        session = ClockedSession("alaw")
        provider = OpenAIRealtimeProvider(session, api_key="k")
        provider.ws = FakeSocket()

        await _feed(provider, {"type": "response.output_audio.delta",
                               "item_id": junk, "content_index": 0,
                               "delta": _b64(_audio())})
        await _feed(provider, {"type": "input_audio_buffer.speech_started"})

        assert _truncates(provider.ws) == [], (
            f"item_id={junk!r} produced a truncate with a bogus identifier")
        assert session.audio, (
            f"item_id={junk!r} left the bot mute: an unreadable id must not "
            "cost the caller any audio")


async def test_openai_truncates_at_zero_when_almost_nothing_was_heard():
    """Zero is a legitimate value, not a missing one.

    The person cutting in on the very first syllable did hear ~0 ms, and that
    truncate is the one that matters most: the model generated a whole sentence
    nobody heard. The guard is `is None` and not a falsy check precisely so this
    case is not swallowed.
    """
    from galcymedia.adapters.openai import OpenAIRealtimeProvider

    session = ClockedSession("alaw")
    provider = OpenAIRealtimeProvider(session, api_key="k")
    provider.ws = FakeSocket()

    provider._audio_item_id = "msg_zero"
    await provider._truncate_played_audio(0)

    truncates = _truncates(provider.ws)
    assert len(truncates) == 1, "a legitimate zero was treated as 'unknown'"
    assert truncates[0]["audio_end_ms"] == 0


# ---------------------------------------------------------------------------
# Deepgram: the diagnostic it declares and nobody was reading
# ---------------------------------------------------------------------------


async def test_deepgram_surfaces_a_warning_instead_of_burying_it(caplog):
    """The provider separates two diagnostics and they are not the same.

    Documented: "An Error indicates a fatal issue that typically ends the
    session, requiring the application to reconnect. A Warning signals a
    non-fatal issue where the session can continue". The warning is the clue
    that explains why a call went odd, and it was falling into the unhandled
    bucket: DEBUG level, once per type, so the second warning of a call never
    showed up anywhere.
    """
    import logging

    from galcymedia.adapters.deepgram import DeepgramProvider

    session = RecordingSession("alaw")
    provider = DeepgramProvider(session, api_key="k",
                                settings=lambda media: {"type": "Settings"})
    provider.ws = FakeSocket()

    with caplog.at_level(logging.WARNING):
        await _feed(provider, {"type": "Warning",
                               "description": "el modelo tardo demasiado",
                               "code": "SLOW_MODEL"})

    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "el modelo tardo demasiado" in logged, (
        "the warning was buried: it is the clue that explains a bad call")
    assert "SLOW_MODEL" in logged, "the code has to travel with it"


async def test_deepgram_does_not_die_on_a_malformed_warning():
    """The fields come over the network, so they can be missing."""
    from galcymedia.adapters.deepgram import DeepgramProvider

    session = RecordingSession("alaw")
    provider = DeepgramProvider(session, api_key="k",
                                settings=lambda media: {"type": "Settings"})
    provider.ws = FakeSocket()

    await _feed(provider, {"type": "Warning"})          # no description, no code


async def test_elevenlabs_correlates_when_the_id_arrives_as_a_string():
    """The provider sends the id both ways, and rejecting the string would
    silently switch the whole correlation off.

    Its types declare `event_id` as an int, but its own SDK tests feed
    `{"event_id": "789"}` quoted, which is why its code converts with
    `int(event["event_id"])` instead of comparing directly. Against a
    deployment that numbers like that, a strict int check would make this
    adapter's discard inert: no exception, no log, and the tail of the
    interrupted phrase playing again.
    """
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    await _feed(provider,
                {"type": "audio",
                 "audio_event": {"event_id": "10",
                                 "audio_base_64": _b64(_audio())}},
                {"type": "interruption",
                 "interruption_event": {"event_id": "12"}},
                {"type": "user_transcript",
                 "user_transcription_event": {"user_transcript": "para"}})

    before = len(session.audio)
    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": "11",
                                           "audio_base_64": _b64(_audio())}})
    assert len(session.audio) == before, (
        "string ids left the correlation inert: the tail of the interrupted "
        "phrase played")

    # And the new response, numbered past the interruption, still sounds.
    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": "13",
                                           "audio_base_64": _b64(_audio())}})
    assert len(session.audio) > before, "it went mute on the new response"


async def test_elevenlabs_does_not_walk_the_interruption_limit_backwards():
    """The limit only moves forward, even if two interruptions arrive out of
    order.

    The provider documents its ids as increasing, so this should not happen;
    the `max()` is defence in depth, and it costs one line. What it buys: if a
    later interruption carried a SMALLER number, the limit would step back and
    audio from the response cut at the higher number would start playing again,
    which is the tail of a phrase the person already interrupted.
    """
    from galcymedia.adapters.elevenlabs import ElevenLabsProvider

    session = RecordingSession("ulaw")
    provider = ElevenLabsProvider(session, agent_id="ag-1")
    provider.ws = FakeSocket()

    await _feed(provider,
                {"type": "interruption", "interruption_event": {"event_id": 20}},
                {"type": "interruption", "interruption_event": {"event_id": 12}},
                {"type": "user_transcript",
                 "user_transcription_event": {"user_transcript": "para"}})

    before = len(session.audio)
    await _feed(provider, {"type": "audio",
                           "audio_event": {"event_id": 15,
                                           "audio_base_64": _b64(_audio())}})

    assert len(session.audio) == before, (
        "the limit walked back to 12, so audio from the response cut at 20 "
        "played again")


async def test_deepgram_does_not_lift_the_discard_on_the_callers_transcript():
    """Only the BOT's text reopens playback, never the person's.

    In this provider the same event carries both sides of the conversation, and
    the transcript of the caller arrives right after their own barge-in. Lifting
    the discard there would undo the interruption the person just made, and the
    tail of the cut phrase would play. It is the same defect that in OpenAI is
    read off `response.status`; here the signal is the `role`.
    """
    from galcymedia.adapters.deepgram import DeepgramProvider

    session = RecordingSession("alaw")
    provider = DeepgramProvider(session, api_key="k",
                                settings=lambda media: {"type": "Settings"})
    provider.ws = FakeSocket()

    await provider.speech.play(_audio())
    await _feed(provider, {"type": "UserStartedSpeaking"})
    assert provider.speech._discarding, "the barge-in put the discard up"

    # What the person said, transcribed after their own interruption.
    await _feed(provider, {"type": "ConversationText",
                           "role": "user", "content": "espera"})

    assert provider.speech._discarding, (
        "the caller's own transcript lifted the discard their interruption "
        "had just put up")

    before = len(session.audio)
    await provider.speech.play(_audio())      # tail of the cut response
    assert len(session.audio) == before, "the interrupted phrase played on"
