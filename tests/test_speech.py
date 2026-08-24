"""
Speaking-turn tests: the barge-in state machine.

It is the piece that three different adapters use without changing a line, so
its invariants are tested here and not in each example.
"""

from __future__ import annotations

import asyncio

from galcymedia import SpeechState


class FakeSession:
    def __init__(self):
        self.audio: list[bytes] = []
        self.events: list[str] = []
        self.published: list = []
        self.flushed = 0
        self.drain_reports = 0
        self.dropped = 0

    def emit(self, event):
        self.events.append(event.type.value)
        self.published.append(event)

    def emitted(self, event_type: str) -> list:
        return [e for e in self.published if e.type.value == event_type]

    async def send_audio(self, chunk) -> bool:
        # Devuelve True como el real: el frame salio. Un doble que devolviera
        # None diria "se descarto", y play() cortaria el bloque entero.
        self.audio.append(chunk)
        return True

    async def flush(self):
        self.flushed += 1

    async def report_bot_audio_drained(self):
        self.drain_reports += 1

    def note_audio_dropped(self, frames: int) -> None:
        self.dropped += frames


async def test_plays_aligned_and_announces_the_bot_is_speaking():
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 6)

    assert speech.speaking
    assert session.events == ["bot-started-speaking"]
    assert session.audio == [b"\x01" * 4], "only the complete frame; the rest waits"


async def test_interrupt_only_acts_while_the_bot_is_sounding():
    """A 'started speaking' with the bot truly quiet (nothing sounding) cannot
    flush anything: it would take away the just-generated response."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.interrupt()
    assert session.flushed == 0

    await speech.play(b"\x01" * 4)
    await speech.interrupt()
    assert session.flushed == 1
    assert not speech.speaking


async def test_interrupt_flushes_even_if_the_provider_already_finished():
    """THE late-barge-in bug: the provider generates 10 s in 1-2 s and says
    end_turn (speaking=False), but the audio keeps sounding in Asterisk. A
    barge-in in THAT window has to flush anyway; looking only at `speaking` let
    it slip and the bot would not go quiet."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn()          # the provider finished generating
    assert not speech.speaking, "the provider no longer generates"

    # ...but the audio is still queued in Asterisk: interrupting MUST flush.
    await speech.interrupt()
    assert session.flushed == 1, "the late barge-in flushes what is sounding"


async def test_with_the_queue_already_empty_a_barge_in_does_not_flush():
    """When Asterisk confirms the queue is empty (on_queue_drained), the bot
    truly stopped sounding: a later barge-in cannot flush a new response. It is
    the guard that separates 'quiet in generation' from 'truly quiet'."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn()
    speech.on_queue_drained()        # Asterisk: the queue became empty

    await speech.interrupt()
    assert session.flushed == 0, "empty queue = nothing to flush"


async def test_end_turn_requests_the_queue_drained_notification():
    """end_turn requests the REPORT_QUEUE_DRAINED to learn when the bot stopped
    sounding; without that, _audio_pending would never turn off."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn()
    assert session.drain_reports == 1


async def test_after_the_interrupt_late_audio_is_discarded():
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.interrupt()

    await speech.play(b"\x02" * 4)
    assert session.audio == [b"\x01" * 4], "the old phrase's audio is dropped"

    # A new response from the provider reopens playback.
    speech.resume()
    await speech.play(b"\x03" * 4)
    assert session.audio[-1] == b"\x03" * 4


async def test_the_end_of_turn_sends_the_rest_with_silence():
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 5)
    await speech.end_turn()

    assert session.audio[-1] == b"\x01" + b"\xd5" * 3, \
        "the rest goes out padded with the codec silence"
    assert session.events[-1] == "bot-stopped-speaking"
    assert not speech.speaking


class RefusingSession(FakeSession):
    """A session under sustained XOFF: every frame is discarded."""

    async def send_audio(self, chunk) -> bool:
        self.refused = getattr(self, "refused", 0) + 1
        return False


async def test_a_refused_frame_stops_the_whole_block():
    """The XOFF budget is paid per frame, so a block must not push them all.

    A 25-frame block would wait the limit 25 times over, and while it grinds
    the adapter is not reading its socket, so it cannot even learn about the
    barge-in that would abort it. One refusal ends the block.
    """
    session = RefusingSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 100)              # 25 frames

    assert session.refused == 1, (
        f"kept pushing after a refusal: {session.refused} frames attempted")


async def test_a_discarded_frame_is_not_counted_as_queued():
    """Nothing was queued in Asterisk, so nothing will drain.

    Marking it as pending leaves the flag stuck waiting for a QUEUE_DRAINED
    that will never come, which kills caller_quiet_for for the rest of the
    call and makes finish() burn its whole budget.
    """
    session = RefusingSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    # 6 bytes on a 4-byte frame: one frame plus a remainder, so end_turn() has
    # a tail to send and both paths that set the flag get exercised.
    await speech.play(b"\x01" * 6)

    # `speaking` stays True and that is right: the provider IS generating. What
    # must not be set is the queued-audio flag, because nothing was queued.
    assert speech.speaking
    await speech.end_turn()
    assert session.drain_reports == 0, (
        "asked Asterisk for a queue-empty notice over an empty queue")
    assert not speech.bot_audio_playing, (
        "a discarded frame left the turn marked as still playing")


async def test_reset_discard_lifts_the_discard_even_right_after_a_barge_in():
    """Documents a trade-off that looks like a bug and is not, so nobody
    "fixes" it without reading this.

    The close of a response the caller just cancelled DOES lift the discard, so
    the tail still in flight can play. Blocking it looks obviously right and
    breaks a provider: in ElevenLabs the interruption and the close are the SAME
    fact, the person speaking, and nothing there calls resume(). That
    `reset_discard` is its only way back to speaking, so closing it costs a whole
    response, which is worse than the tail.

    The obvious next move, making the other two adapters pass
    `reset_discard=False` since they already have resume(), was measured against
    the providers' own docs and does NOT hold: none of the three guarantees in
    writing that the "response started" event precedes the first audio of that
    response. OpenAI documents its order as lifecycle "though some events (like
    the delta events) may happen concurrently"; Deepgram documents no order at
    all, and its own telephony bridge gates playback on the first binary chunk
    rather than on any event. Trading a two-frame tail for a mute bot that
    depends on an order nobody promised is a worse deal.

    What WOULD be solid is correlation instead of ordering: ElevenLabs carries
    `event_id` and `is_final` in its audio event, which this adapter ignores
    today. That is adapter work, and it is written down, not done.
    """
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.interrupt()
    await speech.end_turn(reset_discard=True)

    assert not speech._discarding, (
        "the only door a provider without resume() has was closed")


async def test_the_queue_notice_is_about_the_queue_not_about_a_turn():
    """Documents why there is no per-turn bookkeeping here, measured in the
    channel's own source.

    `report_queue_drained` is a single int in chan_websocket.c: each request
    sets it to 1, and it fires when the queue runs out of frames
    (chan_websocket.c:499). So two requests produce ONE notice, and that notice
    means "the queue is empty NOW".

    Which is why matching notices to turns is the wrong model: it leaves
    requests orphaned waiting for a second notice the channel will never send,
    and with them the pending flag stuck for the rest of the call. If the queue
    is empty now, no turn has audio left.
    """
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn(reset_discard=True)     # asks for a notice
    await speech.play(b"\x02" * 4)
    await speech.end_turn(reset_discard=True)     # asks again, still one notice

    speech.on_queue_drained()

    assert not speech.bot_audio_playing, (
        "one notice has to settle every pending request, or the flag sticks")


async def test_the_goodbye_does_not_reopen():
    """goodbye never lifts: neither resume() nor end_turn() can give the voice
    back to a bot that already said goodbye."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.begin_goodbye()
    assert session.flushed == 1

    speech.resume()
    await speech.play(b"\x02" * 4)
    assert session.audio == [b"\x01" * 4], "in goodbye nothing new sounds"

    await speech.end_turn(reset_discard=True)
    await speech.play(b"\x03" * 4)
    assert session.audio == [b"\x01" * 4]


async def test_mark_goodbye_blocks_without_cutting_the_audio():
    """For the abandonment close: the goodbye has to sound in full."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    speech.mark_goodbye()

    assert session.flushed == 0, "it does not cut what is sounding"
    assert speech.saying_goodbye
    await speech.play(b"\x02" * 4)
    assert session.audio[-1] == b"\x02" * 4, "the goodbye in progress keeps sounding"


class SlowSession(FakeSession):
    """A Session whose send_audio yields control on each frame.

    Reproduces the real system: frame delivery runs in a separate task, so
    `send_audio` yields control and in that gap a barge-in can land and set the
    discard.
    """

    async def send_audio(self, chunk) -> bool:
        await asyncio.sleep(0)
        self.audio.append(chunk)
        return True


async def test_the_barge_in_cuts_the_audio_in_flight():
    """A barge-in midway through a play() of many frames does NOT send the
    remaining ones.

    `play` re-checks `_discarding` BEFORE each frame, not just once at the top.
    Each `send_audio` yields control, and in that gap the `interrupt()` lands.
    If the state were checked only on entry, a block of TTS would keep pushing
    frames AFTER the caller already interrupted: 120 to 500 ms of the old
    phrase stepping on the conversation. It is cut on the next frame.
    """
    session = SlowSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    # 50 frames of 4 bytes: far more than will sound after the cut.
    playback = asyncio.ensure_future(speech.play(b"\x01" * 200))
    await asyncio.sleep(0)                       # let the play start
    await speech.interrupt()                     # barge-in mid-flight
    await playback

    assert len(session.audio) < 50, "not all remaining frames were sent"
    assert not speech.speaking


async def test_two_consecutive_end_turns_emit_a_single_bot_stopped():
    """The second end_turn() with the bot already quiet cannot emit another
    bot-stopped-speaking.

    Emitting it unconditionally unbalances the started/stopped count the client
    keeps of the speaking turn: an extra stopped without its corresponding
    started. It is only emitted on the real transition from speaking to quiet.
    """
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn()                      # real transition
    await speech.end_turn()                      # the bot was already quiet

    stops = [e for e in session.events if e == "bot-stopped-speaking"]
    assert len(stops) == 1, "a single bot-stopped-speaking, not two"


async def test_a_resume_during_an_interrupt_flush_does_not_win():
    """A resume() that lands while an interrupt() is still flushing does NOT
    lift the discard: the interruption, which is the caller's intent, wins over
    a response the provider started without knowing about the barge-in.

    Without the `_interrupting` guard, that resume() would let through the
    audio the interruption wanted to drop.
    """
    class SlowFlush(FakeSession):
        async def flush(self):
            self.flushed += 1
            await asyncio.sleep(0.02)            # the interrupt is still in flight

    session = SlowFlush()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)               # the bot is sounding

    interruption = asyncio.ensure_future(speech.interrupt())
    await asyncio.sleep(0.005)                   # the flush has not finished yet
    speech.resume()                              # lands mid-interruption

    assert speech._discarding, "the interruption wins: the discard stays up"
    await interruption
    assert speech._discarding, "and it stays after the flush finishes"


class _SessionWithMedia(FakeSession):
    """A FakeSession that exposes `media`, like the real Session, to test the
    automatic derivation of frame_size and silence_byte."""

    def __init__(self, media):
        super().__init__()
        self.media = media


async def test_derives_frame_size_and_silence_from_media_if_not_passed():
    """Without arguments, SpeechState takes the frame_size and the silence byte
    from the format the channel declared in the MEDIA_START. It is the right
    default: the channel is the source of truth, and guessing them (setting
    160/alaw when the channel sends slin16) misaligns the audio into silence."""
    from galcymedia.protocol import parse_media_start

    # slin16: frame 640, silence 0x00 (linear PCM).
    media = parse_media_start({"format": "slin16", "optimal_frame_size": 640})
    speech = SpeechState(_SessionWithMedia(media))

    assert speech._aligner._frame_size == 640
    assert speech._silence == 0x00


async def test_explicit_arguments_win_over_the_derivation():
    """An adapter that passes frame_size/silence_byte by hand still rules: the
    derivation is only the default when they are not specified."""
    from galcymedia.protocol import parse_media_start

    media = parse_media_start({"format": "slin16", "optimal_frame_size": 640})
    speech = SpeechState(_SessionWithMedia(media), frame_size=160, silence_byte=0xD5)

    assert speech._aligner._frame_size == 160
    assert speech._silence == 0xD5


def test_without_media_and_without_explicit_values_raises():
    """Before MEDIA_START the format is not guessed: freezing 160/0xD5 with a
    channel that turns out to be slin16 is the silent misalignment we avoid."""
    import pytest

    class SessionWithoutMedia:
        media = None

    with pytest.raises(ValueError):
        SpeechState(SessionWithoutMedia())

    # With both explicit values it is allowed: whoever transcodes knows what
    # they are doing.
    state = SpeechState(SessionWithoutMedia(), 160, 0xD5)
    assert state is not None


async def test_bot_audio_playing_stays_after_end_turn_until_drained():
    """The signal for 'the bot is still heard': speaking turns off at end_turn,
    but bot_audio_playing stays until Asterisk drains the queue. It is what an
    abandonment notice must look at so it does not step on the bot's voice."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    assert speech.speaking and speech.bot_audio_playing

    await speech.end_turn()
    assert not speech.speaking, "the provider finished generating"
    assert speech.bot_audio_playing, "but the audio keeps sounding in Asterisk"

    speech.on_queue_drained()
    assert not speech.bot_audio_playing, "now yes, the queue became empty"


async def test_the_caller_silence_does_not_run_while_the_bot_speaks():
    """The clock starts when the bot stops sounding, not before.

    It is the error each adapter made on its own: counting from the person's
    last turn folds in the time the bot spoke, and a long response looks like
    abandonment.
    """
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    # No one has spoken yet: there is no silence to measure.
    assert speech.caller_quiet_for is None

    await speech.play(b"\x01" * 4)
    assert speech.caller_quiet_for is None, "it is the bot's turn"

    await speech.end_turn()
    assert speech.caller_quiet_for is None, "its audio is still sounding"

    # Only when Asterisk confirms the queue is empty does it start counting.
    speech.on_queue_drained()
    assert speech.caller_quiet_for is not None
    assert speech.caller_quiet_for >= 0


async def test_the_silence_clock_starts_without_a_queue_notice():
    """Some providers only close the bot's turn when the PERSON speaks.

    ElevenLabs does: its end_turn hangs off the caller's transcript. So in the
    exact case this meter exists for, someone who goes quiet, no notice is ever
    requested and the clock would never start. It must not depend on it.
    """
    session = RefusingSession()          # nothing reaches the channel's queue
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn()              # closes with no audio left queued

    assert session.drain_reports == 0, "there was nothing to wait for"
    assert speech.caller_quiet_for is not None, (
        "the abandonment meter never starts without a queue-empty notice")


async def test_the_silence_resets_when_the_person_speaks():
    """Whoever reports that the person spoke is the adapter, not the channel."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn()
    speech.on_queue_drained()
    assert speech.caller_quiet_for is not None

    speech.note_caller_activity()
    assert speech.caller_quiet_for is None


async def test_the_silence_stops_counting_if_the_bot_speaks_again():
    """While the bot sounds the turn is its own, even if there was silence
    before."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.end_turn()
    speech.on_queue_drained()
    assert speech.caller_quiet_for is not None

    await speech.play(b"\x02" * 4)
    assert speech.caller_quiet_for is None


# ---------------------------------------------------------------------------
# How long the bot took to answer
# ---------------------------------------------------------------------------
#
# This is the number that decides whether an agent feels good: above 1.5 s
# the conversation drags. It is measured here and not in each adapter because
# the two ends are already known here, and because only one of the three
# providers reports "the caller stopped talking" as its own event.


async def test_the_answer_time_is_measured_from_the_last_thing_the_caller_said():
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    speech.note_caller_activity()
    await asyncio.sleep(0.05)
    await speech.play(b"\x01" * 4)

    published = session.emitted("metrics")
    assert len(published) == 1
    measured = published[0].data["ttfb"][0]["value"]
    assert 0.04 < measured < 0.4, f"{measured} s is not the 0.05 that went by"


async def test_ttfb_is_reported_in_seconds_as_the_standard_asks():
    """`TTFBMetricsData.value` is "TTFB measurement in seconds"
    (Pipecat `acead05`, `metrics/metrics.py:35`).

    In milliseconds an RTVI client plotting ttfb reads 1562 where it expects
    1.562, with nothing raising: the captured event must carry seconds.
    """
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    speech.note_caller_activity()
    await asyncio.sleep(0.05)
    await speech.play(b"\x01" * 4)

    event = session.emitted("metrics")[0]
    entry = event.data["ttfb"][0]
    assert entry["processor"] == "galcymedia"
    assert entry["value"] < 1, f"{entry['value']} looks like milliseconds"
    assert entry["value"] >= 0.04, "50 ms went by: in seconds that is 0.05"


async def test_the_greeting_is_not_timed():
    """The bot answering when it picks up is not answering anyone: timing it
    would publish a number that means nothing."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)

    assert session.emitted("metrics") == []


async def test_the_time_is_published_once_per_turn():
    """The turn starts with the first block of audio; the rest of the same
    answer is not a new answer."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    speech.note_caller_activity()
    await speech.play(b"\x01" * 4)
    await speech.play(b"\x01" * 4)

    assert len(session.emitted("metrics")) == 1


async def test_the_clock_does_not_start_while_the_bot_is_talking():
    """A transcript of the caller arriving mid-answer does not open a new turn:
    the person did not ask anything new, the provider is just transcribing what
    it already heard."""
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    speech.note_caller_activity()
    await speech.play(b"\x01" * 4)          # first turn, publishes its time
    speech.note_caller_activity()           # arrives with the bot still talking
    await speech.play(b"\x01" * 4)

    assert len(session.emitted("metrics")) == 1


# ---------------------------------------------------------------------------
# How much of the bot's audio the person actually heard
# ---------------------------------------------------------------------------
#
# The value feeds OpenAI's `conversation.item.truncate`, whose server REJECTS an
# `audio_end_ms` past the real audio. A rejection does not drop the call, but it
# publishes an `error` to the client, so overshooting would turn a silent defect
# of ours into a false alarm in the integrator's panel. Hence a ceiling, and
# hence None when the library does not know.


class SessionWithMedia(FakeSession):
    """A session that carries the channel format, like the real one."""

    def __init__(self, frame_size: int = 160, ptime: int = 20) -> None:
        super().__init__()
        self.media = FakeMediaFormat(frame_size, ptime)


class FakeMediaFormat:
    def __init__(self, frame_size: int, ptime: int) -> None:
        self.optimal_frame_size = frame_size
        self.ptime = ptime
        self.silence_byte = 0xD5
        self.passthrough = False


async def test_the_heard_audio_never_exceeds_what_was_played():
    """The invariant that matters: the ceiling can be short, never long.

    The case is the real one, and it is what makes the clock bound load-bearing:
    a speech engine generates ten seconds of audio in one or two and queues them
    in one go (that is why the XOFF exists). The instant that block is queued,
    ten seconds were SENT and roughly zero were HEARD, because Asterisk plays in
    real time. Billing the sent bytes there would ask to truncate at 10000 ms of
    audio the person never got to hear, the server would reject it, and the
    truncate would not happen at all.

    Written so that dropping either bound fails: remove the clock and the
    ceiling jumps to what was sent, remove the sent bytes and a long XOFF makes
    the clock overshoot.
    """
    session = SessionWithMedia()
    speech = SpeechState(session)

    # 10 s of speech queued at once, which is what a TTS engine really does.
    await speech.play(b"\x01" * 80000)

    sent_ms = len(b"".join(session.audio)) / 8      # alaw: 8 bytes per ms
    played = speech.played_ms_at_most

    assert played is not None
    assert sent_ms == 10000, "the fixture has to queue ten seconds"
    assert played <= sent_ms, (
        f"the ceiling ({played} ms) went past what was sent ({sent_ms} ms): "
        "the server would reject the truncate and publish an error")

    # The decisive one: the clock, not the sent bytes, is what caps it. Ten
    # seconds cannot have been heard the microsecond they were queued.
    assert played < 100, (
        f"the ceiling is {played} ms for audio queued just now: the clock "
        "bound is not being applied, so the truncate would be rejected")


async def test_the_heard_audio_is_none_when_a_pause_broke_the_clock():
    """`pause()` stops the audio and not the time, so the clock measures long
    by an amount nobody knows. There the honest answer is None, not a number:
    whoever reads it must not truncate. Same criterion as `caller_quiet_for`.
    """
    session = SessionWithMedia()
    speech = SpeechState(session)

    await speech.play(b"\x01" * 1600)
    assert speech.played_ms_at_most is not None, "with no pause it does answer"

    speech.note_playback_paused()

    assert speech.played_ms_at_most is None, (
        "after a pause the clock overestimates without a known bound, so a "
        "number here would be invented")


async def test_the_heard_audio_is_none_without_a_turn_or_without_a_format():
    """Two more cases of not knowing, and both answer None."""
    session = SessionWithMedia()
    speech = SpeechState(session)

    assert speech.played_ms_at_most is None, "nothing played yet"

    # Without media there is no ptime, so bytes cannot become milliseconds:
    # alaw is 8 bytes per ms and slin16 is 32, and guessing would give double
    # or half without raising anything.
    blind = SpeechState(FakeSession(), frame_size=160, silence_byte=0xD5)
    await blind.play(b"\x01" * 1600)
    assert blind.played_ms_at_most is None, "no format, no milliseconds"


async def test_the_heard_audio_is_none_once_the_queue_drained():
    """Once Asterisk confirms the queue empty the turn is over and the bounds
    go with it: a number from a finished turn is a trap for the next reader
    (it truncated replies heard whole). While the queue still drains the
    number stands, because a barge-in there is real."""
    session = SessionWithMedia()
    speech = SpeechState(session)

    await speech.play(b"\x01" * 1600)
    await speech.end_turn()
    assert speech.played_ms_at_most is not None, "still draining: a number"

    speech.on_queue_drained()

    assert speech.played_ms_at_most is None, (
        "the queue drained and the bounds of a finished turn are still there")


async def test_a_discarded_frame_does_not_count_as_heard():
    """The bound is bytes that LEFT, not bytes attempted: with the queue full
    nothing was queued, so nothing was heard."""

    class Rejecting(SessionWithMedia):
        async def send_audio(self, chunk) -> bool:
            return False                     # sustained XOFF: nothing goes out

    session = Rejecting()
    speech = SpeechState(session)
    await speech.play(b"\x01" * 1600)

    assert speech.played_ms_at_most is None, (
        "no frame left, so there is no turn to truncate")


async def test_the_bounds_reset_on_the_next_turn():
    """The audio of the previous turn can no longer be truncated: carrying its
    count over would give a ceiling above what the new turn played."""
    session = SessionWithMedia()
    speech = SpeechState(session)

    await speech.play(b"\x01" * 1600)
    await speech.end_turn()
    speech.on_queue_drained()

    await speech.play(b"\x02" * 160)         # a new turn, one single frame
    played = speech.played_ms_at_most

    assert played is not None
    assert played <= 160 / 8, (
        f"the ceiling ({played} ms) carries audio from the previous turn")


async def test_discarded_audio_does_not_open_a_turn_that_nobody_hears():
    """Dropping the audio is not enough: the turn must not be opened either.

    The guard at the top of `play()` looks redundant, because the one inside
    the frame loop already stops the audio. It is not: without it the block
    that opens a turn still runs, and a phrase nobody hears publishes
    `bot-started-speaking`, bumps the turn counter and leaves `speaking` True
    with the bot mute. A client counting started/stopped ends up unbalanced,
    and `finish()` reads a turn that is not sounding.
    """
    session = FakeSession()
    speech = SpeechState(session, frame_size=4, silence_byte=0xD5)

    await speech.play(b"\x01" * 4)
    await speech.interrupt()

    turn_before = speech.turn
    started_before = len(session.emitted("bot-started-speaking"))

    await speech.play(b"\x02" * 4)          # audio of the cancelled response

    assert speech.turn == turn_before, (
        "a discarded phrase opened a new turn")
    assert len(session.emitted("bot-started-speaking")) == started_before, (
        "it published bot-started-speaking for audio nobody heard")
    assert not speech.speaking, (
        "it stayed marked as speaking with the bot mute")


async def test_the_heard_ceiling_is_capped_by_what_was_sent_not_only_by_time():
    """The mirror case of the ten-second block, and the one that was missing.

    A short word queued and then a pause: 20 ms of audio SENT and half a second
    on the clock. Only the sent-bytes bound stops it here, and without it the
    truncate would claim 500 ms of audio that was never generated. The server
    rejects that and publishes an `error`, so the phrase stays whole in the
    model's history AND the integrator gets a false alarm.
    """
    session = SessionWithMedia()
    speech = SpeechState(session)

    await speech.play(b"\x01" * 160)         # alaw: 160 B = 20 ms
    await asyncio.sleep(0.3)                 # the clock keeps running

    sent_ms = len(b"".join(session.audio)) / 8
    played = speech.played_ms_at_most

    assert sent_ms == 20, "the fixture has to send one single frame"
    assert played is not None
    assert played <= sent_ms, (
        f"the ceiling ({played} ms) went past the {sent_ms} ms that were sent: "
        "the clock bound alone would ask to truncate audio that never existed")


async def test_the_heard_ceiling_keeps_a_safety_margin():
    """It stays deliberately short, and the margin is what buys that.

    The bound can only be wrong in one direction without cost: too low leaves a
    few syllables in the history, too high is rejected. So the margin comes off
    the top, and it is the channel's own frame that sets it.
    """
    session = SessionWithMedia()
    speech = SpeechState(session)

    await speech.play(b"\x01" * 1600)        # 200 ms queued at once
    await asyncio.sleep(0.25)                # more clock than audio

    sent_ms = len(b"".join(session.audio)) / 8
    played = speech.played_ms_at_most

    assert played is not None
    # Two frames of alaw = 320 B = 40 ms of margin under the smaller bound.
    assert played <= sent_ms - 40, (
        f"the ceiling ({played} ms) does not keep the margin under the "
        f"{sent_ms} ms sent: it is aiming at the exact value, which is the "
        "one the server rejects")
