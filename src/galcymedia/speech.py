"""
The bot's speaking turn: the barge-in state machine.

It decides which bot audio plays, which is discarded for arriving late after
an interruption, and when the bot has said goodbye and must not sound again.
It lives in the library because it knows nothing about any provider: only
the `Session` and frame alignment, so three adapters use it unchanged, with
different codecs and silence bytes.

`SpeechState` (already built as `session.speech`) for an agent that can be
interrupted, which is the normal case; it includes the alignment.
`FrameAligner` alone when there are no turns to interrupt (an echo, a
recording). Whoever uses `SpeechState` never touches `FrameAligner`.

The flags COEXIST, they are not exclusive boxes: a bot can be speaking and
saying goodbye at once. The full model, with its state diagram and the
three owners of the barge-in, is in `docs/architecture.md`.
"""

from __future__ import annotations

import time
from typing import Any

from . import events
from .framing import FrameAligner


class SpeechState:
    """The bot's speaking turn over a `Session`.

    The adapter hands it the provider's audio (`play`) and the conversation
    milestones (`interrupt`, `end_turn`, `resume`, `begin_goodbye`); this
    class decides what sounds, what is dropped, and publishes the RTVI
    events of the bot's speech.
    """

    def __init__(self, session: Any, frame_size: int | None = None,
                 silence_byte: int | None = None) -> None:
        self._session = session

        # frame_size and silence_byte come from the channel format unless the
        # adapter passes them by hand. Deriving is right: the channel is the
        # source of truth, and guessing misaligns the audio in silence
        # (`docs/decisions.md`). With no media yet (before MEDIA_START) there
        # is NO guess: freezing 160/0xD5 for a channel that turns out slin16
        # is exactly the silent misalignment this avoids
        # (`test_without_media_and_without_explicit_values_raises`).
        media = getattr(session, "media", None)
        if media is None and (frame_size is None or silence_byte is None):
            raise ValueError(
                "SpeechState without a format: the session has no media yet "
                "(before MEDIA_START) and frame_size/silence_byte were not "
                "given. Build it from start() onwards, or pass both values."
            )
        if frame_size is None:
            frame_size = media.optimal_frame_size
        if silence_byte is None:
            silence_byte = media.silence_byte

        self._aligner = FrameAligner(frame_size, silence_byte)
        self._silence = silence_byte
        self._frame_size = frame_size

        self._speaking = False
        self._discarding = False
        self._goodbye = False

        # "The provider finished generating" (_speaking, cleared by end_turn)
        # is NOT "Asterisk finished playing". An engine generates 10 s of
        # audio in a couple of seconds and queues them (measured: OpenAI
        # delivered 14.6 s in ~3 s): when the provider says end_turn, that
        # audio is STILL sounding. _audio_pending stays up until Asterisk
        # confirms the queue empty (the Session gets QUEUE_DRAINED and calls
        # on_queue_drained here), so a barge-in AFTER end_turn still flushes
        # what sounds (`test_interrupt_flushes_even_if_the_provider_already_finished`).
        self._audio_pending = False

        # Since when the caller has been quiet WHILE HOLDING the turn. None
        # while there is no silence to measure. Started by on_queue_drained
        # (the bot really stopped sounding) and by end_turn when nothing is
        # queued; cleared by note_caller_activity.
        self._quiet_since: float | None = None

        # When the caller last spoke, to time how long the bot takes to
        # answer. None while there is nothing to measure: the opening
        # greeting answers nobody.
        self._asked_at: float | None = None

        # True while an interrupt() is in flight (during its await flush()).
        # A resume() landing in that window cannot cancel the discard: the
        # interruption, the caller's intent, wins over the response the
        # provider started before it knew of the barge-in
        # (`test_a_resume_during_an_interrupt_flush_does_not_win`).
        self._interrupting = False

        # How many bot turns started or got cut. Goes up when the bot starts
        # speaking and when it is interrupted, so it does not count phrases:
        # it counts turn CHANGES. `finish()` uses it to tell "the bot goes on
        # with the old thing" from "a new goodbye started", which no flag
        # says on its own.
        self._turn = 0

        # The two bounds of how much of the current turn got HEARD, for
        # `played_ms_at_most`. Neither works alone: see that property.
        self._turn_started_at: float | None = None
        self._turn_sent_bytes = 0

        # Milliseconds one byte lasts on THIS channel. Derived from `ptime`
        # and the frame, not from a codec table: alaw is 8 bytes per ms and
        # slin16 is 32, and whoever transcodes may override the frame. A
        # constant here would give double or half the milliseconds without
        # raising anything.
        ptime = getattr(media, "ptime", 0) if media is not None else 0
        self._ms_per_byte = (ptime / frame_size
                             if ptime > 0 and frame_size > 0 else 0.0)

        # A pause misaligns the clock with no known bound: `pause()` stops the
        # audio and not the time, so the measurement lands ABOVE what was
        # heard by an unknown amount. From then on this class no longer knows
        # how much was heard, and says so with None instead of a number
        # (`test_the_heard_audio_is_none_when_a_pause_broke_the_clock`).
        self._clock_unreliable = False

    @property
    def speaking(self) -> bool:
        """True while the PROVIDER is generating the bot's turn.

        Cleared as soon as the provider closes the turn (end_turn), even if
        its audio keeps sounding in Asterisk. For "the bot is still heard",
        see `bot_audio_playing`.
        """
        return self._speaking

    @property
    def bot_audio_playing(self) -> bool:
        """True while there is bot audio in play, generating or sounding.

        Unlike `speaking`, it stays True after the provider closed the turn,
        until Asterisk confirms the queue empty. It is the right signal for
        "do not disturb while the bot talks" (an abandonment notice, for
        instance): the bot holds the turn until its last word sounded
        (`test_bot_audio_playing_stays_after_end_turn_until_drained`).
        """
        return self._speaking or self._audio_pending

    @property
    def turn(self) -> int:
        """How many bot turns started or got cut in this call.

        Goes up when the bot starts speaking and when it is interrupted, so
        it does not count phrases: it counts turn CHANGES. Tells whether what
        sounds now is what sounded before, which no flag says on its own.
        """
        return self._turn

    @property
    def played_ms_at_most(self) -> int | None:
        """Ceiling of how many milliseconds of the current turn the caller HEARD.

        A CEILING, not a measurement: the real value can be lower, never
        higher. It exists for OpenAI's `conversation.item.truncate`, which
        takes the audio PLAYED and whose server REJECTS a value past the real
        one (`conversation_item_truncate_event.py:30-31`). A rejection does
        not cut the call, but publishes an `error` to the client
        (`_shared.report_provider_error`), so overshooting turns a silent
        defect of ours into an alarm on the integrator's panel. The bias is
        deliberate and in the name.

        `None` when the library does NOT KNOW, and that is an answer, not a
        failure: no turn started, no channel format to turn bytes into
        milliseconds, or the clock misaligned by a `pause()`. Whoever gets it
        must not truncate: inventing a number is what this class avoids. Same
        criterion as `caller_quiet_for`, which answers None and not zero.

        The MINIMUM of two bounds that overestimate for different and
        independent reasons, minus a margin:

            clock    the audio plays at real time, one frame per channel
                     tick (`chan_websocket.c:461-462`, `:618-623`), so what
                     was heard does not exceed the time since the turn's
                     first frame. Overestimates under XOFF (the frame did not
                     go out and the clock ran anyway), by the queue's transit
                     time, and with `set_media_direction("in")`, which closes
                     the channel timer (`:908-911`).
            sent     nothing can be heard beyond what went out the socket.
                     Overestimates because a barge-in EMPTIES the Asterisk
                     queue (up to 900 frames, 18 s at 20 ms,
                     `chan_websocket.c:142`), and because the aligner's tail
                     is padded with silence, which is not speech.

        The margin comes from the channel itself, two frames: the server
        error only fires if the value is GREATER than the real one, so
        falling short is the only safe side. Truncating a little less leaves a
        few syllables in the history; overshooting truncates nothing and
        makes noise (`test_the_heard_ceiling_keeps_a_safety_margin`).
        """
        if self._turn_started_at is None or self._ms_per_byte <= 0:
            return None
        if self._clock_unreliable:
            return None

        by_clock = (time.monotonic() - self._turn_started_at) * 1000
        by_sent = self._turn_sent_bytes * self._ms_per_byte

        # The margin comes off the minimum, not off each bound: taking it
        # twice would give history away without gaining safety.
        margin_ms = 2 * self._frame_size * self._ms_per_byte
        return max(0, int(min(by_clock, by_sent) - margin_ms))

    @property
    def saying_goodbye(self) -> bool:
        return self._goodbye

    @property
    def caller_quiet_for(self) -> float | None:
        """Seconds the caller has been quiet WHILE HOLDING the turn.

        None while there is no silence to measure: either the bot is still
        heard (its turn, not an abandonment), or the caller just spoke.

        Measuring it by eye goes wrong the same way in every adapter:
        counting from the caller's last turn mixes in the time the bot
        talked, and a twenty-second bot line looks like abandonment. The
        clock starts when the bot really stops sounding (`bot_audio_playing`,
        up until Asterisk confirms the queue empty).

        The queue notice starts it when it comes, but it is not relied on:
        one provider only closes the bot's turn when the caller SPEAKS
        (ElevenLabs, from its caller transcript:
        `test_the_caller_speaking_closes_the_bot_turn`), so in the very case
        this meter exists for, someone who puts the phone down and says
        nothing, that notice is never requested. So the clock also starts in
        `end_turn`, as soon as no bot audio is in play
        (`test_the_silence_clock_starts_without_a_queue_notice`).

        A METER, not a policy: how many seconds are too many, what is said
        and when to hang up belong to the agent, not the library.
        """
        if self._quiet_since is None:
            return None
        return time.monotonic() - self._quiet_since

    def note_playback_paused(self) -> None:
        """Playback was paused: the clock of what was heard no longer holds.

        Called by `Session.pause()`. `PAUSE_MEDIA` stops the audio and not
        the time, so from here the clock measures long by an unknown amount:
        `played_ms_at_most` answers None for the rest of the turn, which is
        the honest answer. Cleared only when the next turn starts.

        Not undone by `continue_media()` on purpose: on resume the pause's
        length is still unknown, so the clock does not recover.
        """
        self._clock_unreliable = True

    def note_caller_activity(self) -> None:
        """The caller spoke: there is no silence to count.

        Called by the adapter when the provider reports voice or a caller
        transcript. The channel cannot know it on its own: room noise always
        travels on the line, so the silence of a stalled conversation is not
        the silence of the audio.
        """
        self._quiet_since = None

        # And the answer stopwatch starts. It takes the caller's LAST
        # activity and not a "stopped speaking" event, because only one of
        # the three providers sends that: measuring from here is the only
        # thing that gives the same number with any of them
        # (`test_the_answer_time_is_measured_from_the_last_thing_the_caller_said`).
        if not self._speaking and not self._audio_pending:
            self._asked_at = time.monotonic()

    async def play(self, block: bytes) -> None:
        """Bot audio toward the caller, aligned to the channel frame.

        The discard is re-checked BEFORE every frame, not once at the top:
        every `send_audio` yields, and a barge-in landing in that gap has to
        cut the block in progress, not keep pushing frames the caller no
        longer wants (`test_the_barge_in_cuts_the_audio_in_flight`).

        It never looks at `_goodbye`, which is why `mark_goodbye()` can set
        that flag WITHOUT cutting anything: a goodbye in progress sounds
        whole. The only thing that stops this method is `_discarding`, so
        when `begin_goodbye()` really cuts, what cuts is the `_discarding` it
        sets, not the `_goodbye`.
        """
        if self._discarding or not block:
            return

        if not self._speaking:
            self._speaking = True
            self._turn += 1          # a new turn starts
            # The bot's turn again: the caller's silence stops counting
            # until the bot goes quiet once more.
            self._quiet_since = None
            # The heard bounds are PER TURN: reset here, not in `end_turn`.
            # The previous turn's audio can no longer be truncated, and
            # carrying its count would give a ceiling above the real one
            # (`test_the_bounds_reset_on_the_next_turn`).
            self._turn_started_at = None
            self._turn_sent_bytes = 0
            self._clock_unreliable = False
            self._report_ttfb()
            self._session.emit(events.bot_started_speaking())

        frames = self._aligner.push(block)
        for index, frame in enumerate(frames):
            if self._discarding:
                break
            if not await self._session.send_audio(frame):
                # The frame did not go out: the Asterisk queue is full and
                # not draining. The WHOLE block is cut instead of going on,
                # because the XOFF budget is paid per frame: a 25-frame block
                # would take 25 times that limit, and meanwhile the adapter
                # does not read its socket, so it cannot even learn of the
                # barge-in that would abort this
                # (`test_a_refused_frame_stops_the_whole_block`).
                #
                # The remaining ones are counted too, though never tried: the
                # closing log would say "1 frame dropped" when twenty-five
                # were lost, and that number is all that is left to know how
                # much audio did not arrive.
                self._session.note_audio_dropped(len(frames) - index - 1)
                break
            # It really went out: something is sounding (or queued) in
            # Asterisk until the queue is confirmed empty. Holds the barge-in
            # even after the provider said end_turn. Only set on a REAL send:
            # setting it on a dropped frame leaves the flag up waiting for a
            # queue notice Asterisk will not send, because nothing was queued
            # (`test_a_discarded_frame_is_not_counted_as_queued`).
            self._audio_pending = True

            # The heard clock starts with the first frame that GOES OUT, not
            # on entering here: a whole XOFF can sit between the two, and
            # counting from before would give a ceiling above the real one.
            if self._turn_started_at is None:
                self._turn_started_at = time.monotonic()
            self._turn_sent_bytes += len(frame)

    def _report_ttfb(self) -> None:
        """Publishes how long the bot took to start answering.

        THE measurement of a voice agent, and the first thing to look at when
        someone says "the bot is slow". Measured from the last time the
        caller spoke to the bot's first audio frame, which is what the caller
        really waits for.

        Emitted once per turn, and only if the caller had a turn before: the
        bot's greeting on answering replies to nobody, so timing it would
        give a number that means nothing (`test_the_greeting_is_not_timed`).
        """
        if self._asked_at is None:
            return
        # In SECONDS, the standard's unit
        # (Pipecat `acead05`, `metrics/metrics.py:35`, "TTFB measurement in
        # seconds"). In milliseconds an RTVI client reads 1562 where it
        # expects 1.562 (`test_ttfb_is_reported_in_seconds_as_the_standard_asks`).
        elapsed_s = time.monotonic() - self._asked_at
        self._asked_at = None
        self._session.emit(events.metrics(
            ttfb=[{"processor": "galcymedia", "value": round(elapsed_s, 3)}]))

    def resume(self) -> None:
        """A new response from the provider closes the previous one's discard.

        It does NOT give the bot its voice back: all it does is lower
        `_discarding`, so the next `play()` can sound again. Between this
        call and that `play()` the bot stays quiet.

        Without it, an interruption landing on a turn's last frames leaves
        the bot mute for the rest of the call.
        """
        # A `resume()` landing while an `interrupt()` is in flight does not
        # lift the discard: that interruption has not finished flushing and
        # wins over the response this `resume()` stands for.
        if self._goodbye or self._interrupting:
            return
        self._discarding = False

    async def interrupt(self) -> None:
        """Barge-in: the caller talked over the bot.

        Acts if there IS bot audio in play: either the provider is generating
        (`_speaking`), or it finished but its audio still sounds in the
        Asterisk queue (`_audio_pending`). Both have to be flushed; looking
        at `_speaking` alone let the second one through, and the bot did not
        go quiet after an already generated response. With the bot really
        quiet (nothing generated, queue empty) there is nothing to flush and
        doing it would take the freshly created response, so it does not act
        (`test_interrupt_only_acts_while_the_bot_is_sounding`,
        `test_with_the_queue_already_empty_a_barge_in_does_not_flush`).

        Sets `_interrupting` for the length of the `await flush()`: a
        `resume()` landing in that window cannot cancel this discard.
        """
        if not self._speaking and not self._audio_pending:
            return

        self._interrupting = True
        self._speaking = False
        self._audio_pending = False
        self._discarding = True
        # The turn got cut: whatever comes from the provider from here on
        # belongs to a response that no longer counts, even if its closing
        # event arrives later and finds `_interrupting` False again.
        self._turn += 1
        self._aligner.flush()
        try:
            await self._session.flush()
        finally:
            self._interrupting = False

    async def end_turn(self, reset_discard: bool = False) -> None:
        """The provider finished emitting the bot's turn.

        The aligner's tail goes out now, padded with the codec's silence:
        leaving it for the next turn glues those milliseconds of the old
        utterance in front of the new one
        (`test_the_end_of_turn_sends_the_rest_with_silence`).
        """
        was_speaking = self._speaking
        if self._speaking:
            tail = self._aligner.flush()
            if tail and await self._session.send_audio(tail):
                self._audio_pending = True
                self._turn_sent_bytes += len(tail)
                if self._turn_started_at is None:
                    self._turn_started_at = time.monotonic()

        self._speaking = False

        # CAREFUL with hardening this guard. The case that looks wrong is the
        # close of a response the caller just cancelled: it lifts the discard
        # and lets the tail of the interrupted utterance play. Blocking the
        # close that follows an `interrupt()` is NOT an option: in one
        # provider the interruption and the close are the SAME fact, the
        # caller speaking, and nobody calls `resume()`, so this is its only
        # door back to speaking. That adapter drops the stale audio by
        # correlation before it reaches here, which is what lets this guard
        # stay lax (`tests/test_adapters_barge_in.py`, `docs/architecture.md`).
        if reset_discard and not self._goodbye:
            self._discarding = False

        # The provider finished generating, but its audio is still queued in
        # Asterisk: the queue-drained notice is requested to clear
        # _audio_pending when it really stops sounding. Until then a late
        # barge-in still flushes it (`on_queue_drained`, called by the
        # Session on QUEUE_DRAINED;
        # `test_end_turn_requests_the_queue_drained_notification`).
        if was_speaking and self._audio_pending:
            await self._session.report_bot_audio_drained()

        # Only on a real transition from speaking to quiet: emitting it always
        # would send a spurious bot_stopped_speaking on two consecutive
        # end_turns and unbalance the client's started/stopped count
        # (`test_two_consecutive_end_turns_emit_a_single_bot_stopped`).
        if was_speaking:
            # The bot closed its turn: from here the caller's silence counts,
            # without waiting for the queue notice, because one provider only
            # closes the turn when the caller SPEAKS (see `caller_quiet_for`).
            # Only if no audio is still sounding: while it sounds the turn is
            # still the bot's, and `on_queue_drained` starts it there.
            if not self._audio_pending and self._quiet_since is None:
                self._quiet_since = time.monotonic()
            self._session.emit(events.bot_stopped_speaking())

    def on_queue_drained(self) -> None:
        """Asterisk confirmed the output queue is empty.

        Only HERE did the bot really stop sounding: _audio_pending goes down,
        so a later barge-in (with the bot already quiet) does not flush a new
        response. The Session calls it on QUEUE_DRAINED.

        It is also where the caller's silence starts running: until now the
        turn was the bot's (see `caller_quiet_for`).
        """
        # This notice is NOT about one turn, it is about the queue's state.
        # In the channel `report_queue_drained` is a single flag, set on each
        # request and fired when the queue runs out of frames
        # (`chan_websocket.c:495-503`), so two requests produce ONE notice,
        # and that notice means "the queue was empty when it was sent".
        # Pairing notices with turns would leave orphan requests waiting for
        # a second notice the channel will not send, with `_audio_pending`
        # stuck True for the rest of the call, which is worse
        # (`test_the_queue_notice_is_about_the_queue_not_about_a_turn`).
        #
        # A narrow residue remains: between Asterisk emptying the queue and
        # this line running there is a network trip, and in that gap the
        # application may have sent frames of the next turn. There it goes
        # down with audio genuinely queued. Not chased: a turn counter does
        # not fix it (the notice still does not say when it is from) and the
        # cost would be the orphan requests again.
        self._audio_pending = False
        # The heard bounds go with the audio: from here `played_ms_at_most`
        # answers None. A number from a turn that already ended is a trap for
        # the next reader (measured: it truncated replies heard whole;
        # `test_the_heard_audio_is_none_once_the_queue_drained`). Only when
        # nothing new is being generated: if the provider already opened the
        # next turn, the bounds are that turn's.
        if not self._speaking:
            self._turn_started_at = None
            self._turn_sent_bytes = 0
        if not self._speaking and self._quiet_since is None:
            self._quiet_since = time.monotonic()

    async def begin_goodbye(self) -> None:
        """Cuts the bot's voice at once and for the rest of the call.

        Both halves are needed: emptying the Asterisk queue alone is not
        enough, because the provider keeps generating the utterance in
        progress and fills it again; what arrives afterwards has to be
        discarded too (`test_the_goodbye_does_not_reopen`).
        """
        self._goodbye = True
        self._discarding = True
        self._speaking = False
        self._audio_pending = False
        self._aligner.flush()
        await self._session.flush()

    def mark_goodbye(self) -> None:
        """The goodbye is about to sound: blocks reopenings WITHOUT cutting audio.

        For the close by abandonment, where the goodbye has to sound whole
        and what must not happen is a later response reviving the bot
        (`test_mark_goodbye_blocks_without_cutting_the_audio`).
        """
        self._goodbye = True
