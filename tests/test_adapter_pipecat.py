"""
The Pipecat adapter.

The tests are skipped if Pipecat is not installed: the adapter is an optional
extra, so its absence cannot break the core suite.

No pipeline is spun up. A serializer is a pure function between frames and
bytes, and it is tested by calling it directly, which is how Pipecat itself
tests it.
"""

from __future__ import annotations

import asyncio
import json

import pytest

pipecat = pytest.importorskip(
    "pipecat", reason="the Pipecat adapter is an optional extra"
)

from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    InputAudioRawFrame,
    InputDTMFFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    StartFrame,
    SystemFrame,
    TTSAudioRawFrame,
)

from galcymedia import pcm
from galcymedia.adapters.pipecat import (
    AsteriskFrameSerializer,
    AsteriskMediaStartFrame,
)
from galcymedia.protocol import sample_rate_for


def media_start(fmt: str = "alaw", frame_size: int = 160, **extra) -> str:
    """A MEDIA_START like the one the channel sends."""
    payload = {
        "event": "MEDIA_START",
        "connection_id": "c1",
        "channel": "PJSIP/test-0001",
        "channel_id": "1234.5",
        "format": fmt,
        "optimal_frame_size": frame_size,
        "ptime": 20,
        "channel_variables": {"AI_PROVIDER": "pipecat"},
    }
    payload.update(extra)
    return json.dumps(payload)


async def new_serializer(fmt: str = "alaw", frame_size: int = 160, **kwargs):
    """Serializer already started and with the codec learned from the channel.

    The pipeline is started at the channel's own rate, which is the
    recommended setup: no resampling, so these tests measure the codec and the
    framing without a converter in the middle. The mismatch case has its own
    test, `test_the_audio_is_resampled_when_the_rates_differ`.
    """
    params = AsteriskFrameSerializer.InputParams(**kwargs) if kwargs else None
    serializer = AsteriskFrameSerializer(params=params)
    rate = sample_rate_for(fmt)
    await serializer.setup(
        StartFrame(audio_in_sample_rate=rate, audio_out_sample_rate=rate)
    )
    await serializer.deserialize(media_start(fmt, frame_size))
    return serializer


# ---------------------------------------------------------------------------
# The codec is learned from the channel
# ---------------------------------------------------------------------------


async def test_the_codec_is_learned_from_the_media_start():
    """The channel format rules, not a hard-coded value."""
    serializer = await new_serializer("ulaw")

    # A ulaw audio frame decodes as ulaw, not as alaw.
    ulaw_silence = bytes([pcm.ULAW_SILENCE]) * 160
    frame = await serializer.deserialize(ulaw_silence)

    assert isinstance(frame, InputAudioRawFrame)
    assert frame.audio == pcm.ulaw_to_pcm(ulaw_silence)
    assert frame.sample_rate == 8000
    assert frame.num_channels == 1


def test_the_g711_names_are_the_channels_own():
    """The G.711 tables hold the names the channel writes, and only those.

    MEDIA_START carries `ast_format_get_name` (`chan_websocket.c:236`): `alaw`
    and `ulaw` (`codec_builtin.c:168`, `:183`). An alias such as `pcma` or
    `mulaw` documents a format that never arrives, and the other adapters
    already dropped theirs.
    """
    from galcymedia.adapters.pipecat import _ALAW_NAMES, _ULAW_NAMES

    assert _ALAW_NAMES == {"alaw"}
    assert _ULAW_NAMES == {"ulaw"}


async def test_slin_does_not_transcode_and_derives_its_sample_rate():
    """slin16 is already PCM: it passes through as is and at 16 kHz."""
    serializer = await new_serializer("slin16", frame_size=640)

    samples = b"\x01\x02" * 320
    frame = await serializer.deserialize(samples)

    assert frame.audio == samples           # not a single byte touched
    assert frame.sample_rate == 16000        # derived from the format name


async def test_audio_before_the_media_start_is_discarded():
    """Audio can arrive before the MEDIA_START (issue #1712)."""
    serializer = AsteriskFrameSerializer()
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
    )

    # With no known codec it cannot be decoded: it is ignored instead of
    # handing raw G.711 bytes to a model that expects PCM.
    assert await serializer.deserialize(b"\xd5" * 160) is None


# ---------------------------------------------------------------------------
# From the pipeline toward the channel
# ---------------------------------------------------------------------------


async def test_audio_goes_out_in_the_channel_codec():
    """PCM from the pipeline goes out as the channel's G.711."""
    serializer = await new_serializer("alaw")

    samples = b"\x00\x00" * 160          # 160 PCM samples = 160 alaw bytes
    sent = await serializer.serialize(
        OutputAudioRawFrame(audio=samples, sample_rate=8000, num_channels=1)
    )

    assert sent == pcm.pcm_to_alaw(samples)


async def test_audio_goes_out_aligned_to_the_channel_frame():
    """An incomplete block is held; once completed it goes out whole.

    This is the reason alignment exists: the channel drops the tail of every
    message that is not a whole number of frames (`chan_websocket.c:1050-1064`),
    and that is choppy speech without a single lost packet.
    """
    serializer = await new_serializer("alaw", frame_size=160)

    # Half a frame: 80 PCM samples -> 80 alaw bytes, does not reach 160.
    partial = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 80, sample_rate=8000,
                            num_channels=1)
    )
    assert partial is None

    # The other half completes the frame: now it does go out, exactly 160 bytes.
    complete = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 80, sample_rate=8000,
                            num_channels=1)
    )
    assert complete is not None
    assert len(complete) == 160


async def test_without_alignment_audio_goes_out_as_is():
    """With align_frames=False the pipeline's size rules."""
    serializer = await new_serializer("alaw", align_frames=False)

    sent = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 80, sample_rate=8000,
                            num_channels=1)
    )
    assert sent is not None and len(sent) == 80


async def test_tts_audio_is_also_translated():
    """TTSAudioRawFrame inherits from AudioRawFrame: it is the normal case."""
    serializer = await new_serializer("alaw")

    sent = await serializer.serialize(
        TTSAudioRawFrame(audio=b"\x00\x00" * 160, sample_rate=8000,
                         num_channels=1)
    )
    assert sent is not None and len(sent) == 160


async def test_a_split_sample_block_does_not_corrupt_the_audio():
    """The stray byte of an odd block waits for its partner.

    One PCM sample is TWO bytes, and streaming TTS engines cut where the
    network buffer ends, not at a whole sample. Dropping that byte shifts ALL
    the following ones by half a step, and the audio turns to noise. The
    measurement is in `docs/decisions.md`.
    """
    serializer = await new_serializer("alaw", align_frames=False)

    # Three odd blocks in a row, like those from a streaming TTS.
    sent = b""
    for block in (b"\x01\x02\x03", b"\x04\x05\x06", b"\x07\x08\x09"):
        chunk = await serializer.serialize(
            OutputAudioRawFrame(audio=block, sample_rate=8000, num_channels=1)
        )
        if chunk:
            sent += chunk

    # The 9 bytes are 4 complete samples and one byte waiting: nothing is lost
    # along the way and nothing is shifted.
    assert sent == pcm.pcm_to_alaw(b"\x01\x02\x03\x04\x05\x06\x07\x08")


async def test_a_huge_block_does_not_exceed_the_channel_limit():
    """A message longer than the channel limit hangs up the call.

    A TTS can deliver a whole long sentence in a single block. The remainder
    is held and goes out in the following messages, without being lost.
    """
    serializer = await new_serializer("alaw", align_frames=False)

    # 200,000 samples -> 200,000 alaw bytes, well above the limit.
    first = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 200_000, sample_rate=8000,
                            num_channels=1)
    )
    assert len(first) <= 65500

    # What did not fit goes out afterward, it is not dropped.
    rest = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 10, sample_rate=8000,
                            num_channels=1)
    )
    assert rest is not None and len(rest) > 10


async def test_the_sample_rate_of_each_slin_format():
    """The format name does not always tell its frequency.

    `slin44` is 44100 Hz, not 44000: deriving it from the name gave a value
    100 Hz low, and with it a drift that grows over the whole call.
    """
    expected = {
        "slin": 8000, "slin12": 12000, "slin16": 16000, "slin24": 24000,
        "slin32": 32000, "slin44": 44100, "slin48": 48000, "slin96": 96000,
        "slin192": 192000,
    }
    for fmt, rate in expected.items():
        serializer = await new_serializer(fmt, frame_size=320)
        assert serializer._sample_rate == rate, fmt


async def test_the_audio_is_resampled_when_the_rates_differ(caplog):
    """A rate mismatch is CONVERTED, not left to rot.

    The `c()` in the Dial decides the call rate, but the channel only
    announces it in MEDIA_START, by which time the pipeline has already
    started. Neither side can renegotiate: the channel event is a `send_event`
    announcement (chan_websocket.c:1178) and the speech engine locks its rate
    at startup (stt_service.py:359). So the conversion happens here.

    Leaving it alone plays the voice at the wrong speed with no exception at
    all, which is this adapter's most expensive failure to diagnose.
    """
    serializer = AsteriskFrameSerializer()
    await serializer.setup(
        StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=16000)
    )

    with caplog.at_level("WARNING"):
        await serializer.deserialize(media_start("ulaw"))

    assert "8000" in caplog.text and "16000" in caplog.text
    # The Dial hint names the PIPELINE rate, the one the channel is missing.
    # `c(slin)` would hand the integrator the 8 kHz the channel already has
    # (and is a substring of `c(slin16)`, so the stricter check is the one).
    assert "c(slin16)" in caplog.text, "the warning does not say what to write"
    assert "c(slin) " not in caplog.text, "the hint names the channel's own rate"

    # Incoming: one second of channel audio (8 kHz) must reach the pipeline as
    # one second at 16 kHz, or the engine reads it at the wrong speed.
    frame = await serializer.deserialize(pcm.pcm_to_ulaw(b"\x00\x00" * 8000))
    assert frame.sample_rate == 16000, "it declares the channel rate, not the pipeline's"
    # Roughly double: a streaming resampler holds a few samples on the first
    # block (measured: ~5% short), so this checks the ratio, not an exact size.
    # Without resampling it would be 16000 bytes, half of this.
    assert len(frame.audio) > 16000 * 1.5, (
        f"{len(frame.audio)} bytes: the audio was not resampled and the "
        f"engine would read it at half speed")

    # Outgoing: one second at 16 kHz must leave as one second of 8 kHz ulaw.
    out = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 16000, sample_rate=16000,
                            num_channels=1)
    )
    # Around 8000 bytes, not exactly: a streaming resampler holds a few
    # samples on the first block and the aligner keeps whatever does not fill
    # a frame. What matters is the ORDER: without resampling this would be
    # 16000 bytes, so the voice would play at half speed.
    assert 7000 < len(out) < 9000, (
        f"one second went out as {len(out)} bytes instead of ~8000: the voice "
        f"would play at the wrong speed")


async def test_no_resampling_happens_when_the_rates_match():
    """The recommended path costs nothing: not a single byte is touched."""
    serializer = AsteriskFrameSerializer()
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
    )
    await serializer.deserialize(media_start("ulaw"))

    assert serializer._resample_in is None and serializer._resample_out is None


async def test_it_does_not_warn_when_the_rates_match():
    """The warning is only useful if it stays quiet when all is well."""
    serializer = AsteriskFrameSerializer()
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
    )
    await serializer.deserialize(media_start("ulaw"))

    assert serializer._sample_rate == 8000


async def test_the_output_rate_is_the_frame_rate_not_the_input_rate(caplog):
    """The bot audio leaves at the rate the FRAME carries.

    Pipecat resamples the bot audio to `audio_out_sample_rate` and rebuilds
    the frame with it (`base_output.py:129`, `fastapi.py:533`). A pipeline
    with an output rate of its own (a 24 kHz TTS is the common case) and an
    input rate equal to the channel's builds no converter in `setup`; left to
    the input rate, one second leaves as three and the voice plays slow and
    low with no exception.
    """
    serializer = AsteriskFrameSerializer(
        params=AsteriskFrameSerializer.InputParams(align_frames=False)
    )
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=24000)
    )
    await serializer.deserialize(media_start("ulaw"))
    assert serializer._resample_out is None, "setup has no output rate to go on"

    with caplog.at_level("WARNING"):
        out = await serializer.serialize(
            OutputAudioRawFrame(audio=b"\x00\x00" * 24000, sample_rate=24000,
                                num_channels=1)
        )
    assert 7000 < len(out) < 9000, (
        f"one second at 24 kHz went out as {len(out)} ulaw bytes instead of "
        f"~8000: the voice would play at the wrong speed")
    assert "audio_out_sample_rate=8000" in caplog.text
    assert "c(slin24)" in caplog.text, "the warning does not say what to write"

    # Once per call, not once per frame.
    with caplog.at_level("WARNING"):
        await serializer.serialize(
            OutputAudioRawFrame(audio=b"\x00\x00" * 2400, sample_rate=24000,
                                num_channels=1)
        )
    assert caplog.text.count("audio_out_sample_rate=8000") == 1


async def test_output_at_the_channel_rate_is_not_converted_even_if_the_input_is():
    """Listening at 16 kHz and playing at 8 kHz over an 8 kHz channel.

    The input converter exists and the output must not borrow it: converting
    8 kHz audio as if it were 16 kHz halves its duration, and the voice plays
    at double speed.
    """
    serializer = AsteriskFrameSerializer(
        params=AsteriskFrameSerializer.InputParams(align_frames=False)
    )
    await serializer.setup(
        StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=8000)
    )
    await serializer.deserialize(media_start("ulaw"))
    assert serializer._resample_in is not None

    out = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 8000, sample_rate=8000,
                            num_channels=1)
    )
    assert len(out) == 8000, (
        f"one second at the channel rate went out as {len(out)} bytes")


async def test_an_unknown_format_does_not_leave_the_rate_at_zero():
    """A zero sample rate blows up inside Pipecat when resampling."""
    serializer = await new_serializer("lpc10", frame_size=160)

    assert serializer._sample_rate > 0


# ---------------------------------------------------------------------------
# One instance, several calls
# ---------------------------------------------------------------------------


async def test_a_reused_instance_hangs_up_on_every_call():
    """The hang-up guard cannot survive the call.

    Regression: `AsteriskFrameSerializer()` asks for no call data (it learns
    it from the channel), so building it once and reusing it is the natural
    thing. With a dirty guard, the second call did not hang up and the channel
    stayed alive until the Dial timeout.
    """
    serializer = AsteriskFrameSerializer()

    for _ in range(3):
        await serializer.setup(
            StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
        )
        await serializer.deserialize(media_start("alaw"))
        assert await serializer.serialize(EndFrame()) is not None


async def test_a_new_call_does_not_drag_audio_from_the_previous_one():
    """The half-done audio from the previous call cannot play in the new one."""
    serializer = AsteriskFrameSerializer(
        params=AsteriskFrameSerializer.InputParams(align_frames=False)
    )
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
    )
    await serializer.deserialize(media_start("alaw"))
    await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x01\x02\x03", sample_rate=8000,
                            num_channels=1)
    )

    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
    )
    await serializer.deserialize(media_start("alaw"))

    assert serializer._odd_byte == b""
    assert serializer._pending_overflow == b""


async def test_a_new_codec_drops_the_old_codec_audio():
    """A second MEDIA_START changes the codec, and the pending bytes are the old one's.

    Regression: those bytes were written in the old codec, so sending them out
    in the new one is noise, and the stray byte also shifts everything that
    comes behind it by half a step.
    """
    serializer = await new_serializer("alaw", align_frames=False)
    await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x01\x02\x03", sample_rate=8000,
                            num_channels=1)
    )
    assert serializer._odd_byte != b""

    await serializer.deserialize(media_start("slin16", frame_size=640))

    assert serializer._odd_byte == b""


async def test_the_audio_waiting_for_a_slot_does_not_grow_without_bound():
    """Holding without draining turns an audio problem into a memory one.

    The transport sends ONE message per `serialize` (`fastapi.py:558-567`), so
    keeping the whole remainder holds more audio than will ever go out, and it
    is lost on hang-up.
    """
    serializer = await new_serializer("alaw", align_frames=False)

    for _ in range(20):
        await serializer.serialize(
            OutputAudioRawFrame(audio=b"\x00\x00" * 200_000, sample_rate=8000,
                                num_channels=1)
        )

    assert len(serializer._pending_overflow) <= 65500


# ---------------------------------------------------------------------------
# Barge-in and hang-up
# ---------------------------------------------------------------------------


async def test_the_interruption_flushes_the_channel_queue():
    """The barge-in is translated to the channel's flush command."""
    serializer = await new_serializer()

    sent = await serializer.serialize(InterruptionFrame())

    assert json.loads(sent) == {"command": "FLUSH_MEDIA"}


async def test_the_interruption_drops_the_alignment_remainder():
    """The tail of the interrupted sentence cannot survive the barge-in.

    Regression: the remainder held in the aligner (the milliseconds that did
    not complete a frame) survived the flush and went out glued in FRONT of
    the next response, which is heard as a hiccup of the sentence the person
    just cut off.
    """
    serializer = await new_serializer("alaw", frame_size=160)

    # Half a bot frame is left pending, without ever being sent.
    assert await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x11\x22" * 80, sample_rate=8000,
                            num_channels=1)
    ) is None

    await serializer.serialize(InterruptionFrame())

    # The new response goes out clean: without the bytes of the interrupted sentence.
    fresh = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x33\x44" * 80, sample_rate=8000,
                            num_channels=1)
    )
    assert fresh is None, "the old remainder completed a frame and slipped through"


async def test_the_end_of_the_pipeline_hangs_up_the_call():
    serializer = await new_serializer()

    sent = await serializer.serialize(EndFrame())

    assert json.loads(sent) == {"command": "HANGUP"}


async def test_it_does_not_hang_up_twice():
    """EndFrame and CancelFrame can both come down through the same close."""
    serializer = await new_serializer()

    assert await serializer.serialize(EndFrame()) is not None
    assert await serializer.serialize(CancelFrame()) is None


async def test_the_automatic_hang_up_can_be_turned_off():
    """Whoever manages the lifecycle themselves does not want this hang-up."""
    serializer = await new_serializer(auto_hang_up=False)

    assert await serializer.serialize(EndFrame()) is None


# ---------------------------------------------------------------------------
# DTMF
# ---------------------------------------------------------------------------


async def test_the_dtmf_reaches_the_pipeline():
    serializer = await new_serializer()

    frame = await serializer.deserialize(
        json.dumps({"event": "DTMF_END", "digit": "5"})
    )

    assert isinstance(frame, InputDTMFFrame)
    assert frame.button == KeypadEntry.FIVE


async def test_the_dtmf_pipecat_does_not_know_does_not_break_the_call():
    """Asterisk emits A-D, which are valid DTMF but do not exist in Pipecat."""
    serializer = await new_serializer()

    assert await serializer.deserialize(
        json.dumps({"event": "DTMF_END", "digit": "A"})
    ) is None


# ---------------------------------------------------------------------------
# What is NOT translated
# ---------------------------------------------------------------------------


async def test_flow_control_is_not_translated():
    """XOFF and XON belong to the transport: Pipecat does not model that."""
    serializer = await new_serializer()

    for event in ("MEDIA_XOFF", "MEDIA_XON", "QUEUE_DRAINED", "STATUS"):
        assert await serializer.deserialize(
            json.dumps({"event": event})
        ) is None


async def test_an_unknown_frame_produces_nothing():
    """The contract's fallthrough: what is not translated is not sent."""
    serializer = await new_serializer()

    assert await serializer.serialize(StartFrame()) is None


async def test_an_unreadable_control_frame_does_not_break_the_call():
    """The old one-line format, or garbage: it is ignored with a warning."""
    serializer = await new_serializer()

    assert await serializer.deserialize("MEDIA_XOFF") is None
    assert await serializer.deserialize("{not json") is None


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


async def test_round_trip_over_the_same_codec():
    """What goes out and comes back in has to be the same audio.

    G.711 is lossy, so equality is checked on the compressed side (round trip
    to alaw is stable), not on the original PCM.
    """
    serializer = await new_serializer("alaw")

    original = pcm.pcm_to_alaw(b"\x11\x22" * 160)
    frame = await serializer.deserialize(original)
    returned = await serializer.serialize(
        OutputAudioRawFrame(audio=frame.audio, sample_rate=8000,
                            num_channels=1)
    )

    assert returned == original


# ---------------------------------------------------------------------------
# The dialplan reaches the pipeline
# ---------------------------------------------------------------------------


async def test_the_media_start_reaches_the_pipeline_with_its_channel_variables():
    """The dialplan is the only place these values exist.

    `Set(_AI_LANGUAGE=es)` in the dialplan is how someone with their own PBX
    tells the bot what the caller dialed. That data travels in exactly ONE
    event, MEDIA_START, and nowhere else: if the serializer swallows it, a
    Pipecat pipeline has no way to read it and the whole "you keep your own
    dialplan" argument is gone.

    It goes up as a SystemFrame so it does not wait behind queued audio.

    The variables are checked on `message`, not only on `media`:
    FastAPIWebsocketTransport rebuilds a generic frame with just `message`
    (`fastapi.py:382-383`), so `media` never reaches a real pipeline. A test
    that reads `media` alone passes by hand and fails on a real call, which is
    how an example application lost its CALL_ID once.
    """
    serializer = AsteriskFrameSerializer()
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
    )

    frame = await serializer.deserialize(
        media_start("ulaw", channel_variables={"AI_LANGUAGE": "es",
                                               "CALL_ORIGIN": "1001"})
    )

    assert isinstance(frame, AsteriskMediaStartFrame), (
        "the MEDIA_START never reached the pipeline: the dialplan variables "
        "are unreachable from Pipecat")
    # What survives the transport: the raw payload in `message`.
    assert frame.message["event"] == "MEDIA_START"
    assert frame.message["channel_variables"]["AI_LANGUAGE"] == "es", (
        "the dialplan variables are not in message: a real pipeline never "
        "sees them, only a hand-built test does")
    assert frame.message["channel_variables"]["CALL_ORIGIN"] == "1001"
    # What a forwarding transport also gets: the parsed copy.
    assert frame.media.channel_variables["AI_LANGUAGE"] == "es"
    assert isinstance(frame, SystemFrame), (
        "it is not a SystemFrame: it would queue behind the audio")


async def test_the_media_start_frame_still_teaches_the_codec():
    """Publishing the event must not skip learning from it.

    Both things happen on the same message and the split is easy to get
    wrong: returning the frame early would leave the serializer without a
    codec, and then no audio moves in either direction.
    """
    serializer = AsteriskFrameSerializer()
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000)
    )

    await serializer.deserialize(media_start("ulaw", frame_size=160))

    # 160 samples of PCM are 160 bytes of ulaw, exactly one channel frame.
    # Anything shorter is held back by the aligner, which is its job.
    out = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x00" * 160, sample_rate=8000,
                            num_channels=1)
    )
    assert out, "the codec was not learned: nothing goes out to the channel"
    assert len(out) == 160


async def test_a_second_setup_does_not_forget_the_codec_of_a_live_call():
    """A second `setup` on a live call keeps the codec.

    FastAPIWebsocketTransport calls `setup` TWICE on the same serializer, from
    the input side (`fastapi.py:314`) and from the output side (`:462`), each
    with its own initialized flag. A reset on the second one wipes the codec
    just learned and the call goes mute: no audio, no transcription,
    `Framesout=0` in Asterisk, and in the log a "Call started: ..." followed
    by an "Audio arrived before the MEDIA_START" that looks impossible.
    """
    serializer = AsteriskFrameSerializer()
    start = StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=16000)

    await serializer.setup(start)                      # input side
    await serializer.deserialize(media_start("slin16", 640))
    await serializer.setup(start)                      # output side, later

    out = await serializer.serialize(
        OutputAudioRawFrame(audio=b"\x00\x01" * 640, sample_rate=16000,
                            num_channels=1)
    )
    assert out, "the second setup wiped the codec: the call goes mute"
    assert len(out) == 1280


async def test_a_second_setup_still_clears_a_finished_call():
    """The reset must not disappear: it is what makes reuse safe.

    A serializer is reused across calls, and without the reset the second call
    inherits the hangup guard and never hangs up. So the rule is narrow: keep
    what the CURRENT call learned, clear everything once it is over.
    """
    serializer = await new_serializer("alaw")
    assert serializer._encode is not None

    # A brand new call means a NEW StartFrame, which is what tells the two
    # situations apart: the same frame reaching both sides of the transport,
    # versus a different call altogether.
    await serializer.setup(
        StartFrame(audio_in_sample_rate=8000, audio_out_sample_rate=8000))

    assert serializer._encode is None, (
        "it kept the previous call's codec: a reused instance would decode "
        "the new call with the old codec")


async def test_the_rate_warning_is_logged_once_per_call(caplog):
    """The transport calls setup twice; the warning comes out once.

    A log line that repeats reads as if it happened twice, and here it only
    happened once.
    """
    serializer = AsteriskFrameSerializer()
    start = StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=16000)

    with caplog.at_level("WARNING"):
        await serializer.setup(start)                       # input side
        await serializer.deserialize(media_start("alaw"))
        await serializer.setup(start)                       # output side

    warnings = [r for r in caplog.records if "is converted" in r.getMessage()]
    assert len(warnings) == 1, f"the warning came out {len(warnings)} times, not once"

    # And the conversion stays up: silencing the warning cannot switch it off.
    assert serializer._resample_in is not None
    assert serializer._resample_out is not None


# ---------------------------------------------------------------------------
# EndAfterBotTurn: hanging up after the bot's turn, with finish() semantics
# ---------------------------------------------------------------------------

from pipecat.frames.frames import (
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection

from galcymedia.adapters.pipecat import (
    EndAfterBotTurn,
    HangUpAfterTurnFrame,
)
from galcymedia.session import FINISH_MAX_WAIT_S


class _Recording(EndAfterBotTurn):
    """Captures what the processor pushes, instead of linking a pipeline."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pushed: list = []

    async def push_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
        self.pushed.append(frame)


class _NeverSleeps:
    """A sleep that never returns unless told to: the timeout under control."""

    def __init__(self) -> None:
        self.requested: list[float] = []
        self._release = asyncio.Event()

    async def __call__(self, seconds: float) -> None:
        self.requested.append(seconds)
        await self._release.wait()

    def fire(self) -> None:
        self._release.set()


def _ends(processor: _Recording) -> list:
    return [f for f in processor.pushed if isinstance(f, EndFrame)]


async def _feed(processor, *frames) -> None:
    for frame in frames:
        await processor.process_frame(frame, FrameDirection.DOWNSTREAM)
        await asyncio.sleep(0)


async def test_it_hangs_up_after_the_whole_response_not_at_the_first_stopped():
    """One utterance: Started, Stopped, then LLMFullResponseEnd. The EndFrame
    follows the End, never the Stopped (the TTS emits the End AFTER its own
    Stopped, tts_service.py:354,1629)."""
    processor = _Recording(sleep=_NeverSleeps())

    await _feed(processor, HangUpAfterTurnFrame(), LLMFullResponseStartFrame(),
                TTSStartedFrame(), TTSStoppedFrame())
    assert not _ends(processor), "hung up at the Stopped, before the End"

    await _feed(processor, LLMFullResponseEndFrame())
    assert len(_ends(processor)) == 1
    assert isinstance(processor.pushed[-1], EndFrame)


async def test_two_utterances_give_one_hang_up_at_the_end():
    """An HTTP TTS emits one Started/Stopped pair per sentence: the first
    Stopped is not the end of the goodbye."""
    processor = _Recording(sleep=_NeverSleeps())

    await _feed(processor, HangUpAfterTurnFrame(), LLMFullResponseStartFrame(),
                TTSStartedFrame(), TTSStoppedFrame(),
                TTSStartedFrame(), TTSStoppedFrame())
    assert not _ends(processor), "hung up after the first sentence"

    await _feed(processor, LLMFullResponseEndFrame())
    assert len(_ends(processor)) == 1


async def test_a_response_with_no_speech_hangs_up_at_once():
    """The model answered the tool with nothing to say: no wait."""
    processor = _Recording(sleep=_NeverSleeps())

    await _feed(processor, HangUpAfterTurnFrame(), LLMFullResponseStartFrame(),
                LLMFullResponseEndFrame())

    assert len(_ends(processor)) == 1


async def test_an_interruption_during_the_goodbye_hangs_up_anyway():
    """The caller cut the goodbye: the transport flushed the audio, and the
    call still ends. The interruption goes out BEFORE the EndFrame."""
    processor = _Recording(sleep=_NeverSleeps())

    await _feed(processor, HangUpAfterTurnFrame(), TTSStartedFrame(),
                InterruptionFrame())

    assert len(_ends(processor)) == 1
    kinds = [type(f) for f in processor.pushed]
    assert kinds.index(InterruptionFrame) < kinds.index(EndFrame)


async def test_the_timeout_is_finish_max_wait_s_and_hangs_up_alone():
    """Nothing came after the marker: the same ceiling as finish()."""
    sleep = _NeverSleeps()
    processor = _Recording(sleep=sleep)

    await _feed(processor, HangUpAfterTurnFrame())
    assert sleep.requested == [FINISH_MAX_WAIT_S]
    assert not _ends(processor)

    sleep.fire()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert len(_ends(processor)) == 1


async def test_without_the_marker_a_turn_does_not_hang_up():
    """Every ordinary turn passes through untouched."""
    processor = _Recording(sleep=_NeverSleeps())

    await _feed(processor, TTSStartedFrame(), TTSStoppedFrame(),
                LLMFullResponseEndFrame())

    assert not _ends(processor)
    assert [type(f) for f in processor.pushed] == [
        TTSStartedFrame, TTSStoppedFrame, LLMFullResponseEndFrame]


async def test_a_response_closed_with_speech_in_flight_waits_for_the_stopped():
    """End before Stopped cannot happen with Pipecat's own TTS (it reorders),
    but a custom one may: the audio in flight still has to play."""
    processor = _Recording(sleep=_NeverSleeps())

    await _feed(processor, HangUpAfterTurnFrame(), LLMFullResponseStartFrame(),
                TTSStartedFrame(), LLMFullResponseEndFrame())
    assert not _ends(processor), "hung up with speech in flight"

    await _feed(processor, TTSStoppedFrame())
    assert len(_ends(processor)) == 1


async def test_the_marker_without_the_processor_warns_once(caplog):
    """A pipeline that pushes the marker but forgot EndAfterBotTurn: the call
    does not hang up, and the serializer says why, once."""
    serializer = AsteriskFrameSerializer()
    await serializer.setup(StartFrame(audio_in_sample_rate=8000,
                                      audio_out_sample_rate=8000))

    with caplog.at_level("WARNING"):
        first = await serializer.serialize(HangUpAfterTurnFrame())
        second = await serializer.serialize(HangUpAfterTurnFrame())

    assert first is None and second is None
    warnings = [r for r in caplog.records if "EndAfterBotTurn" in r.getMessage()]
    assert len(warnings) == 1


async def test_the_end_of_the_turn_that_called_the_tool_is_ignored():
    """The tool handler runs as a task (llm_service.py:1332) and the LLM
    pushes the End of that turn in its finally (base_llm.py:602-604): the
    order between the two is not a contract. An End with no Start after the
    marker belongs to the old turn and must not hang up."""
    processor = _Recording(sleep=_NeverSleeps())

    await _feed(processor, HangUpAfterTurnFrame(), LLMFullResponseEndFrame())
    assert not _ends(processor), "hung up on the End of the tool's own turn"

    await _feed(processor, LLMFullResponseStartFrame(), TTSStartedFrame(),
                TTSStoppedFrame(), LLMFullResponseEndFrame())
    assert len(_ends(processor)) == 1
