"""Frame alignment: whole frames out, remainder kept, padding only at the end
of an utterance."""

from __future__ import annotations

import pytest

from galcymedia import FrameAligner


def test_delivers_only_complete_frames():
    a = FrameAligner(frame_size=160, silence_byte=0xD5)
    frames = a.push(b"\x00" * 400)
    assert len(frames) == 2
    assert all(len(f) == 160 for f in frames)
    assert a.pending_bytes == 80          # the rest waits for the next block


def test_a_block_smaller_than_the_frame_does_not_go_out_yet():
    """Here is the value of the class.

    If those 100 bytes were sent alone, the channel would pad them with 60
    bytes of silence and a micro-cut would be heard. They are kept and wait.
    """
    a = FrameAligner(frame_size=160, silence_byte=0xD5)
    assert a.push(b"\x00" * 100) == []
    assert a.pending_bytes == 100


def test_two_small_blocks_join_and_complete_a_frame():
    a = FrameAligner(frame_size=160, silence_byte=0xD5)
    a.push(b"\x00" * 100)
    frames = a.push(b"\x00" * 100)
    assert len(frames) == 1                # 100 + 100 = 200, one goes out
    assert a.pending_bytes == 40


def test_not_a_single_byte_is_lost_or_invented():
    """Central invariant: what goes in comes out, plus the final padding."""
    a = FrameAligner(frame_size=160, silence_byte=0xD5)
    input_data = [b"\x01" * n for n in (7, 333, 12, 1000, 45)]
    total_input = sum(len(c) for c in input_data)

    output = b""
    for chunk in input_data:
        for frame in a.push(chunk):
            output += frame
    output += a.flush()

    assert output[:total_input] == b"".join(input_data), "the audio changed"
    assert len(output) % 160 == 0, "the output was not aligned"
    assert len(output) - total_input < 160, "padded too much"


def test_the_final_padding_uses_the_codec_silence():
    """0xD5 is silence in alaw. Padding with zeros injects a click."""
    a = FrameAligner(frame_size=160, silence_byte=0xD5)
    a.push(b"\x01" * 150)
    remainder = a.flush()
    assert len(remainder) == 160
    assert remainder[150:] == b"\xd5" * 10


def test_the_silence_byte_comes_from_the_constructor_not_from_the_caller():
    """The padding byte comes from the channel format, not the call site.

    A per-call argument gets forgotten, and padding ulaw with the alaw byte is
    an audible click from a caller that never mentions codecs. See
    `docs/decisions.md`.
    """
    ulaw = FrameAligner(frame_size=160, silence_byte=0xFF)
    ulaw.push(b"\x01" * 150)
    assert ulaw.flush()[150:] == b"\xff" * 10, "ulaw padded with the alaw byte"

    linear = FrameAligner(frame_size=160, silence_byte=0x00)
    linear.push(b"\x01" * 150)
    assert linear.flush()[150:] == b"\x00" * 10


def test_flush_in_passthrough_returns_empty_instead_of_dividing_by_zero():
    """`flush()` is safe in passthrough, where `frame_size` is zero.

    The empty-buffer check has to come before the modulo, or a live call raises
    ZeroDivisionError. That ordering is invisible while reading, so it goes red
    here instead.
    """
    a = FrameAligner(frame_size=0, silence_byte=0xD5)
    a.push(b"\xd5" * 500)
    assert a.flush() == b""


def test_an_empty_flush_does_not_return_a_frame_of_pure_silence():
    """Without this guard, every phrase would end with extra silence."""
    a = FrameAligner(frame_size=160, silence_byte=0xD5)
    assert a.flush() == b""


def test_flush_leaves_the_aligner_clean():
    a = FrameAligner(frame_size=160, silence_byte=0xD5)
    a.push(b"\x01" * 50)
    a.flush()
    assert a.pending_bytes == 0


def test_a_negative_frame_size_raises_immediately():
    """Better to fail at construction than deliver corrupt audio on the call."""
    with pytest.raises(ValueError):
        FrameAligner(frame_size=-1, silence_byte=0xD5)


def test_a_zero_frame_size_is_passthrough_and_does_not_align():
    """Zero means passthrough, not an error: it is how the channel declares it.

    Rejecting it would force every adapter to check `optimal_frame_size` before
    building the aligner. Blocks come back as-is.
    """
    a = FrameAligner(frame_size=0, silence_byte=0xD5)

    assert a.push(b"\xd5" * 37) == [b"\xd5" * 37]
    assert a.push(b"") == []
    assert a.flush() == b"", "no buffer leaves no residue"


@pytest.mark.parametrize("frame_size", [160, 320, 480, 960])
def test_works_with_any_frame_size_the_channel_reports(frame_size):
    """optimal_frame_size changes with the codec and the ptime. 160 is not
    assumed."""
    a = FrameAligner(frame_size=frame_size, silence_byte=0xD5)
    frames = a.push(b"\x00" * (frame_size * 3 + 7))
    assert len(frames) == 3
    assert a.pending_bytes == 7


def test_passthrough_splits_blocks_larger_than_the_channel_limit():
    """Passthrough still splits on the channel limit.

    A WebSocket message over 65500 bytes makes `chan_websocket` hang up the
    call, and a provider delivering a long utterance crosses it easily. See
    `docs/protocol.md`.
    """
    from galcymedia.protocol import MAX_FRAME_SIZE

    a = FrameAligner(frame_size=0, silence_byte=0xD5)
    frames = a.push(b"\xd5" * 100_000)

    assert all(len(f) <= MAX_FRAME_SIZE for f in frames), \
        "no frame can pass the channel limit"
    assert sum(len(f) for f in frames) == 100_000, \
        "passthrough cannot lose or invent a byte"


def test_passthrough_passes_through_intact_what_fits():
    """Below the limit, passthrough keeps returning the block as-is, in a
    single frame: it does not split what does not need splitting."""
    a = FrameAligner(frame_size=0, silence_byte=0xD5)
    assert a.push(b"\xd5" * 500) == [b"\xd5" * 500]
    assert a.push(b"") == []


def test_a_whole_sentence_in_one_block_is_not_dropped():
    """Any block size comes out whole.

    A ceiling on the incoming block drops the non-streaming case (20 s of alaw
    is 160,000 bytes in one push). Backpressure is the caller's job. See
    `docs/decisions.md`.
    """
    a = FrameAligner(160, silence_byte=0xD5)
    block = b"\x01" * 200_000

    frames = a.push(block)

    assert len(frames) == 1250, "the block did not come out as whole frames"
    assert b"".join(frames) == block, "audio was lost or reordered"
    assert a.pending_bytes == 0


def test_the_buffer_never_holds_more_than_one_frame():
    """The buffer only ever keeps the remainder, because `push()` drains on
    every call.

    A ceiling here would guard a state this class cannot reach. Backpressure is
    the caller's job, see `docs/decisions.md`.
    """
    a = FrameAligner(160, silence_byte=0xD5)
    for size in (1000, 4096, 500, 161, 200_000):
        a.push(b"\x00" * size)
        assert a.pending_bytes < 160, f"the buffer grew after push({size})"
