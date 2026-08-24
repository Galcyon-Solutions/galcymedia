"""Tests of the G.711 conversion.

These codecs are hand-written because `audioop` left the standard library in
Python 3.13 (PEP 594). An error here raises no exception: the call simply
sounds bad, and that is about the most expensive thing to diagnose. So the
tests pin fixed vectors and invariants, and compare every value against the
reference (`audioop`, stdlib or the `audioop-lts` package) when it is
importable.
"""

from __future__ import annotations

import struct

import pytest

from galcymedia import pcm

# ---------------------------------------------------------------------------
# Known vectors
# ---------------------------------------------------------------------------


def test_the_silence_of_each_codec():
    """0xD5 in alaw (decodes to +8, half a step) and 0xFF in ulaw (0).

    The zero byte is not silence: it decodes to -5504 in alaw and -32124 in
    ulaw, which is the click heard when padding with zeros.
    """
    assert pcm.ALAW_SILENCE == 0xD5
    assert pcm.ULAW_SILENCE == 0xFF
    # Within half a step of segment 0 (16 in alaw).
    assert abs(struct.unpack("<h", pcm.alaw_to_pcm(b"\xd5"))[0]) <= 8
    assert abs(struct.unpack("<h", pcm.ulaw_to_pcm(b"\xff"))[0]) <= 8


@pytest.mark.parametrize("sample,expected", [
    (0, 0xD5),
    (-1, 0x55),
    (32767, 0xAA),
    (-32768, 0x2A),
])
def test_alaw_vectors(sample, expected):
    assert pcm.pcm_to_alaw(struct.pack("<h", sample))[0] == expected


@pytest.mark.parametrize("sample,expected", [
    (0, 0xFF),
    (-1, 0x7E),
    (32767, 0x80),
    (-32768, 0x00),
])
def test_ulaw_vectors(sample, expected):
    """-1 -> 0x7E pins the shift order: bias after the >>2, not before.

    Adding the bias before the shift gives 0x7F, which looks reasonable and
    is wrong (`_audioop.c:184`).
    """
    assert pcm.pcm_to_ulaw(struct.pack("<h", sample))[0] == expected


# ---------------------------------------------------------------------------
# Invariants over the whole range
# ---------------------------------------------------------------------------


def test_every_byte_decodes_within_range():
    """No value can fall outside signed 16 bits."""
    for byte in range(256):
        for decode in (pcm.alaw_to_pcm, pcm.ulaw_to_pcm):
            value = struct.unpack("<h", decode(bytes([byte])))[0]
            assert -32768 <= value <= 32767


@pytest.mark.parametrize("encode,decode", [
    (pcm.pcm_to_alaw, pcm.alaw_to_pcm),
    (pcm.pcm_to_ulaw, pcm.ulaw_to_pcm),
])
def test_round_trip_preserves_the_waveform_shape(encode, decode):
    """G.711 loses precision on purpose: 16 bits in, 8 bits out.

    What it must not do is change the sign, which is heard as distortion.
    Measured over all 65,536 samples, the relative error tops at 3.7% (ulaw)
    for |x| >= 915; the 7% below is that ceiling with margin.
    """
    original = [0, 1000, 5000, 20000, -1000, -5000, -20000]
    samples = struct.pack(f"<{len(original)}h", *original)

    recovered = struct.unpack(f"<{len(original)}h", decode(encode(samples)))

    for input_, out in zip(original, recovered):
        if input_ != 0:
            assert (input_ > 0) == (out > 0), "the sign changed"
        # Ceiling with margin over the measured 3.7%.
        assert abs(out - input_) <= max(64, abs(input_) * 0.07)


@pytest.mark.parametrize("encode", [pcm.pcm_to_alaw, pcm.pcm_to_ulaw])
def test_each_16_bit_sample_yields_exactly_one_byte(encode):
    assert len(encode(b"\x00\x10" * 80)) == 80


@pytest.mark.parametrize("encode", [pcm.pcm_to_alaw, pcm.pcm_to_ulaw])
def test_an_odd_number_of_bytes_does_not_blow_up(encode):
    """Half a sample cannot be converted: it is dropped, not guessed."""
    assert len(encode(b"\x00\x10\x00")) == 1


@pytest.mark.parametrize("encode", [pcm.pcm_to_alaw, pcm.pcm_to_ulaw])
def test_empty_input(encode):
    assert encode(b"") == b""


def test_a_complete_telephony_frame():
    """160 bytes of codec are 20 ms, one channel frame.

    `chan_websocket.c:1430`: optimal_frame_size = 20 ms x 80 B / 10 ms.
    """
    samples = b"\x00\x10" * 160          # 160 samples of 16 bits
    assert len(pcm.pcm_to_alaw(samples)) == 160
    assert len(pcm.alaw_to_pcm(pcm.pcm_to_alaw(samples))) == 320


# ---------------------------------------------------------------------------
# Against the reference implementation, if available
# ---------------------------------------------------------------------------


def test_matches_the_reference_exactly():
    """Exhaustive comparison against audioop: all 65,536 samples, exact.

    This is the only net for a wrong segment table or shift order. It uses
    whichever `audioop` is importable: the stdlib one below Python 3.13,
    the `audioop-lts` package (in the `dev` extra) from 3.13 on. With
    neither, it skips and says so.
    """
    audioop = pytest.importorskip(
        "audioop",
        reason="no audioop: stdlib below 3.13, or install the dev extra "
               "(audioop-lts) on 3.13+; without it the byte-for-byte "
               "check against the reference does not run",
    )

    diffs = 0
    for value in range(-32768, 32768):
        sample = struct.pack("<h", value)
        if pcm.pcm_to_alaw(sample) != audioop.lin2alaw(sample, 2):
            diffs += 1
        if pcm.pcm_to_ulaw(sample) != audioop.lin2ulaw(sample, 2):
            diffs += 1

    for byte in range(256):
        raw = bytes([byte])
        if pcm.alaw_to_pcm(raw) != audioop.alaw2lin(raw, 2):
            diffs += 1
        if pcm.ulaw_to_pcm(raw) != audioop.ulaw2lin(raw, 2):
            diffs += 1

    assert diffs == 0, f"{diffs} diffs against the reference"


def test_encoding_on_a_big_endian_host_matches_the_reference(monkeypatch):
    """`array("h")` reads native order; on a big-endian host it must swap.

    Simulated by forcing `_SWAP` and feeding samples with their bytes
    reversed: the codes must equal the reference's for the same samples.
    """
    audioop = pytest.importorskip(
        "audioop", reason="no audioop: the reference is needed for this check"
    )
    monkeypatch.setattr(pcm, "_SWAP", True)
    values = [0, -1, 1000, -1000, 32767, -32768, 0x1234, -0x1234]
    native = struct.pack(f"<{len(values)}h", *values)
    as_big_endian = struct.pack(f">{len(values)}h", *values)

    assert pcm.pcm_to_alaw(as_big_endian) == audioop.lin2alaw(native, 2)
    assert pcm.pcm_to_ulaw(as_big_endian) == audioop.lin2ulaw(native, 2)


def test_the_direct_transcoding_tables():
    """alaw <-> ulaw in a single bytes.translate pass.

    The two silences are not the same PCM value (+8 and 0, half an alaw
    step), so the translated silence must stay within that, and no code may
    change sign or degrade beyond the G.711 error.
    """
    quiet = bytes([pcm.ALAW_SILENCE]).translate(pcm.ALAW_TO_ULAW)
    assert abs(struct.unpack("<h", pcm.ulaw_to_pcm(quiet))[0]) <= 8
    quiet = bytes([pcm.ULAW_SILENCE]).translate(pcm.ULAW_TO_ALAW)
    assert abs(struct.unpack("<h", pcm.alaw_to_pcm(quiet))[0]) <= 8

    directions = [
        (pcm.ALAW_TO_ULAW, pcm.alaw_to_pcm, pcm.ulaw_to_pcm),
        (pcm.ULAW_TO_ALAW, pcm.ulaw_to_pcm, pcm.alaw_to_pcm),
    ]
    for table, decode_in, decode_out in directions:
        for byte in range(256):
            before = struct.unpack("<h", decode_in(bytes([byte])))[0]
            translated = bytes([byte]).translate(table)
            after = struct.unpack("<h", decode_out(translated))[0]
            if abs(before) > 8:
                assert (before > 0) == (after > 0), "the sign changed"
            assert abs(after - before) <= max(64, abs(before) * 0.07)
