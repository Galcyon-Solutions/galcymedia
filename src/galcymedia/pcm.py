"""The bridge to PCM, for providers that do not speak telephony.

Three adapters use it, each for a different thing: `adapters/pipecat.py`
converts every frame to and from linear PCM, `adapters/elevenlabs.py`
transcodes alaw <-> ulaw with the 256-byte tables, and `adapters/deepgram.py`
only takes the silence bytes. Why the module is called `pcm` and not `g711`,
and why it is hand-written instead of `audioop`: `docs/decisions.md`.

Every value matches the reference implementation byte for byte, all 65,536
samples and all 256 codes (`test_matches_the_reference_exactly`). The
reference is CPython's `audioop`, as published in `audioop-lts` 0.2.2.

Cost, measured per 20 ms frame: decoding is a 256-entry lookup (7 us),
encoding a 65,536-entry lookup (9 us). A PCM provider converts both ways at
50 frames per second, so 100 concurrent calls spend 8% of one core here.
The encode tables cost 0.2 s at import.
"""

from __future__ import annotations

import struct
import sys
from array import array

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


def _build_alaw_to_linear() -> list[int]:
    """The 256 alaw codes as signed 16-bit PCM."""
    table = []
    for byte in range(256):
        # alaw inverts the even bits on the wire, not the sign bit: bit 7 is
        # 1 for a positive sample, the opposite of ulaw (`_audioop.c:272`).
        value = byte ^ 0x55
        sign = value & 0x80
        exponent = (value & 0x70) >> 4
        mantissa = value & 0x0F

        if exponent == 0:
            sample = (mantissa << 4) + 8
        else:
            sample = ((mantissa << 4) + 0x108) << (exponent - 1)

        table.append(-sample if sign == 0 else sample)
    return table


def _build_ulaw_to_linear() -> list[int]:
    """The 256 ulaw codes as signed 16-bit PCM."""
    table = []
    for byte in range(256):
        value = ~byte & 0xFF
        sign = value & 0x80
        exponent = (value & 0x70) >> 4
        mantissa = value & 0x0F

        sample = ((mantissa << 3) + 0x84) << exponent
        sample -= 0x84

        table.append(-sample if sign else sample)
    return table


ALAW_TO_LINEAR = _build_alaw_to_linear()
ULAW_TO_LINEAR = _build_ulaw_to_linear()

# The silence of each codec. It is not the zero byte: 0x00 decodes to -5504 in
# alaw and -32124 in ulaw, so padding with zeros is an audible click at every
# edge (`test_the_silence_of_each_codec`).
ALAW_SILENCE = 0xD5
ULAW_SILENCE = 0xFF

# The two codecs use DIFFERENT segment tables (`_audioop.c:73-77`). Mixing them
# up raises nothing: the conversion "works" and 63,220 of the 65,536 samples
# come out wrong, as distortion. `test_matches_the_reference_exactly` catches
# it.
_ALAW_SEG_END = [0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF]
_ULAW_SEG_END = [0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF]

# ulaw clip, already on the 14-bit scale the algorithm works in: CLIP 32635 on
# 16 bits (`_audioop.c:67`), and the caller shifts by 2 first (`:1545`).
CLIP_ULAW = 32635 >> 2


def _find_segment(value: int, table: list[int]) -> int:
    """Index of the first segment whose end is >= value; 8 past the last."""
    for i, fin in enumerate(table):
        if value <= fin:
            return i
    return 8


# ---------------------------------------------------------------------------
# From telephony to PCM
# ---------------------------------------------------------------------------


def alaw_to_pcm(data: bytes) -> bytes:
    """8-bit alaw to 16-bit linear PCM, little endian.

    Args:
        data: alaw bytes, any length.

    Returns:
        Two bytes per input byte, in order.
    """
    return struct.pack(f"<{len(data)}h", *[ALAW_TO_LINEAR[b] for b in data])


def ulaw_to_pcm(data: bytes) -> bytes:
    """8-bit ulaw to 16-bit linear PCM, little endian.

    Args:
        data: ulaw bytes, any length.

    Returns:
        Two bytes per input byte, in order.
    """
    return struct.pack(f"<{len(data)}h", *[ULAW_TO_LINEAR[b] for b in data])


# ---------------------------------------------------------------------------
# From PCM to telephony
# ---------------------------------------------------------------------------


def _alaw_code(sample: int) -> int:
    """One signed 16-bit sample to its alaw code (`_audioop.c:264`)."""
    if sample >= 0:
        sign = 0xD5
    else:
        sign = 0x55
        sample = -sample - 1

    # No clip here: after `-sample - 1` the magnitude is at most 32767,
    # and the reference has none either (`_audioop.c:264-279`).
    sample >>= 3
    segment = _find_segment(sample, _ALAW_SEG_END)

    if segment >= 8:
        return 0x7F ^ sign

    if segment < 2:
        compressed = (sample >> 1) & 0x0F
    else:
        compressed = (sample >> segment) & 0x0F

    return (segment << 4 | compressed) ^ sign


def _ulaw_code(sample: int) -> int:
    """One signed 16-bit sample to its ulaw code (`_audioop.c:169`)."""
    # The order matters: the reference works on 14 bits, so it shifts the
    # sample FIRST (`_audioop.c:1545`) and adds the reduced bias after,
    # BIAS>>2 = 0x21 (`:184`). Adding the full bias before the shift gives
    # a close, wrong result that raises nothing (`test_ulaw_vectors`).
    sample >>= 2

    if sample < 0:
        sample = -sample
        sign = 0x7F
    else:
        sign = 0xFF

    sample = min(sample, CLIP_ULAW)

    sample += 0x21
    segment = _find_segment(sample, _ULAW_SEG_END)

    if segment >= 8:
        return 0x7F ^ sign

    compressed = (segment << 4) | ((sample >> (segment + 1)) & 0x0F)
    return compressed ^ sign


def _build_pcm_to_code_table(code) -> bytes:
    """All 65,536 samples to their code, indexed by `sample & 0xFFFF`.

    A negative sample indexes the table directly: Python's negative index
    lands on the same entry as the two's complement of the sample.
    """
    table = bytearray(65536)
    for sample in range(-32768, 32768):
        table[sample & 0xFFFF] = code(sample)
    return bytes(table)


# Built at import, 0.2 s for both. Encoding a frame is then one C-level map
# over the table instead of a Python loop per sample: 8 us instead of 101 us
# per 20 ms frame, same bytes (`test_matches_the_reference_exactly`).
_PCM_TO_ALAW = _build_pcm_to_code_table(_alaw_code)
_PCM_TO_ULAW = _build_pcm_to_code_table(_ulaw_code)

# `array("h")` reads native byte order; the wire format is little endian.
_SWAP = sys.byteorder != "little"


def _encode(data: bytes, table: bytes) -> bytes:
    samples = array("h")
    samples.frombytes(data[: len(data) // 2 * 2])
    if _SWAP:
        samples.byteswap()
    return bytes(map(table.__getitem__, samples))


def pcm_to_alaw(data: bytes) -> bytes:
    """16-bit linear PCM, little endian, to alaw.

    Args:
        data: PCM bytes. A trailing odd byte is dropped, not guessed
            (`test_an_odd_number_of_bytes_does_not_blow_up`).

    Returns:
        One alaw byte per whole sample.
    """
    return _encode(data, _PCM_TO_ALAW)


def pcm_to_ulaw(data: bytes) -> bytes:
    """16-bit linear PCM, little endian, to ulaw.

    Args:
        data: PCM bytes. A trailing odd byte is dropped, not guessed
            (`test_an_odd_number_of_bytes_does_not_blow_up`).

    Returns:
        One ulaw byte per whole sample.
    """
    return _encode(data, _PCM_TO_ULAW)


# ---------------------------------------------------------------------------
# Direct transcoding between the two G.711 codecs
# ---------------------------------------------------------------------------
#
# 256-byte tables built once at import, through linear PCM. Converting a frame
# is one `bytes.translate` in C (0.2 us per 20 ms frame). For a channel that
# speaks one G.711 and a provider that only takes the other
# (`adapters/elevenlabs.py`).

def _build_transcode_table(decode_table: list[int], encode) -> bytes:
    """One 256-byte translate table: decode each code, re-encode it."""
    return bytes(
        encode(struct.pack("<h", decode_table[b]))[0] for b in range(256)
    )


ALAW_TO_ULAW = _build_transcode_table(ALAW_TO_LINEAR, pcm_to_ulaw)
ULAW_TO_ALAW = _build_transcode_table(ULAW_TO_LINEAR, pcm_to_alaw)
