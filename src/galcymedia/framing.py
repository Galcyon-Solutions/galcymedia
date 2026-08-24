"""Frame alignment for the Asterisk media channel.

The channel DROPS the tail of any message that is not a whole number of
`optimal_frame_size` frames (160 bytes for alaw or ulaw at 20 ms): it queues
whole frames only and clears the rest (Asterisk 23.4.1,
`chan_websocket.c:1031-1044`; only the bulk mode of `START_MEDIA_BUFFERING`
keeps it, `:1047`), and the timer slot it would have filled goes out empty
(`:610`). master changes this from `5eded44fee` (24.0.0-pre1): the timer
flushes the remainder. Text-to-speech engines
deliver whatever block size suits them. Sent unchanged, every block loses
its tail, and the result is choppy speech with no packet loss anywhere,
miserable to debug because every metric looks clean.

This class buffers instead, releasing only whole frames. Use it directly only
when there are no turns to interrupt (an echo, a recording); `session.speech`
already wraps it for a conversational agent.
"""

from __future__ import annotations

from .protocol import MAX_FRAME_SIZE


class FrameAligner:
    """Buffers audio and releases it as whole frames.

    Both arguments come from the same `MediaStart`, so take them together:

        aligner = FrameAligner(media.optimal_frame_size, media.silence_byte)
        for chunk in tts_audio:
            for frame in aligner.push(chunk):
                await session.send_audio(frame)
        tail = aligner.flush()
        if tail:
            await session.send_audio(tail)
    """

    def __init__(self, frame_size: int, silence_byte: int) -> None:
        """Prepares the aligner for one call's audio format.

        Args:
            frame_size: Bytes per channel frame, straight from `MediaStart`.
                Zero means passthrough; nothing to align.
            silence_byte: Padding byte for the channel codec. `MediaStart`
                derives it from the format (`MediaStart.silence_byte`): the
                channel sends the format, not the byte.

        Raises:
            ValueError: If `frame_size` is negative.
        """
        if frame_size < 0:
            raise ValueError(f"frame_size cannot be negative, got {frame_size}")

        self._passthrough = frame_size == 0
        self._frame_size = frame_size
        self._silence_byte = silence_byte
        self._buffer = bytearray()

    def push(self, chunk: bytes) -> list[bytes]:
        """Adds audio and returns the whole frames that are ready to send.

        Takes blocks of any size: a non-streaming TTS returning a whole
        sentence at once is split here like any other. The buffer never holds
        more than one frame minus a byte, so backpressure is the caller's job.
        `Session.send_audio` reports a dropped frame by returning False.

        Args:
            chunk: Audio from the provider, any size.

        Returns:
            Whole frames, in order. Empty while the buffer is still filling.
        """
        if self._passthrough:
            if not chunk:
                return []
            # Passthrough still honors the per-message ceiling: going over it
            # hangs the call (docs/protocol.md). Splitting on that boundary
            # does not alter the audio.
            if len(chunk) <= MAX_FRAME_SIZE:
                return [chunk]
            return [chunk[i:i + MAX_FRAME_SIZE]
                    for i in range(0, len(chunk), MAX_FRAME_SIZE)]

        if chunk:
            self._buffer.extend(chunk)

        frames: list[bytes] = []
        while len(self._buffer) >= self._frame_size:
            frames.append(bytes(self._buffer[: self._frame_size]))
            del self._buffer[: self._frame_size]
        return frames

    def flush(self) -> bytes:
        """Releases what is left, padded up to a whole frame.

        Call this at the END of an utterance, not between blocks: padding
        mid-audio is the whole problem this class is for. Padding once at the
        end is correct, and the alternative is dropping the last milliseconds
        of speech.

        Returns:
            The padded remainder, or empty if there was nothing buffered.
        """
        # The empty check also guards passthrough, where `_frame_size` is zero
        # and the modulo below would raise. Reordering these two breaks it.
        if not self._buffer:
            return b""

        remainder = len(self._buffer) % self._frame_size
        if remainder:
            self._buffer.extend(
                bytes([self._silence_byte]) * (self._frame_size - remainder))

        out = bytes(self._buffer)
        self._buffer.clear()
        return out

    @property
    def pending_bytes(self) -> int:
        """Bytes waiting to complete a frame."""
        return len(self._buffer)
