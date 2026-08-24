"""
The official adapters: one module per voice provider.

Three of them (Deepgram, OpenAI Realtime, ElevenLabs) speak the provider's
raw WebSocket through `websockets` and bring no dependency of their own;
the `pipecat` adapter imports its SDK inside the module, as an optional
extra. Either way `import galcymedia` drags none of them in. Why they live
here and install apart: `docs/decisions.md`.
"""

from __future__ import annotations

# Empty on purpose: importing the adapters here would make every extra a
# requirement for using any one of them. Each is imported by its own module,
# and that is where a missing SDK fails with a message that makes sense
# (`test_importing_the_package_does_not_import_pipecat`).
__all__: list[str] = []
