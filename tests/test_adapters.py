"""
The rule of the adapters package.

A single invariant, and it is the one that holds the whole design together: the
adapter knows the core, the core NEVER knows the adapter. If someone reverses
that arrow, `pip install galcymedia` starts dragging in the SDK of the moment
and the library expires with it.

It is worth a test because the failure is silent: an extra import in the wrong
place breaks no test, it just makes the install heavier until one day the SDK
does not install on the client's machine.
"""

from __future__ import annotations

import subprocess
import sys

# The packages an adapter may import and the core may not.
PROVIDER_SDKS = ("pipecat", "openai", "deepgram", "elevenlabs")


def _sdks_loaded_after(import_line: str) -> list[str]:
    """Provider SDKs left loaded after an import, measured cleanly.

    Runs in a fresh process on purpose: in the tests' own process, another
    test may already have imported the SDK, and the result would be a false
    negative.
    """
    source = (
        "import sys\n"
        f"{import_line}\n"
        f"sdks = {PROVIDER_SDKS!r}\n"
        "print(','.join(sorted({m.split('.')[0] for m in sys.modules "
        "if m.split('.')[0] in sdks})))"
    )
    output = subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    return [name for name in output.split(",") if name]


def test_the_core_drags_in_no_provider():
    """`import galcymedia` installs and loads only websockets."""
    assert _sdks_loaded_after("import galcymedia") == []


def test_the_adapters_package_drags_in_nothing_either():
    """Importing the package must not load ALL the SDKs.

    That is why `adapters/__init__.py` is empty of imports: if it re-exported
    the adapters, using one would require having all four installed.
    """
    assert _sdks_loaded_after("import galcymedia.adapters") == []


def test_importing_pipecat_without_its_extra_says_how_to_install_it():
    """Without the Pipecat SDK, the import has to explain what to do.

    The bare ModuleNotFoundError points at an internal line of the adapter and
    does not say how to fix it; in a live demo that confuses. The absence is
    simulated by blocking the pipecat import in a fresh process.
    """
    source = (
        "import builtins\n"
        "_real = builtins.__import__\n"
        "def fake(name, *a, **k):\n"
        "    if name.split('.')[0] == 'pipecat':\n"
        "        raise ModuleNotFoundError(\"No module named 'pipecat'\")\n"
        "    return _real(name, *a, **k)\n"
        "builtins.__import__ = fake\n"
        "try:\n"
        "    from galcymedia.adapters import pipecat\n"
        "    print('SIN_ERROR')\n"
        "except ImportError as e:\n"
        "    print('MENSAJE:', 'galcymedia[pipecat]' in str(e))\n"
    )
    output = subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    assert output == "MENSAJE: True"


def test_the_doubles_keep_up_with_what_the_adapters_really_use():
    """A double that has fallen behind the real contract is a test that passes
    while production breaks.

    It already happened: `FakeSpeech` did not expose `played_ms_at_most`, and
    the adapter blew up with AttributeError the moment it was used for real.
    The test suite was green the whole time, because no double had ever been
    asked for it.

    This does NOT demand that the double implement the whole class, which would
    be busywork: only what the adapters actually touch.
    """
    import inspect
    import re
    from pathlib import Path

    from galcymedia.speech import SpeechState

    from conftest import FakeSession, FakeSpeech

    adapters = Path(__file__).parent.parent / "src" / "galcymedia" / "adapters"
    real_speech = {n for n, _ in inspect.getmembers(SpeechState)
                   if not n.startswith("_")}

    used: set[str] = set()
    for f in adapters.glob("*.py"):
        text = f.read_text(encoding="utf-8")
        used.update(re.findall(r"\bself\.speech\.(\w+)", text))

    # Only what is really part of the class: the regex also catches names from
    # comments and from other objects.
    # Se inspecciona una INSTANCIA y no la clase: la mitad de lo que expone el
    # doble son atributos puestos en __init__, y sobre la clase no se ven.
    wanted = used & real_speech
    missing = {n for n in wanted if not hasattr(FakeSpeech(), n)}
    assert not missing, (
        f"the adapters use {sorted(missing)} and the double does not have it: "
        "the test would pass while the real call breaks")

    # And the session double carries the channel format, which every adapter
    # reads to resolve its codec.
    assert hasattr(FakeSession("alaw"), "media")


def test_the_speech_double_keeps_the_real_signatures():
    """Matching names is not enough: a double whose signature has drifted lets
    through a call the real class would reject."""
    import inspect

    from galcymedia.speech import SpeechState

    from conftest import FakeSpeech

    for name in ("play", "interrupt", "end_turn", "resume",
                 "note_caller_activity"):
        real = inspect.signature(getattr(SpeechState, name))
        fake = inspect.signature(getattr(FakeSpeech, name))
        assert str(real) == str(fake), (
            f"{name}: the double says {fake} and the real one says {real}")
