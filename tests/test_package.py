"""
The package surface: what `from galcymedia import ...` promises.

`__init__.py` only re-exports, so what can go wrong is drift: a name in
`__all__` that no longer exists, a module renamed in its own pass while the
package still remembers the old name, a version bumped on one side only.
"""

from __future__ import annotations

import importlib
import inspect
import re
import subprocess
import sys
from pathlib import Path

import galcymedia


def test_every_name_in_all_resolves():
    missing = [name for name in galcymedia.__all__ if not hasattr(galcymedia, name)]
    assert not missing, missing


def test_every_exported_object_is_what_its_module_exports_today():
    """Compared against the defining module, not against what `__init__`
    remembers: a rename in `events.py` or `session.py` during their own pass
    shows up here, even when a stale alias would still import."""
    submodules = [m for m in vars(galcymedia).values()
                  if inspect.ismodule(m) and m.__name__.startswith("galcymedia.")]
    for name in galcymedia.__all__:
        obj = getattr(galcymedia, name)
        if inspect.ismodule(obj) or name == "__version__":
            continue
        if not hasattr(obj, "__module__"):
            # A plain constant: some submodule has to export it under the
            # same name with the same value.
            owners = [m.__name__ for m in submodules
                      if getattr(m, name, None) == obj]
            assert owners, f"no submodule exports {name} = {obj!r} today"
            continue
        module = importlib.import_module(obj.__module__)
        own_name = getattr(obj, "__name__", name)
        assert getattr(module, own_name, None) is obj, (
            f"{name} points at {obj!r}, but {module.__name__} no longer "
            f"exports it as {own_name!r}")
        assert module.__name__.startswith("galcymedia."), (
            f"{name} is exported from outside the package: {module.__name__}")


def test_the_version_matches_pyproject():
    """Nobody else checks it: a bump on one side only would ship quietly."""
    pyproject = Path(galcymedia.__file__).parents[2] / "pyproject.toml"
    declared = re.search(r'^version = "([^"]+)"', pyproject.read_text(encoding="utf-8"),
                         re.MULTILINE)
    assert declared is not None
    assert galcymedia.__version__ == declared.group(1)


def test_importing_the_package_does_not_import_pipecat():
    """The promise of `adapters/__init__`: no extra is dragged in by
    `import galcymedia` or `import galcymedia.adapters`. Checked in a fresh
    interpreter, since this one may already have pipecat loaded."""
    code = (
        "import sys, galcymedia, galcymedia.adapters; "
        "print(sorted(m for m in sys.modules if m.split('.')[0] in "
        "('pipecat', 'deepgram', 'openai', 'elevenlabs')))"
    )
    out = subprocess.run([sys.executable, "-c", code],
                         capture_output=True, text=True, check=True).stdout
    assert out.strip() == "[]", out
