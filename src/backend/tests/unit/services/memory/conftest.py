"""
conftest.py for the memory test package.

Loaded by pytest BEFORE any test module in this directory is collected, so the
heavy packages the memory suite touches are stubbed regardless of collection
order. The stubs make these modules importable in isolation.

**Every stub here is conditional on the real package being ABSENT, and that is
load-bearing.** These stubs are process-global and never torn down: nothing
removes a ``sys.modules`` entry or a ``sys.meta_path`` finder at the end of the
session. Under xdist, one memory test poisons every later test on the same
worker.

That was harmless while ``crewai`` was not installed — a stub for an absent
package shadows nothing. It stopped being harmless when CrewAI came back as a
selectable engine: an unconditional ``crewai.rag`` finder shadowed the REAL
library, and every test that imports crewai failed with ``AttributeError:
__spec__`` — 43 of them, none of which had anything to do with memory.

So: stub only what is genuinely missing. If the real package is importable, use
it. See ``services/execution/CLAUDE.md`` — "never stub these modules into
``sys.modules`` in a test" is the same lesson, one directory over.
"""

import importlib.util
import sys
import types
from unittest.mock import MagicMock


def _is_installed(name: str) -> bool:
    """Is ``name`` a real, importable package in this environment?

    ``find_spec`` rather than ``import``: it answers without executing the
    module, so asking the question costs nothing when the answer is yes.
    """
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:  # noqa: BLE001 — a broken parent means "not usable"
        return False


# ── chromadb stub ─────────────────────────────────────────────────────────────

_chromadb_stub = MagicMock()
_chromadb_stub.Settings.return_value = MagicMock()
_chromadb_stub.PersistentClient.return_value = MagicMock()

# Only when chromadb is genuinely absent. It arrives as a crewai dependency in
# a normal install, and stubbing an installed package breaks anything that
# imports it for real — crewai included.
if not _is_installed("chromadb"):
    for _submod in [
        "chromadb",
        "chromadb.config",
        "chromadb.api",
        "chromadb.api.types",
        "chromadb.utils",
        "chromadb.utils.embedding_functions",
    ]:
        sys.modules.setdefault(_submod, _chromadb_stub)

# ── asyncpg stub ──────────────────────────────────────────────────────────────
# Conditional for the same reason as the others: a stub with no ``__spec__``
# makes ``importlib.util.find_spec("asyncpg")`` raise for everyone afterwards.
if not _is_installed("asyncpg"):
    sys.modules.setdefault("asyncpg", MagicMock())


# ── crewai.rag comprehensive stub ─────────────────────────────────────────────
# crewai.rag has many sub-modules. We install a meta_path finder that returns
# a stub module for all crewai.rag.* imports. The stub uses actual Python
# classes (not MagicMock) as placeholder types so Pydantic can handle them.

try:
    from pydantic import BaseModel as _PydanticBaseModel

    class _PlaceholderBase(_PydanticBaseModel):
        """A Pydantic-compatible placeholder for crewai.rag type stubs."""

        model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

        def __class_getitem__(cls, item):
            return cls

except Exception:

    class _PlaceholderBase:
        """Fallback placeholder when Pydantic is not available."""

        def __class_getitem__(cls, item):
            return cls


class _RagModuleStub(types.ModuleType):
    """A module that returns _PlaceholderBase for any attribute access."""

    def __getattr__(self, name):
        # Return a different class each time to avoid conflicts
        cls = type(f"_{name}", (_PlaceholderBase,), {})
        object.__setattr__(self, name, cls)
        return cls


class _RagModuleLoader:
    """Loader that creates _RagModuleStub instances."""

    def create_module(self, spec):
        mod = _RagModuleStub(spec.name)
        mod.__path__ = []
        mod.__package__ = spec.name
        mod.__spec__ = spec
        return mod

    def exec_module(self, module):
        pass  # Module already set up in create_module


class _RagFinder:
    """Meta path finder that intercepts crewai.rag.* imports."""

    @staticmethod
    def find_spec(fullname, path, target=None):
        if fullname == "crewai.rag" or fullname.startswith("crewai.rag."):
            import importlib.util

            spec = importlib.util.spec_from_loader(fullname, _RagModuleLoader())
            return spec
        return None


# Only when crewai is genuinely absent. With crewai installed this finder sits
# at the FRONT of meta_path forever and hands every `crewai.rag.*` import a stub
# instead of the real module — process-wide, for the rest of the session.
if not _is_installed("crewai"):
    if not any(isinstance(f, _RagFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _RagFinder())

    # Remove any already-imported crewai.rag modules so our finder takes over
    for _key in list(sys.modules.keys()):
        if _key == "crewai.rag" or _key.startswith("crewai.rag."):
            del sys.modules[_key]


# ── Lakebase schema self-heal cache ───────────────────────────────────────────
#
# ``lakebase_schema`` remembers which tables it has already checked, in a
# PROCESS-global set — that is the point of it, since the check sits in front of
# every memory read and write. In a test session that makes behaviour depend on
# which test ran first, and this suite runs under pytest-randomly. Clear it
# around every test so each one starts from "nothing has been checked yet".
import pytest


@pytest.fixture(autouse=True)
def _reset_lakebase_schema_cache():
    from src.services.memory.storage.lakebase_schema import reset_schema_cache

    reset_schema_cache()
    yield
    reset_schema_cache()
