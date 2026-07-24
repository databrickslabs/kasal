"""
conftest.py for the memory test package.

This file is loaded by pytest BEFORE any test modules in this directory are
collected, ensuring that chromadb and related heavy packages are stubbed out
regardless of test collection order.

The stubs here make all memory test modules importable in isolation.
"""

import sys
import types
from unittest.mock import MagicMock


# ── chromadb stub ─────────────────────────────────────────────────────────────

_chromadb_stub = MagicMock()
_chromadb_stub.Settings.return_value = MagicMock()
_chromadb_stub.PersistentClient.return_value = MagicMock()

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


# Install the finder at the FRONT of meta_path so it takes priority
if not any(isinstance(f, _RagFinder) for f in sys.meta_path):
    sys.meta_path.insert(0, _RagFinder())

# Remove any already-imported crewai.rag modules so our finder takes over
for _key in list(sys.modules.keys()):
    if _key == "crewai.rag" or _key.startswith("crewai.rag."):
        del sys.modules[_key]
