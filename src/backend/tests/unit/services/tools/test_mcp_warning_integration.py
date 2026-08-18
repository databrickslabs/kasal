"""
Unit tests verifying MCPIntegration warning collection as used by
process_crew_executor and process_flow_executor.

These tests verify the warning lifecycle:
1. reset_warnings() clears prior warnings
2. Warnings accumulate during execution
3. get_warnings() returns all collected warnings
4. The warning list is included in COMPLETED result dicts
"""

import importlib.util
import sys
from types import ModuleType
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Stub out the heavy third-party modules (crewai, crewai_tools) that the
# transitive import chain pulls in, so we can import MCPIntegration without
# actually having crewai installed.
#
# MagicMock with __path__ set behaves like a package, allowing Python to
# resolve sub-module attribute access (e.g. crewai.llms.providers...).
# ---------------------------------------------------------------------------
_crewai_mock = MagicMock()
_crewai_mock.__path__ = []  # Mark it as a package so sub-imports work


def _is_importable(name: str) -> bool:
    """Is this a real, importable module here?"""
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:  # noqa: BLE001 — a broken parent means "not usable"
        return False


# Stub ONLY what is genuinely missing, and remove whatever we installed.
#
# Almost everything below is importable today: `crewai` is a real dependency
# again (the second execution engine), and the `src.*` entries are first-party
# code. These stubs went in at IMPORT time and were never removed, so they
# shadowed the real modules for the rest of the xdist worker — which broke 43
# tests in `services/execution/harnesses/crewai/`, nowhere near this file, with
# `AttributeError: __spec__`.
#
# See `services/execution/CLAUDE.md`: "Never stub these modules into
# `sys.modules` in a test."
_STUBS: dict[str, object] = {}
for _mod_name in [
    "crewai",
    "src.core.llm.transport",
    "crewai.llms",
    "crewai.llms.providers",
    "crewai.llms.providers.openai",
    "src.services.tools.base",
    "src.core.events",
    "crewai.events.types",
    "crewai.flow",
    "src.services.flow_builder.runtime",
    "crewai.utilities",
    "crewai.utilities.exceptions",
    "kasal_engine.utils",
    "crewai.utilities.converter",
    "crewai.utilities.evaluators",
    "crewai.utilities.evaluators.task_evaluator",
    "crewai.project",
    "src.services.memory.engine",
    "crewai.memory.storage",
    "crewai.memory.storage.rag_storage",
    "crewai.memory.storage.ltm_sqlite_storage",
    "crewai.tasks",
    "src.services.execution.runtime",
]:
    if _mod_name not in sys.modules and not _is_importable(_mod_name):
        _mock = MagicMock()
        _mock.__path__ = []
        sys.modules[_mod_name] = _mock
        _STUBS[_mod_name] = _mock

import pytest

from src.services.tools.mcp_integration import MCPIntegration

# The stubs existed only to get that import through. Leaving them installed is
# what made this file everyone else's problem.
for _mod_name in _STUBS:
    sys.modules.pop(_mod_name, None)


class TestMCPWarningLifecycle:
    """Test the warning lifecycle as used in process executors."""

    def setup_method(self):
        MCPIntegration.reset_warnings()

    def test_reset_clears_existing_warnings(self):
        """reset_warnings should clear any previously accumulated warnings."""
        MCPIntegration.add_warning("old warning 1")
        MCPIntegration.add_warning("old warning 2")
        assert len(MCPIntegration.get_warnings()) == 2

        MCPIntegration.reset_warnings()
        assert MCPIntegration.get_warnings() == []

    def test_warnings_accumulate_during_execution(self):
        """Warnings should accumulate as MCP servers fail."""
        MCPIntegration.add_warning("MCP server 'tavily': 403 Forbidden")
        MCPIntegration.add_warning("MCP server 'gmail': timeout")

        warnings = MCPIntegration.get_warnings()
        assert len(warnings) == 2
        assert "tavily" in warnings[0]
        assert "gmail" in warnings[1]

    def test_get_warnings_returns_copy(self):
        """get_warnings should return a copy so external mutation doesn't affect internal state."""
        MCPIntegration.add_warning("warning1")
        copy = MCPIntegration.get_warnings()
        copy.clear()
        assert len(MCPIntegration.get_warnings()) == 1

    def test_completed_result_dict_structure(self):
        """Verify the result dict structure matches what process executors return."""
        MCPIntegration.reset_warnings()
        MCPIntegration.add_warning("MCP server 'test': connection failed")

        # Simulate what process_crew_executor does
        mcp_warnings = MCPIntegration.get_warnings()
        result = {
            "status": "COMPLETED",
            "execution_id": "test-123",
            "result": "some result",
            "process_id": 12345,
            "warnings": mcp_warnings,
        }

        assert result["warnings"] == ["MCP server 'test': connection failed"]
        assert result["status"] == "COMPLETED"

    def test_no_warnings_produces_empty_list(self):
        """When no MCP errors occur, warnings should be empty list."""
        MCPIntegration.reset_warnings()
        mcp_warnings = MCPIntegration.get_warnings()

        result = {
            "status": "COMPLETED",
            "execution_id": "test-456",
            "result": "success",
            "process_id": 12345,
            "warnings": mcp_warnings,
        }

        assert result["warnings"] == []

    def test_warning_message_format(self):
        """Warnings should be joinable with '; ' for the UI message."""
        MCPIntegration.add_warning("warn1")
        MCPIntegration.add_warning("warn2")

        warnings = MCPIntegration.get_warnings()
        if warnings:
            message = "Execution completed with warnings: " + "; ".join(warnings)
        else:
            message = "Execution completed successfully"

        assert "warn1; warn2" in message
