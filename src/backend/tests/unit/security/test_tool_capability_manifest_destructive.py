"""
Unit tests for the PERFORMS_DESTRUCTIVE_OPERATIONS flag and related functions
added to tool_capability_manifest in Phase 4.
"""
import logging
import pytest
from unittest.mock import patch

from src.engines.crewai.security.tool_capability_manifest import (
    ToolCapability,
    TOOL_CAPABILITIES,
    assess_destructive_risk,
    log_destructive_warning,
)


class TestDestructiveFlag:
    def test_flag_exists(self):
        assert hasattr(ToolCapability, "PERFORMS_DESTRUCTIVE_OPERATIONS")

    def test_flag_is_distinct(self):
        d = ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS
        s = ToolCapability.READS_SENSITIVE_DATA
        u = ToolCapability.INGESTS_UNTRUSTED_CONTENT
        e = ToolCapability.EXTERNAL_COMMUNICATION
        assert not (d & s)
        assert not (d & u)
        assert not (d & e)

    def test_databricks_jobs_tool_has_destructive_flag(self):
        caps = TOOL_CAPABILITIES.get("DatabricksJobsTool", ToolCapability.NONE)
        assert caps & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS

    def test_databricks_jobs_tool_runtime_name_has_destructive_flag(self):
        caps = TOOL_CAPABILITIES.get("databricks_jobs_tool", ToolCapability.NONE)
        assert caps & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS

    def test_non_destructive_tools_lack_flag(self):
        for name in ("SerperDevTool", "ScrapeWebsiteTool", "GenieTool", "MCPTool"):
            caps = TOOL_CAPABILITIES.get(name, ToolCapability.NONE)
            assert not (caps & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS), (
                f"{name} should not have PERFORMS_DESTRUCTIVE_OPERATIONS"
            )


class TestAssessDestructiveRisk:
    def test_empty_list_returns_empty(self):
        assert assess_destructive_risk([]) == []

    def test_unknown_tool_returns_empty(self):
        assert assess_destructive_risk(["UnknownTool", "another_unknown"]) == []

    def test_detects_databricks_jobs_tool(self):
        result = assess_destructive_risk(["DatabricksJobsTool"])
        assert "DatabricksJobsTool" in result

    def test_detects_runtime_name(self):
        result = assess_destructive_risk(["databricks_jobs_tool"])
        assert "databricks_jobs_tool" in result

    def test_non_destructive_tools_excluded(self):
        result = assess_destructive_risk(["SerperDevTool", "GenieTool"])
        assert result == []

    def test_mixed_returns_only_destructive(self):
        result = assess_destructive_risk(["SerperDevTool", "DatabricksJobsTool", "MCPTool"])
        assert result == ["DatabricksJobsTool"]


class TestLogDestructiveWarning:
    def test_no_warning_when_no_destructive_tools(self, caplog):
        with caplog.at_level(logging.WARNING):
            log_destructive_warning([])
        assert "[SECURITY] Destructive" not in caplog.text

    def test_warning_logged_when_destructive_tools_present(self, caplog):
        with caplog.at_level(logging.WARNING):
            log_destructive_warning(["DatabricksJobsTool"], context="test crew")
        assert "[SECURITY] Destructive tools detected" in caplog.text
        assert "DatabricksJobsTool" in caplog.text
        assert "human_input" in caplog.text

    def test_context_appears_in_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            log_destructive_warning(["databricks_jobs_tool"], context="crew with 2 tasks")
        assert "crew with 2 tasks" in caplog.text

    def test_no_context_produces_valid_log(self, caplog):
        with caplog.at_level(logging.WARNING):
            log_destructive_warning(["DatabricksJobsTool"])
        assert "[SECURITY] Destructive tools detected" in caplog.text
