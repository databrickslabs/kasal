"""
Unit tests for tool capability manifest and lethal-trifecta detection.
"""
import logging

from src.engines.crewai.security.tool_capability_manifest import (
    TrifectaAssessment,
    ToolCapability,
    TOOL_CAPABILITIES,
    assess_trifecta,
    log_trifecta_warning,
)


class TestAssessTrifecta:
    def test_empty_tool_list(self):
        result = assess_trifecta([])
        assert not result.has_trifecta
        assert not result.reads_sensitive
        assert not result.ingests_untrusted
        assert not result.communicates_externally

    def test_no_trifecta_web_tools_only(self):
        # Web tools have INGESTS_UNTRUSTED_CONTENT + EXTERNAL — missing READS_SENSITIVE
        result = assess_trifecta(["SerperDevTool", "ScrapeWebsiteTool"])
        assert not result.has_trifecta
        assert not result.reads_sensitive
        assert result.ingests_untrusted
        assert result.communicates_externally

    def test_no_trifecta_db_tools_only(self):
        # DB tools have READS_SENSITIVE + EXTERNAL — missing INGESTS_UNTRUSTED
        result = assess_trifecta(["GenieTool", "DatabricksJobsTool"])
        assert not result.has_trifecta
        assert result.reads_sensitive
        assert not result.ingests_untrusted
        assert result.communicates_externally

    def test_no_trifecta_external_only(self):
        # Tool with only EXTERNAL (no sensitive, no untrusted)
        result = assess_trifecta(["Dall-E Tool"])
        assert not result.has_trifecta
        assert not result.reads_sensitive
        assert not result.ingests_untrusted
        assert result.communicates_externally

    def test_trifecta_detected_genie_plus_serper(self):
        # GenieTool: sensitive + external; SerperDevTool: untrusted + external → trifecta
        result = assess_trifecta(["GenieTool", "SerperDevTool"])
        assert result.has_trifecta
        assert result.reads_sensitive
        assert result.ingests_untrusted
        assert result.communicates_externally

    def test_trifecta_detected_knowledge_plus_scrape(self):
        result = assess_trifecta(["DatabricksKnowledgeSearchTool", "ScrapeWebsiteTool"])
        assert result.has_trifecta

    def test_trifecta_detected_databricks_jobs_plus_perplexity(self):
        result = assess_trifecta(["DatabricksJobsTool", "PerplexityTool"])
        assert result.has_trifecta

    def test_trifecta_detected_power_bi_plus_mcp(self):
        result = assess_trifecta(["PowerBIAnalysisTool", "MCPTool"])
        assert result.has_trifecta

    def test_sensitive_tools_list_populated(self):
        result = assess_trifecta(["GenieTool", "SerperDevTool"])
        assert "GenieTool" in result.sensitive_tools

    def test_untrusted_tools_list_populated(self):
        result = assess_trifecta(["GenieTool", "SerperDevTool"])
        assert "SerperDevTool" in result.untrusted_tools

    def test_external_tools_list_populated(self):
        result = assess_trifecta(["GenieTool", "SerperDevTool"])
        assert "GenieTool" in result.external_tools
        assert "SerperDevTool" in result.external_tools

    def test_unknown_tools_ignored(self):
        # Unknown tools should not cause errors
        result = assess_trifecta(["NonExistentTool", "AnotherFakeTool"])
        assert not result.has_trifecta
        assert result.sensitive_tools == []
        assert result.untrusted_tools == []
        assert result.external_tools == []

    def test_mixed_known_unknown(self):
        # Unknown tools are ignored; known tools still classified correctly
        result = assess_trifecta(["GenieTool", "UnknownTool", "SerperDevTool"])
        assert result.has_trifecta

    def test_returns_trifecta_assessment_type(self):
        result = assess_trifecta([])
        assert isinstance(result, TrifectaAssessment)

    def test_duplicate_tool_names(self):
        # Duplicate names should still work correctly
        result = assess_trifecta(["GenieTool", "GenieTool", "SerperDevTool"])
        assert result.has_trifecta


class TestLogTrifectaWarning:
    def test_logs_warning_on_trifecta(self, caplog):
        assessment = TrifectaAssessment(
            has_trifecta=True,
            reads_sensitive=True,
            ingests_untrusted=True,
            communicates_externally=True,
            sensitive_tools=["GenieTool"],
            untrusted_tools=["SerperDevTool"],
            external_tools=["GenieTool", "SerperDevTool"],
        )
        with caplog.at_level(logging.WARNING, logger="src.engines.crewai.security.tool_capability_manifest"):
            log_trifecta_warning(assessment, context="test crew")
        assert any("[SECURITY] Lethal trifecta detected" in r.message for r in caplog.records)

    def test_logs_info_on_no_trifecta(self, caplog):
        assessment = TrifectaAssessment(
            has_trifecta=False,
            reads_sensitive=False,
            ingests_untrusted=False,
            communicates_externally=False,
        )
        with caplog.at_level(logging.INFO, logger="src.engines.crewai.security.tool_capability_manifest"):
            log_trifecta_warning(assessment)
        assert any("[SECURITY] No lethal trifecta" in r.message for r in caplog.records)

    def test_context_included_in_warning(self, caplog):
        assessment = TrifectaAssessment(
            has_trifecta=True,
            reads_sensitive=True,
            ingests_untrusted=True,
            communicates_externally=True,
            sensitive_tools=["GenieTool"],
            untrusted_tools=["SerperDevTool"],
            external_tools=["GenieTool"],
        )
        with caplog.at_level(logging.WARNING, logger="src.engines.crewai.security.tool_capability_manifest"):
            log_trifecta_warning(assessment, context="crew with 3 tasks")
        assert any("crew with 3 tasks" in r.message for r in caplog.records)


class TestToolCapabilityRegistry:
    def test_genie_has_sensitive_and_external(self):
        caps = TOOL_CAPABILITIES["GenieTool"]
        assert caps & ToolCapability.READS_SENSITIVE_DATA
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION

    def test_serper_has_untrusted_and_external(self):
        caps = TOOL_CAPABILITIES["SerperDevTool"]
        assert caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION
        assert not (caps & ToolCapability.READS_SENSITIVE_DATA)

    def test_dalle_has_only_external(self):
        caps = TOOL_CAPABILITIES["Dall-E Tool"]
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION
        assert not (caps & ToolCapability.READS_SENSITIVE_DATA)
        assert not (caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT)

    def test_mcp_tool_has_untrusted_and_external(self):
        caps = TOOL_CAPABILITIES["MCPTool"]
        assert caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION
