"""
Unit tests for tool capability manifest and lethal-trifecta detection.
"""

import logging

from src.services.security.tool_capability_manifest import (
    TOOL_CAPABILITIES,
    ToolCapability,
    TrifectaAssessment,
    assess_trifecta,
    classify_mcp_server,
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
        result = assess_trifecta(["Image Generation Tool"])
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
        with caplog.at_level(
            logging.WARNING, logger="src.services.security.tool_capability_manifest"
        ):
            log_trifecta_warning(assessment, context="test crew")
        assert any(
            "[SECURITY] Lethal trifecta detected" in r.message for r in caplog.records
        )

    def test_logs_info_on_no_trifecta(self, caplog):
        assessment = TrifectaAssessment(
            has_trifecta=False,
            reads_sensitive=False,
            ingests_untrusted=False,
            communicates_externally=False,
        )
        with caplog.at_level(
            logging.INFO, logger="src.services.security.tool_capability_manifest"
        ):
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
        with caplog.at_level(
            logging.WARNING, logger="src.services.security.tool_capability_manifest"
        ):
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
        caps = TOOL_CAPABILITIES["Image Generation Tool"]
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION
        assert not (caps & ToolCapability.READS_SENSITIVE_DATA)
        assert not (caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT)

    def test_mcp_tool_has_untrusted_and_external(self):
        caps = TOOL_CAPABILITIES["MCPTool"]
        assert caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION

    def test_agent_bricks_is_sensitive_untrusted_and_external(self):
        # Agent Bricks is an opaque agent that may read internal data, browse the
        # web, and call out — so it carries all three capabilities (trips alone).
        for key in ("AgentBricksTool", "agent_bricks_tool"):
            caps = TOOL_CAPABILITIES[key]
            assert caps & ToolCapability.READS_SENSITIVE_DATA
            assert caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT
            assert caps & ToolCapability.EXTERNAL_COMMUNICATION

    def test_agent_bricks_alone_trips_trifecta(self):
        assert assess_trifecta(["AgentBricksTool"]).has_trifecta


class TestClassifyMcpServer:
    def test_databricks_sql_is_sensitive_external_destructive(self):
        caps = classify_mcp_server("Databricks SQL")
        assert caps & ToolCapability.READS_SENSITIVE_DATA
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION
        assert caps & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS
        # Not flagged untrusted — it reads internal data, it is not a web channel.
        assert not (caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT)

    def test_uc_functions_with_dynamic_suffix(self):
        # The dynamic "(catalog.schema)" suffix must still match by substring.
        caps = classify_mcp_server("Unity Catalog Functions (main.default)")
        assert caps & ToolCapability.READS_SENSITIVE_DATA
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION
        assert not (caps & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS)

    def test_uc_functions_system_ai_is_destructive(self):
        # system.ai exposes python_exec (arbitrary code) → destructive.
        caps = classify_mcp_server("Unity Catalog Functions (system.ai)")
        assert caps & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS

    def test_genie_ai_search_vector_search_are_sensitive_external(self):
        for name in ("Genie", "AI Search Indexes", "Databricks Vector Search"):
            caps = classify_mcp_server(name)
            assert caps & ToolCapability.READS_SENSITIVE_DATA
            assert caps & ToolCapability.EXTERNAL_COMMUNICATION
            assert not (caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT)

    def test_classification_is_case_insensitive(self):
        # The picker passes display names verbatim, so matching must be lowercased.
        assert (
            classify_mcp_server("DATABRICKS SQL")
            & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS
        )

    def test_unknown_server_defaults_to_untrusted_external(self):
        # The crux: we can't enumerate internet-capable MCP servers, so anything
        # unrecognised is assumed to ingest untrusted content + reach external.
        caps = classify_mcp_server("Some Custom Slack MCP")
        assert caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT
        assert caps & ToolCapability.EXTERNAL_COMMUNICATION
        assert not (caps & ToolCapability.READS_SENSITIVE_DATA)

    def test_empty_name_is_none(self):
        assert classify_mcp_server("") == ToolCapability.NONE


# ==========================================================================
# Additional coverage: assess_mixed_task, log_mixed_task_warning,
# apply_spotlighting_wrappers, run_crew_security_checks
# ==========================================================================
from unittest.mock import MagicMock, patch


def test_assess_mixed_task_no_tools():
    from src.services.security.tool_capability_manifest import assess_mixed_task

    result = assess_mixed_task([])
    assert result.is_mixed is False


def test_assess_mixed_task_only_untrusted():
    from src.services.security.tool_capability_manifest import (
        TOOL_CAPABILITIES,
        ToolCapability,
        assess_mixed_task,
    )

    # Find a tool with INGESTS_UNTRUSTED_CONTENT but no READS_SENSITIVE_DATA
    untrusted_tools = [
        name
        for name, cap in TOOL_CAPABILITIES.items()
        if (cap & ToolCapability.INGESTS_UNTRUSTED_CONTENT)
        and not (cap & ToolCapability.READS_SENSITIVE_DATA)
        and not (cap & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS)
    ]
    if not untrusted_tools:
        pytest.skip("No suitable tool found")

    result = assess_mixed_task([untrusted_tools[0]])
    # Only untrusted, no sensitive/destructive - not mixed
    assert result.is_mixed is False


def test_assess_mixed_task_is_mixed():
    from src.services.security.tool_capability_manifest import (
        TOOL_CAPABILITIES,
        ToolCapability,
        assess_mixed_task,
    )

    # Find tools to create a mixed scenario
    untrusted = [
        name
        for name, cap in TOOL_CAPABILITIES.items()
        if cap & ToolCapability.INGESTS_UNTRUSTED_CONTENT
    ][:1]
    sensitive = [
        name
        for name, cap in TOOL_CAPABILITIES.items()
        if cap & ToolCapability.READS_SENSITIVE_DATA
        and not (cap & ToolCapability.INGESTS_UNTRUSTED_CONTENT)
    ][:1]

    if not untrusted or not sensitive:
        # Manually test with mocked TOOL_CAPABILITIES
        from src.services.security import tool_capability_manifest as mod

        original = mod.TOOL_CAPABILITIES.copy()
        mod.TOOL_CAPABILITIES["fake_untrusted"] = (
            ToolCapability.INGESTS_UNTRUSTED_CONTENT
        )
        mod.TOOL_CAPABILITIES["fake_sensitive"] = ToolCapability.READS_SENSITIVE_DATA
        try:
            result = assess_mixed_task(
                ["fake_untrusted", "fake_sensitive"], "test_task"
            )
            assert result.is_mixed is True
            assert "fake_untrusted" in result.untrusted_tools
            assert "fake_sensitive" in result.sensitive_tools
        finally:
            mod.TOOL_CAPABILITIES.clear()
            mod.TOOL_CAPABILITIES.update(original)
    else:
        result = assess_mixed_task(untrusted + sensitive, "test_task")
        assert result.is_mixed is True


def test_assess_mixed_task_with_destructive():
    from src.services.security import tool_capability_manifest as mod
    from src.services.security.tool_capability_manifest import (
        TOOL_CAPABILITIES,
        ToolCapability,
        assess_mixed_task,
    )

    original = mod.TOOL_CAPABILITIES.copy()
    mod.TOOL_CAPABILITIES["fake_untrusted"] = ToolCapability.INGESTS_UNTRUSTED_CONTENT
    mod.TOOL_CAPABILITIES["fake_destructive"] = (
        ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS
    )
    try:
        result = assess_mixed_task(["fake_untrusted", "fake_destructive"], "test_task")
        assert result.is_mixed is True
        assert "fake_untrusted" in result.untrusted_tools
        assert "fake_destructive" in result.destructive_tools
    finally:
        mod.TOOL_CAPABILITIES.clear()
        mod.TOOL_CAPABILITIES.update(original)


# ---- log_mixed_task_warning ----


def test_log_mixed_task_warning_not_mixed():
    from src.services.security.tool_capability_manifest import (
        MixedTaskAssessment,
        log_mixed_task_warning,
    )

    assessment = MixedTaskAssessment(
        is_mixed=False,
        untrusted_tools=[],
        sensitive_tools=[],
        destructive_tools=[],
        task_name="test",
    )
    # Should return early without logging
    log_mixed_task_warning(assessment)


def test_log_mixed_task_warning_is_mixed():
    from src.services.security.tool_capability_manifest import (
        MixedTaskAssessment,
        log_mixed_task_warning,
    )

    assessment = MixedTaskAssessment(
        is_mixed=True,
        untrusted_tools=["web_search"],
        sensitive_tools=["db_tool"],
        destructive_tools=[],
        task_name="mixed_task",
    )
    # Should log a warning
    log_mixed_task_warning(assessment)


# ---- apply_spotlighting_wrappers ----


def test_apply_spotlighting_no_agents():
    from src.services.security.tool_capability_manifest import (
        apply_spotlighting_wrappers,
    )

    crew = MagicMock()
    crew.agents = []
    result = apply_spotlighting_wrappers(crew)
    assert result == 0


def test_apply_spotlighting_agent_no_untrusted_tools():
    from src.services.security.tool_capability_manifest import (
        apply_spotlighting_wrappers,
    )

    class FakeAgent:
        def __init__(self):
            self.tools = [FakeTool()]

    class FakeTool:
        name = "safe_tool_not_in_manifest"
        _run = lambda self, *a, **k: "result"

    crew = MagicMock()
    crew.agents = [FakeAgent()]
    result = apply_spotlighting_wrappers(crew)
    assert result == 0


def test_apply_spotlighting_with_untrusted_tool():
    from src.services.security import tool_capability_manifest as mod
    from src.services.security.tool_capability_manifest import (
        TOOL_CAPABILITIES,
        ToolCapability,
        apply_spotlighting_wrappers,
    )

    # Inject a fake untrusted tool
    original = mod.TOOL_CAPABILITIES.copy()
    mod.TOOL_CAPABILITIES["fake_web_tool"] = ToolCapability.INGESTS_UNTRUSTED_CONTENT

    try:
        tool = MagicMock()
        tool.name = "fake_web_tool"
        tool._run = lambda *a, **k: "raw_content"

        agent = MagicMock()
        agent.tools = [tool]

        crew = MagicMock()
        crew.agents = [agent]

        result = apply_spotlighting_wrappers(crew)
        assert result == 1
        # Check wrapping
        wrapped_result = tool._run("test")
        assert "<<" in wrapped_result and ">>" in wrapped_result
    finally:
        mod.TOOL_CAPABILITIES.clear()
        mod.TOOL_CAPABILITIES.update(original)


# ---- run_crew_security_checks ----


def test_run_crew_security_checks_empty_crew():
    from src.services.security.tool_capability_manifest import run_crew_security_checks

    crew = MagicMock()
    crew.agents = []
    crew.tasks = []
    # Should not raise
    run_crew_security_checks(crew, context="test")


def test_run_crew_security_checks_with_tasks():
    from src.services.security.tool_capability_manifest import run_crew_security_checks

    crew = MagicMock()

    task = MagicMock()
    task.description = "A test task"
    task.tools = []
    agent = MagicMock()
    agent.tools = []
    task.agent = agent

    crew.agents = []
    crew.tasks = [task]

    # Should not raise
    run_crew_security_checks(crew, context="test_context")


def test_run_crew_security_checks_exception_handled():
    from src.services.security.tool_capability_manifest import run_crew_security_checks

    crew = MagicMock()
    crew.agents = MagicMock(side_effect=Exception("Crew access failed"))
    crew.tasks = MagicMock(side_effect=Exception("Tasks access failed"))
    # Should not raise
    run_crew_security_checks(crew)


def test_run_crew_security_checks_spotlighting_exception():
    from src.services.security.tool_capability_manifest import run_crew_security_checks

    crew = MagicMock()
    crew.agents = []
    crew.tasks = []
    with patch(
        "src.services.security.tool_capability_manifest.apply_spotlighting_wrappers",
        side_effect=Exception("spotlighting error"),
    ):
        # Should not raise
        run_crew_security_checks(crew)
