"""
Coverage tests for engines/crewai/security/tool_capability_manifest.py
Covers: assess_mixed_task, log_mixed_task_warning, apply_spotlighting_wrappers, run_crew_security_checks
"""
import pytest
from unittest.mock import MagicMock, patch


# ---- assess_mixed_task ----

def test_assess_mixed_task_no_tools():
    from src.engines.crewai.security.tool_capability_manifest import assess_mixed_task
    result = assess_mixed_task([])
    assert result.is_mixed is False


def test_assess_mixed_task_only_untrusted():
    from src.engines.crewai.security.tool_capability_manifest import (
        assess_mixed_task,
        TOOL_CAPABILITIES,
        ToolCapability,
    )
    # Find a tool with INGESTS_UNTRUSTED_CONTENT but no READS_SENSITIVE_DATA
    untrusted_tools = [
        name for name, cap in TOOL_CAPABILITIES.items()
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
    from src.engines.crewai.security.tool_capability_manifest import (
        assess_mixed_task,
        TOOL_CAPABILITIES,
        ToolCapability,
    )
    # Find tools to create a mixed scenario
    untrusted = [
        name for name, cap in TOOL_CAPABILITIES.items()
        if cap & ToolCapability.INGESTS_UNTRUSTED_CONTENT
    ][:1]
    sensitive = [
        name for name, cap in TOOL_CAPABILITIES.items()
        if cap & ToolCapability.READS_SENSITIVE_DATA
        and not (cap & ToolCapability.INGESTS_UNTRUSTED_CONTENT)
    ][:1]

    if not untrusted or not sensitive:
        # Manually test with mocked TOOL_CAPABILITIES
        from src.engines.crewai.security import tool_capability_manifest as mod
        original = mod.TOOL_CAPABILITIES.copy()
        mod.TOOL_CAPABILITIES["fake_untrusted"] = ToolCapability.INGESTS_UNTRUSTED_CONTENT
        mod.TOOL_CAPABILITIES["fake_sensitive"] = ToolCapability.READS_SENSITIVE_DATA
        try:
            result = assess_mixed_task(["fake_untrusted", "fake_sensitive"], "test_task")
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
    from src.engines.crewai.security.tool_capability_manifest import (
        assess_mixed_task,
        TOOL_CAPABILITIES,
        ToolCapability,
    )
    from src.engines.crewai.security import tool_capability_manifest as mod
    original = mod.TOOL_CAPABILITIES.copy()
    mod.TOOL_CAPABILITIES["fake_untrusted"] = ToolCapability.INGESTS_UNTRUSTED_CONTENT
    mod.TOOL_CAPABILITIES["fake_destructive"] = ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS
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
    from src.engines.crewai.security.tool_capability_manifest import (
        log_mixed_task_warning,
        MixedTaskAssessment,
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
    from src.engines.crewai.security.tool_capability_manifest import (
        log_mixed_task_warning,
        MixedTaskAssessment,
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
    from src.engines.crewai.security.tool_capability_manifest import apply_spotlighting_wrappers
    crew = MagicMock()
    crew.agents = []
    result = apply_spotlighting_wrappers(crew)
    assert result == 0


def test_apply_spotlighting_agent_no_untrusted_tools():
    from src.engines.crewai.security.tool_capability_manifest import apply_spotlighting_wrappers

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
    from src.engines.crewai.security.tool_capability_manifest import (
        apply_spotlighting_wrappers,
        TOOL_CAPABILITIES,
        ToolCapability,
    )
    from src.engines.crewai.security import tool_capability_manifest as mod

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
    from src.engines.crewai.security.tool_capability_manifest import run_crew_security_checks
    crew = MagicMock()
    crew.agents = []
    crew.tasks = []
    # Should not raise
    run_crew_security_checks(crew, context="test")


def test_run_crew_security_checks_with_tasks():
    from src.engines.crewai.security.tool_capability_manifest import run_crew_security_checks
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
    from src.engines.crewai.security.tool_capability_manifest import run_crew_security_checks
    crew = MagicMock()
    crew.agents = MagicMock(side_effect=Exception("Crew access failed"))
    crew.tasks = MagicMock(side_effect=Exception("Tasks access failed"))
    # Should not raise
    run_crew_security_checks(crew)


def test_run_crew_security_checks_spotlighting_exception():
    from src.engines.crewai.security.tool_capability_manifest import run_crew_security_checks
    crew = MagicMock()
    crew.agents = []
    crew.tasks = []
    with patch('src.engines.crewai.security.tool_capability_manifest.apply_spotlighting_wrappers',
               side_effect=Exception("spotlighting error")):
        # Should not raise
        run_crew_security_checks(crew)
