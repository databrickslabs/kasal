"""
Tool capability manifest and lethal-trifecta detection.

The "lethal trifecta" (per Databricks AI Security, Feb 2026) is the combination of:
  1. Reads sensitive internal data (DB, knowledge search, Databricks APIs)
  2. Ingests untrusted external content (web scraping, search results)
  3. Communicates externally (any outbound network request)

When a crew satisfies all three conditions, indirect prompt injection attacks
can exfiltrate sensitive data to attacker-controlled endpoints.

This module is log-only — it detects and warns but never blocks execution.

Usage:
    assessment = assess_trifecta(tool_names)
    log_trifecta_warning(assessment, context="my crew")
"""

import logging
from dataclasses import dataclass, field
from enum import Flag, auto
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)


class ToolCapability(Flag):
    """Capability flags classifying what a tool can do."""
    NONE = 0
    READS_SENSITIVE_DATA = auto()           # reads internal/confidential data
    INGESTS_UNTRUSTED_CONTENT = auto()      # fetches external, attacker-reachable content
    EXTERNAL_COMMUNICATION = auto()         # makes outbound network requests
    PERFORMS_DESTRUCTIVE_OPERATIONS = auto()  # triggers irreversible actions (delete, run, deploy)


# Shorthand aliases for readability in the registry
_S = ToolCapability.READS_SENSITIVE_DATA
_U = ToolCapability.INGESTS_UNTRUSTED_CONTENT
_E = ToolCapability.EXTERNAL_COMMUNICATION
_D = ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS

# Registry — keys match BOTH class names and runtime t.name values
# (CrewAI tool objects expose a snake_case .name that differs from the class name)
TOOL_CAPABILITIES: Dict[str, ToolCapability] = {
    # Databricks / internal data tools
    "GenieTool":                                      _S | _E,
    "genie_tool":                                     _S | _E,  # runtime name
    "DatabricksJobsTool":                             _S | _E | _D,   # can trigger job runs
    "databricks_jobs_tool":                           _S | _E | _D,   # runtime name
    "DatabricksKnowledgeSearchTool":                  _S | _E,
    "databricks_knowledge_search_tool":               _S | _E,  # runtime name
    "AgentBricksTool":                                _E,
    "agent_bricks_tool":                              _E,        # runtime name

    # Web / external search tools (ingest untrusted content)
    "SerperDevTool":                                  _U | _E,
    "search_the_internet_with_serper":                _U | _E,  # runtime name (snake_case)
    "Search the internet with Serper":                _U | _E,  # runtime name (display)
    "PerplexityTool":                                 _U | _E,
    "perplexity_tool":                                _U | _E,  # runtime name
    "ScrapeWebsiteTool":                              _U | _E,
    "scrape_website":                                 _U | _E,  # runtime name

    # Image generation (external API, no sensitive read)
    "Dall-E Tool":                                    _E,

    # MCP (may ingest untrusted content from MCP servers)
    "MCPTool":                                        _U | _E,

    # Power BI tools (read internal analytics data)
    "PowerBIAnalysisTool":                            _S | _E,
    "Power BI Comprehensive Analysis Tool":           _S | _E,
    "Power BI Intelligent Analysis (Copilot-Style)":  _S | _E,   # runtime name for PowerBIAnalysisTool
    "PowerBIConnectorTool":                           _S | _E,
    "Power BI Connector":                             _S | _E,   # runtime name for PowerBIConnectorTool
    "Measure Conversion Pipeline":                    _S | _E,
    "M-Query Conversion Pipeline":                    _S | _E,
    "Power BI Relationships Tool":                    _S | _E,
    "Power BI Hierarchies Tool":                      _S | _E,
    "Power BI Field Parameters & Calculation Groups Tool": _S | _E,
    "Power BI Report References Tool":                _S | _E,

    # PowerBI tools added in recent PRs — were missing from original manifest
    "Power BI Semantic Model Fetcher":                _S | _E,   # fetches PBI model metadata via API
    "Power BI Semantic Model DAX Generator":          _S | _E,   # reads + executes DAX against PBI
    "Power BI Metadata Reducer":                      _S | _E,   # processes PBI semantic model data

    # DatabricksJobsTool runtime name (BaseTool.name differs from class name)
    "Databricks Jobs Manager":                        _S | _E | _D,
}


@dataclass
class TrifectaAssessment:
    """Result of a lethal-trifecta capability check across a set of tools."""
    has_trifecta: bool
    reads_sensitive: bool
    ingests_untrusted: bool
    communicates_externally: bool
    sensitive_tools: List[str] = field(default_factory=list)
    untrusted_tools: List[str] = field(default_factory=list)
    external_tools: List[str] = field(default_factory=list)


def assess_trifecta(tool_names: Iterable[str]) -> TrifectaAssessment:
    """
    Assess whether the given tool set satisfies the lethal-trifecta condition.

    Args:
        tool_names: Iterable of tool name strings (from task.tools[*].name).

    Returns:
        TrifectaAssessment with has_trifecta=True if all three capability
        dimensions are covered by at least one tool.
    """
    sensitive_tools: List[str] = []
    untrusted_tools: List[str] = []
    external_tools: List[str] = []

    for name in tool_names:
        caps = TOOL_CAPABILITIES.get(name, ToolCapability.NONE)
        if caps & ToolCapability.READS_SENSITIVE_DATA:
            sensitive_tools.append(name)
        if caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT:
            untrusted_tools.append(name)
        if caps & ToolCapability.EXTERNAL_COMMUNICATION:
            external_tools.append(name)

    reads_sensitive = bool(sensitive_tools)
    ingests_untrusted = bool(untrusted_tools)
    communicates_externally = bool(external_tools)
    has_trifecta = reads_sensitive and ingests_untrusted and communicates_externally

    return TrifectaAssessment(
        has_trifecta=has_trifecta,
        reads_sensitive=reads_sensitive,
        ingests_untrusted=ingests_untrusted,
        communicates_externally=communicates_externally,
        sensitive_tools=sensitive_tools,
        untrusted_tools=untrusted_tools,
        external_tools=external_tools,
    )


def log_trifecta_warning(assessment: TrifectaAssessment, context: str = "") -> None:
    """
    Log a structured warning when the lethal-trifecta is detected, or an
    informational message otherwise.

    Args:
        assessment: Result from assess_trifecta().
        context:    Optional description (e.g. "crew with 3 tasks") for the log.
    """
    ctx = f" [{context}]" if context else ""

    if assessment.has_trifecta:
        logger.warning(
            "[SECURITY] Lethal trifecta detected%s: "
            "sensitive_tools=%s, untrusted_tools=%s, external_tools=%s. "
            "This crew can read internal data, ingest untrusted content, and "
            "communicate externally — high risk of indirect prompt injection exfiltration.",
            ctx,
            assessment.sensitive_tools,
            assessment.untrusted_tools,
            assessment.external_tools,
        )
    else:
        logger.info(
            "[SECURITY] No lethal trifecta%s: "
            "reads_sensitive=%s ingests_untrusted=%s communicates_externally=%s",
            ctx,
            assessment.reads_sensitive,
            assessment.ingests_untrusted,
            assessment.communicates_externally,
        )


def assess_destructive_risk(tool_names: Iterable[str]) -> List[str]:
    """
    Return the subset of *tool_names* that carry the PERFORMS_DESTRUCTIVE_OPERATIONS flag.

    Args:
        tool_names: Iterable of tool name strings.

    Returns:
        List of tool names classified as destructive (may be empty).
    """
    return [
        name for name in tool_names
        if TOOL_CAPABILITIES.get(name, ToolCapability.NONE)
        & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS
    ]


@dataclass
class MixedTaskAssessment:
    """Result of checking whether a single task mixes untrusted input with sensitive/destructive tools."""
    is_mixed: bool
    untrusted_tools: List[str]
    sensitive_tools: List[str]
    destructive_tools: List[str]
    task_name: str = ""


def assess_mixed_task(tool_names: Iterable[str], task_name: str = "") -> MixedTaskAssessment:
    """
    Detect the anti-pattern where a single task combines untrusted-input tools
    (_U) with sensitive-data or destructive tools (_S or _D).

    This is the scenario where guardrails are bypassed: the ReAct loop runs
    web scraping + an internal/destructive tool in one shot, so the intermediate
    external output is never scanned before the sensitive tool fires.

    Args:
        tool_names: Iterable of tool name strings for a single task.
        task_name:  Human-readable task label for log context.

    Returns:
        MixedTaskAssessment with is_mixed=True when the anti-pattern is present.
    """
    tool_names = list(tool_names)
    untrusted: List[str] = []
    sensitive: List[str] = []
    destructive: List[str] = []

    for name in tool_names:
        caps = TOOL_CAPABILITIES.get(name, ToolCapability.NONE)
        if caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT:
            untrusted.append(name)
        if caps & ToolCapability.READS_SENSITIVE_DATA:
            sensitive.append(name)
        if caps & ToolCapability.PERFORMS_DESTRUCTIVE_OPERATIONS:
            destructive.append(name)

    is_mixed = bool(untrusted) and bool(sensitive or destructive)
    return MixedTaskAssessment(
        is_mixed=is_mixed,
        untrusted_tools=untrusted,
        sensitive_tools=sensitive,
        destructive_tools=destructive,
        task_name=task_name,
    )


def log_mixed_task_warning(assessment: MixedTaskAssessment) -> None:
    """
    Log an architectural recommendation when a single task mixes untrusted-input
    tools with sensitive-data or destructive tools.

    This is log-only — it never blocks execution.

    Args:
        assessment: Result from assess_mixed_task().
    """
    if not assessment.is_mixed:
        return
    logger.warning(
        "[SECURITY] Mixed-task anti-pattern detected in task '%s': "
        "untrusted_tools=%s, sensitive_tools=%s, destructive_tools=%s. "
        "This task combines external/untrusted input tools with internal data or "
        "destructive tools in a single ReAct loop — a guardrail cannot inspect the "
        "intermediate output before the internal tool fires. "
        "RECOMMENDATION: Split into two tasks — (1) external input only, with an "
        "LLM injection guardrail configured on that task; (2) internal/destructive "
        "tool usage that receives the first task's output as context. "
        "This is the architecture the LLM injection guardrail is designed to protect.",
        assessment.task_name,
        assessment.untrusted_tools,
        assessment.sensitive_tools,
        assessment.destructive_tools,
    )


def apply_spotlighting_wrappers(crew: Any) -> int:
    """
    Wrap every _U (INGESTS_UNTRUSTED_CONTENT) tool's _run output in
    ``<< ... >>`` spotlighting delimiters.

    This is the shared implementation used by both the regular crew path
    (CrewPreparation) and the flow crew path (flow_methods.py).  Keeping
    it here avoids duplicating the wrapping logic and ensures flows get
    identical protection to regular crews.

    Args:
        crew: A CrewAI Crew instance whose agents' tools should be wrapped.

    Returns:
        Number of tools wrapped (0 = no untrusted tools found).
    """
    wrapped_count = 0
    for agent in getattr(crew, "agents", []):
        new_tools = []
        for tool in (getattr(agent, "tools", None) or []):
            tool_name = getattr(tool, "name", "")
            caps = TOOL_CAPABILITIES.get(tool_name, ToolCapability.NONE)
            if caps & ToolCapability.INGESTS_UNTRUSTED_CONTENT:
                original_run = tool._run

                def make_wrapper(fn):
                    def _wrapped(*args, **kwargs):
                        result = fn(*args, **kwargs)
                        return f"<<\n{result}\n>>"
                    return _wrapped

                tool._run = make_wrapper(original_run)
                wrapped_count += 1
            new_tools.append(tool)
        object.__setattr__(agent, "tools", new_tools)
    return wrapped_count


def run_crew_security_checks(crew: Any, context: str = "") -> None:
    """
    Run all assembly-time security checks on a Crew object.

    Covers: spotlighting wrappers, crew-wide trifecta, per-task trifecta,
    mixed-task anti-pattern, and destructive-tool detection.

    This is the shared entry point used by both CrewPreparation and
    flow_methods.py so both execution paths get identical security coverage.

    Args:
        crew:    A CrewAI Crew instance (already assembled).
        context: Label for log messages (e.g. "flow crew 'ResearchFlow'").
    """
    _log = logging.getLogger(__name__)

    # 1. Spotlighting
    try:
        n = apply_spotlighting_wrappers(crew)
        _log.info(
            "[SECURITY]%s Spotlighting: %d untrusted tool(s) wrapped.",
            f" [{context}]" if context else "",
            n,
        )
    except Exception as err:
        _log.warning("[SECURITY] Spotlighting failed (non-blocking): %s", err)

    # 2. Collect all tool names (task-level + agent-level)
    try:
        tool_names = list({
            t.name
            for task in getattr(crew, "tasks", [])
            for t in (getattr(task, "tools", None) or [])
        } | {
            t.name
            for agent in getattr(crew, "agents", [])
            for t in (getattr(agent, "tools", None) or [])
        })

        ctx = f" [{context}]" if context else ""
        n_tasks = len(getattr(crew, "tasks", []))

        # 3. Crew-wide trifecta
        _trifecta = assess_trifecta(tool_names)
        log_trifecta_warning(_trifecta, context=f"{context} (crew-wide)" if context else "crew-wide")

        # 4. Destructive tools
        _destructive = assess_destructive_risk(tool_names)
        log_destructive_warning(_destructive, context=f"{context}" if context else f"crew with {n_tasks} task(s)")

        # 5. Per-task trifecta + mixed-task check
        for task in getattr(crew, "tasks", []):
            task_tool_names: List[str] = [
                t.name for t in (getattr(task, "tools", None) or [])
            ]
            agent = getattr(task, "agent", None)
            if agent:
                task_tool_names += [
                    t.name for t in (getattr(agent, "tools", None) or [])
                ]
            task_label = (getattr(task, "description", "") or "")[:80]

            _task_trifecta = assess_trifecta(task_tool_names)
            log_trifecta_warning(_task_trifecta, context=f"task '{task_label}'{ctx}")

            _mixed = assess_mixed_task(task_tool_names, task_name=task_label)
            log_mixed_task_warning(_mixed)

    except Exception as err:
        _log.debug("[SECURITY] Crew security checks skipped: %s", err)


def log_destructive_warning(destructive_tools: List[str], context: str = "") -> None:
    """
    Log a warning when destructive tools are present in a crew, recommending
    that operators enable human_input=True on the relevant tasks.

    This is log-only — it never blocks execution.

    Args:
        destructive_tools: Output from assess_destructive_risk().
        context:           Optional description for the log line.
    """
    if not destructive_tools:
        return
    ctx = f" [{context}]" if context else ""
    logger.warning(
        "[SECURITY] Destructive tools detected%s: %s. "
        "Consider enabling human_input=True on tasks that use these tools "
        "to prevent unintended irreversible actions.",
        ctx,
        destructive_tools,
    )
