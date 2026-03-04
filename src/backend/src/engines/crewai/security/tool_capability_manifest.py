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
from typing import Dict, Iterable, List

logger = logging.getLogger(__name__)


class ToolCapability(Flag):
    """Capability flags classifying what a tool can do."""
    NONE = 0
    READS_SENSITIVE_DATA = auto()       # reads internal/confidential data
    INGESTS_UNTRUSTED_CONTENT = auto()  # fetches external, attacker-reachable content
    EXTERNAL_COMMUNICATION = auto()     # makes outbound network requests


# Shorthand aliases for readability in the registry
_S = ToolCapability.READS_SENSITIVE_DATA
_U = ToolCapability.INGESTS_UNTRUSTED_CONTENT
_E = ToolCapability.EXTERNAL_COMMUNICATION

# Registry — keys match BOTH class names and runtime t.name values
# (CrewAI tool objects expose a snake_case .name that differs from the class name)
TOOL_CAPABILITIES: Dict[str, ToolCapability] = {
    # Databricks / internal data tools
    "GenieTool":                                      _S | _E,
    "genie_tool":                                     _S | _E,  # runtime name
    "DatabricksJobsTool":                             _S | _E,
    "databricks_jobs_tool":                           _S | _E,  # runtime name
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
    "PowerBIConnectorTool":                           _S | _E,
    "Measure Conversion Pipeline":                    _S | _E,
    "M-Query Conversion Pipeline":                    _S | _E,
    "Power BI Relationships Tool":                    _S | _E,
    "Power BI Hierarchies Tool":                      _S | _E,
    "Power BI Field Parameters & Calculation Groups Tool": _S | _E,
    "Power BI Report References Tool":                _S | _E,
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
