"""The skills Kasal ships.

Written after reading the skills Anthropic publishes at
github.com/anthropics/skills — ``doc-coauthoring``, ``internal-comms``,
``skill-creator``, ``brand-guidelines``, ``webapp-testing`` — and following the
shape that works there rather than inventing one:

- **The description names the SITUATION, loaded with the words a user would
  actually type.** The real ones read "Use when user wants to write
  documentation, proposals, technical specs... Trigger when user mentions
  writing docs, creating proposals, drafting specs". A description that says
  what a skill is *about* rather than *when to reach for it* never activates,
  and this is the only field the model sees at discovery time.
- **The body is a workflow, not an essay.** "When to use this skill", then
  numbered steps.
- **Detail goes in bundled files.** ``internal-comms`` keeps one file per
  communication type and loads only the one it needs. ``databricks-sql`` here
  does the same, which also means tier 3 is exercised by real seed data rather
  than only by tests.

The CONTENT is Kasal's own. That repository publishes no licence, so its text is
not copied — only the structure it demonstrates, and the judgement that the
description is the field that matters.

Each entry may carry ``files``: a list of ``{path, content}`` bundled with the
skill, read through ``read_skill_file``.
"""

from typing import Any, Dict, List

from src.seeds.skills_data import (
    agentic_ai_news,
    analysis_findings,
    dashboard_content,
    databricks_sql,
    document_grounding,
    option_comparison,
    presentation_content,
    query_validation,
    report_writing,
    task_authoring,
    team_updates,
    web_research,
    work_breakdown,
    workplace_messages,
)

BUILTIN_SKILLS: List[Dict[str, Any]] = [
    # Building crews and plans
    task_authoring.SKILL,
    work_breakdown.SKILL,
    # Data: querying and trusting results
    databricks_sql.SKILL,
    query_validation.SKILL,
    # Research and grounding
    web_research.SKILL,
    document_grounding.SKILL,
    agentic_ai_news.SKILL,
    # Deliverables: analyses, documents, decks, dashboards, decisions
    analysis_findings.SKILL,
    report_writing.SKILL,
    presentation_content.SKILL,
    dashboard_content.SKILL,
    option_comparison.SKILL,
    # Communication
    team_updates.SKILL,
    workplace_messages.SKILL,
]

__all__ = ["BUILTIN_SKILLS"]
