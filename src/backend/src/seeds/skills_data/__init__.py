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
    databricks_sql,
    query_validation,
    task_authoring,
    team_updates,
)

BUILTIN_SKILLS: List[Dict[str, Any]] = [
    task_authoring.SKILL,
    databricks_sql.SKILL,
    query_validation.SKILL,
    analysis_findings.SKILL,
    team_updates.SKILL,
    agentic_ai_news.SKILL,
]

__all__ = ["BUILTIN_SKILLS"]
