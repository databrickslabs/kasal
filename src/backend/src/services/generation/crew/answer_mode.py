"""Deep Research mode → what the generated crew actually does differently.

``deep`` differed from ``research`` by exactly one string — ``reasoning_effort``
"high" versus "medium" — and on a model without a native reasoning budget the
two were byte-identical. Everything else the mode implies was either not built
or built and never switched on:

* A guardrail was GENERATED for every task (the templates require one, the
  generator persists it, the executor is fully wired to consume it) and then
  dropped on the floor by both crew-config builders, so deep runs executed
  ungated against a criterion already written for them.
* No execution caps of any kind were set, so every mode ran on identical bare
  engine defaults.
* No tool distinction: the export template gates slow deep-research tools behind
  a mode, the in-app builder never did.

**Scope: this touches ``deep`` and NOTHING else.** ``chat`` and ``research``
keep exactly the behaviour they have today, including their bare engine
defaults and their ungated tasks. The original proposal applied the guardrail
and budget work to research as well; that was deliberately narrowed, because
deep is the mode being changed and adding a judge call plus retries to research
would slow a mode nobody asked to change. Widening later is a one-line edit to
``GATED_MODES`` — but it IS a behaviour change to research, so make it on
purpose.

Kept as a separate module because ``generation/crews.py`` is already over the
file-size ceiling.
"""

import copy
import logging
from typing import Any, Dict, List, Optional

from src.schemas.deep_research import (
    DEEP_RESEARCH_ENVELOPE_SCHEMA,
    DEFAULT_DEEP_GATE,
)
from src.services.execution.config.budget_profile import resolve_budget_profile

logger = logging.getLogger(__name__)

#: The only mode this pass touches. See the scope note in the module docstring
#: before adding to it — every other mode keeps today's behaviour exactly.
GATED_MODES = ("deep",)

#: Marker left on a decorated task so a second pass is a no-op.
#:
#: This runs from TWO places on purpose: generation (so the persisted config
#: shows what the run will actually do) and execution config adaptation (so a
#: crew re-run from the UI, which never goes through generation, is gated
#: identically). Without the marker the second pass would append the contract to
#: the description a second time.
_APPLIED_MARKER = "_answer_mode"

#: Catalog id of the search tool deep mode reconfigures. Deep does not ADD tools
#: the generator did not pick, and does not take any away from other modes — it
#: only changes how the ones already equipped are configured.
PERPLEXITY_TOOL_ID = "31"

#: Perplexity's deep-research model, and the headroom its answers need. The seed
#: default is the fast `sonar`; deep mode is precisely the case the slow model
#: exists for.
_PERPLEXITY_DEEP_CONFIG = {
    "model": "sonar-deep-research",
    "max_tokens": 4000,
    "search_recency_filter": "year",
}


def apply_answer_mode(
    mode: Optional[str],
    agents_yaml: Dict[str, Dict[str, Any]],
    tasks_yaml: Dict[str, Dict[str, Any]],
    generated_tasks: Dict[str, Dict[str, Any]],
) -> None:
    """Apply the answer mode's behaviour to a generated crew config, in place.

    ``generated_tasks`` maps the ``task_<id>`` key to the raw generated task, so
    the fields the builder drops (notably ``llm_guardrail``) can be recovered
    without re-querying.
    """
    normalized = (mode or "chat").strip().lower()
    if normalized not in GATED_MODES:
        return

    pending = {
        key: entry
        for key, entry in tasks_yaml.items()
        if entry.get(_APPLIED_MARKER) != normalized
    }
    if not pending:
        return

    profile = resolve_budget_profile(normalized)

    for spec in agents_yaml.values():
        _apply_agent_budget(spec, profile)
        _apply_deep_tool_policy(spec)

    for key, entry in pending.items():
        source = generated_tasks.get(key) or {}
        _apply_task_verification(entry, source, profile)
        _apply_deep_tool_policy(entry)
        entry[_APPLIED_MARKER] = normalized

    logger.info(
        "Deep Research: %d task(s) gated, caps max_iter=%d "
        "max_execution_time=%ds run_wall_clock=%ds retries=%d",
        len(pending),
        profile.max_iter,
        profile.max_execution_time,
        profile.run_wall_clock,
        profile.guardrail_max_retries,
    )


def _apply_agent_budget(spec: Dict[str, Any], profile) -> None:
    """Per-agent caps, unless the generator set its own.

    Only fills what is absent: an explicit value in the plan is a decision, and
    a mode default should not overrule it.
    """
    spec.setdefault("max_iter", profile.max_iter)
    spec.setdefault("max_execution_time", profile.max_execution_time)


def _apply_task_verification(
    entry: Dict[str, Any],
    source: Dict[str, Any],
    profile,
) -> None:
    """Turn on the verification that was already generated for this task."""
    # THE two-line fix the audit found: the guardrail exists, has always
    # existed, and was simply never copied into the task entry.
    guardrail = source.get("llm_guardrail")
    if guardrail:
        entry["llm_guardrail"] = guardrail

    entry["max_retries"] = profile.guardrail_max_retries

    # Degrade rather than abort. Losing a six-task run because task four could
    # not satisfy a judge on the third attempt — or because one agent blew its
    # clock — throws away everything already produced. The failure stays
    # visible: the output is annotated, and the guardrail/budget events fire
    # either way.
    entry["guardrail_on_exhausted"] = "degrade"
    entry["on_budget_exceeded"] = "degrade"

    # Every deep task answers in the same envelope, and a free, deterministic
    # rule on that envelope decides whether the next task may proceed.
    entry["output_schema"] = copy.deepcopy(DEEP_RESEARCH_ENVELOPE_SCHEMA)
    entry["output_schema_name"] = "DeepResearchEnvelope"
    entry["gate"] = copy.deepcopy(DEFAULT_DEEP_GATE)
    entry["description"] = _with_envelope_instruction(entry.get("description", ""))
    # output_file defaults to a .md path; with a JSON contract the artifact is
    # JSON, and the engine writes json_dict when output_json is set.
    output_file = entry.get("output_file")
    if isinstance(output_file, str) and output_file.endswith(".md"):
        entry["output_file"] = output_file[:-3] + ".json"


def _with_envelope_instruction(description: str) -> str:
    """Tell the agent about the contract up front.

    The engine appends the schema to ``expected_output`` at execution time, but
    that is a formatting instruction. This is the part that changes the WORK:
    an agent that knows every claim needs a source gathers sources as it goes
    instead of reverse-engineering citations at the end to satisfy a gate.
    """
    return (
        f"{description}\n\n"
        "=== DEEP RESEARCH CONTRACT ===\n"
        "Answer as a JSON object with `summary`, `findings`, `open_questions` "
        "and `limitations`.\n"
        "Every entry in `findings` needs a specific `claim`, the `evidence` "
        "behind it, the `source` it came from (a URL or tool result you "
        "actually consulted), and your `confidence` from 0 to 1.\n"
        "Do not invent sources. If you cannot source a claim, leave it out and "
        "record the gap in `open_questions` instead."
    ).strip()


def _apply_deep_tool_policy(spec: Dict[str, Any]) -> None:
    """Point the slow high-value tools at their deep settings.

    ADDITIVE only. An earlier version also stripped these tools from research
    runs — the inverse of the export template's FAST_MODE_DISABLED_TOOLS. That
    was removed: taking a tool away from a mode nobody asked to change is a
    behaviour regression wearing a consistency argument. Deep upgrades what it
    has; no other mode is touched.
    """
    tools = spec.get("tools")
    if not isinstance(tools, list):
        return

    if any(str(tool) == PERPLEXITY_TOOL_ID for tool in tools):
        configs = spec.setdefault("tool_configs", {})
        existing = configs.get("PerplexityTool")
        configs["PerplexityTool"] = {
            **(existing if isinstance(existing, dict) else {}),
            **_PERPLEXITY_DEEP_CONFIG,
        }
        logger.info("Deep mode: PerplexityTool set to sonar-deep-research")
