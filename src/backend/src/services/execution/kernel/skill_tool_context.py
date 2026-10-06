"""Skills that feed a TOOL parameter, not the agent's prompt.

Some know-how is consumed by a tool's own LLM call rather than by the agent:
the UC Metric View Generator's DAX→SQL translation reads ``domain_context``
inside the tool, where the agent's ``<available_skills>`` block never reaches.
A skill tagged ``metadata: {kasal-tool-param: domain_context}`` is how a
workspace authors that context once, in the Skills section, and attaches it to
the agent like any other skill — this module hands its body to the tool.

Two seams, because tools reach an agent two ways: the agent's own tools (built
with it, in ``inject_skills``) and a task's tools (built later, in
``build_task_args``). The resolved context rides on the built agent between
the two, the same way the Genie MCP space id does.
"""

from typing import Any, Dict, Iterable, List

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

#: The skill-metadata key naming the tool parameter a skill's body feeds.
TOOL_PARAM_METADATA_KEY = "kasal-tool-param"

#: Attribute on the built agent carrying ``{param: text}`` to the task seam.
AGENT_ATTR = "_kasal_skill_tool_context"

#: Parameters a skill may feed, and nothing else — a skill cannot overwrite a
#: credential or a warehouse id by naming it.
SKILL_FED_PARAMS = ("domain_context",)


def _metadata(skill: Any) -> Dict[str, Any]:
    meta = getattr(skill, "skill_metadata", None)
    if meta is None:
        meta = getattr(skill, "metadata", None)
    return meta if isinstance(meta, dict) else {}


def collect_tool_context(skills: Iterable[Any]) -> Dict[str, str]:
    """``{param: text}`` from every skill tagged for a tool parameter.

    Several skills tagged for the same parameter are joined in resolution
    order, each under its own heading, so a model-specific skill and a
    company-wide glossary can both be attached.
    """
    parts: Dict[str, List[str]] = {}
    for skill in skills:
        param = str(_metadata(skill).get(TOOL_PARAM_METADATA_KEY) or "").strip()
        body = (getattr(skill, "body", "") or "").strip()
        if param not in SKILL_FED_PARAMS or not body:
            continue
        parts.setdefault(param, []).append(
            f"<!-- skill: {getattr(skill, 'name', '?')} -->\n{body}"
        )
    return {param: "\n\n".join(texts) for param, texts in parts.items()}


def apply_tool_context(
    tools: Iterable[Any], context: Dict[str, str], *, label: str = ""
) -> int:
    """Write the skill context into every tool that takes the parameter.

    Only tools whose ``_default_config`` is the config they read (the UCMV
    generator and drift monitor) and that declare the parameter in
    ``skill_context_params``. An inline value already on the tool — a task
    configured before skills held this — is KEPT and appended after the skill
    text, so a crew does not silently lose context it was saved with.

    Idempotent: the same tool instance can sit in both the agent's and the
    task's list, and applying twice must not duplicate the text.
    """
    if not context:
        return 0
    applied = 0
    for tool in tools or []:
        accepts = getattr(tool, "skill_context_params", ()) or ()
        config = getattr(tool, "_default_config", None)
        if not accepts or not isinstance(config, dict):
            continue
        base = getattr(tool, "_kasal_inline_context", None)
        if base is None:
            base = {p: config.get(p) for p in accepts}
            object.__setattr__(tool, "_kasal_inline_context", base)
        for param, text in context.items():
            if param not in accepts:
                continue
            inline = base.get(param)
            inline = inline.strip() if isinstance(inline, str) else ""
            config[param] = (
                f"{text}\n\n{inline}" if inline and inline not in text else text
            )
            applied += 1
            logger.info(
                "[skills] fed %d chars of skill context into %s.%s for agent '%s'%s",
                len(text),
                getattr(tool, "name", type(tool).__name__),
                param,
                label,
                (
                    " (inline value kept after it)"
                    if inline and inline not in text
                    else ""
                ),
            )
    return applied


def apply_agent_tool_context(tools: Iterable[Any], agent: Any) -> int:
    """The task seam: feed a task's tools the context its agent resolved."""
    context = getattr(agent, AGENT_ATTR, None) or {}
    label = getattr(agent, "_agent_key", None) or getattr(agent, "role", "")
    return apply_tool_context(tools, context, label=str(label))
