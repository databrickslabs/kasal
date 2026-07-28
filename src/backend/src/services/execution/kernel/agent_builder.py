from src.utils.model_config import DEFAULT_ENGINE_MODEL

"""Shared agent-build logic used by BOTH the crew path
(``agent_adapter.create_agent``) and the flow path
(``flow.modules.agent_adapter``).

Single source of truth for:
- ``build_agent_llm`` — build a CrewAI LLM via ``LLMManager.configure_kasal_llm``
  with explicit ``group_id`` (multi-tenant isolation) + temperature override, the
  exact approach the crew path uses. A flow is just composed crews, so flow agents
  must build their LLMs the same way rather than via a divergent ``get_llm`` call.
- ``build_agent_kwargs`` — assemble the kwargs dict passed to ``crewai.Agent``.

The security preamble is injected by the CALLER
(``agent_adapter.inject_security_preamble``) right after ``build_agent_kwargs``
returns, so each path keeps its own ``[SECURITY]`` log line while the injection
itself stays shared (Phase 1).
"""
import os
import re
from typing import Any, Dict, List, Optional

from src.services.execution.runtime import Agent

from src.core.logger import LoggerManager
from src.services.execution.kernel.agent_security import inject_security_preamble

logger = LoggerManager.get_instance().crew


def redact_llm_repr(llm: Any) -> str:
    """LLM repr safe for user-downloadable execution logs.

    CrewAI's LLM repr prints ``api_key='dapi…'`` in cleartext; execution logs
    are downloadable from the UI, so the credential must never land in them.
    Shared by both the crew and flow agent builders.
    """
    return re.sub(r"api_key='[^']*'", "api_key='***REDACTED***'", repr(llm))


# Optional Agent params copied from the spec verbatim when present and not None.
# Mirrors agent_adapter.create_agent / flow agent_adapter exactly.
# NOTE: 'reasoning' / 'max_reasoning_attempts' are deliberately NOT here. Kasal's
# reasoning control is no longer a prompted plan/replan loop — it maps onto the
# MODEL's native reasoning budget on the agent's own LLM (see
# _apply_reasoning_effort below), so there is nothing agent-shaped to pass through.
_ADDITIONAL_AGENT_PARAMS = [
    'max_iter', 'max_rpm', 'max_execution_time', 'code_execution_mode',
    'max_context_window_size', 'max_tokens',
    # Date awareness settings (CrewAI 1.9+) — inject current date into agent context
    'inject_date', 'date_format',
]

# Effort used when the reasoning toggle is on but no explicit level was chosen.
DEFAULT_REASONING_EFFORT = 'low'

#: Seconds one agent may spend inside a single LLM call chain before the engine
#: stops it (``src.core.llm.transport.completion._execution_budget``). 0 disables it.
#:
#: The round cap alone was not enough. An agent searching for something its
#: knowledge base does not contain rephrases the query every round; with
#: max_iter=25 and ~5s per search that is two minutes of a run producing
#: nothing, and the failure it eventually raises ("Tool-calling did not converge
#: within 25 rounds") describes the symptom rather than the cause. A wall clock
#: bounds the wasted time regardless of how cheap each round is.
#:
#: Generous on purpose: a legitimate research turn with slow tools must not be
#: cut off. An explicit ``max_execution_time`` on the agent always wins.
DEFAULT_AGENT_MAX_EXECUTION_TIME = int(
    os.getenv("KASAL_AGENT_MAX_EXECUTION_TIME", "900") or 0
)


def _apply_reasoning_effort(llm: Any, spec: Dict[str, Any], label: str = "") -> None:
    """Put the resolved reasoning budget on the agent's own LLM, when supported.

    Kasal's reasoning control (sidebar "Reasoning" → effort) is a THINKING BUDGET,
    not a planner: the engine has no plan/replan loop. ``reasoning_effort`` is
    emitted by ``src.core.llm.transport.completion`` as ``reasoning_effort`` on Chat
    Completions and as ``reasoning: {"effort": ...}`` on the Responses API.

    Capability-gated: models that do not accept the parameter would 400 on strict
    gateways, so for them the effort is dropped with a debug log and the run
    behaves exactly as before. Never raises — a reasoning preference must not be
    able to fail an execution.
    """
    if llm is None or isinstance(llm, str):
        return
    if not spec.get('reasoning'):
        return
    try:
        _apply_reasoning_effort_unsafe(llm, spec, label)
    except Exception as e:  # noqa: BLE001 — a reasoning preference must never fail a run
        logger.debug(f"Could not apply reasoning effort for agent {label}: {e}")


def _apply_reasoning_effort_unsafe(llm: Any, spec: Dict[str, Any], label: str) -> None:
    """Body of :func:`_apply_reasoning_effort`; see it for the contract."""
    from src.utils.model_config import (
        VALID_REASONING_EFFORTS,
        model_supports_reasoning_effort,
    )

    rc = spec.get('reasoning_config') or {}
    effort = (rc.get('reasoning_effort') or DEFAULT_REASONING_EFFORT)
    effort = str(effort).strip().lower()
    if effort not in VALID_REASONING_EFFORTS:
        logger.debug(
            f"Ignoring unknown reasoning effort {effort!r} for agent {label}; "
            f"expected one of {VALID_REASONING_EFFORTS}"
        )
        return

    # Prefer the RESOLVED provider model on the built LLM (e.g. 'databricks/gpt-5-2',
    # 'openai/gpt-5.2') and fall back to the spec's model key.
    model_name = getattr(llm, 'model', None)
    if not isinstance(model_name, str) or not model_name:
        spec_llm = spec.get('llm')
        model_name = spec_llm.get('model') if isinstance(spec_llm, dict) else spec_llm

    if not model_supports_reasoning_effort(model_name):
        # INFO, not debug: the user explicitly asked for a reasoning budget and
        # is not getting one. At debug this was invisible, so the setting looked
        # applied and the run looked identical — with nothing anywhere saying why.
        logger.info(
            f"Model {model_name!r} has no native reasoning budget — "
            f"reasoning_effort={effort!r} has no effect for agent {label}"
        )
        return

    llm.reasoning_effort = effort
    logger.info(
        f"Reasoning budget for agent {label}: reasoning_effort={effort!r} on model {model_name}"
    )


async def build_agent_llm(
    spec: Dict[str, Any],
    *,
    group_id: Optional[str],
    default_model: str = DEFAULT_ENGINE_MODEL,
    label: str = "",
) -> Any:
    """Build a CrewAI-compatible LLM for an agent the way the crew path always has.

    ``spec`` is a plain dict that may carry ``llm`` (model-name string or a
    ``{'model': ..., **overrides}`` dict) and ``temperature`` (raw 0-100, converted
    to 0.0-1.0 here). ``group_id`` is REQUIRED for multi-tenant isolation — a
    missing group raises rather than silently using an unscoped model.
    ``default_model`` is the per-path default (crew: gpt-4o; flow:
    databricks-llama-4-maverick). Returns the configured LLM, or a model-name
    fallback if configuration fails (never silently drops the isolation error).

    When ``spec['reasoning']`` is set, the resolved reasoning budget
    (``spec['reasoning_config']['reasoning_effort']``) is stamped onto the built
    LLM for models that natively support it — see ``_apply_reasoning_effort``.
    """
    from src.core.llm_manager import LLMManager

    llm = None
    try:
        if 'llm' in spec:
            if isinstance(spec['llm'], str):
                model_name = spec['llm']
                logger.info(f"Configuring agent {label} LLM using LLMManager for model: {model_name}")
                temperature = None
                if spec.get('temperature') is not None:
                    # Convert from 0-100 to 0.0-1.0 range
                    temperature = spec['temperature'] / 100.0
                    logger.info(f"Using temperature override {temperature} for agent {label}")
                if not group_id:
                    raise ValueError("group_id is REQUIRED for LLM configuration")
                llm = await LLMManager.configure_kasal_llm(model_name, group_id, temperature)
                logger.info(f"Successfully configured LLM for agent {label} using model: {model_name}")
            elif isinstance(spec['llm'], dict):
                llm_config = spec['llm']
                model_name = llm_config.get('model', default_model)
                temperature = None
                if spec.get('temperature') is not None:
                    temperature = spec['temperature'] / 100.0
                    logger.info(f"Using temperature override {temperature} for agent {label}")
                if not group_id:
                    raise ValueError("group_id is REQUIRED for LLM configuration")
                # LLMManager handles provider prefix, API key/base, DatabricksRetryLLM,
                # GPT-5 params, temperature-rejection, and telemetry headers.
                llm = await LLMManager.configure_kasal_llm(model_name, group_id, temperature)
                # Apply any additional overrides from llm_config (e.g. top_p, stop, max_tokens)
                skip_keys = {'model'}  # already handled by LLMManager
                for key, value in llm_config.items():
                    if key not in skip_keys and value is not None:
                        setattr(llm, key, value)
                logger.info(f"Created LLM instance for agent {label} with model {model_name}")
        else:
            # Use default model
            logger.info(f"No LLM specified for agent {label}, using default")
            if not group_id:
                raise ValueError("group_id is REQUIRED for LLM configuration")
            llm = await LLMManager.configure_kasal_llm(default_model, group_id)
    except ValueError:
        # Missing group_id is a multi-tenant isolation violation — never fall back
        # to an unscoped model string, surface it instead.
        raise
    except Exception as e:
        # Fallback to simple string if configuration fails
        logger.error(f"Error configuring LLM: {e}")
        llm = spec.get('llm', default_model)
        logger.warning(f"Using string model name as fallback for agent {label}: {llm}")

    # In an execution subprocess, opt the LLM into streamed completions so the
    # engine emits LLMStreamChunkEvent per delta — the event pipe forwards
    # coalesced chunks to the parent's SSE for live typing in the UI. Only
    # meaningful on the Chat Completions branch (the Responses-API branch
    # ignores the flag). Kill-switch: CREW_TOKEN_STREAMING=false.
    if (
        os.environ.get("CREW_SUBPROCESS_MODE", "").lower() == "true"
        and os.environ.get("CREW_TOKEN_STREAMING", "true").strip().lower()
        not in ("0", "false", "no")
        and llm is not None
        and not isinstance(llm, str)
    ):
        try:
            llm.stream = True
        except Exception as stream_err:  # noqa: BLE001
            logger.debug(f"Token streaming not enabled for agent {label}: {stream_err}")

    # Reasoning = the model's native thinking budget, applied to the agent's own LLM.
    _apply_reasoning_effort(llm, spec, label=label)

    return llm


def build_agent_kwargs(
    spec: Dict[str, Any],
    tools: List[Any],
    llm: Any,
    *,
    label: str = "",
) -> Dict[str, Any]:
    """Assemble the kwargs dict for ``crewai.Agent`` from a normalized agent spec.

    ``spec`` is a plain dict with crew-style keys (``role``/``goal``/``backstory``
    required; ``verbose``/``allow_delegation``/``cache``/``max_retry_limit`` and the
    optional params + templates read with the same defaults the crew path uses).
    Does NOT inject the security preamble — the caller does that next via
    ``inject_security_preamble`` so each path keeps its own log line.
    """
    agent_kwargs: Dict[str, Any] = {
        'role': spec['role'],
        'goal': spec['goal'],
        'backstory': spec['backstory'],
        'tools': tools or [],
        'llm': llm,
        'verbose': spec.get('verbose', True),
        'allow_delegation': spec.get('allow_delegation', False),
        'cache': spec.get('cache', False),
        # SECURITY: Always force allow_code_execution to False for safety
        'allow_code_execution': False,  # Hardcoded to False - ignoring spec
        'max_retry_limit': spec.get('max_retry_limit', 3),
        'use_system_prompt': True,
        'respect_context_window': True,
    }

    # NOTE: 'memory' is deliberately NOT propagated to the CrewAI Agent. In
    # CrewAI 1.10+, ``Agent(memory=True)`` builds a per-agent OpenAI-backed default
    # Memory that OVERRIDES the crew's configured Databricks/Lakebase Memory
    # (``getattr(agent, "memory") or self._memory``), so the memory tools would hit
    # OpenAI. Memory is configured once at the CREW level and inherited by agents.
    for param in _ADDITIONAL_AGENT_PARAMS:
        if spec.get(param) is not None:
            agent_kwargs[param] = spec[param]
            logger.info(f"Setting agent parameter '{param}' to {spec[param]} for agent {label}")

    # Bound the wasted time when an agent cannot converge — see
    # DEFAULT_AGENT_MAX_EXECUTION_TIME. Only when the spec did not set one.
    if 'max_execution_time' not in agent_kwargs and DEFAULT_AGENT_MAX_EXECUTION_TIME > 0:
        agent_kwargs['max_execution_time'] = DEFAULT_AGENT_MAX_EXECUTION_TIME
        logger.info(
            f"Applying default max_execution_time="
            f"{DEFAULT_AGENT_MAX_EXECUTION_TIME}s for agent {label}"
        )

    # NOTE: reasoning is NOT an Agent kwarg. Kasal's reasoning control is the model's
    # native thinking budget and is applied to the agent's LLM in build_agent_llm
    # (_apply_reasoning_effort). The engine has no plan/replan loop, so Agent's inert
    # planning / planning_config / reasoning fields are deliberately never set.

    # Handle prompt templates. CrewAI's Agent field names are system_template /
    # prompt_template / response_template — the old system_prompt / task_prompt /
    # format_prompt names are NOT Agent fields and were silently dropped by
    # Pydantic, so custom templates (and the security preamble) never reached the LLM.
    if spec.get('system_template'):
        agent_kwargs['system_template'] = spec['system_template']
    if spec.get('prompt_template'):
        agent_kwargs['prompt_template'] = spec['prompt_template']
    if spec.get('response_template'):
        agent_kwargs['response_template'] = spec['response_template']
    # CrewAI only honors custom templates when BOTH system_template and
    # prompt_template are present — supply a passthrough user template when only
    # the system one was configured.
    if agent_kwargs.get('system_template') and not agent_kwargs.get('prompt_template'):
        agent_kwargs['prompt_template'] = "{{ .Prompt }}"

    return agent_kwargs


async def build_agent(
    spec: Dict[str, Any],
    tools: List[Any],
    *,
    group_id: Optional[str],
    default_model: str = DEFAULT_ENGINE_MODEL,
    label: str = "",
    extra_kwargs: Optional[Dict[str, Any]] = None,
    custom_attrs: Optional[Dict[str, Any]] = None,
) -> Any:
    """Construct a fully-built ``crewai.Agent`` from a normalized spec + already-
    resolved tools. The SINGLE agent builder both paths call:
    ``agent_adapter.create_agent`` (crew) and
    ``flow.modules.agent_adapter.configure_agent_and_tools`` (flow). The paths
    differ only in how they SOURCE tools and normalize their native inputs into
    ``spec`` — the build itself lives here.

    Steps: build the LLM (``configure_kasal_llm`` with explicit group_id +
    temperature) → assemble kwargs → inject the security preamble → merge any
    path-specific ``extra_kwargs`` (e.g. flow's ``config``) → construct the Agent
    → set path-specific ``custom_attrs`` (e.g. ``_agent_key`` /
    ``_kasal_memory_disabled``).
    """
    llm = await build_agent_llm(
        spec, group_id=group_id, default_model=default_model, label=label
    )
    # LLM repr includes the api_key; execution logs are user-downloadable → redact.
    logger.info(f"Final LLM configuration for agent {label}: {redact_llm_repr(llm)}")

    agent_kwargs = build_agent_kwargs(spec, tools, llm, label=label)

    # SECURITY: prompt-injection hardening preamble (shared, so the two paths
    # can never diverge).
    injected_into = inject_security_preamble(agent_kwargs)
    logger.info(
        f"[SECURITY] preamble injected into {injected_into} for agent '{label}', "
        f"starts with: {agent_kwargs[injected_into][:300]!r}"
    )

    if extra_kwargs:
        agent_kwargs.update(extra_kwargs)

    agent = Agent(**agent_kwargs)
    # Path-specific custom attributes set via object.__setattr__ to bypass
    # Pydantic validation (e.g. _agent_key for crew, _kasal_memory_disabled for flow).
    for attr, value in (custom_attrs or {}).items():
        object.__setattr__(agent, attr, value)
    return agent
