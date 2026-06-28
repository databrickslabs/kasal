"""Shared agent-build logic used by BOTH the crew path
(``agent_adapter.create_agent``) and the flow path
(``flow.modules.agent_adapter``).

Single source of truth for:
- ``build_agent_llm`` — build a CrewAI LLM via ``LLMManager.configure_crewai_llm``
  with explicit ``group_id`` (multi-tenant isolation) + temperature override, the
  exact approach the crew path uses. A flow is just composed crews, so flow agents
  must build their LLMs the same way rather than via a divergent ``get_llm`` call.
- ``build_agent_kwargs`` — assemble the kwargs dict passed to ``crewai.Agent``.

The security preamble is injected by the CALLER
(``agent_adapter.inject_security_preamble``) right after ``build_agent_kwargs``
returns, so each path keeps its own ``[SECURITY]`` log line while the injection
itself stays shared (Phase 1).
"""
import re
from typing import Any, Dict, List, Optional

from crewai import Agent

from src.core.logger import LoggerManager
from src.engines.crewai.kernel.agent_security import inject_security_preamble

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
# NOTE: 'reasoning' / 'max_reasoning_attempts' are deliberately NOT here — they are
# deprecated in CrewAI 1.14.x. Reasoning is configured via an explicit, bounded
# PlanningConfig (see _apply_reasoning_planning_config below).
_ADDITIONAL_AGENT_PARAMS = [
    'max_iter', 'max_rpm', 'max_execution_time', 'code_execution_mode',
    'max_context_window_size', 'max_tokens',
    # Date awareness settings (CrewAI 1.9+) — inject current date into agent context
    'inject_date', 'date_format',
]

# Bounded defaults for the agent-reasoning loop (CrewAI 1.14.5 PlanningConfig).
# CrewAI's OWN defaults are unbounded/expansive — max_attempts=None (refine forever),
# max_steps=20, max_step_iterations=15, step_timeout=None, max_replans=3, i.e. up to
# ~300 LLM turns with NO wall-clock cap — which lets a mistooled agent run effectively
# forever. These are Kasal's shipping defaults; each is overridable per crew/agent via
# the spec's 'reasoning_config' dict (wired from the sidebar Reasoning section).
# Validated against the real databricks-claude-haiku-4-5: this low/small profile
# made a non-converging agent (tool returning no usable data) STOP ON ITS OWN in
# ~15s with a synthesized final answer (1 reasoning pass, 5 LLM calls, 0 replans).
# The previous medium/large profile (effort=medium, steps=10, iter=10, replans=2)
# fanned out enough calls to trip the workspace FMAPI rate limit and retry-loop for
# minutes — the "runs forever" symptom. Keep this small so reasoning self-terminates
# and stays under the rate limit; users can raise it per crew in the sidebar.
DEFAULT_REASONING_CONFIG = {
    'reasoning_effort': 'low',      # linear execution, NO replanning re-runs (fastest)
    'max_attempts': 1,              # plan-refinement cap (None = forever)
    'max_steps': 3,                 # steps in the generated plan
    'max_step_iterations': 3,       # LLM turns per step
    'step_timeout': 20,             # per-step wall-clock seconds (None = no cap)
    'max_replans': 0,               # no full replanning attempts
}


def _build_planning_config(spec: Dict[str, Any]) -> Any:
    """Build a bounded CrewAI ``PlanningConfig`` for agent reasoning from the spec.

    Kasal's reasoning toggle (``spec['reasoning']``) plus an optional
    ``spec['reasoning_config']`` override dict (effort + step/replan caps) map onto
    CrewAI's PlanningConfig. We pass this explicitly instead of the deprecated
    ``reasoning=True`` flag, which would auto-migrate to a PlanningConfig with the
    expansive defaults above. Falls back to ``max_reasoning_attempts`` (legacy/flow
    field) for the refine cap when present.
    """
    from crewai.agent.planning_config import PlanningConfig

    rc = spec.get('reasoning_config') or {}
    d = DEFAULT_REASONING_CONFIG

    # max_attempts: reasoning_config wins, then legacy max_reasoning_attempts, then default.
    max_attempts = rc.get('max_attempts')
    if max_attempts is None:
        max_attempts = spec.get('max_reasoning_attempts')
    if max_attempts is None:
        max_attempts = d['max_attempts']

    # max_replans / step_timeout accept 0 / None as meaningful, so check membership.
    max_replans = rc['max_replans'] if rc.get('max_replans') is not None else d['max_replans']
    step_timeout = rc['step_timeout'] if 'step_timeout' in rc else d['step_timeout']

    return PlanningConfig(
        reasoning_effort=rc.get('reasoning_effort') or d['reasoning_effort'],
        max_attempts=max_attempts,
        max_steps=rc.get('max_steps') or d['max_steps'],
        max_step_iterations=rc.get('max_step_iterations') or d['max_step_iterations'],
        step_timeout=step_timeout,
        max_replans=max_replans,
    )


async def build_agent_llm(
    spec: Dict[str, Any],
    *,
    group_id: Optional[str],
    default_model: str = "gpt-4o",
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
                llm = await LLMManager.configure_crewai_llm(model_name, group_id, temperature)
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
                llm = await LLMManager.configure_crewai_llm(model_name, group_id, temperature)
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
            llm = await LLMManager.configure_crewai_llm(default_model, group_id)
    except ValueError:
        # Missing group_id is a multi-tenant isolation violation — never fall back
        # to an unscoped model string, surface it instead.
        raise
    except Exception as e:
        # Fallback to simple string if configuration fails
        logger.error(f"Error configuring LLM: {e}")
        llm = spec.get('llm', default_model)
        logger.warning(f"Using string model name as fallback for agent {label}: {llm}")

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

    # Agent reasoning (CrewAI 1.14.x). The deprecated reasoning=True flag auto-migrates
    # to a PlanningConfig with expansive defaults (unbounded refine loop, up to ~300 step
    # iterations, no wall-clock cap). We pass an explicit, bounded PlanningConfig instead
    # — and do NOT set reasoning/max_reasoning_attempts (passing both makes CrewAI drop the
    # cap). Shared builder => crew and flow get identical, bounded reasoning behavior.
    if spec.get('reasoning'):
        agent_kwargs['planning_config'] = _build_planning_config(spec)
        logger.info(
            f"Reasoning enabled for agent {label}: planning_config={agent_kwargs['planning_config']!r}"
        )

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
    default_model: str = "gpt-4o",
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

    Steps: build the LLM (``configure_crewai_llm`` with explicit group_id +
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
