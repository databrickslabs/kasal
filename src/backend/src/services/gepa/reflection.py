"""GEPA reflection bridge — the sync/async seam GEPA runs across.

GEPA drives the optimizer from a worker THREAD with no event loop, so every LLM
call it makes has to cross back into Kasal's async ``LLMManager``. That bridge,
plus the judge-sampling policy it carries, is what lives here.

Extracted from ``prompt_optimization_service``; the template and crew runners
both need it, and duplicating a bridge that owns shared module state
(``_GEPA_REFLECTION_STATE``) would give the two runners different bridges."""

import asyncio
import hashlib
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.core.exceptions import BadRequestError
from src.repositories.log_repository import LLMLogRepository
from src.repositories.model_config_repository import ModelConfigRepository
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


# Judge sampling: how many times the correctness judge grades one deliverable
# before the grades are reduced by MEDIAN. 1 restores single-draw behavior
# (and takes a no-op path — no extra calls, no median arithmetic).
DEFAULT_JUDGE_SAMPLES = 3


_JUDGE_SYSTEM = """You judge an intent classifier for a CrewAI workflow designer.
Routing rules: the default intent is "generate_crew" (research, analysis, reporting,
multi-step or goal-oriented requests, and any message with 2+ action verbs).
"generate_agent" only when ONE agent/bot/assistant/chatbot is explicitly the entity
created; "generate_task" only when a task is explicitly created; "execute_crew" for
run/execute/start/launch; "configure_crew" for model/tools/settings changes.
Given the user message and the predicted intent, answer with EXACTLY one word:
CORRECT or WRONG."""


# Per-worker-thread override consumed by the patched gepa.optimize below.
_GEPA_REFLECTION_STATE = threading.local()


def _install_gepa_reflection_bridge() -> None:
    """Route GEPA's reflection LM through LLMManager (idempotent).

    MLflow's GepaPromptOptimizer pins `reflection_lm` to a litellm model
    STRING in the kwargs it builds last, so a callable cannot be injected via
    gepa_kwargs. gepa itself accepts any callable conforming to its
    LanguageModel protocol; this one-time patch swaps in the callable armed on
    the current worker thread (each optimization run owns one thread, so
    concurrent runs cannot cross wires).
    """
    import gepa

    if getattr(gepa.optimize, "_kasal_reflection_bridge", False):
        return
    original = gepa.optimize

    def bridged(*args, **kwargs):
        override = getattr(_GEPA_REFLECTION_STATE, "reflection_fn", None)
        if override is not None:
            kwargs["reflection_lm"] = override
        return original(*args, **kwargs)

    bridged._kasal_reflection_bridge = True
    gepa.optimize = bridged


def _make_reflection_fn(
    loop: asyncio.AbstractEventLoop,
    model: str,
    group_context: Optional[GroupContext],
    user_token: Optional[str],
):
    """Build GEPA's reflection callable, backed by LLMManager.

    `model` is a plain Kasal model key — LLMManager resolves provider,
    endpoint, group-scoped API key, and request quirks (it drops temperature
    for Kimi, which 400s on any explicit value)."""

    def reflection_fn(prompt: Any) -> str:
        if isinstance(prompt, list):
            messages = list(prompt)
        else:
            messages = [{"role": "user", "content": str(prompt)}]
        # Cache-buster: the reflection prompt is byte-identical every
        # iteration in the 1-example regime, and the process-global litellm
        # cache would replay one cached proposal forever (observed live:
        # duration=0.00s, identical candidates). A unique system line keeps
        # each request distinct without touching the task content.
        messages = [
            {
                "role": "system",
                "content": f"reflection request {uuid.uuid4().hex}",
            }
        ] + messages
        return _sync_llm_completion(
            loop,
            messages=messages,
            model=model,
            # Forced-thinking models (Kimi K2.x) spend heavily on reasoning
            # before the visible document; starving them returns empty text.
            max_tokens=6000,
            group_context=group_context,
            user_token=user_token,
            # Proposal diversity. LLMManager drops the param for providers
            # that reject it — no per-provider branching here.
            temperature=0.8,
        )

    return reflection_fn


def _judge_sample_count() -> int:
    """How many times to sample the correctness judge (GEPA_JUDGE_SAMPLES).

    Bounded to 1-9: sampling multiplies judge cost per DISTINCT candidate, and
    1 is an explicit opt-out that must not pay any median overhead.
    """
    raw = os.getenv("GEPA_JUDGE_SAMPLES")
    if raw is None or not str(raw).strip():
        return DEFAULT_JUDGE_SAMPLES
    try:
        return max(1, min(9, int(str(raw).strip())))
    except (TypeError, ValueError):
        logger.warning(
            f"Ignoring non-integer GEPA_JUDGE_SAMPLES={raw!r}; "
            f"using {DEFAULT_JUDGE_SAMPLES}"
        )
        return DEFAULT_JUDGE_SAMPLES


def _preflight_reflection(
    loop: asyncio.AbstractEventLoop,
    model: str,
    group_context: Optional[GroupContext],
    user_token: Optional[str],
) -> None:
    """Tiny ping of GEPA's reflection model BEFORE any budget is spent.

    A dead reflection model doesn't fail a run — GEPA just proposes zero
    candidates and 'completes' at the baseline after burning the whole
    execution budget (observed live with a retired provider model name).
    Runs through LLMManager like every other call.
    """
    try:
        _sync_llm_completion(
            loop,
            messages=[{"role": "user", "content": "ping — reply with OK"}],
            model=model,
            # >= 16: Responses-API models (GPT-5/Codex family) reject smaller
            # max_output_tokens outright; LLMManager floors this too, but the
            # preflight should not depend on the safety net.
            max_tokens=32,
            group_context=group_context,
            user_token=user_token,
        )
    except Exception as e:
        raise ValueError(
            f"Reflection model '{model}' failed a test call — the "
            f"optimization cannot generate candidates with it. Pick a different "
            f"reflection model. Provider error: {str(e)[:300]}"
        )


def _sync_llm_completion(
    loop: asyncio.AbstractEventLoop,
    messages: List[Dict[str, str]],
    model: str,
    max_tokens: int,
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None,
    temperature: float = 0.0,
) -> str:
    """Run LLMManager.completion from a worker thread by submitting it to the
    MAIN event loop. Never run it on a fresh loop (asyncio.run) — the app's
    async DB engine is bound to the main loop, and cross-loop access deadlocks
    holding the DB lock, wedging every request in the process.

    The submitted Task does not inherit this thread's contextvars, so the
    request's UserContext (group id + OBO token, which LLMManager needs for
    API-key lookups) is re-established inside the coroutine.
    """
    from src.core.llm_manager import LLMManager
    from src.utils.telemetry import KasalProduct, get_user_agent_header
    from src.utils.user_context import UserContext

    async def _with_context() -> str:
        if group_context:
            UserContext.set_group_context(group_context)
        if user_token:
            UserContext.set_user_token(user_token)
        return await LLMManager.completion(
            messages=messages,
            model=model,
            # 0.0 for deterministic judging/distillation; reflection passes
            # 0.8 for proposal diversity. LLMManager owns per-provider quirks
            # (it drops the param entirely for models that reject it).
            temperature=temperature,
            max_tokens=max_tokens,
            extra_headers=get_user_agent_header(KasalProduct.PROMPT_IMPROVEMENT),
        )

    future = asyncio.run_coroutine_threadsafe(_with_context(), loop)
    return future.result(timeout=300)


def _sync_run_crew(
    loop: asyncio.AbstractEventLoop,
    agents_yaml: Dict[str, Any],
    tasks_yaml: Dict[str, Any],
    model: str,
    timeout: int,
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None,
) -> str:
    """Execute a crew (candidate prompt fields already applied) from a worker
    thread by submitting to the MAIN loop, then poll to a terminal state.
    Returns the final result text, or '' on failure/timeout (scored 0)."""

    async def _run() -> str:
        from src.schemas.execution import CrewConfig
        from src.services.execution_service import ExecutionService
        from src.utils.user_context import UserContext

        if group_context:
            UserContext.set_group_context(group_context)
        if user_token:
            UserContext.set_user_token(user_token)

        service = ExecutionService()
        created = await service.create_execution(
            CrewConfig(
                agents_yaml=agents_yaml,
                tasks_yaml=tasks_yaml,
                inputs={},
                model=model,
                execution_type="crew",
            ),
            None,
            group_context,
        )
        execution_id = created.get("execution_id")
        if not execution_id:
            return ""
        group_ids = group_context.group_ids if group_context else []
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(10)
            # This coroutine runs as a background task with no request session —
            # open a fresh one per poll (background work owns its sessions).
            from src.db.session import async_session_factory

            async with async_session_factory() as poll_session:
                status = await ExecutionService(
                    session=poll_session
                ).get_execution_status(execution_id, group_ids)
            if not status:
                # Row may not be visible yet right after creation — keep polling.
                continue
            state = str(status.get("status", "")).upper()
            if state in ("COMPLETED", "FAILED", "CANCELLED", "ERROR", "STOPPED"):
                if state != "COMPLETED":
                    return ""
                result = status.get("result")
                if isinstance(result, dict):
                    return str(
                        result.get("result")
                        or result.get("output")
                        or result.get("text")
                        or result
                    )
                return str(result or "")
        logger.warning(f"Crew optimization eval timed out for execution {execution_id}")
        return ""

    future = asyncio.run_coroutine_threadsafe(_run(), loop)
    return future.result(timeout=timeout + 120)
