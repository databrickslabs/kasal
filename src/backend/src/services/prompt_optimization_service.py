"""
Service for prompt optimization operations.

Runs GEPA (via mlflow.genai.optimize_prompts) over a seeded meta-prompt:
training examples are mined from the LLM interaction log (or supplied
inline), the current effective template is registered in the MLflow
Prompt Registry, GEPA searches for a better template against scorers,
and the winner is stored on the run for explicit review-and-apply as a
group-scoped template override (never the base row — the seeder
overwrites base rows on startup).

Run state lives in an in-process registry: the optimization itself is
recorded durably in MLflow, but Kasal-side status is lost on backend
restart. A `prompt_optimization_runs` table can replace the registry
when this graduates from Phase 1.
"""

import asyncio
import hashlib
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.repositories.log_repository import LLMLogRepository
from src.repositories.model_config_repository import ModelConfigRepository
from src.schemas.prompt_optimization import PromptOptimizationRequest
from src.schemas.template import PromptTemplateUpdate
from src.services.template_service import TemplateService
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

# Same fallback chain as the dispatcher: intent classification rides a fast model.
DEFAULT_TARGET_MODEL = os.getenv(
    "DEFAULT_DISPATCHER_MODEL", "databricks-llama-4-maverick"
)

MIN_EXAMPLES = 5


def _extract_user_from_log(prompt: str) -> Optional[str]:
    """Generation services log 'System: <template>\\nUser: <request>' — return
    the user request part, or None when the marker is absent."""
    if "\nUser: " not in prompt:
        return None
    return prompt.split("\nUser: ", 1)[1].strip() or None


# ── Crew optimization: the crew's prompt fields travel through GEPA as ONE
# labeled text document (GEPA mutates plain templates). Line-based parse so
# multi-line field values survive the round trip.
_CREW_DOC_FIELD_LABELS = {
    "ROLE": "role",
    "GOAL": "goal",
    "BACKSTORY": "backstory",
    "DESCRIPTION": "description",
    "EXPECTED_OUTPUT": "expected_output",
}


def _serialize_crew_doc(agents: List[Any], tasks: List[Any]) -> tuple:
    """Serialize crew prompt fields into a labeled document + the key set.

    Returns (doc, field_keys) where keys look like 'agent.<id>.role'.
    """
    lines: List[str] = []
    keys: List[str] = []
    for agent in agents:
        lines.append(f"[AGENT {agent.id}]")
        for label, field in (
            ("ROLE", "role"),
            ("GOAL", "goal"),
            ("BACKSTORY", "backstory"),
        ):
            lines.append(f"{label}: {str(getattr(agent, field, '') or '')}")
            keys.append(f"agent.{agent.id}.{field}")
        lines.append("")
    for task in tasks:
        lines.append(f"[TASK {task.id}]")
        for label, field in (
            ("DESCRIPTION", "description"),
            ("EXPECTED_OUTPUT", "expected_output"),
        ):
            lines.append(f"{label}: {str(getattr(task, field, '') or '')}")
            keys.append(f"task.{task.id}.{field}")
        lines.append("")
    return "\n".join(lines).strip(), keys


def _parse_crew_doc(doc: str) -> Optional[Dict[str, str]]:
    """Parse a (possibly GEPA-mutated) crew document back into field values.

    Returns {key: text} or None when the document lost its structure —
    callers score such candidates 0 WITHOUT executing the crew.
    """
    doc = (doc or "").strip()
    # Fence rescue: reflection models sometimes wrap the document in markdown
    # code fences that survive gepa's extraction. The content inside is a
    # perfectly good document — losing the candidate over the wrapper wastes
    # the proposal.
    if doc.startswith("```"):
        doc = re.sub(r"^```\S*\n?", "", doc)
        doc = re.sub(r"\n?```\s*$", "", doc)
    fields: Dict[str, str] = {}
    entity_prefix: Optional[str] = None
    current_key: Optional[str] = None
    for raw_line in (doc or "").splitlines():
        line = raw_line.strip()
        if line.startswith("[AGENT ") and line.endswith("]"):
            entity_prefix = f"agent.{line[len('[AGENT '):-1].strip()}"
            current_key = None
            continue
        if line.startswith("[TASK ") and line.endswith("]"):
            entity_prefix = f"task.{line[len('[TASK '):-1].strip()}"
            current_key = None
            continue
        matched = False
        for label, field in _CREW_DOC_FIELD_LABELS.items():
            if line.startswith(f"{label}:"):
                if entity_prefix is None:
                    return None
                current_key = f"{entity_prefix}.{field}"
                fields[current_key] = line[len(label) + 1 :].strip()
                matched = True
                break
        if matched:
            continue
        if line and current_key:
            fields[current_key] = f"{fields[current_key]}\n{line}".strip()
    return fields or None


# Categorical verdict scale for judges that answer in words rather than
# numbers (MLflow's judge template often does: 'Satisfactory', 'Partial', …).
_CATEGORICAL_GRADES = {
    1.0: ("excellent", "perfect", "outstanding", "flawless", "exceptional"),
    0.75: ("good", "great", "strong", "satisfactory", "yes", "true", "pass", "correct"),
    0.5: (
        "partial",
        "partially correct",
        "fair",
        "moderate",
        "average",
        "mixed",
        "acceptable",
    ),
    0.25: ("poor", "weak", "insufficient", "lacking"),
    0.0: (
        "bad",
        "fail",
        "failed",
        "wrong",
        "incorrect",
        "no",
        "false",
        "unsatisfactory",
    ),
}


def _judge_value_to_grade(value: Any) -> Optional[float]:
    """Normalize a judge verdict (number, bool, or categorical word) to 0-1.

    Returns None when the value carries no usable grade.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        number = float(value)
        return max(0.0, min(1.0, number / 10.0 if number > 1.0 else number))
    if isinstance(value, str):
        text = value.strip().lower()
        try:
            number = float(text)
            return max(0.0, min(1.0, number / 10.0 if number > 1.0 else number))
        except ValueError:
            pass
        for grade, words in _CATEGORICAL_GRADES.items():
            if text in words:
                return grade
    return None


def _distill_requirements(raw_notes: List[str], limit: int = 8) -> List[str]:
    """Collapse harvested human feedback into a deduplicated requirements list.

    The raw harvest repeats the same complaint many times ("french side" x8)
    and carries the grade numbers. Feeding that litany to the judge ANCHORED
    it — a compliant answer was graded 0/10 because every historical line said
    0.0 (verified live with an A/B judge experiment: same answer, litany
    rubric -> 0, requirements checklist -> 6). The judge needs constraints,
    not grade history.
    """
    requirements: List[str] = []
    seen: set = set()
    for note in raw_notes:
        text = str(note or "").strip()
        if not text:
            continue
        normalized = re.sub(r"[^a-z0-9 ]", "", text.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        requirements.append(text)
    return requirements[:limit]


def _pin_local_experiment() -> None:
    """Pin the MLflow experiment for judge/scorer operations.

    Scorers are PER-EXPERIMENT. The optimization runs pin the launch
    experiment ('kasal' by default), but a fresh worker's active experiment
    is Default/0 — a judge registered or listed there silently diverges from
    everything else (risk observed live while chasing a judge that never
    appeared). Every judge CRUD body must call this after set_tracking_uri.
    """
    import mlflow

    exp_name = os.environ.get("MLFLOW_EXPERIMENT_NAME") or "kasal"
    try:
        mlflow.set_experiment(exp_name)
    except Exception as exp_err:
        logger.warning(f"Could not pin experiment '{exp_name}': {exp_err}")


def _parse_requirement_lines(text: str) -> List[str]:
    """Parse 'R1. ...' numbered requirement lines from a distillation reply."""
    return [
        m.group(1).strip()
        for m in re.finditer(r"^\s*R\d+[.:]\s*(.+)$", text or "", re.MULTILINE)
        if m.group(1).strip()
    ]


def _checklist_grade(verdict: str, n_requirements: int) -> Optional[float]:
    """Compute a 0-1 grade from a checklist verdict's PASS/FAIL marks.

    The grade is COMPUTED from the marks, never taken from the model's own
    arithmetic — a judge writing "40" as its final number would clamp to a
    perfect 10/10 (observed live). Blend: 0.8 x fraction of requirements
    passed + 0.2 x the judge's base-quality Q mark (default 5 when absent),
    so requirement-equal candidates still order by answer quality.

    Returns None when no marks are found (caller falls back to last-number
    parsing).
    """
    marks = re.findall(r"\bR(\d+)\s*[:.]?\s*(PASS|FAIL)", verdict or "", re.IGNORECASE)
    if not marks or n_requirements <= 0:
        return None
    seen_marks: Dict[str, bool] = {}
    for num, mark in marks:
        # First mark per requirement wins (models sometimes restate at the end).
        seen_marks.setdefault(num, mark.upper() == "PASS")
    passed = sum(1 for ok in seen_marks.values() if ok)
    fraction = passed / max(n_requirements, len(seen_marks))
    quality = 0.5
    q_match = re.search(r"\bQ\s*[:.]?\s*(\d+(?:\.\d+)?)", verdict or "", re.IGNORECASE)
    if q_match:
        q_value = float(q_match.group(1))
        quality = max(0.0, min(10.0, q_value)) / 10.0
    return max(0.0, min(1.0, 0.8 * fraction + 0.2 * quality))


def _job_name_score(outputs: Any) -> float:
    """Format scorer for generate_job_name: a short plain-text name (2-4 words,
    no JSON/markdown artifacts)."""
    text = str(outputs or "").strip().strip('"').strip()
    if not text or "\n" in text or "{" in text or len(text) > 80:
        return 0.0
    words = len(text.split())
    return 1.0 if 2 <= words <= 4 else 0.5 if 1 <= words <= 6 else 0.0


# Per-template task wiring: where training inputs come from in the LLM log and
# how outputs are scored. Adding an entry here (plus a schema/UI listing) is
# all it takes to make another seeded template optimizable.
TEMPLATE_TASKS: Dict[str, Dict[str, Any]] = {
    "detect_intent": {
        # dispatcher logs the raw user message as `prompt` under this endpoint
        "log_endpoint": "detect-intent",
        "input_key": "message",
        "extract": None,
    },
    "generate_agent": {
        "log_endpoint": "generate-agent",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("name", "role", "goal", "backstory"),
        "judge_system": (
            "You judge an AI-agent generator. Given a user's request and the generated "
            "agent JSON (name/role/goal/backstory), decide if the agent is a faithful, "
            "specific, well-formed configuration for that request: the role matches the "
            "domain, the goal is concrete with an action verb, and the backstory is "
            "relevant professional expertise. Answer with EXACTLY one word: CORRECT or WRONG."
        ),
    },
    "generate_task": {
        "log_endpoint": "generate-task",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("name", "description", "expected_output"),
        "judge_system": (
            "You judge an AI-task generator. Given a user's request and the generated "
            "task JSON (name/description/expected_output), decide if the task is a "
            "faithful, specific, well-formed configuration for that request: the "
            "description covers context/objective/method and the expected output names "
            "a checkable deliverable and its structure. Answer with EXACTLY one word: "
            "CORRECT or WRONG."
        ),
    },
    "generate_crew": {
        "log_endpoint": "generate-crew",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("agents", "tasks"),
        "judge_system": (
            "You judge an AI-crew generator. Given a user's goal and the generated crew "
            "JSON (agents + tasks), decide if the crew is a faithful, minimal, "
            "well-formed plan for that goal: agents have specific roles matching the "
            "domain, every task is assigned to an existing agent, dependencies make "
            "sense, and together the tasks accomplish the goal. Answer with EXACTLY "
            "one word: CORRECT or WRONG."
        ),
    },
    "generate_crew_plan": {
        "log_endpoint": "generate-crew-plan",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("complexity", "process_type", "agents", "tasks"),
        "judge_system": (
            "You judge an AI-crew PLANNER that outputs a skeleton only (complexity, "
            "process_type, agent names/roles, task names with assignments). Given the "
            "user's goal and the plan JSON, decide if the outline is faithful and "
            "right-sized: the minimum agents needed, each task assigned to a listed "
            "agent, and the tasks together covering the goal's distinct actions. "
            "Answer with EXACTLY one word: CORRECT or WRONG."
        ),
    },
    "generate_job_name": {
        "log_endpoint": "generate-execution-name",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "format_fn": _job_name_score,
        "judge_system": (
            "You judge an AI job-run NAMER. Given a description of the agents/tasks "
            "involved and the generated name, decide if the name is a concise (2-4 "
            "word), descriptive title for that work — specific to the subject matter, "
            "no generic filler like 'AI Job' or 'Crew Run'. Answer with EXACTLY one "
            "word: CORRECT or WRONG."
        ),
    },
}

# The intent enum the detect_intent template contract allows.
VALID_INTENTS = {
    "generate_task",
    "generate_agent",
    "generate_crew",
    "execute_crew",
    "configure_crew",
    "unknown",
}

# In-process run registry (see module docstring for the durability tradeoff).
_RUNS: Dict[str, Dict[str, Any]] = {}
_MAX_KEPT_RUNS = 50

_PUBLIC_FIELDS = (
    "run_id",
    "template_name",
    "status",
    "dataset_size",
    "model",
    "initial_score",
    "final_score",
    "baseline_template",
    "optimized_template",
    "error",
    "applied",
    "created_at",
    "kind",
    "crew_id",
    "baseline_fields",
    "optimized_fields",
    "executions_used",
    "execution_cap",
    "human_feedback_count",
    "candidates_tried",
)


def _intent_format_score(outputs: Any) -> float:
    """Deterministic scorer: does the output honor the template's JSON contract?"""
    try:
        parsed = robust_json_parser(str(outputs))
    except Exception:
        return 0.0
    if not isinstance(parsed, dict):
        return 0.0
    score = 0.0
    if parsed.get("intent") in VALID_INTENTS:
        score += 0.6
    try:
        confidence = float(parsed.get("confidence"))
        if 0.0 <= confidence <= 1.0:
            score += 0.2
    except (TypeError, ValueError):
        pass
    if isinstance(parsed.get("extracted_info"), dict):
        score += 0.1
    if (
        isinstance(parsed.get("suggested_prompt"), str)
        and parsed["suggested_prompt"].strip()
    ):
        score += 0.1
    return score


def _json_keys_score(outputs: Any, required_keys: tuple) -> float:
    """Generic format scorer: output parses as a JSON object and every required
    key is present with a non-empty value (string, list, or object). Returns
    the satisfied fraction."""
    try:
        parsed = robust_json_parser(str(outputs))
    except Exception:
        return 0.0
    if not isinstance(parsed, dict):
        return 0.0

    def _ok(value: Any) -> bool:
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, dict)):
            return len(value) > 0
        return value is not None

    satisfied = sum(1 for key in required_keys if _ok(parsed.get(key)))
    return satisfied / len(required_keys) if required_keys else 0.0


_JUDGE_SYSTEM = """You judge an intent classifier for a CrewAI workflow designer.
Routing rules: the default intent is "generate_crew" (research, analysis, reporting,
multi-step or goal-oriented requests, and any message with 2+ action verbs).
"generate_agent" only when ONE agent/bot/assistant/chatbot is explicitly the entity
created; "generate_task" only when a task is explicitly created; "execute_crew" for
run/execute/start/launch; "configure_crew" for model/tools/settings changes.
Given the user message and the predicted intent, answer with EXACTLY one word:
CORRECT or WRONG."""


def _preflight_reflection(reflection_uri: str, reflection_env: Dict[str, str]) -> None:
    """One-token ping of GEPA's reflection model BEFORE any budget is spent.

    A dead reflection model doesn't fail a run — GEPA just proposes zero
    candidates and 'completes' at the baseline after burning the whole
    execution budget (observed live with a retired provider model name).
    """
    import litellm

    saved = {k: os.environ.get(k) for k in reflection_env}
    try:
        for k, v in reflection_env.items():
            os.environ[k] = v
        litellm_model = reflection_uri.replace(":/", "/", 1)
        litellm.completion(
            model=litellm_model,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=1,
        )
    except Exception as e:
        raise ValueError(
            f"Reflection model '{reflection_uri}' failed a test call — the "
            f"optimization cannot generate candidates with it. Pick a different "
            f"reflection model. Provider error: {str(e)[:300]}"
        )
    finally:
        for k, old in saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old


def _sync_llm_completion(
    loop: asyncio.AbstractEventLoop,
    messages: List[Dict[str, str]],
    model: str,
    max_tokens: int,
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None,
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
            temperature=0.0,
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


class PromptOptimizationService:
    """Service for optimizing seeded prompt templates against logged usage."""

    def __init__(self, session: Any):
        self.session = session
        self.log_repository = LLMLogRepository(session)
        self.model_repository = ModelConfigRepository(session)

    # ------------------------------------------------------------------ start

    async def start_optimization(
        self,
        request: PromptOptimizationRequest,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Validate inputs, gather the training set, and launch the run in background."""
        task_cfg = TEMPLATE_TASKS.get(request.template_name)
        if not task_cfg:
            raise ValueError(
                f"Template '{request.template_name}' is not wired for optimization"
            )

        baseline = await TemplateService.get_effective_template_content(
            request.template_name, group_context
        )
        if not baseline or not baseline.strip():
            raise ValueError(
                f"No effective template content found for '{request.template_name}'"
            )

        if request.examples:
            examples = [e.strip() for e in request.examples if e and e.strip()]
        else:
            examples = await self._mine_examples(
                endpoint=task_cfg["log_endpoint"],
                group_context=group_context,
                lookback_days=request.lookback_days,
                max_examples=request.max_examples,
                extract=task_cfg.get("extract"),
            )
        examples = examples[: request.max_examples]
        if len(examples) < MIN_EXAMPLES:
            raise ValueError(
                f"Need at least {MIN_EXAMPLES} training examples, found {len(examples)}. "
                f"Provide 'examples' explicitly or widen 'lookback_days'."
            )

        target_model = request.model or DEFAULT_TARGET_MODEL
        judge_model = request.judge_model or target_model
        reflection_uri, reflection_env, reflection_provider = (
            await self._resolve_reflection_model(
                request.reflection_model or target_model, group_context
            )
        )
        registry_uri, prompt_name = await self._resolve_registry(
            request.template_name, group_context
        )

        run_id = uuid.uuid4().hex[:12]
        group_id = group_context.primary_group_id if group_context else None
        run: Dict[str, Any] = {
            "run_id": run_id,
            "template_name": request.template_name,
            "status": "pending",
            "dataset_size": len(examples),
            "model": target_model,
            "group_id": group_id,
            "baseline_template": baseline,
            "applied": False,
            # Timezone-AWARE so the ISO string carries +00:00 and browsers
            # render local time (a naive UTC stamp displayed as-is showed a
            # 01:20 local run as "11:20 PM" — observed live).
            "created_at": datetime.now(timezone.utc),
        }
        _RUNS[run_id] = run
        self._prune_runs()

        # Keep a strong reference on the run entry so the task isn't GC'd.
        run["task"] = asyncio.create_task(
            self._run_optimization(
                run_id=run_id,
                template_name=request.template_name,
                baseline=baseline,
                examples=examples,
                input_key=task_cfg["input_key"],
                target_model=target_model,
                judge_model=judge_model,
                reflection_uri=reflection_uri,
                reflection_env=reflection_env,
                max_metric_calls=request.max_metric_calls,
                registry_uri=registry_uri,
                prompt_name=prompt_name,
                group_context=group_context,
            )
        )
        return {"run_id": run_id, "status": "pending", "dataset_size": len(examples)}

    async def _mine_examples(
        self,
        endpoint: str,
        group_context: Optional[GroupContext],
        lookback_days: int,
        max_examples: int,
        extract=None,
    ) -> List[str]:
        """Pull distinct, successful inputs for `endpoint` from the LLM log."""
        group_ids = group_context.group_ids if group_context else []
        if not group_ids:
            return []
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
        seen, examples = set(), []
        page = 0
        # Over-fetch pages (dedup shrinks them) but bound total scanned rows.
        while len(examples) < max_examples and page < 20:
            rows = await self.log_repository.get_logs_paginated_by_group(
                page=page, per_page=100, endpoint=endpoint, group_ids=group_ids
            )
            if not rows:
                break
            for row in rows:
                if row.created_at and row.created_at < cutoff:
                    continue
                if row.status != "success" or not row.prompt:
                    continue
                text = extract(row.prompt) if extract else row.prompt.strip()
                if not text:
                    continue
                # Data hygiene — the log contains rows that are not real user
                # requests and would put an unfixable floor under the objective:
                # slash commands (intercepted client-side, and outside the
                # template's intent enum) and system error strings.
                if text.startswith("/"):
                    continue
                if "failed:" in text[:80].lower() or len(text) > 4000:
                    continue
                key = text.lower()
                if not text or key in seen:
                    continue
                seen.add(key)
                examples.append(text)
                if len(examples) >= max_examples:
                    break
            page += 1
        return examples

    # Providers whose GEPA reflection calls authenticate via a single API-key
    # env var (litellm convention), with the key held in Kasal's key store.
    _REFLECTION_KEY_ENV = {
        "openai": "OPENAI_API_KEY",
        "deepseek": "DEEPSEEK_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
        "gemini": "GEMINI_API_KEY",
        "groq": "GROQ_API_KEY",
        "mistral": "MISTRAL_API_KEY",
    }

    async def _resolve_reflection_model(
        self, model_key: str, group_context: Optional[GroupContext] = None
    ) -> tuple:
        """Map a Kasal model key to an MLflow/GEPA reflection-model URI.

        GEPA invokes its reflection model itself (outside LLMManager), so the
        key must become a provider URI plus any env vars the provider needs —
        including the provider API key from Kasal's group-scoped key store.

        Returns (uri, env, provider) — the provider string lets callers apply
        provider-specific request quirks (e.g. Kimi rejects any temperature).
        """
        config = await self.model_repository.find_by_key(model_key)
        provider = (getattr(config, "provider", None) or "").lower() if config else ""
        # The provider API expects the config's `name` (llm_manager does this
        # translation too) — sending the Kasal KEY 400s when they differ
        # (observed live: key deepseek-v3.1-non-thinking vs API name).
        api_model = (
            (getattr(config, "name", None) or model_key) if config else model_key
        )
        if provider == "databricks":
            return f"databricks:/{api_model}", {}, provider
        if provider == "vllm":
            # OpenAI-compatible endpoint — same env resolution as llm_manager.
            return (
                f"openai:/{api_model}",
                {
                    "OPENAI_API_BASE": os.getenv(
                        "VLLM_BASE_URL", "http://localhost:8081/v1"
                    ),
                    "OPENAI_API_KEY": os.getenv("VLLM_API_KEY", "vllm"),
                },
                provider,
            )
        if provider == "kimi":
            # Kimi (Moonshot AI) — OpenAI-compatible endpoint with the key from
            # Kasal's key store, mirroring llm_manager's kimi routing.
            from src.services.api_keys_service import ApiKeysService

            group_id = group_context.primary_group_id if group_context else None
            api_key = await ApiKeysService.get_provider_api_key(
                "kimi", group_id=group_id
            )
            if not api_key:
                raise ValueError(
                    "Reflection model needs a Kimi API key — add KIMI_API_KEY "
                    "under Configuration -> API Keys."
                )
            return (
                f"openai:/{api_model}",
                {
                    "OPENAI_API_BASE": os.getenv(
                        "KIMI_ENDPOINT", "https://api.moonshot.ai/v1"
                    ),
                    "OPENAI_API_KEY": api_key,
                },
                provider,
            )
        if provider in self._REFLECTION_KEY_ENV:
            env: Dict[str, str] = {}
            env_name = self._REFLECTION_KEY_ENV[provider]
            try:
                from src.services.api_keys_service import ApiKeysService

                group_id = group_context.primary_group_id if group_context else None
                api_key = await ApiKeysService.get_provider_api_key(
                    provider, group_id=group_id
                )
                if api_key:
                    env[env_name] = api_key
            except Exception as key_err:
                logger.warning(
                    f"Could not fetch {provider} API key for reflection model: {key_err}"
                )
            # Fall back to a pre-set env var when the store has no key.
            if env_name not in env and not os.getenv(env_name):
                raise ValueError(
                    f"Reflection model '{model_key}' needs a {provider} API key "
                    f"(configure it under API Keys) or pass 'reflection_model' "
                    f"with a different provider."
                )
            return f"{provider}:/{api_model}", env, provider
        raise ValueError(
            f"Reflection model '{model_key}' has unsupported provider '{provider or 'unknown'}' "
            f"(supported: databricks, vllm, kimi, {', '.join(sorted(self._REFLECTION_KEY_ENV))}). "
            f"Pass 'reflection_model' explicitly."
        )

    async def _resolve_registry(
        self, template_name: str, group_context: Optional[GroupContext]
    ) -> tuple:
        """Resolve the MLflow prompt-registry destination and prompt name.

        Policy: managed MLflow (Databricks Unity Catalog prompt registry) is
        the default. A LOCAL MLflow server is used only when explicitly
        enabled for development: MCP_SERVER_ENABLED=true plus
        MLFLOW_TRACKING_URI (e.g. http://127.0.0.1:5555).
        """
        group_id = group_context.primary_group_id if group_context else None
        safe_group = "".join(
            c if c.isalnum() or c in "-_" else "_" for c in (group_id or "base")
        )
        base_name = f"kasal_{template_name}_{safe_group}"

        local_enabled = os.getenv("MCP_SERVER_ENABLED", "").lower() == "true"
        # main.py force-overwrites MLFLOW_TRACKING_URI to "databricks" at
        # startup; the value the process was LAUNCHED with is preserved in
        # KASAL_LAUNCH_MLFLOW_TRACKING_URI. Guard against databricks-schemed
        # values either way — local mode means a local/OSS server.
        local_uri = os.getenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI") or os.getenv(
            "MLFLOW_TRACKING_URI"
        )
        if local_enabled and local_uri and not local_uri.startswith("databricks"):
            return local_uri, base_name

        # Managed MLflow: UC prompt registry needs a three-level name from the
        # workspace's configured catalog + schema.
        from src.services.databricks_service import DatabricksService

        db_config = await DatabricksService(
            self.session, group_id=group_id
        ).get_databricks_config()
        catalog = getattr(db_config, "catalog", None) if db_config else None
        # schema field is `db_schema` (aliased "schema"); reading "schema"
        # returns BaseModel.schema (a method) — same trap as mlflow_parent_setup.
        schema = getattr(db_config, "db_schema", None) if db_config else None
        if not (catalog and schema):
            raise ValueError(
                "Prompt optimization uses the managed MLflow (Unity Catalog) prompt "
                "registry, which requires a catalog and schema in the Databricks "
                "configuration. For local development set MCP_SERVER_ENABLED=true and "
                "MLFLOW_TRACKING_URI (e.g. http://127.0.0.1:5555) to use a local "
                "MLflow server instead."
            )
        return "databricks-uc", f"{catalog}.{schema}.{base_name}"

    # ------------------------------------------------------------ background

    async def _run_optimization(self, run_id: str, sync_fn=None, **kwargs) -> None:
        run = _RUNS.get(run_id)
        if run is None:
            return
        run["status"] = "running"
        try:
            result = await asyncio.to_thread(
                sync_fn or self._execute_optimization_sync,
                loop=asyncio.get_running_loop(),
                **kwargs,
            )
            run.update(result)
            run["status"] = "completed"
            logger.info(
                f"Prompt optimization {run_id} completed: "
                f"{result.get('initial_score')} -> {result.get('final_score')}"
            )
        except Exception as e:
            import traceback as _tb

            if run.get("cancel_requested"):
                logger.info(f"Prompt optimization {run_id} cancelled by user")
                run["status"] = "cancelled"
                run["error"] = None
                return
            logger.error(f"Prompt optimization {run_id} failed: {e}", exc_info=True)
            run["status"] = "failed"
            # Keep the deepest frames — the surface message alone has proven
            # insufficient to locate failures inside optimizer internals.
            run["error"] = (
                f"{e}\n\n{''.join(_tb.format_exc().splitlines(keepends=True)[-30:])}"
            )

    @staticmethod
    def _execute_optimization_sync(
        loop: asyncio.AbstractEventLoop,
        template_name: str,
        baseline: str,
        examples: List[str],
        input_key: str,
        target_model: str,
        judge_model: str,
        reflection_uri: str,
        reflection_env: Dict[str, str],
        max_metric_calls: int,
        registry_uri: str,
        prompt_name: str,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """The blocking optimization body — runs in a worker thread."""
        user_token = (
            getattr(group_context, "access_token", None) if group_context else None
        )
        # Quiet MLflow's background telemetry consumer — it resolves the
        # tracking scheme over HTTP with lazy imports, one of the threads in
        # the import-lock deadlock below. Must be set before mlflow import.
        os.environ.setdefault("MLFLOW_DISABLE_TELEMETRY", "true")

        import mlflow
        from mlflow.genai import optimize_prompts
        from mlflow.genai.optimize import GepaPromptOptimizer
        from mlflow.genai.scorers import scorer

        # Pre-import every module that MLflow imports LAZILY from background
        # threads during this flow: register_prompt spawns an async
        # link-prompt-to-experiment thread and optimize_prompts enables openai
        # autologging via import hooks — concurrent lazy imports across those
        # threads deadlock on importlib module locks (observed live via
        # py-spy). With the modules already in sys.modules, nothing imports.
        # The openai submodules matter most: mlflow/openai/autolog.py imports
        # them lazily inside its import-hook critical section, and the openai
        # package itself lazy-loads submodules (so importing the parent is NOT
        # enough — py-spy showed successive deadlocks marching through beta,
        # then responses). This list mirrors autolog's actual imports; with
        # everything already in sys.modules our thread takes no import locks
        # while MLflow's background threads run their own lazy imports.
        for _mod in (
            "openai",
            "openai.resources",
            "openai.resources.chat.completions",
            "openai.resources.completions",
            "openai.resources.embeddings",
            "openai.resources.images",
            "openai.resources.beta.chat.completions",
            "openai.resources.responses",
            "litellm",
            "databricks.sdk",
            "mlflow.openai",
        ):
            try:
                __import__(_mod)
            except ImportError:
                pass

        # Prompt-registry destination (resolved by _resolve_registry: managed
        # UC by default, local server only when explicitly enabled). Only the
        # REGISTRY is pointed there — the app's global tracking config for
        # tracing is left untouched. Belt and braces: set the module global
        # AND the env var (for optimize_prompts internals), and additionally
        # pin an explicit client for our own calls — in-process something can
        # reset the global between our set and the call (observed live), and a
        # client carries its registry_uri immutably.
        os.environ["MLFLOW_REGISTRY_URI"] = registry_uri
        mlflow.set_registry_uri(registry_uri)
        logger.info(
            f"Prompt optimization registry: requested={registry_uri} "
            f"effective={mlflow.get_registry_uri()}"
        )
        client = mlflow.MlflowClient(registry_uri=registry_uri)

        # In LOCAL mode, redirect TRACKING to the local server for the whole
        # register→optimize span and restore after. This is not just for the
        # optimizer: register_prompt itself resolves the default experiment
        # (MLFLOW_EXPERIMENT_NAME) against the TRACKING store, which is
        # globally "databricks" (main.py) and unauthenticated in local dev.
        # Side benefit: the optimization is visible in the local MLflow UI.
        local_mode = not registry_uri.startswith("databricks")
        prev_tracking = mlflow.get_tracking_uri()
        if local_mode:
            mlflow.set_tracking_uri(registry_uri)

        # Suppress experiment resolution for the whole span: register_prompt
        # spawns an async link-prompt-to-experiment thread when
        # MLFLOW_EXPERIMENT_NAME/_ID resolve, and that thread deadlocks with
        # autologging's import hooks on importlib module locks (py-spy
        # verified twice). Prompt↔experiment linking isn't needed here.
        saved_exp_env = {
            k: os.environ.pop(k, None)
            for k in ("MLFLOW_EXPERIMENT_NAME", "MLFLOW_EXPERIMENT_ID")
        }

        def _restore_span_env() -> None:
            if local_mode:
                mlflow.set_tracking_uri(prev_tracking)
            for k, old in saved_exp_env.items():
                if old is not None:
                    os.environ[k] = old

        logger.info(f"Prompt optimization stage=register registry={registry_uri}")
        try:
            prompt_version = client.register_prompt(
                name=prompt_name,
                template=baseline,
                commit_message="baseline registered by Kasal prompt optimization",
            )
        except Exception:
            _restore_span_env()
            raise
        prompt_uri = prompt_version.uri
        logger.info(f"Prompt optimization stage=optimize prompt_uri={prompt_uri}")

        def predict_fn(**inputs) -> str:
            # Loading via the registry URI is what lets the optimizer inject
            # candidate templates without knowing our LLM stack.
            candidate = client.load_prompt(prompt_uri)
            return _sync_llm_completion(
                loop,
                messages=[
                    {"role": "system", "content": candidate.template},
                    {"role": "user", "content": str(inputs[input_key])},
                ],
                model=target_model,
                max_tokens=1000,
                group_context=group_context,
                user_token=user_token,
            )

        task_cfg = TEMPLATE_TASKS[template_name]

        @scorer
        def output_format(outputs) -> float:
            if template_name == "detect_intent":
                return _intent_format_score(outputs)
            if task_cfg.get("format_fn"):
                return task_cfg["format_fn"](outputs)
            return _json_keys_score(outputs, task_cfg["required_keys"])

        @scorer
        def output_correct(inputs, outputs) -> float:
            if template_name == "detect_intent":
                try:
                    parsed = robust_json_parser(str(outputs))
                    predicted = (
                        parsed.get("intent") if isinstance(parsed, dict) else None
                    )
                except Exception:
                    predicted = None
                if predicted not in VALID_INTENTS:
                    return 0.0
                judge_system = _JUDGE_SYSTEM
                judge_user = (
                    f"User message: {inputs[input_key]}\nPredicted intent: {predicted}"
                )
            else:
                judge_system = task_cfg["judge_system"]
                judge_user = (
                    f"User request: {inputs[input_key]}\nGenerated output: {outputs}"
                )
            verdict = _sync_llm_completion(
                loop,
                messages=[
                    {"role": "system", "content": judge_system},
                    {"role": "user", "content": judge_user},
                ],
                model=judge_model,
                # Room for forced-thinking judges (Kimi K2.x): 300 tokens of
                # allowance was consumed entirely by reasoning, leaving empty
                # visible content (observed live).
                max_tokens=1500,
                group_context=group_context,
                user_token=user_token,
            )
            # Check the verdict's TAIL so reasoning text mentioning "correct"
            # doesn't count — the instructed final word is what matters.
            tail = (verdict or "").strip().upper()[-40:]
            negative = "WRONG" in tail or "INCORRECT" in tail
            return 1.0 if "CORRECT" in tail and not negative else 0.0

        def aggregation(scores: Dict[str, Any]) -> float:
            fmt = float(scores.get("output_format") or 0.0)
            correct = float(scores.get("output_correct") or 0.0)
            return 0.4 * fmt + 0.6 * correct

        train_data = [
            {"inputs": {input_key: ex}, "expectations": {}} for ex in examples
        ]

        saved_env = {k: os.environ.get(k) for k in reflection_env}
        # Mark every autolog flavor disabled for the span: optimize_prompts'
        # evaluation wrapper otherwise registers import hooks that lazily
        # import each installed flavor module (openai, crewai, ...) inside a
        # lock-guarded critical section — which deadlocks against MLflow's own
        # background threads on importlib locks (py-spy verified across five
        # runs, each stuck one flavor further). With disable=True the wrapper
        # skips the flavor entirely: no hooks, no imports, nothing to deadlock.
        from mlflow.models.evaluation.utils.trace import FLAVOR_TO_MODULE_NAME
        from mlflow.utils.autologging_utils import AUTOLOGGING_INTEGRATIONS

        saved_autolog_flags: Dict[str, Any] = {}
        for _flavor in FLAVOR_TO_MODULE_NAME:
            cfg = AUTOLOGGING_INTEGRATIONS.setdefault(_flavor, {})
            saved_autolog_flags[_flavor] = cfg.get("disable")
            cfg["disable"] = True

        # Quiet the JSON-recovery parser for the span: candidate templates that
        # break the output contract are EXPECTED here (that's what the format
        # scorer is for) and each one otherwise emits a burst of parse-recovery
        # ERROR logs.
        _parser_logger = logging.getLogger("src.utils.prompt_utils")
        _prev_parser_level = _parser_logger.level
        _parser_logger.setLevel(logging.CRITICAL)

        # Tracking is already redirected in local mode (see above); this
        # try/finally owns restoring it once the optimization ends.
        try:
            for k, v in reflection_env.items():
                os.environ[k] = v
            result = optimize_prompts(
                predict_fn=predict_fn,
                train_data=train_data,
                prompt_uris=[prompt_uri],
                optimizer=GepaPromptOptimizer(
                    reflection_model=reflection_uri,
                    max_metric_calls=max_metric_calls,
                    # llm_manager enables a process-global litellm cache;
                    # without a per-request bypass, identical reflection
                    # prompts are served the same cached proposal forever
                    # (observed live in crew mode).
                    gepa_kwargs={"reflection_lm_kwargs": {"cache": {"no-cache": True}}},
                ),
                scorers=[output_format, output_correct],
                aggregation=aggregation,
                # Local mode: track into the local server (visible in its UI).
                # Managed mode: registry artifacts suffice; skip tracking writes.
                enable_tracking=local_mode,
            )
        finally:
            _parser_logger.setLevel(_prev_parser_level)
            _restore_span_env()
            for _flavor, old_flag in saved_autolog_flags.items():
                if old_flag is None:
                    AUTOLOGGING_INTEGRATIONS.get(_flavor, {}).pop("disable", None)
                else:
                    AUTOLOGGING_INTEGRATIONS[_flavor]["disable"] = old_flag
            for k, old in saved_env.items():
                if old is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = old

        optimized = result.optimized_prompts[0]
        return {
            "optimized_template": optimized.template,
            "initial_score": _to_float(getattr(result, "initial_eval_score", None)),
            "final_score": _to_float(getattr(result, "final_eval_score", None)),
            "prompt_uri": getattr(optimized, "uri", None),
        }

    # ----------------------------------------------------------- crew (GEPA)

    async def start_crew_optimization(
        self, request: Any, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """GEPA over a saved crew's prompt fields, with REAL crew executions as
        the evaluation: every metric call runs the crew (tools included) and a
        judge scores the final deliverable. Expensive by design — the budget is
        the number of crew executions."""
        from src.repositories.agent_repository import AgentRepository
        from src.repositories.crew_repository import CrewRepository
        from src.repositories.task_repository import TaskRepository

        group_ids = group_context.group_ids if group_context else []
        # The crews PK is a UUID column — normalize the string id and treat any
        # malformed value the same as not-found (clean 400, not a 500).
        try:
            crew_key = uuid.UUID(str(request.crew_id))
        except (ValueError, AttributeError):
            raise ValueError(f"Crew '{request.crew_id}' not found")
        crew = await CrewRepository(self.session).get_by_group(crew_key, group_ids)
        if crew is None:
            raise ValueError(f"Crew '{request.crew_id}' not found")

        agent_repo = AgentRepository(self.session)
        task_repo = TaskRepository(self.session)
        agents = [
            a for a in [await agent_repo.get(i) for i in (crew.agent_ids or [])] if a
        ]
        tasks = [
            t for t in [await task_repo.get(i) for i in (crew.task_ids or [])] if t
        ]
        if not agents or not tasks:
            raise ValueError("Crew has no agent/task records to optimize")

        baseline_doc, field_keys = _serialize_crew_doc(agents, tasks)
        baseline_fields = _parse_crew_doc(baseline_doc) or {}

        # Execution payload bases (candidate fields are overlaid per eval).
        agent_name_by_id = {str(a.id): a.name for a in agents}
        agents_yaml = {
            str(a.name): {
                "name": a.name,
                "role": a.role,
                "goal": a.goal,
                "backstory": a.backstory,
                "tools": a.tools or [],
                "llm": a.llm,
            }
            for a in agents
        }
        tasks_yaml = {
            str(t.name): {
                "name": t.name,
                "description": t.description,
                "expected_output": t.expected_output,
                "tools": t.tools or [],
                "agent": agent_name_by_id.get(str(t.agent_id), ""),
                "async_execution": False,
                "context": [],
                "_field_prefix": f"task.{t.id}",
            }
            for t in tasks
        }
        for a in agents:
            agents_yaml[str(a.name)]["_field_prefix"] = f"agent.{a.id}"

        objective = f"Crew '{crew.name}': " + "; ".join(
            (t.description or "")[:120] for t in tasks
        )
        rubric = "\n".join(f"- {t.name}: {t.expected_output}" for t in tasks)
        if request.guidance:
            rubric += f"\nAdditional guidance: {request.guidance}"

        # HUMAN JUDGMENT: fold this crew's real user feedback (chat 👍/👎 with
        # comments) into the judge's rubric so the automated grade reflects what
        # actual users praised or flagged, not just the task contracts.
        try:
            from src.repositories.crew_feedback_repository import CrewFeedbackRepository

            feedback = await CrewFeedbackRepository(
                self.session
            ).list_by_crew_and_group(str(crew.id), group_ids)
            complaints = [
                f.comment.strip()
                for f in feedback
                if f.rating == "down" and f.comment and f.comment.strip()
            ][:8]
            praise = [
                f.comment.strip()
                for f in feedback
                if f.rating == "up" and f.comment and f.comment.strip()
            ][:4]
            if complaints:
                rubric += (
                    "\nUsers flagged these problems in past runs (penalize any recurrence):\n"
                    + "\n".join(f"- {c}" for c in complaints)
                )
            if praise:
                rubric += "\nUsers praised (preserve these qualities):\n" + "\n".join(
                    f"- {p}" for p in praise
                )
        except Exception as feedback_err:
            logger.warning(f"Could not load crew feedback for rubric: {feedback_err}")

        target_model = request.model or DEFAULT_TARGET_MODEL
        judge_model = request.judge_model or target_model
        reflection_uri, reflection_env, reflection_provider = (
            await self._resolve_reflection_model(
                request.reflection_model or target_model, group_context
            )
        )
        registry_uri, _ = await self._resolve_registry("crew", group_context)
        safe_group = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in (group_context.primary_group_id if group_context else "base")
            or "base"
        )
        prompt_name = f"kasal_crew_{str(crew.id).replace('-', '')[:12]}_{safe_group}"
        if registry_uri == "databricks-uc":
            # UC needs the catalog.schema prefix _resolve_registry computed for
            # its own name; recompute with the crew-specific leaf.
            _, uc_name = await self._resolve_registry("crew", group_context)
            prompt_name = uc_name.rsplit(".", 1)[0] + "." + prompt_name

        run_id = uuid.uuid4().hex[:12]
        group_id = group_context.primary_group_id if group_context else None
        run: Dict[str, Any] = {
            "run_id": run_id,
            "template_name": f"crew:{crew.name}",
            "kind": "crew",
            "crew_id": str(crew.id),
            "status": "pending",
            "executions_used": 0,
            "execution_cap": request.max_metric_calls,
            "dataset_size": 1,
            "model": target_model,
            "group_id": group_id,
            "baseline_template": baseline_doc,
            "baseline_fields": baseline_fields,
            "applied": False,
            "human_feedback_count": 0,
            "candidates_tried": 0,
            "created_at": datetime.now(timezone.utc),
        }
        _RUNS[run_id] = run
        self._prune_runs()

        run["task"] = asyncio.create_task(
            self._run_optimization(
                run_id=run_id,
                sync_fn=self._execute_crew_optimization_sync,
                baseline_doc=baseline_doc,
                field_keys=field_keys,
                objective=objective,
                rubric=rubric,
                agents_yaml=agents_yaml,
                tasks_yaml=tasks_yaml,
                target_model=target_model,
                judge_model=judge_model,
                reflection_uri=reflection_uri,
                reflection_env=reflection_env,
                reflection_provider=reflection_provider,
                max_metric_calls=request.max_metric_calls,
                execution_timeout=request.execution_timeout_seconds,
                registry_uri=registry_uri,
                prompt_name=prompt_name,
                crew_id=str(crew.id),
                cancel_run_id=run_id,
                group_context=group_context,
            )
        )
        return {"run_id": run_id, "status": "pending", "dataset_size": 1}

    @staticmethod
    def _execute_crew_optimization_sync(
        loop: asyncio.AbstractEventLoop,
        baseline_doc: str,
        field_keys: List[str],
        objective: str,
        rubric: str,
        agents_yaml: Dict[str, Any],
        tasks_yaml: Dict[str, Any],
        target_model: str,
        judge_model: str,
        reflection_uri: str,
        reflection_env: Dict[str, str],
        max_metric_calls: int,
        execution_timeout: int,
        registry_uri: str,
        prompt_name: str,
        crew_id: str = "",
        cancel_run_id: str = "",
        reflection_provider: str = "",
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Blocking crew-optimization body (worker thread). Mirrors the
        template body's MLflow span setup; predict = execute the crew."""
        import copy

        user_token = (
            getattr(group_context, "access_token", None) if group_context else None
        )
        os.environ.setdefault("MLFLOW_DISABLE_TELEMETRY", "true")
        import mlflow
        from mlflow.entities import Feedback
        from mlflow.genai import optimize_prompts
        from mlflow.genai.optimize import GepaPromptOptimizer
        from mlflow.genai.scorers import scorer
        from mlflow.models.evaluation.utils.trace import FLAVOR_TO_MODULE_NAME
        from mlflow.utils.autologging_utils import AUTOLOGGING_INTEGRATIONS

        os.environ["MLFLOW_REGISTRY_URI"] = registry_uri
        mlflow.set_registry_uri(registry_uri)
        client = mlflow.MlflowClient(registry_uri=registry_uri)

        local_mode = not registry_uri.startswith("databricks")
        prev_tracking = mlflow.get_tracking_uri()
        if local_mode:
            mlflow.set_tracking_uri(registry_uri)
        saved_exp_env = {
            k: os.environ.pop(k, None)
            for k in ("MLFLOW_EXPERIMENT_NAME", "MLFLOW_EXPERIMENT_ID")
        }
        saved_autolog_flags: Dict[str, Any] = {}
        for _flavor in FLAVOR_TO_MODULE_NAME:
            cfg = AUTOLOGGING_INTEGRATIONS.setdefault(_flavor, {})
            saved_autolog_flags[_flavor] = cfg.get("disable")
            cfg["disable"] = True
        saved_env = {k: os.environ.get(k) for k in reflection_env}

        try:
            for k, v in reflection_env.items():
                os.environ[k] = v

            # Fail fast on a dead reflection model — before ANY crew execution.
            _preflight_reflection(reflection_uri, reflection_env)

            prompt_version = client.register_prompt(
                name=prompt_name,
                template=baseline_doc,
                commit_message="crew baseline registered by Kasal prompt optimization",
            )
            prompt_uri = prompt_version.uri
            logger.info(f"Crew optimization stage=optimize prompt_uri={prompt_uri}")

            # Pin the experiment AFTER register_prompt (the experiment lookup
            # before registration is what spawned the deadlocking link thread).
            # Traces, registered judges, and assessments are all per-experiment,
            # and the request-context endpoints resolve MLFLOW_EXPERIMENT_NAME —
            # the run must land on the SAME experiment or judges/evals become
            # invisible to each other (observed: scorers registered on 'kasal',
            # run searching the default experiment).
            if local_mode:
                exp_name = saved_exp_env.get("MLFLOW_EXPERIMENT_NAME") or "kasal"
                try:
                    mlflow.set_experiment(exp_name)
                except Exception as exp_err:
                    logger.warning(f"Could not pin experiment '{exp_name}': {exp_err}")

            # HUMAN JUDGMENT via MLflow Assessments: every evaluation logs its
            # deliverable as a trace (tagged kasal_crew_id); Feedback and
            # Expectations the user adds on those traces in the MLflow UI are
            # harvested here and folded into the judge's rubric on the NEXT run.
            judge_rubric = rubric
            objective_for_training = objective
            train_expectations: Dict[str, str] = {}
            human_requirements: List[str] = []
            if local_mode and crew_id:
                try:
                    prior = mlflow.search_traces(
                        filter_string=f"tags.kasal_crew_id = '{crew_id}'",
                        max_results=50,
                        return_type="list",
                    )
                    # Oldest-first so the "keep the last 12" slice below keeps
                    # the NEWEST notes (search order is not guaranteed).
                    prior.sort(key=lambda t: t.info.request_time or 0)
                    notes: List[str] = []
                    req_texts: List[str] = []
                    for trace in prior:
                        for assessment in trace.search_assessments() or []:
                            name = getattr(assessment, "name", "") or ""
                            value = getattr(
                                getattr(assessment, "feedback", None), "value", None
                            )
                            exp_value = getattr(
                                getattr(assessment, "expectation", None),
                                "value",
                                None,
                            )
                            if exp_value is not None:
                                req_texts.append(str(exp_value))
                            if value is None:
                                value = exp_value
                            rationale = getattr(assessment, "rationale", None) or ""
                            if rationale:
                                req_texts.append(rationale)
                            if value is not None or rationale:
                                notes.append(
                                    f"- {name}: {value if value is not None else ''} {rationale}".strip()
                                )
                    # Deduplicated constraints, NOT the grade litany: repeating
                    # "human_grade: 0.0 ..." thirteen times anchored the judge
                    # to zero even for a compliant answer (verified live A/B).
                    human_requirements = _distill_requirements(req_texts)
                    harvest_entry = _RUNS.get(cancel_run_id) if cancel_run_id else None
                    if harvest_entry is not None:
                        harvest_entry["human_feedback_count"] = len(notes)
                except Exception as assess_err:
                    logger.warning(
                        f"Could not harvest MLflow assessments: {assess_err}"
                    )

            if human_requirements:
                # LLM-refine the raw complaints into testable imperatives —
                # verified live: a 30B judge fed the raw complaint sentences
                # ("it is giving french side...") as checklist items failed
                # EVERY mark by quoting the requirement itself as evidence;
                # the same judge with cleanly phrased requirements graded a
                # compliant answer 0.96 and a Geneva-containing one 0.60 with
                # correct verbatim quotes. One cheap call per run.
                try:
                    refined_text = _sync_llm_completion(
                        loop,
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "You convert raw human review notes about an "
                                    "AI crew's answers into a clean requirements "
                                    "checklist for FUTURE answers.\n"
                                    "- Merge duplicate and overlapping notes into "
                                    "one requirement.\n"
                                    "- Phrase each as a positive, testable "
                                    "requirement about the answer content.\n"
                                    "- Notes describing one-off failures (e.g. "
                                    "'nothing delivered') become a standing "
                                    "requirement only if sensible.\n"
                                    "- Output ONLY numbered lines 'R1. ...', "
                                    "'R2. ...' — at most 5 requirements, nothing "
                                    "else."
                                ),
                            },
                            {
                                "role": "user",
                                "content": "\n".join(
                                    f"- {r}" for r in human_requirements
                                ),
                            },
                        ],
                        model=judge_model,
                        max_tokens=800,
                        group_context=group_context,
                        user_token=user_token,
                    )
                    refined = _parse_requirement_lines(refined_text)
                    if refined:
                        human_requirements = refined[:5]
                except Exception as distill_err:
                    logger.warning(
                        f"Requirement distillation failed; using raw notes: {distill_err}"
                    )
                req_block = "\n".join(f"- {r}" for r in human_requirements)
                # Ground truth rides GEPA's expectations channel too — the
                # reflective dataset surfaces it to the mutator as explicit
                # targets, not just prose inside the request.
                train_expectations = {"human_requirements": req_block[:2000]}
                # The requirements must ALSO reach GEPA's reflection model,
                # which only sees training inputs and scorer feedback — a
                # judge that grades 0 "because wrong region" is useless to a
                # mutator that never learns the region requirement (observed
                # live: flat 0-scores with mutations blind to the human's why).
                objective_for_training = (
                    objective
                    + "\nHard requirements from human review of past answers:\n"
                    + req_block
                )
                logger.info(
                    "Crew optimization: using "
                    f"{len(human_requirements)} distilled human requirements"
                )

            # CUSTOM JUDGES: LLM judges registered on the local MLflow
            # experiment ("Create LLM judge" in the MLflow UI) participate in
            # scoring alongside the built-in judge — users author evaluation
            # criteria there and optimization honors them automatically.
            registered_scorers: List[Any] = []
            if local_mode:
                try:
                    from mlflow.genai.scorers import list_scorers

                    # Only judges ASSIGNED to this crew (scoped by name prefix)
                    # participate — the shared library is inert until assigned.
                    crew_prefix = (
                        PromptOptimizationService._crew_judge_prefix(crew_id)
                        if crew_id
                        else None
                    )
                    registered_scorers = [
                        s
                        for s in (list_scorers() or [])
                        if crew_prefix
                        and str(getattr(s, "name", "")).startswith(crew_prefix)
                    ]
                    if registered_scorers:
                        logger.info(
                            "Crew optimization: using registered MLflow judges: "
                            + ", ".join(
                                getattr(s, "name", "?") for s in registered_scorers
                            )
                        )
                except Exception as scorer_err:
                    logger.warning(
                        f"Could not load registered MLflow judges: {scorer_err}"
                    )

            expected_keys = set(field_keys)

            def _apply_fields(fields: Dict[str, str]):
                agents_over = copy.deepcopy(agents_yaml)
                tasks_over = copy.deepcopy(tasks_yaml)
                for cfg_map in (agents_over, tasks_over):
                    for entity in cfg_map.values():
                        prefix = entity.pop("_field_prefix", None)
                        if not prefix:
                            continue
                        for field in (
                            "role",
                            "goal",
                            "backstory",
                            "description",
                            "expected_output",
                        ):
                            key = f"{prefix}.{field}"
                            if key in fields and fields[key].strip():
                                entity[field] = fields[key].strip()
                return agents_over, tasks_over

            # Result caches, keyed by content. GEPA re-evaluates the SAME
            # candidate doc many times (upfront smoke test, baseline valset
            # pass, and a fresh reflective-minibatch pass EVERY iteration).
            # Uncached, those re-runs burned most of a small execution budget
            # re-measuring the baseline — a 4-execution run bought exactly ONE
            # distinct candidate (observed live: total_metric_calls=7,
            # candidates=1). Worse, the stochastic judge re-grading identical
            # prompts drew 0.0 and then 4/10 two minutes apart, so accept/
            # reject was a coin flip. With caching, each DISTINCT candidate
            # costs exactly one execution and one judgment, and comparisons
            # against the baseline are stable within the run.
            deliverable_cache: Dict[str, str] = {}
            judge_cache: Dict[str, Any] = {}
            # Serializes check-then-execute: mlflow's eval harness runs batch
            # records through a thread pool, and concurrent calls for the SAME
            # candidate all missed the cache and each ran the crew (observed
            # live: two executions of one candidate finishing in the same
            # second). GEPA itself steps sequentially, so the lock costs
            # nothing in wall-clock.
            execute_lock = threading.Lock()

            def predict_fn(**inputs) -> str:
                run_entry = _RUNS.get(cancel_run_id, {}) if cancel_run_id else {}
                # User-requested stop: abort BEFORE spending a crew execution.
                if run_entry.get("cancel_requested"):
                    raise RuntimeError("Cancelled by user")
                candidate = client.load_prompt(prompt_uri)
                doc_key = hashlib.sha256(candidate.template.encode("utf-8")).hexdigest()
                with execute_lock:
                    return _predict_locked(doc_key, candidate, run_entry, inputs)

            def _predict_locked(doc_key, candidate, run_entry, inputs) -> str:
                # Cache lookup BEFORE the cap check: re-evaluations of an
                # already-executed candidate (usually the baseline) stay
                # truthful even after the budget is spent.
                if doc_key in deliverable_cache:
                    return deliverable_cache[doc_key]
                # HARD execution cap: the user's budget is a promise about crew
                # executions, but GEPA overshoots (parallel batches are only
                # budget-checked between iterations, plus the upfront smoke
                # test). Once the cap is spent, further NEW candidates get a
                # free empty result — they score 0, never win, and GEPA wraps
                # up returning the best already-evaluated candidate.
                if run_entry.get("executions_used", 0) >= max_metric_calls:
                    logger.info(
                        "Crew optimization execution cap reached "
                        f"({max_metric_calls}); skipping further executions"
                    )
                    return ""
                fields = _parse_crew_doc(candidate.template)
                # Malformed candidates never execute — free rejection.
                if fields is None or set(fields) != expected_keys:
                    return ""
                if run_entry:
                    run_entry["executions_used"] = (
                        run_entry.get("executions_used", 0) + 1
                    )
                agents_over, tasks_over = _apply_fields(fields)
                deliverable = _sync_run_crew(
                    loop,
                    agents_yaml=agents_over,
                    tasks_yaml=tasks_over,
                    model=target_model,
                    timeout=execution_timeout,
                    group_context=group_context,
                    user_token=user_token,
                )
                deliverable_cache[doc_key] = deliverable
                if run_entry:
                    run_entry["candidates_tried"] = len(deliverable_cache)
                # Log this evaluation as an MLflow trace so the user can attach
                # Feedback/Expectations (Assessments panel) that steer the judge
                # on the next run. Advisory only — never fail the eval over it.
                if local_mode and crew_id:
                    try:
                        with mlflow.start_span(name="crew_optimization_eval") as span:
                            span.set_inputs(
                                {
                                    "objective": inputs.get("request", objective),
                                    "candidate_prompts": candidate.template[:4000],
                                }
                            )
                            span.set_outputs({"deliverable": deliverable[:8000]})
                            mlflow.update_current_trace(tags={"kasal_crew_id": crew_id})
                    except Exception as trace_err:
                        # Warning, not debug: a lost trace means the user
                        # cannot grade that answer (a baseline eval vanished
                        # silently this way, observed live).
                        logger.warning(f"Eval trace logging failed: {trace_err}")
                return deliverable

            @scorer
            def output_format(outputs) -> float:
                text = str(outputs or "").strip()
                return 1.0 if len(text) > 50 else 0.0

            @scorer
            def output_correct(inputs, outputs):
                # GRADED, not binary: a pass/fail judge saturates at 1.0 for any
                # acceptable baseline, leaving GEPA no gradient to climb (observed
                # live: 1.00 -> 1.00 with zero exploration payoff). A harsh 0-10
                # rubric keeps ordinary output around 6-7 so better prompts can
                # actually outscore the baseline.
                #
                # Returns an mlflow Feedback (value + rationale), NOT a bare
                # float: rationales are the ONLY textual signal the GEPA
                # reflection model receives about WHY a candidate scored low
                # (mlflow folds Feedback.rationale into the reflective
                # dataset). With floats, mutations were blind guesses — the
                # judge knew "wrong region, rentals not sales" but the mutator
                # never heard it (observed live: 1 requirement-aware candidate
                # in ~10 runs).
                text = str(outputs or "").strip()
                if not text:
                    return Feedback(
                        name="output_correct",
                        value=0.0,
                        rationale="Empty deliverable — the crew produced no output.",
                    )
                text_key = hashlib.sha256(text.encode("utf-8")).hexdigest()
                cached = judge_cache.get(text_key)
                if cached is not None:
                    return Feedback(
                        name="output_correct", value=cached[0], rationale=cached[1]
                    )
                if human_requirements:
                    # CHECKLIST mode: per-requirement PASS/FAIL gives GEPA a
                    # gradient to climb — the all-or-nothing harsh grader
                    # produced a flat 0.0 landscape where a fully compliant
                    # candidate could only ever TIE the baseline. The verbatim
                    # -quote rule counters judge hallucination (observed live:
                    # a FAIL claiming Geneva rows in an answer containing
                    # none), and the objective line is deliberately withheld —
                    # the crew's own task text may contradict the human
                    # requirements (it said "cities like Zurich, Geneva" while
                    # the human demanded German-side only).
                    req_lines = "\n".join(
                        f"R{i + 1}. {r}" for i, r in enumerate(human_requirements)
                    )
                    judge_messages = [
                        {
                            "role": "system",
                            "content": (
                                "You are grading an AI crew's final deliverable "
                                "against a numbered requirements checklist distilled "
                                "from human review of PREVIOUS answers.\n"
                                "Rules:\n"
                                "- Judge ONLY the answer shown below. Failures of "
                                "previous answers are irrelevant.\n"
                                "- Each requirement states what the human demanded "
                                "(sometimes phrased as a complaint about an older "
                                "answer); decide whether THIS answer satisfies it.\n"
                                "- For EACH requirement output one line: "
                                "'R<n>: PASS' or 'R<n>: FAIL — ' followed by a "
                                "VERBATIM quote from the answer proving the "
                                "violation.\n"
                                "- If you cannot quote a violating passage from the "
                                "answer, the mark is PASS.\n"
                                "- Then output 'Q: <0-10>' rating base quality "
                                "(completeness, specificity, format) of the answer "
                                "against the task expectations. 10 is rare.\n"
                                "- Output nothing else."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Task expectations:\n{judge_rubric}\n\n"
                                f"Requirements from human review:\n{req_lines}\n\n"
                                f"Answer to grade:\n{text[:6000]}"
                            ),
                        },
                    ]
                else:
                    judge_messages = [
                        {
                            "role": "system",
                            "content": (
                                "You are a HARSH grader of an AI crew's final deliverable. "
                                "Score 0-10 against the per-task expectations:\n"
                                "- completeness: every expectation addressed, none skipped\n"
                                "- specificity: concrete facts/sources/structure, no filler\n"
                                "- fidelity: matches the requested format and scope exactly\n"
                                "10 = flawless and exceptional (rare). 7 = solid with minor "
                                "gaps. 5 = acceptable but generic. 3 = major omissions. "
                                "0 = failed.\n"
                                "First, in one or two sentences, name the SPECIFIC "
                                "failures, quoting the exact expectation that was "
                                "violated. Then write the final grade alone on the "
                                "LAST line as a bare number."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Objective: {inputs.get('request', objective)}\n"
                                f"Expectations:\n{judge_rubric}\n\nFinal output:\n{text[:6000]}"
                            ),
                        },
                    ]
                try:
                    verdict = _sync_llm_completion(
                        loop,
                        messages=judge_messages,
                        model=judge_model,
                        # Room for forced-thinking models (Kimi K2.x): even 300
                        # tokens were consumed entirely by reasoning, leaving
                        # empty visible content (observed live at 300/300).
                        max_tokens=1500,
                        group_context=group_context,
                        user_token=user_token,
                    )
                except Exception as judge_err:
                    # A broken judge must be LOUD: silent zeros flatten the
                    # whole score landscape and make runs look like "no
                    # improvement possible" (observed live with an unsupported
                    # judge provider — every grade was an exception).
                    logger.error(f"Crew optimization judge call failed: {judge_err}")
                    raise
                grades: List[float] = []
                rationale_parts: List[str] = []
                if verdict and str(verdict).strip():
                    rationale_parts.append(str(verdict).strip())
                checklist_value = (
                    _checklist_grade(verdict or "", len(human_requirements))
                    if human_requirements
                    else None
                )
                if checklist_value is not None:
                    grades.append(checklist_value)
                else:
                    # LAST number wins: thinking models may reason (with
                    # incidental numbers) before stating the final grade. A
                    # number above 10 is treated as a percentage — clamping
                    # alone turned a hallucinated "40" into a perfect 10/10.
                    matches = re.findall(r"\d+(?:\.\d+)?", verdict or "")
                    if matches:
                        number = float(matches[-1])
                        if 10.0 < number <= 100.0:
                            number /= 10.0
                        grades.append(max(0.0, min(10.0, number)) / 10.0)
                    else:
                        logger.warning(
                            f"Crew optimization judge reply not numeric: {verdict!r}"
                        )
                # Registered judges grade the SAME deliverable here rather than
                # running as separate MLflow scorers — trace-based scorers were
                # each re-triggering their own crew execution (observed live as
                # bursts of one execution per judge), multiplying the budget.
                for judge in registered_scorers:
                    try:
                        feedback = judge(
                            inputs={"request": inputs.get("request", objective)},
                            outputs=text,
                        )
                        grade = _judge_value_to_grade(getattr(feedback, "value", None))
                        if grade is None:
                            logger.warning(
                                f"Registered judge '{getattr(judge, 'name', '?')}' "
                                f"returned unusable value "
                                f"{getattr(feedback, 'value', None)!r}; skipping"
                            )
                        else:
                            grades.append(grade)
                            judge_rationale = getattr(feedback, "rationale", None)
                            if judge_rationale:
                                rationale_parts.append(
                                    f"[{getattr(judge, 'name', 'judge')}] "
                                    f"{judge_rationale}"
                                )
                    except Exception as judge_err:
                        logger.warning(
                            f"Registered judge '{getattr(judge, 'name', '?')}' "
                            f"failed: {judge_err}"
                        )
                grade_value = sum(grades) / len(grades) if grades else 0.0
                rationale = "\n".join(rationale_parts)[:4000]
                judge_cache[text_key] = (grade_value, rationale)
                return Feedback(
                    name="output_correct", value=grade_value, rationale=rationale
                )

            def aggregation(scores: Dict[str, Any]) -> float:
                # Registered judges are already averaged INSIDE output_correct.
                # Scores arrive RAW: a scorer that returned a Feedback shows up
                # here as the Feedback object, not its numeric value.
                def _num(value: Any) -> float:
                    value = getattr(value, "value", value)
                    try:
                        return float(value or 0.0)
                    except (TypeError, ValueError):
                        return 0.0

                fmt = _num(scores.get("output_format"))
                correct = _num(scores.get("output_correct"))
                return 0.3 * fmt + 0.7 * correct

            result = optimize_prompts(
                predict_fn=predict_fn,
                train_data=[
                    {
                        "inputs": {"request": objective_for_training},
                        "expectations": train_expectations,
                    }
                ],
                prompt_uris=[prompt_uri],
                optimizer=GepaPromptOptimizer(
                    reflection_model=reflection_uri,
                    # METRIC calls are decoupled from crew EXECUTIONS: with
                    # the caches (ours + gepa's) most metric calls are free
                    # re-scores, so the user's number stays a hard cap on real
                    # executions while GEPA gets iteration headroom. Observed
                    # live without this: a 10-execution budget stopped after 4
                    # executions because cached re-evaluations had consumed
                    # the metric budget.
                    max_metric_calls=max_metric_calls * 2 + 3,
                    gepa_kwargs={
                        # Default minibatch of 3 sampled our SINGLE training
                        # example three times per step — every candidate cost
                        # 3 crew executions racing the cache (two finished the
                        # same second, observed live).
                        "reflection_minibatch_size": 1,
                        # Strict improvement rejected TIES: a candidate that
                        # fully incorporated the human requirements scored
                        # 0.9 vs 0.9 on the minibatch and was discarded
                        # (proposals.json, observed live). Lateral moves must
                        # survive so the search can leave a flat region.
                        "acceptance_criterion": "improvement_or_equal",
                        # gepa-side (candidate, example) result cache: skips
                        # the metric call entirely on repeats, preserving the
                        # metric budget for NEW candidates.
                        "cache_evaluation": True,
                        # The reflection prompt is IDENTICAL every iteration
                        # (same parent, same single example, cached rationale),
                        # so a deterministic reflection endpoint re-proposed
                        # the byte-identical candidate 11 times in one run —
                        # every iteration a free cache-hit rejection, budget
                        # drained, zero exploration (observed live in
                        # proposals.json). Explicit sampling temperature is
                        # the only diversity source this setup has — and
                        # no-cache is MANDATORY: llm_manager enables a
                        # process-global litellm disk cache at import, and
                        # gepa's LM rides the same litellm, so identical
                        # reflection prompts were served the SAME cached
                        # response forever (observed live: duration=0.00s,
                        # byte-identical proposals at temperature 1.0).
                        # 0.8, not 1.0: at 1.0 the reflection model emitted a
                        # bare "```" and stopped in 2 of 3 samples; at 0.8
                        # with the no-fence contract below, 4 of 4 samples
                        # parsed, were distinct, and carried the requirements
                        # (validated offline against the live endpoint).
                        # PROVIDER QUIRK — Kimi K-series accepts ONLY its
                        # default temperature: sending 0.8 made Moonshot 400
                        # every reflection call ("invalid temperature: only 1
                        # is allowed"), gepa skipped 10 proposals in a row and
                        # the run 'completed' after the baseline's single
                        # execution (observed live; verified by direct repro —
                        # the same call without temperature returns a clean,
                        # parseable crew doc). Kimi's forced default sampling
                        # is itself diverse, so dropping the knob is safe.
                        "reflection_lm_kwargs": (
                            {"cache": {"no-cache": True}}
                            if reflection_provider == "kimi"
                            else {
                                "temperature": 0.8,
                                "cache": {"no-cache": True},
                            }
                        ),
                        # gepa's default template says "write a new
                        # instruction ... within ``` blocks" — an open
                        # invitation to restructure: the reflection model
                        # returned {"instruction": "..."} JSON blobs that
                        # lost the [AGENT]/[TASK] document structure and
                        # free-rejected every proposal (observed live, 11/11
                        # malformed). Pin the output contract to the crew-doc
                        # format instead.
                        "reflection_prompt_template": (
                            "I provided an assistant with the following "
                            "DOCUMENT of prompt fields that configures an AI "
                            "crew (agents and tasks):\n"
                            "```\n<curr_param>\n```\n\n"
                            "The following are examples of task inputs, the "
                            "crew's final answer, and feedback (score and "
                            "judge rationale) on how the answer could be "
                            "better:\n"
                            "```\n<side_info>\n```\n\n"
                            "Your task is to write an IMPROVED VERSION of the "
                            "document above so that a future answer satisfies "
                            "the feedback and every hard requirement stated "
                            "in the task input.\n\n"
                            "STRICT FORMAT RULES:\n"
                            "- Keep EXACTLY the same structure: the same "
                            "[AGENT <id>] and [TASK <id>] section headers "
                            "with the same ids, and the same field labels "
                            "(ROLE:, GOAL:, BACKSTORY:, DESCRIPTION:, "
                            "EXPECTED_OUTPUT:).\n"
                            "- Each field label starts its line, followed by "
                            "the improved text for that field on the same "
                            "line.\n"
                            "- Do NOT output JSON, commentary, or anything "
                            "except the document.\n"
                            "- Output ONLY the improved document, nothing "
                            "before or after it. Start your reply directly "
                            "with the first [AGENT line."
                        ),
                    },
                ),
                scorers=[output_format, output_correct],
                aggregation=aggregation,
                enable_tracking=local_mode,
            )
        finally:
            if local_mode:
                mlflow.set_tracking_uri(prev_tracking)
            for k, old in saved_exp_env.items():
                if old is not None:
                    os.environ[k] = old
            for _flavor, old_flag in saved_autolog_flags.items():
                if old_flag is None:
                    AUTOLOGGING_INTEGRATIONS.get(_flavor, {}).pop("disable", None)
                else:
                    AUTOLOGGING_INTEGRATIONS[_flavor]["disable"] = old_flag
            for k, old in saved_env.items():
                if old is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = old

        optimized = result.optimized_prompts[0]
        optimized_fields = _parse_crew_doc(optimized.template) or {}
        return {
            "optimized_template": optimized.template,
            "optimized_fields": optimized_fields,
            "initial_score": _to_float(getattr(result, "initial_eval_score", None)),
            "final_score": _to_float(getattr(result, "final_eval_score", None)),
            "prompt_uri": getattr(optimized, "uri", None),
        }

    # -------------------------------------------------- crew eval feedback

    @staticmethod
    def _local_mlflow_uri() -> Optional[str]:
        """The local MLflow server URI when local mode is enabled, else None."""
        if os.getenv("MCP_SERVER_ENABLED", "").lower() != "true":
            return None
        uri = os.getenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI") or os.getenv(
            "MLFLOW_TRACKING_URI"
        )
        if uri and not uri.startswith("databricks"):
            return uri
        return None

    async def list_crew_evals(self, crew_id: str) -> List[Dict[str, Any]]:
        """List this crew's optimization-evaluation traces (for in-app grading)."""
        local_uri = self._local_mlflow_uri()
        if not local_uri:
            return []

        def _list() -> List[Dict[str, Any]]:
            import mlflow

            prev = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(local_uri)
            try:
                # Generous window: eval traces accumulate across runs, and a
                # too-small page silently hides previously GRADED answers
                # (observed live — graded traces fell out of a 25-trace page).
                traces = mlflow.search_traces(
                    filter_string=f"tags.kasal_crew_id = '{crew_id}'",
                    max_results=200,
                    return_type="list",
                )
                out: List[Dict[str, Any]] = []
                for trace in traces:
                    deliverable = ""
                    try:
                        for span in trace.search_spans(name="crew_optimization_eval"):
                            outputs = span.outputs or {}
                            deliverable = str(outputs.get("deliverable", ""))
                            break
                    except Exception:
                        pass
                    assessments = []
                    try:
                        assessments = trace.search_assessments() or []
                    except Exception:
                        pass
                    info = trace.info
                    out.append(
                        {
                            "trace_id": getattr(info, "trace_id", None)
                            or getattr(info, "request_id", ""),
                            "timestamp_ms": getattr(info, "timestamp_ms", None)
                            or getattr(info, "request_time", None),
                            "deliverable": deliverable[:4000],
                            "assessment_count": len(assessments),
                        }
                    )
                return out
            finally:
                mlflow.set_tracking_uri(prev)

        return await asyncio.to_thread(_list)

    async def add_eval_feedback(
        self,
        trace_id: str,
        value: Optional[float] = None,
        comment: Optional[str] = None,
        expectation: Optional[str] = None,
    ) -> bool:
        """Attach human assessments to an eval trace — a grade (Feedback:
        judgment of what WAS produced) and/or an expectation (ground truth of
        what SHOULD have been produced). Both are harvested into the judge's
        rubric on the next optimization run."""
        if value is None and not (expectation or "").strip():
            raise ValueError("Provide a grade, an expectation, or both")
        local_uri = self._local_mlflow_uri()
        if not local_uri:
            raise ValueError(
                "In-app eval feedback requires the local MLflow server "
                "(MCP_SERVER_ENABLED=true + MLFLOW_TRACKING_URI)."
            )

        def _log() -> bool:
            import mlflow

            prev = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(local_uri)
            try:
                if value is not None:
                    mlflow.log_feedback(
                        trace_id=trace_id,
                        name="human_grade",
                        value=max(0.0, min(10.0, float(value))),
                        rationale=(comment or "").strip() or None,
                    )
                if (expectation or "").strip():
                    mlflow.log_expectation(
                        trace_id=trace_id,
                        name="human_expectation",
                        value=expectation.strip(),
                    )
                return True
            finally:
                mlflow.set_tracking_uri(prev)

        return await asyncio.to_thread(_log)

    # ----------------------------------------------------------- LLM judges

    @staticmethod
    def _crew_judge_prefix(crew_id: str) -> str:
        """Registry-name prefix that scopes a judge to one crew (assignment is
        encoded in the name — no schema change, survives restarts)."""
        return f"crew_{str(crew_id).replace('-', '')[:12]}__"

    async def list_judges(self) -> List[Dict[str, Any]]:
        """List LLM judges registered on the local MLflow experiment.

        Names starting with a crew prefix ('crew_<id>__') are ASSIGNED to that
        crew; others are shared library judges. `name` is the display name,
        `full_name` the registry name.
        """
        local_uri = self._local_mlflow_uri()
        if not local_uri:
            return []

        def _list() -> List[Dict[str, Any]]:
            import mlflow

            prev = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(local_uri)
            try:
                _pin_local_experiment()
                from mlflow.genai.scorers import list_scorers

                out = []
                for s in list_scorers() or []:
                    full_name = getattr(s, "name", "?")
                    crew_id = None
                    display = full_name
                    match = re.match(r"^crew_([0-9a-f]{1,12})__(.+)$", full_name)
                    if match:
                        crew_id = match.group(1)
                        display = match.group(2)
                    out.append(
                        {
                            "name": display,
                            "full_name": full_name,
                            "crew_id": crew_id,
                            "model": getattr(s, "model", None),
                            # Full text (bounded): the edit dialog round-trips
                            # this — a truncated copy would corrupt the judge
                            # on save.
                            "instructions": (getattr(s, "instructions", "") or "")[
                                :4000
                            ],
                        }
                    )
                return out
            finally:
                mlflow.set_tracking_uri(prev)

        return await asyncio.to_thread(_list)

    async def create_judge(
        self,
        name: str,
        instructions: str,
        model: Optional[str] = None,
        crew_id: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Create + register an MLflow LLM judge from Kasal (no MLflow UI needed).

        `instructions` is plain-language criteria; it must reference the answer
        via the {{ outputs }} template variable (added automatically when
        missing). `model` is a Kasal model key, resolved to a judge model URI.
        """
        local_uri = self._local_mlflow_uri()
        if not local_uri:
            raise ValueError(
                "Judge creation requires the local MLflow server "
                "(MCP_SERVER_ENABLED=true + MLFLOW_TRACKING_URI)."
            )
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name.strip())
        if not safe_name:
            raise ValueError("Judge name is required")
        text = instructions.strip()
        if not text:
            raise ValueError("Judge instructions are required")
        if "{{ outputs }}" not in text and "{{outputs}}" not in text:
            text += "\n\nThe answer to evaluate:\n{{ outputs }}"
        model_uri, _env, _provider = await self._resolve_reflection_model(
            model or DEFAULT_TARGET_MODEL, group_context
        )
        scoped_name = (
            f"{self._crew_judge_prefix(crew_id)}{safe_name}" if crew_id else None
        )

        def _create() -> Dict[str, Any]:
            import mlflow

            prev = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(local_uri)
            try:
                _pin_local_experiment()
                from mlflow.genai.judges import make_judge

                # ALWAYS register the shared library original; when created
                # from a crew's dialog, ALSO register the crew-scoped copy
                # (auto-assign). Registering only the scoped copy made the
                # judge invisible in every other crew's Assign menu — there
                # was no library original to assign (observed live).
                for reg_name in filter(None, [safe_name, scoped_name]):
                    judge = make_judge(
                        name=reg_name,
                        instructions=text,
                        model=model_uri,
                        # Numeric verdicts — categorical words ('Satisfactory')
                        # are lossier to fold into an aggregate score.
                        feedback_value_type=float,
                    )
                    judge.register()
                return {
                    "name": safe_name,
                    "full_name": scoped_name or safe_name,
                    "model": model_uri,
                }
            finally:
                mlflow.set_tracking_uri(prev)

        return await asyncio.to_thread(_create)

    async def assign_judge(
        self, name: str, crew_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Assign a shared library judge to a crew by registering a crew-scoped
        copy (same instructions/model) under the crew's name prefix."""
        local_uri = self._local_mlflow_uri()
        if not local_uri:
            raise ValueError("Judge assignment requires the local MLflow server.")

        def _assign() -> Dict[str, Any]:
            import mlflow

            prev = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(local_uri)
            try:
                _pin_local_experiment()
                from mlflow.genai.judges import make_judge
                from mlflow.genai.scorers import get_scorer

                source = get_scorer(name=name)
                scoped_name = f"{self._crew_judge_prefix(crew_id)}{name}"
                judge = make_judge(
                    name=scoped_name,
                    instructions=getattr(source, "instructions", "") or "",
                    model=getattr(source, "model", None),
                    feedback_value_type=float,
                )
                judge.register()
                return {"name": name, "full_name": scoped_name, "crew_id": crew_id}
            finally:
                mlflow.set_tracking_uri(prev)

        return await asyncio.to_thread(_assign)

    async def update_judge(
        self,
        name: str,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Update a judge's instructions and/or model.

        `name` is the FULL registry name (library judge, or a crew-scoped
        'crew_<id>__name' copy — editing an assigned copy changes what that
        crew's runs use). MLflow scorers are versioned: registering under the
        same name creates a new version and get_scorer/list_scorers return the
        latest (verified live against the local registry). Omitted fields keep
        their current values. Editing a library judge does NOT touch copies
        already assigned to crews — those are snapshots taken at assign time.
        """
        local_uri = self._local_mlflow_uri()
        if not local_uri:
            raise ValueError("Judge update requires the local MLflow server.")
        new_text = (instructions or "").strip()
        if not new_text and not model:
            raise ValueError("Nothing to update: provide instructions and/or a model")
        model_uri: Optional[str] = None
        if model:
            model_uri, _env, _provider = await self._resolve_reflection_model(
                model, group_context
            )

        def _update() -> Dict[str, Any]:
            import mlflow

            prev = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(local_uri)
            try:
                _pin_local_experiment()
                from mlflow.genai.judges import make_judge
                from mlflow.genai.scorers import get_scorer

                current = get_scorer(name=name)
                text = new_text or (getattr(current, "instructions", "") or "").strip()
                if not text:
                    raise ValueError("Judge instructions are required")
                if "{{ outputs }}" not in text and "{{outputs}}" not in text:
                    text += "\n\nThe answer to evaluate:\n{{ outputs }}"
                final_model = model_uri or getattr(current, "model", None)
                judge = make_judge(
                    name=name,
                    instructions=text,
                    model=final_model,
                    feedback_value_type=float,
                )
                judge.register()
                return {"name": name, "model": final_model}
            finally:
                mlflow.set_tracking_uri(prev)

        return await asyncio.to_thread(_update)

    async def delete_judge(self, name: str) -> bool:
        """Delete a registered judge by name."""
        local_uri = self._local_mlflow_uri()
        if not local_uri:
            raise ValueError("Judge deletion requires the local MLflow server.")

        def _delete() -> bool:
            import mlflow

            prev = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(local_uri)
            try:
                _pin_local_experiment()
                from mlflow.genai.scorers import delete_scorer

                delete_scorer(name=name, version="all")
                return True
            finally:
                mlflow.set_tracking_uri(prev)

        return await asyncio.to_thread(_delete)

    # ---------------------------------------------------------------- reads

    def get_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Optional[Dict[str, Any]]:
        run = _RUNS.get(run_id)
        if run is None or not self._visible(run, group_context):
            return None
        return {k: run.get(k) for k in _PUBLIC_FIELDS}

    def list_runs(
        self, group_context: Optional[GroupContext] = None
    ) -> List[Dict[str, Any]]:
        runs = [r for r in _RUNS.values() if self._visible(r, group_context)]
        _epoch = datetime.min.replace(tzinfo=timezone.utc)
        runs.sort(key=lambda r: r.get("created_at") or _epoch, reverse=True)
        return [{k: r.get(k) for k in _PUBLIC_FIELDS} for r in runs]

    @staticmethod
    def _visible(run: Dict[str, Any], group_context: Optional[GroupContext]) -> bool:
        group_id = group_context.primary_group_id if group_context else None
        return run.get("group_id") == group_id

    @staticmethod
    def _prune_runs() -> None:
        if len(_RUNS) <= _MAX_KEPT_RUNS:
            return
        # Drop the oldest finished runs first; never prune active ones.
        finished = [
            r for r in _RUNS.values() if r.get("status") in ("completed", "failed")
        ]
        _epoch = datetime.min.replace(tzinfo=timezone.utc)
        finished.sort(key=lambda r: r.get("created_at") or _epoch)
        for run in finished[: len(_RUNS) - _MAX_KEPT_RUNS]:
            _RUNS.pop(run["run_id"], None)

    # ---------------------------------------------------------------- cancel

    def cancel_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Request a running optimization to stop. The flag is honored before
        the NEXT crew execution — an in-flight execution finishes first."""
        run = _RUNS.get(run_id)
        if run is None or not self._visible(run, group_context):
            raise ValueError(f"Optimization run '{run_id}' not found")
        if run.get("status") not in ("pending", "running"):
            raise ValueError(f"Optimization run '{run_id}' is not active")
        run["cancel_requested"] = True
        logger.info(f"Prompt optimization {run_id}: cancellation requested")
        return {"run_id": run_id, "cancelling": True}

    # ---------------------------------------------------------------- apply

    async def apply_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Write a completed run's proposal as a group-scoped template override."""
        run = _RUNS.get(run_id)
        if run is None or not self._visible(run, group_context):
            raise ValueError(f"Optimization run '{run_id}' not found")
        if run.get("status") != "completed" or not run.get("optimized_template"):
            raise ValueError(
                f"Optimization run '{run_id}' has no completed proposal to apply"
            )

        if run.get("kind") == "crew":
            return await self._apply_crew_run(run)

        template_service = TemplateService(self.session)
        row = await template_service.find_by_name_with_group_check(
            run["template_name"], group_context
        )
        if row is None:
            raise ValueError(f"Template '{run['template_name']}' not found")
        updated = await template_service.update_with_group_check(
            row.id,
            PromptTemplateUpdate(template=run["optimized_template"]),
            group_context,
        )
        if updated is None:
            raise ValueError(f"Failed to update template '{run['template_name']}'")
        run["applied"] = True
        return {
            "run_id": run_id,
            "template_name": run["template_name"],
            "applied": True,
        }

    async def _apply_crew_run(self, run: Dict[str, Any]) -> Dict[str, Any]:
        """Write a crew run's optimized fields back onto the agent/task rows."""
        from src.repositories.agent_repository import AgentRepository
        from src.repositories.task_repository import TaskRepository

        optimized: Dict[str, str] = run.get("optimized_fields") or {}
        baseline: Dict[str, str] = run.get("baseline_fields") or {}
        agent_repo = AgentRepository(self.session)
        task_repo = TaskRepository(self.session)

        # Group per entity, skipping unchanged fields.
        changes: Dict[tuple, Dict[str, str]] = {}
        for key, value in optimized.items():
            if not value or value == baseline.get(key):
                continue
            try:
                entity_kind, entity_id, field = key.split(".", 2)
            except ValueError:
                continue
            changes.setdefault((entity_kind, entity_id), {})[field] = value

        applied = 0
        for (entity_kind, entity_id), patch in changes.items():
            if entity_kind == "agent":
                if await agent_repo.update(entity_id, patch):
                    applied += 1
            elif entity_kind == "task":
                if await task_repo.update(entity_id, patch):
                    applied += 1
        run["applied"] = True
        logger.info(f"Crew optimization {run['run_id']} applied to {applied} entities")
        return {
            "run_id": run["run_id"],
            "template_name": run["template_name"],
            "applied": True,
        }


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
