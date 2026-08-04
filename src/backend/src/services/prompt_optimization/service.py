"""
Service for prompt optimization operations.

Runs GEPA (via mlflow.genai.optimize_prompts) over a seeded meta-prompt:
training examples are mined from the LLM interaction log (or supplied
inline), the current effective template is registered in the MLflow
Prompt Registry, GEPA searches for a better template against scorers,
and the winner is stored on the run for explicit review-and-apply as a
group-scoped template override (never the base row — the seeder
overwrites base rows on startup).

Run state is DURABLE: every run is a `prompt_optimization_runs` row
(model + repository), so a proposal — and the before-image an apply can
be reverted from — survives a backend restart. `_RUNS` remains only as
an in-process cache for IN-FLIGHT runs: it carries the asyncio task
handle, the cancel flag, and the live progress counters that the worker
thread mutates without a DB round trip. A heartbeat flushes those
counters (and `updated_at`) to the row, which is also what lets reads
tell a live run from one orphaned by a restart.

JUDGE INTEGRITY (the two properties this module must not lose):
  * The judge is NOT the model under optimization by default. Target ==
    judge is self-preference: the judge systematically prefers its own
    outputs, so the measured gain is partly an artifact.
  * The crew correctness judge is ANSWER-FIRST: it commits to its own
    reference answer for the objective BEFORE it sees any candidate
    deliverable. A reference-free judge is measurably exploitable —
    optimizing against one drove judge pass rate 0.72 -> 0.94 while true
    accuracy stayed at 0.20, and forcing the commit-first order cut the
    false-positive rate 0.719 -> 0.012 (arXiv:2607.05904).
  * Judge grades are SAMPLED and reduced by MEDIAN: identical prompts
    have scored 0.0 and 4/10 minutes apart, so a single draw made
    accept/reject a coin flip.
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

from src.core.exceptions import BadRequestError
from src.db.session import background_task_context
from src.repositories.log_repository import LLMLogRepository
from src.repositories.model_config_repository import ModelConfigRepository
from src.repositories.prompt_optimization_run_repository import (
    PromptOptimizationRunRepository,
)
from src.schemas.prompt_optimization import PromptOptimizationRequest
from src.schemas.template import PromptTemplateUpdate
from src.services.catalog.templates import TemplateService
from src.services.prompt_optimization import (  # noqa: E402,F401
    CrewRunnerMixin,
    JudgeOperationsMixin,
    RunRegistryMixin,
    TemplateRunnerMixin,
    run_state,
)
from src.services.prompt_optimization.config import (  # noqa: E402,F401
    DEFAULT_TARGET_MODEL,
    MIN_EXAMPLES,
    TEMPLATE_TASKS,
    _pin_local_experiment,
)
from src.services.prompt_optimization.gepa.crew_doc import (  # noqa: E402
    _CREW_DOC_FIELD_LABELS,
    _distill_requirements,
    _extract_user_from_log,
    _parse_crew_doc,
    _parse_requirement_lines,
    _serialize_crew_doc,
)

# Helper library, extracted to ``gepa/`` — re-exported so this module stays
# the single import point for them and no caller (or test) has to know where
# they live.
from src.services.prompt_optimization.gepa.grading import (  # noqa: E402
    _CATEGORICAL_GRADES,
    JUDGE_SPREAD_WARN,
    VALID_INTENTS,
    _checklist_grade,
    _grade_judge_verdict,
    _intent_format_score,
    _job_name_score,
    _json_keys_score,
    _judge_value_to_grade,
    _median_sample,
    _parse_grade_from_text,
    _to_float,
)
from src.services.prompt_optimization.gepa.judge_model import (  # noqa: E402
    _crew_target_model,
    _resolve_judge_model,
    _stored_judge_model_to_key,
)

# Extracted modules, re-exported so this module stays the single import point
# for the whole optimization surface — callers and tests keep using
# ``prompt_optimization_service.X`` regardless of which file X now lives in.
# These are IMPORTS, not copies: ``_RUNS`` and ``_GEPA_REFLECTION_STATE`` are
# shared mutable state and must remain one object across every importer.
from src.services.prompt_optimization.gepa.reflection import (  # noqa: E402,F401
    _GEPA_REFLECTION_STATE,
    _JUDGE_SYSTEM,
    DEFAULT_JUDGE_SAMPLES,
    _install_gepa_reflection_bridge,
    _judge_sample_count,
    _make_reflection_fn,
    _preflight_reflection,
    _sync_llm_completion,
    _sync_run_crew,
)
from src.services.prompt_optimization.run_state import (  # noqa: E402,F401
    _LIVE_COUNTERS,
    _MAX_KEPT_RUNS,
    _PUBLIC_FIELDS,
    _RUN_COLUMNS,
    _RUNS,
    RUN_HEARTBEAT_SECONDS,
    RUN_STALE_SECONDS,
    _persist_run_changes,
    _row_to_public,
    _run_to_columns,
)
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


# A sample spread at or above this is reported: an unstable judge cannot rank
# candidates, and a run whose judge disagrees with itself this much is not
# measuring prompt quality.


# ── Crew optimization: the crew's prompt fields travel through GEPA as ONE
# labeled text document (GEPA mutates plain templates). Line-based parse so
# multi-line field values survive the round trip.


# Categorical verdict scale for judges that answer in words rather than
# numbers (MLflow's judge template often does: 'Satisfactory', 'Partial', …).


# The intent enum the detect_intent template contract allows.


# ---------------------------------------------------------------------------
# ALL LLM calls in this service go through LLMManager (judge, distillation,
# preflight, AND GEPA's reflection model via the bridge below). LLMManager
# owns provider routing, group-scoped API keys, and per-provider request
# quirks (e.g. Kimi rejecting any explicit temperature) — re-implementing
# those here produced a string of provider-specific failures (retired
# DeepSeek names, Kimi temperature 400s, per-tenant keys written into the
# shared process env). None of that belongs outside the manager.
# ---------------------------------------------------------------------------


class PromptOptimizationService(
    TemplateRunnerMixin,
    CrewRunnerMixin,
    JudgeOperationsMixin,
    RunRegistryMixin,
):
    """Service for optimizing seeded prompt templates against logged usage."""

    def __init__(self, session: Any):
        self.session = session
        self.log_repository = LLMLogRepository(session)
        self.model_repository = ModelConfigRepository(session)
        self.run_repository = PromptOptimizationRunRepository(session)

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
        # NOT `or target_model`: judging with the model under optimization is
        # self-preference (see _resolve_judge_model).
        judge_model = _resolve_judge_model(
            request.judge_model,
            target_model,
            f"Prompt optimization '{request.template_name}'",
        )
        # Reflection is a Kasal model key like every other model here —
        # invocation goes through LLMManager (keys, endpoints, provider quirks
        # all owned there), so no URI/env resolution happens in this service.
        reflection_model = request.reflection_model or target_model
        registry_uri, prompt_name = await self._resolve_registry(
            request.template_name, group_context
        )

        run_id = uuid.uuid4().hex[:12]
        group_id = group_context.primary_group_id if group_context else None
        run: Dict[str, Any] = {
            "run_id": run_id,
            "template_name": request.template_name,
            "kind": "template",
            "status": "pending",
            "dataset_size": len(examples),
            "model": target_model,
            "judge_model": judge_model,
            "reflection_model": reflection_model,
            "budget": request.max_metric_calls,
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
        await self._record_run(run, group_context)

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
                reflection_model=reflection_model,
                max_metric_calls=request.max_metric_calls,
                registry_uri=registry_uri,
                prompt_name=prompt_name,
                group_context=group_context,
            ),
            # Spawn without the request's DB session: this task outlives the
            # request, whose session FastAPI closes at response end. group_context
            # / user token are re-established inside the run. See
            # db.session.background_task_context.
            context=background_task_context(),
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

    async def _resolve_crew_traces_experiment(
        self, group_context: Optional[GroupContext]
    ) -> str:
        """The experiment GEPA/crew traces pin, from the MLflow configuration.

        Delegates to :meth:`MLflowService.configured_crew_traces_experiment` so
        the GEPA path pins the SAME experiment the tracing path uses and the
        admin attaches to the app — the configured name, not a hardcoded default.
        Empty string on any failure (local dev / no workspace), which lets the
        worker thread fall back to its env/default without raising.
        """
        group_id = group_context.primary_group_id if group_context else None
        if not group_id:
            return ""
        try:
            from src.services.mlflow.service import MLflowService

            return await MLflowService(
                self.session, group_id=group_id
            ).configured_crew_traces_experiment()
        except Exception as exc:  # noqa: BLE001 — pin is best-effort
            logger.debug(f"Could not resolve crew-traces experiment: {exc}")
            return ""

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
        from src.services.databricks.workspace.service import DatabricksService

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

    # ------------------------------------------------------------- durability

    async def _record_run(
        self, run: Dict[str, Any], group_context: Optional[GroupContext]
    ) -> None:
        """Insert the run's durable row.

        Uses the REQUEST session (this runs while the caller's request is still
        open) and flushes without committing — the session lifecycle owns the
        transaction. A failure here is logged, not raised: an unrecordable run
        still optimizes, it just loses restart survival, and failing the start
        call would be a worse outcome for the user.
        """
        try:
            await self.run_repository.create(
                {
                    "id": run["run_id"],
                    "group_id": run.get("group_id"),
                    "group_email": getattr(group_context, "group_email", None),
                    "created_by_email": getattr(group_context, "group_email", None),
                    # Naive UTC in the DB (every other model's convention); the
                    # API re-attaches tzinfo on read so browsers localize.
                    "created_at": run["created_at"].replace(tzinfo=None),
                    "updated_at": run["created_at"].replace(tzinfo=None),
                    **_run_to_columns(run),
                }
            )
        except Exception as create_err:
            logger.warning(
                f"Could not record prompt optimization run {run['run_id']}: "
                f"{create_err}"
            )

    @staticmethod
    async def _heartbeat(run_id: str) -> None:
        """Flush a live run's progress counters to its row until it ends.

        Two jobs: keep `executions_used`/`candidates_tried` recoverable after a
        restart, and keep `updated_at` fresh so a reader can tell this run is
        ALIVE. Without the second, a run orphaned by `--reload` would sit at
        'running' forever and the UI's "run in progress" lock would never clear.
        """
        try:
            while True:
                await asyncio.sleep(RUN_HEARTBEAT_SECONDS)
                run = _RUNS.get(run_id)
                if run is None or run.get("status") not in ("pending", "running"):
                    return
                await run_state._persist_run_changes(
                    run_id,
                    {k: run.get(k) for k in _LIVE_COUNTERS if run.get(k) is not None},
                )
        except asyncio.CancelledError:
            return

    # ------------------------------------------------------------ background

    async def _run_optimization(self, run_id: str, sync_fn=None, **kwargs) -> None:
        run = _RUNS.get(run_id)
        if run is None:
            return
        run["status"] = "running"
        await run_state._persist_run_changes(run_id, {"status": "running"})
        heartbeat = asyncio.create_task(self._heartbeat(run_id))
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
            await run_state._persist_run_changes(
                run_id,
                {
                    "status": "completed",
                    **{k: run.get(k) for k in _LIVE_COUNTERS if k in run},
                    **_run_to_columns(result),
                },
            )
        except Exception as e:
            import traceback as _tb

            if run.get("cancel_requested"):
                logger.info(f"Prompt optimization {run_id} cancelled by user")
                run["status"] = "cancelled"
                run["error"] = None
                await run_state._persist_run_changes(
                    run_id,
                    {
                        "status": "cancelled",
                        "error": None,
                        **{k: run.get(k) for k in _LIVE_COUNTERS if k in run},
                    },
                )
                return
            logger.error(f"Prompt optimization {run_id} failed: {e}", exc_info=True)
            run["status"] = "failed"
            # Keep the deepest frames — the surface message alone has proven
            # insufficient to locate failures inside optimizer internals.
            run["error"] = (
                f"{e}\n\n{''.join(_tb.format_exc().splitlines(keepends=True)[-30:])}"
            )
            await run_state._persist_run_changes(
                run_id,
                {
                    "status": "failed",
                    "error": run["error"],
                    **{k: run.get(k) for k in _LIVE_COUNTERS if k in run},
                },
            )
        finally:
            heartbeat.cancel()

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

        # Fall back to the model the crew ACTUALLY runs on, not a global default.
        # Each agent keeps its own ``llm`` during optimization (agents_yaml below
        # carries it, and agent_builder prefers the spec's llm over the crew-level
        # model), so DEFAULT_DISPATCHER_MODEL named a model that never executes
        # anything here. That mattered because target_model is what the judge is
        # checked against — comparing the judge to a phantom model let a judge
        # that IS the crew's model pass as "different".
        target_model = (
            request.model or _crew_target_model(agents) or DEFAULT_TARGET_MODEL
        )
        # NOT `or target_model`: the crew judge grades deliverables the target
        # model produced, so target == judge is self-preference — and the crew
        # judge is the one whose grade drives accept/reject.
        judge_model = _resolve_judge_model(
            request.judge_model, target_model, f"Crew optimization '{crew.name}'"
        )
        # A Kasal model key; invoked through LLMManager (no URI/env plumbing).
        reflection_model = request.reflection_model or target_model
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

        # Resolve the crew-traces experiment on the event loop (a DB read) so the
        # worker thread pins the SAME experiment tracing uses — the configured
        # name (Configuration.tsx), the source of truth, not a hardcoded default.
        crew_traces_experiment = await self._resolve_crew_traces_experiment(
            group_context
        )

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
            "judge_model": judge_model,
            "reflection_model": reflection_model,
            "budget": request.max_metric_calls,
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
        await self._record_run(run, group_context)

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
                reflection_model=reflection_model,
                max_metric_calls=request.max_metric_calls,
                execution_timeout=request.execution_timeout_seconds,
                registry_uri=registry_uri,
                prompt_name=prompt_name,
                crew_id=str(crew.id),
                cancel_run_id=run_id,
                group_context=group_context,
                crew_traces_experiment=crew_traces_experiment,
            ),
            # Spawn without the request's DB session (closed at response end);
            # see db.session.background_task_context.
            context=background_task_context(),
        )
        return {"run_id": run_id, "status": "pending", "dataset_size": 1}

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

    # ----------------------------------------------------------- LLM judges

    # ---------------------------------------------------------------- reads

    # ---------------------------------------------------------------- cancel

    # ---------------------------------------------------------------- apply

    # --------------------------------------------------------------- revert
