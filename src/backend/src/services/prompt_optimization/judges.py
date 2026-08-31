"""Judge lifecycle: listing, creating, assigning and deleting the judges a
run is graded by, plus the eval feedback loop.

Judges are stored in the MLflow Prompt Registry (see ``judge_registry``) and
invoked on demand through LLMManager — never registered as MLflow scorers.

Mixed into ``PromptOptimizationService`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,031-line file, and the public surface is unchanged.
"""

import asyncio
from typing import Any, Dict, List, Optional, Tuple

from src.services.prompt_optimization.config import DEFAULT_TARGET_MODEL
from src.services.prompt_optimization.gepa.mlflow_session import (
    mlflow_session,
    resolve_mlflow_backend,
)
from src.services.prompt_optimization.judge_registry import (
    JudgeRegistry,
    uc_schema_of,
)
from src.utils.user_context import GroupContext


class JudgeOperationsMixin:
    async def list_crew_evals(
        self, crew_id: str, group_context: Optional[GroupContext] = None
    ) -> List[Dict[str, Any]]:
        """List this crew's optimization-evaluation traces (for in-app grading)."""
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            return []

        def _list() -> List[Dict[str, Any]]:
            import mlflow

            with mlflow_session(backend):
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

        return await asyncio.to_thread(_list)

    async def add_eval_feedback(
        self,
        trace_id: str,
        value: Optional[float] = None,
        comment: Optional[str] = None,
        expectation: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
        judge: Optional[str] = None,
    ) -> bool:
        """Attach human assessments to an eval trace — a grade (Feedback:
        judgment of what WAS produced) and/or an expectation (ground truth of
        what SHOULD have been produced). Both are harvested into the judge's
        rubric on the next optimization run.

        ``judge`` (a crew-scoped registry name) additionally records the grade
        under THAT judge's name, sourced HUMAN. That pair is what MemAlign
        matches when aligning the judge (see alignment.py): a grade logged only
        as ``human_grade`` is harvested by GEPA but invisible to alignment."""
        if value is None and not (expectation or "").strip():
            raise ValueError("Provide a grade, an expectation, or both")
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            raise ValueError(
                "In-app eval feedback requires MLflow: a Databricks workspace "
                "(deployed) or a local MLflow server (dev)."
            )

        def _log() -> bool:
            import mlflow
            from mlflow.entities import AssessmentSource, AssessmentSourceType

            # Explicitly HUMAN: mlflow's default source is not, and MemAlign
            # only learns from human-sourced assessments.
            source = AssessmentSource(
                source_type=AssessmentSourceType.HUMAN,
                source_id=(getattr(group_context, "group_email", None) or "kasal"),
            )
            with mlflow_session(backend):
                if value is not None:
                    grade = max(0.0, min(10.0, float(value)))
                    rationale = (comment or "").strip() or None
                    for feedback_name in filter(None, ["human_grade", judge]):
                        mlflow.log_feedback(
                            trace_id=trace_id,
                            name=feedback_name,
                            value=grade,
                            rationale=rationale,
                            source=source,
                        )
                if (expectation or "").strip():
                    mlflow.log_expectation(
                        trace_id=trace_id,
                        name="human_expectation",
                        value=expectation.strip(),
                    )
                return True

        return await asyncio.to_thread(_log)

    @staticmethod
    def _crew_judge_prefix(crew_id: str) -> str:
        """Registry-name prefix that scopes a judge to one crew (assignment is
        encoded in the name — no schema change, survives restarts)."""
        return f"crew_{str(crew_id).replace('-', '')[:12]}__"

    async def _judge_registry_target(
        self, group_context: Optional[GroupContext]
    ) -> Tuple[str, Optional[str]]:
        """Where this group's judge prompts live: ``(registry_uri, uc_schema)``.
        Resolved exactly like GEPA's crew prompts — Unity Catalog needs the
        three-level prefix, a local server does not."""
        registry_uri, sample_name = await self._resolve_registry("judge", group_context)
        return registry_uri, uc_schema_of(registry_uri, sample_name)

    async def list_judges(
        self, group_context: Optional[GroupContext] = None
    ) -> List[Dict[str, Any]]:
        """List the judges in the prompt registry.

        Names starting with a crew prefix ('crew_<id>__') are ASSIGNED to that
        crew; others are shared library judges. `name` is the display name,
        `full_name` the identity.
        """
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            return []
        registry_uri, uc_schema = await self._judge_registry_target(group_context)

        def _list() -> List[Dict[str, Any]]:
            with mlflow_session(backend):
                registry = JudgeRegistry(registry_uri, uc_schema)
                return [spec.as_dict() for spec in registry.list()]

        return await asyncio.to_thread(_list)

    async def create_judge(
        self,
        name: str,
        instructions: str,
        model: Optional[str] = None,
        crew_id: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Create an LLM judge from Kasal (no MLflow UI needed).

        `instructions` is plain-language criteria; it must reference the answer
        via the {{ outputs }} template variable (added automatically when
        missing). `model` is a Kasal model key — the judge is invoked through
        LLMManager, so the key is stored as-is.
        """
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            raise ValueError(
                "Judge creation requires MLflow: a Databricks workspace "
                "(deployed) or a local MLflow server "
                "(MCP_SERVER_ENABLED=true + MLFLOW_TRACKING_URI, for dev)."
            )
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name.strip())
        if not safe_name:
            raise ValueError("Judge name is required")
        text = instructions.strip()
        if not text:
            raise ValueError("Judge instructions are required")
        if "{{ outputs }}" not in text and "{{outputs}}" not in text:
            text += "\n\nThe answer to evaluate:\n{{ outputs }}"
        model_key = model or DEFAULT_TARGET_MODEL
        scoped_name = (
            f"{self._crew_judge_prefix(crew_id)}{safe_name}" if crew_id else None
        )
        registry_uri, uc_schema = await self._judge_registry_target(group_context)

        def _create() -> Dict[str, Any]:
            with mlflow_session(backend):
                registry = JudgeRegistry(registry_uri, uc_schema)
                # ALWAYS save the shared library original; when created from a
                # crew's dialog, ALSO save the crew-scoped copy (auto-assign).
                # Saving only the scoped copy made the judge invisible in every
                # other crew's Assign menu — there was no library original to
                # assign (observed live).
                for reg_name in filter(None, [safe_name, scoped_name]):
                    registry.save(
                        reg_name,
                        text,
                        model_key,
                        commit_message="created in Kasal's Optimize dialog",
                    )
                return {
                    "name": safe_name,
                    "full_name": scoped_name or safe_name,
                    "model": model_key,
                }

        return await asyncio.to_thread(_create)

    async def assign_judge(
        self, name: str, crew_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Assign a shared library judge to a crew by saving a crew-scoped copy
        (same instructions/model) under the crew's name prefix."""
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            raise ValueError("Judge assignment requires MLflow (Databricks or local).")
        registry_uri, uc_schema = await self._judge_registry_target(group_context)

        def _assign() -> Dict[str, Any]:
            with mlflow_session(backend):
                registry = JudgeRegistry(registry_uri, uc_schema)
                source = registry.load(name)
                if source is None:
                    raise ValueError(f"Judge '{name}' is not in the library.")
                scoped_name = f"{self._crew_judge_prefix(crew_id)}{name}"
                registry.save(
                    scoped_name,
                    source.instructions,
                    source.model,
                    commit_message=f"assigned from library judge '{name}'",
                )
                return {"name": name, "full_name": scoped_name, "crew_id": crew_id}

        return await asyncio.to_thread(_assign)

    async def update_judge(
        self,
        name: str,
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Update a judge's instructions and/or model.

        `name` is the FULL identity (library judge, or a crew-scoped
        'crew_<id>__name' copy — editing an assigned copy changes what that
        crew's runs use). Saving under the same name creates a new prompt
        version, and reads return the latest. Omitted fields keep their current
        values. Editing a library judge does NOT touch copies already assigned
        to crews — those are snapshots taken at assign time.
        """
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            raise ValueError("Judge update requires MLflow (Databricks or local).")
        new_text = (instructions or "").strip()
        if not new_text and not model:
            raise ValueError("Nothing to update: provide instructions and/or a model")
        registry_uri, uc_schema = await self._judge_registry_target(group_context)

        def _update() -> Dict[str, Any]:
            with mlflow_session(backend):
                registry = JudgeRegistry(registry_uri, uc_schema)
                current = registry.load(name)
                if current is None:
                    raise ValueError(f"Judge '{name}' not found.")
                text = new_text or current.instructions.strip()
                if not text:
                    raise ValueError("Judge instructions are required")
                if "{{ outputs }}" not in text and "{{outputs}}" not in text:
                    text += "\n\nThe answer to evaluate:\n{{ outputs }}"
                final_model = model or current.model
                registry.save(
                    name,
                    text,
                    final_model,
                    commit_message="edited in Kasal's Optimize dialog",
                )
                return {"name": name, "model": final_model}

        return await asyncio.to_thread(_update)

    async def delete_judge(
        self, name: str, group_context: Optional[GroupContext] = None
    ) -> bool:
        """Delete a judge (all versions) by its full identity."""
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            raise ValueError("Judge deletion requires MLflow (Databricks or local).")
        registry_uri, uc_schema = await self._judge_registry_target(group_context)

        def _delete() -> bool:
            with mlflow_session(backend):
                return JudgeRegistry(registry_uri, uc_schema).delete(name)

        return await asyncio.to_thread(_delete)
