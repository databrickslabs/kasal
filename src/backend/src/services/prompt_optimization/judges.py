"""Judge lifecycle: listing, creating, assigning and deleting the judges a
run is graded by, plus the eval feedback loop.

Mixed into ``PromptOptimizationService`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,031-line file, and the public surface is unchanged.
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
from src.repositories.log_repository import LLMLogRepository
from src.repositories.model_config_repository import ModelConfigRepository
from src.utils.user_context import GroupContext
from src.services.prompt_optimization.config import DEFAULT_TARGET_MODEL, _pin_local_experiment


class JudgeOperationsMixin:
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
        # The judge is INVOKED through LLMManager with the plain Kasal model
        # key; the 'openai:/' wrapper exists only to satisfy make_judge's URI
        # shape and is stripped back on invocation. No provider resolution.
        model_uri = f"openai:/{model or DEFAULT_TARGET_MODEL}"
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
        # Plain Kasal model key, wrapped only for make_judge's URI shape —
        # invocation goes through LLMManager (see _stored_judge_model_to_key).
        model_uri: Optional[str] = f"openai:/{model}" if model else None

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
