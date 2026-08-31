"""Align a crew's LLM judge to the human grades left on its evaluation answers.

MemAlign (Databricks, 2026; shipped in MLflow behind ``Scorer.align()``) turns
a handful of natural-language grades into a dual memory the judge carries:

* SEMANTIC — feedback distilled into general guidelines ("prefer certified
  views over raw tables"), folded into the judge's ``instructions``.
* EPISODIC — the graded examples themselves, retrieved by similarity at
  judgment time inside MLflow's ``MemoryAugmentedJudge.__call__``.

Why it belongs HERE and not in HITL: the Optimize dialog already captures
exactly MemAlign's input — a grade, a comment and an expectation per evaluated
answer, attributed to a judge — and GEPA scores every candidate crew with the
registered judges. Without alignment GEPA optimizes the crew toward the judge's
taste; with it, the judge is first brought to the reviewer's standard, and GEPA
optimizes toward that.

What Kasal's own scoring sees: crew_runner renders registered judges through
LLMManager (deliberately — never via mlflow's model client), reading
``judge.instructions``. A MemoryAugmentedJudge appends its guidelines to
``instructions``, so the semantic memory reaches GEPA scoring unchanged;
episodic retrieval stays inside mlflow's own judge invocation.

Model routing: nothing here is configured by environment. The judge grades
with the model chosen for it in the Optimize dialog, so alignment distils with
that same model; the graded answers are embedded with the embedder the crew's
agents carry (Agent form). Both go through LLMManager — see
``gepa/memalign_bridge``.
"""

import asyncio
import json
import logging
import uuid
from collections import Counter
from typing import Any, Dict, List, Optional

from src.services.prompt_optimization.gepa.judge_model import (
    _stored_judge_model_to_key,
)
from src.services.prompt_optimization.gepa.memalign_bridge import (
    memalign_via_llm_manager,
)
from src.services.prompt_optimization.gepa.mlflow_session import (
    mlflow_session,
    resolve_mlflow_backend,
)
from src.utils.user_context import GroupContext, UserContext

logger = logging.getLogger(__name__)

#: Similar past examples retrieved per judgment (MemAlign's k).
RETRIEVAL_K = 5
#: Eval traces scanned for grades — the same window list_crew_evals uses.
TRACE_WINDOW = 200


async def crew_embedder_config(
    session: Any, crew_id: str, group_context: Optional[GroupContext]
) -> Optional[Dict[str, Any]]:
    """The embedder the crew's agents are configured with (Agent form), or
    ``None`` for LLMManager's default. Agents may disagree; the most common
    configuration wins. Crews and agents are catalog's domain, so this goes
    through their services — the group check lives there.
    """
    from src.services.catalog.agents import AgentService
    from src.services.catalog.crews import CrewService

    try:
        crew_key = uuid.UUID(str(crew_id))
    except (ValueError, AttributeError):
        return None
    crew = await CrewService(session).get_by_group(crew_key, group_context)
    if crew is None:
        return None
    agent_service = AgentService(session)
    seen: List[str] = []
    for agent_id in crew.agent_ids or []:
        agent = await agent_service.get_with_group_check(agent_id, group_context)
        config = getattr(agent, "embedder_config", None) if agent else None
        if isinstance(config, dict) and config:
            seen.append(json.dumps(config, sort_keys=True))
    if not seen:
        return None
    return json.loads(Counter(seen).most_common(1)[0][0])


def has_human_feedback(trace: Any, judge_name: str) -> bool:
    """Whether ``trace`` carries a HUMAN assessment named for this judge.

    That is the exact predicate MemAlign uses to build examples; passing it
    anything else makes ``align()`` raise for the whole batch, so the filter
    runs here first. Tolerant of both trace shapes (``info.assessments`` and
    ``search_assessments()``) and of enum-or-string source types.
    """
    assessments: List[Any] = []
    try:
        assessments = list(getattr(trace.info, "assessments", None) or [])
    except Exception:  # noqa: BLE001
        assessments = []
    if not assessments:
        try:
            assessments = list(trace.search_assessments() or [])
        except Exception:  # noqa: BLE001
            return False
    wanted = judge_name.strip().lower()
    for a in assessments:
        name = str(getattr(a, "name", "") or "").strip().lower()
        source = getattr(getattr(a, "source", None), "source_type", None)
        source_text = getattr(source, "value", source)
        if name == wanted and str(source_text or "").upper().endswith("HUMAN"):
            return True
    return False


class JudgeAlignmentMixin:
    """``align_judge`` for PromptOptimizationService — see the module docstring."""

    async def align_judge(
        self,
        name: str,
        crew_id: str,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Align one of the crew's judges to the human grades on its eval traces.

        ``name`` is the judge as assigned to the crew (its crew-scoped registry
        name, or the bare name — the prefix is added). Registering the aligned
        judge under the same name creates a new version, which is what
        ``get_scorer``/``list_scorers`` return — so the next optimization run
        scores with it, no further wiring.

        Returns what was learned, for the UI: the distilled guidelines, how
        many graded answers were used, the model that distilled them and the
        judge's registry name.
        """
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            raise ValueError("Judge alignment requires MLflow (Databricks or local).")
        prefix = self._crew_judge_prefix(crew_id)
        full_name = name if name.startswith(prefix) else f"{prefix}{name}"
        embedder_config = await crew_embedder_config(
            self.session, crew_id, group_context
        )
        # Captured on the request: the bridge submits LLMManager calls back to
        # THIS loop from MemAlign's threads, under this user's context.
        loop = asyncio.get_running_loop()
        user_token = UserContext.get_user_token()

        def _align() -> Dict[str, Any]:
            import mlflow
            from mlflow.genai.judges.optimizers import MemAlignOptimizer
            from mlflow.genai.scorers import get_scorer

            with mlflow_session(backend):
                judge = get_scorer(name=full_name)
                model = _stored_judge_model_to_key(getattr(judge, "model", None))
                if not model:
                    raise ValueError(
                        "This judge has no model. Edit it, pick the model it "
                        "grades with, then align."
                    )
                traces = mlflow.search_traces(
                    filter_string=f"tags.kasal_crew_id = '{crew_id}'",
                    max_results=TRACE_WINDOW,
                    return_type="list",
                )
                graded = [t for t in traces if has_human_feedback(t, full_name)]
                if not graded:
                    raise ValueError(
                        "No graded evaluation answers for this judge yet. Grade a "
                        "few answers with this judge selected, then align."
                    )
                with memalign_via_llm_manager(
                    loop, model, embedder_config, group_context, user_token
                ) as models:
                    optimizer = MemAlignOptimizer(retrieval_k=RETRIEVAL_K, **models)
                    aligned = optimizer.align(judge, graded)
                    aligned.register()
                dump = aligned.model_dump()
                guidelines = [
                    str(g.get("guideline_text", "")).strip()
                    for g in (dump.get("semantic_memory") or [])
                    if isinstance(g, dict)
                ]
                logger.info(
                    "Aligned judge %s with %s from %d graded answers: %d guidelines",
                    full_name,
                    model,
                    len(graded),
                    len(guidelines),
                )
                return {
                    "name": full_name[len(prefix) :],
                    "full_name": full_name,
                    "traces_used": len(graded),
                    "guidelines": [g for g in guidelines if g],
                    "model": model,
                }

        return await asyncio.to_thread(_align)
