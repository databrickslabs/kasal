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

Model routing: MemAlign's distillation and embeddings run inside DSPy/LiteLLM,
NOT through LLMManager, so they need LiteLLM-resolvable URIs. On Databricks
``databricks:/<endpoint>`` resolves through the workspace auth mlflow_session
already exports. On a local server the URIs come from the environment
(``KASAL_MEMALIGN_REFLECTION_MODEL`` / ``KASAL_MEMALIGN_EMBEDDING_MODEL``,
e.g. ``ollama:/qwen3:8b`` and ``ollama:/nomic-embed-text``).
"""

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

from src.services.prompt_optimization.gepa.mlflow_session import (
    mlflow_session,
    resolve_mlflow_backend,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

#: Workspace embedding endpoint used on Databricks when none is given.
DATABRICKS_EMBEDDING_MODEL = "databricks-gte-large-en"
DATABRICKS_EMBEDDING_DIM = 1024
#: Similar past examples retrieved per judgment (MemAlign's k).
RETRIEVAL_K = 5
#: Eval traces scanned for grades — the same window list_crew_evals uses.
TRACE_WINDOW = 200


def memalign_models(
    backend: Any,
    reflection_model: Optional[str] = None,
    embedding_model: Optional[str] = None,
    embedding_dim: Optional[int] = None,
) -> Dict[str, Any]:
    """Resolve the LiteLLM-facing model URIs MemAlign will call.

    Explicit arguments win. A value that already carries a ``scheme:/`` is
    used verbatim; a bare Kasal model key is wrapped for the backend. On a
    local server nothing can be guessed — the judge keys are routed by
    LLMManager, which DSPy does not go through — so the environment must say.
    """

    def _uri(value: Optional[str], env: str, databricks_default: Optional[str]) -> str:
        chosen = value or os.environ.get(env) or ""
        if ":/" in chosen:
            return chosen
        if getattr(backend, "kind", "") == "databricks":
            key = chosen or databricks_default or ""
            if key:
                return f"databricks:/{key}"
        raise ValueError(
            f"MemAlign needs a LiteLLM-resolvable model URI: set {env} "
            "(e.g. 'ollama:/qwen3:8b' or 'openai:/gpt-4.1-mini') or pass one "
            "explicitly. On Databricks a bare endpoint name is enough."
        )

    reflection = _uri(
        reflection_model,
        "KASAL_MEMALIGN_REFLECTION_MODEL",
        os.environ.get("GEPA_JUDGE_MODEL"),
    )
    embedding = _uri(
        embedding_model, "KASAL_MEMALIGN_EMBEDDING_MODEL", DATABRICKS_EMBEDDING_MODEL
    )
    if embedding_dim is None:
        env_dim = os.environ.get("KASAL_MEMALIGN_EMBEDDING_DIM")
        if env_dim:
            embedding_dim = int(env_dim)
        elif embedding.startswith("databricks:/"):
            embedding_dim = DATABRICKS_EMBEDDING_DIM
        else:
            # nomic-embed-text and most small open embedders; a mismatch only
            # costs retrieval quality, never a failed alignment.
            embedding_dim = 768
    return {
        "reflection_lm": reflection,
        "embedding_model": embedding,
        "embedding_dim": int(embedding_dim),
    }


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
        reflection_model: Optional[str] = None,
        embedding_model: Optional[str] = None,
        embedding_dim: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Align one of the crew's judges to the human grades on its eval traces.

        ``name`` is the judge as assigned to the crew (its crew-scoped registry
        name, or the bare name — the prefix is added). Registering the aligned
        judge under the same name creates a new version, which is what
        ``get_scorer``/``list_scorers`` return — so the next optimization run
        scores with it, no further wiring.

        Returns what was learned, for the UI: the distilled guidelines, how
        many graded answers were used, and the judge's registry name.
        """
        backend = await resolve_mlflow_backend(self.session, group_context)
        if not backend:
            raise ValueError("Judge alignment requires MLflow (Databricks or local).")
        prefix = self._crew_judge_prefix(crew_id)
        full_name = name if name.startswith(prefix) else f"{prefix}{name}"
        models = memalign_models(
            backend, reflection_model, embedding_model, embedding_dim
        )

        def _align() -> Dict[str, Any]:
            import mlflow
            from mlflow.genai.judges.optimizers import MemAlignOptimizer
            from mlflow.genai.scorers import get_scorer

            with mlflow_session(backend):
                judge = get_scorer(name=full_name)
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
                    "Aligned judge %s from %d graded answers: %d guidelines",
                    full_name,
                    len(graded),
                    len(guidelines),
                )
                return {
                    "name": full_name[len(prefix) :],
                    "full_name": full_name,
                    "traces_used": len(graded),
                    "guidelines": [g for g in guidelines if g],
                    "models": models,
                }

        return await asyncio.to_thread(_align)
