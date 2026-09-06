"""MemAlign persistence and retrieval, with Kasal-owned model routing.

The registry stores MLflow's semantic records and episodic trace references,
not DSPy objects or credentials. Retrieval reconstructs an index once per judge
per optimization run from the active backend's traces. MLflow's private memory
helpers are isolated here and exercised against the installed MLflow package.
"""

from __future__ import annotations

import json
import threading
from collections import Counter
from typing import Any, Dict, Optional

from src.services.prompt_optimization.gepa.memalign_bridge import _make_embedder
from src.services.prompt_optimization.judge_registry import JudgeSpec, strip_guidelines


def serialize_memory(aligned: Any) -> Dict[str, Any]:
    """Read the actual MLflow serialization envelope, not a mocked flat dump."""
    data = aligned.model_dump()["memory_augmented_judge_data"]
    return {
        "schema_version": 1,
        "semantic_memory": data["semantic_memory"],
        "episodic_trace_ids": list(dict.fromkeys(data.get("episodic_trace_ids") or [])),
        "retrieval_k": data.get("retrieval_k", 5),
    }


def majority_embedder(configs: list) -> Optional[Dict[str, Any]]:
    choices = [
        json.dumps(c, sort_keys=True) for c in configs if isinstance(c, dict) and c
    ]
    return json.loads(Counter(choices).most_common(1)[0][0]) if choices else None


def load_memory_trace(trace_id: str):
    """Expired/deleted traces are absent; auth and connectivity failures are errors."""
    import mlflow
    from mlflow.exceptions import MlflowException

    try:
        return mlflow.get_trace(trace_id)
    except MlflowException as exc:
        if exc.error_code in {"RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"}:
            return None
        raise


class JudgeMemory:
    """Per-run retrieval state; never shared between workspaces or judge versions."""

    def __init__(
        self,
        spec: JudgeSpec,
        loop: Any,
        embedder_config=None,
        group_context=None,
        user_token=None,
    ):
        self.spec = spec
        self.loop = loop
        self.embedder_config = embedder_config
        self.group_context = group_context
        self.user_token = user_token
        self._judge = None
        self._lock = threading.Lock()

    def _load(self):
        from mlflow.genai.judges import make_judge
        from mlflow.genai.judges.optimizers.dspy_utils import (
            create_dspy_signature,
            trace_to_dspy_example,
        )
        from mlflow.genai.judges.optimizers.memalign.optimizer import (
            MemoryAugmentedJudge,
        )

        base, _ = strip_guidelines(self.spec.instructions)
        judge = MemoryAugmentedJudge(
            base_judge=make_judge(
                name=self.spec.name,
                instructions=base,
                model="openai:/kasal-llm-manager",
                feedback_value_type=float,
            ),
            retrieval_k=self.spec.memory.get("retrieval_k", 5),
            embedding_model="openai:/kasal-embedder",
            _defer_init=True,
        )
        # Explicit instance wiring avoids the process-wide alignment bridge:
        # simultaneous scoring in two workspaces must never share an embedder.
        judge._base_signature = create_dspy_signature(judge._base_judge)
        judge._embedder = _make_embedder(
            self.loop, self.embedder_config, self.group_context, self.user_token
        )
        judge._episodic_trace_ids = self.spec.memory.get("episodic_trace_ids", [])
        # MLflow's default reconstruction uses get_trace(silent=True), which
        # also hides permission/server errors. Only missing traces may be skipped.
        judge._episodic_memory = []
        for trace_id in judge._episodic_trace_ids:
            trace = load_memory_trace(trace_id)
            if trace is not None:
                judge._episodic_memory.extend(
                    trace_to_dspy_example(trace, judge._base_judge)
                )
        if judge._episodic_memory:
            judge._build_episodic_memory()
        return judge

    def instructions(self, *, inputs: Any, outputs: Any) -> str:
        if not getattr(self.spec, "memory", None):
            return self.spec.instructions
        from mlflow.genai.judges.optimizers.memalign.utils import (
            retrieve_relevant_examples,
        )

        # DSPy initializes its index lazily; keep initialization and retrieval
        # serialized for the one judge, without locking other workspaces.
        with self._lock:
            if self._judge is None:
                self._judge = self._load()
            judge = self._judge
            examples = retrieve_relevant_examples(
                retriever=judge._retriever,
                examples=judge._episodic_memory,
                query_kwargs={"inputs": inputs, "outputs": outputs},
                signature=judge._base_signature,
            )
            _, guidelines = strip_guidelines(self.spec.instructions)
            return judge._build_augmented_instructions(
                guidelines, [example for example, _ in examples]
            )
