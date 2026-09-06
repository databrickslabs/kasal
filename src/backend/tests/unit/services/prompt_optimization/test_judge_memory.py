"""Exercise real MLflow serialization, trace conversion and DSPy retrieval.

Only external storage and embedding/model calls are replaced. This catches
MLflow envelope/API changes that a mocked MemAlignOptimizer would conceal.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.prompt_optimization.gepa.judge_memory import (
    JudgeMemory,
    load_memory_trace,
    serialize_memory,
)
from src.services.prompt_optimization.judge_registry import (
    JudgeRegistry,
    JudgeSpec,
    with_guidelines,
)

NAME = "crew_88ab4478823c__accuracy"
BASE = "Rate {{ outputs }} for accuracy."


def trace(tid, answer, rationale, grade=2):
    from mlflow.entities import AssessmentSource, Feedback

    feedback = Feedback(
        name=NAME,
        value=grade,
        rationale=rationale,
        source=AssessmentSource(source_type="HUMAN", source_id="reviewer"),
    )
    return SimpleNamespace(
        info=SimpleNamespace(trace_id=tid, assessments=[feedback]),
        data=SimpleNamespace(
            _get_root_span=lambda: SimpleNamespace(inputs="research", outputs=answer)
        ),
        search_assessments=lambda **kwargs: [],
    )


def aligned_judge():
    from mlflow.genai.judges import make_judge
    from mlflow.genai.judges.optimizers.memalign.optimizer import MemoryAugmentedJudge
    from mlflow.genai.judges.optimizers.memalign.utils import Guideline

    judge = MemoryAugmentedJudge(
        make_judge(
            name=NAME,
            instructions=BASE,
            model="openai:/test",
            feedback_value_type=float,
        ),
        retrieval_k=1,
        _defer_init=True,
    )
    judge._semantic_memory = [
        Guideline(guideline_text="Use certified sources.", source_trace_ids=["swiss"])
    ]
    judge._episodic_trace_ids = ["swiss", "dutch"]
    return judge


def round_trip(memory):
    client = MagicMock()
    client.register_prompt.side_effect = lambda **kw: SimpleNamespace(
        template=kw["template"], tags=kw["tags"], version=4
    )
    registry = JudgeRegistry("http://mlflow", client=client)
    original = with_guidelines(BASE, ["Use certified sources."])
    registry.save(NAME, original, "kasal-model", memory=memory)
    client.search_prompt_versions.return_value = [SimpleNamespace(version=4)]
    client.load_prompt.return_value = client.register_prompt.side_effect(
        **client.register_prompt.call_args.kwargs
    )
    loaded = JudgeRegistry("http://mlflow", client=client).load(NAME)
    assert loaded.instructions == original
    assert loaded.memory == memory
    assert "episodic_trace_ids" not in loaded.as_dict()["instructions"]
    return loaded


@pytest.mark.asyncio
async def test_reload_retrieves_relevant_human_examples_through_kasal_embedder():
    memory = serialize_memory(aligned_judge())
    assert memory["semantic_memory"][0]["guideline_text"] == "Use certified sources."
    spec = round_trip(memory)
    traces = {
        "swiss": trace("swiss", "Swiss news", "Swiss stories must cite Swiss sources."),
        "dutch": trace("dutch", "Dutch news", "Dutch stories must cite Dutch sources."),
    }
    config = {"provider": "ollama", "config": {"model": "embed"}}
    seen = []

    async def embed(text, **kwargs):
        seen.append(kwargs["embedder_config"])
        return [1.0, 0.0] if "Swiss" in text else [0.0, 1.0]

    reader = JudgeMemory(spec, asyncio.get_running_loop(), config)
    with (
        patch(
            "mlflow.get_trace", side_effect=lambda tid, **kw: traces[tid]
        ) as get_trace,
        patch("src.services.llm.manager.LLMManager.get_embedding", new=embed),
    ):
        swiss = await asyncio.to_thread(
            reader.instructions, inputs="news", outputs="Swiss headlines"
        )
        dutch = await asyncio.to_thread(
            reader.instructions, inputs="news", outputs="Dutch headlines"
        )
    assert "Swiss stories must cite Swiss sources." in swiss
    assert "Dutch stories must cite Dutch sources." not in swiss
    assert "Dutch stories must cite Dutch sources." in dutch
    assert "Swiss stories must cite Swiss sources." not in dutch
    assert "Use certified sources." in swiss and "Use certified sources." in dutch
    assert get_trace.call_count == 2  # corpus loaded once, not per judgment
    assert seen and all(c == config for c in seen)
    assert (
        "Example Judgements" not in spec.instructions
    )  # per-call context stays transient


def test_legacy_judge_needs_no_trace_or_embedding_access():
    spec = JudgeSpec(NAME, BASE, "model")
    reader = JudgeMemory(spec, None)
    with patch("mlflow.get_trace") as get_trace:
        assert reader.instructions(inputs="x", outputs="y") == BASE
        get_trace.assert_not_called()


@pytest.mark.asyncio
async def test_embedding_failure_is_reported_instead_of_silently_ignoring_memory():
    spec = round_trip(serialize_memory(aligned_judge()))
    reader = JudgeMemory(spec, asyncio.get_running_loop())
    with (
        patch(
            "mlflow.get_trace",
            side_effect=lambda tid, **kw: trace(tid, "Swiss", "Use sources"),
        ),
        patch(
            "src.services.llm.manager.LLMManager.get_embedding",
            new=AsyncMock(return_value=None),
        ),
    ):
        with pytest.raises(ValueError, match="Embedding failed"):
            await asyncio.to_thread(reader.instructions, inputs="news", outputs="Swiss")


@pytest.mark.asyncio
async def test_missing_traces_do_not_break_guideline_only_scoring():
    spec = round_trip(serialize_memory(aligned_judge()))
    reader = JudgeMemory(spec, asyncio.get_running_loop())
    with patch("mlflow.get_trace", return_value=None):
        instructions = await asyncio.to_thread(
            reader.instructions, inputs="news", outputs="Swiss"
        )
    assert "Use certified sources." in instructions
    assert "Example Judgements" not in instructions


def test_only_missing_traces_are_skipped_not_permissions_or_server_failures():
    from mlflow.exceptions import MlflowException
    from mlflow.protos.databricks_pb2 import PERMISSION_DENIED, RESOURCE_DOES_NOT_EXIST

    with patch(
        "mlflow.get_trace",
        side_effect=MlflowException("gone", error_code=RESOURCE_DOES_NOT_EXIST),
    ):
        assert load_memory_trace("gone") is None
    with patch(
        "mlflow.get_trace",
        side_effect=MlflowException("denied", error_code=PERMISSION_DENIED),
    ):
        with pytest.raises(MlflowException, match="denied"):
            load_memory_trace("private")
