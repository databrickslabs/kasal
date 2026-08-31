"""Tests for the MemAlign -> LLMManager bridge.

The point of the bridge: MemAlign's distillation model and embedder are the
judge's UI-configured model and the crew's embedder, routed through
LLMManager — not LiteLLM URIs and environment credentials.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from src.services.prompt_optimization.gepa import memalign_bridge as bridge


class TestBridgedLM:
    def test_forward_calls_llm_manager_with_the_kasal_key(self):
        loop = MagicMock()
        with patch.object(
            bridge, "_sync_llm_completion", return_value='{"guidelines": []}'
        ) as completion:
            lm = bridge._make_lm(loop, "qwen-30b", None, "tok")
            out = lm(
                messages=[{"role": "user", "content": "distil"}],
                response_format={"type": "json_object"},
            )
        # DSPy's legacy contract: a list of completions, text first.
        assert out == ['{"guidelines": []}']
        kwargs = completion.call_args.kwargs
        assert kwargs["model"] == "qwen-30b"
        assert kwargs["messages"] == [{"role": "user", "content": "distil"}]
        assert kwargs["user_token"] == "tok"
        assert completion.call_args.args[0] is loop
        # The placeholder is what mlflow records; it is never what is called.
        assert lm.model == bridge.REFLECTION_PLACEHOLDER


class TestBridgedEmbedder:
    @pytest.mark.asyncio
    async def test_embeds_through_llm_manager_with_the_crews_embedder(self):
        loop = asyncio.get_running_loop()
        config = {"provider": "ollama", "config": {"model": "nomic-embed-text"}}
        seen = []

        async def fake_embedding(
            text, model="databricks-gte-large-en", embedder_config=None
        ):
            seen.append((text, embedder_config))
            return [1.0, 2.0, 3.0]

        with patch(
            "src.services.llm.manager.LLMManager.get_embedding", new=fake_embedding
        ):
            vectors = await asyncio.to_thread(
                bridge._sync_embed, loop, ["a", "b"], config, None, None
            )
        assert vectors == [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]]
        assert seen == [("a", config), ("b", config)]

    @pytest.mark.asyncio
    async def test_a_failed_embedding_is_an_error_not_a_hole(self):
        loop = asyncio.get_running_loop()
        with patch(
            "src.services.llm.manager.LLMManager.get_embedding",
            new=AsyncMock(return_value=None),
        ):
            with pytest.raises(ValueError, match="crew's embedder \\(ollama\\)"):
                await asyncio.to_thread(
                    bridge._sync_embed, loop, ["a"], {"provider": "ollama"}, None, None
                )


class TestInstallAndArm:
    def test_install_is_idempotent_and_an_armed_override_wins(self):
        from mlflow.genai.judges.optimizers.memalign import optimizer as opt
        from mlflow.genai.judges.optimizers.memalign import utils

        bridge._install_memalign_bridge()
        first_lm, first_embedder = utils.construct_dspy_lm, opt._build_embedder
        bridge._install_memalign_bridge()
        assert utils.construct_dspy_lm is first_lm  # not wrapped twice
        assert opt._build_embedder is first_embedder
        assert first_lm._kasal_bridge and first_embedder._kasal_bridge

        lm, embedder = object(), object()
        bridge._OVERRIDE.update(lm=lm, embedder=embedder)
        try:
            assert utils.construct_dspy_lm("openai:/anything") is lm
            assert opt._build_embedder("openai:/anything", 3) is embedder
        finally:
            bridge._OVERRIDE.clear()
        # Disarmed, the wrappers defer to mlflow's own factories.
        assert callable(first_lm._kasal_original)
        assert callable(first_embedder._kasal_original)

    def test_context_measures_the_dimension_and_disarms_after(self):
        loop = MagicMock()
        fake_embedder = MagicMock(return_value=np.zeros(7))
        fake_lm = object()
        with (
            patch.object(bridge, "_install_memalign_bridge"),
            patch.object(bridge, "_make_embedder", return_value=fake_embedder),
            patch.object(bridge, "_make_lm", return_value=fake_lm) as make_lm,
        ):
            with bridge.memalign_via_llm_manager(
                loop, "qwen-30b", {"provider": "ollama"}, None, "tok"
            ) as kwargs:
                assert kwargs == {
                    "reflection_lm": bridge.REFLECTION_PLACEHOLDER,
                    "embedding_model": bridge.EMBEDDING_PLACEHOLDER,
                    "embedding_dim": 7,
                }
                assert bridge._OVERRIDE == {"lm": fake_lm, "embedder": fake_embedder}
        assert bridge._OVERRIDE == {}
        fake_embedder.assert_called_once_with("kasal")
        assert make_lm.call_args.args[1] == "qwen-30b"
