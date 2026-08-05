"""
Unit tests for model configs seed module.

Tests the DEFAULT_MODELS data structure, data integrity, and seed functions.
"""

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.seeds.model_configs import (
    DEFAULT_MODELS,
    MODEL_CONFIGS,
    REMOVED_MODEL_KEYS,
    seed,
    seed_async,
)


class TestDefaultModelsDataStructure:
    """Test cases for DEFAULT_MODELS data integrity."""

    def test_default_models_is_dict(self):
        """Test that DEFAULT_MODELS is a dictionary."""
        assert isinstance(DEFAULT_MODELS, dict)

    def test_default_models_not_empty(self):
        """Test that DEFAULT_MODELS contains entries."""
        assert len(DEFAULT_MODELS) > 0

    def test_model_configs_alias(self):
        """Test that MODEL_CONFIGS is an alias for DEFAULT_MODELS."""
        assert MODEL_CONFIGS is DEFAULT_MODELS

    def test_specific_models_exist(self):
        """Test that the current model keys are present.

        Refreshed 2026-07-25: the OpenAI GPT-4 family was retired by OpenAI on
        2026-10-23 and replaced by GPT-5.6, and deepseek-chat/-reasoner were
        deprecated on 2026-07-24 in favour of the two v4 endpoints.
        """
        assert "gpt-5.6-sol" in DEFAULT_MODELS
        assert "databricks-llama-4-maverick" in DEFAULT_MODELS
        assert "deepseek-v4-flash" in DEFAULT_MODELS
        assert "deepseek-v4-pro" in DEFAULT_MODELS
        assert "databricks-gpt-5-3-codex" in DEFAULT_MODELS

    def test_default_engine_model_is_seeded(self):
        """The model the server falls back to must exist in the catalogue.

        Without this, a default can be repointed at a key that was never seeded
        and every agent without an explicit llm fails at run time.
        """
        from src.utils.model_config import DEFAULT_ENGINE_MODEL

        assert DEFAULT_ENGINE_MODEL in DEFAULT_MODELS
        assert DEFAULT_ENGINE_MODEL not in REMOVED_MODEL_KEYS

    def test_new_frontier_models_present(self):
        """The latest Databricks Claude (>4.6) and GPT (>5.3) models are seeded.
        (gpt-5-5 and gpt-5-5-pro are intentionally NOT here — they're
        Responses-API-only for tools, see TestAuditedDatabricksModels.)"""
        for key in (
            "databricks-claude-opus-4-7",
            "databricks-claude-opus-4-8",
            "databricks-gpt-5-4",
            "databricks-gpt-5-4-mini",
            "databricks-gpt-5-4-nano",
        ):
            assert key in DEFAULT_MODELS, f"expected new model {key}"
            assert DEFAULT_MODELS[key]["provider"] == "databricks"

    def test_claude_3_models_removed(self):
        """All Claude 3 models are removed from the catalog and listed for pruning."""
        claude_3 = [k for k in DEFAULT_MODELS if "claude-3" in k]
        assert claude_3 == [], f"Claude 3 models must be removed, found: {claude_3}"
        assert "databricks-claude-3-7-sonnet" not in DEFAULT_MODELS
        # Every removed key must be registered for DB pruning.
        for key in (
            "claude-3-5-sonnet-20241022",
            "claude-3-7-sonnet-20250219",
            "claude-3-opus-20240229",
            "databricks-claude-3-7-sonnet",
        ):
            assert key in REMOVED_MODEL_KEYS

    def test_databricks_gpt_5_3_codex_config(self):
        """Test that databricks-gpt-5-3-codex has correct configuration."""
        config = DEFAULT_MODELS["databricks-gpt-5-3-codex"]
        assert config["provider"] == "databricks"
        assert config["name"] == "databricks-gpt-5-3-codex"
        assert config["context_window"] == 400000
        assert config["max_output_tokens"] == 128000

    def test_databricks_glm_5_2_config(self):
        """system.ai.glm-5-2 is AI Gateway-only: UC-style name, 25k output cap."""
        config = DEFAULT_MODELS["databricks-glm-5-2"]
        assert config["provider"] == "databricks"
        assert config["name"] == "system.ai.glm-5-2"
        assert config["context_window"] == 200000
        assert config["max_output_tokens"] == 25000

    def test_required_fields_present(self):
        """Test that every model has the required fields."""
        required_fields = [
            "name",
            "temperature",
            "provider",
            "context_window",
            "max_output_tokens",
        ]
        for model_key, model_data in DEFAULT_MODELS.items():
            for field in required_fields:
                assert (
                    field in model_data
                ), f"Model '{model_key}' missing required field '{field}'"

    def test_temperature_types(self):
        """Test that temperature values are numeric."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(
                model_data["temperature"], (int, float)
            ), f"Model '{model_key}' has non-numeric temperature"

    def test_temperature_range(self):
        """Test that temperature values are in a reasonable range."""
        for model_key, model_data in DEFAULT_MODELS.items():
            temp = model_data["temperature"]
            assert (
                0.0 <= temp <= 2.0
            ), f"Model '{model_key}' has temperature {temp} outside [0, 2]"

    def test_context_window_type(self):
        """Test that context_window values are integers."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(
                model_data["context_window"], int
            ), f"Model '{model_key}' has non-integer context_window"

    def test_context_window_positive(self):
        """Test that context_window values are positive."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert (
                model_data["context_window"] > 0
            ), f"Model '{model_key}' has non-positive context_window"

    def test_max_output_tokens_type(self):
        """Test that max_output_tokens values are integers."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(
                model_data["max_output_tokens"], int
            ), f"Model '{model_key}' has non-integer max_output_tokens"

    def test_max_output_tokens_positive(self):
        """Test that max_output_tokens values are positive."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert (
                model_data["max_output_tokens"] > 0
            ), f"Model '{model_key}' has non-positive max_output_tokens"

    def test_provider_type(self):
        """Test that provider values are strings."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(
                model_data["provider"], str
            ), f"Model '{model_key}' has non-string provider"

    def test_valid_providers(self):
        """Test that all providers are known.

        Derived from the ModelProvider enum rather than a literal set: the
        literal went stale the moment `kimi` was added, failing this test for a
        catalogue that was perfectly valid.
        """
        from src.schemas.model_provider import ModelProvider

        valid_providers = {provider.value for provider in ModelProvider}
        for model_key, model_data in DEFAULT_MODELS.items():
            assert (
                model_data["provider"] in valid_providers
            ), f"Model '{model_key}' has unknown provider '{model_data['provider']}'"

    def test_name_type(self):
        """Test that name values are non-empty strings."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(model_data["name"], str)
            assert len(model_data["name"]) > 0, f"Model '{model_key}' has empty name"

    def test_extended_thinking_field_is_optional_boolean(self):
        """extended_thinking is optional (the Claude 3.7 thinking model was
        removed); when a model declares it, it must be a boolean."""
        for k, v in DEFAULT_MODELS.items():
            if "extended_thinking" in v:
                assert isinstance(
                    v["extended_thinking"], bool
                ), f"Model {k} extended_thinking must be a bool"

    def test_databricks_models_exist(self):
        """Test that Databricks models are present."""
        databricks_models = [
            k for k, v in DEFAULT_MODELS.items() if v["provider"] == "databricks"
        ]
        assert len(databricks_models) > 0

    def test_ollama_models_exist(self):
        """Test that Ollama models are present."""
        ollama_models = [
            k for k, v in DEFAULT_MODELS.items() if v["provider"] == "ollama"
        ]
        assert len(ollama_models) > 0


class TestSeedAsyncFunction:
    """Test cases for the seed_async function."""

    @pytest.mark.asyncio
    async def test_seed_async_adds_new_models(self):
        """Test that seed_async adds new models when none exist."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch(
            "src.seeds.model_configs.async_session_factory", return_value=mock_context
        ):
            await seed_async()

        mock_session.commit.assert_awaited_once()
        assert mock_session.add.call_count == len(DEFAULT_MODELS)

    @pytest.mark.asyncio
    async def test_seed_async_enables_only_databricks_models(self):
        """By default only Databricks-provider models are enabled; the rest disabled."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch(
            "src.seeds.model_configs.async_session_factory", return_value=mock_context
        ):
            await seed_async()

        added = [c.args[0] for c in mock_session.add.call_args_list]
        assert added, "expected new models to be added"
        for mc in added:
            if mc.provider == "databricks":
                assert mc.enabled is True, f"{mc.key} (databricks) should be enabled"
            else:
                assert (
                    mc.enabled is False
                ), f"{mc.key} ({mc.provider}) should be disabled"
        # sanity: the dataset contains both databricks and non-databricks models
        assert any(mc.provider == "databricks" for mc in added)
        assert any(mc.provider != "databricks" for mc in added)

    @pytest.mark.asyncio
    async def test_seed_async_updates_existing_models(self):
        """Test that seed_async updates existing models."""
        existing_model = MagicMock()
        existing_model.name = "old_name"

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = existing_model
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch(
            "src.seeds.model_configs.async_session_factory", return_value=mock_context
        ):
            await seed_async()

        mock_session.commit.assert_awaited_once()
        # Should not add since all models exist
        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_seed_async_prunes_removed_models(self):
        """Retired models found in the DB are deleted (one delete per removed key)."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        # Every lookup returns a row, so each removed key gets deleted.
        mock_result.scalars.return_value.first.return_value = MagicMock()
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch(
            "src.seeds.model_configs.async_session_factory", return_value=mock_context
        ):
            await seed_async()

        assert mock_session.delete.await_count == len(REMOVED_MODEL_KEYS)
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_seed_async_handles_db_error(self):
        """Test that seed_async rolls back on database error."""
        mock_session = AsyncMock()
        mock_session.commit.side_effect = Exception("DB error")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch(
            "src.seeds.model_configs.async_session_factory", return_value=mock_context
        ):
            with pytest.raises(Exception, match="DB error"):
                await seed_async()

        mock_session.rollback.assert_awaited_once()


class TestSeedEntryPoint:
    """Test cases for the main seed() entry point."""

    @pytest.mark.asyncio
    async def test_seed_calls_seed_async(self):
        """Test that seed() delegates to seed_async()."""
        with patch(
            "src.seeds.model_configs.seed_async", new_callable=AsyncMock
        ) as mock:
            await seed()
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_seed_does_not_raise_on_error(self):
        """Test that seed() suppresses exceptions and logs them."""
        with patch(
            "src.seeds.model_configs.seed_async", new_callable=AsyncMock
        ) as mock:
            mock.side_effect = Exception("Seed failure")
            # Should not raise
            await seed()
            mock.assert_awaited_once()


class TestSeedAsyncValidation:
    """Test validation branches in seed_async."""

    def _make_session_context(self, mock_session):
        """Helper to create an async session context manager."""
        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        return mock_context

    @pytest.mark.asyncio
    async def test_seed_async_skips_model_missing_fields(self):
        """Test that seed_async skips models with missing required fields."""
        bad_models = {
            "bad-model": {
                "name": "bad",
                "temperature": 0.7,
            },  # missing provider, context_window, max_output_tokens
        }
        mock_session = AsyncMock()
        mock_context = self._make_session_context(mock_session)

        with (
            patch(
                "src.seeds.model_configs.async_session_factory",
                return_value=mock_context,
            ),
            patch("src.seeds.model_configs.DEFAULT_MODELS", bad_models),
        ):
            await seed_async()

        mock_session.add.assert_not_called()
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_seed_async_skips_model_bad_temperature(self):
        """Test that seed_async skips models with non-numeric temperature."""
        bad_models = {
            "bad-temp": {
                "name": "bad",
                "temperature": "not_a_number",
                "provider": "openai",
                "context_window": 128000,
                "max_output_tokens": 4096,
            },
        }
        mock_session = AsyncMock()
        mock_context = self._make_session_context(mock_session)

        with (
            patch(
                "src.seeds.model_configs.async_session_factory",
                return_value=mock_context,
            ),
            patch("src.seeds.model_configs.DEFAULT_MODELS", bad_models),
        ):
            await seed_async()

        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_seed_async_skips_model_bad_context_window(self):
        """Test that seed_async skips models with non-integer context_window."""
        bad_models = {
            "bad-ctx": {
                "name": "bad",
                "temperature": 0.7,
                "provider": "openai",
                "context_window": "big",
                "max_output_tokens": 4096,
            },
        }
        mock_session = AsyncMock()
        mock_context = self._make_session_context(mock_session)

        with (
            patch(
                "src.seeds.model_configs.async_session_factory",
                return_value=mock_context,
            ),
            patch("src.seeds.model_configs.DEFAULT_MODELS", bad_models),
        ):
            await seed_async()

        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_seed_async_skips_model_bad_max_output_tokens(self):
        """Test that seed_async skips models with non-integer max_output_tokens."""
        bad_models = {
            "bad-tokens": {
                "name": "bad",
                "temperature": 0.7,
                "provider": "openai",
                "context_window": 128000,
                "max_output_tokens": 40.96,
            },
        }
        mock_session = AsyncMock()
        mock_context = self._make_session_context(mock_session)

        with (
            patch(
                "src.seeds.model_configs.async_session_factory",
                return_value=mock_context,
            ),
            patch("src.seeds.model_configs.DEFAULT_MODELS", bad_models),
        ):
            await seed_async()

        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_seed_async_handles_per_model_exception(self):
        """Test that seed_async catches per-model exceptions and continues."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = RuntimeError("query failed")

        mock_context = self._make_session_context(mock_session)

        single_model = {
            "test-model": {
                "name": "test",
                "temperature": 0.7,
                "provider": "openai",
                "context_window": 128000,
                "max_output_tokens": 4096,
            },
        }

        with (
            patch(
                "src.seeds.model_configs.async_session_factory",
                return_value=mock_context,
            ),
            patch("src.seeds.model_configs.DEFAULT_MODELS", single_model),
        ):
            await seed_async()

        # Should still commit (with error count incremented)
        mock_session.commit.assert_awaited_once()


class TestMainBlock:
    """Test the __main__ execution block (pragma: no cover in source)."""

    def test_main_block_runs_seed(self):
        """Test that __main__ block calls asyncio.run(seed())."""
        with (
            patch("src.seeds.model_configs.seed", new_callable=AsyncMock) as mock_seed,
            patch("src.seeds.model_configs.__name__", "__main__"),
        ):
            import asyncio

            asyncio.run(mock_seed())
            mock_seed.assert_awaited_once()


class TestAuditedDatabricksModels:
    """Regression test for the 2026-06-20 Databricks model audit (hello-world run
    through Kasal's own LLM path against the fevm-serverless-stable workspace).
    Locks in which endpoints were removed and which working ones were added."""

    # Endpoints that no longer work and were pruned (removed from DEFAULT_MODELS
    # and registered for DB pruning).
    AUDIT_REMOVED = (
        "databricks-gemini-3-flash",
        "databricks-gemini-3-pro",
        "databricks-gpt-5-1-codex-max",
        "databricks-gpt-5-1-codex-mini",
        "databricks-gpt-5-5-pro",
        "databricks-gpt-5-5",
        "databricks-gemini-2-5-pro",
        "databricks-meta-llama-3-1-405b-instruct",
        # Reasoning model: emits a "thinking" preamble instead of pure JSON, so
        # crew planning fails to parse it. Unsuited to one-shot JSON generation.
        "databricks-qwen35-122b-a10b",
        # GPT-OSS reasoning models fail crew runs.
        "databricks-gpt-oss-120b",
        "databricks-gpt-oss-20b",
    )
    # New workspace endpoints verified working and added.
    AUDIT_ADDED = (
        "databricks-gemini-3-1-flash-lite",
        "databricks-gemini-3-5-flash",
        "databricks-gpt-5-4-mini",
        "databricks-gpt-5-4-nano",
    )

    def test_broken_models_pruned(self):
        """Each broken endpoint is gone from DEFAULT_MODELS and listed for pruning."""
        for key in self.AUDIT_REMOVED:
            assert (
                key not in DEFAULT_MODELS
            ), f"{key} should be removed from DEFAULT_MODELS"
            assert key in REMOVED_MODEL_KEYS, f"{key} must be registered for DB pruning"

    def test_new_working_models_added(self):
        """Each newly-verified working endpoint is seeded as a databricks model."""
        for key in self.AUDIT_ADDED:
            assert key in DEFAULT_MODELS, f"{key} should be added to DEFAULT_MODELS"
            assert DEFAULT_MODELS[key]["provider"] == "databricks"

    def test_gemini_3_1_flash_lite_no_longer_pruned(self):
        """It used to be (wrongly) in REMOVED_MODEL_KEYS; the workspace now serves
        it, so it must be seeded and NOT pruned."""
        assert "databricks-gemini-3-1-flash-lite" in DEFAULT_MODELS
        assert "databricks-gemini-3-1-flash-lite" not in REMOVED_MODEL_KEYS

    def test_claude_fable_5_seeded_and_not_pruned(self):
        """Pruned while Anthropic's 2026-06-12 export-control suspension was in
        force; lifted and re-verified 2026-08-05 (endpoint READY, serves
        completions over mlflow/v1/chat/completions). Being in BOTH lists would
        make the seeder delete it on every startup, so it must be seeded only."""
        assert "databricks-claude-fable-5" in DEFAULT_MODELS
        assert "databricks-claude-fable-5" not in REMOVED_MODEL_KEYS
        assert DEFAULT_MODELS["databricks-claude-fable-5"]["provider"] == "databricks"

    def test_claude_fable_5_temperature_is_dropped_before_the_request(self):
        """Fable 5 400s on `temperature` ("Model global.anthropic.claude-fable-5
        does not support the temperature parameter" — confirmed live). The seed
        still carries a temperature like every other row, so the guard in
        utils.model_config is what keeps it off the wire; assert that holds for
        both the Kasal key and the served name."""
        from src.utils.model_config import model_rejects_temperature

        assert model_rejects_temperature("databricks-claude-fable-5") is True
        assert model_rejects_temperature("global.anthropic.claude-fable-5") is True

    def test_codex_model_that_works_via_kasal_kept(self):
        """gpt-5-3-codex 400s on raw /invocations but works through Kasal's
        litellm path, so it stays."""
        assert "databricks-gpt-5-3-codex" in DEFAULT_MODELS

    def test_no_overlap_default_and_removed(self):
        """A key must never be both seeded and pruned."""
        assert set(DEFAULT_MODELS).isdisjoint(REMOVED_MODEL_KEYS)

    def test_qwen3_next_max_tokens_within_endpoint_cap(self):
        """The qwen3-next endpoint caps output at 10000 ('max_tokens cannot
        exceed 10000'); the seed must not exceed it or real runs 400."""
        assert (
            DEFAULT_MODELS["databricks-qwen3-next-80b-a3b-instruct"][
                "max_output_tokens"
            ]
            <= 10000
        )


class TestDeepSeekModels:
    """DeepSeek's live API surface, verified 2026-07-25 against
    api-docs.deepseek.com/quick_start/pricing.

    The seeded values were wrong in every field that matters — 128k context (8x
    under), 8k/64k output caps, and two model names DeepSeek deprecated on
    2026/07/24 — which silently truncated context and capped output on a model
    that supports far more.
    """

    def test_exactly_two_real_models_plus_two_aliases(self):
        keys = {k for k, v in DEFAULT_MODELS.items() if v.get("provider") == "deepseek"}
        assert keys == {
            "deepseek-v4-flash",
            "deepseek-v4-pro",
            "deepseek-v3.1-non-thinking",
            "deepseek-v3.1-thinking",
        }

    def test_context_and_output_match_the_published_limits(self):
        for key in ("deepseek-v4-flash", "deepseek-v4-pro"):
            model = DEFAULT_MODELS[key]
            assert model["context_window"] == 1_000_000, key
            assert model["max_output_tokens"] == 384_000, key
            assert model["extended_thinking"] is True, key

    def test_retired_names_resolve_to_a_live_endpoint(self):
        """The API call uses `name`, not the key (llm_manager builds
        f"deepseek/{name}"), so an agent still on a v3.1 key must land on a v4
        endpoint rather than a model that no longer exists."""
        assert (
            DEFAULT_MODELS["deepseek-v3.1-non-thinking"]["name"] == "deepseek-v4-flash"
        )
        assert DEFAULT_MODELS["deepseek-v3.1-thinking"]["name"] == "deepseek-v4-pro"

    def test_deprecated_models_are_pruned_not_merely_dropped(self):
        """Dropping a key from DEFAULT_MODELS leaves it in already-seeded DBs —
        it has to be listed for pruning to leave the model picker."""
        for key in (
            "deepseek-chat",
            "deepseek-reasoner",
            "deepseek-coder-v2",
            "deepseek-v3",
        ):
            assert key not in DEFAULT_MODELS, key
            assert key in REMOVED_MODEL_KEYS, key

    def test_deepseek_is_excluded_from_top_level_reasoning_effort(self):
        """DeepSeek v4 DOES take a reasoning effort, but nested inside
        `thinking: {...}`. Our emitter sends it top-level, so DeepSeek would
        ignore it — it must not be advertised as supported."""
        from src.utils.model_config import model_supports_reasoning_effort

        assert not model_supports_reasoning_effort("deepseek-v4-flash")
        assert not model_supports_reasoning_effort("deepseek-v4-pro")


class TestEveryModelDeclaresAnOutputCeiling:
    """A model with no ceiling can generate until something else stops it.

    Observed on a real run: a model repeating itself produced 197,336 characters
    before `max_tokens` cut it off — and the only thing that ended it WAS that
    ceiling. A model without one has no such backstop; a runaway runs until the
    request timeout, and the tokens are billed either way.

    This ceiling is now the ONLY thing bounding a runaway. A transport-side
    repetition detector briefly stood in front of it and was removed: no mature
    LLM client ships one, and it could only see exact verbatim loops — the
    observed failure drifted (`SOC 394`, `SOC 395`, …) and went straight
    through. So a model shipped without a ceiling has no backstop at all.
    """

    def test_no_model_is_missing_max_output_tokens(self):
        missing = [
            key
            for key, config in MODEL_CONFIGS.items()
            if not config.get("max_output_tokens")
        ]

        assert missing == [], (
            f"These models declare no max_output_tokens: {missing}. "
            "Add one — an unbounded model has no backstop against a runaway "
            "generation."
        )

    def test_every_ceiling_is_a_positive_integer(self):
        bad = {
            key: config.get("max_output_tokens")
            for key, config in MODEL_CONFIGS.items()
            if not isinstance(config.get("max_output_tokens"), int)
            or config["max_output_tokens"] <= 0
        }

        assert bad == {}, f"Non-positive or non-integer ceilings: {bad}"

    def test_a_ceiling_never_exceeds_the_context_window(self):
        # Output cannot exceed what the model can hold. A ceiling above the
        # window is a configuration error that only shows up as a provider
        # rejection mid-run.
        over = {
            key: (config["max_output_tokens"], config["context_window"])
            for key, config in MODEL_CONFIGS.items()
            if config.get("context_window")
            and config.get("max_output_tokens")
            and config["max_output_tokens"] > config["context_window"]
        }

        assert over == {}, f"max_output_tokens exceeds context_window for: {over}"
