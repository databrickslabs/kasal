"""The catalogue's temperature column is read, not just stored.

``ModelConfig.temperature`` has been seeded on every model for as long as the
catalogue has existed, and nothing consulted it: ``configure_kasal_llm`` keyed
only off its ``temperature`` ARGUMENT, so a caller passing ``None`` sent no
temperature at all and the endpoint's own default (1.0 on an OpenAI-compatible
server) silently won.

``build_agent_llm`` passes ``None`` whenever an agent spec omits a temperature,
which is the ordinary case — so on crew and flow runs the column was dead data
for every model in the catalogue. Nothing failed; the run simply sampled at a
setting nobody chose, and the "Setting temperature ..." log line that would
have given it away was absent rather than wrong.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.llm.manager import LLMManager


def _config(name, provider, temperature=0.7, extra=None):
    config = {
        "name": name,
        "provider": provider,
        "temperature": temperature,
        "context_window": 128000,
        "max_output_tokens": 4096,
    }
    if extra:
        config.update(extra)
    return config


def _patch_lookup(model_config_dict):
    mock_session = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_session
    mock_ctx.__aexit__.return_value = None

    mock_service = AsyncMock()
    mock_service.get_model_config.return_value = model_config_dict

    return (
        patch("src.db.session.request_scoped_session", return_value=mock_ctx),
        patch("src.services.llm.manager.ModelConfigService", return_value=mock_service),
    )


async def _built_kwargs(model_config, temperature):
    """The kwargs the LLM was actually constructed with."""
    p_session, p_service = _patch_lookup(model_config)
    with (
        p_session,
        p_service,
        patch(
            "src.services.llm.manager.ApiKeysService.get_provider_api_key",
            new_callable=AsyncMock,
            return_value="sk-key",
        ),
        patch("src.services.llm.manager.LLM") as MockLLM,
    ):
        await LLMManager.configure_kasal_llm(
            model_config["name"], "group-1", temperature
        )
        return MockLLM.call_args[1]


class TestCatalogueTemperatureFillsTheGap:
    @pytest.mark.asyncio
    async def test_no_override_uses_the_row(self):
        """The case that was silently broken on every crew and flow run."""
        kwargs = await _built_kwargs(_config("gpt-4o", "openai", 0.7), None)

        assert kwargs["temperature"] == 0.7

    @pytest.mark.asyncio
    async def test_an_explicit_override_still_wins(self):
        """Filling a gap, not seizing the decision."""
        kwargs = await _built_kwargs(_config("gpt-4o", "openai", 0.7), 0.2)

        assert kwargs["temperature"] == 0.2

    @pytest.mark.asyncio
    async def test_an_explicit_zero_is_not_mistaken_for_absent(self):
        """0.0 is falsy and is a deliberate, meaningful setting — the check is
        `is None`, and a truthiness test here would silently overwrite a
        caller asking for greedy decoding."""
        kwargs = await _built_kwargs(_config("gpt-4o", "openai", 0.7), 0.0)

        assert kwargs["temperature"] == 0.0

    @pytest.mark.asyncio
    async def test_a_row_without_a_temperature_sends_none(self):
        """Unset stays unset — the server default is the honest answer when
        nobody has expressed a preference."""
        kwargs = await _built_kwargs(_config("gpt-4o", "openai", None), None)

        assert "temperature" not in kwargs


class TestRejectingEndpointsAreStillProtected:
    @pytest.mark.asyncio
    async def test_a_gpt5_row_temperature_is_not_smuggled_through(self):
        """The whole point of the `rejects_temperature` guards: GPT-5 returns
        400 for ANY temperature. Reading the column must not become a new way
        to reach that endpoint with one — there is no drop-params net on this
        path, so what is set IS sent."""
        kwargs = await _built_kwargs(
            _config("gpt-5", "openai", 0.7, extra={"max_output_tokens": 128000}), None
        )

        assert "temperature" not in kwargs
