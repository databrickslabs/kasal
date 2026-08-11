"""Every seeded model must have its real context window registered.

An unregistered model falls back to ``DEFAULT_CONTEXT_WINDOW_SIZE`` (8192 →
6963 after the 0.85 derate), so ``respect_context_window`` and the max_tokens
clamp compact it at ~7k tokens no matter how large its real window is.

The registration loop used to cover only Databricks and the self-hosted
OpenAI-compatible endpoints (vllm / airllm / kimi). That gap was invisible while
the catalogue held gpt-4o, gemini-2.0-flash and deepseek-chat, because the
engine's built-in table happened to know those names. Their 2026 replacements —
gpt-5.6, claude-5, gemini-3.x, deepseek-v4 — are in no built-in table, so a
1M-token model was being compacted at 7k.

The lookup is by EXACT key, so the registered id has to match the
``prefixed_model`` that ``configure_crewai_llm`` builds for that provider.
"""

import pytest

# Importing llm_manager runs the registration at module scope.
import src.services.llm.manager  # noqa: F401
from src.core.llm.transport import LLM_CONTEXT_WINDOW_SIZES
from src.core.llm.transport.constants import DEFAULT_CONTEXT_WINDOW_SIZE
from src.seeds.model_configs import DEFAULT_MODELS

# Mirrors the prefixes in configure_crewai_llm. Duplicated on purpose: if the
# call-time prefix changes without the registration following, this test fails
# rather than the run silently compacting at 7k.
PREFIXES = {
    "databricks": "databricks/",
    "vllm": "openai/",
    "airllm": "openai/",
    "kimi": "openai/",
    "openai": "",
    "anthropic": "anthropic/",
    "gemini": "gemini/",
    "deepseek": "deepseek/",
    "ollama": "ollama/",
    # Self-hosted OpenAI-compatible endpoint: the model name is passed through
    # unchanged, since the box answers for whatever name it was deployed under.
    "custom": "",
}


@pytest.mark.parametrize("model_key", sorted(DEFAULT_MODELS))
def test_seeded_model_window_is_registered_under_its_call_time_id(model_key):
    config = DEFAULT_MODELS[model_key]
    provider = config["provider"]
    assert provider in PREFIXES, f"no litellm prefix known for provider {provider!r}"

    call_time_id = f"{PREFIXES[provider]}{model_key}"
    assert (
        LLM_CONTEXT_WINDOW_SIZES.get(call_time_id) == config["context_window"]
    ), f"{call_time_id} would fall back to {DEFAULT_CONTEXT_WINDOW_SIZE} tokens"


@pytest.mark.parametrize("model_key", sorted(DEFAULT_MODELS))
def test_bare_model_name_also_resolves(model_key):
    """Configs carry the unprefixed key, so that must resolve too."""
    assert (
        LLM_CONTEXT_WINDOW_SIZES.get(model_key)
        == DEFAULT_MODELS[model_key]["context_window"]
    )


def test_large_window_models_are_not_derated_to_the_default():
    """The failure this guards is silent, so assert the symptom directly."""
    big = {
        k: c["context_window"]
        for k, c in DEFAULT_MODELS.items()
        if c["context_window"] >= 200_000
    }
    assert big, "expected the catalogue to contain large-context models"
    for key, window in big.items():
        registered = LLM_CONTEXT_WINDOW_SIZES.get(key)
        assert registered == window
        assert registered > DEFAULT_CONTEXT_WINDOW_SIZE
