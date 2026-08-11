"""
Model provider enums and constants.

This module provides the enumeration of LLM providers and, derived from the
seed catalogue, the list of models Kasal ships per provider.
"""

from enum import Enum
from typing import Dict, List


class ModelProvider(str, Enum):
    """Enum for LLM model providers."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    OLLAMA = "ollama"
    DEEPSEEK = "deepseek"
    DATABRICKS = "databricks"
    GEMINI = "gemini"
    VLLM = "vllm"
    KIMI = "kimi"
    CUSTOM = "custom"


def _supported_models() -> Dict[ModelProvider, List[str]]:
    """Group the seeded model catalogue by provider.

    This used to be a hand-maintained literal, and it drifted badly: it still
    listed gpt-4o, gpt-3.5-turbo, claude-opus-4-20250514, gemini-2.0-flash and
    deepseek-chat long after those were retired, while missing every model added
    since. Deriving it from ``DEFAULT_MODELS`` means the two lists cannot
    disagree — a model is supported exactly when it is seeded.

    A provider with no seeded models maps to an empty list rather than being
    absent, so callers can index by any ``ModelProvider`` member.
    """
    # Imported inside the function: src.seeds.model_configs pulls in the DB
    # session factory, and schemas are imported early enough that doing it at
    # module scope risks an import cycle.
    from src.seeds.model_configs import DEFAULT_MODELS

    grouped: Dict[ModelProvider, List[str]] = {
        provider: [] for provider in ModelProvider
    }
    for key, config in DEFAULT_MODELS.items():
        try:
            provider = ModelProvider(config.get("provider"))
        except ValueError:
            # A seeded model for a provider this enum does not know about; the
            # enum is the contract, so skip rather than inventing a member.
            continue
        grouped[provider].append(key)
    return grouped


# List of supported models per provider, keyed by ModelProvider.
SUPPORTED_MODELS: Dict[ModelProvider, List[str]] = _supported_models()
