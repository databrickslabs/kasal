"""
Model configuration utilities.

This module provides utility functions for getting model configurations,
validating models, and retrieving default settings.
"""

import logging
import os
import re
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

# Configure logging
logger = logging.getLogger(__name__)


# The model used when a spec reaches the engine without one of its own.
#
# Single source of truth for every default: the three execution paths had
# drifted apart — light-agent and flow defaulted to databricks-llama-4-maverick
# while the crew path used a hardcoded "gpt-4o", so in a Databricks-first
# deployment an agent missing an llm silently needed an OPENAI_API_KEY that may
# not exist. The model/schema layers (models/agent.py, schemas/agent.py,
# schemas/crew.py) point here too, so a default can no longer diverge per path.
#
# Sonnet 4.6 rather than llama-4-maverick: maverick is 128k context / 8k output,
# and that 8k output cap is a real ceiling for a fallback. Sonnet 4.6 is the
# current balanced tier at 200k/64k and — unlike the gpt-5* and claude-opus-4-7/
# 4-8 families — accepts `temperature`, so it carries no request-surface quirk
# for a model that has to work without any per-agent configuration.
DEFAULT_ENGINE_MODEL = os.getenv("DEFAULT_LLM_MODEL", "databricks-claude-sonnet-4-6")


# Models whose request surface accepts a native reasoning budget:
#   - Chat Completions: `reasoning_effort: "low"|"medium"|"high"`
#   - Responses API:    `reasoning: {"effort": ...}`
# (see kasal_engine/llm/completion.py — the param is only emitted when set).
# This is a deliberately CONSERVATIVE allow-list of substrings matched against the
# resolved provider model name: sending the param to an endpoint that does not know
# it is a 400 on strict gateways, so anything not proven here is dropped silently.
# Excluded on purpose:
#   - Anthropic Claude: uses a thinking BUDGET, not `reasoning_effort`
#     ("reasoning_effort: Extra inputs are not permitted"). It is handled
#     separately and DOES work — see `_SUPPORTS_THINKING_BUDGET_RE` and
#     `_thinking_for` in core/llm/transport/completion.py, driven by the
#     `ModelConfig.extended_thinking` toggle:
#       * Claude 4.x → `thinking: {"type": "enabled", "budget_tokens": N}` in
#         extra_body, subject to `max_tokens > budget_tokens`. Returns REAL
#         thinking text (haiku-4-5 1,630 chars, sonnet-4-5 1,600 — measured
#         through this transport).
#       * Claude 5 / Fable → reject "enabled", demand `{"type": "adaptive"}`,
#         which takes no sub-keys and still returns an EMPTY summary (Bedrock
#         sends only the opaque `signature`). Nothing to enable, nothing to show.
#     An earlier version of this comment claimed Claude's thinking was
#     unavailable by ANY request. That was wrong, and wrong in a costly way: the
#     first probe set `max_tokens` (3,000) BELOW `budget_tokens` (10,240), and the
#     resulting 400 ("`max_tokens` must be greater than `thinking.budget_tokens`")
#     was misread as "unsupported" and generalised across the whole family.
#   - Kimi / self-hosted vLLM: no `reasoning_effort` param (Kimi K2.7 cannot
#     even disable thinking). Note kimi-k2-7-code DOES return thinking text —
#     unprompted, in a sibling `reasoning_content` field.
#   - DeepSeek v4 (flash/pro): DOES support reasoning effort, but NESTED —
#     `thinking: {"type": "enabled", "reasoning_effort": "high"|"max"}`, where
#     low/medium collapse to "high". Our emitter sends a TOP-LEVEL
#     `reasoning_effort`, which DeepSeek would ignore, so it stays excluded here
#     until the nested shape is emitted (same situation as Anthropic's
#     `thinking: {budget_tokens}`). Verified 2026-07-25 against
#     api-docs.deepseek.com/api/create-chat-completion.
#   - o1 / o1-preview / o1-mini: predate `reasoning_effort`.
#   - *deep-research*: fixed internal budget, rejects an explicit effort.
#
# Gemini 3.x WAS excluded here on the belief that it has no `reasoning_effort`
# param. That is wrong for the Databricks-served endpoints, and it was costing us
# most of the visible chain-of-thought available anywhere in the catalogue.
# WITHOUT the param the response carries a text-only block and no thinking; WITH
# it a populated `reasoning` block comes back ("**My Thought Process for
# Calculating 17 x 23**..."). The native Gemini `thinking` shape is rejected
# (400 Invalid JSON payload), so `reasoning_effort` is the only lever.
#
# Note that ACCEPTING the param and RETURNING the trace are different things.
# Full sweep of all 37 seeded Databricks models, 2026-08-05 — reasoning TEXT via
# `reasoning_effort` (this list) comes back from five:
#     gemini-3-1-flash-lite  2,226    inkling         309
#     gemini-3-5-flash       2,104    kimi-k2-7-code  137
#     gemini-3-1-pro         1,648
# Every gpt-5* ACCEPTS `reasoning_effort` and still returns NOTHING. It genuinely
# reasons — `usage.completion_tokens_details.reasoning_tokens` scales with the
# effort (1,344 at high, 896 at medium, 0 at minimal) — but the message carries
# only ['annotations','content','refusal','role']: no reasoning block, no
# `reasoning_content`. OpenAI's `reasoning: {"summary": ...}` lever is rejected
# here ("Unknown parameter"), so the trace is unobtainable, not merely unrequested.
# Llama, Qwen, Gemma and gemini-2-5-flash expose none. gemini-3-5-flash-lite and
# 3-6-flash accept the param (HTTP 200) but return no text, which is harmless.
#
# Claude is NOT covered by this list and is not "none": the 4.x line returns real
# thinking through the extended-thinking BUDGET instead (see the Anthropic bullet
# above), while Claude 5 / Fable return a redacted block. So this allow-list
# governs one of two mechanisms, and neither one promises visible reasoning.
_REASONING_EFFORT_SUBSTRINGS = ("gpt-5", "gpt5", "gpt-oss", "gemini-3")
# o3 / o4 families (o3, o3-mini, o4-mini, ...) do accept `reasoning_effort`.
_REASONING_EFFORT_PREFIX_RE = re.compile(r"^o[34](\b|[-_.]|$)")

VALID_REASONING_EFFORTS = ("low", "medium", "high")


def model_supports_reasoning_effort(model_name: Optional[str]) -> bool:
    """Return True when the model accepts a native reasoning-budget parameter.

    ``model_name`` may carry a provider prefix (``databricks/gpt-5-2``,
    ``openai/gpt-5.2``) — the prefix is stripped before matching.

    Overrides (both read at call time so they work in execution subprocesses):
      - ``KASAL_REASONING_EFFORT_DISABLED=true`` — kill switch, always False.
      - ``KASAL_REASONING_EFFORT_MODELS`` — comma-separated extra substrings to
        treat as supported (e.g. a workspace endpoint we cannot name here).
    """
    # Defensive isinstance check: callers pass whatever the built LLM reports as
    # its model, which is not guaranteed to be a str.
    if not model_name or not isinstance(model_name, str):
        return False
    if os.getenv("KASAL_REASONING_EFFORT_DISABLED", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return False

    m = model_name.lower()
    # Strip a provider prefix ("databricks/", "openai/", ...) if present.
    if "/" in m:
        m = m.rpartition("/")[2]

    extra = [
        s.strip().lower()
        for s in os.getenv("KASAL_REASONING_EFFORT_MODELS", "").split(",")
        if s.strip()
    ]
    if any(s in m for s in extra):
        return True

    if "deep-research" in m:
        return False
    if any(s in m for s in _REASONING_EFFORT_SUBSTRINGS):
        return True
    if _REASONING_EFFORT_PREFIX_RE.match(m):
        return True
    return False


def model_rejects_temperature(model_name: Optional[str]) -> bool:
    """
    Return True for models whose serving endpoint rejects the `temperature`
    parameter (a 400 BAD_REQUEST otherwise). Covers GPT-5 / reasoning models and
    the newest Anthropic Claude Opus models (4.7+) on Databricks, e.g.
    `databricks-claude-opus-4-8` (served as `global.anthropic.claude-opus-4-8`).
    """
    if not model_name:
        return False
    m = model_name.lower()
    if "gpt-5" in m or "gpt5" in m:
        return True
    # Opus 4.7 and NEWER. Matched by prefix rather than an exact list: the
    # enumerated form ("4-7" or "4-8") silently missed opus-5 the day it shipped,
    # and every run on it died with "Model global.anthropic.claude-opus-5 does
    # not support the temperature parameter."
    if "claude-opus-4-7" in m or "claude-opus-4-8" in m or "claude-opus-5" in m:
        return True
    if "claude-fable" in m:
        # Fable 5 has the same request surface as Opus 4.7/4.8 — sampling
        # params (temperature/top_p/top_k) return 400.
        return True
    return False


def get_model_config(model_key: str, db: Optional[Session] = None) -> Dict[str, Any]:
    """
    Get model configuration based on model key.

    Only returns configurations from the database.
    Returns None if model not found in database.

    Args:
        model_key: The model key (e.g., 'gpt-4', 'claude-3-opus')
        db: Optional database session

    Returns:
        Dictionary with model configuration or None if not found
    """
    # Only retrieve from database, no fallbacks
    if db:
        try:
            from src.models.model_config import ModelConfig

            # Query the database for the model configuration
            result = db.execute(
                select(ModelConfig).filter(ModelConfig.key == model_key)
            )
            model_config = result.scalars().first()

            if model_config:
                logger.info(f"Found model config for {model_key} in database")
                return {
                    "key": model_config.key,
                    "name": model_config.name,
                    "provider": model_config.provider,
                    "temperature": model_config.temperature,
                    "context_window": model_config.context_window,
                    "max_output_tokens": model_config.max_output_tokens,
                    "extended_thinking": model_config.extended_thinking,
                    "enabled": model_config.enabled,
                }
            logger.warning(f"Model {model_key} not found in database")
            return None
        except Exception as e:
            logger.warning(f"Error retrieving model config from database: {str(e)}")
            return None

    logger.warning(f"No database session provided to get_model_config for {model_key}")
    return None


def get_max_rpm_for_model(model_key: str) -> int:
    """
    Get the maximum requests per minute (RPM) for a given model.

    Args:
        model_key: The model key (e.g., 'gpt-4', 'claude-3-opus')

    Returns:
        Integer representing the maximum RPM
    """
    # RPM limits for various models - these are conservative defaults.
    # Refreshed 2026-07-25 alongside the model catalogue: the retired OpenAI
    # (gpt-4*/gpt-3.5*/o1), Anthropic (Claude 4.0 snapshots) and Gemini 2.x
    # entries were dropped. Unlisted models fall through to the provider
    # heuristics below, so this map only needs entries that differ from them.
    rpm_limits = {
        # OpenAI models
        "gpt-5.6-sol": 50,
        "gpt-5.6-terra": 100,
        "gpt-5.6-luna": 100,
        "o3-mini": 100,
        # Anthropic models
        "claude-opus-5": 5,  # More conservative for Opus
        "claude-sonnet-5": 10,
        "claude-haiku-4-5": 20,  # Small/fast tier
        # Ollama models are hosted locally, but still use conservative defaults
        "qwen2.5:32b": 5,
        "llama2": 10,
        "llama2:13b": 10,
        "llama3.2:latest": 5,
        "mistral": 10,
        "mixtral": 5,
        "codellama": 10,
        "mistral-nemo:12b-instruct-2407-q2_K": 5,
        "llama3.2:3b-text-q8_0": 20,  # Smaller model can handle more requests
        "gemma2:27b": 5,  # Large model, conservative limit
        "deepseek-r1:32b": 5,  # Large model, conservative limit
        "milkey/QwQ-32B-0305:q4_K_M": 5,  # Large model, conservative limit
        # DeepSeek models
        "deepseek-v4-flash": 5,
        "deepseek-v4-pro": 3,  # More conservative for the thinking-by-default model
        # Databricks models
        "databricks-meta-llama-3-3-70b-instruct": 5,
        "databricks-meta-llama-3-1-405b-instruct": 3,  # Larger model, more conservative
        # Google models
        "gemini-3.6-flash": 10,  # Standard rate limit for Gemini Flash
        "gemini-3.5-flash": 10,
        "gemini-3.5-flash-lite": 20,  # Lite tier tolerates more requests
    }

    # Return the RPM limit if it exists, otherwise return a default
    if model_key in rpm_limits:
        return rpm_limits[model_key]

    # Try to determine a sensible default based on model provider
    if "gpt-5" in model_key or "gpt5" in model_key:
        return 100
    elif "gpt-4" in model_key or "gpt4" in model_key:
        return 50
    elif "gpt-3.5" in model_key or "gpt3" in model_key:
        return 200
    elif "claude" in model_key:
        return 10  # Claude family (Opus/Sonnet/Haiku 4.x)
    elif "llama" in model_key and "3b" in model_key:
        return 20  # Smaller model
    elif "llama" in model_key or "mistral" in model_key or "mixtral" in model_key:
        return 5
    elif "deepseek" in model_key:
        return 5
    elif "databricks" in model_key:
        return 5
    elif "gemini" in model_key:
        return 10  # Default for Gemini models

    # Most conservative default for unknown models
    logger.warning(
        f"Using conservative default RPM limit for unknown model {model_key}"
    )
    return 3
