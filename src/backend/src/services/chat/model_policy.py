"""Intent classification model policy, including installation resource defaults."""

import os

from src.core.databricks_app import DatabricksAppInstallation

# Fast-model fallback chain for intent detection. Intent is a 6-way
# classification emitting fixed JSON — it wants small, fast, reliable instruct
# models, not a reasoning model. detect_intent tries the caller's preferred model
# first (the model picked in Agent Builder, else this chain's first entry), then walks the
# rest so a single gated or erroring endpoint can't drop intent to the dumb
# semantic fallback. Spread across providers (Anthropic / OpenAI / Google) to
# avoid a correlated outage. Override via env (comma-separated), e.g.
#   DISPATCHER_FALLBACK_MODELS="databricks-claude-haiku-4-5,databricks-gpt-5-nano"
_installed_model = DatabricksAppInstallation.from_env().default_model
DEFAULT_DISPATCHER_FALLBACK_MODELS = (
    [_installed_model]
    if _installed_model
    else [
        "databricks-claude-haiku-4-5",
        "databricks-gpt-5-nano",
        "databricks-gemini-3-5-flash",
    ]
)
DISPATCHER_FALLBACK_MODELS = [
    m.strip()
    for m in os.getenv(
        "DISPATCHER_FALLBACK_MODELS", ",".join(DEFAULT_DISPATCHER_FALLBACK_MODELS)
    ).split(",")
    if m.strip()
]
# First chain entry doubles as the default when no model is selected in chat.
DEFAULT_DISPATCHER_MODEL = _installed_model or os.getenv(
    "DEFAULT_DISPATCHER_MODEL",
    (
        DISPATCHER_FALLBACK_MODELS[0]
        if DISPATCHER_FALLBACK_MODELS
        else "databricks-claude-haiku-4-5"
    ),
)
