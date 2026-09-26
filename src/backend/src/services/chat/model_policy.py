"""Intent classification model policy, including installation resource defaults."""

from src.core.databricks_app import DatabricksAppInstallation

# Fast-model fallback chain for intent detection. Intent is a 6-way
# classification emitting fixed JSON — it wants small, fast, reliable instruct
# models, not a reasoning model. detect_intent tries the caller's preferred model
# first (the model picked in Agent Builder, else this chain's first entry), then walks the
# rest so a single gated or erroring endpoint can't drop intent to the dumb
# semantic fallback. Spread across providers (Anthropic / OpenAI / Google) to
# avoid a correlated outage. Inside Databricks Apps the chain is just the
# installed model (the one endpoint the app is granted); the old
# DISPATCHER_FALLBACK_MODELS / DEFAULT_DISPATCHER_MODEL env overrides are gone.
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
DISPATCHER_FALLBACK_MODELS = list(DEFAULT_DISPATCHER_FALLBACK_MODELS)
# First chain entry doubles as the default when no model is selected in chat.
DEFAULT_DISPATCHER_MODEL = DISPATCHER_FALLBACK_MODELS[0]
