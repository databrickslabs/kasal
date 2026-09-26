"""Where a model's API lives.

Configuration → Models is the source: each model may carry its own endpoint in
``params["api_base"]``. Without one, a hosted provider uses its public API, and
a self-hosted one (vLLM, Ollama, the custom OpenAI-compatible server) uses its
usual local port — but only outside Databricks Apps, where "localhost" is the
app container itself and can never be a model server.

This replaces the VLLM_BASE_URL / KAT_BASE_URL / OLLAMA_API_BASE /
ANTHROPIC_API_BASE / GEMINI_API_BASE / DEEPSEEK_ENDPOINT / KIMI_ENDPOINT env
vars, which a Databricks App never sets.
"""

from typing import Any, Dict, Mapping, Optional

from src.core.databricks_app import on_databricks_apps

#: Public APIs of the hosted providers (unchanged from the former env defaults).
HOSTED_DEFAULTS = {
    "anthropic": "https://api.anthropic.com",
    "deepseek": "https://api.deepseek.com",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
    "kimi": "https://api.moonshot.ai/v1",
}

#: Usual local ports of the self-hosted servers — local development only.
#: ``params`` keys an administrator sets on a model in Configuration → Models
#: (the endpoint and its tool options). The model seeder must never overwrite
#: them: it used to replace ``params`` wholesale on every startup, erasing a
#: saved endpoint each time the server restarted.
ADMIN_PARAM_KEYS = ("api_base", "supports_tools", "tool_choice", "output_token_cap")


def merge_seed_params(
    seed: Optional[Mapping[str, Any]], existing: Optional[Mapping[str, Any]]
) -> Optional[Dict[str, Any]]:
    """The seed's params with the administrator's :data:`ADMIN_PARAM_KEYS` kept."""
    merged: Dict[str, Any] = dict(seed or {})
    if isinstance(existing, Mapping):
        for key in ADMIN_PARAM_KEYS:
            if key in existing:
                merged[key] = existing[key]
    return merged or None


SELF_HOSTED_DEFAULTS = {
    "vllm": "http://localhost:8081/v1",
    "ollama": "http://localhost:11434",
    "custom": "http://127.0.0.1:8082/v1",
}


def configured_api_base(params: Optional[Mapping[str, Any]]) -> Optional[str]:
    """The endpoint an admin set on the model in Configuration → Models."""
    if not isinstance(params, Mapping):
        return None
    value = params.get("api_base")
    return (
        value.strip().rstrip("/") if isinstance(value, str) and value.strip() else None
    )


def model_api_base(provider: str, params: Optional[Mapping[str, Any]]) -> Optional[str]:
    """The endpoint to call for a model, or None when it has none.

    None means: a self-hosted model inside Databricks Apps with no endpoint set.
    The caller reports that instead of calling the app's own container.
    """
    configured = configured_api_base(params)
    if configured:
        return configured
    provider = (provider or "").lower()
    if provider in HOSTED_DEFAULTS:
        return HOSTED_DEFAULTS[provider]
    if provider in SELF_HOSTED_DEFAULTS and not on_databricks_apps():
        return SELF_HOSTED_DEFAULTS[provider]
    return None


def require_api_base(
    provider: str, params: Optional[Mapping[str, Any]], model: str
) -> str:
    """:func:`model_api_base`, raising a clear error when there is none."""
    api_base = model_api_base(provider, params)
    if not api_base:
        raise ValueError(
            f"Model '{model}' ({provider}) has no endpoint. Set its endpoint URL in "
            f"Configuration → Models; inside Databricks Apps a self-hosted model "
            f"cannot default to localhost."
        )
    return api_base


#: The local embedding model used when Databricks embeddings are unavailable
#: (was OLLAMA_EMBED_MODEL / KNOWLEDGE_OLLAMA_EMBED_MODEL).
DEFAULT_OLLAMA_EMBED_MODEL = "nomic-embed-text"


def ollama_base_url() -> Optional[str]:
    """The Ollama server for the local embedding fallback, or None inside Apps."""
    return model_api_base("ollama", None)


def require_ollama_base_url() -> str:
    """:func:`ollama_base_url`, raising a clear error inside Databricks Apps."""
    url = ollama_base_url()
    if not url:
        raise ValueError("Ollama embeddings are unavailable inside Databricks Apps")
    return url
