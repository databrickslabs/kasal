"""How this app builds an LLM: endpoint routing, auth, and parameter shaping.

Split out of ``agent.py`` because it is one coherent job and that file is far
over the size limit. It also puts the routing decisions next to
``databricks_llm.DatabricksLLM``, the class most of them exist to configure.

Three routes, all on Kasal's vendored transport:

  LOCAL_LLM_BASE_URL set  -> plain OpenAICompletion against that endpoint;
                             the whole app then runs with no Databricks auth.
  gpt-5-3-codex           -> DatabricksResponsesLLM (the Chat Completions route
                             404s with "Supervisor API is not enabled").
  everything else         -> DatabricksLLM.

``ENABLE_OBO`` lives here rather than in ``agent.py`` because the only thing
that reads it is ``_databricks_host_token`` below.
"""

import os

from agent_server.utils import get_user_workspace_client

# --- Runaway / hang guard -----------------------------------------------------
# Per LLM HTTP call; a hung call fails instead of hanging the turn forever.
LLM_REQUEST_TIMEOUT = int(os.environ.get("LLM_REQUEST_TIMEOUT", "300"))

# GENERATED — overwritten by Kasal on export.
# Run Databricks calls as the requesting user (on-behalf-of) when available.
ENABLE_OBO = {{ENABLE_OBO}}


def _is_codex_model(model_name: str) -> bool:
    """gpt-5-3-codex on Databricks only works via the OpenAI Responses API."""
    return bool(model_name) and "gpt-5-3-codex" in str(model_name).lower()


def _model_rejects_temperature(model_name: str) -> bool:
    """True for models whose Databricks endpoint 400s on the `temperature` param.

    Covers GPT-5 / reasoning models and the newest Anthropic models (Claude Opus
    4.7+, Fable 5) — e.g. ``databricks-claude-opus-4-8`` (served as
    ``us.anthropic.claude-opus-4-8``) raises BAD_REQUEST: "Model ... does not
    support the temperature parameter." litellm's DatabricksConfig lists
    temperature as supported, so we must drop it explicitly. Mirrors Kasal's
    src/utils/model_config.model_rejects_temperature so the exported app behaves
    like live chat.
    """
    if not model_name:
        return False
    m = str(model_name).lower()
    if "gpt-5" in m or "gpt5" in m:
        return True
    if "claude-opus-4-7" in m or "claude-opus-4-8" in m:
        return True
    if "claude-fable" in m:
        return True
    return False


def _gateway_on() -> bool:
    """Whether the workspace routes model traffic through the AI Gateway."""
    return os.environ.get("DATABRICKS_AI_GATEWAY_ENABLED", "false").lower() in (
        "1",
        "true",
        "yes",
    )


def _databricks_host_token() -> tuple:
    """Resolve the workspace host + a bearer token (OBO user token, else app SP).

    Uses ``config.authenticate()`` so the token is valid for any auth type (OBO,
    app service-principal OAuth, or PAT).
    """
    from databricks.sdk import WorkspaceClient

    w = get_user_workspace_client() if ENABLE_OBO else WorkspaceClient()
    host = (
        getattr(w.config, "host", None) or os.environ.get("DATABRICKS_HOST", "")
    ).rstrip("/")
    try:
        token = (
            (w.config.authenticate() or {}).get("Authorization", "").split(" ", 1)[-1]
        )
    except Exception:  # noqa: BLE001
        token = os.environ.get("DATABRICKS_TOKEN", "")
    return host, token


def _make_llm(model_name: str, temperature: float = 0.7):
    """Build the LLM for an agent.

    Every path here runs on Kasal's vendored transport — the same OpenAI-SDK
    client Kasal itself drives endpoints with. There is no LiteLLM in the loop
    any more: LiteLLM's Databricks provider was only ever reached through
    CrewAI's fallback, and it brought its own parameter quirks (its
    ``DatabricksConfig`` advertises ``temperature`` support that newer frontier
    models reject, which is why ``additional_drop_params`` used to be set here).

    Databricks models are called as ``databricks/<endpoint>`` with an EXPLICIT
    ``base_url`` + ``api_key``, so the app authenticates with its own identity
    (OBO/SP) rather than hoping the runtime exports Databricks env vars — it
    does not. ``DatabricksLLM`` adds the endpoint policy Kasal's
    ``DatabricksRetryLLM`` provides: message sanitization and retry/backoff.

    gpt-5-3-codex is the exception — the Chat Completions route returns 404
    "Supervisor API is not enabled", so it uses the Databricks Responses API
    via ``DatabricksResponsesLLM``.

    Local/self-hosted serving: when LOCAL_LLM_BASE_URL is set (an OpenAI-compatible
    endpoint, e.g. a vLLM server), EVERY model routes there instead of Databricks,
    so the whole app — crew + conversation + A2UI composer — runs with no Databricks
    auth. The crew's configured model names (e.g. ``databricks-gpt-5-3-codex``) won't
    exist on a local server, so set LOCAL_LLM_MODEL to the one model that server
    actually serves and every call uses it. No-op when LOCAL_LLM_BASE_URL is unset.
    """
    local_base = os.environ.get("LOCAL_LLM_BASE_URL")
    if local_base:
        from agent_server.kasal_runtime.core.llm.transport import OpenAICompletion

        # Prefer an explicit local model name; otherwise fall back to the
        # configured name (stripping any "provider/" prefix).
        endpoint = os.environ.get("LOCAL_LLM_MODEL") or (
            model_name.split("/", 1)[1] if "/" in str(model_name) else model_name
        )
        # Some hosted models pin the sampling temperature (e.g. Kimi K2 only
        # accepts 1); LOCAL_LLM_TEMPERATURE overrides the caller's value when set.
        temp_override = os.environ.get("LOCAL_LLM_TEMPERATURE")
        return OpenAICompletion(
            model=endpoint,
            base_url=local_base,
            api_key=os.environ.get("LOCAL_LLM_API_KEY", "dummy"),
            temperature=float(temp_override) if temp_override else temperature,
            timeout=LLM_REQUEST_TIMEOUT,
        )
    host, token = _databricks_host_token()
    if _is_codex_model(model_name):
        # gpt-5-3-codex ONLY works via the Databricks Responses API, and plain
        # OpenAICompletion(api="responses") does NOT complete the tool-execution
        # loop — it emits a tool call and stops (the raw tool-call is returned
        # instead of the answer). DatabricksResponsesLLM (vendored verbatim from
        # Kasal's llm_manager) adds the two things that make tool-calling work:
        #   • phase preservation — re-injects prior output items WITH their `phase`
        #     field so codex doesn't early-stop after the first tool call;
        #   • a forced tool loop (tool_choice="required" until enough tool calls).
        from agent_server.databricks_responses_llm import DatabricksResponsesLLM

        # Responses API: AI Gateway on -> /ai-gateway/openai/v1 ; off -> /serving-endpoints.
        base_path = "ai-gateway/openai/v1" if _gateway_on() else "serving-endpoints"
        return DatabricksResponsesLLM(
            model=model_name,
            api="responses",
            base_url=f"{host}/{base_path}",
            api_key=token,
            timeout=max(LLM_REQUEST_TIMEOUT, 300),
        )
    from agent_server.databricks_llm import DatabricksLLM

    endpoint = (
        model_name.split("/", 1)[1]
        if str(model_name).startswith("databricks/")
        else model_name
    )
    # The OpenAI SDK appends /chat/completions to base_url:
    # AI Gateway on -> /ai-gateway/mlflow/v1 ; off -> /serving-endpoints.
    kwargs = {
        "model": f"databricks/{endpoint}",
        "timeout": LLM_REQUEST_TIMEOUT,
    }
    # Newer frontier models (GPT-5, Claude Opus 4.7+, Fable 5) 400 on `temperature`.
    # OMITTING it is the whole fix now — the transport sends exactly the params it
    # is given, so there is no drop-params safety net to configure (and nothing to
    # re-add it behind our back, which is what `additional_drop_params` existed to
    # stop LiteLLM doing).
    if not _model_rejects_temperature(endpoint):
        kwargs["temperature"] = temperature
    if host:
        kwargs["base_url"] = (
            f"{host}/ai-gateway/mlflow/v1" if _gateway_on() else f"{host}/serving-endpoints"
        )
    if token:
        kwargs["api_key"] = token
    # Databricks Apps rotate credentials; a long run can outlive the token it
    # started with, so give the LLM a way to fetch a fresh one on a 401.
    kwargs["token_provider"] = lambda: _databricks_host_token()[1]
    return DatabricksLLM(**kwargs)
