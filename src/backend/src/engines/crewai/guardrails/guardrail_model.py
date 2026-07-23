"""Shared resolver for the model an LLM guardrail should validate with.

An LLM guardrail judges a task's output, so it must use the SAME model the run
is using — the model the user selected in the chat input (Agent Builder or
ChatMode) or, more generally, the model the validated task's agent runs with.
That selection flows top-down: chat input -> execution ``config['model']`` ->
each agent's LLM. The guardrail therefore sources its model from the agent it
validates (falling back to the run-level config model), NOT from a per-task
``llm_model`` field that can drift from the run.

Centralizing this keeps one definition of "the guardrail's model" and one copy
of the provider-prefix handling, reused by every guardrail-build site (crew
tasks, flow tasks, and the code-based GuardrailFactory guardrails).
"""

from typing import Any, Optional

# Last-resort model, used only when neither the agent nor the run config exposes
# one (should not happen in practice — an agent is always built with a model).
DEFAULT_GUARDRAIL_MODEL = "databricks-claude-sonnet-4-5"


def _strip_provider_prefix(model: str) -> str:
    """LLMManager re-adds the provider prefix from DB config, so a guardrail
    model must be the bare name. Strip only the leading ``databricks/`` provider
    prefix — never the ``databricks-`` name prefix of a model id."""
    if model.startswith("databricks/"):
        return model[len("databricks/"):]
    return model


def _agent_model(agent: Any) -> Optional[str]:
    """Best-effort read of the model an agent's LLM runs with.

    Handles the LLM variants ``LLMManager.configure_crewai_llm`` returns: a plain
    crewai ``LLM`` (``.model``) and the ``DatabricksRetryLLM`` wrapper (``.model``
    via ``__getattr__`` delegation, or ``.model_name``).
    """
    llm = getattr(agent, "llm", None)
    if llm is None:
        return None
    model = getattr(llm, "model", None) or getattr(llm, "model_name", None)
    if isinstance(model, str) and model.strip():
        return model
    return None


def resolve_guardrail_model(
    explicit: Any = None,
    agent: Any = None,
    config: Optional[dict] = None,
) -> str:
    """Return the model an LLM guardrail should use for THIS run.

    Resolution order:
      1. ``explicit`` — a model the user deliberately picked for this task's
         guardrail (the per-task ``llm_guardrail.llm_model``). An explicit choice
         always wins, so users can pin a specific judge model when they want;
      2. the validated task's agent model (``agent.llm.model``) — the DEFAULT:
         the model the agent runs with, i.e. the chat-input selection top-down;
      3. the run-level model (``config['model']``) — covers paths where the
         agent model is not readable;
      4. ``DEFAULT_GUARDRAIL_MODEL`` — defensive last resort.

    The returned name has any ``databricks/`` provider prefix stripped, since
    LLMManager re-adds it from DB config.
    """
    if isinstance(explicit, str) and explicit.strip():
        return _strip_provider_prefix(explicit)
    candidate = _agent_model(agent)
    if not candidate and config:
        run_model = config.get("model")
        if isinstance(run_model, str) and run_model.strip():
            candidate = run_model
    if not candidate:
        candidate = DEFAULT_GUARDRAIL_MODEL
    return _strip_provider_prefix(candidate)
