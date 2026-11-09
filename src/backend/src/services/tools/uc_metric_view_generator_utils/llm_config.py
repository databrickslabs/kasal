"""LLM configuration for the generator's DAX→SQL translation (opt-in fallback)."""

from __future__ import annotations

from src.services.tools.async_bridge import workspace_llm_credentials


def build_llm_config(
    *,
    use_llm: bool,
    llm_workspace_url: str | None,
    llm_token: str | None,
    llm_model: str | None,
    translation_mode: str | None,
    domain_context: object,
    in_recon_loop: bool,
) -> dict[str, object] | None:
    """The LLM fallback config for the pipeline, or None when fallback is off."""
    if not use_llm:
        return None
    llm_url, llm_token_value = workspace_llm_credentials(llm_workspace_url, llm_token)
    llm_config: dict[str, object] = {
        "use_llm_fallback": True,
        "llm_model": llm_model or "databricks-claude-sonnet-4-5",
        "llm_workspace_url": llm_url,
        "llm_token": llm_token_value,
        # LLM-first translation (skill-corpus driven) is the default; the
        # regex patterns become a trivial fast-path. 'regex_first' restores
        # the prior regex-primary behaviour.
        "translation_mode": translation_mode or "llm_first",
    }
    # Customer-supplied domain context (free text) → the DAX→SQL translation
    # prompt, so the LLM translates with the model's business semantics.
    # Advisory and fail-open; only meaningful when LLM fallback is on, which is
    # why it rides llm_config.
    if isinstance(domain_context, str) and domain_context.strip():
        llm_config["domain_context"] = domain_context.strip()
    # Time-intelligence is translated ONLY inside the reconciliation loop (which
    # validates the numbers); otherwise the DAX-LLM declines it so no
    # unvalidated prior-year/YTD math ships.
    if in_recon_loop:
        llm_config["reconciliation_active"] = True
    return llm_config
