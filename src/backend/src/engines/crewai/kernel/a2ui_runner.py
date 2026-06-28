"""Live-app adapter for the shared A2UI composer.

The composer itself (``src.shared.a2ui.compose``) is portable and stdlib-only —
it takes an injected ``llm_call``. This thin adapter is the Kasal-side wiring:
it builds ``llm_call`` from Kasal's ``LLMManager`` (which already injects the
Kasal User-Agent telemetry header) and runs the blocking composer off the event
loop via ``asyncio.to_thread``. The exported app has its own equivalent wiring in
``agent_server/agent.py`` — same composer, different ``llm_call``.

Used post-answer by the execution runners (light + crew paths) to turn an
agent's text answer into a renderable A2UI surface. Never raises; returns a
surface dict, or ``None`` when A2UI is disabled or there is no text to render.
"""

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from src.shared.a2ui.compose import (
    compose_a2ui,
    guidance_for,
    infer_deliverable,
    load_catalog,
    resolve_catalog,
    resolve_directives,
    wants_rich_surface,
)

logger = logging.getLogger(__name__)

# The catalog is the same for every run; load it once per process.
_CATALOG: Optional[Dict[str, Any]] = None


def _catalog() -> Dict[str, Any]:
    global _CATALOG
    if _CATALOG is None:
        _CATALOG = load_catalog()
    return _CATALOG


def a2ui_enabled() -> bool:
    """Master switch (env-gated, default on)."""
    return os.getenv("A2UI_ENABLED", "true").lower() in ("1", "true", "yes")


def _retries() -> int:
    """Composer attempts before falling back to markdown — env-tunable so weaker
    local models (e.g. a self-hosted Qwen) can be given more attempts without a
    code change."""
    try:
        return max(1, int(os.getenv("A2UI_COMPOSE_RETRIES", "2")))
    except (TypeError, ValueError):
        return 2


# Catalog/directive resolution is shared (stdlib-only) with the exported app so
# both resolve a workspace's UIConfig IDENTICALLY — see src.shared.a2ui.compose.
# These thin adapters turn Kasal's pydantic UIConfigResponse into the plain dict
# view the shared resolvers expect, preserving the live runner's call surface.


def _cfg_dict(cfg: Any) -> Dict[str, Any]:
    """A plain dict view of a pydantic UIConfigResponse for the shared resolvers."""
    return {
        "id": getattr(cfg, "id", None),
        "catalog_type": getattr(cfg, "catalog_type", None),
        "catalog_json": getattr(cfg, "catalog_json", None),
        "style_json": getattr(cfg, "style_json", None),
    }


def _infer_deliverable(query: str) -> Optional[str]:
    """Best-effort deliverable key from the user's request (first keyword wins)."""
    return infer_deliverable(query)


def _resolve_catalog(cfg: Any, default_catalog: Dict[str, Any]) -> Dict[str, Any]:
    """Pick the catalog the composer may use from the workspace UI config.

    Unconfigured workspaces (no saved row → ``cfg.id is None``) get the FULL
    bundled catalog so rich surfaces keep working out of the box — the schema's
    ``catalog_type`` default of "minimal" applies only once an admin saves a
    choice. (Delegates to the shared, export-shared resolver.)"""
    return resolve_catalog(_cfg_dict(cfg), default_catalog)


def _resolve_guidance(cfg: Any, query: str) -> str:
    """The per-deliverable directive sentence to inject for this turn (or "").

    The configurator persists style_json.directives keyed by deliverable; the
    shared resolver infers the deliverable from the request and injects ONLY that
    one so the prompt isn't bloated with every type's settings."""
    return guidance_for(resolve_directives(_cfg_dict(cfg)), query)


async def _resolve_config(
    group_id: Optional[str], query: str
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Resolve (enabled, catalog, guidance) for this workspace.

    The UIConfigurator is the source of truth; the env flag + bundled catalog are
    only the fallback when there's no group or the lookup fails (UI formatting
    must never break a run)."""
    enabled = a2ui_enabled()
    catalog = _catalog()
    guidance = ""
    if not group_id:
        return enabled, catalog, guidance
    try:
        from src.db.session import request_scoped_session
        from src.services.ui_config_service import UIConfigService

        async with request_scoped_session() as session:
            cfg = await UIConfigService(session, group_id=group_id).get_config()
        enabled = bool(cfg.enabled)
        catalog = _resolve_catalog(cfg, catalog)
        guidance = _resolve_guidance(cfg, query)
    except Exception as exc:  # noqa: BLE001 — fall back to env + bundled catalog
        logger.warning(
            f"[a2ui] workspace UI config lookup failed ({exc}); using defaults"
        )
    return enabled, catalog, guidance


async def compose_surface(
    text: str,
    *,
    purpose: str = "",
    query: str = "",
    hint: str = "",
    model: Optional[str] = None,
    group_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Compose an A2UI surface from ``text`` for the live app.

    Args:
        text: the agent's final answer.
        purpose: agent/crew purpose (steers surface choice).
        query: the user's request this turn (the primary surfaceKind signal).
        hint: default surfaceKind hint (used only if the request implies none).
        model: model name to compose with (defaults to CREW_MODEL env or a
            sensible fallback). Resolved through ``LLMManager`` like any other call.
        group_id: the workspace whose UIConfigurator drives enabled + catalog +
            per-deliverable directives (the source of truth). When omitted, falls
            back to the env flag + bundled catalog.

    Returns:
        A surface dict, or ``None`` if A2UI is disabled / there is nothing to render.
    """
    # Returns None for every "no rich surface" path so the caller keeps the result
    # a PLAIN STRING (full back-compat) — the envelope is used ONLY when a genuine
    # rich surface (presentation/dashboard/mindmap/quiz/…) is produced.
    if not (text or "").strip():
        return None

    # The UIConfigurator (per workspace) is the source of truth: whether A2UI is on,
    # which component catalog the composer may use, and the per-deliverable settings.
    enabled, catalog, guidance = await _resolve_config(group_id, query)
    if not enabled or not catalog:
        return None

    # Skip building an LLM entirely when this turn obviously won't produce a rich
    # surface — keeps plain-prose answers fast (important on a single local model)
    # and leaves the result as a plain string. The agent goal / crew purpose is
    # folded into the intent signal so a "create a presentation" deliverable fires
    # even when the user's chat prompt itself carries no rich-intent keyword.
    if not wants_rich_surface(text, f"{query}\n{purpose}"):
        return None

    model_name = model or os.getenv("CREW_MODEL") or "databricks-llama-4-maverick"

    try:
        from src.core.llm_manager import LLMManager

        # temperature=0 for deterministic, well-formed JSON. LLMManager injects
        # the Kasal User-Agent header automatically.
        llm = await LLMManager.get_llm(model_name, temperature=0)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            f"[a2ui] could not build composer LLM ({exc}); keeping plain text"
        )
        return None

    def _llm_call(messages: List[Dict[str, str]]) -> str:
        out = llm.call(messages)
        return out if isinstance(out, str) else str(out)

    try:
        surface = await asyncio.to_thread(
            compose_a2ui,
            text,
            purpose,
            hint,
            query,
            llm_call=_llm_call,
            catalog=catalog,
            enabled=True,
            retries=_retries(),
            guidance=guidance,
        )
    except Exception as exc:  # noqa: BLE001 — UI composition must never break a run
        logger.warning(f"[a2ui] compose_surface failed ({exc}); keeping plain text")
        return None

    # The composer falls back to a markdown 'conversation' surface when it can't
    # build a rich one; treat that as "no rich surface" so the result stays a plain
    # string rather than a redundant envelope around the same prose.
    if not surface or surface.get("surfaceKind") in (None, "conversation"):
        return None
    return surface


# --- Crew-path composition -------------------------------------------------
# The light-agent path composes its surface inline in execution_runner (it has the
# answer string + the chat query right there). The CREW path needs a little glue:
# a completed crew result may be a CrewOutput/dict rather than a string, and a crew
# has task definitions instead of a chat query — so we derive the rich-surface
# intent the same way the retired ``ui_emission`` did. These helpers keep both crew
# runners (threaded + process) DRY and identical to the light-agent behavior.


def _result_text(result: Any) -> str:
    """Best-effort plain text from a completed crew result.

    The crew runners persist whatever the executor returns — a plain string, a
    CrewAI ``CrewOutput`` (``.raw``), or a ``{result|text|raw|output}`` dict. The
    composer wants the answer text, the same way the light path feeds it ``answer``.
    """
    if result is None:
        return ""
    if isinstance(result, str):
        return result
    for attr in ("raw", "output"):
        val = getattr(result, attr, None)
        if isinstance(val, str) and val.strip():
            return val
    if isinstance(result, dict):
        for key in ("text", "result", "raw", "output"):
            val = result.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return str(result)


def crew_intent_text(
    config: Optional[Dict[str, Any]], inputs: Optional[Dict[str, Any]] = None
) -> str:
    """Synthesize the rich-surface intent signal for the CREW path.

    ``compose_surface`` keys its "is a rich surface worth composing?" decision off
    the user's request (``query``). A crew has task definitions instead of a chat
    turn, so — exactly like the retired ``ui_emission`` did — we derive the signal
    from the crew/task text (and any user inputs). This drives both
    ``wants_rich_surface`` and per-deliverable guidance/theme inference.
    """
    parts: List[str] = []
    cfg = config or {}
    for key in ("crew_name", "name", "description"):
        val = cfg.get(key)
        if isinstance(val, str) and val:
            parts.append(val)
    for task in cfg.get("tasks") or []:
        if isinstance(task, dict):
            for key in ("description", "expected_output", "name"):
                val = task.get(key)
                if isinstance(val, str) and val:
                    parts.append(val)
    for val in (inputs or {}).values():
        if isinstance(val, str) and val:
            parts.append(val)
    return "  ".join(parts)


def _crew_purpose(config: Optional[Dict[str, Any]]) -> str:
    """A short purpose string for the composer (steers surface choice)."""
    cfg = config or {}
    for key in ("crew_name", "name", "description"):
        val = cfg.get(key)
        if isinstance(val, str) and val:
            return val
    return ""


async def wrap_result_with_surface(
    result: Any,
    *,
    config: Optional[Dict[str, Any]] = None,
    group_id: Optional[str] = None,
    inputs: Optional[Dict[str, Any]] = None,
) -> Any:
    """Compose an A2UI surface for a COMPLETED crew result.

    Returns a ``{"text", "a2ui"}`` envelope when a rich surface is produced;
    otherwise returns ``result`` unchanged. This is the crew-path counterpart to
    the light-agent composition in ``execution_runner`` — so chat, Crew mode, API
    and schedules all render through the SAME composer + renderer, replacing the
    retired ``ui_emission`` prompt injection. Gated by the workspace UIConfigurator
    (off → unchanged) and surface-worthiness; never raises (UI formatting must not
    break a finished run).
    """
    text = _result_text(result)
    if not text.strip():
        return result
    try:
        surface = await compose_surface(
            text,
            purpose=_crew_purpose(config),
            query=crew_intent_text(config, inputs),
            model=(config or {}).get("model"),
            group_id=group_id,
        )
    except Exception as exc:  # noqa: BLE001 — never break a completed run
        logger.debug(f"[a2ui] crew surface compose skipped: {exc}")
        return result
    if surface:
        return {"text": text, "a2ui": surface}
    return result
