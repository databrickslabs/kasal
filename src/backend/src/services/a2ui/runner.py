"""Live-app adapter for the shared A2UI composer.

The composer itself (``src.services.a2ui.compose``) is portable and stdlib-only —
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

from src.services.a2ui.compose import (
    compose_a2ui,
    guidance_for,
    infer_deliverable,
    load_catalog,
    resolve_catalog,
    resolve_directives,
    wants_rich_surface,
)
from src.services.a2ui.structured_text import render_research_envelope

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


#: Outcomes that mean composition never ran: there was no answer to render, the
#: request implied no rich surface, or A2UI is off for the workspace. They are
#: the ordinary state of a prose conversation, so a trace row for each would add
#: one to EVERY chat turn to say that nothing happened. They still reach the
#: event bus — only the trace row is withheld.
_UNATTEMPTED_OUTCOMES = frozenset({"no_text", "no_rich_intent", "disabled"})


def _emit_surface_event(
    outcome: str,
    *,
    reason: str = "",
    surface: Optional[Dict[str, Any]] = None,
    query: str = "",
    purpose: str = "",
    model: Optional[str] = None,
    started_at: Optional[float] = None,
    execution_id: Optional[str] = None,
    group_context: Any = None,
) -> None:
    """Announce what composition decided, on the bus and therefore in the trace.

    Every outcome, not just the successful one: composition is a series of quiet
    gates (A2UI off for the workspace, no rich intent in the request, a prose
    fallback, a dashboard with no data in it) and each one silently returned
    plain text. "Why did I not get a presentation?" had no answer anywhere in
    the run — no event, no span, no trace row.

    Never raises. Observability must not be able to fail a run, and this one sits
    on the answer path of every chat turn.
    """
    try:
        import time

        from src.core.events.bus import event_bus
        from src.core.events.types import A2UISurfaceEvent

        components = None
        if isinstance(surface, dict):
            declared = surface.get("components")
            if isinstance(declared, (list, tuple)):
                components = len(declared)

        event_bus.emit(
            None,
            A2UISurfaceEvent(
                outcome=outcome,
                reason=reason or None,
                surface_kind=(surface or {}).get("surfaceKind") if surface else None,
                component_count=components,
                # Truncated: these are trace labels, not payloads, and the answer
                # they describe is already recorded on its own row.
                query=(query or "")[:200] or None,
                purpose=(purpose or "")[:200] or None,
                model=model,
                duration_ms=(
                    round((time.monotonic() - started_at) * 1000, 2)
                    if started_at is not None
                    else None
                ),
            ),
        )
    except Exception as emit_err:  # noqa: BLE001
        logger.debug(f"[a2ui] surface event not emitted: {emit_err}")

    # The bus alone does not reach the trace here: the crew path composes in the
    # parent AFTER the subprocess that owns the OTel bridge has exited, and the
    # light path's writer only handles memory/LLM/tool events. Write the row
    # against the originating run directly (services.trace.writer) so it lands
    # INSIDE that run's trace instead of vanishing — or worse, being picked up by
    # whichever other run happens to have handlers on the global bus.
    if execution_id and outcome not in _UNATTEMPTED_OUTCOMES:
        try:
            import asyncio as _asyncio

            from src.services.trace.writer import write_rows

            components = None
            if isinstance(surface, dict) and isinstance(
                surface.get("components"), (list, tuple)
            ):
                components = len(surface["components"])

            metadata = {
                "outcome": outcome,
                "reason": reason or None,
                "surface_kind": (surface or {}).get("surfaceKind") if surface else None,
                "component_count": components,
                "duration_ms": (
                    round((_time_now() - started_at) * 1000, 2)
                    if started_at is not None
                    else None
                ),
                # Labels, not payloads: the answer they describe is already on
                # its own row.
                "query": (query or "")[:200] or None,
                "purpose": (purpose or "")[:200] or None,
                "model": model,
            }
            _asyncio.get_running_loop().create_task(
                write_rows(
                    execution_id,
                    [
                        (
                            "a2ui_surface",
                            "kasal.a2ui.compose",
                            reason or outcome,
                            metadata,
                        )
                    ],
                    fallback_source="A2UI",
                    fallback_context=reason or outcome,
                    group_context=group_context,
                )
            )
        except RuntimeError:
            # No running loop (a sync caller): the bus event still fired.
            logger.debug("[a2ui] no running loop; surface trace row skipped")
        except Exception as trace_err:  # noqa: BLE001
            logger.debug(f"[a2ui] surface trace not scheduled: {trace_err}")


def _time_now() -> float:
    import time

    return time.monotonic()


def _retries() -> int:
    """Composer attempts before falling back to markdown — env-tunable so weaker
    local models (e.g. a self-hosted Qwen) can be given more attempts without a
    code change."""
    try:
        return max(1, int(os.getenv("A2UI_COMPOSE_RETRIES", "2")))
    except (TypeError, ValueError):
        return 2


# Catalog/directive resolution is shared (stdlib-only) with the exported app so
# both resolve a workspace's UIConfig IDENTICALLY — see src.services.a2ui.compose.
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
        from src.services.settings.ui import UIConfigService

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


# Surface kinds a plain-prose answer degrades into (as opposed to explicitly
# requested rich kinds like presentation/quiz/mindmap). These are only worth an
# envelope when they carry real data — see `compose_surface`.
_DATA_SURFACE_KINDS = frozenset({"dashboard", "document"})

# Components that make a surface a genuine DELIVERABLE (a real graph/table/number,
# a diagram, a map, an image gallery). A dashboard/document surface with none of
# these is just prose wrapped in Text/Markdown and would render the answer twice
# (see the double-render fix). The data-viz + diagram + gallery components must be
# listed here or their surfaces get dropped back to plain text.
_DATA_COMPONENTS = frozenset(
    {
        "Chart",
        "Table",
        "Stat",
        "KeyValue",
        "Grid",
        "Forecast",
        "Graph",
        "Sequence",
        "Album",
        "Map",
        "Diagram",
        # Region shading and flow ribbons are genuine deliverables: omitted here,
        # their whole surface is dropped as "prose-only" and the answer silently
        # falls back to markdown (the Album bug in the A2UI checklist).
        "RegionHeatmap",
        "Sankey",
    }
)


def _has_data_component(surface: Dict[str, Any]) -> bool:
    """True if the surface contains at least one deliverable-bearing component.

    The composer emits a flat ``components`` array; each node's type is on
    ``component`` (with ``type`` accepted as an alias, mirroring the renderer)."""
    for comp in surface.get("components") or []:
        if not isinstance(comp, dict):
            continue
        if (comp.get("component") or comp.get("type")) in _DATA_COMPONENTS:
            return True
    return False


async def compose_surface(
    text: str,
    *,
    purpose: str = "",
    query: str = "",
    hint: str = "",
    model: Optional[str] = None,
    group_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    group_context: Any = None,
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
    import time as _time

    started_at = _time.monotonic()

    def _skip(
        outcome: str, reason: str, surface: Optional[Dict[str, Any]] = None
    ) -> None:
        _emit_surface_event(
            outcome,
            reason=reason,
            surface=surface,
            query=query,
            purpose=purpose,
            model=model,
            started_at=started_at,
            execution_id=execution_id,
            group_context=group_context,
        )

    if not (text or "").strip():
        _skip("no_text", "the answer was empty, so there was nothing to render")
        return None

    # The UIConfigurator (per workspace) is the source of truth: whether A2UI is on,
    # which component catalog the composer may use, and the per-deliverable settings.
    enabled, catalog, guidance = await _resolve_config(group_id, query)
    if not enabled or not catalog:
        _skip(
            "disabled",
            "A2UI is off for this workspace, or its component catalog is empty",
        )
        return None

    # Skip building an LLM entirely when this turn obviously won't produce a rich
    # surface — keeps plain-prose answers fast (important on a single local model)
    # and leaves the result as a plain string. The agent goal / crew purpose is
    # folded into the intent signal so a "create a presentation" deliverable fires
    # even when the user's chat prompt itself carries no rich-intent keyword.
    if not wants_rich_surface(text, f"{query}\n{purpose}"):
        _skip(
            "no_rich_intent",
            "neither the request nor the agent's purpose implies a rich surface, "
            "so the answer stays plain text",
        )
        return None

    model_name = model or os.getenv("CREW_MODEL") or "databricks-llama-4-maverick"

    try:
        from src.services.llm.manager import LLMManager

        # temperature=0 for deterministic, well-formed JSON. LLMManager injects
        # the Kasal User-Agent header automatically.
        llm = await LLMManager.get_llm(model_name, temperature=0)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            f"[a2ui] could not build composer LLM ({exc}); keeping plain text"
        )
        _skip("composer_unavailable", f"the composer LLM could not be built: {exc}")
        return None

    # The composer runs in a worker thread, so a call cannot await its own trace
    # write — capture the loop here and hand the coroutine back to it.
    try:
        _loop = asyncio.get_running_loop()
    except RuntimeError:
        _loop = None
    _attempt = {"n": 0}

    def _llm_call(messages: List[Dict[str, str]]) -> str:
        import time as _t

        began = _t.monotonic()
        out = llm.call(messages)
        text = out if isinstance(out, str) else str(out)
        _attempt["n"] += 1

        # Each composer call gets its own row: a deck costs an outline call plus
        # a surface call plus any correction pass, and they used to be a silent
        # minute at the end of a run.
        if execution_id and _loop is not None:
            try:
                from src.services.trace.writer import write_rows

                prompt = "\n\n".join(
                    f"{m.get('role', '')}: {m.get('content', '')}" for m in messages
                )
                shared = {
                    "llm_purpose": "a2ui_compose",
                    "model": model_name,
                    "attempt": _attempt["n"],
                }
                # A request row and a response row, like every other LLM call:
                # one row can only report one length, so a lone
                # "A2UI Compose (2,474 chars)" left the reader guessing whether
                # that was sent or received. Stored WHOLE — the list response
                # trims what it ships, so the size costs the browser nothing
                # until someone opens the row.
                asyncio.run_coroutine_threadsafe(
                    write_rows(
                        execution_id,
                        [
                            (
                                "llm_call",
                                "kasal.a2ui.llm_call",
                                prompt,
                                {**shared, "prompt": prompt},
                            ),
                            (
                                "llm_response",
                                "kasal.a2ui.llm_response",
                                text,
                                {
                                    **shared,
                                    "duration_ms": round(
                                        (_t.monotonic() - began) * 1000, 2
                                    ),
                                },
                            ),
                        ],
                        fallback_source="A2UI",
                        fallback_context="a2ui compose",
                        group_context=group_context,
                    ),
                    _loop,
                )
            except Exception as call_trace_err:  # noqa: BLE001
                logger.debug(f"[a2ui] composer call trace skipped: {call_trace_err}")
        return text

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
        _skip("compose_failed", f"the composer raised: {exc}")
        return None

    # The composer falls back to a markdown 'conversation' surface when it can't
    # build a rich one; treat that as "no rich surface" so the result stays a plain
    # string rather than a redundant envelope around the same prose.
    if not surface or surface.get("surfaceKind") in (None, "conversation"):
        _skip(
            "conversation_fallback",
            "the composer produced prose rather than a rich surface",
            surface=surface if isinstance(surface, dict) else None,
        )
        return None
    # A rich-intent keyword in the request (e.g. an "analytics"/"billing" Genie
    # crew) fires the composer even when THIS answer is just prose — a greeting,
    # a clarification, a Genie space overview. For the data-oriented kinds a prose
    # answer degrades into (dashboard/document), a Text-only surface renders the
    # SAME words twice: once as the chat bubble (the `text` field) and once inside
    # the surface. Require an actual data component — Chart/Table/Stat/KeyValue/Grid
    # — for those kinds so plain prose stays in the chat transcript and only genuine
    # graphs/tables become a deliverable. Explicitly-requested rich kinds
    # (presentation/quiz/mindmap/flashcards/map) are legitimately non-tabular, so
    # they are never gated on data content.
    if surface.get("surfaceKind") in _DATA_SURFACE_KINDS and not _has_data_component(
        surface
    ):
        _skip(
            "no_data_component",
            f"a {surface.get('surfaceKind')} surface carried no Chart/Table/Stat "
            "component, so it would have repeated the answer's own words",
            surface=surface,
        )
        return None

    _skip("composed", "", surface=surface)
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

    # A DECLARED structured output — a crew or task with output_pydantic. It has
    # no `.raw`, so it used to fall through to ``str()`` and reach the composer
    # as a repr: ``WebSearchResult(query='...', has_results=True)``. The composer
    # reads text, and a repr is not the JSON that ``render_research_envelope``
    # and the surface inference know how to work with — so a typed answer, the
    # most structured thing a run can produce, was the one shape that could not
    # become a surface.
    dump = getattr(result, "model_dump_json", None)
    if callable(dump):
        try:
            return dump()
        except Exception as exc:  # noqa: BLE001 — formatting must not break a run
            logger.debug(f"[a2ui] could not serialize structured result: {exc}")

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
    # ``run_name`` is what a FLOW config carries — it has no crew_name/name and
    # no `tasks` list, so without this a flow's signal was whatever happened to
    # be in `inputs`, and the composer declined to build anything.
    # ``user_message`` first: infer_deliverable is first-keyword-wins, and what
    # the person asked for outranks what the flow calls itself.
    for key in ("user_message", "crew_name", "name", "description", "run_name"):
        val = cfg.get(key)
        if isinstance(val, str) and val:
            parts.append(val)
    for task in cfg.get("tasks") or []:
        if isinstance(task, dict):
            for key in ("description", "expected_output", "name"):
                val = task.get(key)
                if isinstance(val, str) and val:
                    parts.append(val)
    parts.extend(_flow_node_text(cfg))
    for val in (inputs or {}).values():
        if isinstance(val, str) and val:
            parts.append(val)
    return "  ".join(parts)


def _flow_node_text(cfg: Dict[str, Any]) -> List[str]:
    """What a FLOW says about itself: its crew nodes and their task text.

    A flow's shape is nodes + edges, not a crew's tasks list, so the crew
    extraction above finds nothing in one. The deliverable a flow is building is
    named in its node labels and in the referenced crews\' task descriptions —
    "Create Presentation", "Mindmap" — which is exactly the signal
    ``wants_rich_surface`` needs.
    """
    parts: List[str] = []
    for node in cfg.get("nodes") or []:
        if not isinstance(node, dict):
            continue
        data = node.get("data")
        if not isinstance(data, dict):
            continue
        for key in ("label", "crewName"):
            val = data.get(key)
            if isinstance(val, str) and val:
                parts.append(val)
        for task in data.get("allTasks") or []:
            if isinstance(task, dict):
                for key in ("name", "description", "expected_output"):
                    val = task.get(key)
                    if isinstance(val, str) and val:
                        parts.append(val)
    return parts


def _crew_purpose(config: Optional[Dict[str, Any]]) -> str:
    """A short purpose string for the composer (steers surface choice)."""
    cfg = config or {}
    for key in ("crew_name", "name", "description", "run_name"):
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
    execution_id: Optional[str] = None,
    group_context: Any = None,
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
    # Deep Research's output_schema routes through output_json, which rewrites
    # `raw` to a JSON dump — so the composer was being handed a JSON document,
    # produced a prose-only `document`, and had it dropped by the data-component
    # gate. A dropped surface leaves the result a plain string, which is how raw
    # envelope JSON ended up in the chat. Render it first: the composer then sees
    # a markdown table and emits a Table, and the fallback text is readable
    # either way. See services/a2ui/structured_text.
    rendered = render_research_envelope(text)
    text = rendered or text
    try:
        surface = await compose_surface(
            text,
            purpose=_crew_purpose(config),
            query=crew_intent_text(config, inputs),
            model=(config or {}).get("model"),
            group_id=group_id,
            execution_id=execution_id,
            group_context=group_context,
        )
    except Exception as exc:  # noqa: BLE001 — never break a completed run
        logger.debug(f"[a2ui] crew surface compose skipped: {exc}")
        return result
    if surface:
        return {"text": text, "a2ui": surface}
    # No surface. Still return the rendered markdown when there was one — the
    # whole point is that a reader never sees the raw JSON, whether or not A2UI
    # produced a surface. `result` is returned untouched otherwise so a
    # non-envelope answer keeps its exact original shape.
    return rendered if rendered else result
