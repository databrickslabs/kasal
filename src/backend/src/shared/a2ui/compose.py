"""A2UI generative-UI composer — the single, portable implementation shared by
the live Kasal app and every exported Databricks app.

It turns an agent's plain-text answer into ONE declarative A2UI *surface*
(``{surfaceKind, root, components[], dataModel}``) that the shared frontend
renderer draws as rich UI (presentation / dashboard / mindmap / quiz / document /
conversation).

Design constraints (do NOT break — they keep this bundleable into a
self-contained export):
  * Import ONLY the stdlib (json / os / pathlib / typing). No ``src.*``, no
    framework imports, no network.
  * The LLM is ALWAYS injected by the caller as ``llm_call(messages) -> str``.
    The live app wraps Kasal's ``LLMManager``; the exported app wraps its own
    ``_make_llm``. This module never knows which.
  * ``compose_a2ui`` NEVER raises — it always returns a valid surface, falling
    back to a markdown surface for plain prose or on any error.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Caller-injected LLM: takes a list of {"role","content"} messages, returns text.
LLMCall = Callable[[List[Dict[str, str]]], str]

_DEFAULT_CATALOG_PATH = Path(__file__).parent / "catalog.json"


def load_catalog(path: Optional[str] = None) -> Dict[str, Any]:
    """Load the component catalog (the one contract). Returns {} if unavailable
    so callers degrade to markdown surfaces instead of crashing."""
    try:
        p = Path(path) if path else _DEFAULT_CATALOG_PATH
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"A2UI catalog unavailable ({exc}); markdown surfaces only.")
        return {}


def markdown_surface(text: str) -> Dict[str, Any]:
    """The always-valid fallback / cheap conversational surface."""
    return {
        "surfaceKind": "conversation",
        "root": "r",
        "components": [
            {"id": "r", "component": "Markdown", "content": {"path": "/md"}}
        ],
        "dataModel": {"md": text or ""},
    }


# Essentials for the "minimal" catalog preset: structure + prose only. A surface
# needing an excluded component (Chart/SlideDeck/Quiz/…) simply won't validate,
# so the composer falls back to markdown — i.e. "minimal" = document/conversation
# surfaces, no rich decks/dashboards/quizzes.
MINIMAL_COMPONENTS = (
    "Markdown",
    "Text",
    "Heading",
    "List",
    "Table",
    "Divider",
    "Row",
    "Column",
    "Card",
    "Image",
)


def subset_catalog(catalog: Dict[str, Any], names) -> Dict[str, Any]:
    """Return a shallow copy of ``catalog`` whose ``components`` are limited to the
    intersection of ``names`` and what the catalog defines. surfaceKinds are kept
    verbatim. Used to realize the admin's 'minimal' catalog choice from the full
    bundled catalog without maintaining a second file."""
    allowed = set(names)
    comps = {k: v for k, v in (catalog.get("components") or {}).items() if k in allowed}
    out = dict(catalog)
    out["components"] = comps
    return out


# ── Workspace UI-config resolution (shared by the live runner AND the exporter) ──
# These turn a workspace's stored UIConfig (a plain dict: catalog_type +
# catalog_json + style_json, plus the row ``id``) into the catalog the composer
# may use and the per-deliverable directive for a turn. Kept here — stdlib-only —
# so the live app and every exported app resolve config IDENTICALLY (one source of
# truth). The live adapter passes a dict view of its pydantic config; the exporter
# resolves at export time and bakes the result into the export.

# Keyword → deliverable key, ordered by specificity (first match wins). Mirrors
# the UIConfigurator's deliverable types (frontend ``uiConfigShared.ts``).
DELIVERABLE_KEYWORDS = [
    ("flashcard", "flashcards"),
    ("flash card", "flashcards"),
    ("anki", "flashcards"),
    ("quiz", "quiz"),
    ("assessment", "quiz"),
    ("mindmap", "mindmap"),
    ("mind map", "mindmap"),
    ("concept map", "mindmap"),
    ("album", "album"),
    ("gallery", "album"),
    ("presentation", "presentation"),
    ("slide", "presentation"),
    ("deck", "presentation"),
    ("dashboard", "dashboard"),
    ("kpi", "dashboard"),
    ("metric", "dashboard"),
    ("genie", "genie"),
    ("report", "report"),
    ("briefing", "report"),
]


def infer_deliverable(query: str) -> Optional[str]:
    """Best-effort deliverable key from the user's request (first keyword wins)."""
    if not query:
        return None
    lowered = query.lower()
    for keyword, deliverable in DELIVERABLE_KEYWORDS:
        if keyword in lowered:
            return deliverable
    return None


def resolve_directives(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """The per-deliverable directives map from a UIConfig dict's ``style_json``.

    Returns {} when there is no style_json or it carries no directives.
    ``style_json`` may be a JSON string (as stored) or an already-parsed dict."""
    raw = (cfg or {}).get("style_json")
    if not raw:
        return {}
    try:
        style = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return {}
    directives = style.get("directives") if isinstance(style, dict) else None
    return directives if isinstance(directives, dict) else {}


def resolve_themes(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """The per-deliverable theme palettes from a UIConfig dict's ``style_json``.

    Returns {} when there is no style_json or it carries no themes. Mirrors
    ``resolve_directives`` (one shared style_json parser) so the live app and the
    export resolve workspace themes identically. The map is
    ``{deliverableKey: palette}`` where a palette has accent/background/surface/
    text/heading/muted (+ optional font/density) — structurally the renderer's
    ``Palette``. Theming is applied entirely on the frontend, so this is consumed
    by the export's UI (baked into App.tsx), not by the backend composer."""
    raw = (cfg or {}).get("style_json")
    if not raw:
        return {}
    try:
        style = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return {}
    themes = style.get("themes") if isinstance(style, dict) else None
    return themes if isinstance(themes, dict) else {}


def guidance_for(directives: Dict[str, Any], query: str) -> str:
    """Pick the directive sentence to inject THIS turn from a directives map.

    A turn builds one deliverable, so we send only its settings: infer the
    deliverable from the request, else fall back to a 'default' directive, else
    "" (no guidance). Keeps the prompt from bloating with every type's settings."""
    if not isinstance(directives, dict):
        return ""
    deliverable = infer_deliverable(query)
    if (
        deliverable
        and isinstance(directives.get(deliverable), str)
        and directives[deliverable].strip()
    ):
        return directives[deliverable].strip()
    default = directives.get("default")
    return default.strip() if isinstance(default, str) and default.strip() else ""


def resolve_catalog(
    cfg: Dict[str, Any], default_catalog: Dict[str, Any]
) -> Dict[str, Any]:
    """Pick the catalog the composer may use from a workspace UIConfig dict.

    Unconfigured workspaces (no saved row → ``id`` is None) get the FULL bundled
    catalog so rich surfaces keep working out of the box. Admin choices: minimal →
    essentials subset; custom → the admin's catalog_json (surfaceKinds backfilled);
    full (or any legacy/unknown value like "basic") → the full bundled catalog."""
    cfg = cfg or {}
    if cfg.get("id") is None:
        return default_catalog
    ctype = (cfg.get("catalog_type") or "full").lower()
    if ctype == "custom":
        raw = (cfg.get("catalog_json") or "").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and parsed.get("components"):
                    if not parsed.get("surfaceKinds"):
                        parsed["surfaceKinds"] = default_catalog.get("surfaceKinds", [])
                    return parsed
            except (ValueError, TypeError):
                pass
        return default_catalog
    if ctype == "minimal":
        return subset_catalog(default_catalog, MINIMAL_COMPONENTS)
    return default_catalog  # "basic" and anything unknown → full bundled catalog


def extract_json(raw: str) -> Optional[Dict[str, Any]]:
    """Tolerant parse: strip ``` fences, scan for the first balanced {...} block."""
    if not raw:
        return None
    s = raw.strip()
    if s.startswith("```"):
        # ```json\n{...}\n```  ->  {...}
        s = s.split("```", 2)[1] if s.count("```") >= 2 else s.strip("`")
        if s.lower().startswith("json"):
            s = s[4:]
    start = s.find("{")
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(s[start : i + 1])
                except Exception:  # noqa: BLE001
                    return None
    return None


def validate_surface(payload: Any, catalog: Dict[str, Any]) -> bool:
    """A surface is valid if every component is in the catalog and root resolves."""
    if not isinstance(payload, dict):
        return False
    comps = payload.get("components")
    if not isinstance(comps, list) or not comps:
        return False
    allowed = set((catalog.get("components") or {}).keys())
    ids = set()
    for c in comps:
        if (
            not isinstance(c, dict)
            or "id" not in c
            or c.get("component") not in allowed
        ):
            return False
        ids.add(c["id"])
    return payload.get("root") in ids


def a2ui_system_prompt(
    catalog: Dict[str, Any],
    purpose: str,
    hint: str,
    query: str = "",
    guidance: str = "",
) -> str:
    comp_lines = []
    for name, spec in (catalog.get("components") or {}).items():
        props = list((spec.get("props") or {}).keys())
        comp_lines.append(f"- {name}: {spec.get('summary', '')} props={props}")
    kinds = catalog.get("surfaceKinds", [])
    example = json.dumps(
        {
            "surfaceKind": "dashboard",
            "root": "root",
            "components": [
                {
                    "id": "root",
                    "component": "Grid",
                    "columns": 2,
                    "children": ["k1", "c1"],
                },
                {
                    "id": "k1",
                    "component": "KeyValue",
                    "label": "Revenue",
                    "value": "$1.2M",
                },
                {
                    "id": "c1",
                    "component": "Chart",
                    "chartType": "bar",
                    "xKey": "month",
                    "yKeys": ["sales"],
                    "data": {"path": "/series"},
                },
            ],
            "dataModel": {
                "series": [{"month": "Jan", "sales": 10}, {"month": "Feb", "sales": 14}]
            },
        }
    )
    return (
        "You convert an AI agent's final answer into ONE A2UI surface, returned as JSON.\n"
        f"Allowed surfaceKind values: {kinds}.\n"
        "Allowed components (use ONLY these names):\n" + "\n".join(comp_lines) + "\n\n"
        "Rules:\n"
        "1. Output ONE JSON object only — no prose, no markdown code fences.\n"
        '2. Shape: {"surfaceKind","root","components":[{"id","component",...props,"children"?}],"dataModel"}.\n'
        "3. components is a FLAT list; nest by listing child ids in a parent's children. root is a component id.\n"
        '4. Put long text / arrays in dataModel and reference them with {"path":"/key"} (JSON pointer).\n'
        "5. Choose surfaceKind from the USER'S REQUEST first: if they ask for a "
        "presentation/slides/deck use 'presentation' with a SlideDeck of Slides; for a "
        "dashboard/metrics/charts use 'dashboard' with Grid+Chart/KeyValue/Table; for a "
        "mind map use 'mindmap'; for a quiz/assessment/test use 'quiz' with ONE Quiz "
        "component; otherwise use 'document' with Markdown.\n"
        "6. For presentations build a REAL deck of Slides, each with a 'variant': start "
        "with variant='title' (a short UPPERCASE 'kicker', a strong 'title', a 'subtitle'); "
        "then near the front a variant='stats' slide whose children are 3-4 KeyValue big "
        "numbers IF the topic has notable figures; then several variant='content' slides "
        "(each with a short UPPERCASE 'kicker' naming the topic + a concise title + a few "
        "bullets or short markdown); use variant='quote' for a punchy takeaway (put it in "
        "'title'); end with a closing slide. Use AS MANY slides as the content needs, give "
        "each slide a DISTINCT focus, and NEVER cram everything onto one slide or repeat the "
        "same structure. Keep ONE consistent theme — the app styles it, so do not specify colors.\n"
        "7. For a quiz/assessment build ONE Quiz component whose 'questions' is a list of "
        "REAL, answerable questions — each {question, options:[4 distinct strings], "
        "answer:<0-based index of the correct option>, explanation:<one sentence why>}. "
        "Produce the ACTUAL questions and options (as many as the request asks for, else "
        "about 10), NOT a description of a quiz or a grading rubric. VARY which option is "
        "correct across questions — the answer index must be spread across 0/1/2/3, never "
        "always the same slot. Put the questions array in dataModel and bind it with "
        '{"path":"/questions"}. The app handles selection, scoring and navigation.\n'
        f"Crew purpose: {purpose}\n"
        + (f"The user's request this turn: {query}\n" if query else "")
        + (
            f"Default surfaceKind (use only if the request doesn't imply another): {hint}\n"
            if hint
            else ""
        )
        + (
            "DELIVERABLE SETTINGS — apply these as DEFAULTS, but anything the request "
            "states explicitly ALWAYS overrides them (an explicit quantity in the request "
            f"wins over any count below): {guidance}\n"
            if guidance
            else ""
        )
        + "Example of a valid surface:\n"
        + example
    )


# Words in the user's request (or the crew hint) that signal a rich, non-prose
# surface is wanted — used to decide whether to spend a composer LLM call.
RICH_INTENT = (
    "presentation",
    "slide",
    "slides",
    "deck",
    "slideshow",
    "powerpoint",
    "pptx",
    "ppt",
    "pitch",
    "dashboard",
    "kpi",
    "metric",
    "metrics",
    "chart",
    "charts",
    "graph",
    "plot",
    "visualize",
    "visualise",
    "visualization",
    "visualisation",
    "analytics",
    "mindmap",
    "mind map",
    "concept map",
    "quiz",
    "quizzes",
    "assessment",
    "trivia",
    "exam",
    "test my",
    "test your",
)


def wants_rich_surface(text: str, query: str) -> bool:
    """True when a rich surface is worth a composer LLM call: the user asked for
    one this turn, or the answer carries a table worth turning into a real
    Table/Chart. Plain prose renders fine as markdown, so we skip the call.

    ``query`` carries the user's request AND (for crew/agent runs) the agent goal
    / crew purpose, so a "create a presentation" deliverable triggers even when the
    chat prompt itself has no rich-intent keyword."""
    intent = (query or "").lower()
    rich_intent = any(k in intent for k in RICH_INTENT)
    body = text or ""
    has_table = (
        "\n|" in body or "|---" in body or "| -" in body or "<table" in body.lower()
    )
    return rich_intent or has_table


def compose_a2ui(
    output_text: str,
    purpose: str = "",
    hint: str = "",
    query: str = "",
    *,
    llm_call: LLMCall,
    catalog: Optional[Dict[str, Any]] = None,
    enabled: bool = True,
    retries: int = 2,
    guidance: str = "",
) -> Dict[str, Any]:
    """Compose an A2UI surface from the agent's text answer. Generic, never raises.

    Args:
        output_text: the agent's final plain-text answer (becomes the content).
        purpose: the crew/agent purpose (steers surface choice).
        hint: default surfaceKind hint (used only if the request doesn't imply one).
        query: the user's request THIS turn (the primary surfaceKind signal).
        llm_call: injected ``(messages) -> str`` — the only LLM dependency.
        catalog: pre-loaded catalog; falls back to the bundled ``catalog.json``.
        enabled: master switch; when False returns a markdown surface immediately.
        retries: composer attempts before falling back to markdown.
        guidance: optional per-deliverable settings sentence (e.g. "aim for ~8
            slides; ≤4 bullets per slide") appended to the prompt as defaults the
            request can override. Supplied by the host from its UI config.

    Returns:
        A valid A2UI surface dict (a markdown surface on the cheap/fallback paths).
    """
    text = output_text or ""
    if not enabled:
        return markdown_surface(text)
    catalog = catalog if catalog is not None else load_catalog()
    if not catalog:
        return markdown_surface(text)
    # Cheap path: only spend a composer LLM call when a genuinely rich surface is
    # likely. Decide PER TURN from what the user actually asked (query), NOT the
    # static crew hint — folding the hint in here would turn EVERY answer
    # (including clarifying questions) into a deck for a presentation-biased crew.
    if not wants_rich_surface(text, query):
        return markdown_surface(text)
    try:
        messages: List[Dict[str, str]] = [
            {
                "role": "system",
                "content": a2ui_system_prompt(catalog, purpose, hint, query, guidance),
            },
            {"role": "user", "content": text},
        ]
        for _ in range(max(1, retries)):
            raw = llm_call(messages)
            raw_str = raw if isinstance(raw, str) else str(raw)
            payload = extract_json(raw_str)
            if payload and validate_surface(payload, catalog):
                return payload
            messages += [
                {"role": "assistant", "content": raw_str},
                {
                    "role": "user",
                    "content": "That was not a valid A2UI surface. "
                    "Reply with ONLY the corrected JSON object, using only allowed components.",
                },
            ]
    except Exception as exc:  # noqa: BLE001
        print(f"A2UI compose failed ({exc}); markdown fallback.")
    return markdown_surface(text)
