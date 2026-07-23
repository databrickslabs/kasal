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
    # Mindmap keywords MUST precede the bare "map" keyword below, else "mind map"
    # / "concept map" greedily match "map" and mis-route to the geographic map.
    ("mindmap", "mindmap"),
    ("mind map", "mindmap"),
    ("concept map", "mindmap"),
    ("geographic", "map"),
    ("geospatial", "map"),
    ("on a map", "map"),
    ("map", "map"),
    ("album", "album"),
    ("gallery", "album"),
    ("forecast", "forecast"),
    ("forecasting", "forecast"),
    ("projection", "forecast"),
    ("prediction", "forecast"),
    ("sequence diagram", "sequence"),
    ("interaction diagram", "sequence"),
    # "network graph"/"node graph" must precede the bare "graph" so a plain chart
    # request ("bar graph") doesn't route to the node-link Graph.
    ("network graph", "graph"),
    ("node graph", "graph"),
    ("network diagram", "graph"),
    ("dependency graph", "graph"),
    ("relationship graph", "graph"),
    ("presentation", "presentation"),
    ("slide", "presentation"),
    ("deck", "presentation"),
    ("dashboard", "dashboard"),
    ("kpi", "dashboard"),
    ("metric", "dashboard"),
    # Diagram-archetype keywords sit AFTER presentation/dashboard (so "a deck with
    # a timeline" stays a presentation) and after the sequence/network entries
    # above (so "sequence diagram" keeps routing to Sequence). Bare "diagram" last.
    ("flowchart", "diagram"),
    ("flow chart", "diagram"),
    ("process diagram", "diagram"),
    ("org chart", "diagram"),
    ("organization chart", "diagram"),
    ("timeline", "diagram"),
    ("roadmap", "diagram"),
    ("funnel", "diagram"),
    ("pyramid", "diagram"),
    ("diagram", "diagram"),
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


# Slide variants that are expected to carry a real body in `children`
# (title/section/quote/stats slides are body-less or KeyValue-only by design).
_BODY_SLIDE_VARIANTS = (
    "content",
    "two-column",
    "two_column",
    "twocolumn",
    "visual",
    "agenda",
)


def presentation_needs_body(payload: Any) -> bool:
    """True when a presentation surface is a hollow skeleton — half or more of its
    body-bearing slides (content / two-column / visual / agenda) have only a
    kicker+title and no real body, so they render as near-empty slides.
    title/section/quote/stats slides are body-less by design and don't count.
    Used to retry (then fall back to a readable markdown document) instead of
    returning an empty deck.
    """
    if not isinstance(payload, dict):
        return False
    if str(payload.get("surfaceKind") or "").lower() != "presentation":
        return False
    comps = {
        c.get("id"): c
        for c in (payload.get("components") or [])
        if isinstance(c, dict) and c.get("id") is not None
    }

    def has_content(cid: str) -> bool:
        child = comps.get(cid)
        if not isinstance(child, dict):
            return False
        comp = child.get("component")
        if comp in ("Text", "Heading"):
            return bool(str(child.get("text") or "").strip())
        if comp == "Markdown":
            content = child.get("content")
            return (
                content.strip() != ""
                if isinstance(content, str)
                else content is not None
            )
        if comp in ("Slide", "SlideDeck", "Divider"):
            return False
        # KeyValue / Chart / Table / Image / Card / Grid / List … = real content
        return True

    content_slides = [
        c
        for c in comps.values()
        if c.get("component") == "Slide"
        and str(c.get("variant") or "content").lower() in _BODY_SLIDE_VARIANTS
    ]
    if not content_slides:
        return False
    empty = sum(
        1
        for s in content_slides
        if not any(has_content(cid) for cid in (s.get("children") or []))
    )
    return empty * 2 >= len(content_slides)


# Components that read as a "visual" on a slide — drives the deck design lint.
_VISUAL_COMPONENTS = frozenset(
    {
        "Chart",
        "Diagram",
        "Table",
        "Graph",
        "Sequence",
        "Forecast",
        "Map",
        "Image",
        "Album",
        "KeyValue",
    }
)


def _slide_has_visual(slide: Dict[str, Any], comps: Dict[Any, Dict[str, Any]]) -> bool:
    """True when a slide is a stats slide or any descendant is a visual component."""
    if str(slide.get("variant") or "").lower() == "stats":
        return True
    seen: set = set()
    stack = list(slide.get("children") or [])
    while stack:
        cid = stack.pop()
        if cid in seen:
            continue
        seen.add(cid)
        child = comps.get(cid)
        if not isinstance(child, dict):
            continue
        if child.get("component") in _VISUAL_COMPONENTS:
            return True
        stack.extend(child.get("children") or [])
    return False


def presentation_design_lint(payload: Any) -> List[str]:
    """Deterministic design critique of a VALID presentation surface (no LLM).

    Returns human-readable findings when the deck is visually flat — the
    cheap analogue of PPTAgent-style reflective evaluation. Findings drive ONE
    correction retry in ``compose_a2ui``; a deck that stays flat is still
    returned (text-only slides beat no deck), unlike the hollow-body gate.
    """
    if not isinstance(payload, dict):
        return []
    if str(payload.get("surfaceKind") or "").lower() != "presentation":
        return []
    comps = {
        c.get("id"): c
        for c in (payload.get("components") or [])
        if isinstance(c, dict) and c.get("id") is not None
    }
    root = comps.get(payload.get("root"))
    ordered_ids = list((root or {}).get("children") or [])
    slides = [
        comps[cid]
        for cid in ordered_ids
        if isinstance(comps.get(cid), dict) and comps[cid].get("component") == "Slide"
    ]
    if len(slides) < 6:
        return []  # short decks are fine text-heavy; don't over-engineer them
    body_slides = [
        s
        for s in slides
        if str(s.get("variant") or "content").lower() in _BODY_SLIDE_VARIANTS
        or str(s.get("variant") or "").lower() == "stats"
    ]
    if not body_slides:
        return []
    visual_count = sum(1 for s in body_slides if _slide_has_visual(s, comps))
    findings: List[str] = []
    if visual_count == 0:
        findings.append(
            "no slide carries any visual (no Chart, Diagram, Table or stats slide)"
        )
    elif visual_count * 3 < len(body_slides):
        findings.append(
            f"only {visual_count} of {len(body_slides)} body slides carry a visual "
            "(aim for at least one in three)"
        )
    # Monotony: a run of >3 consecutive text-only body slides reads as a bullet wall.
    run = 0
    for s in body_slides:
        if _slide_has_visual(s, comps):
            run = 0
        else:
            run += 1
            if run > 3:
                findings.append(
                    "more than three consecutive text-only slides (a bullet wall) — "
                    "break the run with a Diagram, Chart, stats or two-column slide"
                )
                break
    return findings


def plan_presentation_outline(
    text: str,
    query: str,
    purpose: str,
    llm_call: LLMCall,
    guidance: str = "",
) -> Optional[List[Dict[str, str]]]:
    """Outline pre-pass for presentations: one small LLM call that plans the deck
    (slide titles + layout variant + which visual each slide carries) BEFORE the
    full compose. Two-stage generation is what the strong deck generators do
    (outline → slides); planning visuals per slide up front is what actually gets
    diagrams/charts onto slides instead of bullet walls. Returns None on any
    failure/weak plan so the composer degrades to today's single-pass behavior.
    """
    try:
        prompt = (
            "You plan a slide deck OUTLINE from an answer's content. Reply with ONE "
            'JSON object only: {"slides": [{"title": str, "variant": str, '
            '"visual": str, "focus": str}]}.\n'
            "variant is one of: title, content, two-column, visual, stats, agenda, "
            "quote, section. visual names the visual that slide carries: "
            "'chart:<bar|line|pie|area|scatter|radar>', "
            "'diagram:<process|timeline|cycle|funnel|pyramid|comparison|matrix2x2|hierarchy>', "
            "'table', 'stats', or 'none'. focus is one short sentence on what the "
            "slide covers.\n"
            "Rules: open with a title slide and end with a takeaways slide; give AT "
            "LEAST one in three body slides a real visual — classify the content: "
            "steps/phases -> diagram:process, dated milestones -> diagram:timeline, "
            "loops -> diagram:cycle, narrowing stages -> diagram:funnel, layered "
            "levels -> diagram:pyramid, two options -> diagram:comparison, two axes "
            "-> diagram:matrix2x2, org/tree structure -> diagram:hierarchy, numeric "
            "series -> chart, key figures -> stats; never plan two consecutive "
            "slides with the same variant unless both are 'content'; plan only "
            "slides the content can genuinely fill.\n"
            + (f"Deck purpose: {purpose}\n" if purpose else "")
            + (f"The user's request: {query}\n" if query else "")
            + (
                f"Deliverable settings (defaults the request overrides): {guidance}\n"
                if guidance
                else ""
            )
        )
        raw = llm_call(
            [
                {"role": "system", "content": prompt},
                {"role": "user", "content": text or ""},
            ]
        )
        payload = extract_json(raw if isinstance(raw, str) else str(raw))
        slides = payload.get("slides") if isinstance(payload, dict) else None
        out: List[Dict[str, str]] = []
        for s in slides or []:
            if not isinstance(s, dict):
                continue
            title = str(s.get("title") or "").strip()
            if not title:
                continue
            out.append(
                {
                    "title": title,
                    "variant": str(s.get("variant") or "content").strip().lower(),
                    "visual": str(s.get("visual") or "none").strip().lower(),
                    "focus": str(s.get("focus") or "").strip(),
                }
            )
        # A plan under 3 slides is weaker than no plan; an absurdly long one is
        # noise — clamp to a real deck's size.
        return out[:24] if len(out) >= 3 else None
    except Exception:  # noqa: BLE001
        return None


def _deref(value: Any, data_model: Dict[str, Any]) -> Any:
    """Resolve a literal-or-{path} binding against dataModel (shallow JSON pointer)."""
    if isinstance(value, dict) and isinstance(value.get("path"), str):
        cur: Any = data_model or {}
        for part in value["path"].split("/"):
            if part == "":
                continue
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        return cur
    return value


def quiz_needs_work(payload: Any) -> bool:
    """True when a quiz surface is low quality — the model returned a *description*
    of a quiz, too few real questions, or malformed items (bad option lists / answer
    indices). Drives a retry, then a markdown fallback, instead of shipping a broken
    quiz. Option ORDER is not checked: the renderer shuffles display order, so a model
    that parks every answer at one index is already handled there.
    """
    if not isinstance(payload, dict):
        return False
    if str(payload.get("surfaceKind") or "").lower() != "quiz":
        return False
    comps = [c for c in (payload.get("components") or []) if isinstance(c, dict)]
    quiz = next((c for c in comps if c.get("component") == "Quiz"), None)
    if quiz is None:
        return False
    questions = _deref(quiz.get("questions"), payload.get("dataModel") or {})
    if not isinstance(questions, list) or not questions:
        return True

    def ok(q: Any) -> bool:
        if not isinstance(q, dict):
            return False
        if not str(q.get("question") or "").strip():
            return False
        opts = q.get("options")
        if not isinstance(opts, list):
            return False
        texts = [str(o).strip() for o in opts if str(o).strip()]
        # need >= 3 non-empty, DISTINCT options (4 is the asked-for norm)
        if len(texts) < 3 or len(set(texts)) != len(texts):
            return False
        ans = q.get("answer")
        if isinstance(ans, bool):
            return False
        try:
            ans_i = int(ans)
        except (TypeError, ValueError):
            return False
        return 0 <= ans_i < len(opts)

    valid = [q for q in questions if ok(q)]
    if len(valid) < 3:
        return True
    return (len(questions) - len(valid)) * 2 >= len(questions)


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
                    "icon": "trending-up",
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
    # A presentation example: every content slide has a BODY, and one slide carries
    # a chart bound to dataModel. This is the structure weaker models most often get
    # wrong (emitting title-only slides), so showing it concretely matters.
    pres_example = json.dumps(
        {
            "surfaceKind": "presentation",
            "root": "deck",
            "components": [
                {
                    "id": "deck",
                    "component": "SlideDeck",
                    "children": ["s1", "s2", "s3", "s4"],
                },
                {
                    "id": "s1",
                    "component": "Slide",
                    "variant": "title",
                    "kicker": "INTRODUCTION",
                    "title": "How LLMs Work",
                    "subtitle": "Understanding large language models",
                },
                {
                    "id": "s2",
                    "component": "Slide",
                    "variant": "content",
                    "kicker": "ARCHITECTURE",
                    "title": "Transformer Foundation",
                    "children": ["s2a", "s2b", "s2c"],
                },
                {
                    "id": "s2a",
                    "component": "Text",
                    "text": "Self-attention lets every token weigh all others in the sequence.",
                },
                {
                    "id": "s2b",
                    "component": "Text",
                    "text": "Positional encodings inject word order into an otherwise order-agnostic model.",
                },
                {
                    "id": "s2c",
                    "component": "Text",
                    "text": "Stacked decoder blocks refine the representation layer by layer.",
                },
                {
                    "id": "s3",
                    "component": "Slide",
                    "variant": "visual",
                    "kicker": "SCALE",
                    "title": "Parameters Over Time",
                    "children": ["s3c"],
                },
                {
                    "id": "s3c",
                    "component": "Chart",
                    "chartType": "bar",
                    "xKey": "model",
                    "yKeys": ["params"],
                    "data": {"path": "/sizes"},
                },
                {
                    "id": "s4",
                    "component": "Slide",
                    "variant": "two-column",
                    "kicker": "TRAINING",
                    "title": "From Text to Model",
                    "children": ["s4a", "s4b", "s4d"],
                },
                {
                    "id": "s4a",
                    "component": "Text",
                    "text": "Each stage refines the model: scale builds knowledge, tuning builds usefulness.",
                },
                {
                    "id": "s4b",
                    "component": "Text",
                    "text": "Alignment is what turns a raw predictor into a safe assistant.",
                },
                {
                    "id": "s4d",
                    "component": "Diagram",
                    "archetype": "process",
                    "items": [
                        {
                            "label": "Pretraining",
                            "detail": "Next-token prediction at scale",
                        },
                        {"label": "Fine-tuning", "detail": "Instruction data"},
                        {"label": "Alignment", "detail": "RLHF"},
                    ],
                },
            ],
            "dataModel": {
                "sizes": [
                    {"model": "GPT-2", "params": 1.5},
                    {"model": "GPT-3", "params": 175},
                    {"model": "GPT-4", "params": 1800},
                ]
            },
        }
    )
    # A quiz example: REAL questions with plausible distractors and teaching
    # explanations, and the correct index spread across questions. Weaker models
    # otherwise emit a description of a quiz or park every answer at one slot.
    quiz_example = json.dumps(
        {
            "surfaceKind": "quiz",
            "root": "q",
            "components": [
                {
                    "id": "q",
                    "component": "Quiz",
                    "title": "LLM Basics",
                    "questions": {"path": "/questions"},
                }
            ],
            "dataModel": {
                "questions": [
                    {
                        "question": "What does self-attention let each token do?",
                        "options": [
                            "Weigh the relevance of every other token",
                            "Store gradients between training epochs",
                            "Compress the vocabulary size",
                            "Encode images into patches",
                        ],
                        "answer": 0,
                        "explanation": "Self-attention scores each token against all others, so every token is represented in context.",
                    },
                    {
                        "question": "Why are positional encodings added to token embeddings?",
                        "options": [
                            "To shrink the model",
                            "Because attention is otherwise order-agnostic",
                            "To tokenize punctuation",
                            "To set the learning rate",
                        ],
                        "answer": 1,
                        "explanation": "Attention treats the input as a set, so word order must be injected explicitly.",
                    },
                ]
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
        "component. For these SPECIAL deliverables, use a 'dashboard' or 'document' "
        "surface whose ROOT is the matching component (see rule 11): a photo "
        "album/image gallery -> ONE Album; a forecast/projection/prediction over time "
        "-> ONE Forecast; a relationship/network/dependency graph -> ONE Graph; a "
        "sequence/interaction diagram -> ONE Sequence; a flowchart/process/timeline/"
        "roadmap/funnel/org chart/comparison -> ONE Diagram with the matching "
        "archetype. Only when NONE of the above fit, use 'document' with Markdown.\n"
        "6. For presentations build a REAL deck of Slides, each with a 'variant'. Start "
        "with variant='title' (a short UPPERCASE 'kicker', a strong 'title', a 'subtitle'), "
        "then the body slides, and end with a closing slide. "
        "EVERY body slide (variant 'content', 'two-column', 'visual' or 'agenda') MUST "
        "carry a BODY in its 'children': for 'content', 3-5 Text nodes (one concise, FULL "
        "sentence each) OR a Markdown node whose content is 3-5 '- ' bullet lines. A body "
        "slide with only a kicker+title and no children is INVALID — never emit one. Make "
        "each bullet substantive (a real fact/insight from the answer), not a single word. "
        "MAKE THE DECK VISUAL — this is what separates a good deck from a bullet wall. "
        "Target: at least ONE IN THREE body slides carries a visual, chosen by CLASSIFYING "
        "the slide's content: steps/phases/workflow -> a Diagram archetype 'process'; dated "
        "milestones/roadmap -> 'timeline'; a repeating loop -> 'cycle'; narrowing stages/"
        "conversion -> 'funnel'; layered levels -> 'pyramid'; two options weighed -> "
        "'comparison'; two evaluation axes -> 'matrix2x2'; org/tree structure -> "
        "'hierarchy'. Numeric series/breakdowns -> a Chart (chartType 'bar' | 'line' | "
        "'pie' | 'area' | 'scatter' | 'radar', with 'xKey', 'yKeys', and its 'data' array "
        'in dataModel referenced by {"path":"/key"}); key figures -> a variant=\'stats\' '
        "slide whose children are 3-4 KeyValue big numbers (give each an 'icon'); detailed "
        "rows -> a Table. LAYOUT VARIETY: pair bullets WITH a visual using "
        "variant='two-column' (children = the Text nodes then the visual node); give a "
        "dominant Chart/Diagram/Table its own variant='visual' slide; use variant='agenda' "
        "(children = short Text nodes) for the overview; variant='quote' for a punchy "
        "takeaway (put it in 'title'). NEVER use the same variant on more than two "
        "consecutive slides. Use AS MANY slides as the content needs, give each a DISTINCT "
        "focus, and NEVER cram everything onto one slide. Keep ONE consistent theme — the "
        "app styles it, so do not specify colors.\n"
        "7. For a quiz/assessment build ONE Quiz component whose 'questions' is a list of "
        "REAL, answerable questions — each {question, options:[4 distinct strings], "
        "answer:<0-based index of the correct option>, explanation:<one sentence why>}. "
        "Produce the ACTUAL questions and options (as many as the request asks for, else "
        "about 10), NOT a description of a quiz or a grading rubric. QUALITY BAR: each "
        "question tests ONE clear idea with a single unambiguous correct answer; make the "
        "three distractors PLAUSIBLE (common misconceptions or near-misses), similar in "
        "length and style to the answer — never joke options, 'all of the above', or "
        "'none of the above'; cover DIFFERENT facets of the topic (definition, application, "
        "comparison, cause/effect) and vary difficulty rather than rephrasing one fact; keep "
        "each stem concise and self-contained; write every explanation to TEACH why the "
        "answer is correct (ideally noting why a tempting distractor is wrong). VARY which "
        "option is correct across questions — spread the answer index across 0/1/2/3, never "
        "always the same slot. Put the questions array in dataModel and bind it with "
        '{"path":"/questions"}. The app handles selection, scoring and navigation.\n'
        "8. For a dashboard build a SYMMETRIC, COHERENT layout, not a random pile of "
        "cards. Use a Grid with a CONSISTENT column count and group like with like: "
        "(a) lead with ONE balanced row of KeyValue KPI tiles — pick a count that FILLS "
        "the row evenly (2, 3, or 4 — e.g. 3 or 6 KPIs in a 3-column grid), never leave a "
        "lone orphan tile in a half-empty row; (b) then the Chart cells, also balanced per "
        "row (two or three charts of the SAME kind of size sit together) — give EVERY chart "
        "a short 'title' and pick the right chartType (bar for comparisons, line for trends "
        "over time, pie for parts-of-a-whole); (c) put any Table LAST — it renders full-width "
        "across the bottom, so it is the wide footer, never squeezed into one narrow column. "
        "Keep tiles in a row visually parallel (each a single value + a short label), keep "
        "spacing and structure uniform, and use ONE consistent theme — the app styles colors, "
        "so do NOT specify them. Aim for a grid that reads as a tidy, aligned whole. "
        "(d) if the answer's data carries REAL latitude/longitude coordinates (e.g. "
        "per-site or per-region rows with lat/lng), ADD a Map component as a full-width "
        "cell (placed LAST like a Table) plotting those points — {lat, lng, label?, "
        'value?} in dataModel, bound with {"path":"/points"}; when the geography IS the '
        "main story, prefer surfaceKind 'map' instead (rule 10). NEVER invent coordinates "
        "— omit the map when the data only names places (e.g. 'US East') without lat/lng.\n"
        "9. For flashcards/anki build ONE Flashcards component whose 'cards' is a list of "
        "REAL study cards, each {front, back, hint?}: front is a concise prompt "
        "(question / term / cloze), back is the correct answer/definition, hint is an "
        "OPTIONAL nudge. Produce the ACTUAL cards (as many as the request asks for, else "
        "about 12), each testing ONE idea — never a description of a deck. Put the cards "
        'array in dataModel and bind it with {"path":"/cards"}. The app handles flipping, '
        "navigation and shuffle.\n"
        "10. For a map use surfaceKind 'map' with ONE Map component ONLY WHEN the data has "
        "real latitude/longitude coordinates. points is a list of {lat:<number>, "
        "lng:<number>, label?, value?} — emit the ACTUAL numeric coordinates for each place "
        '(put the array in dataModel, bind with {"path":"/points"}). value is an optional '
        "magnitude that sizes the marker (e.g. count, population). If you do NOT have real "
        "coordinates, use a dashboard or table instead — never invent coordinates.\n"
        "11. SPECIAL DATA/DIAGRAM COMPONENTS (use inside a 'dashboard' or 'document' surface "
        "when the content fits — a single one may be the surface root):\n"
        "  - Forecast: a time-series prediction with a confidence band. Use it (NOT a plain "
        "Chart) whenever the data has a forecast/predicted value over time, especially with "
        "lower/upper bounds. Pass the query rows AS-IS in data (the renderer auto-detects the "
        "time, forecast, lower, upper, actual and category columns); set seriesKey to a "
        "category column to draw one line+band per group (e.g. risk_category).\n"
        "  - Graph: a node-link/network diagram for RELATIONSHIPS between entities — nodes "
        "[{id,label,group?}] + edges [{from,to,label?}]. Use for dependencies, networks, "
        "entity links; NOT for hierarchy (use Mindmap) or time series.\n"
        "  - Sequence: a sequence diagram for INTERACTIONS/flows over time between participants "
        "— actors [names] + messages [{from,to,text,dashed?}].\n"
        "  - Album: a photo carousel for IMAGE galleries — items [{src,caption?,href?}] where "
        "src is a DIRECT image URL. Never put non-image page links in an Album (use a Table).\n"
        "  - Diagram: a business diagram from a curated ARCHETYPE — the layout is automatic, "
        "supply ONLY labels. archetype: 'process' (3-6 sequential steps), 'timeline' (dated "
        "milestones), 'cycle' (repeating loop), 'funnel' (narrowing stages), 'pyramid' "
        "(layered levels, apex first), 'comparison' (EXACTLY 2 items, each with 'points': "
        "[strings]), 'matrix2x2' (EXACTLY 4 items in reading order + optional xLabel/yLabel), "
        "'hierarchy' (items[0] is the root with nested 'children'). items: [{label (2-5 "
        "words), detail? (one short sentence), value?, points?, children?}]. Use whenever "
        "content describes steps, phases, structure or comparisons — NOT for numeric series "
        "(Chart) or entity networks (Graph).\n"
        "12. ICONS: KeyValue and Card accept an optional 'icon' — pick the closest from: "
        "trending-up, trending-down, users, dollar, clock, check, alert, target, zap, "
        "globe, database, server, shield, rocket, lightbulb, chart, calendar, settings, "
        "search, link, cloud, cpu, layers, gauge, award, briefcase, building, star, "
        "package, wrench, brain, lock. Omit 'icon' rather than invent a name.\n"
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
        + "Example of a valid dashboard surface:\n"
        + example
        + "\nExample of a valid PRESENTATION surface (note EVERY body slide has "
        "children, one slide is a full-bleed chart, and one pairs bullets with a "
        "process Diagram in a two-column layout):\n"
        + pres_example
        + "\nExample of a valid QUIZ surface (real questions, plausible distractors, "
        "answer index varied, teaching explanations):\n" + quiz_example
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
    "flashcard",
    "flash card",
    "anki",
    "map",
    "geographic",
    "geospatial",
    "forecast",
    "forecasting",
    "projection",
    "predict",
    "prediction",
    "graph",
    "network",
    "sequence diagram",
    "diagram",
    "flowchart",
    "flow chart",
    "timeline",
    "roadmap",
    "funnel",
    "org chart",
    "pyramid",
    "album",
    "gallery",
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
    # A valid-but-visually-flat deck kept as the floor: if the design-lint retry
    # burns the last attempt, fails, or raises, we ship this instead of falling
    # all the way back to markdown. Declared outside the try so an exception
    # mid-retry can't lose an already-composed deck.
    best: Optional[Dict[str, Any]] = None
    try:
        # Presentation OUTLINE pre-pass (two-stage generation): plan slide titles,
        # layout variants and per-slide visuals with a small extra LLM call, then
        # hand the plan to the composer. Skipped for every other deliverable; any
        # failure degrades to the single-pass behavior. Disable with
        # A2UI_PRESENTATION_OUTLINE=0.
        user_content = text
        if (
            infer_deliverable(query) == "presentation"
            and os.getenv("A2UI_PRESENTATION_OUTLINE", "1") != "0"
        ):
            outline = plan_presentation_outline(
                text, query, purpose, llm_call, guidance
            )
            if outline:
                user_content = (
                    text
                    + "\n\n[SLIDE PLAN — a planning pass already chose each slide's "
                    "title, variant and visual. FOLLOW IT (adjust only where the "
                    "content genuinely cannot fill a slide):]\n"
                    + json.dumps({"slides": outline})
                )
        messages: List[Dict[str, str]] = [
            {
                "role": "system",
                "content": a2ui_system_prompt(catalog, purpose, hint, query, guidance),
            },
            {"role": "user", "content": user_content},
        ]
        design_retry_done = False
        for _ in range(max(1, retries)):
            raw = llm_call(messages)
            raw_str = raw if isinstance(raw, str) else str(raw)
            payload = extract_json(raw_str)
            if payload and validate_surface(payload, catalog):
                if presentation_needs_body(payload):
                    correction = (
                        "Most body slides have NO body (only a kicker and title), "
                        "so they render as empty slides. Give EVERY body slide "
                        "(variant 'content', 'two-column', 'visual' or 'agenda') a "
                        "real body in its 'children': 3-5 Text nodes (a full "
                        "sentence each) or a Markdown node with 3-5 '- ' bullet lines, "
                        "and add a Chart, Diagram or stats slide where the topic has "
                        "numbers or structure. Reply with ONLY the corrected JSON object."
                    )
                elif quiz_needs_work(payload):
                    correction = (
                        "This quiz is weak or malformed. Return a Quiz whose 'questions' "
                        '(in dataModel, bound by {"path":"/questions"}) is a list of at '
                        "least several REAL questions, each {question, options:[4 "
                        "distinct, plausible strings], answer:<0-based index>, "
                        "explanation:<one teaching sentence>}. Every question needs a "
                        "non-empty stem, four distinct plausible options, and a valid "
                        "answer index — produce the ACTUAL questions, not a description. "
                        "Reply with ONLY the corrected JSON object."
                    )
                else:
                    findings = (
                        presentation_design_lint(payload)
                        if not design_retry_done
                        else []
                    )
                    if not findings:
                        return payload
                    # ONE reflective design retry (cheap PPTAgent-style critique);
                    # if the retry can't do better we still ship this valid deck.
                    design_retry_done = True
                    best = payload
                    correction = (
                        "This deck is valid but visually flat: "
                        + "; ".join(findings)
                        + ". Improve it: CLASSIFY slide content into a Diagram "
                        "(archetype 'process' for steps, 'timeline' for milestones, "
                        "'cycle' for loops, 'funnel' for narrowing stages, "
                        "'comparison' for two options, 'matrix2x2' for two axes, "
                        "'hierarchy' for org/tree structure), add a Chart where "
                        "there are numeric series, or a variant='stats' slide of "
                        "KeyValue big numbers; pair bullets with a visual using "
                        "variant='two-column'. Keep the same content and slide "
                        "count. Reply with ONLY the corrected JSON object."
                    )
            else:
                correction = (
                    "That was not a valid A2UI surface. Reply with ONLY the corrected "
                    "JSON object, using only allowed components."
                )
            messages += [
                {"role": "assistant", "content": raw_str},
                {"role": "user", "content": correction},
            ]
    except Exception as exc:  # noqa: BLE001
        print(f"A2UI compose failed ({exc}); markdown fallback.")
    return best if best is not None else markdown_surface(text)
