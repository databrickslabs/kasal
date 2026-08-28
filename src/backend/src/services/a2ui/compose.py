"""A2UI generative-UI composer — the single, portable implementation shared by
the live Kasal app and every exported Databricks app.

It turns an agent's plain-text answer into ONE declarative A2UI *surface*
(``{surfaceKind, root, components[], dataModel}``) that the shared frontend
renderer draws as rich UI (dashboard / mindmap / quiz / document /
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
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Caller-injected LLM: takes a list of {"role","content"} messages, returns text.
LLMCall = Callable[[List[Dict[str, str]]], str]


class ComposeStream:
    """What the composer tells its host as a surface is being generated.

    The composer does not know how a surface reaches a reader — that is SSE here,
    something else in an exported app — so it only announces the milestones and
    lets the host ship them. A no-op base class rather than a Protocol so the
    default (``compose_a2ui`` with no stream) costs nothing and every method is
    always safe to call.
    """

    def attempt(self, n: int) -> None:
        """A surface generation is about to start; its tokens belong to revision n.

        Called before the SURFACE calls only, never the outline pre-pass — the
        outline is not a surface and parsing it as one would emit nonsense.
        """

    def final(self, surface: Optional[Dict[str, Any]]) -> None:
        """Composition finished. This surface, or None, is authoritative."""


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
    # Board keywords precede "dashboard": a "sprint board" is a Kanban, not a KPI
    # dashboard, and the bare word would otherwise never be reached.
    ("kanban", "kanban"),
    ("sprint board", "kanban"),
    ("task board", "kanban"),
    ("project board", "kanban"),
    ("backlog", "kanban"),
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
    essentials subset; select → the full catalog minus ``disabled_components``
    (what the per-component toggles write); custom → the admin's catalog_json
    (surfaceKinds backfilled); full (or any legacy/unknown value like "basic") →
    the full bundled catalog.

    ``minimal`` and ``custom`` are retained for rows saved before the toggles
    existed — the UI no longer offers either, but a stored value must keep
    resolving to what it always meant."""
    cfg = cfg or {}
    if cfg.get("id") is None:
        return default_catalog
    ctype = (cfg.get("catalog_type") or "full").lower()
    # "select" — the admin ticked components off in the UI. Everything is enabled
    # by default and `disabled_components` names the exceptions, so a workspace
    # AUTOMATICALLY gains new components as A2UI grows. Hand-written catalog JSON
    # cannot do that: it is a frozen snapshot, so anything added later is invisible
    # to the composer until someone re-pastes it. Storing the exclusions rather
    # than the inclusions is the whole difference.
    if ctype == "select":
        disabled = cfg.get("disabled_components")
        if isinstance(disabled, str):
            try:
                disabled = json.loads(disabled)
            except (ValueError, TypeError):
                disabled = None
        if not isinstance(disabled, (list, tuple, set)) or not disabled:
            return default_catalog
        off = {str(x) for x in disabled}
        keep = [k for k in (default_catalog.get("components") or {}) if k not in off]
        # Never hand back an empty catalog: the composer cannot emit anything and
        # every surface silently degrades to plain text. A config that disabled
        # everything is a mistake, not an instruction.
        return subset_catalog(default_catalog, keep) if keep else default_catalog
    if ctype == "custom":
        raw = (cfg.get("catalog_json") or "").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                # A catalog's ``components`` is a DICT of name -> spec (the shape
                # ``a2ui_system_prompt`` iterates with ``.items()``). Guard on that
                # exact shape, not merely "truthy": a saved SURFACE is also a dict
                # with a ``components`` key, but there it is a LIST of component
                # INSTANCES. One got pasted into a workspace's custom catalog_json,
                # passed the old truthy check, and every rich surface for that
                # workspace 500'd inside the prompt build ("'list' object has no
                # attribute 'items'") — swallowed by compose's broad except, so the
                # deck silently fell back to plain text on every model. A non-dict
                # (or empty) ``components`` is not a usable catalog; fall back.
                components = (
                    parsed.get("components") if isinstance(parsed, dict) else None
                )
                if isinstance(components, dict) and components:
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
        "5. Choose surfaceKind from the USER'S REQUEST first: for a "
        "dashboard/metrics/charts use 'dashboard' with Grid+Chart/KeyValue/Table; for a "
        "mind map use 'mindmap'; for a quiz/assessment/test use 'quiz' with ONE Quiz "
        "component. For these SPECIAL deliverables, use a 'dashboard' or 'document' "
        "surface whose ROOT is the matching component (see rule 10): a photo "
        "album/image gallery -> ONE Album; a forecast/projection/prediction over time "
        "-> ONE Forecast; a relationship/network/dependency graph -> ONE Graph; a "
        "sequence/interaction diagram -> ONE Sequence. "
        "Only when NONE of the above fit, use 'document' with Markdown.\n"
        "6. For a quiz/assessment build ONE Quiz component whose 'questions' is a list of "
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
        "7. For a dashboard build a SYMMETRIC, COHERENT layout, not a random pile of "
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
        "main story, prefer surfaceKind 'map' instead (rule 9). NEVER invent coordinates "
        "— omit the map when the data only names places (e.g. 'US East') without lat/lng.\n"
        "8. For flashcards/anki build ONE Flashcards component whose 'cards' is a list of "
        "REAL study cards, each {front, back, hint?}: front is a concise prompt "
        "(question / term / cloze), back is the correct answer/definition, hint is an "
        "OPTIONAL nudge. Produce the ACTUAL cards (as many as the request asks for, else "
        "about 12), each testing ONE idea — never a description of a deck. Put the cards "
        'array in dataModel and bind it with {"path":"/cards"}. The app handles flipping, '
        "navigation and shuffle.\n"
        "9. For a map use surfaceKind 'map' with ONE Map component ONLY WHEN the data has "
        "real latitude/longitude coordinates. points is a list of {lat:<number>, "
        "lng:<number>, label?, value?} — emit the ACTUAL numeric coordinates for each place "
        '(put the array in dataModel, bind with {"path":"/points"}). value is an optional '
        "magnitude that sizes the marker (e.g. count, population). If you do NOT have real "
        "coordinates, use a dashboard or table instead — never invent coordinates.\n"
        "10. SPECIAL DATA COMPONENTS (use inside a 'dashboard' or 'document' surface "
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
        "11. ICONS: KeyValue and Card accept an optional 'icon' — pick the closest from: "
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
        + "\nExample of a valid QUIZ surface (real questions, plausible distractors, "
        "answer index varied, teaching explanations):\n" + quiz_example
    )


# Words in the user's request (or the crew hint) that signal a rich, non-prose
# surface is wanted — used to decide whether to spend a composer LLM call.
RICH_INTENT = (
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


# Intents that the HTML path owns end-to-end (rendered as self-contained
# ```html in chat, with fullscreen + export), so A2UI must NOT compose for them.
# Deck intent: word-boundary anchored, never bare substrings. A prompt that
# merely MENTIONS one of these words ("how do slide-out panels work?") must not
# hijack the turn away from A2UI — html_owned_intent disables ALL of A2UI for
# the turn, so a false positive silently costs the palette deck, charts and
# tables. Branches (each verified against a false-positive battery in
# test_wants_rich_surface):
#   - "presentation", except the architecture sense ("presentation layer") and
#     the formatting sense ("the presentation of this data"); "the presentation"
#     alone stays a deck follow-up ("make the presentation shorter")
#   - slideshow/keynote/PowerPoint/ppt(x), and slide/pitch/sales/investor deck
#     (space, hyphen or joined)
#   - "slides" as a noun — the verb sense is excluded by its trailing particle
#     ("the panel slides in", "revenue slides each summer")
#   - a numbered, spelled-out, possessive or deck-positional single slide
#     ("slide 3", "slide #2", "slide two", "the slide's title", "a closing
#     slide", "on one slide") so deck FOLLOW-UPS stay on the HTML path; bare
#     "a/the slide" is NOT enough ("revenue took a slide") and "slide-"
#     compounds ("slide-out") are excluded
#   - bare "deck" only with a topic preposition ("deck about X") or a
#     making/request verb in the same sentence ("build a deck", "turn this
#     into a deck", "I need a deck") — and never the idiom "on deck"
_DECK_INTENT_RE = re.compile(
    r"(?<!the )\bpresentations?\b(?!\s+layer\b)"
    r"|\bthe\s+presentations?\b(?!\s+(?:layer|of)\b)"
    r"|\bslideshows?\b"
    r"|\bkeynotes?\b"
    r"|\bpower\s*points?\b"
    r"|\bpptx?\b"
    r"|\b(?:slide|pitch|sales|investor)[\s-]*decks?\b"
    r"|\bslides\b(?!\s+(?:in|out|up|down|into|onto|across|over|open|closed|each|every)\b)"
    r"|\bslide\s*#?\s*\d+\b"
    r"|\bslide\s+(?:one|two|three|four|five|six|seven|eight|nine|ten)\b"
    r"|\bslide['\u2019]s\b"
    r"|\b(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth"
    r"|last|next|previous|prev|cover|title|final|closing|opening|intro"
    r"|another|new|one|single|each|every)\s+slides?\b(?!-)"
    r"|(?<!on )\bdecks?\s+(?:about|on|for)\b"
    r"|\b(?:create|make|build|generate|prepare|design|produce|draft|turn|convert"
    r"|need|want|give\s+me|show\s+me|send\s+me|put\s+together)\b"
    r"[^.?!\n]{0,60}?(?<!on )\bdecks?\b",
    re.IGNORECASE,
)
# Diagram intent: diagram/flowchart nouns; an explicit "no diagram" opts out.
# A residual false positive ("explain the architecture diagram") costs only a
# prose turn without A2UI — accepted, unlike the deck branches above.
_DIAGRAM_INTENT_RE = re.compile(
    r"(?<!no )\bdiagrams?\b|\bflow[\s-]*charts?\b", re.IGNORECASE
)


def deck_intent(query: str) -> bool:
    """True when the request asks for a presentation/slides/deck (word-boundary
    matched — see ``_DECK_INTENT_RE``). Also used by the chat directive to decide
    when the rich slide-design template (and the workspace deck palette) apply."""
    return bool(_DECK_INTENT_RE.search(query or ""))


def html_owned_intent(query: str) -> bool:
    """True when the request is for a diagram/slides/presentation — handled by the
    HTML renderer, not A2UI. Used by the chat path to disable A2UI composition
    for these intents (A2UI no longer builds decks or diagrams)."""
    q = query or ""
    return bool(_DECK_INTENT_RE.search(q) or _DIAGRAM_INTENT_RE.search(q))


def wants_rich_surface(text: str, query: str) -> bool:
    """True when a rich surface is worth a composer LLM call: the user asked for
    one this turn, or the answer carries a table worth turning into a real
    Table/Chart. Plain prose renders fine as markdown, so we skip the call.

    ``query`` carries the user's request AND (for crew/agent runs) the agent goal
    / crew purpose, so a rich deliverable in the crew goal triggers even when the
    chat prompt itself has no rich-intent keyword."""
    intent = (query or "").lower()
    rich_intent = any(k in intent for k in RICH_INTENT)
    body = text or ""
    has_table = (
        "\n|" in body or "|---" in body or "| -" in body or "<table" in body.lower()
    )
    # Yield to an agent-authored diagram: a self-contained ```html/```svg block is
    # rendered directly in chat as a %md-sandbox diagram, so composing an A2UI
    # surface over it would double-render (or replace) the agent's own drawing.
    if re.search(r"```(?:html|svg)\s*\n", body, re.IGNORECASE):
        return False
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
    stream: Optional[ComposeStream] = None,
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
        guidance: optional per-deliverable settings sentence appended to the
            prompt as defaults the request can override. Supplied by the host
            from its UI config.
        stream: optional host hook told when each surface generation starts
            and what the final surface is — so the host can render progressively
            instead of waiting for the return.
    Returns:
        A valid A2UI surface dict (a markdown surface on the cheap/fallback paths).
    """
    text = output_text or ""
    stream = stream or ComposeStream()

    def _done(surface: Dict[str, Any]) -> Dict[str, Any]:
        """Every exit reports its result: a host left without a final would sit
        on a half-drawn surface forever."""
        try:
            stream.final(surface)
        except Exception:  # noqa: BLE001 — reporting must not fail composition
            pass
        return surface

    if not enabled:
        return _done(markdown_surface(text))
    catalog = catalog if catalog is not None else load_catalog()
    if not catalog:
        return _done(markdown_surface(text))
    # Cheap path: only spend a composer LLM call when a genuinely rich surface is
    # likely. The intent signal is the user's request AND the agent goal / crew
    # purpose — exactly what wants_rich_surface documents its `query` arg to be,
    # and what the host (compose_surface) already checked before shipping the
    # skeleton. Passing `query` alone here re-decided it on a narrower signal and
    # DISAGREED whenever the rich intent lived in the purpose (a research/report
    # crew whose chat prompt has no rich keyword): the host shipped a deck shell,
    # then this bailed to markdown BEFORE any LLM call — retracting the shell to
    # plain text on every model, strong ones included. Fold the purpose in so the
    # two gates agree.
    if not wants_rich_surface(text, f"{query}\n{purpose}"):
        return _done(markdown_surface(text))
    try:
        user_content = text
        messages: List[Dict[str, str]] = [
            {
                "role": "system",
                "content": a2ui_system_prompt(catalog, purpose, hint, query, guidance),
            },
            {"role": "user", "content": user_content},
        ]
        for attempt_n in range(max(1, retries)):
            # A correction pass regenerates the WHOLE surface, so each attempt is
            # its own revision and supersedes whatever the last one streamed.
            stream.attempt(attempt_n)
            raw = llm_call(messages)
            raw_str = raw if isinstance(raw, str) else str(raw)
            payload = extract_json(raw_str)
            if payload and validate_surface(payload, catalog):
                if quiz_needs_work(payload):
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
                    return _done(payload)
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
        # WARNING with the traceback, not a bare print: this except wraps the
        # whole compose (prompt build, every LLM call, validation), so anything
        # it swallows silently drops the surface to markdown with no way to tell
        # WHY — which is exactly how a streamed-compose failure looked like a weak
        # model. logger reaches logs/system.log; a print only reached stdout.
        logger.warning(
            "A2UI compose failed (%s); markdown fallback.", exc, exc_info=True
        )
    return _done(markdown_surface(text))
