"""
Predefined "UI Configurator" emission for crew execution.

Kept in its own module so crew_preparation stays lean: this owns reading the
per-workspace UI config and appending an "emit a UI document" instruction to a
crew's final task when the workspace has it enabled. Because every execution
channel (chat, Crew mode, API, schedules) funnels through crew_preparation,
enforcing it here makes the behavior consistent everywhere.

The structured-UI format conforms to the A2UI protocol (see THIRD_PARTY_NOTICES);
the component list mirrors the frontend renderer (uiDocument.ts / UiRenderer.tsx).
"""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# Deliverable → human label used in the per-type theme guidance. Keys mirror the
# frontend UI-Configurator (UIConfigurator.tsx) and the ChatInput format options.
_THEME_LABELS = {
    "default": "Default (any deliverable not listed below)",
    "dashboard": "Dashboard / metrics / KPIs",
    "presentation": "Presentation / slides / deck",
    "genie": "Data answer / Genie / query result",
    "mindmap": "Mindmap / concept map",
    "album": "Album / image gallery",
    "quiz": "Quiz / assessment",
    "flashcards": "Flashcards / Anki / spaced repetition",
    "report": "Report / summary / briefing",
}
_THEME_ORDER = [
    "default",
    "dashboard",
    "presentation",
    "genie",
    "mindmap",
    "album",
    "quiz",
    "flashcards",
    "report",
]
_THEME_KEYS = (
    "accent",
    "background",
    "surface",
    "text",
    "heading",
    "muted",
    "font",
    "density",
)


def _palette_str(theme: Dict[str, Any]) -> str:
    """Render a single palette dict as a compact, readable token list."""
    parts = [f"{k} {theme[k]}" for k in _THEME_KEYS if theme.get(k)]
    return ", ".join(parts)


def _build_theme_block(
    themes: Dict[str, Any], deliverable: Optional[str] = None
) -> List[str]:
    """Per-deliverable branding guidance: tell the agent to put a matching
    `theme` on createSurface (the renderer applies it as the stage palette).

    When ``deliverable`` is known, emit only that deliverable's palette (plus the
    ``default`` fallback) instead of all eight — a task builds exactly one
    deliverable, so the other palettes are dead weight re-sent every iteration."""
    if not isinstance(themes, dict) or not themes:
        return []
    keys = _THEME_ORDER
    if deliverable:
        # Deliverable-specific palette FIRST, Default as the explicit fallback —
        # models tend to copy whichever palette is listed first.
        keys = [deliverable] if deliverable == "default" else [deliverable, "default"]
    palette_lines: List[str] = []
    for key in keys:
        theme = themes.get(key)
        if isinstance(theme, dict):
            palette = _palette_str(theme)
            if palette:
                palette_lines.append(f"- {_THEME_LABELS.get(key, key)}: {palette}.")
    if not palette_lines:
        return []
    return [
        "",
        'THEME / BRANDING — createSurface MUST carry a "theme" object copied from the',
        "palette below that MATCHES the deliverable you build (e.g. Slides → the",
        "Presentation palette; use the Default palette ONLY when no specific palette",
        "matches). The renderer applies it as the stage background, accent, text",
        "colors and font, so copy the matching palette's values exactly:",
        '  { "createSurface": { "surfaceId": "s1", "catalogId": "basic", "theme":',
        '    { "accent": "<accent>", "background": "<background>", "surface": "<surface>",',
        '      "text": "<text>", "heading": "<heading>", "muted": "<muted>",',
        '      "font": "<font>", "density": "<density>" } } }',
        '("font" is one of sans/serif/rounded/mono; "density" is comfortable/compact.)',
        "Use the palette for what you build:",
        *palette_lines,
    ]


def _build_directives_block(
    directives: Dict[str, Any], deliverable: Optional[str] = None
) -> List[str]:
    """Per-deliverable behavior settings, phrased on the frontend (UIConfigurator)
    and appended verbatim so each deliverable follows its configured options
    (slide count, KPI layout, quiz length, mindmap depth, report tone, …).

    When ``deliverable`` is known, emit only that deliverable's settings."""
    if not isinstance(directives, dict):
        return []
    keys = [k for k in _THEME_ORDER if k != "default"]
    if deliverable and deliverable != "default":
        keys = [deliverable]
    lines: List[str] = []
    for key in keys:
        text = directives.get(key)
        if isinstance(text, str) and text.strip():
            lines.append(f"- {_THEME_LABELS.get(key, key)}: {text.strip()}")
    if not lines:
        return []
    return [
        "",
        "DELIVERABLE SETTINGS — when you build one of these, follow its settings:",
        *lines,
    ]


# Always-on core: the output contract, the document skeleton, and the UNIVERSAL
# building blocks every deliverable shares. Everything deliverable-specific lives
# in _DELIVERABLE_GUIDANCE below and is added per-run for ONLY the inferred
# deliverable — so the prompt stays ~constant as new artifact types are added (a
# new type costs tokens only on ITS OWN runs, not on every other deliverable's).
_CORE: List[str] = [
    'OUTPUT FORMAT (STRICT): Return your result ONLY as a single JSON "UI document".',
    "Do NOT write HTML, CSS or JavaScript, and no prose or markdown code fences — only",
    "the JSON below. It is rendered by a design-system renderer into a polished app.",
    "",
    'Shape (note: each component\'s type goes in the "component" field, NOT "type"):',
    '{ "messages": [',
    '  { "createSurface": { "surfaceId": "s1", "catalogId": "basic" } },',
    '  { "updateComponents": { "surfaceId": "s1", "components": [',
    '    { "id": "root", "component": "Column", "children": ["title", "body"] },',
    '    { "id": "title", "component": "Text", "variant": "h1", "text": "Heading" },',
    '    { "id": "body", "component": "Text", "variant": "body", "text": "..." }',
    "  ] } }",
    "] }",
    "",
    'Every component object MUST use the key "component" (e.g. "component":"Card"), not',
    '"type". components is a FLAT list; build the tree by referencing child ids. The',
    'root component MUST have id "root". Use these universal building blocks PLUS the',
    "deliverable-specific components listed below — do NOT invent other component types:",
    "- Text (text, variant: h1..h5/body/caption), Row/Column (children, justify, align)",
    "- Card (title?, children), List (children), Divider, Image (url, alt?), Icon (name)",
    "- Badge (text, tone: good/warn/bad/neutral), Button (child), TextField (label, value)",
    "- CheckBox (label, value), Slider (label, min, max, value)",
]

# Per-deliverable guidance: the build intent + the components / shapes / rules
# UNIQUE to that deliverable. Sent for ONLY the inferred deliverable (or, when none
# is inferred, all of them behind the chooser header). Add a new artifact type by
# adding ONE entry here (+ a renderer component + a keyword) — it does not touch any
# other deliverable's prompt.
_DELIVERABLE_GUIDANCE: Dict[str, List[str]] = {
    "presentation": [
        "BUILD A PRESENTATION / slides / deck:",
        "- Slides (children) — a navigable deck; Slide (title?, children).",
        "- Inside slides use: Chart (chartType: bar/line/pie, title?, data:[{label,value}]),",
        "  Stat (label, value, delta?, tone), Dashboard (children), Table (columns, rows).",
        "SLIDE DESIGN RULES (important):",
        "- Each slide MUST fit ONE screen with no scrolling. One focal element per slide:",
        "  a short bullet list, OR a Dashboard of stats, OR 1-2 charts — NOT all at once.",
        "- Do NOT stack a Stat Dashboard AND multiple Charts on one slide; split them across",
        "  slides. A Dashboard should be the slide's full-width main element.",
        "- A short title + at most 3-4 brief points (≤1 sentence each). Aim for 6-9 slides.",
        "- Prefer Charts, Stat tiles, Badges and Icons over paragraphs. Never put dense",
        "  multi-paragraph text on a slide, and never wrap long text in a Card.",
        "- BALANCE two-column Rows: columns must carry similar visual weight; a Row has 2-3",
        "  columns max with a Chart or Stat in at least one. Do NOT pair a big text block",
        "  with a small chart — that leaves empty space and looks broken.",
    ],
    "dashboard": [
        "BUILD A DASHBOARD / metrics / KPIs:",
        "- Dashboard (children) — a responsive KPI/card grid; Stat (label, value, delta?,",
        "  tone: good/warn/bad/neutral) for headline numbers.",
        "- Chart (chartType: bar/line/pie, title?, data:[{label,value}]) for trends/breakdowns.",
        "- Table (columns:[str], rows:[[cell,…]]) for tabular detail.",
        "Lead with Stat tiles, add Charts for trends; keep it a clean grid (do NOT use Slides).",
    ],
    "genie": [
        "BUILD A DATA ANSWER / Genie / query result:",
        "- A short answer Text, then a Table (columns:[str], rows:[[cell,…]]) of the returned",
        "  rows, plus a Chart (chartType: bar/line/pie, data:[{label,value}]) when it helps.",
        "  Do NOT use Slides for data answers.",
    ],
    "report": [
        "BUILD A REPORT / summary / briefing:",
        "- Cards with Text, List and Badges; lead with a short executive-summary Card. Use",
        "  Badges for status/labels and a Chart or Table where it aids understanding.",
    ],
    "quiz": [
        "BUILD A QUIZ / assessment:",
        "- ONE Quiz component: Quiz (title?, questions:[{ question, options:[str],",
        "  answer: <0-based index of the correct option> }]). The renderer handles",
        "  selection, scoring and navigation — supply ONLY the data. Do NOT hand-build a",
        "  quiz from Cards/HTML and do NOT use Slides.",
    ],
    "flashcards": [
        "BUILD FLASHCARDS / an Anki deck (spaced-repetition study cards):",
        "- ONE Flashcards component: Flashcards (title?, layout?: grid|carousel, cards:[{ front, back }]).",
        "  Each card shows its \"front\" (question/term) and FLIPS to reveal \"back\" (the answer) when",
        '  clicked. layout:"carousel" shows ONE flashcard per screen; default is a grid.',
        "  Supply ONLY the data — do NOT hand-build flip cards from Cards/Buttons,",
        "  and do NOT use a Quiz (flashcards are self-graded, not multiple-choice).",
    ],
    "mindmap": [
        "BUILD A MINDMAP / concept map / idea tree:",
        "- ONE Mindmap: Mindmap (title?, root:{label, children:[{label, children:[…]}]}).",
        '  Nest children to any depth. A node may add "description" — a longer note shown in',
        "  the node's hover tooltip (use it for detail that won't fit the short label).",
    ],
    "album": [
        "BUILD AN ALBUM / image gallery:",
        "- ONE Album: Album (title?, layout?: grid|carousel, images:[{url, alt?, caption?}]).",
        "  Put EXISTING image links into images[].url (prefer DIRECT links ending in",
        '  .jpg/.png/.webp). layout:"carousel" shows ONE image per screen; default is a grid.',
        "  ALWAYS emit the Album with the URLs you have — never refuse, never fall back to a",
        "  markdown/text list, never invent images.",
    ],
}

# Order the deliverables are listed in when none is inferred (the chooser menu).
_GUIDANCE_ORDER = [
    "presentation",
    "dashboard",
    "genie",
    "report",
    "quiz",
    "flashcards",
    "mindmap",
    "album",
]

_CHOOSER_HEADER = [
    "MATCH THE REQUESTED DELIVERABLE — infer the single most fitting one from the",
    "request and build just that (do NOT default to slides). Your options:",
]

# Default directives for each deliverable type. Applied when a deliverable is
# inferred but the workspace config has no customizations yet. These mirror the
# frontend defaults from uiConfigShared.ts optionSpecs.
_DEFAULT_DIRECTIVES = {
    "flashcards": 'show one flashcard per screen that scrolls left→right — set the Flashcards component\'s "layout" to "carousel"; make about 12 flashcards; use question → answer cards; keep answers concise without examples.',
    "album": 'show one image per screen that scrolls left→right — set the Album component\'s "layout" to "carousel"; give every image a short caption; include at most 12 images.',
    "presentation": "aim for about 8 slides; at most 4 bullet points per slide; open with a dedicated title slide; end with a summary / takeaways slide.",
    "dashboard": "lay out KPI Stat tiles 3 per row; pick the chart type that best fits each metric; show a delta/trend on each Stat tile.",
    "quiz": "write as many questions as the request asks for — use the exact count from the request (e.g. 50 or 100) and never cap it; only when no count is given, default to about 10; mix easy, medium and hard questions; give each question 4 answer options.",
    "mindmap": "nest the tree up to 3 levels deep; give the central topic about 5 main branches; keep node labels plain text.",
    "report": "keep the report standard in length; write in a neutral tone; lead with an executive summary card; list sources / citations at the end.",
    "genie": "pick the chart type that best fits each metric; show at most 20 rows in the Table; do not surface the SQL query.",
}


def build_ui_instruction(
    accent: Optional[str] = None,
    themes: Optional[Dict[str, Any]] = None,
    directives: Optional[Dict[str, Any]] = None,
    deliverable: Optional[str] = None,
) -> str:
    """The instruction appended to the final task so the agent returns a
    renderable UI document instead of arbitrary HTML. When per-deliverable
    ``themes`` are configured they steer the surface palette and ``directives``
    steer each deliverable's behavior; otherwise a bare ``accent`` (legacy
    single-color config) is used as a lightweight hint.

    ``deliverable`` (one of the _THEME_ORDER keys, when it can be inferred from the
    task) narrows the per-deliverable theme/directive guidance to just that type,
    trimming ~550 tokens that would otherwise be re-sent every agent iteration."""
    accent_line = (
        f'\nUse "{accent}" as the accent color where relevant.'
        if accent and not themes
        else ""
    )
    # Always-on core + ONLY the inferred deliverable's component guidance. When the
    # deliverable can't be inferred, fall back to the full chooser menu (all slices)
    # so the agent can still pick — that ambiguous case is the exception, not the norm.
    lines: List[str] = list(_CORE)
    if deliverable and deliverable in _DELIVERABLE_GUIDANCE:
        lines.append("")
        lines.extend(_DELIVERABLE_GUIDANCE[deliverable])
    else:
        lines.append("")
        lines.extend(_CHOOSER_HEADER)
        for key in _GUIDANCE_ORDER:
            lines.append("")
            lines.extend(_DELIVERABLE_GUIDANCE[key])
    base = "\n".join(lines) + accent_line
    theme_block = _build_theme_block(themes, deliverable) if themes else []
    if theme_block:
        base = base + "\n" + "\n".join(theme_block)
    directives_block = (
        _build_directives_block(directives, deliverable) if directives else []
    )
    if directives_block:
        base = base + "\n" + "\n".join(directives_block)
    return base


# Keyword → deliverable key. Ordered by specificity; first match wins. Used to
# infer which single deliverable a task builds so the UI instruction can carry
# only that deliverable's theme/directive guidance instead of all eight.
_DELIVERABLE_KEYWORDS = [
    ("flashcard", "flashcards"),
    ("flash card", "flashcards"),
    ("anki", "flashcards"),
    ("spaced repetition", "flashcards"),
    ("quiz", "quiz"),
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
    ("genie", "genie"),
    ("data answer", "genie"),
    ("report", "report"),
    ("briefing", "report"),
]


def _infer_deliverable(text: str) -> Optional[str]:
    """Best-effort guess of the deliverable type from the final task's text.

    _DELIVERABLE_KEYWORDS is ordered by specificity, and a final task builds
    exactly ONE deliverable — so when several keywords match (e.g. a
    presentation task that mentions KPI charts), the FIRST match in the
    ordered list wins. The previous exactly-one-match rule returned ``None``
    for every multi-keyword task, which made the caller re-send ALL eight
    theme/directive blocks (~1.5-1.9k tokens) on every agent iteration.
    ``None`` (full-set fallback) is reserved for tasks that mention no
    deliverable at all."""
    if not text:
        return None
    lowered = text.lower()
    for keyword, deliverable in _DELIVERABLE_KEYWORDS:
        if keyword in lowered:
            return deliverable
    return None


async def apply_ui_emission(
    tasks: List[Dict[str, Any]], group_id: Optional[str]
) -> None:
    """
    If the workspace has Predefined UI enabled, append the UI-output instruction
    to the LAST task's description (mutated in place). No-op when disabled, when
    there are no tasks, or on any error (UI formatting must never break a run).
    """
    if not tasks or not group_id:
        return
    try:
        from src.db.session import request_scoped_session
        from src.services.ui_config_service import UIConfigService

        async with request_scoped_session() as session:
            config = await UIConfigService(session, group_id=group_id).get_config()

        if not config.enabled:
            return

        accent: Optional[str] = None
        themes: Optional[Dict[str, Any]] = None
        directives: Optional[Dict[str, Any]] = None
        if config.style_json:
            try:
                style = json.loads(config.style_json)
                accent = style.get("accent")
                raw_themes = style.get("themes")
                if isinstance(raw_themes, dict) and raw_themes:
                    themes = raw_themes
                raw_directives = style.get("directives")
                if isinstance(raw_directives, dict) and raw_directives:
                    directives = raw_directives
            except (ValueError, TypeError):
                accent = None

        last_task = tasks[-1]
        original = last_task.get("description", "") or ""

        # Infer the deliverable from the final task so the per-deliverable theme/
        # directive guidance is emitted for ONLY that type (a task builds one).
        # This trims ~550 tokens that would otherwise be re-sent every iteration.
        deliverable = _infer_deliverable(
            f"{original}\n{last_task.get('expected_output', '') or ''}"
        )

        # Apply default directives for the inferred deliverable if not already customized.
        # This ensures users get sensible defaults (e.g., carousel layout for flashcards)
        # even if they haven't visited the workspace configurator yet.
        if deliverable and deliverable in _DEFAULT_DIRECTIVES:
            if not directives:
                directives = {}
            if deliverable not in directives or not directives[deliverable]:
                directives[deliverable] = _DEFAULT_DIRECTIVES[deliverable]

        instruction = build_ui_instruction(accent, themes, directives, deliverable)
        last_task["description"] = f"{original}\n\n{instruction}"

        # The generated crew often bakes "produce raw HTML" into BOTH the task
        # description AND its expected_output (e.g. "Raw HTML source code starting
        # with <!DOCTYPE html>"). That directly conflicts with the UI-document
        # instruction we just appended: strong models reconcile it in our favor,
        # but weaker ones (e.g. Haiku) follow the louder HTML framing and emit
        # <!DOCTYPE html>, which then renders as raw HTML instead of through the
        # UI renderer. Overwrite expected_output so the agent has ONE consistent
        # target. Predefined-UI being enabled means every final output is meant
        # to render through the UI renderer, so this override is the intended
        # behavior, not a special case.
        last_task["expected_output"] = (
            'A single JSON "UI document" — the '
            '{"messages":[{"createSurface":...},{"updateComponents":...}]} structure '
            "described in the task. NOT HTML, NOT a file, NOT prose or code fences — "
            "only the JSON document."
        )
        logger.info(
            "[UIEmission] Appended UI-output instruction + overrode expected_output "
            "on final task (group=%s)",
            group_id,
        )
    except Exception as e:  # noqa: BLE001 — never let UI formatting break execution
        logger.warning("[UIEmission] Skipped UI emission: %s", e)
