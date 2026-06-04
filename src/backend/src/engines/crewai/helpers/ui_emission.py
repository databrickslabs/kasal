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


def build_ui_instruction(accent: Optional[str] = None) -> str:
    """The instruction appended to the final task so the agent returns a
    renderable UI document instead of arbitrary HTML."""
    accent_line = (
        f'\nUse "{accent}" as the accent color where relevant.' if accent else ""
    )
    return "\n".join([
        'OUTPUT FORMAT (STRICT): Return your result ONLY as a single JSON "UI document".',
        "Do NOT write HTML, CSS or JavaScript, and no prose or markdown code fences — only",
        "the JSON below. It is rendered by a design-system renderer into a polished app.",
        "",
        "MATCH THE REQUESTED DELIVERABLE — do not default to slides. Build what the",
        "task/user asked for:",
        "- QUIZ / assessment / 'interactive quiz' → ONE Quiz component with a questions",
        "  array (the renderer handles selection, scoring, navigation). Do NOT hand-build a",
        "  quiz from Cards/HTML and do NOT use Slides.",
        "- PRESENTATION / deck / 'slides' → Slides with Slide children.",
        "- DASHBOARD / metrics / KPIs → Dashboard of Stat tiles + Chart(s).",
        "- REPORT / summary / briefing → Cards with Text, List and Badges.",
        "- DATA ANSWER / Genie / query result → a short answer Text, then a Table of the",
        "  returned rows, plus a Chart when it helps; do NOT use Slides for data answers.",
        "If the request is ambiguous, infer the most fitting one (don't always pick Slides).",
        "",
        "Shape (note: each component's type goes in the \"component\" field, NOT \"type\"):",
        '{ "messages": [',
        '  { "createSurface": { "surfaceId": "s1", "catalogId": "basic" } },',
        '  { "updateComponents": { "surfaceId": "s1", "components": [',
        '    { "id": "root", "component": "Column", "children": ["title", "body"] },',
        '    { "id": "title", "component": "Text", "variant": "h1", "text": "Heading" },',
        '    { "id": "body", "component": "Text", "variant": "body", "text": "..." }',
        "  ] } }",
        "] }",
        "",
        'Every component object MUST use the key "component" (e.g. "component":"Card"),',
        "not \"type\". components is a FLAT list; build the tree by referencing child",
        'ids. The root component MUST have id "root". Allowed components ONLY:',
        '- Text (text, variant: h1..h5/body/caption), Row/Column (children, justify, align)',
        "- Card (title?, children), List (children), Divider, Image (url, alt?), Icon (name)",
        '- Badge (text, tone: good/warn/bad/neutral), Button (child), TextField (label, value)',
        "- CheckBox (label, value), Slider (label, min, max, value)",
        "- ChoicePicker (label, options:[{label,value}], value) — single choice for quizzes/forms",
        "- Dashboard (children) — responsive KPI/card grid; Stat (label, value, delta?, tone)",
        '- Chart (chartType: bar/line/pie, title?, data:[{label,value}])',
        '- Table (columns:[str], rows:[[cell,…],…]) — for data / Genie query results',
        '- Quiz (title?, questions:[{ question, options:[str], answer: <0-based index of the',
        '  correct option> }]) — a complete interactive scored quiz; supply ONLY the data',
        "- Slides (children) — navigable deck; Slide (title?, children)",
        "",
        "Pick the components that fit the deliverable: Dashboard+Stat+Chart for metrics,",
        "Slides+Slide for presentations, ChoicePicker for quizzes. Do not invent other types.",
        "",
        "SLIDE DESIGN RULES (important):",
        "- Each slide MUST fit ONE screen with no scrolling. One focal element per slide:",
        "  either a short bullet list, OR a Dashboard of stats, OR 1-2 charts — NOT all at once.",
        "- Do NOT stack a Stat Dashboard AND multiple Charts on the same slide; split them",
        "  across separate slides. A Dashboard should be the slide's full-width main element.",
        "- A short title + at most 3-4 brief points (≤1 sentence each). Aim for 6-9 slides total.",
        "- Prefer Charts, Stat tiles, Badges and Icons over paragraphs. Never put dense",
        "  multi-paragraph text on a slide, and never wrap long text in a Card.",
        "- BALANCE two-column Rows: each column must carry similar visual weight (e.g. a few",
        "  short points on one side, a Chart or Stat group on the other). Do NOT pair a large",
        "  text block with a small chart — that leaves empty space and looks broken.",
        "- A Row should have 2-3 columns max; put a Chart or Stat in at least one of them.",
        "- Use Stat tiles for numbers, Chart for trends/breakdowns, Badge for status/labels." + accent_line,
    ])


async def apply_ui_emission(tasks: List[Dict[str, Any]], group_id: Optional[str]) -> None:
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
        if config.style_json:
            try:
                accent = json.loads(config.style_json).get("accent")
            except (ValueError, TypeError):
                accent = None

        instruction = build_ui_instruction(accent)
        last_task = tasks[-1]
        original = last_task.get("description", "") or ""
        last_task["description"] = f"{original}\n\n{instruction}"
        logger.info(
            "[UIEmission] Appended UI-output instruction to final task (group=%s)", group_id
        )
    except Exception as e:  # noqa: BLE001 — never let UI formatting break execution
        logger.warning("[UIEmission] Skipped UI emission: %s", e)
