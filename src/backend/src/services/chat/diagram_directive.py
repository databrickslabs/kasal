"""Chat light-agent directive: emit diagrams and slide decks as self-contained HTML.

The ChatMode UI renders any assistant message containing a fenced ```html (or
```svg) block as a live diagram, or — when it holds `<section class="slide">`
elements — as a paged, downloadable slide deck. To make the chat light agent
PRODUCE that block on request, we append the directive below to its backstory at
run time (see ``chat/service.py``). Chat runs a single default "Assistant" agent,
not a user-edited crew agent, so the instruction belongs in the run path.

Two pieces:
- ``DIAGRAM_DIRECTIVE`` — always applied; the diagram rules + a brief slide note.
- ``DECK_TEMPLATE`` — a concrete, elegant example deck, applied ONLY for
  presentation/slide/deck requests so non-deck chat turns stay lean. A weak model
  imitates a good example far better than it follows adjectives.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# Markers so each piece is appended at most once, even if a spec is reused.
_MARKER = "%md-sandbox diagram specialist"
_DECK_MARKER = "SLIDE DECK DESIGN SYSTEM"

DIAGRAM_DIRECTIVE = (
    "\n\nWhen (and only when) the user asks for a diagram, chart, or visual, you "
    "are a %md-sandbox diagram specialist: reply with a single fenced ```html "
    "code block that renders inside a Databricks %md-sandbox notebook cell. "
    "Rules for that block:\n"
    "- Output ONLY the ```html block (a one-line intro before it is fine). Do "
    "NOT include the %md-sandbox magic line — the app adds it on copy.\n"
    "- Make it fully self-contained: inline <style> and inline <svg> only. No "
    "external URLs, no remote <img>, no CDN scripts, no web fonts. Use a system "
    'font stack (-apple-system, "Segoe UI", Roboto, sans-serif).\n'
    "- Build the diagram from inline SVG (boxes, arrows, labels) with a wrapping "
    "<div> for legends/captions; scope all CSS under one wrapper class.\n"
    "- Do NOT put blank lines anywhere inside the HTML — a blank line terminates "
    "the inline-HTML block. Keep every line contiguous (indentation is fine).\n"
    "- Keep text legible (font-size >= 14px, good contrast). On a follow-up, "
    "return the FULL updated ```html block, never a diff.\n"
    "\n"
    "When the user asks for a presentation, slides, or a deck, output a single "
    '```html block of <section class="slide">…</section> elements (one per '
    "slide, 6–10 slides). Follow the slide-design example below if one is "
    "provided. For every other request, answer normally in prose/markdown."
)

# A concrete, polished example the model extends. Everything is styled INSIDE
# each <section> on purpose: the app renders (and exports) one slide at a time,
# so a shared <style> outside the sections would be lost.
#
# COLORS COME FROM THE WORKSPACE PALETTE (Configuration -> UI -> themes), not
# from this file: ``deck_template(palette)`` substitutes the workspace's
# presentation palette into the example, so HTML decks match A2UI decks. The
# defaults below apply only when no palette is configured.
_DEFAULT_DECK_PALETTE: Dict[str, str] = {
    "accent": "#FF5F46",
    "background": "#0b2026",
    "surface": "#F9F7F4",
    "text": "#425763",
    "heading": "#0b2026",
    "muted": "#618794",
}


#: #rgb/#rrggbb(aa) hex, or a bare CSS color name — nothing else may be
#: interpolated into the template's style attributes.
_COLOR_TOKEN_RE = __import__("re").compile(
    r"#[0-9a-fA-F]{3,8}|[a-zA-Z][a-zA-Z0-9-]{2,30}"
)


def _luminance(color: str) -> float:
    """Relative luminance of a #rgb/#rrggbb color; 0.5 on any parse failure."""
    c = (color or "").strip().lstrip("#")
    if len(c) == 3:
        c = "".join(ch * 2 for ch in c)
    if len(c) != 6:
        return 0.5
    try:
        r, g, b = (int(c[i : i + 2], 16) / 255 for i in (0, 2, 4))
    except ValueError:
        return 0.5
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _on(color: str, dark: str = "#101418", light: str = "#FFFFFF") -> str:
    """A readable text color for the given background."""
    return light if _luminance(color) < 0.5 else dark


def deck_template(palette: Optional[Dict[str, Any]] = None) -> str:
    """The slide-design directive, colored by the workspace's deck palette.

    ``palette`` is the UIConfig themes entry for the "presentation" deliverable
    (accent/background/surface/text/heading/muted); missing or malformed keys
    fall back to the defaults. The cover slide sits on ``background`` with
    auto-contrast text; content slides sit on ``surface`` with ``heading``/
    ``text``/``muted`` type and ``accent`` details — the same roles the A2UI
    renderer gives these tokens, so both deck systems read as one design.
    """
    p = dict(_DEFAULT_DECK_PALETTE)
    if isinstance(palette, dict):  # a malformed themes entry must never crash a run
        for key, value in palette.items():
            # Only real color tokens (#hex or a bare CSS color name) may enter
            # the template — the values land inside style attributes, so a stray
            # quote or arbitrary string would corrupt the example the model
            # imitates.
            if (
                key in p
                and isinstance(value, str)
                and _COLOR_TOKEN_RE.fullmatch(value.strip())
            ):
                p[key] = value.strip()
    cover_text = _on(p["background"])
    cover_muted = cover_text + "CC"  # _on returns #rrggbb, so +alpha is safe
    card_bg = "#FFFFFF" if _luminance(p["surface"]) >= 0.5 else p["surface"]
    return (
        "\n\nSLIDE DECK DESIGN SYSTEM — build the deck to match these two example "
        "slides exactly (same palette, type scale, spacing, cards), producing 6–10 "
        "slides and VARYING the layout (cover, section divider, 2–3 card grid, a "
        "big-number stat row, a comparison, a simple inline-SVG diagram). Rules that "
        "matter:\n"
        "- Put ALL styling INSIDE each <section> (inline style attributes) — each "
        "slide renders independently, so styles outside the section are lost.\n"
        "- Each section is EXACTLY width:1280px;height:720px;box-sizing:border-box; "
        "position:relative;overflow:hidden with ~56–72px padding, a full-bleed "
        "background (never bare white), a colored accent bar, an uppercase kicker, a "
        "bold title, and a small footer with the slide number.\n"
        "- A diagram slide's inline SVG must be PLANNED before drawing: set a "
        "viewBox sized to the free area, divide the height into horizontal "
        "bands (title band, one band per layer, 36-48px empty gap between "
        "bands), draw boxes only inside their band, route arrows only through "
        "the gaps, and give layer labels their own empty band — text must "
        "never sit on an arrow or another shape, and nothing may overlap or "
        "extend past the viewBox.\n"
        f"- Palette (use ONLY these colors): cover/divider background {p['background']}, "
        f"content background {p['surface']}, cards {card_bg}, accent {p['accent']}, "
        f"headings {p['heading']}, body text {p['text']}, muted labels {p['muted']}. "
        "No blank lines anywhere in the HTML.\n"
        "Example cover slide:\n"
        '<section class="slide" style="width:1280px;height:720px;box-sizing:border-box;'
        "position:relative;overflow:hidden;padding:72px;display:flex;flex-direction:column;"
        f"justify-content:center;background:{p['background']};color:{cover_text};"
        "font-family:-apple-system,'Segoe UI',Roboto,sans-serif\">"
        f'<div style="width:64px;height:6px;background:{p["accent"]};border-radius:3px;margin-bottom:28px"></div>'
        f'<div style="text-transform:uppercase;letter-spacing:3px;font-size:16px;color:{cover_muted};margin-bottom:14px">Overview</div>'
        f'<h1 style="font-size:56px;line-height:1.1;margin:0;font-weight:800">Deck Title</h1>'
        f'<p style="font-size:24px;color:{cover_muted};margin-top:18px;max-width:820px">One-line subtitle that frames the story.</p>'
        f'<div style="position:absolute;bottom:40px;left:72px;font-size:14px;color:{cover_muted}">Presenter • 2026</div>'
        "</section>\n"
        "Example content slide (card grid):\n"
        '<section class="slide" style="width:1280px;height:720px;box-sizing:border-box;'
        f"position:relative;overflow:hidden;padding:64px;background:{p['surface']};color:{p['heading']};"
        "font-family:-apple-system,'Segoe UI',Roboto,sans-serif\">"
        f'<div style="text-transform:uppercase;letter-spacing:2px;font-size:14px;color:{p["muted"]};margin-bottom:8px">Concepts</div>'
        f'<h2 style="font-size:38px;margin:0 0 28px;font-weight:800;color:{p["heading"]}">Section Heading</h2>'
        '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px">'
        f'<div style="background:{card_bg};border-radius:14px;box-shadow:0 6px 20px rgba(27,49,57,.08);overflow:hidden">'
        f'<div style="height:6px;background:{p["accent"]}"></div><div style="padding:22px">'
        '<h3 style="margin:0 0 8px;font-size:22px">Card One</h3>'
        f'<p style="margin:0;font-size:18px;color:{p["text"]};line-height:1.5">Short supporting sentence.</p></div></div>'
        f'<div style="background:{card_bg};border-radius:14px;box-shadow:0 6px 20px rgba(27,49,57,.08);overflow:hidden">'
        f'<div style="height:6px;background:{p["accent"]}"></div><div style="padding:22px">'
        '<h3 style="margin:0 0 8px;font-size:22px">Card Two</h3>'
        f'<p style="margin:0;font-size:18px;color:{p["text"]};line-height:1.5">Short supporting sentence.</p></div></div>'
        f'<div style="background:{card_bg};border-radius:14px;box-shadow:0 6px 20px rgba(27,49,57,.08);overflow:hidden">'
        f'<div style="height:6px;background:{p["accent"]}"></div><div style="padding:22px">'
        '<h3 style="margin:0 0 8px;font-size:22px">Card Three</h3>'
        f'<p style="margin:0;font-size:18px;color:{p["text"]};line-height:1.5">Short supporting sentence.</p></div></div>'
        "</div>"
        f'<div style="position:absolute;bottom:28px;right:64px;font-size:14px;color:{p["muted"]}">2</div>'
        "</section>"
    )


#: The default-palette rendering — kept as a constant for callers/tests that
#: need the template's shape without a workspace palette.
DECK_TEMPLATE = deck_template()


def _is_deck_request(prompt: str) -> bool:
    # One matcher for "is this a deck request" everywhere: the same anchored
    # regex that routes the turn to the HTML renderer (see compose.deck_intent).
    from src.services.a2ui.compose import deck_intent

    return deck_intent(prompt)


def apply_diagram_directive(
    agent_spec: Dict[str, Any],
    prompt: str = "",
    themes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Append the diagram directive to ``agent_spec['backstory']`` (idempotent),
    plus the rich deck template when ``prompt`` is a presentation/slide request.

    ``themes`` is the workspace UIConfig themes map ({deliverableKey: palette});
    the "presentation" palette (or "default") colors the deck template so HTML
    decks follow the workspace palette exactly like A2UI decks do.

    Mutates and returns the same dict for convenience.
    """
    backstory = str(agent_spec.get("backstory") or "")
    if _MARKER not in backstory:
        backstory = backstory + DIAGRAM_DIRECTIVE
    if _is_deck_request(prompt) and _DECK_MARKER not in backstory:
        palette = None
        if isinstance(themes, dict):
            palette = themes.get("presentation") or themes.get("default")
        backstory = backstory + deck_template(palette)
    agent_spec["backstory"] = backstory
    return agent_spec
