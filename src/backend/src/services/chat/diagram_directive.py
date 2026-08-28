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

from typing import Any, Dict

# Markers so each piece is appended at most once, even if a spec is reused.
_MARKER = "%md-sandbox diagram specialist"
_DECK_MARKER = "SLIDE DECK DESIGN SYSTEM"

# Requests the rich deck template applies to (a subset of the HTML-owned intents).
_DECK_INTENT = ("presentation", "slide", "deck", "slideshow")

DIAGRAM_DIRECTIVE = (
    "\n\nWhen (and only when) the user asks for a diagram, chart, or visual, you "
    "are a %md-sandbox diagram specialist: reply with a single fenced ```html "
    "code block that renders inside a Databricks %md-sandbox notebook cell. "
    "Rules for that block:\n"
    "- Output ONLY the ```html block (a one-line intro before it is fine). Do "
    "NOT include the %md-sandbox magic line — the app adds it on copy.\n"
    "- Make it fully self-contained: inline <style> and inline <svg> only. No "
    "external URLs, no remote <img>, no CDN scripts, no web fonts. Use a system "
    "font stack (-apple-system, \"Segoe UI\", Roboto, sans-serif).\n"
    "- Build the diagram from inline SVG (boxes, arrows, labels) with a wrapping "
    "<div> for legends/captions; scope all CSS under one wrapper class.\n"
    "- Do NOT put blank lines anywhere inside the HTML — a blank line terminates "
    "the inline-HTML block. Keep every line contiguous (indentation is fine).\n"
    "- Keep text legible (font-size >= 14px, good contrast). On a follow-up, "
    "return the FULL updated ```html block, never a diff.\n"
    "\n"
    "When the user asks for a presentation, slides, or a deck, output a single "
    "```html block of <section class=\"slide\">…</section> elements (one per "
    "slide, 6–10 slides). Follow the slide-design example below if one is "
    "provided. For every other request, answer normally in prose/markdown."
)

# A concrete, polished example the model extends. Everything is styled INSIDE
# each <section> on purpose: the app renders (and exports) one slide at a time,
# so a shared <style> outside the sections would be lost.
DECK_TEMPLATE = (
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
    "- Palette: dark #0b2026 / teal #1B5162 backgrounds, warm-white #F9F7F4 or "
    "#FFFFFF cards, accents #4299E0 / #00A972 / #FF5F46 / #FFAB00. No blank lines.\n"
    "Example cover slide:\n"
    "<section class=\"slide\" style=\"width:1280px;height:720px;box-sizing:border-box;"
    "position:relative;overflow:hidden;padding:72px;display:flex;flex-direction:column;"
    "justify-content:center;background:linear-gradient(135deg,#0b2026,#1B5162);color:#fff;"
    "font-family:-apple-system,'Segoe UI',Roboto,sans-serif\">"
    "<div style=\"width:64px;height:6px;background:#FF5F46;border-radius:3px;margin-bottom:28px\"></div>"
    "<div style=\"text-transform:uppercase;letter-spacing:3px;font-size:16px;color:#9fc0cc;margin-bottom:14px\">Overview</div>"
    "<h1 style=\"font-size:56px;line-height:1.1;margin:0;font-weight:800\">Deck Title</h1>"
    "<p style=\"font-size:24px;color:#cfe0e6;margin-top:18px;max-width:820px\">One-line subtitle that frames the story.</p>"
    "<div style=\"position:absolute;bottom:40px;left:72px;font-size:14px;color:#7fa3b0\">Presenter • 2026</div>"
    "</section>\n"
    "Example content slide (card grid):\n"
    "<section class=\"slide\" style=\"width:1280px;height:720px;box-sizing:border-box;"
    "position:relative;overflow:hidden;padding:64px;background:#F9F7F4;color:#0b2026;"
    "font-family:-apple-system,'Segoe UI',Roboto,sans-serif\">"
    "<div style=\"text-transform:uppercase;letter-spacing:2px;font-size:14px;color:#618794;margin-bottom:8px\">Concepts</div>"
    "<h2 style=\"font-size:38px;margin:0 0 28px;font-weight:800\">Section Heading</h2>"
    "<div style=\"display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px\">"
    "<div style=\"background:#fff;border-radius:14px;box-shadow:0 6px 20px rgba(27,49,57,.08);overflow:hidden\">"
    "<div style=\"height:6px;background:#4299E0\"></div><div style=\"padding:22px\">"
    "<h3 style=\"margin:0 0 8px;font-size:22px\">Card One</h3>"
    "<p style=\"margin:0;font-size:18px;color:#425763;line-height:1.5\">Short supporting sentence.</p></div></div>"
    "<div style=\"background:#fff;border-radius:14px;box-shadow:0 6px 20px rgba(27,49,57,.08);overflow:hidden\">"
    "<div style=\"height:6px;background:#00A972\"></div><div style=\"padding:22px\">"
    "<h3 style=\"margin:0 0 8px;font-size:22px\">Card Two</h3>"
    "<p style=\"margin:0;font-size:18px;color:#425763;line-height:1.5\">Short supporting sentence.</p></div></div>"
    "<div style=\"background:#fff;border-radius:14px;box-shadow:0 6px 20px rgba(27,49,57,.08);overflow:hidden\">"
    "<div style=\"height:6px;background:#FFAB00\"></div><div style=\"padding:22px\">"
    "<h3 style=\"margin:0 0 8px;font-size:22px\">Card Three</h3>"
    "<p style=\"margin:0;font-size:18px;color:#425763;line-height:1.5\">Short supporting sentence.</p></div></div>"
    "</div>"
    "<div style=\"position:absolute;bottom:28px;right:64px;font-size:14px;color:#8aa2ac\">2</div>"
    "</section>"
)


def _is_deck_request(prompt: str) -> bool:
    p = (prompt or "").lower()
    return any(k in p for k in _DECK_INTENT)


def apply_diagram_directive(agent_spec: Dict[str, Any], prompt: str = "") -> Dict[str, Any]:
    """Append the diagram directive to ``agent_spec['backstory']`` (idempotent),
    plus the rich deck template when ``prompt`` is a presentation/slide request.

    Mutates and returns the same dict for convenience.
    """
    backstory = str(agent_spec.get("backstory") or "")
    if _MARKER not in backstory:
        backstory = backstory + DIAGRAM_DIRECTIVE
    if _is_deck_request(prompt) and _DECK_MARKER not in backstory:
        backstory = backstory + DECK_TEMPLATE
    agent_spec["backstory"] = backstory
    return agent_spec
