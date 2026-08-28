"""apply_diagram_directive: append the %md-sandbox diagram rule to a chat agent."""

from src.services.chat.diagram_directive import (
    DECK_TEMPLATE,
    DIAGRAM_DIRECTIVE,
    apply_diagram_directive,
)


def test_appends_directive_to_existing_backstory():
    spec = {"role": "Assistant", "backstory": "You are helpful."}
    apply_diagram_directive(spec)
    assert spec["backstory"].startswith("You are helpful.")
    assert "%md-sandbox diagram specialist" in spec["backstory"]
    assert "```html" in spec["backstory"]


def test_handles_missing_backstory():
    spec = {"role": "Assistant"}
    apply_diagram_directive(spec)
    assert spec["backstory"] == DIAGRAM_DIRECTIVE


def test_directive_covers_the_slide_deck_contract():
    # Slides/presentations must be emitted as <section class="slide">.
    assert 'class="slide"' in DIAGRAM_DIRECTIVE
    # The rich, sized template lives in DECK_TEMPLATE.
    assert 'class="slide"' in DECK_TEMPLATE
    assert "1280px" in DECK_TEMPLATE and "720px" in DECK_TEMPLATE


def test_deck_template_added_only_for_presentation_requests():
    # A presentation request gets the rich example deck…
    deck = apply_diagram_directive({"backstory": ""}, "create a presentation on X")
    assert "SLIDE DECK DESIGN SYSTEM" in deck["backstory"]
    # …an ordinary chat turn does not (keeps the prompt lean).
    plain = apply_diagram_directive({"backstory": ""}, "what is a knowledge graph")
    assert "SLIDE DECK DESIGN SYSTEM" not in plain["backstory"]
    assert "%md-sandbox diagram specialist" in plain["backstory"]


def test_is_idempotent():
    spec = {"backstory": "base"}
    apply_diagram_directive(spec)
    once = spec["backstory"]
    apply_diagram_directive(spec)
    assert spec["backstory"] == once  # not appended twice


def test_returns_same_dict():
    spec = {"backstory": ""}
    assert apply_diagram_directive(spec) is spec
