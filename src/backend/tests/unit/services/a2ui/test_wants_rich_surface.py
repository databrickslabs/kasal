"""wants_rich_surface: A2UI composition gate, incl. yielding to md-sandbox diagrams."""

from src.services.a2ui.compose import html_owned_intent, wants_rich_surface


def test_html_owned_intent_matches_diagram_and_deck_requests():
    assert html_owned_intent("create a presentation on knowledge graphs") is True
    assert html_owned_intent("make me some slides") is True
    assert html_owned_intent("draw a diagram of the architecture") is True
    assert html_owned_intent("build a deck") is True


def test_html_owned_intent_leaves_data_surfaces_to_a2ui():
    assert html_owned_intent("build a dashboard of sales") is False
    assert html_owned_intent("show a chart of revenue") is False
    assert html_owned_intent("what is switzerland") is False


def test_rich_intent_keyword_triggers_composition():
    assert wants_rich_surface("some prose", "make me a diagram") is True


def test_table_in_body_triggers_composition():
    assert wants_rich_surface("col a | col b\n|---|---|\n1 | 2", "list it") is True


def test_plain_prose_does_not_trigger():
    assert wants_rich_surface("just a sentence.", "what is switzerland") is False


def test_yields_to_selfcontained_html_diagram_even_with_rich_intent():
    # Agent authored a %md-sandbox diagram — A2UI must NOT compose over it.
    body = "Here is the diagram:\n```html\n<div><svg></svg></div>\n```"
    assert wants_rich_surface(body, "draw me a diagram") is False


def test_yields_to_svg_diagram():
    body = "```svg\n<svg></svg>\n```"
    assert wants_rich_surface(body, "diagram please") is False


def test_ordinary_code_fence_does_not_suppress_composition():
    # A bare/other-language fence is not a diagram; a diagram request still composes.
    body = "```python\nx = 1\n```"
    assert wants_rich_surface(body, "make me a diagram") is True
