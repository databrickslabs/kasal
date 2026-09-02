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


def test_table_alone_does_not_trigger_composition():
    """A markdown table renders fine in chat. Composing a document surface from
    it re-laid-out what the reader had just watched stream in (the streamed
    text stays under the "never take away what the reader saw" rule), so an
    unrequested surface was pure duplication."""
    assert wants_rich_surface("col a | col b\n|---|---|\n1 | 2", "list it") is False
    assert (
        wants_rich_surface(
            "| GPU | Price |\n|---|---|\n| RTX 3090 | 900 |",
            "assemble a budget PC build with purchase links",
        )
        is False
    )


def test_table_with_rich_intent_still_composes():
    assert (
        wants_rich_surface("col a | col b\n|---|---|\n1 | 2", "chart these results")
        is True
    )


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


def test_html_owned_intent_ignores_incidental_mentions():
    # Substring matching used to hijack these turns away from A2UI entirely.
    assert html_owned_intent("how do slide-out panels work in the UI?") is False
    assert html_owned_intent("what's on deck for today?") is False
    assert html_owned_intent("revenue took a slide last quarter, why?") is False
    assert html_owned_intent("is the deck of the ship wood?") is False


def test_html_owned_intent_catches_deck_follow_ups():
    # Follow-up edits must STAY on the HTML path or A2UI composes over the deck.
    assert html_owned_intent("make slide 3 blue") is True
    assert html_owned_intent("change the title slide") is True
    assert html_owned_intent("add a closing slide") is True


def test_html_owned_intent_topic_phrasings():
    assert html_owned_intent("slides about the roadmap") is True
    assert html_owned_intent("a presentation on genie") is True
    assert html_owned_intent("put together a deck for the board") is True


def test_html_owned_intent_real_world_deck_phrasings():
    # The phrasings the adversarial review proved were missed.
    assert html_owned_intent("Can you turn this into a PowerPoint?") is True
    assert html_owned_intent("make me a ppt of these results") is True
    assert html_owned_intent("turn this into a deck") is True
    assert html_owned_intent("I need a pitch deck") is True
    assert html_owned_intent("give me a deck summarizing Q3 revenue") is True
    assert html_owned_intent("a slide-deck of our Q3 results please") is True
    assert html_owned_intent("summarize this on one slide") is True
    assert html_owned_intent("prepare a keynote on our product strategy") is True
    assert html_owned_intent("draw me a flowchart of the login process") is True


def test_html_owned_intent_more_follow_ups():
    assert html_owned_intent("fix the fourth slide") is True
    assert html_owned_intent("update slide #2 with the new numbers") is True
    assert html_owned_intent("go to slide two and fix the chart") is True
    assert html_owned_intent("make the slide's title bigger") is True
    assert html_owned_intent("make the presentation shorter") is True


def test_html_owned_intent_non_deck_senses_stay_on_a2ui():
    assert (
        html_owned_intent("How does the presentation layer talk to the service layer?")
        is False
    )
    assert (
        html_owned_intent("Can you improve the presentation of this data in the table?")
        is False
    )
    assert (
        html_owned_intent(
            "The side panel slides in from the left, can you make it smoother?"
        )
        is False
    )
    assert (
        html_owned_intent("Make a chart showing how revenue slides each summer")
        is False
    )
    assert (
        html_owned_intent(
            "Make a table of finished tickets and what's on deck for next sprint"
        )
        is False
    )
    assert html_owned_intent("Just explain it in words, no diagram needed") is False
