"""The design-polish pass is optional, so it is bounded by a clock.

`presentation_design_lint` fires on a deck that is already VALID and already
shippable — the composer even keeps it as `best` and ships it if the retry
fails. Acting on those findings costs a SECOND full generation: the entire deck
regenerated, with the previous deck appended to the prompt.

Measured on a real run, that cost 63s (49s of it before the first token, all
prefill) on top of an 18s answer. These tests pin the rule that came out of it —
polish only while it is still cheap relative to what the reader has waited —
and, just as importantly, that correctness retries are NOT bounded by it.
"""

import json

import pytest

from src.services.a2ui.compose import (
    _design_retry_budget_s,
    compose_a2ui,
    presentation_design_lint,
)

QUERY = "make me a presentation about alpine chalets"


#: Valid, every body slide has a real body — so `presentation_needs_body` is
#: satisfied — but no Chart, no Diagram, no stats slide. That is exactly the
#: shape the design lint complains about.
#:
#: Seven slides, not two: `presentation_design_lint` exempts decks under six
#: slides outright ("short decks are fine text-heavy"), so a small fixture would
#: produce no findings and every test below would pass without the polish pass
#: ever running.
def _flat_deck(body_slides: int = 6):
    components = [
        {
            "id": "deck",
            "component": "SlideDeck",
            "children": ["s0"] + [f"s{i}" for i in range(1, body_slides + 1)],
        },
        {"id": "s0", "component": "Slide", "variant": "title", "title": "Alps"},
    ]
    for i in range(1, body_slides + 1):
        components += [
            {
                "id": f"s{i}",
                "component": "Slide",
                "variant": "content",
                "title": f"Chalets {i}",
                "children": [f"md{i}"],
            },
            {
                "id": f"md{i}",
                "component": "Markdown",
                "content": (
                    "- Timber frames carry the roof load\n"
                    "- Deep eaves shed snow clear of the walls\n"
                    "- Stone footings keep the sill beams dry\n"
                    "- South-facing balconies catch the winter sun"
                ),
            },
        ]
    return {
        "surfaceKind": "presentation",
        "root": "deck",
        "components": components,
        "dataModel": {},
    }


FLAT_DECK = _flat_deck()

CATALOG = {
    "components": {
        "SlideDeck": {"summary": "deck"},
        "Slide": {"summary": "slide"},
        "Markdown": {"summary": "md"},
    },
    "surfaceKinds": ["presentation", "conversation"],
}


class CountingLLM:
    """Returns the same flat deck however often it is asked."""

    def __init__(self, reply=None):
        self.reply = json.dumps(reply if reply is not None else FLAT_DECK)
        self.calls = 0

    def __call__(self, messages):
        self.calls += 1
        return self.reply


@pytest.fixture(autouse=True)
def _no_outline_prepass(monkeypatch):
    """Isolate the retry: the outline pre-pass is its own LLM call."""
    monkeypatch.setenv("A2UI_PRESENTATION_OUTLINE", "0")


def _compose(llm, text="Alpine chalets are timber-framed."):
    return compose_a2ui(text, query=QUERY, llm_call=llm, catalog=CATALOG, retries=2)


class TestTheFixtureActuallyTripsTheLint:
    """Without this, the budget tests would pass for the wrong reason."""

    def test_the_flat_deck_has_findings(self):
        assert presentation_design_lint(FLAT_DECK, answer_has_sources=False)


class TestTheBudgetBoundsThePolishPass:
    def test_a_zero_budget_ships_the_valid_deck_on_the_first_call(self, monkeypatch):
        monkeypatch.setenv("A2UI_DESIGN_RETRY_BUDGET_S", "0")
        llm = CountingLLM()
        out = _compose(llm)
        assert llm.calls == 1
        assert out["surfaceKind"] == "presentation"

    def test_a_generous_budget_still_polishes(self, monkeypatch):
        """The budget must not become a silent removal of the feature: under it,
        behaviour is exactly what it was."""
        monkeypatch.setenv("A2UI_DESIGN_RETRY_BUDGET_S", "600")
        llm = CountingLLM()
        _compose(llm)
        assert llm.calls == 2

    def test_the_skipped_deck_is_the_valid_one_not_a_markdown_fallback(
        self, monkeypatch
    ):
        """Skipping polish must not degrade to prose — that would trade 60s of
        latency for the whole deck."""
        monkeypatch.setenv("A2UI_DESIGN_RETRY_BUDGET_S", "0")
        out = _compose(CountingLLM())
        assert [c["component"] for c in out["components"]][0] == "SlideDeck"


class TestCorrectnessRetriesAreNotBounded:
    def test_an_invalid_surface_still_retries_past_the_budget(self, monkeypatch):
        """A budget that also gagged correctness retries would ship broken decks
        under load — the failure mode this must not have."""
        monkeypatch.setenv("A2UI_DESIGN_RETRY_BUDGET_S", "0")

        class Rubbish(CountingLLM):
            def __call__(self, messages):
                self.calls += 1
                return "not json at all"

        llm = Rubbish()
        _compose(llm)
        assert llm.calls == 2


class TestTheKnobIsHardToMisconfigure:
    """A malformed environment variable must not be able to break composition."""

    def test_unset_uses_the_default(self, monkeypatch):
        monkeypatch.delenv("A2UI_DESIGN_RETRY_BUDGET_S", raising=False)
        assert _design_retry_budget_s() == 25.0

    @pytest.mark.parametrize("raw", ["", "   ", "banana", "-5"])
    def test_nonsense_falls_back_rather_than_raising(self, monkeypatch, raw):
        monkeypatch.setenv("A2UI_DESIGN_RETRY_BUDGET_S", raw)
        assert _design_retry_budget_s() == 25.0

    def test_a_real_value_is_honoured(self, monkeypatch):
        monkeypatch.setenv("A2UI_DESIGN_RETRY_BUDGET_S", "12.5")
        assert _design_retry_budget_s() == 12.5
