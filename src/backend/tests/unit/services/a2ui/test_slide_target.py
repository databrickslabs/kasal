"""The configured slide count must be the ONE place deck length is set.

Before this, "Target slide count" in the UIConfigurator rendered into the phrase
"aim for about N slides" and was appended to a prompt that ALSO hardcoded
"plan 10-16 slides" in two places, while the outline pass sliced any plan to a
fixed 24. The setting therefore did nothing: the model got contradictory
instructions and the code overrode whichever won.

These tests pin the contract — the target drives both prompt sites and the
clamp — so a future edit cannot quietly reintroduce a second source of truth.
"""

import pytest

from src.services.a2ui.compose import (
    DEFAULT_SLIDE_DEPTH,
    MIN_OUTLINE_CAP,
    a2ui_system_prompt,
    load_catalog,
    outline_cap,
    slide_depth_phrase,
    slide_target,
)


def prompt_with(guidance: str) -> str:
    """The compose system prompt, built the way the runner builds it."""
    return a2ui_system_prompt(
        load_catalog(), purpose="", hint="presentation", guidance=guidance
    )


CONFIGURED = "Aim for about 50 slides; at most 4 bullet points per slide."


class TestSlideTarget:
    def test_reads_the_number_out_of_the_configured_directive(self):
        assert slide_target(CONFIGURED) == 50

    def test_is_case_insensitive(self):
        assert slide_target("aim for about 34 slides") == 34

    def test_none_when_nothing_is_configured(self):
        assert slide_target("") is None
        assert slide_target("at most 4 bullet points per slide") is None

    @pytest.mark.parametrize("n", [0, 1, 2, 201, 9999])
    def test_rejects_implausible_counts(self, n):
        """A deck of 1 is not a deck, and 9999 is a typo — honouring either would
        produce a broken deck or a very expensive compose call."""
        assert slide_target(f"aim for about {n} slides") is None


class TestDepthPhrase:
    def test_uses_the_configured_target(self):
        assert slide_depth_phrase(CONFIGURED) == "about 50"

    def test_falls_back_to_the_default_range(self):
        assert slide_depth_phrase("") == DEFAULT_SLIDE_DEPTH


class TestOutlineCap:
    def test_leaves_room_above_the_target(self):
        """A 50-slide request must not be sliced to the old fixed 24."""
        assert outline_cap(CONFIGURED) >= 50

    def test_never_drops_below_the_floor(self):
        assert outline_cap("aim for about 5 slides") == MIN_OUTLINE_CAP
        assert outline_cap("") == MIN_OUTLINE_CAP


class TestPromptCarriesOneNumber:
    def test_configured_target_replaces_the_hardcoded_range(self):
        prompt = prompt_with(CONFIGURED)
        assert "about 50 slides" in prompt
        # The old hardcoded range must not survive alongside it, or the model
        # receives two different answers to "how long is this deck?".
        assert "10-16 slides" not in prompt

    def test_default_range_is_used_when_unconfigured(self):
        prompt = prompt_with("")
        assert f"{DEFAULT_SLIDE_DEPTH} slides" in prompt
