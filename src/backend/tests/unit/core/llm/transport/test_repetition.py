"""A model that stops answering and repeats itself has to be stopped.

Taken from a real incident: an agent was told to research from multiple sources
and had no tools attached, so it announced its plan, could not act on it, and
announced it again — 611 times, until it hit max_tokens at 32,768. The run did
not fail. It returned 197,336 characters, and the chat showed the first 300 as
though they were the answer.

No round cap could have caught it. `MAX_TOOL_ROUNDS`, `Agent.max_iter`,
LangGraph's `recursion_limit` and CrewAI's `max_iter` all count STEPS, and this
was one step: a single request, a single response.
"""

import pytest

from src.core.llm.transport.repetition import (
    MIN_REPEATS,
    WINDOW,
    RepetitionWatch,
    check_response,
    looping_unit,
    without_loop,
)

#: The phrase from the incident, near enough.
PHRASE = (
    "I'll research and compile a comprehensive catalog of agentic AI frameworks "
    "across open-source, commercial, and research ecosystems. Let me start by "
    "gathering authoritative framework lists and technical documentation. This "
    "may take several turns to cover the breadth needed. I'll run multiple "
    "targeted searches in parallel. "
)


class TestSpottingTheLoop:
    def test_the_incident_is_detected(self):
        looped = PHRASE * 611

        unit = looping_unit(looped)

        # A ROTATION of the phrase: the trailing window starts wherever the
        # output happened to reach, so the unit begins mid-sentence. Which
        # rotation is reported does not matter — its length is the cycle, and
        # it is genuinely present in the text, which is all the caller needs to
        # trim the loop and describe it.
        assert unit is not None
        assert len(unit) == len(PHRASE)
        assert unit in looped

    def test_a_single_repeated_character_is_detected(self):
        # The other reported shape: a model stuck emitting one token.
        assert looping_unit("!" * (WINDOW * 2)) == "!"

    def test_ordinary_prose_is_left_alone(self):
        prose = " ".join(
            f"Paragraph {i} discusses a different framework and its trade-offs."
            for i in range(400)
        )

        assert looping_unit(prose) is None

    def test_a_repeated_phrase_inside_a_real_answer_is_not_a_loop(self):
        # An answer may restate its premise several times. What is never
        # ordinary is the TAIL being nothing else — so the check looks only
        # there, and this must pass or the guard would kill real answers.
        answer = (PHRASE * 4) + " ".join(
            f"### {i}. Framework {i}\n\nIt provides X, Y and Z for use case {i}."
            for i in range(200)
        )

        assert looping_unit(answer) is None

    def test_too_few_repeats_is_not_yet_a_loop(self):
        # Deliberately conservative: killing a legitimate answer mid-generation
        # is worse than the loop it would have prevented. (The unit must have no
        # internal period of its own, or it would be a loop at a smaller cycle.)
        unit = "".join(f"part-{i} " for i in range(90))[: WINDOW // (MIN_REPEATS - 1)]

        assert looping_unit(unit * (MIN_REPEATS - 1)) is None

    def test_short_output_is_never_a_loop(self):
        assert looping_unit(PHRASE * 2) is None
        assert looping_unit("") is None

    def test_near_repetition_is_not_treated_as_exact(self):
        # Each copy differs, so this is not the failure being guarded against.
        # The failure function alone would report a period here; verifying the
        # window against the unit is what rejects it.
        drifting = "".join(f"Attempt {i}: let me try that again. " for i in range(400))

        assert looping_unit(drifting) is None


class TestWhatSurvives:
    def test_the_first_copy_is_kept(self):
        # The sentence itself was fine; saying it 611 times was not.
        kept = without_loop("Preamble. " + PHRASE * 20, PHRASE)

        assert kept == "Preamble. " + PHRASE

    def test_text_without_the_unit_is_untouched(self):
        assert without_loop("a clean answer", "zzz") == "a clean answer"

    def test_a_single_occurrence_is_untouched(self):
        text = "intro " + PHRASE
        assert without_loop(text, PHRASE) == text


class TestWatchingAStream:
    @staticmethod
    def _feed(watch, text, size=200):
        """Push text through in chunks, returning the first detection."""
        for i in range(0, len(text), size):
            unit = watch.feed(text[i : i + size])
            if unit:
                return unit, i + size
        return None, len(text)

    def test_it_fires_partway_through_rather_than_at_the_end(self):
        # The whole point: the incident ran to 197,336 characters. Detection has
        # to happen early enough that the tokens after it are never paid for.
        unit, consumed = self._feed(RepetitionWatch(), PHRASE * 611)

        assert unit == PHRASE
        assert consumed < 3 * WINDOW

    def test_a_clean_stream_never_fires(self):
        text = " ".join(f"Sentence {i} says something new." for i in range(2000))

        unit, _ = self._feed(RepetitionWatch(), text)

        assert unit is None

    def test_cost_does_not_grow_with_the_answer(self):
        # The tail is bounded, so a long answer costs the same per chunk as a
        # short one. Checking the whole accumulated text would be quadratic —
        # and on the incident that meant scanning 197KB every 1000 characters.
        watch = RepetitionWatch()
        self._feed(watch, "unique filler text. " * 5000)

        assert len(watch._tail) <= WINDOW


class TestTheCallIsStopped:
    """The detector is only useful if the transport acts on it."""

    def test_a_looping_response_raises_instead_of_returning(self):
        from src.core.llm.transport.exceptions import LLMRepetitionLoopError

        with pytest.raises(LLMRepetitionLoopError) as caught:
            check_response("test-model", "intro. " + PHRASE * 611)

        assert "repeated" in str(caught.value)

    def test_the_failure_carries_the_work_that_preceded_the_loop(self):
        from src.core.llm.transport.exceptions import LLMRepetitionLoopError

        with pytest.raises(LLMRepetitionLoopError) as caught:
            check_response("test-model", "intro. " + PHRASE * 611)

        # One copy of the repeated text survives; the other 610 do not.
        assert caught.value.partial.startswith("intro. ")
        assert len(caught.value.partial) < 2 * len(PHRASE) + len("intro. ")

    def test_it_is_a_budget_breach_so_existing_handling_applies(self):
        # Subclassing is what makes every caller that already degrades or fails
        # on a budget breach do the right thing here without being touched.
        from src.core.llm.transport.exceptions import (
            ExecutionBudgetExceededError,
            LLMRepetitionLoopError,
        )

        assert issubclass(LLMRepetitionLoopError, ExecutionBudgetExceededError)

    def test_a_normal_response_passes_through(self):
        check_response("test-model", "A short, ordinary answer.")

    def test_no_content_is_not_a_loop(self):
        check_response("test-model", None)
        check_response("test-model", "")
