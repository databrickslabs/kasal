"""Which crew a turn needs, and the least work that produces it.

Two decisions in a flow both get called routing, and they are not the same. A
ROUTER asks "given what the flow computed, which branch is valid?" — it reads
state, during execution. An OUTCOME answers "given what the person asked, what
must this turn produce?" — it reads the turn, before execution. Nothing here
touches routers.

Every failure declines and runs the whole flow. Slow and correct beats fast and
answering a question nobody asked.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.services.flow_builder.conversation.outcomes import (
    build_registry,
    identity_of,
    outcome_crews,
    outcome_descriptions,
    parse_outcome,
    render_recent,
    select_outcome,
    terminal_crews,
    trigger_for,
)
from src.services.flow_builder.runtime import Flow, listen, start

# gather -> features -> compare, plus quiz hanging off gather.
FLOW = {
    "startingPoints": [{"crewName": "gather", "taskId": "t1", "tasks": [{"id": "t1"}]}],
    "listeners": [
        {"crewName": "features", "listenToTaskIds": ["t1"], "tasks": [{"id": "t2"}]},
        {"crewName": "compare", "listenToTaskIds": ["t2"], "tasks": [{"id": "t3"}]},
        {"crewName": "quiz", "listenToTaskIds": ["t1"], "tasks": [{"id": "t4"}]},
    ],
}


class TestWhatATurnMayAskFor:
    def test_crews_nothing_listens_to_are_the_default(self):
        assert outcome_crews(FLOW) == {"compare", "quiz"}

    def test_a_written_description_makes_a_crew_askable(self):
        # Writing what a crew delivers IS marking it askable. A separate flag
        # would be a second thing to remember that says nothing more.
        described = {**FLOW, "outcomes": {"gather": "the raw material"}}

        assert outcome_crews(described) == {"gather"}

    def test_a_blank_description_marks_nothing(self):
        described = {**FLOW, "outcomes": {"gather": "   "}}

        assert outcome_crews(described) == {"compare", "quiz"}

    def test_terminal_is_one_definition_shared_with_reuse(self):
        # Two implementations would eventually disagree, and the day they do a
        # crew is both "the answer to this turn" and "safe to reuse" — so the
        # turn returns the previous answer.
        from src.services.flow_builder.conversation.reuse import terminal_crew_names

        assert terminal_crews(FLOW) == terminal_crew_names(FLOW)


class TestDescriptions:
    def test_the_authored_line_wins(self):
        described = {**FLOW, "outcomes": {"compare": "a ranked comparison table"}}

        assert outcome_descriptions(described)["compare"] == "a ranked comparison table"

    def test_it_falls_back_to_what_the_task_expects_to_produce(self):
        # `expected_output` says what comes OUT; `description` says how the work
        # is done. For telling outcomes apart the former is the signal.
        flow = {
            "listeners": [
                {
                    "crewName": "compare",
                    "tasks": [
                        {
                            "name": "Compare",
                            "description": "Analyze and rank the frameworks…",
                            "expected_output": "A feature comparison matrix",
                        }
                    ],
                }
            ]
        }

        assert "comparison matrix" in outcome_descriptions(flow)["compare"]


class TestParsing:
    def test_a_named_outcome_is_taken(self):
        choice = parse_outcome(
            '{"outcome":"compare","confidence":0.9,"reason":"asked to compare"}',
            {"compare", "quiz"},
        )

        assert (choice.outcome, choice.confidence) == ("compare", 0.9)

    def test_an_outcome_the_flow_does_not_have_is_discarded(self):
        # Trusting it would narrow the graph to nothing and the turn would
        # produce no answer at all.
        assert (
            parse_outcome(
                '{"outcome":"invented","confidence":0.99}', {"compare"}
            ).outcome
            is None
        )

    def test_low_confidence_declines(self):
        assert (
            parse_outcome('{"outcome":"compare","confidence":0.2}', {"compare"}).outcome
            is None
        )

    def test_an_unusable_response_declines(self):
        assert parse_outcome("not json", {"compare"}).outcome is None
        assert parse_outcome('{"outcome":null}', {"compare"}).outcome is None


class TestTheConversationIsPartOfTheTurn:
    def test_recent_turns_are_rendered(self):
        # "and for Germany?" names no outcome on its own; what came before is
        # the difference between narrowing it and running everything.
        rendered = render_recent(
            [
                {"role": "user", "content": "compare the frameworks"},
                {"role": "assistant", "content": "here is the table"},
            ]
        )

        assert "compare the frameworks" in rendered

    def test_no_conversation_renders_nothing(self):
        assert render_recent(None) == ""
        assert render_recent([]) == ""


class TestSelection:
    def test_it_declines_when_there_is_nothing_to_narrow(self):
        one_outcome = {"listeners": [{"crewName": "only", "tasks": []}]}

        assert asyncio.run(select_outcome("anything", one_outcome)).outcome is None

    def test_a_model_outage_declines_rather_than_guessing(self):
        with (
            patch(
                "src.services.catalog.templates.TemplateService.get_effective_template_content",
                new=AsyncMock(return_value="SYSTEM"),
            ),
            patch(
                "src.services.llm.manager.LLMManager.completion",
                new=AsyncMock(side_effect=RuntimeError("down")),
            ),
        ):
            choice = asyncio.run(select_outcome("make a quiz", FLOW))

        assert choice.outcome is None
        assert "unavailable" in choice.reason


class TestRegistry:
    def test_an_outcome_triggers_one_method(self):
        registry = build_registry(
            {"listener_0": "compare", "starting_point_0": "gather"}, {"compare": "h1"}
        )

        assert trigger_for(registry, "compare") == "listener_0"

    def test_it_carries_the_crew_the_outcome_was_described_against(self):
        # A name is stable while everything behind it changes. The hash is what
        # makes a stored answer safe to replay.
        registry = build_registry({"listener_0": "compare"}, {"compare": "h1"})

        assert identity_of(registry, "compare") == "h1"

    def test_an_unknown_outcome_triggers_nothing(self):
        assert trigger_for(build_registry({}, {}), "compare") is None


class TestNarrowing:
    """The runtime half: only what produces the target may run."""

    @staticmethod
    def _flow_class(ran):
        class _F(Flow):
            @start()
            def gather(self):
                ran.append("gather")
                return "material"

            @listen("gather")
            def features(self, previous):
                ran.append("features")
                return "f"

            @listen("features")
            def compare(self, previous):
                ran.append("compare")
                return "c"

            @listen("gather")
            def quiz(self, previous):
                ran.append("quiz")
                return "q"

        return _F

    def test_everything_runs_when_no_outcome_was_chosen(self):
        ran = []
        asyncio.run(self._flow_class(ran)().kickoff_async())

        assert set(ran) == {"gather", "features", "compare", "quiz"}

    def test_only_the_target_and_its_ancestors_run(self):
        ran = []
        flow = self._flow_class(ran)()
        flow.narrow_to({"compare"})
        asyncio.run(flow.kickoff_async())

        assert ran == ["gather", "features", "compare"]

    def test_a_sibling_branch_does_not_run(self):
        ran = []
        flow = self._flow_class(ran)()
        flow.narrow_to({"quiz"})
        asyncio.run(flow.kickoff_async())

        assert "features" not in ran and "compare" not in ran

    def test_downstream_of_the_target_does_not_run(self):
        # Asking for a parent must not drag its children along: they produce
        # things the turn did not ask for.
        ran = []
        flow = self._flow_class(ran)()
        flow.narrow_to({"gather"})
        asyncio.run(flow.kickoff_async())

        assert ran == ["gather"]

    def test_an_unreachable_target_does_not_narrow(self):
        # Far more likely a mistake in the selection than a flow with no path to
        # its own crew — and running everything is the safe reading.
        ran = []
        flow = self._flow_class(ran)()

        assert flow.narrow_to({"nonexistent"}) is False
        asyncio.run(flow.kickoff_async())
        assert len(ran) == 4

    def test_a_new_turn_clears_the_narrowing(self):
        # Otherwise turn 2 would inherit turn 1's target and quietly produce the
        # wrong thing.
        ran = []
        flow = self._flow_class(ran)()
        flow.narrow_to({"quiz"})
        asyncio.run(flow.kickoff_async())
        ran.clear()

        asyncio.run(flow.kickoff_async())

        assert set(ran) == {"gather", "features", "compare", "quiz"}


class TestATurnThatNeedsNoWork:
    """The material is already in state; retelling it should cost nothing.

    Turn 1 gathered the frameworks. Turn 2 asks which ones were found. Running a
    crew to answer that spends minutes reproducing something in memory — and
    produces a worse answer, because a fresh run gathers again and may not find
    the same things.
    """

    def test_retrieval_is_recognised(self):
        choice = parse_outcome(
            '{"outcome":null,"answer_from_state":true,"confidence":0.9,'
            '"reason":"asks about what was already produced"}',
            {"compare", "quiz"},
        )

        assert choice.answer_from_state is True
        assert choice.outcome is None

    def test_retrieval_needs_confidence_too(self):
        # An unsure "probably already answered" must not silently skip the work.
        choice = parse_outcome(
            '{"outcome":null,"answer_from_state":true,"confidence":0.2}',
            {"compare"},
        )

        assert choice.answer_from_state is False

    def test_new_work_is_not_retrieval(self):
        choice = parse_outcome(
            '{"outcome":"quiz","answer_from_state":false,"confidence":0.9}',
            {"compare", "quiz"},
        )

        assert choice.answer_from_state is False
        assert choice.outcome == "quiz"


class TestMaterialForAnswering:
    def test_bookkeeping_channels_are_not_material(self):
        # A question is about the WORK. Putting the conversation and the
        # identity hashes in front of the model spends the budget that should
        # go to the material.
        from src.services.flow_builder.conversation.retrieval import material_from_state
        from src.services.flow_builder.conversation.reuse import IDENTITY_CHANNEL

        material = material_from_state(
            {
                "id": "thread-1",
                "messages": [{"role": "user", "content": "hi"}],
                "last_user_message": "hi",
                IDENTITY_CHANNEL: {"gather": "h1"},
                "agentic ai frameworks": "LangChain, LlamaIndex, AutoGen",
            }
        )

        assert list(material) == ["agentic ai frameworks"]

    def test_empty_outputs_are_skipped(self):
        from src.services.flow_builder.conversation.retrieval import material_from_state

        assert material_from_state({"gather": "", "compare": None}) == {}

    def test_no_material_means_it_cannot_be_answered(self):
        # Falls back to running the flow rather than answering from nothing.
        from src.services.flow_builder.conversation.retrieval import answer_from_state

        assert asyncio.run(answer_from_state("what did you find?", {})) is None

    def test_a_model_outage_falls_back_to_running(self):
        from src.services.flow_builder.conversation.retrieval import answer_from_state

        with patch(
            "src.services.llm.manager.LLMManager.completion",
            new=AsyncMock(side_effect=RuntimeError("down")),
        ):
            answer = asyncio.run(
                answer_from_state("what did you find?", {"gather": "frameworks"})
            )

        assert answer is None

    def test_an_answer_is_written_from_the_material(self):
        from src.services.flow_builder.conversation.retrieval import answer_from_state

        with patch(
            "src.services.llm.manager.LLMManager.completion",
            new=AsyncMock(
                return_value={
                    "choices": [
                        {"message": {"content": "LangChain, LlamaIndex and AutoGen."}}
                    ]
                }
            ),
        ):
            answer = asyncio.run(
                answer_from_state(
                    "which frameworks did you find?",
                    {"agentic ai frameworks": "LangChain, LlamaIndex, AutoGen"},
                )
            )

        assert answer == "LangChain, LlamaIndex and AutoGen."
