"""Not paying twice for work a conversation has already done.

Turn 1 spends minutes on three crews gathering material. Turn 2 asks a follow-up
about that same material and — until this — gathered it all again. The outputs
were never lost: each crew writes its result into state under its own name, and
a conversational flow restores that state before every turn. The answers were
sitting in memory while the flow recomputed them.

What must still run is the crew that ANSWERS the turn. Reusing that one returns
turn 1's answer to turn 2's question, which is the failure the whole feature
exists to avoid.
"""

from types import SimpleNamespace
from src.services.flow_builder.conversation.reuse import (
    IDENTITY_CHANNEL,
    record_identity,
    reusable_output,
    reuse_enabled,
    crews_that_answer,
    terminal_crew_names,
)

# A linear flow: gather -> features -> compare. `compare` answers the turn.
FLOW_CONFIG = {
    "startingPoints": [
        {"crewName": "gather", "taskId": "t-gather", "tasks": [{"id": "t-gather"}]}
    ],
    "listeners": [
        {
            "crewName": "features",
            "listenToTaskIds": ["t-gather"],
            "tasks": [{"id": "t-features"}],
        },
        {
            "crewName": "compare",
            "listenToTaskIds": ["t-features"],
            "tasks": [{"id": "t-compare"}],
        },
    ],
}


def state_with(outputs, identities=None):
    """A restored state holding earlier turns' outputs."""
    data = dict(outputs)
    if identities:
        data[IDENTITY_CHANNEL] = dict(identities)
    return data


class TestWhichCrewsAnswerTheTurn:
    def test_a_crew_nothing_listens_to_is_terminal(self):
        assert terminal_crew_names(FLOW_CONFIG) == {"compare"}

    def test_upstream_crews_are_not_terminal(self):
        terminal = terminal_crew_names(FLOW_CONFIG)

        assert "gather" not in terminal
        assert "features" not in terminal

    def test_a_flow_with_no_config_has_no_terminal_crews(self):
        # Nothing known about the graph means nothing is safe to reuse; the
        # empty set makes every crew look terminal to the caller, which runs.
        assert terminal_crew_names(None) == set()


class TestReuse:
    def test_an_upstream_crew_reuses_its_earlier_answer(self):
        state = state_with({"gather": "the material"}, {"gather": "hash-1"})

        assert (
            reusable_output(state, "gather", "hash-1", terminal_crew_names(FLOW_CONFIG))
            == "the material"
        )

    def test_the_crew_that_answers_always_runs(self):
        # Reusing it would answer turn 2 with turn 1's answer.
        state = state_with({"compare": "turn 1 answer"}, {"compare": "hash-1"})

        assert (
            reusable_output(
                state, "compare", "hash-1", terminal_crew_names(FLOW_CONFIG)
            )
            is None
        )

    def test_an_edited_crew_runs_again(self):
        # Identity is a content hash of the crew's tasks, agents and model. If it
        # changed, the stored output came from a different crew and replaying it
        # would silently ignore the edit.
        state = state_with({"gather": "old"}, {"gather": "hash-1"})

        assert reusable_output(state, "gather", "hash-2", set()) is None

    def test_an_output_with_no_recorded_identity_runs_again(self):
        # Written before identities were recorded — its provenance cannot be
        # checked, so it is not trusted.
        state = state_with({"gather": "old"})

        assert reusable_output(state, "gather", "hash-1", set()) is None

    def test_nothing_stored_means_run(self):
        assert reusable_output({}, "gather", "hash-1", set()) is None

    def test_a_refresh_ignores_everything_stored(self):
        # Some follow-up genuinely means "go and look again", and no rule read
        # from state can tell that turn from the others.
        state = state_with({"gather": "old"}, {"gather": "hash-1"})

        assert reusable_output(state, "gather", "hash-1", set(), refresh=True) is None

    def test_no_state_at_all_means_run(self):
        assert reusable_output(None, "gather", "hash-1", set()) is None


class TestWhenReuseApplies:
    def test_only_a_conversational_flow_reuses(self):
        # A one-shot run has no earlier turn to reuse from, and a resumed run
        # has its own skip machinery.
        assert reuse_enabled({"conversational": True}) is True
        assert reuse_enabled({"enabled": True}) is False
        assert reuse_enabled(None) is False

    def test_a_refresh_turns_it_off_wholesale(self):
        assert reuse_enabled({"conversational": True}, refresh=True) is False


class TestIdentityBookkeeping:
    def test_an_identity_is_remembered_beside_the_output(self):
        state = {}

        record_identity(state, "gather", "hash-1")

        assert state[IDENTITY_CHANNEL] == {"gather": "hash-1"}

    def test_recording_one_does_not_lose_the_others(self):
        state = state_with({}, {"gather": "hash-1"})

        record_identity(state, "features", "hash-2")

        assert state[IDENTITY_CHANNEL] == {"gather": "hash-1", "features": "hash-2"}

    def test_a_missing_identity_is_not_recorded(self):
        state = {}

        record_identity(state, "gather", None)

        assert IDENTITY_CHANNEL not in state


class TestTheIdentityChannelSurvivesBeingSaved:
    """Bookkeeping that does not persist is bookkeeping that does not exist.

    Typed flow state is a pydantic model, and pydantic v2 treats a name starting
    with an underscore as a PRIVATE attribute: assigning it succeeds, reading it
    back with getattr succeeds, and `model_dump()` — which is what state is
    persisted from — silently omits it.

    The channel was called `__crew_identities`. So every turn recorded its
    identities, used them correctly in-process, and lost them the instant the
    turn ended. The next turn found outputs with no provenance, refused to trust
    them, and re-ran every crew. Reuse could not fire once, and nothing anywhere
    reported a failure.
    """

    @staticmethod
    def _typed_state():
        from src.services.flow_builder.conversation.state_model import build_state_model
        from src.services.flow_builder.conversation.turn import ConversationState

        return build_state_model({}, "TestState", ConversationState)()

    def test_the_channel_name_cannot_start_with_an_underscore(self):
        # The whole defect in one assertion.
        assert not IDENTITY_CHANNEL.startswith("_")

    def test_a_recorded_identity_is_in_the_dump_that_gets_persisted(self):
        state = self._typed_state()

        record_identity(state, "gather", "hash-1")

        assert state.model_dump().get(IDENTITY_CHANNEL) == {"gather": "hash-1"}

    def test_an_output_is_reusable_after_a_save_and_restore(self):
        # The end-to-end property that matters: turn 1 stores, the state is
        # written as JSON and read back, and turn 2 reuses. Testing the record
        # and the check in one process passes even when the channel never
        # survives the trip between them.
        import json

        state = self._typed_state()
        state["gather"] = "the material"
        record_identity(state, "gather", "hash-1")

        restored = json.loads(json.dumps(state.model_dump(), default=str))

        assert reusable_output(restored, "gather", "hash-1", set()) == "the material"

    def test_an_edited_crew_still_re_runs_after_a_restore(self):
        import json

        state = self._typed_state()
        state["gather"] = "the material"
        record_identity(state, "gather", "hash-1")

        restored = json.loads(json.dumps(state.model_dump(), default=str))

        assert reusable_output(restored, "gather", "hash-2", set()) is None


class TestRestoringATurnBringsBackTheWork:
    """A restore that returns the conversation but not the work is not a restore.

    Typed state is a pydantic model, and `_restore_state` used to copy only keys
    the state already had (`hasattr`). A freshly constructed state has the
    DECLARED fields — id, messages, last_user_message, last_outcome — and none
    of the channels an earlier turn created: each crew's output under its own
    name, and the identity bookkeeping beside it.

    So turn 2 came back knowing what had been said and nothing about what had
    been done. It looked like a working restore, and every crew ran again with
    its answer already sitting in the row that had just been read.
    """

    @staticmethod
    def _flow_class(state_model, ran):
        from src.services.flow_builder.runtime import Flow, listen, start

        class _F(Flow):
            initial_state = state_model

            @start()
            def gather(self):
                ran.append("gather")
                return "fresh material"

            @listen("gather")
            def answer(self, previous):
                ran.append("answer")
                return "the answer"

        return _F

    @staticmethod
    def _state_model():
        from src.services.flow_builder.conversation.state_model import (
            build_state_model,
        )
        from src.services.flow_builder.conversation.turn import ConversationState

        return build_state_model({}, "ThreadState", ConversationState)

    def _restore_into(self, stored):
        """Run `_restore_state` against a real typed state and return it."""
        from unittest.mock import MagicMock

        flow = self._flow_class(self._state_model(), [])()
        flow._persistence = MagicMock()
        flow._persistence.load_state.return_value = stored
        flow._restore_state("thread-1")
        return flow.state

    def test_a_declared_channel_comes_back(self):
        # This always worked — and passing on its own is what made the drop of
        # everything else look like a working restore.
        state = self._restore_into({"last_user_message": "compare them"})

        assert state.last_user_message == "compare them"

    def test_a_crews_stored_output_comes_back(self):
        state = self._restore_into({"agentic ai frameworks": "the material"})

        assert state["agentic ai frameworks"] == "the material"

    def test_the_identity_bookkeeping_comes_back(self):
        state = self._restore_into({IDENTITY_CHANNEL: {"gather": "hash-1"}})

        assert state[IDENTITY_CHANNEL] == {"gather": "hash-1"}

    def test_a_restored_output_is_reusable(self):
        # The property the whole feature rests on, across the restore boundary.
        state = self._restore_into(
            {"gather": "the material", IDENTITY_CHANNEL: {"gather": "hash-1"}}
        )

        assert reusable_output(state, "gather", "hash-1", set()) == "the material"

    def test_an_edited_crew_is_still_refused_after_a_restore(self):
        state = self._restore_into(
            {"gather": "the material", IDENTITY_CHANNEL: {"gather": "hash-1"}}
        )

        assert reusable_output(state, "gather", "hash-2", set()) is None


class TestTheSelectedOutcomeAlwaysRuns:
    """The turn's target must never be reused, terminal or not.

    Observed: a conversational flow answered turn 1 and then answered NOTHING,
    for every turn after, while each trace showed a completed run. From the log:

        [flow-outcome] this turn produces 'Agentic AI Frameworks' (confidence 0.95)
        ♻️  Reusing 'Agentic AI Frameworks' output from an earlier turn — not running it again

    Selection chose the right crew and reuse skipped it, so nothing executed —
    one LLM call in the whole run, and that was the surface composer. The chat
    received two activity cards and no answer; the result in the database was
    turn 1's, carried forward.

    Cause: that crew fed four listeners, so `terminal_crews` called it MATERIAL,
    and material is reusable. Both features were satisfied by their own rule and
    the turn ran nothing. A caller may legitimately ask for an intermediate
    artefact, which is exactly what had happened.
    """

    def _state(self, **stored):
        state = dict(stored)
        state[IDENTITY_CHANNEL] = {name: "hash-1" for name in stored}
        return state

    def _flow(self, outcome=None, stale_state_outcome="Agentic AI Frameworks"):
        """A flow carrying this turn's selection, over a STALE state channel.

        The staleness is the point: `_plan_turn` writes `last_outcome` before
        kickoff, and kickoff restores the checkpoint over the state — so the
        channel holds the PREVIOUS turn's outcome for the whole of this turn.
        Reading the selection from there protected the wrong crew.
        """
        return SimpleNamespace(
            _kasal_selected_outcome=outcome,
            state={"last_outcome": stale_state_outcome},
        )

    def test_the_selected_crew_is_not_reused_even_when_upstream(self):
        state = self._state(gather="turn 1's list")

        assert (
            reusable_output(
                state,
                "gather",
                "hash-1",
                crews_that_answer(FLOW_CONFIG, self._flow(outcome="gather")),
            )
            is None
        )

    def test_material_the_turn_did_not_select_is_still_reused(self):
        """The feature keeps working — this is a narrowing of reuse, not a
        removal of it.

        Note the flow's STATE still says 'Agentic AI Frameworks' (the previous
        turn's outcome, restored from the checkpoint). Reading that instead of
        the flow's own selection is what re-ran this crew for real: the turn
        asked for websites and the upstream research ran again, Perplexity call
        and all."""
        state = self._state(gather="turn 1's list")

        assert (
            reusable_output(
                state,
                "gather",
                "hash-1",
                crews_that_answer(FLOW_CONFIG, self._flow(outcome="compare")),
            )
            == "turn 1's list"
        )

    def test_terminal_crews_stay_protected_with_no_selection(self):
        """A turn that declined to narrow keeps the original guarantee."""
        assert crews_that_answer(FLOW_CONFIG, self._flow(outcome=None)) == {"compare"}

    def test_the_selection_is_added_to_the_terminal_set(self):
        assert crews_that_answer(FLOW_CONFIG, self._flow(outcome="gather")) == {
            "compare",
            "gather",
        }

    def test_a_stale_state_channel_is_never_consulted(self):
        """The regression this file now guards: state['last_outcome'] holds the
        PREVIOUS turn's value, and trusting it protected the wrong crew."""
        flow = self._flow(outcome="compare", stale_state_outcome="gather")

        assert crews_that_answer(FLOW_CONFIG, flow) == {"compare"}

    def test_an_empty_selection_changes_nothing(self):
        assert crews_that_answer(FLOW_CONFIG, self._flow(outcome="")) == {"compare"}

    def test_a_flow_without_the_attribute_is_not_an_error(self):
        """A non-conversational flow never narrows and carries no selection."""
        assert crews_that_answer(FLOW_CONFIG, SimpleNamespace()) == {"compare"}
