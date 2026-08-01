"""What a run passes into flow state when the run belongs to a conversation.

This is the wiring between an execution request and the thread: the lineage id
the turn restores, and — for a conversational flow — the user's line, written
through the same inputs path everything else uses so it merges through the
`messages` reducer instead of replacing the history.
"""

from src.services.flow_builder.backend_flow import BackendFlow
from src.services.flow_builder.conversation.thread import thread_state_uuid

CONVERSATIONAL = {"flow_config": {"state": {"enabled": True, "conversational": True}}}
ONE_SHOT = {"flow_config": {"state": {"enabled": True}}}


def backend_flow(config, flow_data=None):
    flow = BackendFlow(job_id="job-1")
    flow.config = config
    flow._flow_data = flow_data
    return flow


class TestThreadId:
    def test_a_session_and_flow_derive_the_lineage(self):
        flow = backend_flow(
            {**ONE_SHOT, "session_id": "s1", "flow_id": "f1", "group_id": "g1"}
        )

        assert flow._kickoff_inputs()["id"] == thread_state_uuid("s1", "f1", "g1")

    def test_an_explicit_resume_wins_over_the_derived_lineage(self):
        # The caller named a lineage and must get that one — resuming a specific
        # checkpoint is not the same request as continuing a conversation.
        flow = backend_flow(
            {
                **ONE_SHOT,
                "session_id": "s1",
                "flow_id": "f1",
                "resume_from_flow_uuid": "explicit-uuid",
            }
        )

        assert flow._kickoff_inputs()["id"] == "explicit-uuid"

    def test_no_session_means_a_one_shot_run(self):
        # Which is every flow today: no id, so the state keeps the fresh uuid4
        # it was built with and nothing is continued.
        flow = backend_flow({**ONE_SHOT, "flow_id": "f1"})

        assert "id" not in flow._kickoff_inputs()

    def test_the_lineage_is_stable_across_turns(self):
        config = {**CONVERSATIONAL, "session_id": "s1", "flow_id": "f1"}

        first = backend_flow(dict(config))._kickoff_inputs()["id"]
        second = backend_flow(dict(config))._kickoff_inputs()["id"]

        assert first == second


class TestTurnInputs:
    def test_a_conversational_run_carries_the_user_line(self):
        flow = backend_flow(
            {
                **CONVERSATIONAL,
                "session_id": "s1",
                "flow_id": "f1",
                "user_message": "hi",
            }
        )

        inputs = flow._kickoff_inputs()

        assert inputs["last_user_message"] == "hi"
        assert inputs["messages"] == [{"role": "user", "content": "hi"}]

    def test_a_one_shot_run_carries_no_conversation_channels(self):
        # A non-conversational state has no `messages` channel, and merging one
        # in would raise on a channel the flow never declared.
        flow = backend_flow(
            {**ONE_SHOT, "session_id": "s1", "flow_id": "f1", "user_message": "hi"}
        )

        inputs = flow._kickoff_inputs()

        assert "messages" not in inputs
        assert "last_user_message" not in inputs

    def test_the_user_line_may_arrive_in_inputs(self):
        # Callers that already pack everything into `inputs` should not need a
        # second mechanism; it is consumed rather than passed through as a
        # stray state key.
        flow = backend_flow(
            {
                **CONVERSATIONAL,
                "session_id": "s1",
                "flow_id": "f1",
                "inputs": {"user_message": "from inputs", "topic": "news"},
            }
        )

        inputs = flow._kickoff_inputs()

        assert inputs["last_user_message"] == "from inputs"
        assert "user_message" not in inputs
        assert inputs["topic"] == "news"

    def test_declared_inputs_survive_alongside_the_turn(self):
        flow = backend_flow(
            {
                **CONVERSATIONAL,
                "session_id": "s1",
                "flow_id": "f1",
                "user_message": "hi",
                "inputs": {"topic": "news"},
            }
        )

        assert flow._kickoff_inputs()["topic"] == "news"

    def test_an_intent_is_passed_when_the_caller_classified_one(self):
        flow = backend_flow(
            {
                **CONVERSATIONAL,
                "session_id": "s1",
                "flow_id": "f1",
                "user_message": "hi",
                "intent": "greeting",
            }
        )

        assert flow._kickoff_inputs()["last_intent"] == "greeting"


class TestStateConfigSource:
    def test_a_saved_flow_carries_its_config_on_flow_data(self):
        # `load_flow` puts it there; reading only `_config` would make a saved
        # conversational flow behave as a one-shot.
        flow = backend_flow(
            {"session_id": "s1", "flow_id": "f1", "user_message": "hi"},
            flow_data=CONVERSATIONAL,
        )

        assert flow._kickoff_inputs()["last_user_message"] == "hi"

    def test_an_unsaved_flow_carries_it_on_the_run_config(self):
        flow = backend_flow(
            {
                **CONVERSATIONAL,
                "session_id": "s1",
                "flow_id": "f1",
                "user_message": "hi",
            }
        )

        assert flow._kickoff_inputs()["last_user_message"] == "hi"


class TestRunMetadataIsNotState:
    """The UI puts run bookkeeping in the same dict as the user's inputs.

    `flow_id` and `run_name` describe the REQUEST, not the flow. On an untyped
    dict state they were harmless strays nobody read; a typed state refuses a
    key it has no channel for — the behaviour that makes a misspelled input
    visible — so every conversational flow failed its kickoff with
    `Flow state has no channel(s) ['flow_id', 'run_name']`.
    """

    @staticmethod
    def _flow(config):
        flow = BackendFlow(job_id="job-1")
        flow.config = config
        flow._flow_data = None
        return flow

    def test_run_metadata_never_reaches_state(self):
        flow = self._flow(
            {
                **CONVERSATIONAL,
                "session_id": "s1",
                "flow_id": "f1",
                "user_message": "swiss news",
                "inputs": {"flow_id": "f1", "run_name": "Some Run", "topic": "news"},
            }
        )

        inputs = flow._kickoff_inputs()

        assert "flow_id" not in inputs
        assert "run_name" not in inputs

    def test_the_user_s_own_inputs_still_arrive(self):
        # The filter is a named list, not a heuristic: a real input must not be
        # dropped because it sits beside metadata.
        flow = self._flow(
            {
                **ONE_SHOT,
                "session_id": "s1",
                "flow_id": "f1",
                "inputs": {"flow_id": "f1", "run_name": "R", "topic": "news"},
            }
        )

        assert flow._kickoff_inputs()["topic"] == "news"

    def test_the_lineage_id_is_still_passed(self):
        # `flow_id` is filtered as an INPUT while still being read from the
        # config to derive the thread — the two uses must not be confused.
        flow = self._flow(
            {
                **CONVERSATIONAL,
                "session_id": "s1",
                "flow_id": "f1",
                "inputs": {"flow_id": "f1"},
            }
        )

        assert flow._kickoff_inputs()["id"] == thread_state_uuid("s1", "f1", None)
