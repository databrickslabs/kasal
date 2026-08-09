"""A flow can be given a value — the five drops that used to swallow it.

They were in SERIES: the frontend hardcoded `inputs: {}`, no variable gate ever
ran, the kickoff call sites passed the checkpoint id INSTEAD of the inputs, and
the engine dropped any key its state had no field for without a word. Fixing any
four changed nothing observable, which is why they are tested together.

Two of the five are here (the backend ones). The frontend's `buildFlowConfig`
and the variable gate are covered by the vitest suite.
"""

from types import SimpleNamespace

import pytest

from src.services.flow_builder.backend_flow import BackendFlow
from src.services.flow_builder.modules.flow_methods import crew_inputs_from_state
from src.services.flow_builder.runtime.flow import Flow


class TestKickoffInputs:
    """The two call sites used to pass ``{"id": uuid}`` or nothing at all."""

    @staticmethod
    def _flow(config):
        flow = BackendFlow.__new__(BackendFlow)
        flow._config = config
        return flow

    def test_a_normal_run_passes_its_inputs(self):
        assert self._flow({"inputs": {"region": "DACH"}})._kickoff_inputs() == {
            "region": "DACH"
        }

    def test_a_resume_keeps_the_inputs_AND_the_checkpoint_id(self):
        # The old code passed one or the other, so resuming a flow silently
        # discarded every input it was given.
        assert self._flow(
            {"inputs": {"region": "DACH"}, "resume_from_flow_uuid": "abc"}
        )._kickoff_inputs() == {"region": "DACH", "id": "abc"}

    def test_the_checkpoint_id_wins_a_collision(self):
        # `id` addresses the checkpoint. A flow whose own variable is called
        # `id` must not be able to redirect a restore.
        assert self._flow(
            {"inputs": {"id": "not-the-checkpoint"}, "resume_from_flow_uuid": "abc"}
        )._kickoff_inputs() == {"id": "abc"}

    def test_nothing_to_pass_stays_nothing(self):
        assert self._flow({})._kickoff_inputs() == {}
        assert self._flow({"inputs": None})._kickoff_inputs() == {}


class _TypedState:
    def __init__(self):
        self.id = ""
        self.region = ""


class TestMergeInputs:
    @staticmethod
    def _flow(state):
        flow = Flow.__new__(Flow)
        flow._state = state
        return flow

    def test_a_dict_state_takes_anything(self):
        state = {"id": "x"}
        self._flow(state)._merge_inputs({"region": "DACH"})
        assert state["region"] == "DACH"

    def test_a_typed_state_takes_the_fields_it_has(self):
        state = _TypedState()
        self._flow(state)._merge_inputs({"region": "DACH"})
        assert state.region == "DACH"

    def test_a_key_the_state_cannot_hold_RAISES(self):
        # It used to be skipped in silence. With evaluate_condition also
        # swallowing its errors, one typo meant the value vanished, the
        # condition reading it went False, the flow took the wrong branch, and
        # the run reported success.
        with pytest.raises(ValueError) as exc:
            self._flow(_TypedState())._merge_inputs({"regoin": "DACH"})

        message = str(exc.value)
        assert "regoin" in message  # names the key that was wrong
        assert "region" in message  # and what the state does accept

    def test_nothing_to_merge_is_not_an_error(self):
        self._flow(_TypedState())._merge_inputs({})


class TestCrewInputsFromState:
    """The link between a flow HAVING a value and that value meaning anything.

    A flow merges its inputs into state; a crew interpolates `{placeholders}`
    from the inputs passed to ITS kickoff. Both crew call sites passed none, so
    on a real run `topic="swiss news"` reached flow state correctly and the crew
    then executed a task that still read "related to a specified {topic}"
    literally — searched for whatever its memory suggested, and returned a raw
    tool payload instead of an answer.
    """

    @staticmethod
    def _flow(state):
        return SimpleNamespace(state=state)

    def test_a_dict_state_becomes_crew_inputs(self):
        assert crew_inputs_from_state(self._flow({"topic": "swiss news"})) == {
            "topic": "swiss news"
        }

    def test_the_checkpoint_id_is_not_an_input(self):
        # `id` is the checkpoint handle. A task must never interpolate it.
        assert crew_inputs_from_state(self._flow({"topic": "x", "id": "abc"})) == {
            "topic": "x"
        }

    def test_a_typed_state_works_too(self):
        state = SimpleNamespace(topic="x", id="abc")
        state._internal = "hidden"
        assert crew_inputs_from_state(self._flow(state)) == {"topic": "x"}

    def test_no_state_passes_nothing_rather_than_failing(self):
        assert crew_inputs_from_state(SimpleNamespace()) == {}
        assert crew_inputs_from_state(self._flow(None)) == {}
        assert crew_inputs_from_state(self._flow("not a mapping")) == {}


class TestRouterEvaluatesWithoutArgs:
    """A router that listens to a STARTING POINT is called with no args.

    Its condition helpers — strip_code_fences, looks_like_json,
    merge_parsed_json — used to be defined inside `if args:`, while the state
    scan that uses them runs unconditionally. So a starting-point router hit
    UnboundLocalError before reading state at all; the handler swallowed it and
    abandoned the whole evaluation. Observed on a real flow: the log said
    "cannot access local variable 'strip_code_fences'" and a route whose
    condition was TRUE never ran.
    """

    def test_the_helpers_are_defined_before_the_args_branch(self):
        import inspect

        # build_eval_context and its helpers now live in flow_eval_context;
        # the invariant is unchanged, only its address.
        from src.services.flow_builder.modules import flow_eval_context

        source = inspect.getsource(flow_eval_context)
        # Position, not behaviour: the failure was purely one of definition
        # order, and only the order can prevent it recurring.
        helpers = [
            source.index("def merge_parsed_json"),
            source.index("def strip_code_fences"),
            source.index("def looks_like_json"),
        ]
        args_branch = source.index("if args:\n")

        assert max(helpers) < args_branch, (
            "A condition helper is defined inside `if args:` again. A router "
            "listening to a starting point is called WITHOUT args, so it would "
            "raise UnboundLocalError and silently skip every route."
        )
