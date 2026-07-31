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
