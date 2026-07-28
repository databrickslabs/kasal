"""The canonical external state vocabulary.

The point of these tests is that the mapping is TOTAL and lives in one place. If
an adapter ever re-derives it, or a new ExecutionStatus appears without a home
here, an external caller starts getting a state that does not describe the run.
"""

import pytest

from src.models.execution_status import ExecutionStatus
from src.services.external.state import (
    ExternalTaskState,
    is_terminal,
    to_external_state,
)


class TestEveryStatusIsMapped:
    def test_no_execution_status_is_unmapped(self):
        """Every ExecutionStatus must translate deliberately.

        The failure this catches: someone adds a status to the enum, no adapter
        is updated, and external callers silently see `working` forever for a
        run that has actually finished.
        """
        from src.services.external.state import _STATUS_TO_EXTERNAL

        unmapped = [s for s in ExecutionStatus if s not in _STATUS_TO_EXTERNAL]
        assert not unmapped, (
            "these ExecutionStatus values have no external state:\n  "
            + "\n  ".join(s.value for s in unmapped)
            + "\n\nAdd them to _STATUS_TO_EXTERNAL in services/external/state.py."
        )

    @pytest.mark.parametrize(
        "status,expected",
        [
            (ExecutionStatus.PENDING, ExternalTaskState.SUBMITTED),
            (ExecutionStatus.PREPARING, ExternalTaskState.SUBMITTED),
            (ExecutionStatus.RUNNING, ExternalTaskState.WORKING),
            (ExecutionStatus.WAITING_FOR_APPROVAL, ExternalTaskState.INPUT_REQUIRED),
            (ExecutionStatus.COMPLETED, ExternalTaskState.COMPLETED),
            (ExecutionStatus.FAILED, ExternalTaskState.FAILED),
            (ExecutionStatus.STOPPING, ExternalTaskState.CANCELED),
            (ExecutionStatus.STOPPED, ExternalTaskState.CANCELED),
            (ExecutionStatus.CANCELLED, ExternalTaskState.CANCELED),
            (ExecutionStatus.REJECTED, ExternalTaskState.REJECTED),
        ],
    )
    def test_mapping(self, status, expected):
        assert to_external_state(status.value) is expected

    def test_hitl_pause_is_input_required(self):
        """The state most platforms cannot express, and the reason the MCP
        surface gets a human-in-the-loop story at all."""
        assert (
            to_external_state(ExecutionStatus.WAITING_FOR_APPROVAL.value)
            is ExternalTaskState.INPUT_REQUIRED
        )


class TestUnknownInput:
    def test_unknown_status_reads_as_working_not_failed(self):
        """A status this layer has not been taught means Kasal grew a state.
        Reporting it as `failed` would make a client abandon a live run; `working`
        keeps it polling until a state that IS known arrives — and every terminal
        status is mapped."""
        assert to_external_state("SOME_NEW_STATUS") is ExternalTaskState.WORKING

    def test_none_reads_as_working(self):
        assert to_external_state(None) is ExternalTaskState.WORKING

    def test_lowercase_status_is_accepted(self):
        """Callers hold the raw persisted string; be forgiving about case."""
        assert to_external_state("completed") is ExternalTaskState.COMPLETED


class TestTerminality:
    @pytest.mark.parametrize(
        "state",
        [
            ExternalTaskState.COMPLETED,
            ExternalTaskState.FAILED,
            ExternalTaskState.CANCELED,
            ExternalTaskState.REJECTED,
        ],
    )
    def test_terminal(self, state):
        assert is_terminal(state)

    @pytest.mark.parametrize(
        "state",
        [
            ExternalTaskState.SUBMITTED,
            ExternalTaskState.WORKING,
            ExternalTaskState.INPUT_REQUIRED,
            ExternalTaskState.AUTH_REQUIRED,
        ],
    )
    def test_not_terminal(self, state):
        assert not is_terminal(state)

    def test_input_required_is_not_terminal(self):
        """A run waiting for a human is still live. Treating it as terminal is
        how a HITL gate turns into a silently abandoned run."""
        assert not is_terminal(ExternalTaskState.INPUT_REQUIRED)


class TestVocabularyIsA2As:
    def test_wire_values_match_the_a2a_names(self):
        """These strings are a published standard, not a Kasal invention — an
        A2A client maps them to TASK_STATE_* directly. Renaming one silently
        breaks every external caller."""
        assert [s.value for s in ExternalTaskState] == [
            "submitted",
            "working",
            "input_required",
            "auth_required",
            "completed",
            "failed",
            "canceled",
            "rejected",
        ]
