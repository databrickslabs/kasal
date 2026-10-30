"""
Unit tests for flow execution exceptions.

Tests the FlowPausedForApprovalException class.
"""

import pytest

from src.services.flow_builder.exceptions import (
    FlowPausedForApprovalException,
)


class TestFlowPausedForApprovalException:
    """Tests for the FlowPausedForApprovalException class."""

    def _make_exc(self, **overrides):
        defaults = dict(
            approval_id=42,
            gate_node_id="gate_node_001",
            message="Approval required before proceeding",
        )
        defaults.update(overrides)
        return FlowPausedForApprovalException(**defaults)  # type: ignore[arg-type]

    # --- Instantiation / attribute storage ---

    def test_required_fields_stored(self):
        """Required constructor arguments are stored as attributes."""
        exc = self._make_exc()
        assert exc.approval_id == 42
        assert exc.gate_node_id == "gate_node_001"
        assert exc.message == "Approval required before proceeding"

    def test_optional_fields_default_to_none(self):
        """Optional fields default to None when not supplied."""
        exc = self._make_exc()
        assert exc.execution_id is None
        assert exc.crew_sequence is None
        assert exc.flow_uuid is None

    def test_optional_fields_stored_when_provided(self):
        """Optional fields are stored when provided."""
        exc = self._make_exc(
            execution_id="exec-123",
            crew_sequence=3,
            flow_uuid="uuid-abc-def",
        )
        assert exc.execution_id == "exec-123"
        assert exc.crew_sequence == 3
        assert exc.flow_uuid == "uuid-abc-def"

    def test_is_exception_subclass(self):
        """FlowPausedForApprovalException IS a plain Exception subclass."""
        assert issubclass(
            FlowPausedForApprovalException, BaseException
        )  # control-flow signal, not a catchable Exception

    # --- String representation ---

    def test_str_contains_gate_node_id(self):
        """String representation contains the gate node ID."""
        exc = self._make_exc(gate_node_id="my_gate")
        assert "my_gate" in str(exc)

    def test_str_contains_approval_id(self):
        """String representation contains the approval_id."""
        exc = self._make_exc(approval_id=99)
        assert "99" in str(exc)

    def test_str_contains_message(self):
        """String representation contains the human-readable message."""
        exc = self._make_exc(message="Need manager sign-off")
        assert "Need manager sign-off" in str(exc)

    def test_str_format(self):
        """String representation matches the expected format."""
        exc = FlowPausedForApprovalException(
            approval_id=7,
            gate_node_id="g_node",
            message="check it",
        )
        s = str(exc)
        assert "g_node" in s
        assert "check it" in s
        assert "7" in s

    # --- to_dict ---

    def test_to_dict_keys(self):
        """to_dict() returns all expected keys."""
        exc = self._make_exc()
        d = exc.to_dict()
        expected_keys = {
            "approval_id",
            "gate_node_id",
            "message",
            "execution_id",
            "crew_sequence",
            "flow_uuid",
            "status",
        }
        assert expected_keys == set(d.keys())

    def test_to_dict_required_values(self):
        """to_dict() values match constructor arguments for required fields."""
        exc = self._make_exc(
            approval_id=5,
            gate_node_id="gate_5",
            message="waiting",
        )
        d = exc.to_dict()
        assert d["approval_id"] == 5
        assert d["gate_node_id"] == "gate_5"
        assert d["message"] == "waiting"

    def test_to_dict_optional_values_default_none(self):
        """to_dict() optional fields are None when not provided."""
        exc = self._make_exc()
        d = exc.to_dict()
        assert d["execution_id"] is None
        assert d["crew_sequence"] is None
        assert d["flow_uuid"] is None

    def test_to_dict_optional_values_when_provided(self):
        """to_dict() optional fields reflect supplied values."""
        exc = self._make_exc(
            execution_id="job-999",
            crew_sequence=2,
            flow_uuid="flow-uuid-xyz",
        )
        d = exc.to_dict()
        assert d["execution_id"] == "job-999"
        assert d["crew_sequence"] == 2
        assert d["flow_uuid"] == "flow-uuid-xyz"

    def test_to_dict_status_is_waiting_for_approval(self):
        """to_dict() always sets status to 'waiting_for_approval'."""
        exc = self._make_exc()
        assert exc.to_dict()["status"] == "waiting_for_approval"

    def test_to_dict_returns_new_dict_on_each_call(self):
        """to_dict() returns a fresh dict; mutations do not affect the exception."""
        exc = self._make_exc()
        d1 = exc.to_dict()
        d1["approval_id"] = 9999
        d2 = exc.to_dict()
        assert d2["approval_id"] == 42

    # --- raise / catch behaviour ---

    def test_can_be_raised_and_caught(self):
        """FlowPausedForApprovalException can be raised and caught."""
        with pytest.raises(FlowPausedForApprovalException) as exc_info:
            raise FlowPausedForApprovalException(
                approval_id=1,
                gate_node_id="g",
                message="paused",
            )
        assert exc_info.value.approval_id == 1
