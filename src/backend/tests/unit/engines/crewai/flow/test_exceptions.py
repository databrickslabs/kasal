"""
Unit tests for flow execution exceptions.

Tests the FlowPausedForApprovalException, FlowResumeError,
FlowCheckpointError, HITLGateConfigError, and FlowExecutionError classes.
"""
import pytest

from src.engines.crewai.paths.flow.exceptions import (
    FlowExecutionError,
    FlowPausedForApprovalException,
    FlowResumeError,
    FlowCheckpointError,
    HITLGateConfigError,
)


class TestFlowExecutionError:
    """Tests for the FlowExecutionError base exception."""

    def test_is_exception_subclass(self):
        """FlowExecutionError is a subclass of Exception."""
        assert issubclass(FlowExecutionError, Exception)

    def test_can_be_raised_and_caught(self):
        """FlowExecutionError can be raised and caught as Exception."""
        with pytest.raises(Exception):
            raise FlowExecutionError("something went wrong")

    def test_can_be_caught_as_own_type(self):
        """FlowExecutionError can be caught by its own type."""
        with pytest.raises(FlowExecutionError):
            raise FlowExecutionError("something went wrong")

    def test_message_is_preserved(self):
        """Message is preserved in the exception args."""
        exc = FlowExecutionError("test message")
        assert "test message" in str(exc)

    def test_empty_message(self):
        """FlowExecutionError can be created with an empty message."""
        exc = FlowExecutionError("")
        assert str(exc) == ""

    def test_no_message(self):
        """FlowExecutionError can be created with no arguments."""
        exc = FlowExecutionError()
        assert exc.args == ()


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

    def test_is_not_subclass_of_flow_execution_error(self):
        """FlowPausedForApprovalException is NOT a FlowExecutionError — it's a signal."""
        assert not issubclass(FlowPausedForApprovalException, FlowExecutionError)

    def test_is_exception_subclass(self):
        """FlowPausedForApprovalException IS a plain Exception subclass."""
        assert issubclass(FlowPausedForApprovalException, BaseException)  # control-flow signal, not a catchable Exception

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


class TestFlowResumeError:
    """Tests for the FlowResumeError exception."""

    def test_is_flow_execution_error_subclass(self):
        """FlowResumeError is a subclass of FlowExecutionError."""
        assert issubclass(FlowResumeError, FlowExecutionError)

    def test_is_exception_subclass(self):
        """FlowResumeError is ultimately an Exception subclass."""
        assert issubclass(FlowResumeError, Exception)

    def test_attributes_stored(self):
        """execution_id and reason are stored as attributes."""
        exc = FlowResumeError(execution_id="exec-001", reason="checkpoint missing")
        assert exc.execution_id == "exec-001"
        assert exc.reason == "checkpoint missing"

    def test_str_contains_execution_id(self):
        """String representation contains the execution_id."""
        exc = FlowResumeError(execution_id="exec-XYZ", reason="bad state")
        assert "exec-XYZ" in str(exc)

    def test_str_contains_reason(self):
        """String representation contains the reason."""
        exc = FlowResumeError(execution_id="e", reason="serialization failed")
        assert "serialization failed" in str(exc)

    def test_str_format(self):
        """String representation starts with expected prefix."""
        exc = FlowResumeError(execution_id="e-1", reason="no data")
        assert str(exc).startswith("Failed to resume flow")

    def test_can_be_caught_as_flow_execution_error(self):
        """FlowResumeError can be caught as FlowExecutionError."""
        with pytest.raises(FlowExecutionError):
            raise FlowResumeError(execution_id="e", reason="r")

    def test_can_be_caught_by_own_type(self):
        """FlowResumeError can be caught by its own type."""
        with pytest.raises(FlowResumeError):
            raise FlowResumeError(execution_id="e", reason="r")

    def test_reason_preserved_with_special_chars(self):
        """Reason containing special characters is preserved."""
        reason = "failed: unexpected token '<' at position 0"
        exc = FlowResumeError(execution_id="e", reason=reason)
        assert exc.reason == reason
        assert reason in str(exc)


class TestFlowCheckpointError:
    """Tests for the FlowCheckpointError exception."""

    def test_is_flow_execution_error_subclass(self):
        """FlowCheckpointError is a subclass of FlowExecutionError."""
        assert issubclass(FlowCheckpointError, FlowExecutionError)

    def test_is_exception_subclass(self):
        """FlowCheckpointError is ultimately an Exception subclass."""
        assert issubclass(FlowCheckpointError, Exception)

    def test_attributes_stored(self):
        """execution_id and reason are stored as attributes."""
        exc = FlowCheckpointError(execution_id="exec-007", reason="disk full")
        assert exc.execution_id == "exec-007"
        assert exc.reason == "disk full"

    def test_str_contains_execution_id(self):
        """String representation contains the execution_id."""
        exc = FlowCheckpointError(execution_id="exec-007", reason="disk full")
        assert "exec-007" in str(exc)

    def test_str_contains_reason(self):
        """String representation contains the reason."""
        exc = FlowCheckpointError(execution_id="e", reason="write error")
        assert "write error" in str(exc)

    def test_str_format(self):
        """String representation starts with expected prefix."""
        exc = FlowCheckpointError(execution_id="e-1", reason="r")
        assert str(exc).startswith("Checkpoint error for flow")

    def test_can_be_caught_as_flow_execution_error(self):
        """FlowCheckpointError can be caught as FlowExecutionError."""
        with pytest.raises(FlowExecutionError):
            raise FlowCheckpointError(execution_id="e", reason="r")

    def test_can_be_caught_by_own_type(self):
        """FlowCheckpointError can be caught by its own type."""
        with pytest.raises(FlowCheckpointError):
            raise FlowCheckpointError(execution_id="e", reason="r")

    def test_distinct_from_resume_error(self):
        """FlowCheckpointError and FlowResumeError are distinct types."""
        checkpoint_exc = FlowCheckpointError(execution_id="e", reason="r")
        assert not isinstance(checkpoint_exc, FlowResumeError)

        resume_exc = FlowResumeError(execution_id="e", reason="r")
        assert not isinstance(resume_exc, FlowCheckpointError)


class TestHITLGateConfigError:
    """Tests for the HITLGateConfigError exception."""

    def test_is_flow_execution_error_subclass(self):
        """HITLGateConfigError is a subclass of FlowExecutionError."""
        assert issubclass(HITLGateConfigError, FlowExecutionError)

    def test_is_exception_subclass(self):
        """HITLGateConfigError is ultimately an Exception subclass."""
        assert issubclass(HITLGateConfigError, Exception)

    def test_attributes_stored(self):
        """gate_node_id and reason are stored as attributes."""
        exc = HITLGateConfigError(gate_node_id="gate_A", reason="missing approver list")
        assert exc.gate_node_id == "gate_A"
        assert exc.reason == "missing approver list"

    def test_str_contains_gate_node_id(self):
        """String representation contains the gate_node_id."""
        exc = HITLGateConfigError(gate_node_id="gate_B", reason="bad config")
        assert "gate_B" in str(exc)

    def test_str_contains_reason(self):
        """String representation contains the reason."""
        exc = HITLGateConfigError(gate_node_id="g", reason="timeout not set")
        assert "timeout not set" in str(exc)

    def test_str_format(self):
        """String representation starts with the expected prefix."""
        exc = HITLGateConfigError(gate_node_id="g", reason="r")
        assert str(exc).startswith("Invalid HITL gate config")

    def test_can_be_caught_as_flow_execution_error(self):
        """HITLGateConfigError can be caught as FlowExecutionError."""
        with pytest.raises(FlowExecutionError):
            raise HITLGateConfigError(gate_node_id="g", reason="r")

    def test_can_be_caught_by_own_type(self):
        """HITLGateConfigError can be caught by its own type."""
        with pytest.raises(HITLGateConfigError):
            raise HITLGateConfigError(gate_node_id="g", reason="r")

    def test_reason_with_technical_detail(self):
        """Reason with technical detail is fully preserved."""
        reason = "approvers field must be a non-empty list of email strings"
        exc = HITLGateConfigError(gate_node_id="gate_complex", reason=reason)
        assert exc.reason == reason
        assert reason in str(exc)


class TestExceptionInheritanceHierarchy:
    """Verify the overall exception hierarchy is correct."""

    def test_resume_error_is_not_checkpoint_error(self):
        assert not issubclass(FlowResumeError, FlowCheckpointError)

    def test_checkpoint_error_is_not_resume_error(self):
        assert not issubclass(FlowCheckpointError, FlowResumeError)

    def test_hitl_config_error_is_not_resume_error(self):
        assert not issubclass(HITLGateConfigError, FlowResumeError)

    def test_hitl_config_error_is_not_checkpoint_error(self):
        assert not issubclass(HITLGateConfigError, FlowCheckpointError)

    def test_paused_exception_is_not_flow_execution_error(self):
        """The pause signal is NOT an error, so it must not be caught by FlowExecutionError."""
        with pytest.raises(FlowPausedForApprovalException):
            try:
                raise FlowPausedForApprovalException(
                    approval_id=1, gate_node_id="g", message="m"
                )
            except FlowExecutionError:
                pytest.fail("FlowPausedForApprovalException should NOT be caught as FlowExecutionError")

    def test_catching_flow_execution_error_catches_subclasses(self):
        """A single FlowExecutionError handler catches all its subclasses."""
        caught = []
        for exc_cls, kwargs in [
            (FlowResumeError, {"execution_id": "e", "reason": "r"}),
            (FlowCheckpointError, {"execution_id": "e", "reason": "r"}),
            (HITLGateConfigError, {"gate_node_id": "g", "reason": "r"}),
        ]:
            try:
                raise exc_cls(**kwargs)
            except FlowExecutionError as e:
                caught.append(type(e))

        assert FlowResumeError in caught
        assert FlowCheckpointError in caught
        assert HITLGateConfigError in caught
