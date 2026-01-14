"""
Flow Execution Exceptions.

This module defines exceptions used during flow execution,
particularly for Human in the Loop (HITL) gate handling.
"""

from typing import Optional, Dict, Any


class FlowExecutionError(Exception):
    """Base exception for flow execution errors."""
    pass


class FlowPausedForApprovalException(Exception):
    """
    Raised when a flow pauses at an HITL gate awaiting human approval.

    This is not an error condition - it's a controlled pause that signals
    the flow should stop and wait for human intervention.

    The flow can be resumed after approval by using the approval_id
    and the checkpoint data stored in the execution.

    Attributes:
        approval_id: ID of the created HITLApproval record
        gate_node_id: ID of the HITL gate node in the flow
        message: Human-readable message about the approval required
        execution_id: Job ID of the execution that is paused
        crew_sequence: Sequence number to resume from after approval
        flow_uuid: CrewAI flow state ID for persistence
    """

    def __init__(
        self,
        approval_id: int,
        gate_node_id: str,
        message: str,
        execution_id: Optional[str] = None,
        crew_sequence: Optional[int] = None,
        flow_uuid: Optional[str] = None
    ):
        self.approval_id = approval_id
        self.gate_node_id = gate_node_id
        self.message = message
        self.execution_id = execution_id
        self.crew_sequence = crew_sequence
        self.flow_uuid = flow_uuid

        super().__init__(
            f"Flow paused at HITL gate '{gate_node_id}': {message} "
            f"(approval_id={approval_id})"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception data to dictionary for API response."""
        return {
            "approval_id": self.approval_id,
            "gate_node_id": self.gate_node_id,
            "message": self.message,
            "execution_id": self.execution_id,
            "crew_sequence": self.crew_sequence,
            "flow_uuid": self.flow_uuid,
            "status": "waiting_for_approval"
        }


class FlowResumeError(FlowExecutionError):
    """Raised when there's an error resuming a flow from checkpoint."""

    def __init__(self, execution_id: str, reason: str):
        self.execution_id = execution_id
        self.reason = reason
        super().__init__(f"Failed to resume flow {execution_id}: {reason}")


class FlowCheckpointError(FlowExecutionError):
    """Raised when there's an error with flow checkpointing."""

    def __init__(self, execution_id: str, reason: str):
        self.execution_id = execution_id
        self.reason = reason
        super().__init__(f"Checkpoint error for flow {execution_id}: {reason}")


class HITLGateConfigError(FlowExecutionError):
    """Raised when HITL gate configuration is invalid."""

    def __init__(self, gate_node_id: str, reason: str):
        self.gate_node_id = gate_node_id
        self.reason = reason
        super().__init__(f"Invalid HITL gate config for '{gate_node_id}': {reason}")
