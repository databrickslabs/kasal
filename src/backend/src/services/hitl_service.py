"""
Service for Human in the Loop (HITL) operations.

This module provides the core business logic for HITL approval management,
including creating approval requests, processing approvals/rejections,
handling timeouts, and triggering flow resume.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from src.models.hitl_approval import (
    HITLApproval,
    HITLApprovalStatus,
    HITLTimeoutAction,
    HITLRejectionAction
)
from src.models.execution_status import ExecutionStatus
from src.repositories.hitl_repository import HITLApprovalRepository, HITLWebhookRepository
from src.schemas.hitl import (
    HITLApprovalCreate,
    HITLApprovalResponse,
    HITLApprovalListResponse,
    HITLActionResponse,
    ExecutionHITLStatus,
    HITLApprovalStatusEnum,
    HITLRejectionActionEnum
)

logger = logging.getLogger(__name__)


class HITLServiceError(Exception):
    """Base exception for HITL service errors."""
    pass


class HITLApprovalNotFoundError(HITLServiceError):
    """Raised when an HITL approval is not found."""
    pass


class HITLApprovalAlreadyProcessedError(HITLServiceError):
    """Raised when trying to process an already processed approval."""
    pass


class HITLApprovalExpiredError(HITLServiceError):
    """Raised when trying to process an expired approval."""
    pass


class HITLPermissionDeniedError(HITLServiceError):
    """Raised when user is not allowed to approve/reject."""
    pass


class HITLService:
    """Service for Human in the Loop approval operations."""

    def __init__(
        self,
        session: AsyncSession,
        approval_repository: Optional[HITLApprovalRepository] = None,
        webhook_repository: Optional[HITLWebhookRepository] = None
    ):
        """
        Initialize the service with session and repositories.

        Args:
            session: Database session from FastAPI DI
            approval_repository: Optional repository instance (for testing)
            webhook_repository: Optional webhook repository instance (for testing)
        """
        self.session = session
        self.approval_repo = approval_repository or HITLApprovalRepository(session)
        self.webhook_repo = webhook_repository or HITLWebhookRepository(session)

    async def create_approval_request(
        self,
        execution_id: str,
        flow_id: str,
        gate_node_id: str,
        crew_sequence: int,
        gate_config: Dict[str, Any],
        group_id: str,
        previous_crew_name: Optional[str] = None,
        previous_crew_output: Optional[str] = None,
        flow_state_snapshot: Optional[Dict[str, Any]] = None
    ) -> HITLApproval:
        """
        Create a new HITL approval request when flow hits a gate.

        This method is called by the flow execution engine when an HITL gate
        is reached. It creates an approval record, updates execution status,
        and triggers webhook notifications.

        Args:
            execution_id: Job ID of the execution
            flow_id: ID of the flow being executed
            gate_node_id: ID of the HITL gate node
            crew_sequence: Sequence number of the crew that completed before gate
            gate_config: Gate configuration dict
            group_id: Group ID for multi-tenant isolation
            previous_crew_name: Name of the crew that completed before gate
            previous_crew_output: Output from previous crew for review
            flow_state_snapshot: State of the flow at the gate point

        Returns:
            Created HITLApproval record
        """
        try:
            # Calculate expiration
            timeout_seconds = gate_config.get("timeout_seconds", 3600)
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)

            # Create approval record
            approval = HITLApproval(
                execution_id=execution_id,
                flow_id=flow_id,
                gate_node_id=gate_node_id,
                crew_sequence=crew_sequence,
                status=HITLApprovalStatus.PENDING,
                gate_config=gate_config,
                previous_crew_name=previous_crew_name,
                previous_crew_output=previous_crew_output,
                flow_state_snapshot=flow_state_snapshot or {},
                expires_at=expires_at,
                group_id=group_id
            )

            created_approval = await self.approval_repo.create(approval)

            # Update execution status to WAITING_FOR_APPROVAL
            await self._update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.WAITING_FOR_APPROVAL.value,
                message=f"Waiting for approval at gate: {gate_node_id}"
            )

            logger.info(
                f"Created HITL approval {created_approval.id} for execution {execution_id} "
                f"at gate {gate_node_id}, expires at {expires_at}"
            )

            return created_approval

        except SQLAlchemyError as e:
            logger.error(f"Database error creating HITL approval: {str(e)}")
            raise HITLServiceError(f"Failed to create approval request: {str(e)}")

    async def approve(
        self,
        approval_id: int,
        approved_by: str,
        group_id: str,
        comment: Optional[str] = None,
        user_token: Optional[str] = None
    ) -> HITLActionResponse:
        """
        Approve an HITL gate and resume flow execution.

        Args:
            approval_id: ID of the approval to approve
            approved_by: Email of the user approving
            group_id: Group ID for authorization
            comment: Optional comment with approval
            user_token: User's access token for OBO authentication on resume

        Returns:
            HITLActionResponse with result

        Raises:
            HITLApprovalNotFoundError: If approval not found
            HITLApprovalAlreadyProcessedError: If already processed
            HITLApprovalExpiredError: If approval has expired
            HITLPermissionDeniedError: If user not allowed to approve
        """
        try:
            # Get approval
            approval = await self.approval_repo.get_by_id(approval_id, group_id)
            if not approval:
                raise HITLApprovalNotFoundError(f"Approval {approval_id} not found")

            # Validate status
            if approval.status != HITLApprovalStatus.PENDING:
                raise HITLApprovalAlreadyProcessedError(
                    f"Approval {approval_id} already processed with status: {approval.status}"
                )

            # Check expiration
            if approval.is_expired:
                raise HITLApprovalExpiredError(f"Approval {approval_id} has expired")

            # Check permission
            if not approval.can_be_approved_by(approved_by):
                raise HITLPermissionDeniedError(
                    f"User {approved_by} is not allowed to approve this gate"
                )

            # Update approval status
            await self.approval_repo.update_status(
                approval_id=approval_id,
                status=HITLApprovalStatus.APPROVED,
                responded_by=approved_by,
                approval_comment=comment
            )

            # Resume flow execution with user's token for OBO auth
            execution_resumed = await self._resume_flow_execution(approval, user_token=user_token)

            logger.info(f"HITL approval {approval_id} approved by {approved_by}")

            return HITLActionResponse(
                success=True,
                approval_id=approval_id,
                status=HITLApprovalStatusEnum.APPROVED,
                message="Gate approved successfully",
                execution_resumed=execution_resumed
            )

        except (HITLApprovalNotFoundError, HITLApprovalAlreadyProcessedError,
                HITLApprovalExpiredError, HITLPermissionDeniedError):
            raise
        except SQLAlchemyError as e:
            logger.error(f"Database error approving HITL: {str(e)}")
            raise HITLServiceError(f"Failed to approve: {str(e)}")

    async def reject(
        self,
        approval_id: int,
        rejected_by: str,
        group_id: str,
        reason: str,
        action: HITLRejectionActionEnum = HITLRejectionActionEnum.REJECT
    ) -> HITLActionResponse:
        """
        Reject an HITL gate.

        Args:
            approval_id: ID of the approval to reject
            rejected_by: Email of the user rejecting
            group_id: Group ID for authorization
            reason: Reason for rejection
            action: Action to take (reject = fail flow, retry = re-run previous crew)

        Returns:
            HITLActionResponse with result
        """
        try:
            # Get approval
            approval = await self.approval_repo.get_by_id(approval_id, group_id)
            if not approval:
                raise HITLApprovalNotFoundError(f"Approval {approval_id} not found")

            # Validate status
            if approval.status != HITLApprovalStatus.PENDING:
                raise HITLApprovalAlreadyProcessedError(
                    f"Approval {approval_id} already processed with status: {approval.status}"
                )

            # Check expiration
            if approval.is_expired:
                raise HITLApprovalExpiredError(f"Approval {approval_id} has expired")

            # Check permission
            if not approval.can_be_approved_by(rejected_by):
                raise HITLPermissionDeniedError(
                    f"User {rejected_by} is not allowed to reject this gate"
                )

            # Determine new status based on action
            new_status = (
                HITLApprovalStatus.RETRY if action == HITLRejectionActionEnum.RETRY
                else HITLApprovalStatus.REJECTED
            )

            # Update approval status
            await self.approval_repo.update_status(
                approval_id=approval_id,
                status=new_status,
                responded_by=rejected_by,
                rejection_reason=reason,
                rejection_action=action.value
            )

            # Handle rejection action
            if action == HITLRejectionActionEnum.RETRY:
                # Retry: Re-run the previous crew
                execution_resumed = await self._retry_previous_crew(approval)
                message = "Gate rejected - retrying previous crew"
            else:
                # Reject: Fail the flow
                await self._fail_execution(approval, reason)
                execution_resumed = False
                message = "Gate rejected - flow execution failed"

            logger.info(
                f"HITL approval {approval_id} rejected by {rejected_by} "
                f"with action {action.value}: {reason}"
            )

            return HITLActionResponse(
                success=True,
                approval_id=approval_id,
                status=HITLApprovalStatusEnum(new_status),
                message=message,
                execution_resumed=execution_resumed
            )

        except (HITLApprovalNotFoundError, HITLApprovalAlreadyProcessedError,
                HITLApprovalExpiredError, HITLPermissionDeniedError):
            raise
        except SQLAlchemyError as e:
            logger.error(f"Database error rejecting HITL: {str(e)}")
            raise HITLServiceError(f"Failed to reject: {str(e)}")

    async def get_pending_approvals(
        self,
        group_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> HITLApprovalListResponse:
        """
        Get all pending HITL approvals for a group.

        Args:
            group_id: Group ID for filtering
            limit: Maximum number of items
            offset: Number of items to skip

        Returns:
            HITLApprovalListResponse with pending approvals
        """
        try:
            approvals, total = await self.approval_repo.get_pending_for_group(
                group_id=group_id,
                limit=limit,
                offset=offset
            )

            items = [
                HITLApprovalResponse(
                    id=a.id,
                    execution_id=a.execution_id,
                    flow_id=a.flow_id,
                    gate_node_id=a.gate_node_id,
                    crew_sequence=a.crew_sequence,
                    status=HITLApprovalStatusEnum(a.status),
                    gate_config=a.gate_config,
                    previous_crew_name=a.previous_crew_name,
                    previous_crew_output=a.previous_crew_output,
                    flow_state_snapshot=a.flow_state_snapshot,
                    responded_by=a.responded_by,
                    responded_at=a.responded_at,
                    approval_comment=a.approval_comment,
                    rejection_reason=a.rejection_reason,
                    rejection_action=(
                        HITLRejectionActionEnum(a.rejection_action)
                        if a.rejection_action else None
                    ),
                    expires_at=a.expires_at,
                    is_expired=a.is_expired,
                    created_at=a.created_at,
                    group_id=a.group_id
                )
                for a in approvals
            ]

            return HITLApprovalListResponse(
                items=items,
                total=total,
                limit=limit,
                offset=offset
            )

        except SQLAlchemyError as e:
            logger.error(f"Database error getting pending approvals: {str(e)}")
            raise HITLServiceError(f"Failed to get pending approvals: {str(e)}")

    async def get_execution_hitl_status(
        self,
        execution_id: str,
        group_id: str
    ) -> ExecutionHITLStatus:
        """
        Get HITL status for an execution.

        Args:
            execution_id: Job ID of the execution
            group_id: Group ID for authorization

        Returns:
            ExecutionHITLStatus with approval history
        """
        try:
            # Get all approvals for execution
            approvals = await self.approval_repo.get_all_for_execution(
                execution_id=execution_id,
                group_id=group_id
            )

            # Find pending approval
            pending = next(
                (a for a in approvals if a.status == HITLApprovalStatus.PENDING),
                None
            )

            # Convert to response objects
            approval_responses = [
                HITLApprovalResponse(
                    id=a.id,
                    execution_id=a.execution_id,
                    flow_id=a.flow_id,
                    gate_node_id=a.gate_node_id,
                    crew_sequence=a.crew_sequence,
                    status=HITLApprovalStatusEnum(a.status),
                    gate_config=a.gate_config,
                    previous_crew_name=a.previous_crew_name,
                    previous_crew_output=a.previous_crew_output,
                    flow_state_snapshot=a.flow_state_snapshot,
                    responded_by=a.responded_by,
                    responded_at=a.responded_at,
                    approval_comment=a.approval_comment,
                    rejection_reason=a.rejection_reason,
                    rejection_action=(
                        HITLRejectionActionEnum(a.rejection_action)
                        if a.rejection_action else None
                    ),
                    expires_at=a.expires_at,
                    is_expired=a.is_expired,
                    created_at=a.created_at,
                    group_id=a.group_id
                )
                for a in approvals
            ]

            pending_response = next(
                (r for r in approval_responses if r.status == HITLApprovalStatusEnum.PENDING),
                None
            )

            # Count passed gates
            gates_passed = sum(
                1 for a in approvals
                if a.status == HITLApprovalStatus.APPROVED
            )

            return ExecutionHITLStatus(
                execution_id=execution_id,
                has_pending_approval=pending is not None,
                pending_approval=pending_response,
                approval_history=approval_responses,
                total_gates_passed=gates_passed
            )

        except SQLAlchemyError as e:
            logger.error(f"Database error getting execution HITL status: {str(e)}")
            raise HITLServiceError(f"Failed to get HITL status: {str(e)}")

    async def process_expired_approvals(self) -> List[int]:
        """
        Process all expired pending approvals.

        This method is called by a background task to handle timeouts.
        It applies the configured timeout_action (auto_reject or fail).

        Returns:
            List of processed approval IDs
        """
        try:
            expired_approvals = await self.approval_repo.get_expired_pending()
            processed_ids = []

            for approval in expired_approvals:
                try:
                    timeout_action = approval.timeout_action

                    if timeout_action == HITLTimeoutAction.AUTO_REJECT:
                        # Auto-reject the approval
                        await self.approval_repo.update_status(
                            approval_id=approval.id,
                            status=HITLApprovalStatus.TIMEOUT,
                            responded_by="system",
                            rejection_reason="Approval timed out (auto-rejected)"
                        )
                        await self._fail_execution(
                            approval,
                            "HITL gate timed out and was auto-rejected"
                        )
                    else:
                        # Fail action - fail the execution
                        await self.approval_repo.update_status(
                            approval_id=approval.id,
                            status=HITLApprovalStatus.TIMEOUT,
                            responded_by="system",
                            rejection_reason="Approval timed out"
                        )
                        await self._fail_execution(
                            approval,
                            "HITL gate timed out"
                        )

                    processed_ids.append(approval.id)
                    logger.info(
                        f"Processed expired HITL approval {approval.id} "
                        f"with action {timeout_action}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing expired approval {approval.id}: {str(e)}"
                    )

            return processed_ids

        except SQLAlchemyError as e:
            logger.error(f"Database error processing expired approvals: {str(e)}")
            raise HITLServiceError(f"Failed to process expired approvals: {str(e)}")

    # =========================================================================
    # Private Methods
    # =========================================================================

    async def _update_execution_status(
        self,
        execution_id: str,
        status: str,
        message: Optional[str] = None
    ) -> None:
        """Update execution status in the database."""
        from src.repositories.execution_history_repository import ExecutionHistoryRepository

        repo = ExecutionHistoryRepository(self.session)
        execution = await repo.get_execution_by_job_id(execution_id)

        if execution:
            execution.status = status
            if message:
                # Store message in error field for visibility
                if status in [ExecutionStatus.FAILED.value, ExecutionStatus.REJECTED.value]:
                    execution.error = message
            await self.session.flush()

    async def _resume_flow_execution(
        self,
        approval: HITLApproval,
        user_token: Optional[str] = None
    ) -> bool:
        """
        Resume flow execution after approval.

        Args:
            approval: The approved HITLApproval
            user_token: User's access token for OBO authentication

        Returns:
            True if execution was resumed successfully
        """
        try:
            # Import here to avoid circular imports
            from src.repositories.execution_history_repository import ExecutionHistoryRepository
            from src.services.crewai_execution_service import CrewAIExecutionService
            from src.services.execution_status_service import ExecutionStatusService
            from src.utils.user_context import GroupContext
            import asyncio

            # Get execution record
            exec_repo = ExecutionHistoryRepository(self.session)
            execution = await exec_repo.get_execution_by_job_id(approval.execution_id)

            if not execution:
                logger.error(f"Execution {approval.execution_id} not found for resume")
                return False

            # Update execution status to RUNNING
            await self._update_execution_status(
                execution_id=approval.execution_id,
                status=ExecutionStatus.RUNNING.value
            )

            # Extract the original flow configuration from the execution inputs
            original_inputs = execution.inputs or {}
            flow_id = str(execution.flow_id) if execution.flow_id else original_inputs.get('flow_id')
            flow_uuid = execution.flow_uuid  # CrewAI's state.id for @persist

            # Build the resume configuration
            # We resume from the crew AFTER the HITL gate (crew_sequence + 1)
            # approval.crew_sequence is the crew that COMPLETED before the gate
            # resume_from_crew_sequence should be the FIRST crew TO RUN (which is crew_sequence + 1)
            resume_config = {
                **original_inputs,
                'resume_from_flow_uuid': flow_uuid,
                'resume_from_execution_id': approval.execution_id,
                'resume_from_crew_sequence': approval.crew_sequence + 1,  # Skip completed crew, start from next
            }

            logger.info(
                f"🚀 Resuming flow execution {approval.execution_id} after HITL approval, "
                f"flow_uuid={flow_uuid}, crew {approval.crew_sequence} completed, "
                f"resuming from crew sequence {approval.crew_sequence + 1}"
            )

            # Create group context for the resume with user's token for OBO auth
            group_context = GroupContext(
                group_ids=[approval.group_id],
                group_email=approval.responded_by,
                access_token=user_token  # Pass user's token for OBO authentication
            )

            # Trigger the flow execution asynchronously in a background task
            # This ensures we don't block the approval response
            async def _trigger_resume():
                try:
                    crewai_service = CrewAIExecutionService()
                    result = await crewai_service.run_flow_execution(
                        flow_id=flow_id,
                        nodes=original_inputs.get('nodes'),
                        edges=original_inputs.get('edges'),
                        job_id=approval.execution_id,  # Reuse the same job_id
                        config=resume_config,
                        group_context=group_context
                    )
                    logger.info(f"✅ Flow resume completed for {approval.execution_id}: {result.get('status', 'unknown')}")
                except Exception as e:
                    logger.error(f"❌ Flow resume failed for {approval.execution_id}: {str(e)}", exc_info=True)
                    # Update status to FAILED
                    await ExecutionStatusService.update_status(
                        job_id=approval.execution_id,
                        status=ExecutionStatus.FAILED.value,
                        message=f"Resume failed after HITL approval: {str(e)}"
                    )

            # Create the background task
            asyncio.create_task(_trigger_resume())

            return True

        except Exception as e:
            logger.error(f"Error resuming flow execution: {str(e)}", exc_info=True)
            return False

    async def _retry_previous_crew(self, approval: HITLApproval) -> bool:
        """
        Retry the previous crew after rejection with retry action.

        Args:
            approval: The rejected HITLApproval with retry action

        Returns:
            True if retry was initiated successfully
        """
        try:
            # Import here to avoid circular imports
            from src.repositories.execution_history_repository import ExecutionHistoryRepository
            from src.services.crewai_execution_service import CrewAIExecutionService
            from src.services.execution_status_service import ExecutionStatusService
            from src.utils.user_context import GroupContext
            import asyncio

            # Get execution record
            exec_repo = ExecutionHistoryRepository(self.session)
            execution = await exec_repo.get_execution_by_job_id(approval.execution_id)

            if not execution:
                logger.error(f"Execution {approval.execution_id} not found for retry")
                return False

            # Update execution status to RUNNING
            await self._update_execution_status(
                execution_id=approval.execution_id,
                status=ExecutionStatus.RUNNING.value
            )

            # Extract the original flow configuration from the execution inputs
            original_inputs = execution.inputs or {}
            flow_id = str(execution.flow_id) if execution.flow_id else original_inputs.get('flow_id')
            flow_uuid = execution.flow_uuid  # CrewAI's state.id for @persist

            # Build the retry configuration
            # For retry, we re-run from the SAME crew sequence (not +1)
            # This means crew_sequence - 1 since we want to include the failed crew
            retry_from_sequence = max(0, approval.crew_sequence - 1)
            retry_config = {
                **original_inputs,
                'resume_from_flow_uuid': flow_uuid,
                'resume_from_execution_id': approval.execution_id,
                'resume_from_crew_sequence': retry_from_sequence,  # Re-run the same crew
            }

            logger.info(
                f"🔄 Retrying flow execution {approval.execution_id} after HITL rejection, "
                f"flow_uuid={flow_uuid}, re-running from crew sequence {approval.crew_sequence}"
            )

            # Create group context for the retry
            group_context = GroupContext(
                group_ids=[approval.group_id],
                group_email=approval.responded_by
            )

            # Trigger the flow execution asynchronously in a background task
            async def _trigger_retry():
                try:
                    crewai_service = CrewAIExecutionService()
                    result = await crewai_service.run_flow_execution(
                        flow_id=flow_id,
                        nodes=original_inputs.get('nodes'),
                        edges=original_inputs.get('edges'),
                        job_id=approval.execution_id,  # Reuse the same job_id
                        config=retry_config,
                        group_context=group_context
                    )
                    logger.info(f"✅ Flow retry completed for {approval.execution_id}: {result.get('status', 'unknown')}")
                except Exception as e:
                    logger.error(f"❌ Flow retry failed for {approval.execution_id}: {str(e)}", exc_info=True)
                    # Update status to FAILED
                    await ExecutionStatusService.update_status(
                        job_id=approval.execution_id,
                        status=ExecutionStatus.FAILED.value,
                        message=f"Retry failed after HITL rejection: {str(e)}"
                    )

            # Create the background task
            asyncio.create_task(_trigger_retry())

            return True

        except Exception as e:
            logger.error(f"Error retrying previous crew: {str(e)}", exc_info=True)
            return False

    async def _fail_execution(
        self,
        approval: HITLApproval,
        reason: str
    ) -> None:
        """
        Fail the flow execution.

        Args:
            approval: The HITLApproval that caused the failure
            reason: Reason for failure
        """
        try:
            await self._update_execution_status(
                execution_id=approval.execution_id,
                status=ExecutionStatus.REJECTED.value,
                message=f"HITL gate rejected: {reason}"
            )

            logger.info(
                f"Flow execution {approval.execution_id} failed at gate "
                f"{approval.gate_node_id}: {reason}"
            )

        except Exception as e:
            logger.error(f"Error failing execution: {str(e)}")
