"""
Repository for HITL (Human in the Loop) data access.

This module provides database operations for HITL approval and webhook models.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, and_, or_
from sqlalchemy.exc import SQLAlchemyError

from src.models.hitl_approval import (
    HITLApproval,
    HITLWebhook,
    HITLApprovalStatus,
    HITLTimeoutAction
)

logger = logging.getLogger(__name__)


class HITLApprovalRepository:
    """Repository for HITL approval data access operations."""

    def __init__(self, session: AsyncSession):
        """Initialize with required session."""
        self.session = session

    async def create(self, approval: HITLApproval) -> HITLApproval:
        """
        Create a new HITL approval request.

        Args:
            approval: HITLApproval instance to create

        Returns:
            Created HITLApproval with ID
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        self.session.add(approval)
        await self.session.flush()
        await self.session.refresh(approval)
        logger.info(f"Created HITL approval {approval.id} for execution {approval.execution_id}")
        return approval

    async def get_by_id(
        self,
        approval_id: int,
        group_id: Optional[str] = None
    ) -> Optional[HITLApproval]:
        """
        Get an HITL approval by ID.

        Args:
            approval_id: ID of the approval
            group_id: Optional group ID for filtering

        Returns:
            HITLApproval if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        filters = [HITLApproval.id == approval_id]
        if group_id:
            filters.append(HITLApproval.group_id == group_id)

        stmt = select(HITLApproval).where(*filters)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def get_pending_for_execution(
        self,
        execution_id: str,
        group_id: Optional[str] = None
    ) -> Optional[HITLApproval]:
        """
        Get pending HITL approval for an execution.

        Args:
            execution_id: Job ID of the execution
            group_id: Optional group ID for filtering

        Returns:
            Pending HITLApproval if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        filters = [
            HITLApproval.execution_id == execution_id,
            HITLApproval.status == HITLApprovalStatus.PENDING
        ]
        if group_id:
            filters.append(HITLApproval.group_id == group_id)

        stmt = select(HITLApproval).where(*filters).order_by(desc(HITLApproval.created_at))
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def get_pending_for_group(
        self,
        group_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> tuple[List[HITLApproval], int]:
        """
        Get all pending HITL approvals for a group.

        Args:
            group_id: Group ID for filtering
            limit: Maximum number of items
            offset: Number of items to skip

        Returns:
            Tuple of (list of pending approvals, total count)
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        filters = [
            HITLApproval.group_id == group_id,
            HITLApproval.status == HITLApprovalStatus.PENDING
        ]

        # Get total count
        count_stmt = select(func.count()).select_from(HITLApproval).where(*filters)
        count_result = await self.session.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Get paginated results
        stmt = (
            select(HITLApproval)
            .where(*filters)
            .order_by(desc(HITLApproval.created_at))
            .offset(offset)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        approvals = result.scalars().all()

        return list(approvals), total_count

    async def get_all_for_execution(
        self,
        execution_id: str,
        group_id: Optional[str] = None
    ) -> List[HITLApproval]:
        """
        Get all HITL approvals for an execution.

        Args:
            execution_id: Job ID of the execution
            group_id: Optional group ID for filtering

        Returns:
            List of HITLApproval records
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        filters = [HITLApproval.execution_id == execution_id]
        if group_id:
            filters.append(HITLApproval.group_id == group_id)

        stmt = (
            select(HITLApproval)
            .where(*filters)
            .order_by(HITLApproval.crew_sequence)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def update_status(
        self,
        approval_id: int,
        status: str,
        responded_by: Optional[str] = None,
        approval_comment: Optional[str] = None,
        rejection_reason: Optional[str] = None,
        rejection_action: Optional[str] = None
    ) -> bool:
        """
        Update the status of an HITL approval.

        Args:
            approval_id: ID of the approval
            status: New status
            responded_by: Email of responder
            approval_comment: Comment for approval
            rejection_reason: Reason for rejection
            rejection_action: Action to take on rejection (reject/retry)

        Returns:
            True if successful, False if not found
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        stmt = select(HITLApproval).where(HITLApproval.id == approval_id)
        result = await self.session.execute(stmt)
        approval = result.scalars().first()

        if not approval:
            logger.warning(f"HITL approval {approval_id} not found for status update")
            return False

        approval.status = status
        approval.responded_at = datetime.now(timezone.utc)

        if responded_by:
            approval.responded_by = responded_by
        if approval_comment:
            approval.approval_comment = approval_comment
        if rejection_reason:
            approval.rejection_reason = rejection_reason
        if rejection_action:
            approval.rejection_action = rejection_action

        await self.session.flush()
        logger.info(f"Updated HITL approval {approval_id} status to {status}")
        return True

    async def get_expired_pending(self) -> List[HITLApproval]:
        """
        Get all pending approvals that have expired.

        Returns:
            List of expired pending approvals
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        now = datetime.now(timezone.utc)
        stmt = (
            select(HITLApproval)
            .where(
                HITLApproval.status == HITLApprovalStatus.PENDING,
                HITLApproval.expires_at.isnot(None),
                HITLApproval.expires_at < now
            )
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def mark_webhook_sent(
        self,
        approval_id: int,
        response: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark webhook as sent for an approval.

        Args:
            approval_id: ID of the approval
            response: Optional response from webhook

        Returns:
            True if successful, False if not found
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        stmt = select(HITLApproval).where(HITLApproval.id == approval_id)
        result = await self.session.execute(stmt)
        approval = result.scalars().first()

        if not approval:
            return False

        approval.webhook_sent = True
        approval.webhook_sent_at = datetime.now(timezone.utc)
        if response:
            approval.webhook_response = response

        await self.session.flush()
        return True

    async def delete_by_execution_id(self, execution_id: str) -> int:
        """
        Delete all HITL approvals for an execution.

        Args:
            execution_id: Job ID of the execution

        Returns:
            Number of records deleted
        """
        if not self.session:
            raise RuntimeError("HITLApprovalRepository requires a session")

        from sqlalchemy import delete
        stmt = delete(HITLApproval).where(HITLApproval.execution_id == execution_id)
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount


class HITLWebhookRepository:
    """Repository for HITL webhook data access operations."""

    def __init__(self, session: AsyncSession):
        """Initialize with required session."""
        self.session = session

    async def create(self, webhook: HITLWebhook) -> HITLWebhook:
        """
        Create a new HITL webhook.

        Args:
            webhook: HITLWebhook instance to create

        Returns:
            Created HITLWebhook with ID
        """
        if not self.session:
            raise RuntimeError("HITLWebhookRepository requires a session")

        self.session.add(webhook)
        await self.session.flush()
        await self.session.refresh(webhook)
        logger.info(f"Created HITL webhook {webhook.id} for group {webhook.group_id}")
        return webhook

    async def get_by_id(
        self,
        webhook_id: int,
        group_id: Optional[str] = None
    ) -> Optional[HITLWebhook]:
        """
        Get an HITL webhook by ID.

        Args:
            webhook_id: ID of the webhook
            group_id: Optional group ID for filtering

        Returns:
            HITLWebhook if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("HITLWebhookRepository requires a session")

        filters = [HITLWebhook.id == webhook_id]
        if group_id:
            filters.append(HITLWebhook.group_id == group_id)

        stmt = select(HITLWebhook).where(*filters)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def get_for_group(
        self,
        group_id: str,
        enabled_only: bool = True
    ) -> List[HITLWebhook]:
        """
        Get all webhooks for a group.

        Args:
            group_id: Group ID for filtering
            enabled_only: If True, only return enabled webhooks

        Returns:
            List of HITLWebhook records
        """
        if not self.session:
            raise RuntimeError("HITLWebhookRepository requires a session")

        filters = [HITLWebhook.group_id == group_id]
        if enabled_only:
            filters.append(HITLWebhook.enabled == True)

        stmt = select(HITLWebhook).where(*filters).order_by(HITLWebhook.name)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_for_event(
        self,
        group_id: str,
        event: str,
        flow_id: Optional[str] = None
    ) -> List[HITLWebhook]:
        """
        Get webhooks that should be triggered for a specific event.

        Args:
            group_id: Group ID for filtering
            event: Event name (gate_reached, gate_approved, etc.)
            flow_id: Optional flow ID to filter webhooks
                     - Returns webhooks with matching flow_id OR no flow_id (global)

        Returns:
            List of webhooks subscribed to the event
        """
        if not self.session:
            raise RuntimeError("HITLWebhookRepository requires a session")

        # Get all enabled webhooks for the group
        webhooks = await self.get_for_group(group_id, enabled_only=True)

        # Filter by event subscription and flow_id
        matching_webhooks = []
        for w in webhooks:
            # Check if webhook is subscribed to this event
            if event not in (w.events or []):
                continue

            # Check flow_id matching:
            # - If webhook has no flow_id (null), it applies to ALL flows (global webhook)
            # - If webhook has flow_id, it only applies to that specific flow
            if w.flow_id is None or w.flow_id == flow_id:
                matching_webhooks.append(w)

        return matching_webhooks

    async def update(
        self,
        webhook_id: int,
        updates: Dict[str, Any],
        group_id: Optional[str] = None
    ) -> bool:
        """
        Update an HITL webhook.

        Args:
            webhook_id: ID of the webhook
            updates: Dictionary of fields to update
            group_id: Optional group ID for filtering

        Returns:
            True if successful, False if not found
        """
        if not self.session:
            raise RuntimeError("HITLWebhookRepository requires a session")

        webhook = await self.get_by_id(webhook_id, group_id)
        if not webhook:
            return False

        for key, value in updates.items():
            if hasattr(webhook, key):
                setattr(webhook, key, value)

        webhook.updated_at = datetime.now(timezone.utc)
        await self.session.flush()
        return True

    async def delete(
        self,
        webhook_id: int,
        group_id: Optional[str] = None
    ) -> bool:
        """
        Delete an HITL webhook.

        Args:
            webhook_id: ID of the webhook
            group_id: Optional group ID for filtering

        Returns:
            True if deleted, False if not found
        """
        if not self.session:
            raise RuntimeError("HITLWebhookRepository requires a session")

        webhook = await self.get_by_id(webhook_id, group_id)
        if not webhook:
            return False

        await self.session.delete(webhook)
        await self.session.flush()
        logger.info(f"Deleted HITL webhook {webhook_id}")
        return True
