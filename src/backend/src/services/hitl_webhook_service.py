"""
Service for HITL Webhook operations.

This module provides functionality for sending webhook notifications
when HITL events occur (gate reached, approved, rejected, timeout).
"""

import logging
import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from src.models.hitl_approval import HITLApproval, HITLWebhook
from src.repositories.hitl_repository import HITLApprovalRepository, HITLWebhookRepository
from src.schemas.hitl import (
    HITLWebhookCreate,
    HITLWebhookUpdate,
    HITLWebhookResponse,
    HITLWebhookListResponse,
    HITLWebhookPayload,
    HITLWebhookEventEnum,
    HITLApprovalStatusEnum
)

logger = logging.getLogger(__name__)

# Timeout for webhook HTTP requests
WEBHOOK_TIMEOUT_SECONDS = 30


class HITLWebhookServiceError(Exception):
    """Base exception for webhook service errors."""
    pass


class HITLWebhookNotFoundError(HITLWebhookServiceError):
    """Raised when a webhook is not found."""
    pass


class HITLWebhookService:
    """Service for HITL webhook management and notifications."""

    def __init__(
        self,
        session: AsyncSession,
        webhook_repository: Optional[HITLWebhookRepository] = None,
        approval_repository: Optional[HITLApprovalRepository] = None
    ):
        """
        Initialize the service with session and repositories.

        Args:
            session: Database session from FastAPI DI
            webhook_repository: Optional repository instance (for testing)
            approval_repository: Optional approval repository instance (for testing)
        """
        self.session = session
        self.webhook_repo = webhook_repository or HITLWebhookRepository(session)
        self.approval_repo = approval_repository or HITLApprovalRepository(session)

    # =========================================================================
    # Webhook CRUD Operations
    # =========================================================================

    async def create_webhook(
        self,
        webhook_data: HITLWebhookCreate,
        group_id: str
    ) -> HITLWebhookResponse:
        """
        Create a new HITL webhook.

        Args:
            webhook_data: Webhook creation data
            group_id: Group ID for the webhook

        Returns:
            Created webhook response
        """
        try:
            webhook = HITLWebhook(
                group_id=group_id,
                flow_id=webhook_data.flow_id,  # Optional: scope to specific flow
                name=webhook_data.name,
                url=webhook_data.url,
                enabled=webhook_data.enabled,
                events=[e.value for e in webhook_data.events],
                headers=webhook_data.headers,
                secret=webhook_data.secret
            )

            created_webhook = await self.webhook_repo.create(webhook)

            logger.info(f"Created HITL webhook {created_webhook.id} for group {group_id}")

            return HITLWebhookResponse(
                id=created_webhook.id,
                group_id=created_webhook.group_id,
                flow_id=created_webhook.flow_id,
                name=created_webhook.name,
                url=created_webhook.url,
                enabled=created_webhook.enabled,
                events=[HITLWebhookEventEnum(e) for e in created_webhook.events],
                headers=created_webhook.headers,
                created_at=created_webhook.created_at,
                updated_at=created_webhook.updated_at
            )

        except SQLAlchemyError as e:
            logger.error(f"Database error creating webhook: {str(e)}")
            raise HITLWebhookServiceError(f"Failed to create webhook: {str(e)}")

    async def get_webhook(
        self,
        webhook_id: int,
        group_id: str
    ) -> HITLWebhookResponse:
        """
        Get a webhook by ID.

        Args:
            webhook_id: ID of the webhook
            group_id: Group ID for authorization

        Returns:
            Webhook response

        Raises:
            HITLWebhookNotFoundError: If webhook not found
        """
        webhook = await self.webhook_repo.get_by_id(webhook_id, group_id)
        if not webhook:
            raise HITLWebhookNotFoundError(f"Webhook {webhook_id} not found")

        return HITLWebhookResponse(
            id=webhook.id,
            group_id=webhook.group_id,
            flow_id=webhook.flow_id,
            name=webhook.name,
            url=webhook.url,
            enabled=webhook.enabled,
            events=[HITLWebhookEventEnum(e) for e in webhook.events],
            headers=webhook.headers,
            created_at=webhook.created_at,
            updated_at=webhook.updated_at
        )

    async def list_webhooks(
        self,
        group_id: str
    ) -> HITLWebhookListResponse:
        """
        List all webhooks for a group.

        Args:
            group_id: Group ID for filtering

        Returns:
            List of webhooks
        """
        webhooks = await self.webhook_repo.get_for_group(group_id, enabled_only=False)

        items = [
            HITLWebhookResponse(
                id=w.id,
                group_id=w.group_id,
                flow_id=w.flow_id,
                name=w.name,
                url=w.url,
                enabled=w.enabled,
                events=[HITLWebhookEventEnum(e) for e in w.events],
                headers=w.headers,
                created_at=w.created_at,
                updated_at=w.updated_at
            )
            for w in webhooks
        ]

        return HITLWebhookListResponse(
            items=items,
            total=len(items)
        )

    async def update_webhook(
        self,
        webhook_id: int,
        webhook_data: HITLWebhookUpdate,
        group_id: str
    ) -> HITLWebhookResponse:
        """
        Update a webhook.

        Args:
            webhook_id: ID of the webhook
            webhook_data: Update data
            group_id: Group ID for authorization

        Returns:
            Updated webhook response

        Raises:
            HITLWebhookNotFoundError: If webhook not found
        """
        # Build updates dict
        updates = {}
        if webhook_data.name is not None:
            updates["name"] = webhook_data.name
        if webhook_data.url is not None:
            updates["url"] = webhook_data.url
        if webhook_data.flow_id is not None:
            updates["flow_id"] = webhook_data.flow_id
        if webhook_data.enabled is not None:
            updates["enabled"] = webhook_data.enabled
        if webhook_data.events is not None:
            updates["events"] = [e.value for e in webhook_data.events]
        if webhook_data.headers is not None:
            updates["headers"] = webhook_data.headers
        if webhook_data.secret is not None:
            updates["secret"] = webhook_data.secret

        success = await self.webhook_repo.update(webhook_id, updates, group_id)
        if not success:
            raise HITLWebhookNotFoundError(f"Webhook {webhook_id} not found")

        # Return updated webhook
        return await self.get_webhook(webhook_id, group_id)

    async def delete_webhook(
        self,
        webhook_id: int,
        group_id: str
    ) -> bool:
        """
        Delete a webhook.

        Args:
            webhook_id: ID of the webhook
            group_id: Group ID for authorization

        Returns:
            True if deleted

        Raises:
            HITLWebhookNotFoundError: If webhook not found
        """
        success = await self.webhook_repo.delete(webhook_id, group_id)
        if not success:
            raise HITLWebhookNotFoundError(f"Webhook {webhook_id} not found")

        logger.info(f"Deleted HITL webhook {webhook_id}")
        return True

    # =========================================================================
    # Webhook Notification Methods
    # =========================================================================

    async def send_gate_reached_notification(
        self,
        approval: HITLApproval,
        approval_url: Optional[str] = None
    ) -> bool:
        """
        Send webhook notification when a gate is reached.

        Args:
            approval: The HITL approval that was created
            approval_url: Optional direct URL to approval page

        Returns:
            True if at least one webhook was sent successfully
        """
        return await self._send_notification(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_REACHED,
            approval_url=approval_url
        )

    async def send_gate_approved_notification(
        self,
        approval: HITLApproval
    ) -> bool:
        """
        Send webhook notification when a gate is approved.

        Args:
            approval: The approved HITL approval

        Returns:
            True if at least one webhook was sent successfully
        """
        return await self._send_notification(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_APPROVED
        )

    async def send_gate_rejected_notification(
        self,
        approval: HITLApproval
    ) -> bool:
        """
        Send webhook notification when a gate is rejected.

        Args:
            approval: The rejected HITL approval

        Returns:
            True if at least one webhook was sent successfully
        """
        return await self._send_notification(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_REJECTED
        )

    async def send_gate_timeout_notification(
        self,
        approval: HITLApproval
    ) -> bool:
        """
        Send webhook notification when a gate times out.

        Args:
            approval: The timed out HITL approval

        Returns:
            True if at least one webhook was sent successfully
        """
        return await self._send_notification(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_TIMEOUT
        )

    # =========================================================================
    # Private Methods
    # =========================================================================

    async def _send_notification(
        self,
        approval: HITLApproval,
        event: HITLWebhookEventEnum,
        approval_url: Optional[str] = None
    ) -> bool:
        """
        Send webhook notification for an event.

        Args:
            approval: The HITL approval
            event: Event type
            approval_url: Optional approval URL

        Returns:
            True if at least one webhook was sent successfully
        """
        try:
            # Get webhooks subscribed to this event, filtered by flow_id
            webhooks = await self.webhook_repo.get_for_event(
                group_id=approval.group_id,
                event=event.value,
                flow_id=approval.flow_id  # Filter to flow-specific or global webhooks
            )

            if not webhooks:
                logger.debug(f"No webhooks configured for event {event.value} on flow {approval.flow_id}")
                return True  # No webhooks is not an error

            # Build payload
            payload = self._build_payload(approval, event, approval_url)

            # Send to each webhook
            success_count = 0
            for webhook in webhooks:
                try:
                    response = await self._send_webhook(webhook, payload)
                    if response:
                        success_count += 1

                        # Mark webhook sent for gate_reached event
                        if event == HITLWebhookEventEnum.GATE_REACHED:
                            await self.approval_repo.mark_webhook_sent(
                                approval_id=approval.id,
                                response=response
                            )

                except Exception as e:
                    logger.error(
                        f"Error sending webhook {webhook.id} for event {event.value}: {str(e)}"
                    )

            logger.info(
                f"Sent {success_count}/{len(webhooks)} webhooks for event {event.value} "
                f"on approval {approval.id}"
            )

            return success_count > 0

        except Exception as e:
            logger.error(f"Error sending notifications: {str(e)}")
            return False

    def _build_payload(
        self,
        approval: HITLApproval,
        event: HITLWebhookEventEnum,
        approval_url: Optional[str] = None
    ) -> HITLWebhookPayload:
        """Build webhook payload from approval."""
        # Truncate output preview
        output_preview = None
        if approval.previous_crew_output:
            output_preview = approval.previous_crew_output[:500]
            if len(approval.previous_crew_output) > 500:
                output_preview += "..."

        return HITLWebhookPayload(
            event=event,
            timestamp=datetime.now(timezone.utc),
            approval_id=approval.id,
            execution_id=approval.execution_id,
            flow_id=approval.flow_id,
            gate_node_id=approval.gate_node_id,
            message=approval.message,
            previous_crew_name=approval.previous_crew_name,
            previous_crew_output_preview=output_preview,
            status=(
                HITLApprovalStatusEnum(approval.status)
                if approval.status != "pending" else None
            ),
            responded_by=approval.responded_by,
            approval_url=approval_url,
            expires_at=approval.expires_at
        )

    async def _send_webhook(
        self,
        webhook: HITLWebhook,
        payload: HITLWebhookPayload
    ) -> Optional[Dict[str, Any]]:
        """
        Send HTTP request to webhook URL.

        Args:
            webhook: The webhook configuration
            payload: The payload to send

        Returns:
            Response data if successful, None otherwise
        """
        try:
            # Serialize payload
            payload_dict = payload.model_dump(mode="json")
            payload_json = json.dumps(payload_dict)

            # Build headers
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Kasal-HITL-Webhook/1.0"
            }

            # Add custom headers
            if webhook.headers:
                headers.update(webhook.headers)

            # Add signature if secret is configured
            if webhook.secret:
                signature = self._generate_signature(payload_json, webhook.secret)
                headers["X-Kasal-Signature"] = signature

            # Send request
            async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    webhook.url,
                    content=payload_json,
                    headers=headers
                )

                if response.is_success:
                    logger.info(
                        f"Webhook {webhook.id} sent successfully to {webhook.url}"
                    )
                    return {
                        "status_code": response.status_code,
                        "success": True
                    }
                else:
                    logger.warning(
                        f"Webhook {webhook.id} returned status {response.status_code}"
                    )
                    return {
                        "status_code": response.status_code,
                        "success": False,
                        "error": response.text[:500] if response.text else None
                    }

        except httpx.TimeoutException:
            logger.error(f"Webhook {webhook.id} timed out")
            return None
        except httpx.RequestError as e:
            logger.error(f"Webhook {webhook.id} request error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Webhook {webhook.id} unexpected error: {str(e)}")
            return None

    def _generate_signature(self, payload: str, secret: str) -> str:
        """
        Generate HMAC-SHA256 signature for webhook payload.

        Args:
            payload: JSON payload string
            secret: Webhook secret

        Returns:
            Hex-encoded signature
        """
        return hmac.new(
            secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
