"""
HITL Timeout Background Service.

This module provides a background task that periodically checks for
expired HITL approvals and processes them according to their timeout action.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Check interval in seconds
HITL_TIMEOUT_CHECK_INTERVAL = 60  # Check every minute


class HITLTimeoutService:
    """
    Background service for processing expired HITL approvals.

    This service runs as a background task and periodically checks for
    HITL approvals that have exceeded their timeout. When found, it
    applies the configured timeout action (auto_reject or fail).
    """

    _instance: Optional["HITLTimeoutService"] = None
    _task: Optional[asyncio.Task] = None
    _running: bool = False

    @classmethod
    def get_instance(cls) -> "HITLTimeoutService":
        """Get or create the singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def start(self) -> None:
        """Start the background timeout checking task."""
        if self._running:
            logger.info("HITL timeout service already running")
            return

        self._running = True
        logger.info("Starting HITL timeout service...")

        while self._running:
            try:
                await self._check_expired_approvals()
            except Exception as e:
                logger.error(f"Error in HITL timeout check: {str(e)}")

            # Wait for next check interval
            try:
                await asyncio.sleep(HITL_TIMEOUT_CHECK_INTERVAL)
            except asyncio.CancelledError:
                logger.info("HITL timeout service cancelled")
                break

        logger.info("HITL timeout service stopped")

    async def stop(self) -> None:
        """Stop the background task."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("HITL timeout service stop requested")

    async def _check_expired_approvals(self) -> None:
        """
        Check for and process expired HITL approvals.

        This method:
        1. Queries for pending approvals past their expiration time
        2. Updates each to TIMEOUT status
        3. Applies the configured timeout action (auto_reject or fail)
        4. Sends webhook notifications for timeouts
        """
        from src.db.session import async_session_factory
        from src.services.hitl_service import HITLService
        from src.services.hitl_webhook_service import HITLWebhookService
        from src.repositories.hitl_repository import HITLApprovalRepository

        try:
            async with async_session_factory() as session:
                approval_repo = HITLApprovalRepository(session)
                hitl_service = HITLService(session, approval_repository=approval_repo)
                webhook_service = HITLWebhookService(session)

                # Get expired pending approvals
                expired_approvals = await approval_repo.get_expired_pending()

                if not expired_approvals:
                    return

                logger.info(f"Found {len(expired_approvals)} expired HITL approvals")

                # Process each expired approval
                processed_ids = await hitl_service.process_expired_approvals()

                # Send webhook notifications for timeouts
                for approval_id in processed_ids:
                    try:
                        approval = await approval_repo.get_by_id(approval_id)
                        if approval:
                            await webhook_service.send_gate_timeout_notification(approval)
                    except Exception as e:
                        logger.error(
                            f"Error sending timeout notification for approval {approval_id}: {e}"
                        )

                await session.commit()

                logger.info(f"Processed {len(processed_ids)} expired HITL approvals")

        except Exception as e:
            logger.error(f"Error checking expired approvals: {str(e)}")


# Global instance access
hitl_timeout_service = HITLTimeoutService.get_instance()


async def start_hitl_timeout_service() -> None:
    """Start the HITL timeout service as a background task."""
    import asyncio

    service = HITLTimeoutService.get_instance()
    service._task = asyncio.create_task(service.start())
    logger.info("HITL timeout service started in background")


async def stop_hitl_timeout_service() -> None:
    """Stop the HITL timeout service."""
    service = HITLTimeoutService.get_instance()
    await service.stop()
