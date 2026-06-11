"""
Service for crew thumbs feedback (chat-mode votes surfaced in the Agent
Builder catalog). Follows the service → repository pattern.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from src.repositories.crew_feedback_repository import CrewFeedbackRepository
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class CrewFeedbackService:
    def __init__(self, session):
        self.session = session
        self.repository = CrewFeedbackRepository(session)

    async def add_feedback(
        self,
        crew_id: str,
        rating: str,
        comment: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ):
        """Record a vote. Thumbs-down must carry a comment (enforced at the
        schema layer; double-checked here so the rule holds for all callers)."""
        if rating not in ("up", "down"):
            raise ValueError("rating must be 'up' or 'down'")
        if rating == "down" and not (comment or "").strip():
            raise ValueError("a comment explaining what went wrong is required for thumbs-down")

        data: Dict[str, Any] = {
            "id": str(uuid4()),
            "crew_id": str(crew_id),
            "rating": rating,
            "comment": (comment or "").strip() or None,
            "created_at": datetime.utcnow(),
        }
        if group_context:
            data["group_id"] = group_context.primary_group_id
            data["group_email"] = group_context.group_email
        return await self.repository.create(data)

    async def list_for_crew(
        self, crew_id: str, group_context: Optional[GroupContext] = None
    ) -> List[Any]:
        if not group_context or not group_context.group_ids:
            return []
        return await self.repository.list_by_crew_and_group(
            str(crew_id), group_context.group_ids
        )

    async def summary(
        self, group_context: Optional[GroupContext] = None
    ) -> List[Dict[str, Any]]:
        if not group_context or not group_context.group_ids:
            return []
        return await self.repository.summary_by_group(group_context.group_ids)
