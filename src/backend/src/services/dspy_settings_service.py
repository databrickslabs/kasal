"""
Service for enabling/disabling DSPy per workspace (group).

We persist a boolean flag in the generic database_configs table (LakebaseConfig)
using a namespaced key per group: f"dspy_enabled:{group_id or 'global'}".

This avoids schema migrations and mirrors the MLflow toggle pattern while keeping
it independent from Databricks config.
"""
from __future__ import annotations

from typing import Optional
import os

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.dspy_config_repository import DSPyConfigRepository
from src.services.group_service import GroupService
from src.utils.user_context import GroupContext


class DSPySettingsService:
    def __init__(self, session: AsyncSession, group_id: Optional[str] = None) -> None:
        self.session = session
        self.group_id = group_id
        self.repo = DSPyConfigRepository(session)

    async def is_enabled(self) -> bool:
        """Return whether DSPy is enabled for this group.
        Falls back to env var USE_DSPY_OPTIMIZATION when no record exists.
        """
        val = await self.repo.get_dspy_enabled(self.group_id)
        if isinstance(val, bool):
            return val
        # fallback to env
        return os.getenv("USE_DSPY_OPTIMIZATION", "false").lower() == "true"

    async def set_enabled(self, enabled: bool) -> bool:
        """
        Enable/disable DSPy for this group. Ensures the group row exists to satisfy
        the foreign key on dspy_configs.group_id before persisting the setting.
        """
        # Ensure group exists when a group_id is provided, to avoid FK violations
        if self.group_id:
            try:
                # Minimal context with just the primary group id
                ctx = GroupContext(group_ids=[self.group_id])
                await GroupService(self.session).ensure_group_exists(ctx)
            except Exception:
                # Don't fail the toggle because of auto-create; repo insert will still
                # raise a clear error if FK truly cannot be satisfied
                pass

        return await self.repo.set_dspy_enabled(enabled, self.group_id)

