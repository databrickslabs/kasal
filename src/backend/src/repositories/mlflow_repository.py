import logging
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.databricks_config import DatabricksConfig
from src.repositories.databricks_config_repository import DatabricksConfigRepository

logger = logging.getLogger(__name__)


class MLflowRepository:
    """
    Repository to manage MLflow-related persistence via DatabricksConfig table.
    We purposely don't create a separate table; MLflow enablement is a field on DatabricksConfig.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self.dbx_repo = DatabricksConfigRepository(session)
        self._base_repo = BaseRepository(DatabricksConfig, session)

    async def is_enabled(self, group_id: Optional[str] = None) -> bool:
        cfg = await self.dbx_repo.get_active_config(group_id=group_id)
        return bool(getattr(cfg, "mlflow_enabled", False)) if cfg else False

    async def set_enabled(self, enabled: bool, group_id: Optional[str] = None) -> bool:
        cfg = await self.dbx_repo.get_active_config(group_id=group_id)
        if not cfg:
            # No config yet; nothing to update.
            return False
        updated = await self._base_repo.update(cfg.id, {"mlflow_enabled": enabled})
        return bool(updated)

    # Evaluation toggle helpers
    async def is_evaluation_enabled(self, group_id: Optional[str] = None) -> bool:
        cfg = await self.dbx_repo.get_active_config(group_id=group_id)
        return bool(getattr(cfg, "evaluation_enabled", False)) if cfg else False

    async def set_evaluation_enabled(self, enabled: bool, group_id: Optional[str] = None) -> bool:
        cfg = await self.dbx_repo.get_active_config(group_id=group_id)
        if not cfg:
            return False
        updated = await self._base_repo.update(cfg.id, {"evaluation_enabled": enabled})
        return bool(updated)


    async def get_evaluation_judge_model(self, group_id: Optional[str] = None) -> Optional[str]:
        """Return the configured Databricks judge model route if set."""
        cfg = await self.dbx_repo.get_active_config(group_id=group_id)
        if not cfg:
            return None
        val = getattr(cfg, "evaluation_judge_model", None)
        return val if isinstance(val, str) and val.strip() else None
