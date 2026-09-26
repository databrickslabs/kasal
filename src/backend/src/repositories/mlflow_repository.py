import logging
from typing import Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.mlflow_config import MLflowConfig

logger = logging.getLogger(__name__)


class MLflowRepository:
    """Persistence for MLflow tracing settings.

    These flags used to live on ``DatabricksConfig`` — "we purposely don't create
    a separate table", as the previous docstring here put it. That held while
    MLflow *was* Databricks. Once tracing could also target a local OSS server it
    became a bug rather than a simplification: ``is_enabled`` read the flag off
    the Databricks row and returned False whenever no such row existed, so a
    workspace with no Databricks configuration could never turn MLflow on **by
    construction rather than by choice**. The UI showed the same seam from the
    other side ("Please save Databricks settings first to persist MLflow"), which
    in a dev environment with nothing to save is a dead end.

    Every caller already went through this class, so the storage moved and the
    method signatures did not.

    The old columns on ``databricksconfig`` still exist — the migration copies
    rather than moves — but nothing reads them any more.
    """

    def __init__(self, session: AsyncSession, *, default_enabled: bool = False):
        self.session = session
        self.default_enabled = default_enabled

    async def _get(self, group_id: Optional[str] = None) -> Optional[MLflowConfig]:
        result = await self.session.execute(
            select(MLflowConfig)
            .where(MLflowConfig.group_id == group_id)
            .order_by(MLflowConfig.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _ensure(self, group_id: Optional[str] = None) -> MLflowConfig:
        """The group's row, created on first write.

        Created lazily rather than seeded so a workspace that never touches
        MLflow carries no row — and, unlike the previous implementation, without
        fabricating a Databricks configuration as a side effect of toggling an
        unrelated feature.
        """
        cfg = await self._get(group_id)
        if cfg is not None:
            return cfg
        cfg = MLflowConfig(group_id=group_id, enabled=self.default_enabled)
        self.session.add(cfg)
        await self.session.commit()
        await self.session.refresh(cfg)
        logger.info("Created MLflowConfig for group_id=%s", group_id)
        return cfg

    async def is_enabled(self, group_id: Optional[str] = None) -> bool:
        cfg = await self._get(group_id)
        return bool(cfg.enabled) if cfg else self.default_enabled

    async def set_enabled(self, enabled: bool, group_id: Optional[str] = None) -> bool:
        cfg = await self._ensure(group_id)
        cfg.enabled = enabled
        await self.session.commit()
        return True

    # Evaluation toggle helpers
    async def is_evaluation_enabled(self, group_id: Optional[str] = None) -> bool:
        cfg = await self._get(group_id)
        return bool(cfg.evaluation_enabled) if cfg else False

    async def set_evaluation_enabled(
        self, enabled: bool, group_id: Optional[str] = None
    ) -> bool:
        cfg = await self._ensure(group_id)
        cfg.evaluation_enabled = enabled
        await self.session.commit()
        return True

    async def get_evaluation_judge_model(
        self, group_id: Optional[str] = None
    ) -> Optional[str]:
        """The configured judge endpoint route, or None when unset."""
        cfg = await self._get(group_id)
        if not cfg:
            return None
        val = cfg.evaluation_judge_model
        return val if isinstance(val, str) and val.strip() else None

    async def get_experiment_name(
        self, group_id: Optional[str] = None
    ) -> Optional[str]:
        """The configured experiment, or None to let the backend choose.

        Stored WITHOUT a workspace-path prefix: the Databricks backend adds
        ``/Shared/`` and the local one must not carry it.
        """
        cfg = await self._get(group_id)
        if not cfg:
            return None
        val = cfg.experiment_name
        return val if isinstance(val, str) and val.strip() else None

    async def set_experiment_name(
        self, name: Optional[str], group_id: Optional[str] = None
    ) -> bool:
        cfg = await self._ensure(group_id)
        cfg.experiment_name = (name or "").strip() or None
        await self.session.commit()
        return True

    async def set_evaluation_judge_model(
        self, model: Optional[str], group_id: Optional[str] = None
    ) -> bool:
        cfg = await self._ensure(group_id)
        setattr(cfg, "evaluation_judge_model", (model or "").strip() or None)
        await self.session.commit()
        return True

    async def get_advanced(
        self, group_id: Optional[str] = None
    ) -> Dict[str, Optional[int]]:
        """The Advanced settings; None means "use the built-in default"."""
        cfg = await self._get(group_id)
        return {
            "evaluation_max_rows": getattr(cfg, "evaluation_max_rows", None),
            "optimization_judge_samples": getattr(
                cfg, "optimization_judge_samples", None
            ),
        }

    async def set_advanced(
        self, values: Dict[str, Optional[int]], group_id: Optional[str] = None
    ) -> bool:
        """Set the given Advanced settings; a None value resets one to its default."""
        cfg = await self._ensure(group_id)
        for field in ("evaluation_max_rows", "optimization_judge_samples"):
            if field in values:
                setattr(cfg, field, values[field])
        await self.session.commit()
        return True
