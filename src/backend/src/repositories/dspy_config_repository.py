"""
Repository for DSPy configuration data access.

This repository handles all database operations related to DSPy configurations,
training examples, and optimization runs.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from uuid import UUID
import uuid

import sqlalchemy as sa
from sqlalchemy import select, and_, or_, desc, func, Interval
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models.dspy_config import (
    DSPyConfig,
    DSPyTrainingExample,
    DSPyOptimizationRun,
    DSPyModuleCache
)
from src.schemas.dspy_schemas import (
    OptimizationType,
    DeploymentStage,
    OptimizationStatus,
    ExampleSourceType
)

logger = logging.getLogger(__name__)


class DSPyConfigRepository:
    """Repository for DSPy configuration operations."""

    def __init__(self, session: AsyncSession):
        """Initialize repository with database session."""
        self.session = session

    async def get_active_config(
        self,
        optimization_type: OptimizationType,
        group_id: Optional[str] = None,
        stage: DeploymentStage = DeploymentStage.PRODUCTION
    ) -> Optional[DSPyConfig]:
        """
        Get the active configuration for a specific optimization type.

        Args:
            optimization_type: Type of optimization
            group_id: Optional group ID for multi-tenant isolation
            stage: Deployment stage to filter by

        Returns:
            Active DSPyConfig or None
        """
        query = select(DSPyConfig).where(
            and_(
                DSPyConfig.optimization_type == optimization_type.value,
                DSPyConfig.is_active == True,
                DSPyConfig.deployment_stage == stage.value
            )
        )

        if group_id:
            query = query.where(DSPyConfig.group_id == group_id)

        query = query.order_by(desc(DSPyConfig.version))

        result = await self.session.execute(query)
        return result.scalars().first()

    async def create_config(
        self,
        optimization_type: OptimizationType,
        prompts_json: Dict[str, Any],
        module_config: Dict[str, Any],
        optimizer_config: Dict[str, Any],
        mlflow_run_id: Optional[str] = None,
        mlflow_model_uri: Optional[str] = None,
        group_id: Optional[str] = None
    ) -> DSPyConfig:
        """Create a new DSPy configuration."""
        # Get the latest version number
        version_query = select(func.max(DSPyConfig.version)).where(
            DSPyConfig.optimization_type == optimization_type.value
        )
        if group_id:
            version_query = version_query.where(DSPyConfig.group_id == group_id)

        result = await self.session.execute(version_query)
        max_version = result.scalar() or 0

        config = DSPyConfig(
            optimization_type=optimization_type.value,
            version=max_version + 1,
            prompts_json=prompts_json,
            module_config=module_config,
            optimizer_config=optimizer_config,
            mlflow_run_id=mlflow_run_id,
            mlflow_model_uri=mlflow_model_uri,
            group_id=group_id
        )

        self.session.add(config)
        await self.session.commit()
        await self.session.refresh(config)

        return config

    async def update_config_stage(
        self,
        config_id: str,
        stage: DeploymentStage
    ) -> DSPyConfig:
        """Update the deployment stage of a configuration."""
        query = select(DSPyConfig).where(DSPyConfig.id == config_id)
        result = await self.session.execute(query)
        config = result.scalars().first()

        if config:
            config.deployment_stage = stage.value
            config.updated_at = datetime.utcnow()
            if stage == DeploymentStage.PRODUCTION:
                config.deployed_at = datetime.utcnow()

            await self.session.commit()
            await self.session.refresh(config)

        return config

    async def get_training_examples(
        self,
        optimization_type: OptimizationType,
        group_id: Optional[str] = None,
        min_quality_score: float = 0.0,
        limit: int = 1000,
        hours_back: Optional[int] = None
    ) -> List[DSPyTrainingExample]:
        """Get training examples for optimization."""
        query = select(DSPyTrainingExample).where(
            and_(
                DSPyTrainingExample.optimization_type == optimization_type.value,
                DSPyTrainingExample.quality_score >= min_quality_score
            )
        )

        if group_id:
            query = query.where(DSPyTrainingExample.group_id == group_id)

        if hours_back:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
            query = query.where(DSPyTrainingExample.created_at >= cutoff_time)

        query = query.order_by(desc(DSPyTrainingExample.quality_score)).limit(limit)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def create_training_examples(
        self,
        examples: List[Dict[str, Any]],
        optimization_type: OptimizationType,
        group_id: Optional[str] = None
    ) -> List[DSPyTrainingExample]:
        """Create multiple training examples."""
        db_examples = []

        for example in examples:
            db_example = DSPyTrainingExample(
                optimization_type=optimization_type.value,
                input_data=example['input_data'],
                output_data=example['output_data'],
                quality_score=example.get('quality_score', 0.0),
                trace_id=example.get('trace_id'),
                execution_id=example.get('execution_id'),
                source_type=example.get('source_type', ExampleSourceType.TRACE.value),
                metadata_json=example.get('metadata'),
                group_id=group_id
            )
            self.session.add(db_example)
            db_examples.append(db_example)

        await self.session.commit()

        for db_example in db_examples:
            await self.session.refresh(db_example)

        return db_examples

    async def create_optimization_run(
        self,
        optimization_type: OptimizationType,
        optimizer_type: str,
        optimizer_params: Dict[str, Any],
        group_id: Optional[str] = None,
        triggered_by: str = "api",
        triggered_by_user: Optional[str] = None
    ) -> DSPyOptimizationRun:
        """Create a new optimization run record."""
        run = DSPyOptimizationRun(
            optimization_type=optimization_type.value,
            optimizer_type=optimizer_type,
            optimizer_params=optimizer_params,
            status=OptimizationStatus.PENDING.value,
            started_at=datetime.utcnow(),
            triggered_by=triggered_by,
            triggered_by_user=triggered_by_user,
            group_id=group_id
        )

        self.session.add(run)
        await self.session.commit()
        await self.session.refresh(run)

        return run

    async def update_optimization_run(
        self,
        run_id: str,
        status: OptimizationStatus,
        metrics: Optional[Dict[str, Any]] = None,
        best_score: Optional[float] = None,
        error_message: Optional[str] = None,
        mlflow_run_id: Optional[str] = None,
        num_training_examples: Optional[int] = None,
        num_validation_examples: Optional[int] = None
    ) -> DSPyOptimizationRun:
        """Update an optimization run with results."""
        query = select(DSPyOptimizationRun).where(DSPyOptimizationRun.id == run_id)
        result = await self.session.execute(query)
        run = result.scalars().first()

        if run:
            run.status = status.value
            run.metrics = metrics
            run.best_score = best_score
            run.error_message = error_message
            run.mlflow_run_id = mlflow_run_id
            run.num_training_examples = num_training_examples
            run.num_validation_examples = num_validation_examples

            if status in [OptimizationStatus.COMPLETED, OptimizationStatus.FAILED]:
                run.completed_at = datetime.utcnow()
                run.duration_seconds = int((run.completed_at - run.started_at).total_seconds())

            await self.session.commit()
            await self.session.refresh(run)

        return run

    async def get_recent_optimization_runs(
        self,
        optimization_type: Optional[OptimizationType] = None,
        group_id: Optional[str] = None,
        limit: int = 10
    ) -> List[DSPyOptimizationRun]:
        """Get recent optimization runs."""
        query = select(DSPyOptimizationRun)

        conditions = []
        if optimization_type:
            conditions.append(DSPyOptimizationRun.optimization_type == optimization_type.value)
        if group_id:
            conditions.append(DSPyOptimizationRun.group_id == group_id)

        if conditions:
            query = query.where(and_(*conditions))

        query = query.order_by(desc(DSPyOptimizationRun.started_at)).limit(limit)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_module_cache(
        self,
        optimization_type: OptimizationType,
        group_id: Optional[str] = None
    ) -> Optional[DSPyModuleCache]:
        """Get cached module for an optimization type."""
        cache_key = f"{optimization_type.value}_{group_id or 'global'}"

        query = select(DSPyModuleCache).where(
            DSPyModuleCache.cache_key == cache_key
        )

        result = await self.session.execute(query)
        cache_entry = result.scalars().first()

        if cache_entry:
            # Update access tracking
            cache_entry.last_accessed = datetime.utcnow()
            cache_entry.access_count += 1
            await self.session.commit()

        return cache_entry

    async def save_module_cache(
        self,
        optimization_type: OptimizationType,
        config_version: int,
        module_pickle: str,
        module_hash: str,
        group_id: Optional[str] = None,
        ttl_hours: int = 24
    ) -> DSPyModuleCache:
        """Save module to cache."""
        cache_key = f"{optimization_type.value}_{group_id or 'global'}"

        # Check if cache entry exists
        query = select(DSPyModuleCache).where(
            DSPyModuleCache.cache_key == cache_key
        )
        result = await self.session.execute(query)
        cache_entry = result.scalars().first()

        if cache_entry:
            # Update existing entry
            cache_entry.config_version = config_version
            cache_entry.module_pickle = module_pickle
            cache_entry.module_hash = module_hash
            cache_entry.loaded_at = datetime.utcnow()
            cache_entry.last_accessed = datetime.utcnow()
            cache_entry.ttl_hours = ttl_hours
        else:
            # Create new entry
            cache_entry = DSPyModuleCache(
                optimization_type=optimization_type.value,
                config_version=config_version,
                module_pickle=module_pickle,
                module_hash=module_hash,
                cache_key=cache_key,
                ttl_hours=ttl_hours,
                group_id=group_id
            )
            self.session.add(cache_entry)

        await self.session.commit()
        await self.session.refresh(cache_entry)

        return cache_entry

    async def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries."""
        # Find expired entries - calculate expiration in Python for SQLite compatibility
        query = select(DSPyModuleCache)

        result = await self.session.execute(query)
        all_entries = result.scalars().all()

        # Filter expired entries in Python
        current_time = datetime.utcnow()
        expired_entries = []
        for entry in all_entries:
            if entry.loaded_at:
                expiry_time = entry.loaded_at + timedelta(hours=entry.ttl_hours)
                if expiry_time < current_time:
                    expired_entries.append(entry)

        count = len(expired_entries)
        for entry in expired_entries:
            await self.session.delete(entry)

        if count > 0:
            await self.session.commit()

        return count

    # --- DSPy Settings (enabled flag) stored in DSPyConfig as a special type ---
    async def get_dspy_enabled(self, group_id: Optional[str] = None) -> Optional[bool]:
        """Return the latest enabled flag from DSPyConfig with optimization_type='__settings__'."""
        query = select(DSPyConfig).where(DSPyConfig.optimization_type == "__settings__")
        if group_id:
            query = query.where(DSPyConfig.group_id == group_id)
        query = query.order_by(desc(DSPyConfig.version))
        result = await self.session.execute(query)
        cfg = result.scalars().first()
        if not cfg:
            return None
        try:
            val = (cfg.module_config or {}).get("enabled")
            return bool(val) if isinstance(val, bool) else None
        except Exception:
            return None

    async def set_dspy_enabled(self, enabled: bool, group_id: Optional[str] = None) -> bool:
        """Upsert a DSPyConfig row of type '__settings__' with the enabled flag."""
        # Determine next version
        version_query = select(func.max(DSPyConfig.version)).where(DSPyConfig.optimization_type == "__settings__")
        if group_id:
            version_query = version_query.where(DSPyConfig.group_id == group_id)
        result = await self.session.execute(version_query)
        max_version = result.scalar() or 0

        cfg = DSPyConfig(
            optimization_type="__settings__",
            version=max_version + 1,
            prompts_json={},
            module_config={"enabled": bool(enabled)},
            optimizer_config={},
            deployment_stage=DeploymentStage.PRODUCTION.value,
            is_active=True,
            group_id=group_id,
        )
        self.session.add(cfg)
        await self.session.commit()
        return True
