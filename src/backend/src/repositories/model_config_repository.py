from typing import Any, List, Optional, cast

from sqlalchemy import ColumnElement, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.model_config import ModelConfig


class ModelConfigRepository(BaseRepository[ModelConfig]):
    """
    Repository for ModelConfig with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(ModelConfig, session)

    async def find_all(self) -> List[ModelConfig]:
        """
        Find all model configurations.

        Returns:
            List of all model configurations ordered by key
        """
        query = select(self.model).order_by(self.model.key)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_key(
        self, key: str, group_id: Optional[str] = None
    ) -> Optional[ModelConfig]:
        """The row ``key`` resolves to for one teamspace.

        That teamspace's own row (its override) when ``group_id`` is given and
        one exists, else the global row. Nothing else is returned. Without
        ``group_id`` it is the global row only. Ordered, so the answer never
        depends on the order the database returns rows in.
        """
        scope: ColumnElement[bool] = self.model.group_id.is_(None)
        if group_id:
            scope = scope | (self.model.group_id == group_id)
        query = (
            select(self.model)
            .where((self.model.key == key) & scope)
            .order_by(self.model.group_id.is_(None), self.model.id)
            .limit(1)
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_by_key_and_group(
        self, key: str, group_id: str
    ) -> Optional[ModelConfig]:
        """
        Find a model configuration by key and group_id.

        Args:
            key: Model configuration key to search for
            group_id: Group ID to filter by

        Returns:
            ModelConfig if found, else None
        """
        query = (
            select(self.model)
            .where((self.model.key == key) & (self.model.group_id == group_id))
            .order_by(self.model.id)
            .limit(1)
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_enabled_models(self) -> List[ModelConfig]:
        """
        Find all enabled model configurations.

        Returns:
            List of enabled model configurations ordered by key
        """
        query = (
            select(self.model)
            .where(self.model.enabled.is_(True))
            .order_by(self.model.key)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def toggle_enabled(self, key: str, enabled: bool) -> bool:
        """
        Toggle the enabled status of the global row for ``key``. Avoid using
        this for group-specific toggles; use toggle_enabled_in_group instead.
        """
        try:
            model = await self.find_by_key(key)
            if not model:
                return False
            model.enabled = enabled
            await self.session.flush()
            return True
        except Exception as e:
            import logging

            logging.error(f"Error in toggle_enabled for {key}: {str(e)}")
            await self.session.rollback()
            raise

    async def toggle_enabled_in_group(
        self, key: str, group_id: str, enabled: bool
    ) -> bool:
        """
        Toggle the enabled status for a specific model within a specific group.

        Args:
            key: Model key
            group_id: Group identifier
            enabled: New enabled state

        Returns:
            True if a row was found for that key+group and updated; False otherwise
        """
        try:
            model = await self.find_by_key_and_group(key, group_id)
            if not model:
                return False
            model.enabled = enabled
            await self.session.flush()
            return True
        except Exception as e:
            import logging

            logging.error(
                f"Error in toggle_enabled_in_group for {key} in group {group_id}: {str(e)}"
            )
            await self.session.rollback()
            raise

    async def enable_all_models(self) -> bool:
        """
        Enable all model configurations with a single DML operation.

        Returns:
            True if the operation was successful
        """
        try:
            # Use SQLAlchemy update with proper boolean values
            stmt = (
                update(self.model)
                .where(self.model.enabled == False)
                .values(enabled=True)
            )
            await self.session.execute(stmt)

            # Only flush, don't commit - let the session dependency handle it
            await self.session.flush()

            # Return success
            return True

        except Exception as e:
            # Log the error and rollback
            import logging

            logging.error(f"Error in enable_all_models: {str(e)}")
            await self.session.rollback()
            raise

    async def disable_all_models(self) -> bool:
        """
        Disable all model configurations with a single DML operation.

        Returns:
            True if the operation was successful
        """
        try:
            # Use SQLAlchemy update with proper boolean values
            stmt = (
                update(self.model)
                .where(self.model.enabled == True)
                .values(enabled=False)
            )
            await self.session.execute(stmt)

            # Only flush, don't commit - let the session dependency handle it
            await self.session.flush()

            # Return success
            return True

        except Exception as e:
            # Log the error and rollback
            import logging

            logging.error(f"Error in disable_all_models: {str(e)}")
            await self.session.rollback()
            raise

    async def delete_by_key(self, key: str) -> bool:
        """
        Delete a catalog model: its global row and every teamspace's override
        of it (an override of a model that no longer exists means nothing).

        Args:
            key: Key of the model configuration to delete

        Returns:
            True if the global row was found and deleted, False otherwise
        """
        import logging

        from sqlalchemy import delete as sql_delete

        logger = logging.getLogger(__name__)

        try:
            logger.debug(f"Attempting to delete model with key: {key}")

            # Find the model first
            model = await self.find_by_key(key)
            if not model:
                logger.warning(f"Model with key {key} not found for deletion")
                return False

            model_id = model.id
            logger.debug(
                f"Found model with key {key} (ID: {model_id}), proceeding with deletion"
            )

            # Use SQL DELETE statement instead of ORM delete
            # This ensures the DELETE is actually executed
            stmt = sql_delete(self.model).where(self.model.key == key)
            result = await self.session.execute(stmt)
            logger.debug(
                f"Executed SQL DELETE for model key={key}, rows affected: {cast('CursorResult[Any]', result).rowcount}"
            )

            # Flush to ensure the delete is sent to the database
            await self.session.flush()
            logger.debug("Flushed session after SQL DELETE")

            logger.info(f"Successfully deleted model with key {key} (ID: {model_id})")
            return cast("CursorResult[Any]", result).rowcount > 0

        except Exception as e:
            logger.error(f"Error deleting model with key {key}: {str(e)}")
            import traceback

            logger.error(traceback.format_exc())
            await self.session.rollback()
            raise

    async def find_all_global(self) -> List[ModelConfig]:
        """Find all global (system-wide) model configurations (group_id IS NULL)."""
        query = (
            select(self.model)
            .where(self.model.group_id.is_(None))
            .order_by(self.model.key)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_global_by_key(self, key: str) -> Optional[ModelConfig]:
        """Find a global (system-wide) model configuration by key."""
        return await self.find_by_key(key)

    async def toggle_global_enabled(self, key: str, enabled: bool) -> bool:
        """Toggle enabled for the global model with the given key. Returns True if updated."""
        model = await self.find_global_by_key(key)
        if not model:
            return False
        model.enabled = enabled
        await self.session.flush()
        return True
