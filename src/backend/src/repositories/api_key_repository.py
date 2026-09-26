from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.api_key import ApiKey


class ApiKeyRepository(BaseRepository[ApiKey]):
    """
    Repository for ApiKey model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(ApiKey, session)

    async def find_by_name(
        self, name: str, group_id: Optional[str] = None
    ) -> Optional[ApiKey]:
        """
        Find an API key by name, optionally filtered by group.

        Args:
            name: Name to search for
            group_id: Optional group ID to filter by

        Returns:
            ApiKey if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)
        result = await self.session.execute(query)
        return result.scalars().first()

    def find_by_name_sync(
        self, name: str, group_id: Optional[str] = None
    ) -> Optional[ApiKey]:
        """
        Find an API key by name synchronously, optionally filtered by group.

        Args:
            name: Name to search for
            group_id: Optional group ID to filter by

        Returns:
            ApiKey if found, else None
        """
        if not isinstance(self.session, Session):
            raise TypeError(
                "Session must be a synchronous SQLAlchemy Session for find_by_name_sync"
            )

        query = select(self.model).where(self.model.name == name)
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)
        result = self.session.execute(query)
        return result.scalars().first()

    async def find_all(self, group_id: Optional[str] = None) -> List[ApiKey]:
        """
        Find all API keys, optionally filtered by group.

        Args:
            group_id: Optional group ID to filter by

        Returns:
            List of all API keys
        """
        query = select(self.model)
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def delete(self, id: int) -> bool:
        """
        Override delete method to ensure proper deletion of API keys.

        Args:
            id: ID of the API key to delete

        Returns:
            True if deleted, False if not found
        """
        import logging

        logger = logging.getLogger(__name__)

        try:
            logger.debug(f"Deleting ApiKey with ID {id}")

            # Use a direct SQL delete statement for reliability
            result = await self.session.execute(delete(ApiKey).where(ApiKey.id == id))

            # Flush to ensure changes are visible
            await self.session.flush()

            # Check if any rows were deleted
            if result.rowcount > 0:
                logger.debug(f"Successfully deleted ApiKey with ID {id}")
                return True
            else:
                logger.warning(f"ApiKey with ID {id} not found for deletion")
                return False

        except Exception as e:
            logger.error(f"Error deleting ApiKey with ID {id}: {str(e)}")
            await self.session.rollback()
            raise
