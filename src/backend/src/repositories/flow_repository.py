from typing import List, Optional, Union
import uuid
from uuid import UUID
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
# Session import removed - use AsyncSession only

from src.core.base_repository import BaseRepository
from src.models.flow import Flow
# SessionLocal removed - use async_session_factory instead


class FlowRepository(BaseRepository[Flow]):
    """
    Repository for Flow model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(Flow, session)
    
    async def find_by_name(self, name: str) -> Optional[Flow]:
        """
        Find a flow by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Flow if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_by_name_and_group(self, name: str, group_ids: List[str], exclude_id: Optional[UUID] = None) -> Optional[Flow]:
        """
        Find a flow by name within the given groups.

        Args:
            name: Name to search for
            group_ids: List of group IDs to filter by
            exclude_id: Optional flow ID to exclude (for updates)

        Returns:
            Flow if found, else None
        """
        if not group_ids:
            return None

        conditions = [self.model.name == name, self.model.group_id.in_(group_ids)]
        if exclude_id is not None:
            conditions.append(self.model.id != exclude_id)

        query = select(self.model).where(*conditions)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_by_crew_id(self, crew_id: Union[uuid.UUID, str]) -> List[Flow]:
        """
        Find all flows for a specific crew.
        
        Args:
            crew_id: ID of the crew (UUID)
            
        Returns:
            List of flows associated with the crew
        """
        # Convert string to UUID if needed
        if isinstance(crew_id, str):
            try:
                crew_id = uuid.UUID(crew_id)
            except ValueError:
                return []
                
        query = select(self.model).where(self.model.crew_id == crew_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def find_all(self) -> List[Flow]:
        """
        Find all flows.
        
        Returns:
            List of all flows
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def delete_with_executions(self, flow_id: uuid.UUID) -> bool:
        """
        Delete a flow and all its related execution records to handle foreign key constraints.
        
        Args:
            flow_id: UUID of the flow to delete
            
        Returns:
            True if flow was deleted, False if not found
        """
        import logging
        logger = logging.getLogger(__name__)

        # Check if the flow exists
        flow = await self.get(flow_id)
        if not flow:
            logger.warning(f"Flow with ID {flow_id} not found for deletion")
            return False
        
        try:
            # Delete all flow executions from executionhistory table
            exec_delete_query = text("""
            DELETE FROM executionhistory
            WHERE flow_id = :flow_id AND execution_type = 'flow'
            """)
            result = await self.session.execute(exec_delete_query, {"flow_id": flow_id})
            deleted_count = result.rowcount
            if deleted_count > 0:
                logger.info(f"Deleted {deleted_count} flow executions for flow {flow_id}")
            
            # Now delete the flow
            flow_delete_query = text("""
            DELETE FROM flows WHERE id = :flow_id
            """)
            result = await self.session.execute(flow_delete_query, {"flow_id": flow_id})
            
            # Flush all changes
            await self.session.flush()

            logger.info(f"Successfully deleted flow {flow_id} and all its executions")
            return True
            
        except Exception as e:
            # Roll back on error
            await self.session.rollback()
            logger.error(f"Error during cascading deletion of flow {flow_id}: {str(e)}")
            raise
    
    async def delete_all(self) -> None:
        """
        Delete all flows, handling foreign key constraints by deleting related records first.
        
        Returns:
            None
        """
        import logging
        logger = logging.getLogger(__name__)

        try:
            # Delete all flow executions from executionhistory table
            exec_delete_query = text("""
            DELETE FROM executionhistory WHERE execution_type = 'flow'
            """)
            await self.session.execute(exec_delete_query)
            logger.info("Deleted all flow executions from executionhistory")

            # Delete all flows
            flow_delete_query = text("""
            DELETE FROM flows
            """)
            await self.session.execute(flow_delete_query)
            logger.info("Deleted all flows")

            # Flush the changes
            await self.session.flush()

        except Exception as e:
            # Roll back on error
            await self.session.rollback()
            logger.error(f"Error during delete_all operation: {str(e)}")
            raise

# SyncFlowRepository removed - use async FlowRepository instead
# All database operations must be async 