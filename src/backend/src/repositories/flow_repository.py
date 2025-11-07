from typing import List, Optional, Union
import uuid
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
        from sqlalchemy import text
        logger = logging.getLogger(__name__)
        
        # Check if the flow exists
        flow = await self.get(flow_id)
        if not flow:
            logger.warning(f"Flow with ID {flow_id} not found for deletion")
            return False
        
        try:
            # Step 1: Identify all flow_executions for this flow
            find_executions_query = text("""
            SELECT id FROM flow_executions WHERE flow_id = :flow_id
            """)
            result = await self.session.execute(find_executions_query, {"flow_id": flow_id})
            execution_ids = [row[0] for row in result.fetchall()]
            
            if execution_ids:
                logger.info(f"Found {len(execution_ids)} flow executions to delete for flow {flow_id}")
                
                # Step 2: Delete related flow_node_executions (process in chunks to avoid very large queries)
                chunk_size = 50  # Adjust based on your database performance
                for i in range(0, len(execution_ids), chunk_size):
                    chunk = execution_ids[i:i+chunk_size]
                    placeholders = ", ".join([f":id{j}" for j in range(len(chunk))])
                    params = {f"id{j}": execution_id for j, execution_id in enumerate(chunk)}
                    
                    node_delete_query = text(f"""
                    DELETE FROM flow_node_executions 
                    WHERE flow_execution_id IN ({placeholders})
                    """)
                    await self.session.execute(node_delete_query, params)
                
                logger.info(f"Deleted flow_node_executions for {len(execution_ids)} executions of flow {flow_id}")
                
                # Step 3: Delete all flow_executions
                exec_delete_query = text("""
                DELETE FROM flow_executions WHERE flow_id = :flow_id
                """)
                await self.session.execute(exec_delete_query, {"flow_id": flow_id})
                logger.info(f"Deleted flow_executions for flow {flow_id}")
            
            # Step 4: Now delete the flow
            flow_delete_query = text("""
            DELETE FROM flows WHERE id = :flow_id
            """)
            result = await self.session.execute(flow_delete_query, {"flow_id": flow_id})
            
            # Commit all changes
            await self.session.commit()
            
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
        from sqlalchemy import text
        logger = logging.getLogger(__name__)
        
        try:
            # First delete all flow_node_executions
            node_delete_query = text("""
            DELETE FROM flow_node_executions 
            WHERE flow_execution_id IN (SELECT id FROM flow_executions)
            """)
            await self.session.execute(node_delete_query)
            logger.info("Deleted all flow node executions")
            
            # Then delete all flow_executions
            exec_delete_query = text("""
            DELETE FROM flow_executions
            """)
            await self.session.execute(exec_delete_query)
            logger.info("Deleted all flow executions")
            
            # Finally delete all flows
            flow_delete_query = text("""
            DELETE FROM flows
            """)
            await self.session.execute(flow_delete_query)
            logger.info("Deleted all flows")
            
            # Commit the changes
            await self.session.commit()
            
        except Exception as e:
            # Roll back on error
            await self.session.rollback()
            logger.error(f"Error during delete_all operation: {str(e)}")
            raise

# SyncFlowRepository removed - use async FlowRepository instead
# All database operations must be async 