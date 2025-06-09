import json
import logging
import uuid
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from src.models.flow import Flow
from src.repositories.flow_repository import FlowRepository
from src.schemas.flow import FlowCreate, FlowUpdate, FlowResponse

logger = logging.getLogger(__name__)


class FlowService:
    """
    Service for Flow model with business logic.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the service with a database session.
        
        Args:
            session: Database session for operations
        """
        self.session = session
    
    async def create_flow(self, flow_in: FlowCreate) -> Flow:
        """
        Create a new flow.
        
        Args:
            flow_in: Flow data for creation
            
        Returns:
            Created flow
        """
        try:
            # Log details for debugging
            logger.info(f"Creating flow with name: {flow_in.name}")
            logger.info(f"Crew ID: {flow_in.crew_id}")
            logger.info(f"Number of nodes: {len(flow_in.nodes)}")
            logger.info(f"Number of edges: {len(flow_in.edges)}")
            
            # Validate and normalize flow configuration
            flow_config = flow_in.flow_config or {}
            
            # Generate a new UUID for the flow
            flow_uuid = str(uuid.uuid4())
            
            # Ensure required fields exist in flow_config
            flow_config = {
                "id": flow_uuid,
                "name": flow_config.get("name", flow_in.name),
                "type": flow_config.get("type", "default"),
                "listeners": flow_config.get("listeners", []),
                "actions": flow_config.get("actions", []),
                "startingPoints": flow_config.get("startingPoints", [])
            }
            
            # Validate listeners
            for listener in flow_config["listeners"]:
                if not isinstance(listener, dict):
                    raise ValueError(f"Invalid listener format: {listener}")
                required_fields = ["id", "name", "crewId"]
                missing_fields = [field for field in required_fields if field not in listener]
                if missing_fields:
                    raise ValueError(f"Missing required fields in listener: {missing_fields}")
            
            # Validate actions
            for action in flow_config["actions"]:
                if not isinstance(action, dict):
                    raise ValueError(f"Invalid action format: {action}")
                required_fields = ["id", "crewId", "taskId"]
                missing_fields = [field for field in required_fields if field not in action]
                if missing_fields:
                    raise ValueError(f"Missing required fields in action: {missing_fields}")
            
            # Create flow dictionary with validated data
            flow_dict = {
                "name": flow_in.name,
                "crew_id": flow_in.crew_id,
                "nodes": [node.model_dump() for node in flow_in.nodes],
                "edges": [edge.model_dump() for edge in flow_in.edges],
                "flow_config": flow_config
            }
            
            repository = FlowRepository(self.session)
            flow = await repository.create(flow_dict)
            
            logger.info(f"Successfully created flow with ID: {flow.id}")
            return flow
            
        except ValueError as ve:
            logger.error(f"Validation error while creating flow: {str(ve)}")
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            logger.error(f"Error creating flow: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error creating flow: {str(e)}")
    
    async def get_flow(self, flow_id: uuid.UUID) -> Flow:
        """
        Get a flow by ID.
        
        Args:
            flow_id: UUID of the flow to get
            
        Returns:
            Flow if found, else raises HTTPException
        """
        repository = FlowRepository(self.session)
        flow = await repository.get(flow_id)
        
        if not flow:
            raise HTTPException(status_code=404, detail="Flow not found")
        
        return flow
    
    async def get_all_flows(self) -> List[Flow]:
        """
        Get all flows.
        
        Returns:
            List of all flows
        """
        repository = FlowRepository(self.session)
        return await repository.find_all()
    
    async def get_flows_by_crew(self, crew_id: Union[uuid.UUID, str]) -> List[Flow]:
        """
        Get all flows for a specific crew.
        
        Args:
            crew_id: ID of the crew (UUID)
            
        Returns:
            List of flows for the crew
        """
        # Convert string to UUID if needed
        if isinstance(crew_id, str):
            try:
                crew_id = uuid.UUID(crew_id)
            except ValueError:
                # Return empty list if the UUID is invalid
                return []
                
        repository = FlowRepository(self.session)
        return await repository.find_by_crew_id(crew_id)
    
    async def update_flow(self, flow_id: uuid.UUID, flow_in: FlowUpdate) -> Flow:
        """
        Update a flow.
        
        Args:
            flow_id: UUID of the flow to update
            flow_in: Flow data for update
            
        Returns:
            Updated flow if found, else raises HTTPException
        """
        try:
            repository = FlowRepository(self.session)
            flow = await repository.get(flow_id)
            
            if not flow:
                raise HTTPException(status_code=404, detail="Flow not found")
            
            # Log the incoming flow data for debugging
            logger.info(f"Updating flow {flow_id} with name: {flow_in.name}")
            
            # Process flow_config if provided
            if flow_in.flow_config is not None:
                logger.info(f"Flow config provided: {type(flow_in.flow_config)}")
                
                # Check for actions specifically
                if 'actions' not in flow_in.flow_config:
                    logger.info("Adding empty actions array to flow_config")
                    flow_in.flow_config['actions'] = []
            
            # Create update data
            update_data = {
                "name": flow_in.name,
                "updated_at": datetime.now()
            }
            
            if flow_in.flow_config is not None:
                update_data["flow_config"] = flow_in.flow_config
            
            # Update the flow
            updated_flow = await repository.update(flow_id, update_data)
            return updated_flow
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating flow: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error updating flow: {str(e)}")
    
    async def delete_flow(self, flow_id: uuid.UUID) -> bool:
        """
        Delete a flow.
        
        Args:
            flow_id: UUID of the flow to delete
            
        Returns:
            True if deleted, raises HTTPException if not found
        """
        try:
            repository = FlowRepository(self.session)
            
            # First check if the flow exists
            flow = await repository.get(flow_id)
            if not flow:
                raise HTTPException(status_code=404, detail="Flow not found")
            
            # Check if there are any executions for this flow
            find_executions_query = text("""
            SELECT COUNT(*) FROM flow_executions WHERE flow_id = :flow_id
            """)
            result = await self.session.execute(find_executions_query, {"flow_id": flow_id})
            count = result.scalar_one()
            
            if count > 0:
                # If there are executions, suggest using force delete
                logger.warning(f"Flow {flow_id} has {count} execution records. Use force_delete_flow_with_executions instead.")
                raise HTTPException(
                    status_code=400, 
                    detail=f"Cannot delete flow because it has {count} execution records. Use force delete instead."
                )
            
            # If no executions, proceed with regular delete
            return await repository.delete(flow_id)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting flow: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error deleting flow: {str(e)}")
    
    async def force_delete_flow_with_executions(self, flow_id: uuid.UUID) -> bool:
        """
        Force delete a flow by first removing any associated flow executions.
        This handles the foreign key constraint issue.
        
        Args:
            flow_id: UUID of the flow to delete
            
        Returns:
            True if deleted, raises HTTPException if not found
        """
        try:
            # Use direct SQL queries instead of the repository to avoid transaction issues
            from sqlalchemy import text
            
            # First check if the flow exists
            check_query = text("SELECT id FROM flows WHERE id = :flow_id")
            result = await self.session.execute(check_query, {"flow_id": flow_id})
            if not result.first():
                raise HTTPException(status_code=404, detail="Flow not found")
            
            logger.info(f"Starting force deletion of flow {flow_id}")
            
            # Identify all flow_executions for this flow
            find_executions_query = text("SELECT id FROM flow_executions WHERE flow_id = :flow_id")
            result = await self.session.execute(find_executions_query, {"flow_id": flow_id})
            execution_ids = [row[0] for row in result.fetchall()]
            
            if execution_ids:
                logger.info(f"Found {len(execution_ids)} flow executions to delete")
                
                # Process each execution ID individually to avoid large query issues
                for exec_id in execution_ids:
                    node_delete_query = text("DELETE FROM flow_node_executions WHERE flow_execution_id = :exec_id")
                    await self.session.execute(node_delete_query, {"exec_id": exec_id})
                
                # Delete all flow_executions after node executions are gone
                execution_delete_query = text("DELETE FROM flow_executions WHERE flow_id = :flow_id")
                await self.session.execute(execution_delete_query, {"flow_id": flow_id})
            
            # Delete the flow itself
            flow_delete_query = text("DELETE FROM flows WHERE id = :flow_id")
            result = await self.session.execute(flow_delete_query, {"flow_id": flow_id})
            
            # Commit the transaction
            await self.session.commit()
            
            logger.info(f"Successfully deleted flow {flow_id} with all its executions")
            return True
            
        except HTTPException:
            await self.session.rollback()
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error force deleting flow with executions: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error force deleting flow with executions: {str(e)}")
    
    async def delete_all_flows(self) -> None:
        """
        Delete all flows.
        
        Returns:
            None
        """
        try:
            repository = FlowRepository(self.session)
            await repository.delete_all()
        except Exception as e:
            logger.error(f"Error deleting all flows: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error deleting all flows: {str(e)}")
            
    async def validate_flow_data(self, flow_in: FlowCreate) -> Dict[str, Any]:
        """
        Validate flow data without creating it.
        
        Args:
            flow_in: Flow data to validate
            
        Returns:
            Validation result
        """
        try:
            # Convert to dict to ensure it's valid
            data_dict = flow_in.model_dump()
            logger.info("Flow data validation successful")
            logger.info(f"Flow name: {data_dict['name']}")
            logger.info(f"Crew ID: {data_dict['crew_id']}")
            logger.info(f"Number of nodes: {len(data_dict['nodes'])}")
            logger.info(f"Number of edges: {len(data_dict['edges'])}")
            
            if data_dict.get('flow_config'):
                logger.info(f"Flow config details: {json.dumps(data_dict['flow_config'], indent=2)}")
            
            return {
                "status": "success",
                "message": "Data validation successful",
                "data": {
                    "name": data_dict['name'],
                    "crew_id": data_dict['crew_id'],
                    "node_count": len(data_dict['nodes']),
                    "edge_count": len(data_dict['edges']),
                    "has_flow_config": data_dict.get('flow_config') is not None
                }
            }
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return {
                "status": "error",
                "message": f"Validation failed: {str(e)}"
            } 