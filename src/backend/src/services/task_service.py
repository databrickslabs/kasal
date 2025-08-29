from typing import List, Optional, Dict, Any, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.core.base_service import BaseService
from src.models.task import Task
from src.repositories.task_repository import TaskRepository
from src.schemas.task import TaskCreate, TaskUpdate
from src.utils.user_context import GroupContext


class TaskService(BaseService[Task, TaskCreate]):
    """
    Service for Task model with business logic.
    """
    
    def __init__(
        self,
        session: AsyncSession,
        repository_class: Type[TaskRepository] = TaskRepository,
        model_class: Type[Task] = Task
    ):
        """
        Initialize the service with session and optional repository and model classes.
        
        Args:
            session: Database session for operations
            repository_class: Repository class to use for data access (optional)
            model_class: Model class associated with this service (optional)
        """
        super().__init__(session)
        self.repository_class = repository_class
        self.model_class = model_class
        self.repository = repository_class(session)
    
    @classmethod
    def create(cls, session: AsyncSession) -> 'TaskService':
        """
        Factory method to create a properly configured TaskService instance.
        
        Args:
            session: Database session for operations
            
        Returns:
            An instance of TaskService
        """
        return cls(session=session)
    
    async def get(self, id: str) -> Optional[Task]:
        """
        Get a task by ID.
        
        Args:
            id: ID of the task to get
            
        Returns:
            Task if found, else None
        """
        return await self.repository.get(id)
        
    async def create(self, obj_in: TaskCreate) -> Task:
        """
        Create a new task.
        
        Args:
            obj_in: Task data for creation
            
        Returns:
            Created task
        """
        data = obj_in.model_dump()
        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in data and data["agent_id"] == "":
            data["agent_id"] = None
            
        return await self.repository.create(data)
    
    async def find_by_name(self, name: str) -> Optional[Task]:
        """
        Find a task by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Task if found, else None
        """
        return await self.repository.find_by_name(name)
    
    async def find_by_agent_id(self, agent_id: str) -> List[Task]:
        """
        Find all tasks for a specific agent.
        
        Args:
            agent_id: ID of the agent
            
        Returns:
            List of tasks assigned to the agent
        """
        return await self.repository.find_by_agent_id(agent_id)
    
    async def find_all(self) -> List[Task]:
        """
        Find all tasks.
        
        Returns:
            List of all tasks
        """
        return await self.repository.find_all()
    
    async def update_with_partial_data(self, id: str, obj_in: TaskUpdate) -> Optional[Task]:
        """
        Update a task with partial data, only updating fields that are set.
        
        Args:
            id: ID of the task to update
            obj_in: Schema with fields to update
            
        Returns:
            Updated task if found, else None
        """
        # Debug logging for tool_configs
        import logging
        logger = logging.getLogger(__name__)
        if hasattr(obj_in, 'tool_configs') and obj_in.tool_configs is not None:
            logger.info(f"TaskService: Updating task {id} with tool_configs: {obj_in.tool_configs}")
        
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        
        # Log if tool_configs is in update_data
        if 'tool_configs' in update_data:
            logger.info(f"TaskService: update_data contains tool_configs: {update_data.get('tool_configs')}")
        
        if not update_data:
            # No fields to update
            return await self.get(id)
        
        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in update_data and update_data["agent_id"] == "":
            update_data["agent_id"] = None
        
        return await self.repository.update(id, update_data)
    
    async def update_full(self, id: str, obj_in: Dict[str, Any]) -> Optional[Task]:
        """
        Update all fields of a task.
        
        Args:
            id: ID of the task to update
            obj_in: Dictionary with all fields to update
            
        Returns:
            Updated task if found, else None
        """
        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in obj_in and obj_in["agent_id"] == "":
            obj_in["agent_id"] = None
            
        return await self.repository.update(id, obj_in)
    
    async def delete(self, id: str) -> bool:
        """
        Delete a task by ID.
        
        Args:
            id: ID of the task to delete
            
        Returns:
            True if task was deleted, False if not found
        """
        return await self.repository.delete(id)
    
    async def delete_all(self) -> None:
        """
        Delete all tasks.
        
        Returns:
            None
        """
        await self.repository.delete_all()
    
    async def create_with_group(self, obj_in: TaskCreate, group_context: GroupContext) -> Task:
        """
        Create a new task with group isolation.
        
        Args:
            obj_in: Task data for creation
            group_context: Group context from headers
            
        Returns:
            Created task with group information
        """
        # Convert schema to dict and add group fields
        task_data = obj_in.model_dump()
        task_data['group_id'] = group_context.primary_group_id
        task_data['created_by_email'] = group_context.group_email
        
        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in task_data and task_data["agent_id"] == "":
            task_data["agent_id"] = None
        
        # Create task using repository (pass dict, not object)
        return await self.repository.create(task_data)
    
    async def find_by_group(self, group_context: GroupContext) -> List[Task]:
        """
        Find all tasks for a specific group.
        
        Args:
            group_context: Group context from headers
            
        Returns:
            List of tasks for the specified group
        """
        if not group_context.group_ids:
            # If no group context, return empty list for security
            return []
        
        stmt = select(Task).where(Task.group_id.in_(group_context.group_ids))
        result = await self.session.execute(stmt)
        return result.scalars().all() 