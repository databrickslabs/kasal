from typing import List, Optional, Dict, Any, Type
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_service import BaseService
from src.models.task import Task
from src.repositories.task_repository import TaskRepository
from src.schemas.task import TaskCreate, TaskUpdate


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
        self.repository = repository_class(model_class, session)
    
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
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
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