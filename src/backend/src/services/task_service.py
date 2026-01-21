import logging
from typing import List, Optional, Dict, Any, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.core.base_service import BaseService
from src.models.task import Task
from src.repositories.task_repository import TaskRepository
from src.schemas.task import TaskCreate, TaskUpdate
from src.utils.user_context import GroupContext
from src.utils.sensitive_data_utils import (
    encrypt_sensitive_fields,
    decrypt_sensitive_fields,
    safe_log_tool_configs,
)

logger = logging.getLogger(__name__)


class TaskService(BaseService[Task, TaskCreate]):
    """
    Service for Task model with business logic.

    Security: This service handles encryption/decryption of sensitive fields
    in tool_configs to protect credentials stored in the database.
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

    def _decrypt_task_tool_configs(self, task: Optional[Task]) -> Optional[Task]:
        """
        Decrypt sensitive fields in task's tool_configs after retrieval.

        Args:
            task: Task with potentially encrypted tool_configs

        Returns:
            Task with decrypted tool_configs (in-memory only)
        """
        if task and task.tool_configs:
            try:
                task.tool_configs = decrypt_sensitive_fields(task.tool_configs)
            except Exception as e:
                logger.error(f"Failed to decrypt tool_configs for task {task.id}: {e}")
        return task

    def _encrypt_tool_configs_in_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in tool_configs before storage.

        Args:
            data: Dictionary containing task data with tool_configs

        Returns:
            Dictionary with encrypted tool_configs
        """
        if 'tool_configs' in data and data['tool_configs']:
            try:
                data['tool_configs'] = encrypt_sensitive_fields(data['tool_configs'])
                logger.debug(safe_log_tool_configs(data['tool_configs'], "Encrypted "))
            except Exception as e:
                logger.error(f"Failed to encrypt tool_configs: {e}")
                raise
        return data
    
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
        Get a task by ID with decrypted tool_configs.

        Args:
            id: ID of the task to get

        Returns:
            Task if found, else None (with decrypted tool_configs)
        """
        task = await self.repository.get(id)
        return self._decrypt_task_tool_configs(task)
    
    async def get_with_group_check(self, id: str, group_context: GroupContext) -> Optional[Task]:
        """
        Get a task by ID with group verification and decrypted tool_configs.

        Args:
            id: ID of the task to get
            group_context: Group context for verification

        Returns:
            Task if found and belongs to user's group, else None (with decrypted tool_configs)
        """
        task = await self.repository.get(id)
        if task and group_context and group_context.group_ids:
            # Check if task belongs to one of the user's groups
            if task.group_id not in group_context.group_ids:
                return None  # Return None to trigger 404, not 403 (avoid information leakage)
        return self._decrypt_task_tool_configs(task)
        
    async def create(self, obj_in: TaskCreate) -> Task:
        """
        Create a new task with encrypted tool_configs.

        Args:
            obj_in: Task data for creation

        Returns:
            Created task (with decrypted tool_configs for response)
        """
        data = obj_in.model_dump()
        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in data and data["agent_id"] == "":
            data["agent_id"] = None

        # Encrypt sensitive fields in tool_configs before storage
        data = self._encrypt_tool_configs_in_data(data)

        task = await self.repository.create(data)
        # Decrypt for response (task is guaranteed to exist after create)
        self._decrypt_task_tool_configs(task)
        return task
    
    async def find_by_name(self, name: str) -> Optional[Task]:
        """
        Find a task by name with decrypted tool_configs.

        Args:
            name: Name to search for

        Returns:
            Task if found, else None (with decrypted tool_configs)
        """
        task = await self.repository.find_by_name(name)
        return self._decrypt_task_tool_configs(task)

    async def find_by_agent_id(self, agent_id: str) -> List[Task]:
        """
        Find all tasks for a specific agent with decrypted tool_configs.

        Args:
            agent_id: ID of the agent

        Returns:
            List of tasks assigned to the agent (with decrypted tool_configs)
        """
        tasks = await self.repository.find_by_agent_id(agent_id)
        for task in tasks:
            self._decrypt_task_tool_configs(task)
        return tasks

    async def find_all(self) -> List[Task]:
        """
        Find all tasks with decrypted tool_configs.

        Returns:
            List of all tasks (with decrypted tool_configs)
        """
        tasks = await self.repository.find_all()
        for task in tasks:
            self._decrypt_task_tool_configs(task)
        return tasks
    
    async def update_with_partial_data(self, id: str, obj_in: TaskUpdate) -> Optional[Task]:
        """
        Update a task with partial data, only updating fields that are set.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            id: ID of the task to update
            obj_in: Schema with fields to update

        Returns:
            Updated task if found, else None (with decrypted tool_configs)
        """
        # Safe logging for tool_configs (masks sensitive fields)
        if hasattr(obj_in, 'tool_configs') and obj_in.tool_configs is not None:
            logger.info(f"TaskService: Updating task {id} - {safe_log_tool_configs(obj_in.tool_configs)}")

        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)

        # Special handling for llm_guardrail - explicitly include null to allow clearing
        # Check if the field was explicitly set in the input (even to null)
        if hasattr(obj_in, 'llm_guardrail'):
            # Get the raw input data to check if llm_guardrail was explicitly sent
            raw_data = obj_in.model_dump(exclude_unset=True)
            if 'llm_guardrail' in raw_data:
                update_data['llm_guardrail'] = obj_in.llm_guardrail

        # Encrypt sensitive fields in tool_configs before storage
        if 'tool_configs' in update_data:
            logger.debug(f"TaskService: encrypting tool_configs for task {id}")
            update_data = self._encrypt_tool_configs_in_data(update_data)

        if not update_data:
            # No fields to update
            return await self.get(id)

        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in update_data and update_data["agent_id"] == "":
            update_data["agent_id"] = None

        task = await self.repository.update(id, update_data)
        return self._decrypt_task_tool_configs(task)

    async def update_with_group_check(self, id: str, obj_in: TaskUpdate, group_context: GroupContext) -> Optional[Task]:
        """
        Update a task with partial data and group verification.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            id: ID of the task to update
            obj_in: Schema with fields to update
            group_context: Group context for verification

        Returns:
            Updated task if found and belongs to user's group, else None (with decrypted tool_configs)
        """
        # First verify the task belongs to the user's group
        task = await self.get_with_group_check(id, group_context)
        if not task:
            return None

        # Safe logging for tool_configs (masks sensitive fields)
        if hasattr(obj_in, 'tool_configs') and obj_in.tool_configs is not None:
            logger.info(f"TaskService: Updating task {id} - {safe_log_tool_configs(obj_in.tool_configs)}")

        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)

        # Special handling for llm_guardrail - explicitly include null to allow clearing
        # Check if the field was explicitly set in the input (even to null)
        if hasattr(obj_in, 'llm_guardrail'):
            # Get the raw input data to check if llm_guardrail was explicitly sent
            raw_data = obj_in.model_dump(exclude_unset=True)
            if 'llm_guardrail' in raw_data:
                update_data['llm_guardrail'] = obj_in.llm_guardrail

        # Encrypt sensitive fields in tool_configs before storage
        if 'tool_configs' in update_data:
            logger.debug(f"TaskService: encrypting tool_configs for task {id}")
            update_data = self._encrypt_tool_configs_in_data(update_data)

        if not update_data:
            # No fields to update
            return task

        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in update_data and update_data["agent_id"] == "":
            update_data["agent_id"] = None

        task = await self.repository.update(id, update_data)
        return self._decrypt_task_tool_configs(task)
    
    async def update_full(self, id: str, obj_in: Dict[str, Any]) -> Optional[Task]:
        """
        Update all fields of a task.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            id: ID of the task to update
            obj_in: Dictionary with all fields to update

        Returns:
            Updated task if found, else None (with decrypted tool_configs)
        """
        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in obj_in and obj_in["agent_id"] == "":
            obj_in["agent_id"] = None

        # Encrypt sensitive fields in tool_configs before storage
        obj_in = self._encrypt_tool_configs_in_data(obj_in)

        task = await self.repository.update(id, obj_in)
        return self._decrypt_task_tool_configs(task)

    async def update_full_with_group_check(self, id: str, obj_in: Dict[str, Any], group_context: GroupContext) -> Optional[Task]:
        """
        Update all fields of a task with group verification.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            id: ID of the task to update
            obj_in: Dictionary with all fields to update
            group_context: Group context for verification

        Returns:
            Updated task if found and belongs to user's group, else None (with decrypted tool_configs)
        """
        # First verify the task belongs to the user's group
        task = await self.get_with_group_check(id, group_context)
        if not task:
            return None

        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in obj_in and obj_in["agent_id"] == "":
            obj_in["agent_id"] = None

        # Encrypt sensitive fields in tool_configs before storage
        obj_in = self._encrypt_tool_configs_in_data(obj_in)

        task = await self.repository.update(id, obj_in)
        return self._decrypt_task_tool_configs(task)
    
    async def delete(self, id: str) -> bool:
        """
        Delete a task by ID.
        
        Args:
            id: ID of the task to delete
            
        Returns:
            True if task was deleted, False if not found
        """
        return await self.repository.delete(id)
    
    async def delete_with_group_check(self, id: str, group_context: GroupContext) -> bool:
        """
        Delete a task by ID with group verification.
        
        Args:
            id: ID of the task to delete
            group_context: Group context for verification
            
        Returns:
            True if task was deleted, False if not found or not authorized
        """
        # First verify the task belongs to the user's group
        task = await self.get_with_group_check(id, group_context)
        if not task:
            return False
        
        return await self.repository.delete(id)
    
    async def delete_all(self) -> None:
        """
        Delete all tasks.
        
        Returns:
            None
        """
        await self.repository.delete_all()
    
    async def delete_all_for_group(self, group_context: GroupContext) -> None:
        """
        Delete all tasks for a specific group.
        
        Args:
            group_context: Group context for filtering
            
        Returns:
            None
        """
        if not group_context or not group_context.group_ids:
            return
        
        # Get all tasks for the group
        tasks = await self.find_by_group(group_context)
        
        # Delete each task
        for task in tasks:
            await self.repository.delete(task.id)
    
    async def create_with_group(self, obj_in: TaskCreate, group_context: GroupContext) -> Task:
        """
        Create a new task with group isolation.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            obj_in: Task data for creation
            group_context: Group context from headers

        Returns:
            Created task with group information (with decrypted tool_configs for response)
        """
        # Convert schema to dict and add group fields
        task_data = obj_in.model_dump()
        task_data['group_id'] = group_context.primary_group_id
        task_data['created_by_email'] = group_context.group_email

        # Convert empty agent_id to None for PostgreSQL compatibility
        if "agent_id" in task_data and task_data["agent_id"] == "":
            task_data["agent_id"] = None

        # Encrypt sensitive fields in tool_configs before storage
        task_data = self._encrypt_tool_configs_in_data(task_data)

        # Create task using repository (pass dict, not object)
        task = await self.repository.create(task_data)
        # Decrypt for response (task is guaranteed to exist after create)
        self._decrypt_task_tool_configs(task)
        return task

    async def find_by_group(self, group_context: GroupContext) -> List[Task]:
        """
        Find all tasks for a specific group with decrypted tool_configs.

        Args:
            group_context: Group context from headers

        Returns:
            List of tasks for the specified group (with decrypted tool_configs)
        """
        if not group_context.group_ids:
            # If no group context, return empty list for security
            return []

        stmt = select(Task).where(Task.group_id.in_(group_context.group_ids))
        result = await self.session.execute(stmt)
        tasks = list(result.scalars().all())
        for task in tasks:
            self._decrypt_task_tool_configs(task)
        return tasks 