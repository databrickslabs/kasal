from typing import List, Optional, Type

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.task import Task
from src.db.session import SessionLocal


class TaskRepository(BaseRepository[Task]):
    """
    Repository for Task model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, model: Type[Task], session: AsyncSession):
        """
        Initialize the repository with model and session.
        
        Args:
            model: SQLAlchemy model class
            session: SQLAlchemy async session
        """
        super().__init__(model, session)
    
    async def get(self, id: str) -> Optional[Task]:
        """
        Get a single task by ID.
        
        Args:
            id: ID of the task to get
            
        Returns:
            The task if found, else None
        """
        try:
            query = select(self.model).where(self.model.id == id)
            result = await self.session.execute(query)
            return result.scalars().first()
        except Exception as e:
            await self.session.rollback()
            raise
    
    async def create(self, obj_in: dict) -> Task:
        """
        Create a new task.
        
        Args:
            obj_in: Dictionary of values to create model with
            
        Returns:
            The created task
        """
        try:
            # Do not convert None to empty string for agent_id
            # as it would violate PostgreSQL foreign key constraints
                
            # Ensure synchronization between config and dedicated fields
            if 'config' in obj_in and obj_in['config'] is not None:
                # If output_pydantic is in config, sync to root
                if 'output_pydantic' in obj_in['config'] and obj_in['config']['output_pydantic']:
                    obj_in['output_pydantic'] = obj_in['config']['output_pydantic']
                    
                # If output_json is in config, sync to root
                if 'output_json' in obj_in['config'] and obj_in['config']['output_json']:
                    obj_in['output_json'] = obj_in['config']['output_json']
                    
                # If output_file is in config, sync to root
                if 'output_file' in obj_in['config'] and obj_in['config']['output_file']:
                    obj_in['output_file'] = obj_in['config']['output_file']
                    
                # If callback is in config, sync to root
                if 'callback' in obj_in['config'] and obj_in['config']['callback']:
                    obj_in['callback'] = obj_in['config']['callback']

                # If guardrail is in config, sync to root
                if 'guardrail' in obj_in['config'] and obj_in['config']['guardrail']:
                    obj_in['guardrail'] = obj_in['config']['guardrail']
            
            # Vice versa: if fields are at root level, ensure they're in config too
            if 'config' not in obj_in:
                obj_in['config'] = {}
                
            if 'output_pydantic' in obj_in and obj_in['output_pydantic']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['output_pydantic'] = obj_in['output_pydantic']
                
            if 'output_json' in obj_in and obj_in['output_json']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['output_json'] = obj_in['output_json']
                
            if 'output_file' in obj_in and obj_in['output_file']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['output_file'] = obj_in['output_file']
                
            if 'callback' in obj_in and obj_in['callback']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['callback'] = obj_in['callback']

            if 'guardrail' in obj_in and obj_in['guardrail']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['guardrail'] = obj_in['guardrail']
                
            if 'markdown' in obj_in and obj_in['markdown'] is not None:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['markdown'] = obj_in['markdown']
            
            # Also sync markdown from config to root if present
            if 'config' in obj_in and obj_in['config'] is not None:
                if 'markdown' in obj_in['config'] and obj_in['config']['markdown'] is not None:
                    obj_in['markdown'] = obj_in['config']['markdown']
            
            db_obj = self.model(**obj_in)
            self.session.add(db_obj)
            await self.session.flush()
            return db_obj
        except Exception as e:
            await self.session.rollback()
            raise
            
    async def update(self, id: str, obj_in: dict) -> Optional[Task]:
        """
        Update an existing task.
        
        Args:
            id: ID of the task to update
            obj_in: Dictionary of values to update model with
            
        Returns:
            The updated task if found, else None
        """
        try:
            # Do not convert None to empty string for agent_id
            # as it would violate PostgreSQL foreign key constraints
                
            # Ensure synchronization between config and dedicated fields
            if 'config' in obj_in and obj_in['config'] is not None:
                # If output_pydantic is in config, sync to root
                if 'output_pydantic' in obj_in['config'] and obj_in['config']['output_pydantic']:
                    obj_in['output_pydantic'] = obj_in['config']['output_pydantic']
                    
                # If output_json is in config, sync to root
                if 'output_json' in obj_in['config'] and obj_in['config']['output_json']:
                    obj_in['output_json'] = obj_in['config']['output_json']
                    
                # If output_file is in config, sync to root
                if 'output_file' in obj_in['config'] and obj_in['config']['output_file']:
                    obj_in['output_file'] = obj_in['config']['output_file']
                    
                # If callback is in config, sync to root
                if 'callback' in obj_in['config'] and obj_in['config']['callback']:
                    obj_in['callback'] = obj_in['config']['callback']
                
                # If guardrail is in config, sync to root
                if 'guardrail' in obj_in['config'] and obj_in['config']['guardrail']:
                    obj_in['guardrail'] = obj_in['config']['guardrail']
            
            # Vice versa: if fields are at root level, ensure they're in config too
            if 'config' not in obj_in:
                obj_in['config'] = {}
                
            if 'output_pydantic' in obj_in and obj_in['output_pydantic']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['output_pydantic'] = obj_in['output_pydantic']
                
            if 'output_json' in obj_in and obj_in['output_json']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['output_json'] = obj_in['output_json']
                
            if 'output_file' in obj_in and obj_in['output_file']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['output_file'] = obj_in['output_file']
                
            if 'callback' in obj_in and obj_in['callback']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['callback'] = obj_in['callback']
                
            if 'guardrail' in obj_in and obj_in['guardrail']:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['guardrail'] = obj_in['guardrail']
                
            if 'markdown' in obj_in and obj_in['markdown'] is not None:
                if 'config' not in obj_in:
                    obj_in['config'] = {}
                obj_in['config']['markdown'] = obj_in['markdown']
            
            # Also sync markdown from config to root if present
            if 'config' in obj_in and obj_in['config'] is not None:
                if 'markdown' in obj_in['config'] and obj_in['config']['markdown'] is not None:
                    obj_in['markdown'] = obj_in['config']['markdown']
            
            db_obj = await self.get(id)
            if db_obj:
                for key, value in obj_in.items():
                    setattr(db_obj, key, value)
                await self.session.flush()
            return db_obj
        except Exception as e:
            await self.session.rollback()
            raise
            
    async def delete(self, id: str) -> bool:
        """
        Delete a task by ID.
        
        Args:
            id: ID of the task to delete
            
        Returns:
            True if task was deleted, False if not found
        """
        try:
            db_obj = await self.get(id)
            if db_obj:
                await self.session.delete(db_obj)
                await self.session.flush()
                return True
            return False
        except Exception as e:
            await self.session.rollback()
            raise
    
    async def find_by_name(self, name: str) -> Optional[Task]:
        """
        Find a task by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Task if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_by_agent_id(self, agent_id: str) -> List[Task]:
        """
        Find all tasks for a specific agent.
        
        Args:
            agent_id: ID of the agent
            
        Returns:
            List of tasks assigned to the agent
        """
        query = select(self.model).where(self.model.agent_id == agent_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def find_all(self) -> List[Task]:
        """
        Find all tasks.
        
        Returns:
            List of all tasks
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def delete_all(self) -> None:
        """
        Delete all tasks.
        
        Returns:
            None
        """
        stmt = delete(self.model)
        await self.session.execute(stmt)
        await self.session.flush()


class SyncTaskRepository:
    """
    Synchronous repository for Task model with custom query methods.
    Used by services that require synchronous DB operations.
    """
    
    def __init__(self, db: Session):
        """
        Initialize the repository with session.
        
        Args:
            db: SQLAlchemy synchronous session
        """
        self.db = db
    
    def find_by_id(self, task_id: int) -> Optional[Task]:
        """
        Find a task by ID.
        
        Args:
            task_id: ID of the task to find
            
        Returns:
            Task if found, else None
        """
        return self.db.query(Task).filter(Task.id == task_id).first()
    
    def find_by_name(self, name: str) -> Optional[Task]:
        """
        Find a task by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Task if found, else None
        """
        return self.db.query(Task).filter(Task.name == name).first()
    
    def find_by_agent_id(self, agent_id: int) -> List[Task]:
        """
        Find all tasks for a specific agent.
        
        Args:
            agent_id: ID of the agent
            
        Returns:
            List of tasks associated with the agent
        """
        return self.db.query(Task).filter(Task.agent_id == agent_id).all()
    
    def find_all(self) -> List[Task]:
        """
        Find all tasks.
        
        Returns:
            List of all tasks
        """
        return self.db.query(Task).all()

# Factory function to get a repository instance without managing the session in the service
def get_sync_task_repository() -> SyncTaskRepository:
    """
    Factory function to create and return a SyncTaskRepository instance.
    This handles session creation internally.
    
    Returns:
        A SyncTaskRepository instance with an active session
    """
    db = SessionLocal()
    return SyncTaskRepository(db) 