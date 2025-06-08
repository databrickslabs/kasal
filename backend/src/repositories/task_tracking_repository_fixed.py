import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy import select, update, func, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.task import Task
from src.schemas.task_tracking import TaskStatusEnum, TaskCreate, TaskUpdate
from src.db.session import async_session_factory, SessionLocal

logger = logging.getLogger(__name__)

class TaskTrackingRepository(BaseRepository[Task]):
    """
    Repository for Task tracking with CRUD operations.
    Manages task lifecycle, status tracking, and persistence.
    """
    
    def __init__(self, session: AsyncSession):
        """Initialize with async session."""
        super().__init__(Task, session)
    
    async def create_task(self, task_data: TaskCreate) -> Task:
        """Create a new task."""
        try:
            task = Task(
                name=task_data.name,
                description=task_data.description,
                status=task_data.status or TaskStatusEnum.PENDING,
                priority=task_data.priority,
                agent_id=task_data.agent_id,
                crew_id=task_data.crew_id,
                expected_output=task_data.expected_output,
                tools=task_data.tools or [],
                context=task_data.context or {},
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            return await self.create(task)
        except Exception as e:
            logger.error(f"Error creating task: {str(e)}")
            raise
    
    async def update_task(self, task_id: int, task_data: TaskUpdate) -> Optional[Task]:
        """Update an existing task."""
        try:
            task = await self.get_by_id(task_id)
            if not task:
                return None
            
            # Update only provided fields
            update_data = task_data.model_dump(exclude_unset=True)
            for field, value in update_data.items():
                if hasattr(task, field):
                    setattr(task, field, value)
            
            task.updated_at = datetime.utcnow()
            return await self.update(task_id, update_data)
        except Exception as e:
            logger.error(f"Error updating task {task_id}: {str(e)}")
            raise
    
    async def get_tasks_by_status(self, status: TaskStatusEnum) -> List[Task]:
        """Get all tasks with a specific status."""
        try:
            query = select(self.model).where(self.model.status == status)
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting tasks by status {status}: {str(e)}")
            raise
    
    async def get_tasks_by_agent(self, agent_id: int) -> List[Task]:
        """Get all tasks assigned to a specific agent."""
        try:
            query = select(self.model).where(self.model.agent_id == agent_id)
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting tasks by agent {agent_id}: {str(e)}")
            raise
    
    async def get_tasks_by_crew(self, crew_id: int) -> List[Task]:
        """Get all tasks assigned to a specific crew."""
        try:
            query = select(self.model).where(self.model.crew_id == crew_id)
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting tasks by crew {crew_id}: {str(e)}")
            raise
    
    async def update_status(self, task_id: int, status: TaskStatusEnum) -> bool:
        """Update task status."""
        try:
            task = await self.get_by_id(task_id)
            if not task:
                return False
            
            task.status = status
            task.updated_at = datetime.utcnow()
            
            if status == TaskStatusEnum.IN_PROGRESS:
                task.started_at = datetime.utcnow()
            elif status in [TaskStatusEnum.COMPLETED, TaskStatusEnum.FAILED]:
                task.completed_at = datetime.utcnow()
            
            # Let service layer handle commit
            return True
        except Exception as e:
            logger.error(f"Error updating task status {task_id}: {str(e)}")
            raise
    
    async def mark_as_completed(self, task_id: int, result: Optional[str] = None) -> bool:
        """Mark task as completed with optional result."""
        try:
            task = await self.get_by_id(task_id)
            if not task:
                return False
            
            task.status = TaskStatusEnum.COMPLETED
            task.completed_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            
            if result:
                if not task.context:
                    task.context = {}
                task.context['result'] = result
            
            # Let service layer handle commit
            return True
        except Exception as e:
            logger.error(f"Error marking task as completed {task_id}: {str(e)}")
            raise
    
    async def mark_as_failed(self, task_id: int, error: Optional[str] = None) -> bool:
        """Mark task as failed with optional error message."""
        try:
            task = await self.get_by_id(task_id)
            if not task:
                return False
            
            task.status = TaskStatusEnum.FAILED
            task.completed_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            
            if error:
                if not task.context:
                    task.context = {}
                task.context['error'] = error
            
            # Let service layer handle commit
            return True
        except Exception as e:
            logger.error(f"Error marking task as failed {task_id}: {str(e)}")
            raise
    
    async def get_task_statistics(self) -> Dict[str, int]:
        """Get task statistics by status."""
        try:
            query = select(
                self.model.status,
                func.count(self.model.id).label('count')
            ).group_by(self.model.status)
            
            result = await self.session.execute(query)
            # No commit needed for read operations
            
            stats = {}
            for row in result:
                stats[row.status.value] = row.count
            
            return stats
        except Exception as e:
            logger.error(f"Error getting task statistics: {str(e)}")
            raise
    
    async def delete_completed_tasks(self, days_old: int = 30) -> int:
        """Delete completed tasks older than specified days."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            query = delete(self.model).where(
                self.model.status == TaskStatusEnum.COMPLETED,
                self.model.completed_at < cutoff_date
            )
            
            result = await self.session.execute(query)
            # Let service layer handle commit
            
            deleted_count = result.rowcount
            logger.info(f"Deleted {deleted_count} completed tasks older than {days_old} days")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting completed tasks: {str(e)}")
            raise
    
    async def bulk_update_status(self, task_ids: List[int], status: TaskStatusEnum) -> int:
        """Bulk update status for multiple tasks."""
        try:
            query = update(self.model).where(
                self.model.id.in_(task_ids)
            ).values(
                status=status,
                updated_at=datetime.utcnow()
            )
            
            result = await self.session.execute(query)
            # Let service layer handle commit
            
            updated_count = result.rowcount
            logger.info(f"Updated {updated_count} tasks to status {status}")
            return updated_count
        except Exception as e:
            logger.error(f"Error bulk updating task status: {str(e)}")
            raise
    
    async def get_all_tasks(self) -> List[Task]:
        """Get all tasks."""
        try:
            query = select(self.model)
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting all tasks: {str(e)}")
            raise
    
    # Sync methods for callback/non-async contexts (kept as-is for backward compatibility)
    def get_tasks_by_status_sync(self, status: TaskStatusEnum) -> List[Task]:
        """Synchronous version - get all tasks with specific status."""
        try:
            with SessionLocal() as db:
                query = select(Task).where(Task.status == status)
                result = db.execute(query)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting tasks by status {status} (sync): {str(e)}")
            raise
    
    def update_task_status_sync(self, task_id: int, status: TaskStatusEnum) -> bool:
        """Synchronous version - update task status."""
        try:
            with SessionLocal() as db:
                task = db.get(Task, task_id)
                if not task:
                    return False
                
                task.status = status
                task.updated_at = datetime.utcnow()
                
                if status == TaskStatusEnum.IN_PROGRESS:
                    task.started_at = datetime.utcnow()
                elif status in [TaskStatusEnum.COMPLETED, TaskStatusEnum.FAILED]:
                    task.completed_at = datetime.utcnow()
                
                db.commit()
                return True
        except Exception as e:
            logger.error(f"Error updating task status {task_id} (sync): {str(e)}")
            raise
    
    def mark_as_completed_sync(self, task_id: int, result: Optional[str] = None) -> bool:
        """Synchronous version - mark task as completed."""
        try:
            with SessionLocal() as db:
                task = db.get(Task, task_id)
                if not task:
                    return False
                
                task.status = TaskStatusEnum.COMPLETED
                task.completed_at = datetime.utcnow()
                task.updated_at = datetime.utcnow()
                
                if result:
                    if not task.context:
                        task.context = {}
                    task.context['result'] = result
                
                db.commit()
                return True
        except Exception as e:
            logger.error(f"Error marking task as completed {task_id} (sync): {str(e)}")
            raise
    
    def mark_as_failed_sync(self, task_id: int, error: Optional[str] = None) -> bool:
        """Synchronous version - mark task as failed."""
        try:
            with SessionLocal() as db:
                task = db.get(Task, task_id)
                if not task:
                    return False
                
                task.status = TaskStatusEnum.FAILED
                task.completed_at = datetime.utcnow()
                task.updated_at = datetime.utcnow()
                
                if error:
                    if not task.context:
                        task.context = {}
                    task.context['error'] = error
                
                db.commit()
                return True
        except Exception as e:
            logger.error(f"Error marking task as failed {task_id} (sync): {str(e)}")
            raise
    
    def get_task_statistics_sync(self) -> Dict[str, int]:
        """Synchronous version - get task statistics."""
        try:
            with SessionLocal() as db:
                query = select(
                    Task.status,
                    func.count(Task.id).label('count')
                ).group_by(Task.status)
                
                result = db.execute(query)
                
                stats = {}
                for row in result:
                    stats[row.status.value] = row.count
                
                return stats
        except Exception as e:
            logger.error(f"Error getting task statistics (sync): {str(e)}")
            raise
    
    def delete_completed_tasks_sync(self, days_old: int = 30) -> int:
        """Synchronous version - delete completed tasks."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            
            with SessionLocal() as db:
                query = delete(Task).where(
                    Task.status == TaskStatusEnum.COMPLETED,
                    Task.completed_at < cutoff_date
                )
                
                result = db.execute(query)
                db.commit()
                
                deleted_count = result.rowcount
                logger.info(f"Deleted {deleted_count} completed tasks older than {days_old} days (sync)")
                return deleted_count
        except Exception as e:
            logger.error(f"Error deleting completed tasks (sync): {str(e)}")
            raise
    
    def bulk_update_status_sync(self, task_ids: List[int], status: TaskStatusEnum) -> int:
        """Synchronous version - bulk update status."""
        try:
            with SessionLocal() as db:
                query = update(Task).where(
                    Task.id.in_(task_ids)
                ).values(
                    status=status,
                    updated_at=datetime.utcnow()
                )
                
                result = db.execute(query)
                db.commit()
                
                updated_count = result.rowcount
                logger.info(f"Updated {updated_count} tasks to status {status} (sync)")
                return updated_count
        except Exception as e:
            logger.error(f"Error bulk updating task status (sync): {str(e)}")
            raise
    
    def create_task_sync(self, task_data: TaskCreate) -> Task:
        """Synchronous version - create a new task."""
        try:
            with SessionLocal() as db:
                task = Task(
                    name=task_data.name,
                    description=task_data.description,
                    status=task_data.status or TaskStatusEnum.PENDING,
                    priority=task_data.priority,
                    agent_id=task_data.agent_id,
                    crew_id=task_data.crew_id,
                    expected_output=task_data.expected_output,
                    tools=task_data.tools or [],
                    context=task_data.context or {},
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                db.add(task)
                db.commit()
                db.refresh(task)
                return task
        except Exception as e:
            logger.error(f"Error creating task (sync): {str(e)}")
            raise
    
    def update_task_sync(self, task_id: int, task_data: TaskUpdate) -> Optional[Task]:
        """Synchronous version - update an existing task."""
        try:
            with SessionLocal() as db:
                task = db.get(Task, task_id)
                if not task:
                    return None
                
                # Update only provided fields
                update_data = task_data.model_dump(exclude_unset=True)
                for field, value in update_data.items():
                    if hasattr(task, field):
                        setattr(task, field, value)
                
                task.updated_at = datetime.utcnow()
                db.commit()
                db.refresh(task)
                return task
        except Exception as e:
            logger.error(f"Error updating task {task_id} (sync): {str(e)}")
            raise
    
    def get_tasks_by_agent_sync(self, agent_id: int) -> List[Task]:
        """Synchronous version - get tasks by agent."""
        try:
            with SessionLocal() as db:
                query = select(Task).where(Task.agent_id == agent_id)
                result = db.execute(query)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting tasks by agent {agent_id} (sync): {str(e)}")
            raise
    
    def get_tasks_by_crew_sync(self, crew_id: int) -> List[Task]:
        """Synchronous version - get tasks by crew."""
        try:
            with SessionLocal() as db:
                query = select(Task).where(Task.crew_id == crew_id)
                result = db.execute(query)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting tasks by crew {crew_id} (sync): {str(e)}")
            raise
    
    # Utility methods
    async def _get_session(self):
        """Get the current session."""
        return self.session
    
    def get_pending_tasks_sync(self) -> List[Task]:
        """Get all pending tasks - sync version for callbacks."""
        return self.get_tasks_by_status_sync(TaskStatusEnum.PENDING)
    
    def get_in_progress_tasks_sync(self) -> List[Task]:
        """Get all in-progress tasks - sync version for callbacks."""
        return self.get_tasks_by_status_sync(TaskStatusEnum.IN_PROGRESS)
    
    def get_completed_tasks_sync(self) -> List[Task]:
        """Get all completed tasks - sync version for callbacks."""
        return self.get_tasks_by_status_sync(TaskStatusEnum.COMPLETED)
    
    def get_failed_tasks_sync(self) -> List[Task]:
        """Get all failed tasks - sync version for callbacks."""
        return self.get_tasks_by_status_sync(TaskStatusEnum.FAILED)