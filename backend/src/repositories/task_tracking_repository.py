import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, UTC
from sqlalchemy.orm import Session

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory, get_db
from src.models.execution_history import ExecutionHistory, TaskStatus as DBTaskStatus, ErrorTrace
from src.schemas.task_tracking import TaskStatusEnum, TaskStatusCreate, TaskStatusUpdate

logger = logging.getLogger(__name__)

class TaskTrackingRepository:
    """
    Repository for task tracking and job execution status operations.
    Handles data access for Run and TaskStatus models.
    """
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize the repository with optional session.
        If no session is provided, the repository will manage its own sessions.
        
        Args:
            db: Optional SQLAlchemy session (sync or async)
        """
        self.db = db
        self._owns_session = db is None
    
    async def _get_session(self):
        """
        Get a session for async operations.
        If the repository owns its session, create a new one for each operation.
        
        Returns:
            AsyncSession instance
        """
        if self._owns_session:
            # Create a new session for this operation
            async with async_session_factory() as session:
                return session
        return self.db
    
    async def find_job_by_id(self, job_id: str) -> Optional[ExecutionHistory]:
        """
        Find a job execution by ID.
        
        Args:
            job_id: Job identifier to search for
            
        Returns:
            Run object if found, else None
        """
        session = await self._get_session()
        query = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
        result = await session.execute(query)
        return result.scalars().first()
    
    async def find_task_statuses_by_job_id(self, job_id: str) -> List[DBTaskStatus]:
        """
        Find all task statuses for a specific job ID ordered by start time.
        
        Args:
            job_id: Job identifier to filter by
            
        Returns:
            List of TaskStatus objects for the specified job
        """
        session = await self._get_session()
        query = select(DBTaskStatus).where(
            DBTaskStatus.job_id == job_id
        ).order_by(DBTaskStatus.started_at)
        
        result = await session.execute(query)
        return list(result.scalars().all())
    
    async def get_job_execution_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job execution status including all task statuses.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Dictionary with job execution status and task statuses
            
        Raises:
            Exception: If job not found or retrieval fails
        """
        try:
            # Find the job
            job = await self.find_job_by_id(job_id)
            if not job:
                logger.warning(f"Job not found with ID: {job_id}")
                raise ValueError(f"Job not found with ID: {job_id}")
            
            # Get task statuses for the job
            task_statuses = await self.find_task_statuses_by_job_id(job_id)
            
            # Format response
            return {
                "job_id": job_id,
                "status": job.status,
                "tasks": [
                    {
                        "id": status.id,
                        "task_id": status.task_id,
                        "status": status.status,
                        "agent_name": status.agent_name,
                        "started_at": status.started_at,
                        "completed_at": status.completed_at
                    }
                    for status in task_statuses
                ]
            }
        except ValueError:
            # Re-raise value errors (job not found)
            raise
        except Exception as e:
            logger.error(f"Error retrieving job execution status for job {job_id}: {str(e)}")
            raise
            
    async def get_all_tasks(self) -> List[DBTaskStatus]:
        """
        Get all task statuses.
        
        Returns:
            List of all task statuses
        """
        session = await self._get_session()
        query = select(DBTaskStatus).order_by(DBTaskStatus.started_at)
        result = await session.execute(query)
        return list(result.scalars().all())
        
    async def create_task(self, task: TaskStatusCreate) -> DBTaskStatus:
        """
        Create a new task status.
        
        Args:
            task: The task data to create
            
        Returns:
            The created task
        """
        session = await self._get_session()
        
        # Check if task already exists
        query = select(DBTaskStatus).where(
            DBTaskStatus.job_id == task.job_id,
            DBTaskStatus.task_id == task.task_id
        )
        result = await session.execute(query)
        existing = result.scalars().first()
        
        if existing:
            return existing
            
        # Create new task
        db_task = DBTaskStatus(
            job_id=task.job_id,
            task_id=task.task_id,
            status=task.status,
            agent_name=task.agent_name,
            started_at=datetime.now(UTC),
            completed_at=None
        )
        
        session.add(db_task)
        await session.commit()
        await session.refresh(db_task)
        
        return db_task
        
    async def update_task(self, task_id: int, task_update: TaskStatusUpdate) -> Optional[DBTaskStatus]:
        """
        Update a task by ID.
        
        Args:
            task_id: The ID of the task to update
            task_update: The data to update
            
        Returns:
            The updated task or None if not found
        """
        session = await self._get_session()
        
        query = select(DBTaskStatus).where(DBTaskStatus.id == task_id)
        result = await session.execute(query)
        db_task = result.scalars().first()
        
        if not db_task:
            return None
            
        # Update task fields
        db_task.status = task_update.status
        
        # If status is completed or failed, update completed_at
        if task_update.status in [TaskStatusEnum.COMPLETED, TaskStatusEnum.FAILED]:
            db_task.completed_at = datetime.now(UTC)
            
        await session.commit()
        await session.refresh(db_task)
        
        return db_task

    def create_task_status(self, task_status: TaskStatusCreate) -> DBTaskStatus:
        """
        Create a new task status entry with RUNNING status (sync version).
        
        Args:
            task_status: The task status data to create
            
        Returns:
            The created task status record
        """
        if self._owns_session:
            # Create a session for this operation
            from src.db.session import SessionLocal
            with SessionLocal() as db:
                return self._create_task_status_sync(db, task_status)
        else:
            return self._create_task_status_sync(self.db, task_status)
            
    def _create_task_status_sync(self, db: Session, task_status: TaskStatusCreate) -> DBTaskStatus:
        """
        Create a task status with a provided session.
        
        Args:
            db: SQLAlchemy session
            task_status: The task status data
            
        Returns:
            The created task status
        """
        # Check if status already exists
        existing = db.query(DBTaskStatus).filter(
            DBTaskStatus.job_id == task_status.job_id,
            DBTaskStatus.task_id == task_status.task_id
        ).first()
        
        if existing:
            return existing
        
        # Check if the job exists in the executionhistory table
        job_exists = db.query(ExecutionHistory).filter(
            ExecutionHistory.job_id == task_status.job_id
        ).first()
        
        # If job doesn't exist, create it first
        if not job_exists:
            logger.info(f"Job {task_status.job_id} not found, creating it before adding task status")
            
            # Create minimal execution record
            job_record = ExecutionHistory(
                job_id=task_status.job_id,
                status="running",
                trigger_type="api",
                run_name=f"Auto-created for task {task_status.task_id}",
                inputs={"auto_created": True}
            )
            
            # Add and commit to ensure it exists
            db.add(job_record)
            db.commit()
            
            logger.info(f"Created job record for {task_status.job_id}")
        
        # Create new status entry
        db_task_status = DBTaskStatus(
            job_id=task_status.job_id,
            task_id=task_status.task_id,
            status=task_status.status,
            agent_name=task_status.agent_name,
            started_at=datetime.now(UTC),
            completed_at=None
        )
        
        db.add(db_task_status)
        db.commit()
        db.refresh(db_task_status)
        
        return db_task_status

    def update_task_status(self, job_id: str, task_id: str, task_status: TaskStatusUpdate) -> Optional[DBTaskStatus]:
        """
        Update the status of a task (sync version).
        
        Args:
            job_id: The ID of the job
            task_id: The ID/key of the task
            task_status: The new status data
            
        Returns:
            The updated task status record or None if not found
        """
        if self._owns_session:
            # Create a session for this operation
            from src.db.session import SessionLocal
            with SessionLocal() as db:
                return self._update_task_status_sync(db, job_id, task_id, task_status)
        else:
            return self._update_task_status_sync(self.db, job_id, task_id, task_status)
            
    def _update_task_status_sync(self, db: Session, job_id: str, task_id: str, 
                                 task_status: TaskStatusUpdate) -> Optional[DBTaskStatus]:
        """
        Update task status with a provided session.
        
        Args:
            db: SQLAlchemy session
            job_id: The job ID
            task_id: The task ID
            task_status: The update data
            
        Returns:
            Updated task status or None
        """
        db_task_status = db.query(DBTaskStatus).filter(
            DBTaskStatus.job_id == job_id,
            DBTaskStatus.task_id == task_id
        ).first()
        
        if not db_task_status:
            return None
        
        # Update status
        db_task_status.status = task_status.status
        
        # If status is completed or failed, update completed_at
        if task_status.status in [TaskStatusEnum.COMPLETED, TaskStatusEnum.FAILED]:
            db_task_status.completed_at = datetime.now(UTC)
        
        db.commit()
        db.refresh(db_task_status)
        
        return db_task_status

    def get_task_status(self, job_id: str, task_id: str) -> Optional[DBTaskStatus]:
        """
        Get the current status of a task (sync version).
        
        Args:
            job_id: The ID of the job
            task_id: The ID/key of the task
            
        Returns:
            The task status record or None if not found
        """
        if self._owns_session:
            # Create a session for this operation
            from src.db.session import SessionLocal
            with SessionLocal() as db:
                return db.query(DBTaskStatus).filter(
                    DBTaskStatus.job_id == job_id,
                    DBTaskStatus.task_id == task_id
                ).first()
        else:
            return self.db.query(DBTaskStatus).filter(
                DBTaskStatus.job_id == job_id,
                DBTaskStatus.task_id == task_id
            ).first()
            
    def get_task_status_by_task_id(self, task_id: str) -> Optional[DBTaskStatus]:
        """
        Get the current status of a task by task_id only (sync version).
        Used when job_id is not known or needed.
        
        Args:
            task_id: The ID/key of the task
            
        Returns:
            The task status record or None if not found
        """
        if self._owns_session:
            # Create a session for this operation
            from src.db.session import SessionLocal
            with SessionLocal() as db:
                return db.query(DBTaskStatus).filter(
                    DBTaskStatus.task_id == task_id
                ).first()
        else:
            return self.db.query(DBTaskStatus).filter(
                DBTaskStatus.task_id == task_id
            ).first()

    def get_all_task_statuses(self, job_id: str) -> List[DBTaskStatus]:
        """
        Get all task statuses for a job (sync version).
        
        Args:
            job_id: The ID of the job
            
        Returns:
            List of task status records
        """
        if self._owns_session:
            # Create a session for this operation
            from src.db.session import SessionLocal
            with SessionLocal() as db:
                return db.query(DBTaskStatus).filter(
                    DBTaskStatus.job_id == job_id
                ).order_by(DBTaskStatus.started_at).all()
        else:
            return self.db.query(DBTaskStatus).filter(
                DBTaskStatus.job_id == job_id
            ).order_by(DBTaskStatus.started_at).all()

    def create_task_statuses_for_job(self, job_id: str, tasks_config: Dict[str, Dict]) -> List[DBTaskStatus]:
        """
        Create task status entries for all tasks in a job with RUNNING status (sync version).
        
        Args:
            job_id: The ID of the job
            tasks_config: Dictionary of tasks with their configurations
            
        Returns:
            List of created task status records
        """
        created_statuses = []
        
        for task_key, task_config in tasks_config.items():
            # Get the agent assigned to this task
            agent_name = task_config.get('agent')
            
            # Create task status entry
            task_status = TaskStatusCreate(
                job_id=job_id,
                task_id=task_key,
                status=TaskStatusEnum.RUNNING,
                agent_name=agent_name
            )
            db_task_status = self.create_task_status(task_status)
            created_statuses.append(db_task_status)
        
        return created_statuses
        
    def record_error_trace(self, run_id: int, task_key: str, error_type: str, error_message: str, 
                          error_metadata: Optional[Dict[str, Any]] = None) -> ErrorTrace:
        """
        Record an error trace for a task (sync version).
        
        Args:
            run_id: The database ID of the run
            task_key: The ID/key of the task
            error_type: The type of error
            error_message: The error message
            error_metadata: Additional metadata about the error
            
        Returns:
            The created error trace record
        """
        if self._owns_session:
            # Create a session for this operation
            from src.db.session import SessionLocal
            with SessionLocal() as db:
                return self._record_error_trace_sync(
                    db, run_id, task_key, error_type, error_message, error_metadata
                )
        else:
            return self._record_error_trace_sync(
                self.db, run_id, task_key, error_type, error_message, error_metadata
            )
            
    def _record_error_trace_sync(self, db: Session, run_id: int, task_key: str, error_type: str, 
                                error_message: str, error_metadata: Optional[Dict[str, Any]] = None) -> ErrorTrace:
        """
        Record an error trace with a provided session.
        
        Args:
            db: SQLAlchemy session
            run_id: The run ID
            task_key: The task key
            error_type: The error type
            error_message: The error message
            error_metadata: Additional metadata
            
        Returns:
            The created error trace
        """
        error_trace = ErrorTrace(
            run_id=run_id,
            task_key=task_key,
            error_type=error_type,
            error_message=error_message,
            timestamp=datetime.now(UTC),
            error_metadata=error_metadata or {}
        )
        
        db.add(error_trace)
        db.commit()
        db.refresh(error_trace)
        
        return error_trace 