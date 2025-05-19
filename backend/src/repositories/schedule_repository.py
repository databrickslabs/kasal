import logging
from typing import List, Optional, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.schedule import Schedule
from src.utils.cron_utils import calculate_next_run_from_last

logger = logging.getLogger(__name__)

class ScheduleRepository:
    """
    Repository for schedule operations.
    Handles data access for Schedule model.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
    
    async def create(self, schedule_data: Dict[str, Any]) -> Schedule:
        """
        Create a new schedule.
        
        Args:
            schedule_data: Schedule data for creation
            
        Returns:
            Created Schedule object
        """
        # Calculate next run time if not provided
        if 'next_run_at' not in schedule_data:
            schedule_data['next_run_at'] = calculate_next_run_from_last(schedule_data['cron_expression'])
        
        # Create schedule
        db_schedule = Schedule(**schedule_data)
        self.session.add(db_schedule)
        await self.session.commit()
        await self.session.refresh(db_schedule)
        
        return db_schedule
    
    async def find_all(self) -> List[Schedule]:
        """
        Find all schedules.
        
        Returns:
            List of Schedule objects
        """
        query = select(Schedule)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def find_by_id(self, schedule_id: int) -> Optional[Schedule]:
        """
        Find a schedule by ID.
        
        Args:
            schedule_id: ID of the schedule to find
            
        Returns:
            Schedule object if found, else None
        """
        query = select(Schedule).where(Schedule.id == schedule_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def find_due_schedules(self, current_time) -> List[Schedule]:
        """
        Find all schedules that are due to run.
        
        Args:
            current_time: Current datetime to compare against
            
        Returns:
            List of due Schedule objects
        """
        query = select(Schedule).where(
            Schedule.is_active == True,
            Schedule.next_run_at <= current_time
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def update(self, schedule_id: int, schedule_data: Dict[str, Any]) -> Optional[Schedule]:
        """
        Update a schedule by ID.
        
        Args:
            schedule_id: ID of the schedule to update
            schedule_data: Schedule data for update
            
        Returns:
            Updated Schedule object if found, else None
        """
        # Find the schedule
        schedule = await self.find_by_id(schedule_id)
        if not schedule:
            return None
        
        # Update fields
        for field, value in schedule_data.items():
            setattr(schedule, field, value)
        
        # If cron expression is updated, recalculate next run time
        if 'cron_expression' in schedule_data:
            schedule.next_run_at = calculate_next_run_from_last(
                schedule_data['cron_expression'],
                schedule.last_run_at
            )
        
        await self.session.commit()
        await self.session.refresh(schedule)
        
        return schedule
    
    async def delete(self, schedule_id: int) -> bool:
        """
        Delete a schedule by ID.
        
        Args:
            schedule_id: ID of the schedule to delete
            
        Returns:
            True if deleted, False if not found
        """
        # Find the schedule
        schedule = await self.find_by_id(schedule_id)
        if not schedule:
            return False
        
        # Delete the schedule
        await self.session.delete(schedule)
        await self.session.commit()
        
        return True
    
    async def toggle_active(self, schedule_id: int) -> Optional[Schedule]:
        """
        Toggle the active state of a schedule.
        
        Args:
            schedule_id: ID of the schedule to toggle
            
        Returns:
            Updated Schedule object if found, else None
        """
        # Find the schedule
        schedule = await self.find_by_id(schedule_id)
        if not schedule:
            return None
        
        # Toggle active state
        schedule.is_active = not schedule.is_active
        
        # If activating, recalculate next run time
        if schedule.is_active:
            schedule.next_run_at = calculate_next_run_from_last(
                schedule.cron_expression,
                schedule.last_run_at
            )
        
        await self.session.commit()
        await self.session.refresh(schedule)
        
        return schedule
    
    async def update_after_execution(self, schedule_id: int, execution_time) -> Optional[Schedule]:
        """
        Update schedule after execution.
        
        Args:
            schedule_id: ID of the schedule that was executed
            execution_time: Time when the execution occurred
            
        Returns:
            Updated Schedule object if found, else None
        """
        # Find the schedule
        schedule = await self.find_by_id(schedule_id)
        if not schedule:
            return None
        
        # Update last run time and calculate next run time
        schedule.last_run_at = execution_time
        schedule.next_run_at = calculate_next_run_from_last(
            schedule.cron_expression,
            execution_time
        )
        
        await self.session.commit()
        await self.session.refresh(schedule)
        
        return schedule 