"""
Conversion Repositories
Repository pattern implementations for converter models
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from sqlalchemy import select, update, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.conversion import (
    ConversionHistory,
    ConversionJob,
    SavedConverterConfiguration,
)


class ConversionHistoryRepository(BaseRepository[ConversionHistory]):
    """
    Repository for ConversionHistory model.
    Tracks all conversion attempts with audit trail.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(ConversionHistory, session)

    async def find_by_execution_id(self, execution_id: str) -> List[ConversionHistory]:
        """
        Find all conversion history entries for a specific execution.

        Args:
            execution_id: Execution ID to filter by

        Returns:
            List of conversion history entries
        """
        query = select(self.model).where(
            self.model.execution_id == execution_id
        ).order_by(desc(self.model.created_at))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_group(
        self,
        group_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[ConversionHistory]:
        """
        Find conversion history for a specific group.

        Args:
            group_id: Group ID to filter by
            limit: Maximum number of results
            offset: Number of results to skip

        Returns:
            List of conversion history entries
        """
        query = (
            select(self.model)
            .where(self.model.group_id == group_id)
            .order_by(desc(self.model.created_at))
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_formats(
        self,
        source_format: str,
        target_format: str,
        group_id: Optional[str] = None,
        limit: int = 50
    ) -> List[ConversionHistory]:
        """
        Find conversion history by source and target formats.

        Args:
            source_format: Source format (e.g., "powerbi", "yaml")
            target_format: Target format (e.g., "dax", "sql")
            group_id: Optional group ID to filter by
            limit: Maximum number of results

        Returns:
            List of conversion history entries
        """
        conditions = [
            self.model.source_format == source_format,
            self.model.target_format == target_format,
        ]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.created_at))
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_successful(
        self,
        group_id: Optional[str] = None,
        limit: int = 100
    ) -> List[ConversionHistory]:
        """
        Find successful conversions.

        Args:
            group_id: Optional group ID to filter by
            limit: Maximum number of results

        Returns:
            List of successful conversion history entries
        """
        conditions = [self.model.status == "success"]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.created_at))
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_failed(
        self,
        group_id: Optional[str] = None,
        limit: int = 100
    ) -> List[ConversionHistory]:
        """
        Find failed conversions for debugging.

        Args:
            group_id: Optional group ID to filter by
            limit: Maximum number of results

        Returns:
            List of failed conversion history entries
        """
        conditions = [self.model.status == "failed"]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.created_at))
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_statistics(
        self,
        group_id: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get conversion statistics for analytics.

        Args:
            group_id: Optional group ID to filter by
            days: Number of days to look back

        Returns:
            Dictionary with statistics
        """
        since = datetime.utcnow() - timedelta(days=days)
        conditions = [self.model.created_at >= since]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        # Total conversions
        total_query = select(func.count(self.model.id)).where(and_(*conditions))
        total_result = await self.session.execute(total_query)
        total = total_result.scalar()

        # Success count
        success_conditions = conditions + [self.model.status == "success"]
        success_query = select(func.count(self.model.id)).where(and_(*success_conditions))
        success_result = await self.session.execute(success_query)
        success_count = success_result.scalar()

        # Failed count
        failed_conditions = conditions + [self.model.status == "failed"]
        failed_query = select(func.count(self.model.id)).where(and_(*failed_conditions))
        failed_result = await self.session.execute(failed_query)
        failed_count = failed_result.scalar()

        # Average execution time
        avg_time_query = select(func.avg(self.model.execution_time_ms)).where(
            and_(*conditions, self.model.execution_time_ms.isnot(None))
        )
        avg_time_result = await self.session.execute(avg_time_query)
        avg_execution_time = avg_time_result.scalar() or 0

        # Most common conversions
        popular_query = (
            select(
                self.model.source_format,
                self.model.target_format,
                func.count(self.model.id).label('count')
            )
            .where(and_(*conditions))
            .group_by(self.model.source_format, self.model.target_format)
            .order_by(desc('count'))
            .limit(10)
        )
        popular_result = await self.session.execute(popular_query)
        popular_conversions = [
            {
                'source': row.source_format,
                'target': row.target_format,
                'count': row.count
            }
            for row in popular_result
        ]

        return {
            'total_conversions': total,
            'successful': success_count,
            'failed': failed_count,
            'success_rate': (success_count / total * 100) if total > 0 else 0,
            'average_execution_time_ms': round(avg_execution_time, 2),
            'popular_conversions': popular_conversions,
            'period_days': days,
        }


class ConversionJobRepository(BaseRepository[ConversionJob]):
    """
    Repository for ConversionJob model.
    Manages async conversion jobs with status tracking.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(ConversionJob, session)

    async def find_by_status(
        self,
        status: str,
        group_id: Optional[str] = None,
        limit: int = 50
    ) -> List[ConversionJob]:
        """
        Find jobs by status.

        Args:
            status: Job status (pending, running, completed, failed, cancelled)
            group_id: Optional group ID to filter by
            limit: Maximum number of results

        Returns:
            List of conversion jobs
        """
        conditions = [self.model.status == status]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.created_at))
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_active_jobs(
        self,
        group_id: Optional[str] = None
    ) -> List[ConversionJob]:
        """
        Find all active (pending or running) jobs.

        Args:
            group_id: Optional group ID to filter by

        Returns:
            List of active conversion jobs
        """
        conditions = [
            self.model.status.in_(['pending', 'running'])
        ]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        query = select(self.model).where(and_(*conditions))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def update_status(
        self,
        job_id: str,
        status: str,
        progress: Optional[float] = None,
        error_message: Optional[str] = None
    ) -> Optional[ConversionJob]:
        """
        Update job status and progress.

        Args:
            job_id: Job ID
            status: New status
            progress: Optional progress (0.0 to 1.0)
            error_message: Optional error message

        Returns:
            Updated job if found, else None
        """
        update_data: Dict[str, Any] = {
            'status': status,
            'updated_at': datetime.utcnow(),
        }

        if progress is not None:
            update_data['progress'] = progress

        if error_message is not None:
            update_data['error_message'] = error_message

        if status == 'running' and not await self.session.scalar(
            select(self.model.started_at).where(self.model.id == job_id)
        ):
            update_data['started_at'] = datetime.utcnow()

        if status in ['completed', 'failed', 'cancelled']:
            update_data['completed_at'] = datetime.utcnow()

        query = (
            update(self.model)
            .where(self.model.id == job_id)
            .values(**update_data)
        )
        await self.session.execute(query)

        # Fetch and return the updated job
        return await self.get(job_id)

    async def update_result(
        self,
        job_id: str,
        result: Dict[str, Any]
    ) -> Optional[ConversionJob]:
        """
        Update job result.

        Args:
            job_id: Job ID
            result: Conversion result data

        Returns:
            Updated job if found, else None
        """
        query = (
            update(self.model)
            .where(self.model.id == job_id)
            .values(result=result, updated_at=datetime.utcnow())
        )
        await self.session.execute(query)
        return await self.get(job_id)

    async def cancel_job(self, job_id: str) -> Optional[ConversionJob]:
        """
        Cancel a pending or running job.

        Args:
            job_id: Job ID

        Returns:
            Updated job if found and cancellable, else None
        """
        query = (
            update(self.model)
            .where(
                and_(
                    self.model.id == job_id,
                    self.model.status.in_(['pending', 'running'])
                )
            )
            .values(
                status='cancelled',
                updated_at=datetime.utcnow(),
                completed_at=datetime.utcnow()
            )
        )
        result = await self.session.execute(query)
        if result.rowcount > 0:
            return await self.get(job_id)
        return None


class SavedConverterConfigurationRepository(BaseRepository[SavedConverterConfiguration]):
    """
    Repository for SavedConverterConfiguration model.
    Manages user-saved converter configurations.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(SavedConverterConfiguration, session)

    async def find_by_user(
        self,
        created_by_email: str,
        group_id: Optional[str] = None
    ) -> List[SavedConverterConfiguration]:
        """
        Find configurations created by a specific user.

        Args:
            created_by_email: User's email
            group_id: Optional group ID to filter by

        Returns:
            List of saved configurations
        """
        conditions = [self.model.created_by_email == created_by_email]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.last_used_at), desc(self.model.created_at))
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_public(
        self,
        group_id: Optional[str] = None
    ) -> List[SavedConverterConfiguration]:
        """
        Find public/shared configurations.

        Args:
            group_id: Optional group ID to filter by

        Returns:
            List of public configurations
        """
        conditions = [self.model.is_public == True]
        if group_id:
            conditions.append(self.model.group_id == group_id)

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.use_count), desc(self.model.created_at))
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_templates(self) -> List[SavedConverterConfiguration]:
        """
        Find system template configurations.

        Returns:
            List of template configurations
        """
        query = (
            select(self.model)
            .where(self.model.is_template == True)
            .order_by(self.model.name)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_formats(
        self,
        source_format: str,
        target_format: str,
        group_id: Optional[str] = None,
        user_email: Optional[str] = None
    ) -> List[SavedConverterConfiguration]:
        """
        Find configurations by conversion formats.

        Args:
            source_format: Source format
            target_format: Target format
            group_id: Optional group ID to filter by
            user_email: Optional user email to filter by

        Returns:
            List of matching configurations
        """
        conditions = [
            self.model.source_format == source_format,
            self.model.target_format == target_format,
        ]
        if group_id:
            conditions.append(self.model.group_id == group_id)
        if user_email:
            conditions.append(
                or_(
                    self.model.created_by_email == user_email,
                    self.model.is_public == True
                )
            )

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.use_count), desc(self.model.created_at))
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def increment_use_count(
        self,
        config_id: int
    ) -> Optional[SavedConverterConfiguration]:
        """
        Increment use count and update last_used_at.

        Args:
            config_id: Configuration ID

        Returns:
            Updated configuration if found, else None
        """
        query = (
            update(self.model)
            .where(self.model.id == config_id)
            .values(
                use_count=self.model.use_count + 1,
                last_used_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        )
        await self.session.execute(query)
        return await self.get(config_id)

    async def search_by_name(
        self,
        search_term: str,
        group_id: Optional[str] = None,
        user_email: Optional[str] = None
    ) -> List[SavedConverterConfiguration]:
        """
        Search configurations by name.

        Args:
            search_term: Search term for name
            group_id: Optional group ID to filter by
            user_email: Optional user email to filter by (shows user's + public)

        Returns:
            List of matching configurations
        """
        conditions = [
            self.model.name.ilike(f'%{search_term}%')
        ]
        if group_id:
            conditions.append(self.model.group_id == group_id)
        if user_email:
            conditions.append(
                or_(
                    self.model.created_by_email == user_email,
                    self.model.is_public == True
                )
            )

        query = (
            select(self.model)
            .where(and_(*conditions))
            .order_by(desc(self.model.use_count), self.model.name)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
