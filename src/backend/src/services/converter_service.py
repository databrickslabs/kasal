"""
Converter Service
Business logic for measure converter operations
Orchestrates conversion repositories and integrates with KPI conversion infrastructure
"""

import logging
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import HTTPException, status

from src.repositories.conversion_repository import (
    ConversionHistoryRepository,
    ConversionJobRepository,
    SavedConverterConfigurationRepository,
)
from src.schemas.conversion import (
    # History
    ConversionHistoryCreate,
    ConversionHistoryUpdate,
    ConversionHistoryResponse,
    ConversionHistoryListResponse,
    ConversionHistoryFilter,
    ConversionStatistics,
    # Jobs
    ConversionJobCreate,
    ConversionJobUpdate,
    ConversionJobResponse,
    ConversionJobListResponse,
    ConversionJobStatusUpdate,
    # Saved Configs
    SavedConfigurationCreate,
    SavedConfigurationUpdate,
    SavedConfigurationResponse,
    SavedConfigurationListResponse,
    SavedConfigurationFilter,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class ConverterService:
    """
    Service for converter business logic.
    Orchestrates conversion operations, job management, and configuration storage.
    Integrates with existing KPI conversion infrastructure.
    """

    def __init__(self, session, group_context: Optional[GroupContext] = None):
        """
        Initialize service with session and group context.

        Args:
            session: Database session from FastAPI DI
            group_context: Optional group context for multi-tenant isolation
        """
        self.session = session
        self.group_context = group_context

        # Initialize repositories
        self.history_repo = ConversionHistoryRepository(session)
        self.job_repo = ConversionJobRepository(session)
        self.config_repo = SavedConverterConfigurationRepository(session)

    # ===== CONVERSION HISTORY METHODS =====

    async def create_history(
        self,
        history_data: ConversionHistoryCreate
    ) -> ConversionHistoryResponse:
        """
        Create a new conversion history entry.

        Args:
            history_data: Conversion history data

        Returns:
            Created conversion history entry
        """
        # Add group context
        history_dict = history_data.model_dump()
        if self.group_context:
            history_dict['group_id'] = self.group_context.primary_group_id
            history_dict['created_by_email'] = self.group_context.user_email

        # Create history
        history = await self.history_repo.create(history_dict)
        return ConversionHistoryResponse.model_validate(history)

    async def get_history(self, history_id: int) -> ConversionHistoryResponse:
        """
        Get conversion history by ID.

        Args:
            history_id: History entry ID

        Returns:
            Conversion history entry

        Raises:
            HTTPException: If not found
        """
        history = await self.history_repo.get(history_id)
        if not history:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversion history {history_id} not found"
            )
        return ConversionHistoryResponse.model_validate(history)

    async def update_history(
        self,
        history_id: int,
        update_data: ConversionHistoryUpdate
    ) -> ConversionHistoryResponse:
        """
        Update conversion history.

        Args:
            history_id: History entry ID
            update_data: Update data

        Returns:
            Updated conversion history

        Raises:
            HTTPException: If not found
        """
        history = await self.history_repo.get(history_id)
        if not history:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversion history {history_id} not found"
            )

        updated = await self.history_repo.update(
            history_id,
            update_data.model_dump(exclude_unset=True)
        )
        return ConversionHistoryResponse.model_validate(updated)

    async def list_history(
        self,
        filter_params: Optional[ConversionHistoryFilter] = None
    ) -> ConversionHistoryListResponse:
        """
        List conversion history with filters.

        Args:
            filter_params: Optional filter parameters

        Returns:
            List of conversion history entries
        """
        filter_params = filter_params or ConversionHistoryFilter()

        # Get group ID from context
        group_id = self.group_context.primary_group_id if self.group_context else None

        # Apply filters
        if filter_params.execution_id:
            history_list = await self.history_repo.find_by_execution_id(
                filter_params.execution_id
            )
        elif filter_params.source_format and filter_params.target_format:
            history_list = await self.history_repo.find_by_formats(
                filter_params.source_format,
                filter_params.target_format,
                group_id=group_id,
                limit=filter_params.limit
            )
        elif filter_params.status == "success":
            history_list = await self.history_repo.find_successful(
                group_id=group_id,
                limit=filter_params.limit
            )
        elif filter_params.status == "failed":
            history_list = await self.history_repo.find_failed(
                group_id=group_id,
                limit=filter_params.limit
            )
        else:
            history_list = await self.history_repo.find_by_group(
                group_id=group_id,
                limit=filter_params.limit,
                offset=filter_params.offset
            )

        return ConversionHistoryListResponse(
            history=[ConversionHistoryResponse.model_validate(h) for h in history_list],
            count=len(history_list),
            limit=filter_params.limit,
            offset=filter_params.offset
        )

    async def get_statistics(self, days: int = 30) -> ConversionStatistics:
        """
        Get conversion statistics.

        Args:
            days: Number of days to analyze

        Returns:
            Conversion statistics
        """
        group_id = self.group_context.primary_group_id if self.group_context else None
        stats = await self.history_repo.get_statistics(group_id=group_id, days=days)
        return ConversionStatistics(**stats)

    # ===== CONVERSION JOB METHODS =====

    async def create_job(
        self,
        job_data: ConversionJobCreate
    ) -> ConversionJobResponse:
        """
        Create a new conversion job.

        Args:
            job_data: Job creation data

        Returns:
            Created conversion job
        """
        # Generate UUID for job
        job_id = str(uuid.uuid4())

        # Add group context
        job_dict = job_data.model_dump()
        job_dict['id'] = job_id
        job_dict['status'] = 'pending'
        if self.group_context:
            job_dict['group_id'] = self.group_context.primary_group_id
            job_dict['created_by_email'] = self.group_context.user_email

        # Create job
        job = await self.job_repo.create(job_dict)
        return ConversionJobResponse.model_validate(job)

    async def get_job(self, job_id: str) -> ConversionJobResponse:
        """
        Get conversion job by ID.

        Args:
            job_id: Job UUID

        Returns:
            Conversion job

        Raises:
            HTTPException: If not found
        """
        job = await self.job_repo.get(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversion job {job_id} not found"
            )
        return ConversionJobResponse.model_validate(job)

    async def update_job(
        self,
        job_id: str,
        update_data: ConversionJobUpdate
    ) -> ConversionJobResponse:
        """
        Update conversion job.

        Args:
            job_id: Job UUID
            update_data: Update data

        Returns:
            Updated conversion job

        Raises:
            HTTPException: If not found
        """
        job = await self.job_repo.get(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversion job {job_id} not found"
            )

        updated = await self.job_repo.update(
            job_id,
            update_data.model_dump(exclude_unset=True)
        )
        return ConversionJobResponse.model_validate(updated)

    async def update_job_status(
        self,
        job_id: str,
        status_update: ConversionJobStatusUpdate
    ) -> ConversionJobResponse:
        """
        Update job status and progress.

        Args:
            job_id: Job UUID
            status_update: Status update data

        Returns:
            Updated conversion job

        Raises:
            HTTPException: If not found
        """
        updated = await self.job_repo.update_status(
            job_id,
            status=status_update.status,
            progress=status_update.progress,
            error_message=status_update.error_message
        )

        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversion job {job_id} not found"
            )

        return ConversionJobResponse.model_validate(updated)

    async def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 50
    ) -> ConversionJobListResponse:
        """
        List conversion jobs with optional status filter.

        Args:
            status: Optional status filter
            limit: Maximum number of results

        Returns:
            List of conversion jobs
        """
        group_id = self.group_context.primary_group_id if self.group_context else None

        if status:
            jobs = await self.job_repo.find_by_status(
                status=status,
                group_id=group_id,
                limit=limit
            )
        else:
            # Get all active jobs by default
            jobs = await self.job_repo.find_active_jobs(group_id=group_id)

        return ConversionJobListResponse(
            jobs=[ConversionJobResponse.model_validate(j) for j in jobs],
            count=len(jobs)
        )

    async def cancel_job(self, job_id: str) -> ConversionJobResponse:
        """
        Cancel a pending or running job.

        Args:
            job_id: Job UUID

        Returns:
            Cancelled job

        Raises:
            HTTPException: If not found or not cancellable
        """
        cancelled = await self.job_repo.cancel_job(job_id)

        if not cancelled:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Job {job_id} not found or cannot be cancelled"
            )

        return ConversionJobResponse.model_validate(cancelled)

    # ===== SAVED CONFIGURATION METHODS =====

    async def create_saved_config(
        self,
        config_data: SavedConfigurationCreate
    ) -> SavedConfigurationResponse:
        """
        Create a saved converter configuration.

        Args:
            config_data: Configuration data

        Returns:
            Created configuration

        Raises:
            HTTPException: If user not authenticated
        """
        if not self.group_context or not self.group_context.user_email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required to save configurations"
            )

        # Add group context
        config_dict = config_data.model_dump()
        config_dict['group_id'] = self.group_context.primary_group_id
        config_dict['created_by_email'] = self.group_context.user_email

        # Create configuration
        config = await self.config_repo.create(config_dict)
        return SavedConfigurationResponse.model_validate(config)

    async def get_saved_config(self, config_id: int) -> SavedConfigurationResponse:
        """
        Get saved configuration by ID.

        Args:
            config_id: Configuration ID

        Returns:
            Saved configuration

        Raises:
            HTTPException: If not found
        """
        config = await self.config_repo.get(config_id)
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Configuration {config_id} not found"
            )
        return SavedConfigurationResponse.model_validate(config)

    async def update_saved_config(
        self,
        config_id: int,
        update_data: SavedConfigurationUpdate
    ) -> SavedConfigurationResponse:
        """
        Update saved configuration.

        Args:
            config_id: Configuration ID
            update_data: Update data

        Returns:
            Updated configuration

        Raises:
            HTTPException: If not found or not authorized
        """
        config = await self.config_repo.get(config_id)
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Configuration {config_id} not found"
            )

        # Check ownership (unless admin)
        if self.group_context and config.created_by_email != self.group_context.user_email:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to update this configuration"
            )

        updated = await self.config_repo.update(
            config_id,
            update_data.model_dump(exclude_unset=True)
        )
        return SavedConfigurationResponse.model_validate(updated)

    async def delete_saved_config(self, config_id: int) -> Dict[str, str]:
        """
        Delete saved configuration.

        Args:
            config_id: Configuration ID

        Returns:
            Success message

        Raises:
            HTTPException: If not found or not authorized
        """
        config = await self.config_repo.get(config_id)
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Configuration {config_id} not found"
            )

        # Check ownership (unless admin)
        if self.group_context and config.created_by_email != self.group_context.user_email:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to delete this configuration"
            )

        await self.config_repo.delete(config_id)
        return {"message": f"Configuration {config_id} deleted successfully"}

    async def list_saved_configs(
        self,
        filter_params: Optional[SavedConfigurationFilter] = None
    ) -> SavedConfigurationListResponse:
        """
        List saved configurations with filters.

        Args:
            filter_params: Optional filter parameters

        Returns:
            List of saved configurations
        """
        filter_params = filter_params or SavedConfigurationFilter()

        group_id = self.group_context.primary_group_id if self.group_context else None
        user_email = self.group_context.user_email if self.group_context else None

        # Apply filters
        if filter_params.is_template:
            configs = await self.config_repo.find_templates()
        elif filter_params.is_public:
            configs = await self.config_repo.find_public(group_id=group_id)
        elif filter_params.source_format and filter_params.target_format:
            configs = await self.config_repo.find_by_formats(
                source_format=filter_params.source_format,
                target_format=filter_params.target_format,
                group_id=group_id,
                user_email=user_email
            )
        elif filter_params.search:
            configs = await self.config_repo.search_by_name(
                search_term=filter_params.search,
                group_id=group_id,
                user_email=user_email
            )
        elif user_email:
            configs = await self.config_repo.find_by_user(
                created_by_email=user_email,
                group_id=group_id
            )
        else:
            # Return empty list if no user context
            configs = []

        # Apply limit
        configs = configs[:filter_params.limit]

        return SavedConfigurationListResponse(
            configurations=[SavedConfigurationResponse.model_validate(c) for c in configs],
            count=len(configs)
        )

    async def use_saved_config(self, config_id: int) -> SavedConfigurationResponse:
        """
        Mark a configuration as used (increment use count).

        Args:
            config_id: Configuration ID

        Returns:
            Updated configuration

        Raises:
            HTTPException: If not found
        """
        updated = await self.config_repo.increment_use_count(config_id)

        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Configuration {config_id} not found"
            )

        return SavedConfigurationResponse.model_validate(updated)
