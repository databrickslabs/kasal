"""
Unit tests for ExecutionCleanupService.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime

from src.services.execution_cleanup_service import ExecutionCleanupService
from src.models.execution_status import ExecutionStatus


class TestExecutionCleanupService:
    """Test cases for ExecutionCleanupService."""
    
    @pytest.mark.asyncio
    async def test_cleanup_stale_jobs_on_startup_no_jobs(self):
        """Test cleanup when no stale jobs exist."""
        # Mock the repository and session
        mock_repo = MagicMock()
        mock_repo.get_execution_history = AsyncMock(return_value=([], 0))
        
        with patch('src.services.execution_cleanup_service.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session
            
            with patch('src.services.execution_cleanup_service.ExecutionRepository', return_value=mock_repo):
                # Run cleanup
                result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()
                
                # Verify no jobs were cleaned
                assert result == 0
                mock_repo.get_execution_history.assert_called_once_with(
                    limit=1000,
                    offset=0,
                    status_filter=[
                        ExecutionStatus.PENDING.value,
                        ExecutionStatus.PREPARING.value,
                        ExecutionStatus.RUNNING.value
                    ]
                )
    
    @pytest.mark.asyncio
    async def test_cleanup_stale_jobs_on_startup_with_stale_jobs(self):
        """Test cleanup when stale jobs exist."""
        # Create mock stale jobs
        mock_jobs = [
            MagicMock(job_id="job1", status=ExecutionStatus.RUNNING.value),
            MagicMock(job_id="job2", status=ExecutionStatus.PREPARING.value),
            MagicMock(job_id="job3", status=ExecutionStatus.PENDING.value)
        ]
        
        # Mock the repository
        mock_repo = MagicMock()
        mock_repo.get_execution_history = AsyncMock(return_value=(mock_jobs, 3))
        
        with patch('src.services.execution_cleanup_service.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session
            
            with patch('src.services.execution_cleanup_service.ExecutionRepository', return_value=mock_repo):
                with patch('src.services.execution_cleanup_service.ExecutionStatusService.update_status', 
                          new_callable=AsyncMock) as mock_update_status:
                    # Mock successful updates
                    mock_update_status.return_value = True
                    
                    # Run cleanup
                    result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()
                    
                    # Verify all jobs were cleaned
                    assert result == 3
                    assert mock_update_status.call_count == 3
                    
                    # Verify each job was updated correctly
                    for i, job in enumerate(mock_jobs):
                        mock_update_status.assert_any_call(
                            job_id=job.job_id,
                            status=ExecutionStatus.CANCELLED.value,
                            message="Job cancelled - service was restarted while job was running"
                        )
    
    @pytest.mark.asyncio
    async def test_cleanup_stale_jobs_on_startup_with_update_failures(self):
        """Test cleanup when some status updates fail."""
        # Create mock stale jobs
        mock_jobs = [
            MagicMock(job_id="job1", status=ExecutionStatus.RUNNING.value),
            MagicMock(job_id="job2", status=ExecutionStatus.PREPARING.value),
            MagicMock(job_id="job3", status=ExecutionStatus.PENDING.value)
        ]
        
        # Mock the repository
        mock_repo = MagicMock()
        mock_repo.get_execution_history = AsyncMock(return_value=(mock_jobs, 3))
        
        with patch('src.services.execution_cleanup_service.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session
            
            with patch('src.services.execution_cleanup_service.ExecutionRepository', return_value=mock_repo):
                with patch('src.services.execution_cleanup_service.ExecutionStatusService.update_status', 
                          new_callable=AsyncMock) as mock_update_status:
                    # Mock mixed success/failure updates
                    mock_update_status.side_effect = [True, False, True]
                    
                    # Run cleanup
                    result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()
                    
                    # Verify only successful updates were counted
                    assert result == 2
                    assert mock_update_status.call_count == 3
    
    @pytest.mark.asyncio
    async def test_cleanup_stale_jobs_on_startup_with_exception(self):
        """Test cleanup handles exceptions gracefully."""
        with patch('src.services.execution_cleanup_service.async_session_factory') as mock_session_factory:
            # Mock session factory to raise an exception
            mock_session_factory.side_effect = Exception("Database connection failed")
            
            # Run cleanup
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()
            
            # Verify it returns 0 on error
            assert result == 0
    
    @pytest.mark.asyncio
    async def test_get_stale_jobs(self):
        """Test getting list of stale job IDs."""
        # Create mock stale jobs
        mock_jobs = [
            MagicMock(job_id="job1", status=ExecutionStatus.RUNNING.value),
            MagicMock(job_id="job2", status=ExecutionStatus.PREPARING.value),
            MagicMock(job_id="job3", status=ExecutionStatus.PENDING.value)
        ]
        
        # Mock the repository
        mock_repo = MagicMock()
        mock_repo.get_execution_history = AsyncMock(return_value=(mock_jobs, 3))
        
        with patch('src.services.execution_cleanup_service.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session
            
            with patch('src.services.execution_cleanup_service.ExecutionRepository', return_value=mock_repo):
                # Get stale jobs
                result = await ExecutionCleanupService.get_stale_jobs()
                
                # Verify job IDs were returned
                assert result == ["job1", "job2", "job3"]
    
    @pytest.mark.asyncio
    async def test_get_stale_jobs_with_exception(self):
        """Test get_stale_jobs handles exceptions gracefully."""
        with patch('src.services.execution_cleanup_service.async_session_factory') as mock_session_factory:
            # Mock session factory to raise an exception
            mock_session_factory.side_effect = Exception("Database connection failed")
            
            # Get stale jobs
            result = await ExecutionCleanupService.get_stale_jobs()
            
            # Verify it returns empty list on error
            assert result == []