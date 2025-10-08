import pytest
from unittest.mock import AsyncMock, patch, Mock
from fastapi import HTTPException
from src.api.dspy_router import (
    get_dspy_enabled,
    set_dspy_enabled,
    get_dspy_stats,
    DSPyEnabledRequest,
)


class TestGetDSPyEnabled:
    """Test get_dspy_enabled endpoint."""

    @pytest.mark.asyncio
    async def test_get_dspy_enabled_true(self):
        """Test getting DSPy enabled status when enabled."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(return_value=True)
            mock_service_class.return_value = mock_service
            
            result = await get_dspy_enabled(session=session, group_context=group_context)
            
            assert result.enabled is True
            mock_service_class.assert_called_once_with(session, group_id="test-group")
            mock_service.is_enabled.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_dspy_enabled_false(self):
        """Test getting DSPy enabled status when disabled."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(return_value=False)
            mock_service_class.return_value = mock_service
            
            result = await get_dspy_enabled(session=session, group_context=group_context)
            
            assert result.enabled is False

    @pytest.mark.asyncio
    async def test_get_dspy_enabled_no_group_context(self):
        """Test getting DSPy enabled status without group context."""
        session = AsyncMock()
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(return_value=False)
            mock_service_class.return_value = mock_service
            
            result = await get_dspy_enabled(session=session, group_context=None)
            
            assert result.enabled is False
            mock_service_class.assert_called_once_with(session, group_id=None)

    @pytest.mark.asyncio
    async def test_get_dspy_enabled_exception(self):
        """Test getting DSPy enabled status when exception occurs."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(side_effect=Exception("Database error"))
            mock_service_class.return_value = mock_service
            
            with pytest.raises(HTTPException) as exc_info:
                await get_dspy_enabled(session=session, group_context=group_context)
            
            assert exc_info.value.status_code == 500
            assert "Database error" in str(exc_info.value.detail)


class TestSetDSPyEnabled:
    """Test set_dspy_enabled endpoint."""

    @pytest.mark.asyncio
    async def test_set_dspy_enabled_true(self):
        """Test enabling DSPy."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        payload = DSPyEnabledRequest(enabled=True)
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.set_enabled = AsyncMock()
            mock_service_class.return_value = mock_service
            
            result = await set_dspy_enabled(payload=payload, session=session, group_context=group_context)
            
            assert result.enabled is True
            mock_service_class.assert_called_once_with(session, group_id="test-group")
            mock_service.set_enabled.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_set_dspy_enabled_false(self):
        """Test disabling DSPy."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        payload = DSPyEnabledRequest(enabled=False)
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.set_enabled = AsyncMock()
            mock_service_class.return_value = mock_service
            
            result = await set_dspy_enabled(payload=payload, session=session, group_context=group_context)
            
            assert result.enabled is False
            mock_service.set_enabled.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_set_dspy_enabled_no_group_context(self):
        """Test setting DSPy enabled without group context."""
        session = AsyncMock()
        payload = DSPyEnabledRequest(enabled=True)
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.set_enabled = AsyncMock()
            mock_service_class.return_value = mock_service
            
            result = await set_dspy_enabled(payload=payload, session=session, group_context=None)
            
            assert result.enabled is True
            mock_service_class.assert_called_once_with(session, group_id=None)

    @pytest.mark.asyncio
    async def test_set_dspy_enabled_exception(self):
        """Test setting DSPy enabled when exception occurs."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        payload = DSPyEnabledRequest(enabled=True)
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.set_enabled = AsyncMock(side_effect=Exception("Database error"))
            mock_service_class.return_value = mock_service
            
            with pytest.raises(HTTPException) as exc_info:
                await set_dspy_enabled(payload=payload, session=session, group_context=group_context)
            
            assert exc_info.value.status_code == 500
            assert "Database error" in str(exc_info.value.detail)


class TestGetDSPyStats:
    """Test get_dspy_stats endpoint."""

    @pytest.mark.asyncio
    async def test_get_dspy_stats_enabled(self):
        """Test getting DSPy stats when enabled."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(return_value=True)
            mock_service_class.return_value = mock_service
            
            result = await get_dspy_stats(session=session, group_context=group_context)
            
            assert result.enabled is True
            assert result.total_optimizations == 0
            assert result.active_signatures == 0
            assert result.average_improvement == 0.0
            assert result.last_optimization is None
            assert result.examples_collected == 0

    @pytest.mark.asyncio
    async def test_get_dspy_stats_disabled(self):
        """Test getting DSPy stats when disabled."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(return_value=False)
            mock_service_class.return_value = mock_service
            
            result = await get_dspy_stats(session=session, group_context=group_context)
            
            assert result.enabled is False

    @pytest.mark.asyncio
    async def test_get_dspy_stats_no_group_context(self):
        """Test getting DSPy stats without group context."""
        session = AsyncMock()
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(return_value=False)
            mock_service_class.return_value = mock_service
            
            result = await get_dspy_stats(session=session, group_context=None)
            
            assert result.enabled is False
            mock_service_class.assert_called_once_with(session, group_id=None)

    @pytest.mark.asyncio
    async def test_get_dspy_stats_exception(self):
        """Test getting DSPy stats when exception occurs."""
        session = AsyncMock()
        group_context = Mock()
        group_context.primary_group_id = "test-group"
        
        with patch('src.api.dspy_router.DSPySettingsService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.is_enabled = AsyncMock(side_effect=Exception("Database error"))
            mock_service_class.return_value = mock_service
            
            with pytest.raises(HTTPException) as exc_info:
                await get_dspy_stats(session=session, group_context=group_context)
            
            assert exc_info.value.status_code == 500
            assert "Database error" in str(exc_info.value.detail)

