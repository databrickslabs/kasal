import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import Optional, Dict, Any
import asyncio
import threading

# Test DSPyOptimizationService - based on actual code inspection

from src.services.dspy_optimization_service import (
    DSPyOptimizationService,
    OptimizationType,
    _get_group_lock,
    _get_trace_hydration_lock,
    RateLimitedLM
)


class TestDSPyOptimizationServiceInit:
    """Test DSPyOptimizationService initialization"""

    def test_dspy_optimization_service_init_valid_session(self):
        """Test DSPyOptimizationService __init__ with valid session"""
        mock_session = Mock()

        service = DSPyOptimizationService(mock_session)

        assert service.session == mock_session
        assert service.group_context is None
        assert service.group_id is None
        assert service.mlflow_service is None
        assert service.databricks_service is None
        assert service.modules == {}
        assert service.optimized_modules == {}
        assert hasattr(service, 'config')

    def test_dspy_optimization_service_init_none_session(self):
        """Test DSPyOptimizationService __init__ with None session"""
        mock_session = None

        service = DSPyOptimizationService(mock_session)

        assert service.session is None


class TestDSPyOptimizationServiceProperties:
    """Test DSPyOptimizationService properties and utility methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = DSPyOptimizationService(self.mock_session)

    def test_group_context_property_none(self):
        """Test group_context property is None initially"""
        assert self.service.group_context is None

    def test_group_context_property_set(self):
        """Test group_context property can be set"""
        mock_context = Mock()
        self.service.group_context = mock_context
        assert self.service.group_context == mock_context

    def test_group_id_property_none(self):
        """Test group_id property is None initially"""
        assert self.service.group_id is None

    def test_group_id_property_set(self):
        """Test group_id property can be set"""
        test_group_id = "test-group-id"
        self.service.group_id = test_group_id
        assert self.service.group_id == test_group_id

    def test_modules_property_empty(self):
        """Test modules property is empty dict initially"""
        assert self.service.modules == {}

    def test_optimized_modules_property_empty(self):
        """Test optimized_modules property is empty dict initially"""
        assert self.service.optimized_modules == {}


class TestOptimizationType:
    """Test OptimizationType enum"""

    def test_optimization_type_values(self):
        """Test OptimizationType enum has expected values"""
        assert OptimizationType.INTENT_DETECTION == "intent_detection"
        assert OptimizationType.AGENT_GENERATION == "agent_generation"
        assert OptimizationType.CREW_GENERATION == "crew_generation"
        assert OptimizationType.TASK_GENERATION == "task_generation"

    def test_optimization_type_list_all(self):
        """Test OptimizationType can list all values"""
        values = list(OptimizationType)
        assert len(values) == 4
        assert OptimizationType.INTENT_DETECTION in values
        assert OptimizationType.AGENT_GENERATION in values
        assert OptimizationType.CREW_GENERATION in values
        assert OptimizationType.TASK_GENERATION in values


class TestDSPyOptimizationServiceServiceInitialization:
    """Test DSPyOptimizationService service initialization"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = DSPyOptimizationService(self.mock_session)

    def test_ensure_services_initialized_no_group_context(self):
        """Test _ensure_services_initialized with no group context"""
        self.service.group_context = None

        with patch('src.services.dspy_optimization_service.MLflowService') as mock_mlflow_service:
            with patch('src.services.dspy_optimization_service.DatabricksService') as mock_databricks_service:
                self.service._ensure_services_initialized()

                # Should not initialize services without group context
                mock_mlflow_service.assert_not_called()
                mock_databricks_service.assert_not_called()

    def test_ensure_services_initialized_with_group_context(self):
        """Test _ensure_services_initialized with group context"""
        mock_group_context = Mock()
        mock_group_context.primary_group_id = "test-group-id"
        self.service.group_context = mock_group_context

        with patch('src.services.dspy_optimization_service.MLflowService') as mock_mlflow_service:
            with patch('src.services.dspy_optimization_service.DatabricksService') as mock_databricks_service:
                self.service._ensure_services_initialized()

                assert self.service.group_id == "test-group-id"
                mock_mlflow_service.assert_called_once_with(self.mock_session, group_id="test-group-id")
                mock_databricks_service.assert_called_once_with(session=self.mock_session, group_id="test-group-id")

    def test_ensure_services_initialized_services_already_exist(self):
        """Test _ensure_services_initialized when services already exist"""
        mock_group_context = Mock()
        mock_group_context.primary_group_id = "test-group-id"
        self.service.group_context = mock_group_context
        self.service.mlflow_service = Mock()
        self.service.databricks_service = Mock()

        with patch('src.services.dspy_optimization_service.MLflowService') as mock_mlflow_service:
            with patch('src.services.dspy_optimization_service.DatabricksService') as mock_databricks_service:
                self.service._ensure_services_initialized()

                # Should not create new services
                mock_mlflow_service.assert_not_called()
                mock_databricks_service.assert_not_called()


class TestDSPyOptimizationServiceUtilityFunctions:
    """Test module-level utility functions"""

    def test_get_group_lock_creates_new_lock(self):
        """Test _get_group_lock creates new lock for new key"""
        key = "test-key-1"
        
        lock = _get_group_lock(key)
        
        assert lock is not None
        assert hasattr(lock, 'acquire')
        assert hasattr(lock, 'release')

    def test_get_group_lock_returns_same_lock(self):
        """Test _get_group_lock returns same lock for same key"""
        key = "test-key-2"
        
        lock1 = _get_group_lock(key)
        lock2 = _get_group_lock(key)
        
        assert lock1 is lock2

    def test_get_trace_hydration_lock(self):
        """Test _get_trace_hydration_lock returns lock"""
        lock = _get_trace_hydration_lock()
        
        assert lock is not None
        assert hasattr(lock, 'acquire')
        assert hasattr(lock, 'release')

    def test_get_trace_hydration_lock_singleton(self):
        """Test _get_trace_hydration_lock returns same lock instance"""
        lock1 = _get_trace_hydration_lock()
        lock2 = _get_trace_hydration_lock()
        
        assert lock1 is lock2


class TestRateLimitedLM:
    """Test RateLimitedLM wrapper class"""

    def test_rate_limited_lm_init(self):
        """Test RateLimitedLM initialization"""
        mock_base_lm = Mock()
        mock_semaphore = Mock()
        
        rate_limited_lm = RateLimitedLM(mock_base_lm, mock_semaphore)
        
        assert rate_limited_lm._base == mock_base_lm
        assert rate_limited_lm._sem == mock_semaphore

    def test_rate_limited_lm_getattr(self):
        """Test RateLimitedLM __getattr__ delegation"""
        mock_base_lm = Mock()
        mock_base_lm.test_attribute = "test_value"
        mock_semaphore = Mock()
        
        rate_limited_lm = RateLimitedLM(mock_base_lm, mock_semaphore)
        
        assert rate_limited_lm.test_attribute == "test_value"

    def test_rate_limited_lm_call_with_semaphore(self):
        """Test RateLimitedLM __call__ with semaphore"""
        mock_base_lm = Mock()
        mock_base_lm.return_value = "test_result"
        mock_semaphore = Mock()
        
        rate_limited_lm = RateLimitedLM(mock_base_lm, mock_semaphore)
        
        result = rate_limited_lm("test_arg", test_kwarg="test_value")
        
        assert result == "test_result"
        mock_semaphore.acquire.assert_called_once()
        mock_semaphore.release.assert_called_once()
        mock_base_lm.assert_called_once_with("test_arg", test_kwarg="test_value")
