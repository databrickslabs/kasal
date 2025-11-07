import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List, Optional
import json

# Test crew generation service - based on actual code inspection

from src.services.crew_generation_service import CrewGenerationService


class TestCrewGenerationServiceInit:
    """Test CrewGenerationService initialization"""

    def test_crew_generation_service_init_basic(self):
        """Test CrewGenerationService __init__ with basic parameters"""
        mock_session = Mock()
        
        with patch('src.services.crew_generation_service.LLMLogService') as mock_log_service:
            with patch('src.services.crew_generation_service.LLMLogRepository') as mock_log_repo:
                with patch('src.services.crew_generation_service.CrewGeneratorRepository') as mock_crew_repo:
                    mock_log_instance = Mock()
                    mock_log_repo_instance = Mock()
                    mock_crew_repo_instance = Mock()
                    
                    mock_log_service.return_value = mock_log_instance
                    mock_log_repo.return_value = mock_log_repo_instance
                    mock_crew_repo.return_value = mock_crew_repo_instance
                    
                    service = CrewGenerationService(mock_session)
                    
                    assert service.session == mock_session
                    assert service.log_service == mock_log_instance
                    assert service.tool_service is None  # Initialized when needed
                    assert service.crew_generator_repository == mock_crew_repo_instance

    def test_crew_generation_service_init_creates_repositories(self):
        """Test CrewGenerationService __init__ creates repository instances"""
        mock_session = Mock()
        
        with patch('src.services.crew_generation_service.LLMLogService') as mock_log_service:
            with patch('src.services.crew_generation_service.LLMLogRepository') as mock_log_repo:
                with patch('src.services.crew_generation_service.CrewGeneratorRepository') as mock_crew_repo:
                    service = CrewGenerationService(mock_session)
                    
                    # Verify repositories were created with session
                    mock_log_repo.assert_called_once_with(mock_session)
                    mock_crew_repo.assert_called_once_with(mock_session)
                    mock_log_service.assert_called_once()

    def test_crew_generation_service_init_tool_service_lazy(self):
        """Test CrewGenerationService __init__ tool_service is lazy initialized"""
        mock_session = Mock()
        
        with patch('src.services.crew_generation_service.LLMLogService'):
            with patch('src.services.crew_generation_service.LLMLogRepository'):
                with patch('src.services.crew_generation_service.CrewGeneratorRepository'):
                    service = CrewGenerationService(mock_session)
                    
                    # tool_service should be None initially
                    assert service.tool_service is None


class TestCrewGenerationServiceSafeGetAttr:
    """Test CrewGenerationService _safe_get_attr method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        with patch('src.services.crew_generation_service.LLMLogService'):
            with patch('src.services.crew_generation_service.LLMLogRepository'):
                with patch('src.services.crew_generation_service.CrewGeneratorRepository'):
                    self.service = CrewGenerationService(self.mock_session)

    def test_safe_get_attr_dict_existing_key(self):
        """Test _safe_get_attr with dictionary and existing key"""
        obj = {"name": "test_name", "value": 42}
        
        result = self.service._safe_get_attr(obj, "name")
        
        assert result == "test_name"

    def test_safe_get_attr_dict_missing_key(self):
        """Test _safe_get_attr with dictionary and missing key"""
        obj = {"name": "test_name"}
        
        result = self.service._safe_get_attr(obj, "missing_key")
        
        assert result is None

    def test_safe_get_attr_dict_missing_key_with_default(self):
        """Test _safe_get_attr with dictionary, missing key, and default value"""
        obj = {"name": "test_name"}
        default_value = "default"
        
        result = self.service._safe_get_attr(obj, "missing_key", default_value)
        
        assert result == default_value

    def test_safe_get_attr_object_existing_attr(self):
        """Test _safe_get_attr with object and existing attribute"""
        class TestObj:
            def __init__(self):
                self.name = "test_name"
                self.value = 42
        
        obj = TestObj()
        
        result = self.service._safe_get_attr(obj, "name")
        
        assert result == "test_name"

    def test_safe_get_attr_object_missing_attr(self):
        """Test _safe_get_attr with object and missing attribute"""
        class TestObj:
            def __init__(self):
                self.name = "test_name"
        
        obj = TestObj()
        
        result = self.service._safe_get_attr(obj, "missing_attr")
        
        assert result is None

    def test_safe_get_attr_object_missing_attr_with_default(self):
        """Test _safe_get_attr with object, missing attribute, and default value"""
        class TestObj:
            def __init__(self):
                self.name = "test_name"
        
        obj = TestObj()
        default_value = "default"
        
        result = self.service._safe_get_attr(obj, "missing_attr", default_value)
        
        assert result == default_value

    def test_safe_get_attr_none_object(self):
        """Test _safe_get_attr with None object"""
        result = self.service._safe_get_attr(None, "any_attr")
        
        assert result is None

    def test_safe_get_attr_none_object_with_default(self):
        """Test _safe_get_attr with None object and default value"""
        default_value = "default"
        
        result = self.service._safe_get_attr(None, "any_attr", default_value)
        
        assert result == default_value


class TestCrewGenerationServiceCreateToolNameToIdMap:
    """Test CrewGenerationService _create_tool_name_to_id_map method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        with patch('src.services.crew_generation_service.LLMLogService'):
            with patch('src.services.crew_generation_service.LLMLogRepository'):
                with patch('src.services.crew_generation_service.CrewGeneratorRepository'):
                    self.service = CrewGenerationService(self.mock_session)

    def test_create_tool_name_to_id_map_basic(self):
        """Test _create_tool_name_to_id_map with basic tools"""
        tools = [
            {"id": "tool1", "name": "Tool One"},
            {"id": "tool2", "name": "Tool Two"},
            {"id": "tool3", "name": "Tool Three"}
        ]
        
        result = self.service._create_tool_name_to_id_map(tools)
        
        expected = {
            "Tool One": "tool1",
            "Tool Two": "tool2", 
            "Tool Three": "tool3"
        }
        assert result == expected

    def test_create_tool_name_to_id_map_empty_list(self):
        """Test _create_tool_name_to_id_map with empty tools list"""
        tools = []
        
        result = self.service._create_tool_name_to_id_map(tools)
        
        assert result == {}

    def test_create_tool_name_to_id_map_missing_name(self):
        """Test _create_tool_name_to_id_map with tools missing name"""
        tools = [
            {"id": "tool1", "name": "Tool One"},
            {"id": "tool2"},  # Missing name
            {"id": "tool3", "name": "Tool Three"}
        ]
        
        result = self.service._create_tool_name_to_id_map(tools)
        
        # Should handle missing name gracefully
        expected = {
            "Tool One": "tool1",
            "Tool Three": "tool3"
        }
        assert result == expected

    def test_create_tool_name_to_id_map_missing_id(self):
        """Test _create_tool_name_to_id_map with tools missing id"""
        tools = [
            {"id": "tool1", "name": "Tool One"},
            {"name": "Tool Two"},  # Missing id
            {"id": "tool3", "name": "Tool Three"}
        ]
        
        result = self.service._create_tool_name_to_id_map(tools)
        
        # Should handle missing id gracefully
        expected = {
            "Tool One": "tool1",
            "Tool Three": "tool3"
        }
        assert result == expected

    def test_create_tool_name_to_id_map_duplicate_names(self):
        """Test _create_tool_name_to_id_map with duplicate tool names"""
        tools = [
            {"id": "tool1", "name": "Duplicate Name"},
            {"id": "tool2", "name": "Tool Two"},
            {"id": "tool3", "name": "Duplicate Name"}  # Duplicate name
        ]
        
        result = self.service._create_tool_name_to_id_map(tools)
        
        # Later tool should overwrite earlier one
        expected = {
            "Tool Two": "tool2",
            "Duplicate Name": "tool3"  # Last one wins
        }
        assert result == expected

    def test_create_tool_name_to_id_map_various_data_types(self):
        """Test _create_tool_name_to_id_map with various data types"""
        tools = [
            {"id": "tool1", "name": "String Name"},
            {"id": 2, "name": "Numeric ID"},  # Numeric ID
            {"id": "tool3", "name": 123}  # Numeric name
        ]

        result = self.service._create_tool_name_to_id_map(tools)

        # Should handle various data types, IDs are converted to strings
        expected = {
            "String Name": "tool1",
            "Numeric ID": "2",  # ID converted to string
            123: "tool3"
        }
        assert result == expected


class TestCrewGenerationServiceConstants:
    """Test CrewGenerationService constants and module-level attributes"""

    def test_logger_initialization(self):
        """Test logger is properly initialized"""
        from src.services.crew_generation_service import logger
        
        assert logger is not None
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')

    def test_required_imports(self):
        """Test that required imports are available"""
        from src.services.crew_generation_service import (
            json, logging, os, traceback, uuid, litellm
        )
        
        assert json is not None
        assert logging is not None
        assert os is not None
        assert traceback is not None
        assert uuid is not None
        assert litellm is not None

    def test_schema_imports(self):
        """Test schema imports"""
        from src.services.crew_generation_service import (
            CrewGenerationRequest, CrewGenerationResponse
        )
        
        assert CrewGenerationRequest is not None
        assert CrewGenerationResponse is not None

    def test_service_imports(self):
        """Test service imports"""
        from src.services.crew_generation_service import (
            TemplateService, ToolService, LLMLogService
        )
        
        assert TemplateService is not None
        assert ToolService is not None
        assert LLMLogService is not None

    def test_model_imports(self):
        """Test model imports"""
        from src.services.crew_generation_service import Agent, Task
        
        assert Agent is not None
        assert Task is not None

    def test_utils_imports(self):
        """Test utils imports"""
        from src.services.crew_generation_service import robust_json_parser, GroupContext
        
        assert robust_json_parser is not None
        assert GroupContext is not None


class TestCrewGenerationServiceAttributes:
    """Test CrewGenerationService attribute access and properties"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        with patch('src.services.crew_generation_service.LLMLogService'):
            with patch('src.services.crew_generation_service.LLMLogRepository'):
                with patch('src.services.crew_generation_service.CrewGeneratorRepository'):
                    self.service = CrewGenerationService(self.mock_session)

    def test_service_has_required_attributes(self):
        """Test that service has all required attributes after initialization"""
        # Check all required attributes exist
        assert hasattr(self.service, 'session')
        assert hasattr(self.service, 'log_service')
        assert hasattr(self.service, 'tool_service')
        assert hasattr(self.service, 'crew_generator_repository')
        
        # Check attribute values
        assert self.service.session == self.mock_session
        assert self.service.log_service is not None
        assert self.service.tool_service is None  # Lazy initialized
        assert self.service.crew_generator_repository is not None

    def test_service_session_storage(self):
        """Test service stores session correctly"""
        assert self.service.session == self.mock_session
        
        # Test with different session
        new_mock_session = Mock()
        with patch('src.services.crew_generation_service.LLMLogService'):
            with patch('src.services.crew_generation_service.LLMLogRepository'):
                with patch('src.services.crew_generation_service.CrewGeneratorRepository'):
                    new_service = CrewGenerationService(new_mock_session)
                    assert new_service.session == new_mock_session
                    assert new_service.session != self.mock_session

    def test_service_repositories_are_separate(self):
        """Test that repositories are separate instances"""
        assert self.service.log_service is not self.service.crew_generator_repository
        assert self.service.log_service is not None
        assert self.service.crew_generator_repository is not None


class TestCrewGenerationServiceAsyncMethods:
    """Test CrewGenerationService async method signatures"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        with patch('src.services.crew_generation_service.LLMLogService'):
            with patch('src.services.crew_generation_service.LLMLogRepository'):
                with patch('src.services.crew_generation_service.CrewGeneratorRepository'):
                    self.service = CrewGenerationService(self.mock_session)

    def test_async_methods_exist(self):
        """Test that key async methods exist and are callable"""
        async_methods = [
            '_log_llm_interaction',
            '_prepare_prompt_template',
            '_get_relevant_documentation',
            'create_crew_complete',
            '_get_tool_details'
        ]
        
        for method_name in async_methods:
            assert hasattr(self.service, method_name)
            method = getattr(self.service, method_name)
            assert callable(method)

    def test_sync_methods_exist(self):
        """Test that key sync methods exist and are callable"""
        sync_methods = [
            '_process_crew_setup',
            '_safe_get_attr',
            '_create_tool_name_to_id_map'
        ]
        
        for method_name in sync_methods:
            assert hasattr(self.service, method_name)
            method = getattr(self.service, method_name)
            assert callable(method)
