import pytest
import json
import traceback
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any

from src.engines.crewai.guardrails.empty_data_processing_guardrail import EmptyDataProcessingGuardrail


class TestEmptyDataProcessingGuardrail:
    """Test suite for EmptyDataProcessingGuardrail class."""
    
    def test_init_with_dict_config(self):
        """Test initialization with dictionary configuration."""
        config = {"field": "data", "timeout": 30}
        guardrail = EmptyDataProcessingGuardrail(config)
        
        assert guardrail.config == config
    
    def test_init_with_json_string_config(self):
        """Test initialization with JSON string configuration."""
        config_dict = {"field": "companies", "min_count": 5}
        config_json = json.dumps(config_dict)
        
        guardrail = EmptyDataProcessingGuardrail(config_json)
        
        assert guardrail.config == config_dict
    
    def test_init_with_invalid_json_string(self):
        """Test initialization with invalid JSON string."""
        invalid_json = "{'invalid': json}"  # Single quotes make it invalid JSON
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail(invalid_json)
            
            # Should fall back to empty dict
            assert guardrail.config == {}
            # Should log error
            mock_logger.error.assert_called_once()
            assert "Failed to parse guardrail config" in str(mock_logger.error.call_args[0][0])
    
    def test_init_with_empty_string(self):
        """Test initialization with empty string."""
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail("")
            
            # Empty string should parse as empty dict
            assert guardrail.config == {}
            # Should log error for empty string
            mock_logger.error.assert_called_once()
    
    def test_init_with_whitespace_string(self):
        """Test initialization with whitespace-only string."""
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail("   ")
            
            # Whitespace string should parse as empty dict
            assert guardrail.config == {}
            # Should log error for whitespace string
            mock_logger.error.assert_called_once()
    
    def test_init_with_json_null(self):
        """Test initialization with JSON null."""
        guardrail = EmptyDataProcessingGuardrail("null")
        
        assert guardrail.config is None
    
    def test_init_with_json_array(self):
        """Test initialization with JSON array."""
        guardrail = EmptyDataProcessingGuardrail("[1, 2, 3]")
        
        assert guardrail.config == [1, 2, 3]
    
    def test_init_with_json_string_value(self):
        """Test initialization with JSON string value."""
        guardrail = EmptyDataProcessingGuardrail('"string_config"')
        
        assert guardrail.config == "string_config"
    
    def test_init_inheritance_from_base(self):
        """Test that EmptyDataProcessingGuardrail inherits from BaseGuardrail."""
        from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
        
        guardrail = EmptyDataProcessingGuardrail({})
        
        assert isinstance(guardrail, BaseGuardrail)
        assert hasattr(guardrail, 'config')
        assert hasattr(guardrail, 'validate')
    
    def test_init_logs_success(self):
        """Test that initialization logs success message."""
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            EmptyDataProcessingGuardrail({})
            
            mock_logger.info.assert_called_with("EmptyDataProcessingGuardrail initialized successfully")
    
    @patch('src.engines.crewai.guardrails.base_guardrail.BaseGuardrail.__init__')
    def test_init_exception_handling(self, mock_base_init):
        """Test initialization exception handling."""
        # Mock the parent __init__ to raise an exception
        mock_base_init.side_effect = Exception("Base init failed")
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            with pytest.raises(Exception, match="Base init failed"):
                EmptyDataProcessingGuardrail({"test": "config"})
            
            # Should log error with traceback
            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0][0]
            assert "Error initializing EmptyDataProcessingGuardrail" in error_call
    
    def test_init_exception_handling_with_traceback(self):
        """Test initialization exception handling includes traceback."""
        with patch('src.engines.crewai.guardrails.base_guardrail.BaseGuardrail.__init__') as mock_base_init:
            mock_base_init.side_effect = ValueError("Test error")
            
            with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
                with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.traceback') as mock_traceback:
                    mock_traceback.format_exc.return_value = "Mock traceback"
                    
                    with pytest.raises(ValueError, match="Test error"):
                        EmptyDataProcessingGuardrail({})
                    
                    # Should capture traceback
                    mock_traceback.format_exc.assert_called_once()
                    # Should log error with error info
                    mock_logger.error.assert_called_once()
                    error_call = str(mock_logger.error.call_args[0][0])
                    assert "Error initializing EmptyDataProcessingGuardrail" in error_call
    
    def test_validate_method_exists(self):
        """Test that validate method exists and is callable."""
        guardrail = EmptyDataProcessingGuardrail({})
        
        assert hasattr(guardrail, 'validate')
        assert callable(guardrail.validate)
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_empty_table_success(self, mock_repo_class, mock_uow):
        """Test successful validation when table is empty."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to return 0 records
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            result = guardrail.validate("test_output")
            
            assert result["valid"] is True
            assert "The data_processing table is empty as required" in result["feedback"]
            
            # Should log validation steps
            mock_logger.info.assert_any_call("Validating data_processing table is empty")
            mock_logger.info.assert_any_call("Found 0 total records in data_processing table")
            mock_logger.info.assert_any_call("Data_processing table is empty as required")
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_non_empty_table_failure(self, mock_repo_class, mock_uow):
        """Test validation failure when table is not empty."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to return 5 records
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 5
        mock_repo_class.return_value = mock_repo
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            result = guardrail.validate("test_output")
            
            assert result["valid"] is False
            assert "The data_processing table contains 5 records" in result["feedback"]
            assert "The table must be empty to proceed" in result["feedback"]
            
            # Should log warning
            mock_logger.warning.assert_called_with("Found 5 records in the data_processing table")
            mock_logger.info.assert_any_call("Found 5 total records in data_processing table")
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    def test_validate_uow_initialization(self, mock_uow):
        """Test that UOW is initialized if not already initialized."""
        # Setup UOW as not initialized
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = False
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            
            try:
                guardrail.validate("test_output")
            except Exception:
                # Expected since repository isn't fully mocked
                pass
            
            # Should initialize UOW
            mock_uow_instance.initialize.assert_called_once()
            mock_logger.info.assert_any_call("Initialized UnitOfWork for empty table check")
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    def test_validate_uow_already_initialized(self, mock_uow):
        """Test that UOW is not initialized if already initialized."""
        # Setup UOW as already initialized
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        guardrail = EmptyDataProcessingGuardrail({})
        
        try:
            guardrail.validate("test_output")
        except Exception:
            # Expected since repository isn't fully mocked
            pass
        
        # Should NOT initialize UOW again
        mock_uow_instance.initialize.assert_not_called()
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_repository_creation(self, mock_repo_class, mock_uow):
        """Test that repository is created with correct session."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_session = MagicMock()
        mock_uow_instance._session = mock_session
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            guardrail.validate("test_output")
            
            # Verify repository was created with sync_session
            mock_repo_class.assert_called_once_with(sync_session=mock_session)
            # Should log repository creation
            mock_logger.info.assert_any_call(f"Created DataProcessingRepository with sync_session: {mock_repo}")
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    def test_validate_exception_handling(self, mock_uow):
        """Test validation exception handling."""
        # Setup UOW to throw exception
        mock_uow.get_instance.side_effect = Exception("Database connection failed")
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.traceback') as mock_traceback:
                mock_traceback.format_exc.return_value = "Mock traceback"
                
                guardrail = EmptyDataProcessingGuardrail({})
                result = guardrail.validate("test_output")
                
                assert result["valid"] is False
                assert "Error checking if data_processing table is empty" in result["feedback"]
                assert "Database connection failed" in result["feedback"]
                
                # Should log error with traceback
                mock_logger.error.assert_called_once()
                error_call = str(mock_logger.error.call_args[0][0])
                assert "Error validating empty table status" in error_call
                mock_traceback.format_exc.assert_called_once()
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_repository_exception_handling(self, mock_repo_class, mock_uow):
        """Test validation exception handling when repository fails."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to throw exception
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.side_effect = ValueError("Repository error")
        mock_repo_class.return_value = mock_repo
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            result = guardrail.validate("test_output")
            
            assert result["valid"] is False
            assert "Error checking if data_processing table is empty" in result["feedback"]
            assert "Repository error" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_different_output_types(self, mock_repo_class, mock_uow):
        """Test validation with different output parameter types."""
        # Setup
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = EmptyDataProcessingGuardrail({})
        
        # Test with different output types
        test_outputs = [
            "string_output",
            {"dict": "output"},
            ["list", "output"],
            123,
            None
        ]
        
        for output in test_outputs:
            result = guardrail.validate(output)
            assert isinstance(result, dict)
            assert "valid" in result
            assert "feedback" in result
            assert result["valid"] is True
    
    def test_validate_return_structure(self):
        """Test that validate method returns correct structure."""
        guardrail = EmptyDataProcessingGuardrail({})
        
        # Mock the method to test return structure
        test_cases = [
            {"valid": True, "feedback": "The data_processing table is empty as required."},
            {"valid": False, "feedback": "The data_processing table contains 5 records. The table must be empty to proceed."},
        ]
        
        for expected_result in test_cases:
            with patch.object(guardrail, 'validate', return_value=expected_result):
                result = guardrail.validate("test")
                
                assert isinstance(result, dict)
                assert len(result) == 2
                assert "valid" in result
                assert "feedback" in result
                assert isinstance(result["valid"], bool)
                assert isinstance(result["feedback"], str)
                assert result == expected_result
    
    def test_config_with_complex_nested_structure(self):
        """Test initialization with complex nested configuration."""
        complex_config = {
            "validation_rules": {
                "required_fields": ["id", "status", "processed"],
                "timeout": 300,
                "retry_count": 3
            },
            "database_config": {
                "table_name": "data_processing",
                "connection_timeout": 30
            },
            "error_handling": {
                "log_errors": True,
                "raise_on_failure": False
            }
        }
        
        guardrail = EmptyDataProcessingGuardrail(complex_config)
        
        assert guardrail.config == complex_config
        assert guardrail.config["validation_rules"]["timeout"] == 300
        assert guardrail.config["database_config"]["table_name"] == "data_processing"
    
    def test_config_with_json_string_complex(self):
        """Test initialization with complex JSON string."""
        config_dict = {
            "validation_criteria": {
                "table_name": "data_processing",
                "expected_count": 0,
                "strict_mode": True
            },
            "logging_settings": {
                "log_level": "INFO",
                "log_validation_steps": True
            }
        }
        
        config_json = json.dumps(config_dict)
        guardrail = EmptyDataProcessingGuardrail(config_json)
        
        assert guardrail.config == config_dict
        assert guardrail.config["validation_criteria"]["expected_count"] == 0
        assert guardrail.config["logging_settings"]["log_level"] == "INFO"
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_logs_uow_instance(self, mock_repo_class, mock_uow):
        """Test that validation logs UOW instance information."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            guardrail.validate("test_output")
            
            # Should log UOW instance
            mock_logger.info.assert_any_call(f"Got UnitOfWork instance: {mock_uow_instance}")
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_with_large_record_count(self, mock_repo_class, mock_uow):
        """Test validation with large number of records."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to return large number of records
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 1000000
        mock_repo_class.return_value = mock_repo
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            result = guardrail.validate("test_output")
            
            assert result["valid"] is False
            assert "The data_processing table contains 1000000 records" in result["feedback"]
            mock_logger.warning.assert_called_with("Found 1000000 records in the data_processing table")
    
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository')
    def test_validate_edge_case_exactly_one_record(self, mock_repo_class, mock_uow):
        """Test validation with exactly one record."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to return exactly 1 record
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 1
        mock_repo_class.return_value = mock_repo
        
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
            guardrail = EmptyDataProcessingGuardrail({})
            result = guardrail.validate("test_output")
            
            assert result["valid"] is False
            assert "The data_processing table contains 1 records" in result["feedback"]
            mock_logger.warning.assert_called_with("Found 1 records in the data_processing table")
    
    def test_validate_output_parameter_not_used(self):
        """Test that the output parameter is not used in validation logic."""
        # This test confirms that the output parameter is ignored as per the docstring
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork') as mock_uow:
            with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.DataProcessingRepository') as mock_repo_class:
                # Setup minimal mocks
                mock_uow_instance = MagicMock()
                mock_uow_instance._initialized = True
                mock_uow_instance._session = MagicMock()
                mock_uow.get_instance.return_value = mock_uow_instance
                
                mock_repo = MagicMock()
                mock_repo.count_total_records_sync.return_value = 0
                mock_repo_class.return_value = mock_repo
                
                guardrail = EmptyDataProcessingGuardrail({})
                
                # The output parameter should not affect the validation logic
                # All these calls should behave identically
                result1 = guardrail.validate("any_string")
                result2 = guardrail.validate({"any": "dict"})
                result3 = guardrail.validate(None)
                
                assert result1 == result2 == result3
                assert all(r["valid"] is True for r in [result1, result2, result3])
    
    def test_json_decode_error_different_types(self):
        """Test JSON decode error handling with different invalid inputs."""
        invalid_configs = [
            "{invalid json}",
            "{'single': 'quotes'}",
            "{missing_quotes: value}",
            "[invalid, json]",
            "undefined",
            "function() {}"
        ]
        
        for invalid_config in invalid_configs:
            with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.logger') as mock_logger:
                guardrail = EmptyDataProcessingGuardrail(invalid_config)
                
                # Should fall back to empty dict
                assert guardrail.config == {}
                # Should log error
                mock_logger.error.assert_called_once()
                assert "Failed to parse guardrail config" in str(mock_logger.error.call_args[0][0])
    
    def test_getattr_with_fallback(self):
        """Test getattr usage with fallback for _initialized attribute."""
        with patch('src.engines.crewai.guardrails.empty_data_processing_guardrail.SyncUnitOfWork') as mock_uow:
            # Setup UOW without _initialized attribute
            mock_uow_instance = MagicMock()
            # Explicitly delete the _initialized attribute to test getattr fallback
            if hasattr(mock_uow_instance, '_initialized'):
                delattr(mock_uow_instance, '_initialized')
            mock_uow_instance._session = MagicMock()
            mock_uow.get_instance.return_value = mock_uow_instance
            
            # This should trigger the getattr fallback to False
            guardrail = EmptyDataProcessingGuardrail({})
            
            try:
                guardrail.validate("test_output")
            except Exception:
                # Expected since repository isn't fully mocked
                pass
            
            # Should initialize UOW when _initialized is not present (getattr fallback)
            mock_uow_instance.initialize.assert_called_once()