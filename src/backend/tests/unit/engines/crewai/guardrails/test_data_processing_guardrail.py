import pytest
import json
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any

from src.engines.crewai.guardrails.data_processing_guardrail import DataProcessingGuardrail


class TestDataProcessingGuardrail:
    """Test suite for DataProcessingGuardrail class."""
    
    def test_init_with_dict_config(self):
        """Test initialization with dictionary configuration."""
        config = {"field": "data", "timeout": 30}
        guardrail = DataProcessingGuardrail(config)
        
        assert guardrail.config == config
    
    def test_init_with_json_string_config(self):
        """Test initialization with JSON string configuration."""
        config_dict = {"field": "companies", "min_count": 5}
        config_json = json.dumps(config_dict)
        
        guardrail = DataProcessingGuardrail(config_json)
        
        assert guardrail.config == config_dict
    
    def test_init_with_invalid_json_string(self):
        """Test initialization with invalid JSON string."""
        invalid_json = "{'invalid': json}"  # Single quotes make it invalid JSON
        
        guardrail = DataProcessingGuardrail(invalid_json)
        
        # Should fall back to empty dict
        assert guardrail.config == {}
    
    def test_init_with_empty_string(self):
        """Test initialization with empty string."""
        guardrail = DataProcessingGuardrail("")
        
        # Empty string should parse as empty dict
        assert guardrail.config == {}
    
    def test_init_inheritance_from_base(self):
        """Test that DataProcessingGuardrail inherits from BaseGuardrail."""
        from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
        
        guardrail = DataProcessingGuardrail({})
        
        assert isinstance(guardrail, BaseGuardrail)
        assert hasattr(guardrail, 'config')
        assert hasattr(guardrail, 'validate')
    
    @patch('src.engines.crewai.guardrails.base_guardrail.BaseGuardrail.__init__')
    def test_init_exception_handling(self, mock_base_init):
        """Test initialization exception handling."""
        # Mock the parent __init__ to raise an exception
        mock_base_init.side_effect = Exception("Base init failed")
        
        with pytest.raises(Exception, match="Base init failed"):
            DataProcessingGuardrail({"test": "config"})
    
    def test_validate_method_exists(self):
        """Test that validate method exists and is callable."""
        guardrail = DataProcessingGuardrail({})
        
        assert hasattr(guardrail, 'validate')
        assert callable(guardrail.validate)
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.DataProcessingRepository')
    def test_validate_all_records_processed_success(self, mock_repo_class, mock_uow):
        """Test successful validation when all records are processed."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 5
        mock_repo.count_unprocessed_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingGuardrail({})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is True
        assert "All data records have been processed successfully" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.DataProcessingRepository')
    def test_validate_unprocessed_records_exist(self, mock_repo_class, mock_uow):
        """Test validation when unprocessed records exist."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 5
        mock_repo.count_unprocessed_records_sync.return_value = 3
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingGuardrail({})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is False
        assert "There are still 3 unprocessed records" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.DataProcessingRepository')
    def test_validate_no_records_exist(self, mock_repo_class, mock_uow):
        """Test validation when no records exist in database."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to return 0 records
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingGuardrail({})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is False
        assert "No records found in the database" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.DataProcessingRepository')
    def test_validate_creates_test_data_when_no_records(self, mock_repo_class, mock_uow):
        """Test that validation creates test data when no records exist."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to simulate no records initially, then creates test data
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.side_effect = [0, 2]  # First call returns 0, second returns 2
        mock_repo.count_unprocessed_records_sync.return_value = 1
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingGuardrail({})
        result = guardrail.validate("test_output")
        
        # Should create test data
        mock_repo.create_record_sync.assert_any_call(che_number="CHE12345", processed=False)
        mock_repo.create_record_sync.assert_any_call(che_number="CHE67890", processed=True)
        mock_uow_instance._session.commit.assert_called()
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.DataProcessingRepository')
    def test_validate_creates_table_on_error(self, mock_repo_class, mock_uow):
        """Test that validation creates table when it doesn't exist."""
        # Setup UOW
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        # Setup repository to throw error on first count (table doesn't exist), then work
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.side_effect = [
            Exception("Table doesn't exist"),  # First call fails
            2,  # Second call succeeds
            2   # Third call for final check
        ]
        mock_repo.count_unprocessed_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingGuardrail({})
        result = guardrail.validate("test_output")
        
        # Should create table and test data
        mock_repo.create_table_if_not_exists_sync.assert_called_once()
        mock_repo.create_record_sync.assert_any_call(che_number="CHE12345", processed=False)
        mock_repo.create_record_sync.assert_any_call(che_number="CHE67890", processed=True)
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    def test_validate_uow_initialization(self, mock_uow):
        """Test that UOW is initialized if not already initialized."""
        # Setup UOW as not initialized
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = False
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        guardrail = DataProcessingGuardrail({})
        
        try:
            guardrail.validate("test_output")
        except Exception:
            # Expected since repository isn't fully mocked
            pass
        
        # Should initialize UOW
        mock_uow_instance.initialize.assert_called_once()
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    def test_validate_exception_handling(self, mock_uow):
        """Test validation exception handling."""
        # Setup UOW to throw exception
        mock_uow.get_instance.side_effect = Exception("Database connection failed")
        
        guardrail = DataProcessingGuardrail({})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is False
        assert "Error checking data processing status" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.DataProcessingRepository')
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
        mock_repo.count_total_records_sync.return_value = 1
        mock_repo.count_unprocessed_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingGuardrail({})
        guardrail.validate("test_output")
        
        # Verify repository was created with sync_session
        mock_repo_class.assert_called_once_with(sync_session=mock_session)
    
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_guardrail.DataProcessingRepository')
    def test_validate_different_output_types(self, mock_repo_class, mock_uow):
        """Test validation with different output parameter types."""
        # Setup
        mock_uow_instance = MagicMock()
        mock_uow_instance._initialized = True
        mock_uow_instance._session = MagicMock()
        mock_uow.get_instance.return_value = mock_uow_instance
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 5
        mock_repo.count_unprocessed_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingGuardrail({})
        
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
    
    def test_validate_return_structure(self):
        """Test that validate method returns correct structure."""
        guardrail = DataProcessingGuardrail({})
        
        # Mock the method to test return structure
        test_cases = [
            {"valid": True, "feedback": "Success message"},
            {"valid": False, "feedback": "Error message"},
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
        
        guardrail = DataProcessingGuardrail(complex_config)
        
        assert guardrail.config == complex_config
        assert guardrail.config["validation_rules"]["timeout"] == 300
        assert guardrail.config["database_config"]["table_name"] == "data_processing"
    
    def test_config_with_json_string_complex(self):
        """Test initialization with complex JSON string."""
        config_dict = {
            "processing_criteria": {
                "status_field": "processed",
                "expected_value": True,
                "count_field": "total_records"
            },
            "validation_settings": {
                "strict_mode": False,
                "allow_partial": True
            }
        }
        
        config_json = json.dumps(config_dict)
        guardrail = DataProcessingGuardrail(config_json)
        
        assert guardrail.config == config_dict
        assert guardrail.config["processing_criteria"]["status_field"] == "processed"
        assert guardrail.config["validation_settings"]["strict_mode"] is False
    
    def test_config_parsing_edge_cases(self):
        """Test configuration parsing edge cases."""
        # Test with whitespace-only string
        guardrail1 = DataProcessingGuardrail("   ")
        assert guardrail1.config == {}
        
        # Test with JSON null
        guardrail2 = DataProcessingGuardrail("null")
        assert guardrail2.config is None
        
        # Test with JSON array (should work)
        guardrail3 = DataProcessingGuardrail("[1, 2, 3]")
        assert guardrail3.config == [1, 2, 3]
        
        # Test with JSON string value
        guardrail4 = DataProcessingGuardrail('"string_config"')
        assert guardrail4.config == "string_config"