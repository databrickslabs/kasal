import pytest
import json
import traceback
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any

from src.engines.crewai.guardrails.data_processing_count_guardrail import DataProcessingCountGuardrail
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail


class TestDataProcessingCountGuardrail:
    """Test suite for DataProcessingCountGuardrail class."""

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_dict_config_default_minimum_count(self, mock_logger):
        """Test initialization with dict config and default minimum_count."""
        config = {}
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.config == config
        assert guardrail.minimum_count == 0
        mock_logger.warning.assert_called_with("No minimum_count found in config, defaulting to 0")
        mock_logger.info.assert_called_with("DataProcessingCountGuardrail initialized with minimum_count: 0")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_dict_config_custom_minimum_count(self, mock_logger):
        """Test initialization with dict config and custom minimum_count."""
        config = {"minimum_count": 5}
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.config == config
        assert guardrail.minimum_count == 5
        mock_logger.info.assert_called_with("DataProcessingCountGuardrail initialized with minimum_count: 5")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_json_string_config(self, mock_logger):
        """Test initialization with JSON string config."""
        config = '{"minimum_count": 10}'
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.config == {"minimum_count": 10}
        assert guardrail.minimum_count == 10
        mock_logger.info.assert_called_with("DataProcessingCountGuardrail initialized with minimum_count: 10")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_invalid_json_string(self, mock_logger):
        """Test initialization with invalid JSON string."""
        config = '{"invalid": json}'
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.config == {}
        assert guardrail.minimum_count == 0
        mock_logger.error.assert_called_with(f"Failed to parse guardrail config: {config}")
        mock_logger.warning.assert_called_with("No minimum_count found in config, defaulting to 0")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_zero_minimum_count(self, mock_logger):
        """Test initialization with zero minimum_count."""
        config = {"minimum_count": 0}
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.minimum_count == 0
        mock_logger.info.assert_called_with("DataProcessingCountGuardrail initialized with minimum_count: 0")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_negative_minimum_count(self, mock_logger):
        """Test initialization with negative minimum_count."""
        config = {"minimum_count": -5}
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.minimum_count == -5
        mock_logger.info.assert_called_with("DataProcessingCountGuardrail initialized with minimum_count: -5")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_string_minimum_count(self, mock_logger):
        """Test initialization with string minimum_count that can be converted to int."""
        config = {"minimum_count": "15"}
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.minimum_count == 15
        mock_logger.info.assert_called_with("DataProcessingCountGuardrail initialized with minimum_count: 15")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_exception_handling(self, mock_logger):
        """Test initialization exception handling."""
        # Mock the parent class constructor to raise an exception
        with patch('src.engines.crewai.guardrails.base_guardrail.BaseGuardrail.__init__', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                DataProcessingCountGuardrail({})
            
            # Verify error logging
            assert mock_logger.error.called
            error_call = mock_logger.error.call_args[0][0]
            assert "Error initializing DataProcessingCountGuardrail" in error_call

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_inherits_from_base_guardrail(self, mock_logger):
        """Test that DataProcessingCountGuardrail inherits from BaseGuardrail."""
        guardrail = DataProcessingCountGuardrail({})
        
        assert isinstance(guardrail, BaseGuardrail)
        assert hasattr(guardrail, 'config')
        assert hasattr(guardrail, 'validate')

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_success_with_sufficient_records(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method with sufficient records."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 10
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is True
        assert "Success: The number of records in the data_processing table (10) meets or exceeds the minimum count (5)" in result["feedback"]
        mock_logger.info.assert_any_call("Validating data processing count against minimum count: 5")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_failure_with_insufficient_records(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method with insufficient records."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 3
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is False
        assert "Insufficient records: The number of records in the data_processing table (3) is below the minimum count required (5)" in result["feedback"]
        mock_logger.warning.assert_any_call("Validation failed: Actual count (3) is below minimum count (5)")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_with_uninitialized_uow(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method with uninitialized UnitOfWork."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = False
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 10
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is True
        mock_uow.initialize.assert_called_once()
        mock_logger.info.assert_any_call("Initialized UnitOfWork for data processing count check")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_with_table_creation(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method when table doesn't exist and needs to be created."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        # First call to count_total_records_sync raises exception (table doesn't exist)
        # Second call (after table creation) returns 2
        # Third call returns 2 again for final validation
        mock_repo.count_total_records_sync.side_effect = [Exception("Table doesn't exist"), 2, 2]
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 1})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is True
        mock_repo.create_table_if_not_exists_sync.assert_called_once()
        mock_repo.create_record_sync.assert_any_call(che_number="CHE12345", processed=False)
        mock_repo.create_record_sync.assert_any_call(che_number="CHE67890", processed=True)
        mock_uow._session.commit.assert_called_once()
        mock_logger.warning.assert_any_call("Error checking records, table may not exist: Table doesn't exist")
        mock_logger.info.assert_any_call("Created table and test records via repository")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_with_none_session(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method when session is None."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = None
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 5
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 3})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is True
        # Should skip the session check block and go directly to final count

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_general_exception_handling(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method general exception handling."""
        # Setup mocks to raise exception
        mock_uow_class.get_instance.side_effect = Exception("General error")
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is False
        assert "Error checking data processing count: General error" in result["feedback"]
        
        # Verify error logging
        error_call = mock_logger.error.call_args[0][0]
        assert "Error validating data processing count" in error_call

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_validate_method_exists(self, mock_logger):
        """Test that validate method exists and is callable."""
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        
        assert hasattr(guardrail, 'validate')
        assert callable(guardrail.validate)

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_equal_counts(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method when actual count equals minimum count."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 5
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is True
        assert "Success: The number of records in the data_processing table (5) meets or exceeds the minimum count (5)" in result["feedback"]

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_complex_config(self, mock_logger):
        """Test initialization with complex configuration."""
        config = {
            "minimum_count": 25,
            "other_param": "value",
            "nested": {
                "key": "value"
            },
            "list_param": [1, 2, 3]
        }
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.config == config
        assert guardrail.minimum_count == 25
        assert guardrail.config["other_param"] == "value"
        assert guardrail.config["nested"]["key"] == "value"

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_float_minimum_count(self, mock_logger):
        """Test initialization with float minimum_count."""
        config = {"minimum_count": 5.7}
        guardrail = DataProcessingCountGuardrail(config)
        
        assert guardrail.minimum_count == 5  # int() conversion truncates
        mock_logger.info.assert_called_with("DataProcessingCountGuardrail initialized with minimum_count: 5")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    def test_initialization_with_invalid_minimum_count_type(self, mock_logger):
        """Test initialization with invalid minimum_count type that cannot be converted to int."""
        config = {"minimum_count": "invalid"}
        
        with pytest.raises(ValueError):
            DataProcessingCountGuardrail(config)

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_with_zero_actual_count(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method with zero actual count."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 1})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is False
        assert "Insufficient records: The number of records in the data_processing table (0) is below the minimum count required (1)" in result["feedback"]

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_with_negative_minimum_count_validation(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test validate method with negative minimum count."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 0
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": -1})
        result = guardrail.validate("test_output")
        
        # Even with 0 records, it should pass because minimum is -1
        assert result["valid"] is True
        assert "Success: The number of records in the data_processing table (0) meets or exceeds the minimum count (-1)" in result["feedback"]

    def test_module_imports(self):
        """Test that all required modules can be imported."""
        from src.engines.crewai.guardrails.data_processing_count_guardrail import DataProcessingCountGuardrail
        from src.engines.crewai.guardrails.data_processing_count_guardrail import logger
        
        assert DataProcessingCountGuardrail is not None
        assert logger is not None

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.traceback')
    def test_initialization_exception_traceback_capture(self, mock_traceback, mock_logger):
        """Test that initialization exception properly captures traceback."""
        mock_traceback.format_exc.return_value = "Test traceback"
        
        # Mock int() to raise an exception during minimum_count conversion
        with patch('builtins.int', side_effect=ValueError("Cannot convert")):
            config = {"minimum_count": "invalid"}
            with pytest.raises(ValueError):
                DataProcessingCountGuardrail(config)
            
            # Verify traceback was captured
            mock_traceback.format_exc.assert_called()

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.traceback')
    def test_validate_exception_traceback_capture(self, mock_traceback, mock_repo_class, mock_uow_class, mock_logger):
        """Test that validate exception properly captures traceback."""
        mock_traceback.format_exc.return_value = "Test validation traceback"
        
        # Setup mocks to raise exception
        mock_uow_class.get_instance.side_effect = RuntimeError("Test runtime error")
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        result = guardrail.validate("test_output")
        
        assert result["valid"] is False
        mock_traceback.format_exc.assert_called()
        
        # Verify error logging includes detailed error info
        error_call = mock_logger.error.call_args[0][0]
        assert "Error validating data processing count" in error_call

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.SyncUnitOfWork')
    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.DataProcessingRepository')
    def test_validate_logs_all_steps(self, mock_repo_class, mock_uow_class, mock_logger):
        """Test that validate method logs all important steps."""
        # Setup mocks
        mock_uow = MagicMock()
        mock_uow._initialized = True
        mock_uow._session = MagicMock()
        mock_uow_class.get_instance.return_value = mock_uow
        
        mock_repo = MagicMock()
        mock_repo.count_total_records_sync.return_value = 10
        mock_repo_class.return_value = mock_repo
        
        guardrail = DataProcessingCountGuardrail({"minimum_count": 5})
        result = guardrail.validate("test_output")
        
        # Verify all expected log calls are made
        mock_logger.info.assert_any_call("Validating data processing count against minimum count: 5")
        mock_logger.info.assert_any_call(f"Got UnitOfWork instance: {mock_uow}")
        mock_logger.info.assert_any_call(f"Created DataProcessingRepository with sync_session: {mock_repo}")
        mock_logger.info.assert_any_call("Found 10 total records in data_processing table")
        mock_logger.info.assert_any_call("Validation passed: Actual count (10) meets or exceeds minimum count (5)")

    @patch('src.engines.crewai.guardrails.data_processing_count_guardrail.logger')  
    def test_validate_method_signature(self, mock_logger):
        """Test that validate method has correct signature and return type."""
        guardrail = DataProcessingCountGuardrail({})
        
        # Test method signature by calling with different parameter types
        with patch.object(guardrail, 'validate', return_value={"valid": True, "feedback": ""}) as mock_validate:
            result = guardrail.validate("string_output")
            assert isinstance(result, dict)
            assert "valid" in result
            assert "feedback" in result
            mock_validate.assert_called_with("string_output")
            
            result = guardrail.validate(123)
            mock_validate.assert_called_with(123)
            
            result = guardrail.validate(None)
            mock_validate.assert_called_with(None)