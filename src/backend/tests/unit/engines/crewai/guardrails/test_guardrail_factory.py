import pytest
import json
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any, Optional

from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.engines.crewai.guardrails.company_count_guardrail import CompanyCountGuardrail
from src.engines.crewai.guardrails.data_processing_guardrail import DataProcessingGuardrail
from src.engines.crewai.guardrails.empty_data_processing_guardrail import EmptyDataProcessingGuardrail
from src.engines.crewai.guardrails.data_processing_count_guardrail import DataProcessingCountGuardrail
from src.engines.crewai.guardrails.company_name_not_null_guardrail import CompanyNameNotNullGuardrail
from src.engines.crewai.guardrails.minimum_number_guardrail import MinimumNumberGuardrail


class TestGuardrailFactory:
    """Test suite for GuardrailFactory class."""
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_with_valid_json_string(self, mock_logger):
        """Test creating guardrail with valid JSON string configuration."""
        config_str = '{"type": "company_count", "min_companies": 10}'
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.CompanyCountGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config_str)
            
            assert result == mock_instance
            # The factory passes the parsed config_data, not the original config string
            expected_config = {"type": "company_count", "min_companies": 10}
            mock_guardrail.assert_called_once_with(expected_config)
            mock_logger.info.assert_called_with("Creating guardrail of type: company_count")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_with_invalid_json_string(self, mock_logger):
        """Test creating guardrail with invalid JSON string configuration."""
        config_str = '{"type": "company_count", invalid json}'
        
        result = GuardrailFactory.create_guardrail(config_str)
        
        assert result is None
        mock_logger.error.assert_called_with(f"Failed to parse guardrail config: {config_str}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_with_dict_config(self, mock_logger):
        """Test creating guardrail with dictionary configuration."""
        config_dict = {"type": "data_processing", "field": "companies"}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.DataProcessingGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config_dict)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config_dict)
            mock_logger.info.assert_any_call("Creating guardrail of type: data_processing")
            mock_logger.info.assert_any_call("Creating DataProcessingGuardrail...")
            mock_logger.info.assert_any_call(f"Successfully created DataProcessingGuardrail: {mock_instance}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_missing_type(self, mock_logger):
        """Test creating guardrail with missing type in configuration."""
        config = {"field": "companies", "min_count": 5}
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("No guardrail type specified in config")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_empty_type(self, mock_logger):
        """Test creating guardrail with empty type in configuration."""
        config = {"type": "", "field": "companies"}
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("No guardrail type specified in config")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_none_type(self, mock_logger):
        """Test creating guardrail with None type in configuration."""
        config = {"type": None, "field": "companies"}
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("No guardrail type specified in config")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_company_count_guardrail(self, mock_logger):
        """Test creating company_count guardrail."""
        config = {"type": "company_count", "min_companies": 20}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.CompanyCountGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config)
            mock_logger.info.assert_called_with("Creating guardrail of type: company_count")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_data_processing_guardrail(self, mock_logger):
        """Test creating data_processing guardrail."""
        config = {"type": "data_processing", "field": "data"}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.DataProcessingGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config)
            mock_logger.info.assert_any_call("Creating guardrail of type: data_processing")
            mock_logger.info.assert_any_call("Creating DataProcessingGuardrail...")
            mock_logger.info.assert_any_call(f"Successfully created DataProcessingGuardrail: {mock_instance}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_empty_data_processing_guardrail(self, mock_logger):
        """Test creating empty_data_processing guardrail."""
        config = {"type": "empty_data_processing", "field": "data"}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.EmptyDataProcessingGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config)
            mock_logger.info.assert_any_call("Creating guardrail of type: empty_data_processing")
            mock_logger.info.assert_any_call("Creating EmptyDataProcessingGuardrail...")
            mock_logger.info.assert_any_call(f"Successfully created EmptyDataProcessingGuardrail: {mock_instance}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_data_processing_count_guardrail(self, mock_logger):
        """Test creating data_processing_count guardrail."""
        config = {"type": "data_processing_count", "min_count": 15}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.DataProcessingCountGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config)
            mock_logger.info.assert_any_call("Creating guardrail of type: data_processing_count")
            mock_logger.info.assert_any_call("Creating DataProcessingCountGuardrail...")
            mock_logger.info.assert_any_call(f"Successfully created DataProcessingCountGuardrail: {mock_instance}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_company_name_not_null_guardrail(self, mock_logger):
        """Test creating company_name_not_null guardrail."""
        config = {"type": "company_name_not_null", "field": "company_name"}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.CompanyNameNotNullGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config)
            mock_logger.info.assert_any_call("Creating guardrail of type: company_name_not_null")
            mock_logger.info.assert_any_call("Creating CompanyNameNotNullGuardrail...")
            mock_logger.info.assert_any_call(f"Successfully created CompanyNameNotNullGuardrail: {mock_instance}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_minimum_number_guardrail(self, mock_logger):
        """Test creating minimum_number guardrail."""
        config = {"type": "minimum_number", "minimum": 5}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.MinimumNumberGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config)
            mock_logger.info.assert_any_call("Creating guardrail of type: minimum_number")
            mock_logger.info.assert_any_call("Creating MinimumNumberGuardrail...")
            mock_logger.info.assert_any_call(f"Successfully created MinimumNumberGuardrail: {mock_instance}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_unknown_type(self, mock_logger):
        """Test creating guardrail with unknown type."""
        config = {"type": "unknown_guardrail_type", "param": "value"}
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("Unknown guardrail type: unknown_guardrail_type")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_exception_during_creation(self, mock_logger):
        """Test exception handling during guardrail creation."""
        config = {"type": "company_count", "min_companies": 10}
        exception = Exception("Test exception")
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.CompanyCountGuardrail') as mock_guardrail:
            mock_guardrail.side_effect = exception
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result is None
            mock_logger.error.assert_any_call("Error creating guardrail of type company_count: Test exception")
            # The second error call should be the traceback
            assert mock_logger.error.call_count == 2
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_returns_none(self, mock_logger):
        """Test handling when guardrail creation returns None."""
        config = {"type": "company_count", "min_companies": 10}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.CompanyCountGuardrail') as mock_guardrail:
            mock_guardrail.return_value = None
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result is None
            mock_logger.error.assert_called_with("Failed to create guardrail of type company_count - returned None")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    @patch('src.engines.crewai.guardrails.guardrail_factory.traceback')
    def test_create_guardrail_traceback_logged(self, mock_traceback, mock_logger):
        """Test that traceback is logged when exception occurs."""
        config = {"type": "data_processing", "field": "test"}
        exception = ValueError("Invalid configuration")
        mock_traceback.format_exc.return_value = "Traceback details here"
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.DataProcessingGuardrail') as mock_guardrail:
            mock_guardrail.side_effect = exception
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result is None
            mock_logger.error.assert_any_call("Error creating guardrail of type data_processing: Invalid configuration")
            mock_traceback.format_exc.assert_called_once()
            mock_logger.error.assert_any_call("Traceback details here")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_empty_config_dict(self, mock_logger):
        """Test creating guardrail with empty configuration dictionary."""
        config = {}
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("No guardrail type specified in config")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_empty_json_string(self, mock_logger):
        """Test creating guardrail with empty JSON string."""
        config_str = "{}"
        
        result = GuardrailFactory.create_guardrail(config_str)
        
        assert result is None
        mock_logger.error.assert_called_with("No guardrail type specified in config")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_malformed_json(self, mock_logger):
        """Test creating guardrail with malformed JSON."""
        config_str = '{"type": "company_count", "min_companies": }'
        
        result = GuardrailFactory.create_guardrail(config_str)
        
        assert result is None
        mock_logger.error.assert_called_with(f"Failed to parse guardrail config: {config_str}")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_with_complex_config(self, mock_logger):
        """Test creating guardrail with complex configuration."""
        config = {
            "type": "data_processing",
            "field": "companies",
            "validation_rules": {
                "min_length": 5,
                "max_length": 100
            },
            "callbacks": [
                {"name": "validate_format", "params": {"format": "json"}},
                {"name": "check_duplicates", "params": {"tolerance": 0.9}}
            ]
        }
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.DataProcessingGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config)
            
            assert result == mock_instance
            mock_guardrail.assert_called_once_with(config)
            mock_logger.info.assert_any_call("Creating guardrail of type: data_processing")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_json_with_unicode(self, mock_logger):
        """Test creating guardrail with JSON containing unicode characters."""
        config_str = '{"type": "company_count", "description": "Test with unicode: ñáéíóú"}'
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.CompanyCountGuardrail') as mock_guardrail:
            mock_instance = MagicMock()
            mock_guardrail.return_value = mock_instance
            
            result = GuardrailFactory.create_guardrail(config_str)
            
            assert result == mock_instance
            expected_config = {"type": "company_count", "description": "Test with unicode: ñáéíóú"}
            mock_guardrail.assert_called_once_with(expected_config)
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_case_sensitive_type(self, mock_logger):
        """Test that guardrail type matching is case sensitive."""
        config = {"type": "Company_Count", "min_companies": 10}  # Wrong case
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("Unknown guardrail type: Company_Count")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_create_guardrail_with_whitespace_type(self, mock_logger):
        """Test creating guardrail with type containing whitespace."""
        config = {"type": " company_count ", "min_companies": 10}
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("Unknown guardrail type:  company_count ")
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_factory_is_static_method(self, mock_logger):
        """Test that create_guardrail is a static method."""
        # Should be able to call without instantiating the class
        config = {"type": "unknown_type"}
        
        result = GuardrailFactory.create_guardrail(config)
        
        assert result is None
        mock_logger.error.assert_called_with("Unknown guardrail type: unknown_type")
    
    def test_factory_does_not_require_instantiation(self):
        """Test that GuardrailFactory doesn't need to be instantiated."""
        # This test ensures the factory pattern is properly implemented
        # We should be able to call the method directly on the class
        config = {"type": "unknown_type"}
        
        with patch('src.engines.crewai.guardrails.guardrail_factory.logger'):
            result = GuardrailFactory.create_guardrail(config)
            assert result is None
    
    @patch('src.engines.crewai.guardrails.guardrail_factory.logger')
    def test_all_guardrail_types_covered(self, mock_logger):
        """Test that all guardrail types mentioned in imports are handled."""
        guardrail_types = [
            "company_count",
            "data_processing", 
            "empty_data_processing",
            "data_processing_count",
            "company_name_not_null",
            "minimum_number"
        ]
        
        patches = []
        mock_instances = []
        
        # Create patches for all guardrail classes
        patches.append(patch('src.engines.crewai.guardrails.guardrail_factory.CompanyCountGuardrail'))
        patches.append(patch('src.engines.crewai.guardrails.guardrail_factory.DataProcessingGuardrail'))
        patches.append(patch('src.engines.crewai.guardrails.guardrail_factory.EmptyDataProcessingGuardrail'))
        patches.append(patch('src.engines.crewai.guardrails.guardrail_factory.DataProcessingCountGuardrail'))
        patches.append(patch('src.engines.crewai.guardrails.guardrail_factory.CompanyNameNotNullGuardrail'))
        patches.append(patch('src.engines.crewai.guardrails.guardrail_factory.MinimumNumberGuardrail'))
        
        # Start all patches
        mocks = [p.start() for p in patches]
        
        try:
            # Create mock instances for each
            for mock in mocks:
                instance = MagicMock()
                mock.return_value = instance
                mock_instances.append(instance)
            
            # Test each guardrail type
            for i, guardrail_type in enumerate(guardrail_types):
                config = {"type": guardrail_type, "test": "param"}
                
                result = GuardrailFactory.create_guardrail(config)
                
                assert result == mock_instances[i]
                mocks[i].assert_called_with(config)
        
        finally:
            # Stop all patches
            for p in patches:
                p.stop()