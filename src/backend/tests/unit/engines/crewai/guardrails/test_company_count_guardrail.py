import pytest
import json
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any
from crewai.tasks.task_output import TaskOutput

from src.engines.crewai.guardrails.company_count_guardrail import CompanyCountGuardrail


class TestCompanyCountGuardrail:
    """Test suite for CompanyCountGuardrail class."""
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger_manager')
    def test_guardrail_initialization_default_config(self, mock_logger_manager):
        """Test guardrail initialization with default configuration."""
        mock_logger = MagicMock()
        mock_logger_manager._initialized = True
        mock_logger_manager.guardrails = mock_logger
        mock_logger_manager._log_dir = "/test/log/dir"
        mock_logger.handlers = ["handler1", "handler2"]
        
        config = {}
        guardrail = CompanyCountGuardrail(config)
        
        assert guardrail.config == config
        assert guardrail.min_companies == 50  # Default value
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger_manager')
    def test_guardrail_initialization_custom_config(self, mock_logger_manager):
        """Test guardrail initialization with custom configuration."""
        mock_logger = MagicMock()
        mock_logger_manager._initialized = True
        mock_logger_manager.guardrails = mock_logger
        mock_logger_manager._log_dir = "/test/log/dir"
        mock_logger.handlers = ["handler1", "handler2"]
        
        config = {"min_companies": 25, "other_param": "value"}
        guardrail = CompanyCountGuardrail(config)
        
        assert guardrail.config == config
        assert guardrail.min_companies == 25
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger_manager')
    def test_guardrail_initialization_zero_min_companies(self, mock_logger_manager):
        """Test guardrail initialization with zero min_companies."""
        mock_logger = MagicMock()
        mock_logger_manager._initialized = True
        mock_logger_manager.guardrails = mock_logger
        mock_logger_manager._log_dir = "/test/log/dir"
        mock_logger.handlers = ["handler1", "handler2"]
        
        config = {"min_companies": 0}
        guardrail = CompanyCountGuardrail(config)
        
        assert guardrail.min_companies == 0
    
    def test_module_level_logger_initialization(self):
        """Test that logger manager initialization is called at module level."""
        # This test verifies that the module-level initialization check exists
        # The actual initialization happens at import time, not during class creation
        from src.engines.crewai.guardrails import company_count_guardrail
        
        # Verify the logger_manager exists and has the expected structure
        assert hasattr(company_count_guardrail, 'logger_manager')
        assert hasattr(company_count_guardrail, 'logger')
        
        # Create an instance to ensure the class works correctly
        config = {"min_companies": 10}
        guardrail = CompanyCountGuardrail(config)
        assert guardrail.min_companies == 10
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_method_exists(self, mock_logger):
        """Test that validate method exists and is callable."""
        guardrail = CompanyCountGuardrail({"min_companies": 5})
        
        assert hasattr(guardrail, 'validate')
        assert callable(guardrail.validate)
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_with_empty_output(self, mock_logger):
        """Test validate method with empty output."""
        guardrail = CompanyCountGuardrail({"min_companies": 2})
        
        result = guardrail.validate("")
        
        assert result["valid"] is False
        assert "No content found" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_with_none_text_content(self, mock_logger):
        """Test validate method when _get_output_text returns None."""
        guardrail = CompanyCountGuardrail({"min_companies": 2})
        
        # Create an object that will cause _get_output_text to return None
        class BadObject:
            def __str__(self):
                raise Exception("Cannot convert to string")
        
        # Mock _get_output_text to return None directly to test the empty content path
        with patch.object(guardrail, '_get_output_text', return_value=None):
            result = guardrail.validate(BadObject())
            
            assert result["valid"] is False
            assert "No content found" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_with_sufficient_companies(self, mock_logger):
        """Test validate method with sufficient companies."""
        guardrail = CompanyCountGuardrail({"min_companies": 2})
        
        # Create text with company names that will be extracted
        text = "Here are some companies: Apple Inc., Microsoft Corporation, Google LLC, Amazon Ltd."
        
        result = guardrail.validate(text)
        
        assert result["valid"] is True
        assert result["feedback"] == ""
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_with_insufficient_companies(self, mock_logger):
        """Test validate method with insufficient companies."""
        guardrail = CompanyCountGuardrail({"min_companies": 50})
        
        # Create text with just one company
        text = "Here is one company: Apple Inc."
        
        result = guardrail.validate(text)
        
        assert result["valid"] is False
        assert "only includes" in result["feedback"]
        assert "at least 50" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_guardrail_inherits_from_base(self, mock_logger):
        """Test that CompanyCountGuardrail inherits from BaseGuardrail."""
        from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
        
        guardrail = CompanyCountGuardrail({})
        
        assert isinstance(guardrail, BaseGuardrail)
        assert hasattr(guardrail, 'config')
        assert hasattr(guardrail, 'validate')
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_config_parameter_access(self, mock_logger):
        """Test access to various config parameters."""
        config = {
            "min_companies": 15,
            "strict_mode": True,
            "patterns": ["Company", "Corp", "Inc"],
            "validation_rules": {
                "case_sensitive": False,
                "remove_duplicates": True
            }
        }
        
        guardrail = CompanyCountGuardrail(config)
        
        assert guardrail.min_companies == 15
        assert guardrail.config["strict_mode"] is True
        assert guardrail.config["patterns"] == ["Company", "Corp", "Inc"]
        assert guardrail.config["validation_rules"]["case_sensitive"] is False
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_negative_min_companies(self, mock_logger):
        """Test guardrail with negative min_companies."""
        config = {"min_companies": -5}
        guardrail = CompanyCountGuardrail(config)
        
        assert guardrail.min_companies == -5
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_large_min_companies(self, mock_logger):
        """Test guardrail with very large min_companies."""
        config = {"min_companies": 1000000}
        guardrail = CompanyCountGuardrail(config)
        
        assert guardrail.min_companies == 1000000
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_config_with_non_integer_min_companies(self, mock_logger):
        """Test guardrail with non-integer min_companies."""
        # Test with string that can be converted
        config = {"min_companies": "25"}
        guardrail = CompanyCountGuardrail(config)
        assert guardrail.min_companies == "25"  # Gets stored as-is from config
        
        # Test with float
        config = {"min_companies": 25.5}
        guardrail = CompanyCountGuardrail(config)
        assert guardrail.min_companies == 25.5
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_empty_config_uses_defaults(self, mock_logger):
        """Test that empty config uses default values."""
        guardrail = CompanyCountGuardrail({})
        
        assert guardrail.min_companies == 50  # Default value
        assert guardrail.config == {}
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_config_parameter_types(self, mock_logger):
        """Test various config parameter types."""
        config = {
            "min_companies": 20,
            "boolean_param": True,
            "string_param": "test_value",
            "list_param": [1, 2, 3],
            "none_param": None,
            "dict_param": {"nested": "value"}
        }
        
        guardrail = CompanyCountGuardrail(config)
        
        assert guardrail.config == config
        assert all(key in guardrail.config for key in config.keys())
    
    # Test _get_output_text method comprehensively
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_string(self, mock_logger):
        """Test _get_output_text with string input."""
        guardrail = CompanyCountGuardrail({})
        
        result = guardrail._get_output_text("test string")
        assert result == "test string"
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_task_output(self, mock_logger):
        """Test _get_output_text with TaskOutput object."""
        guardrail = CompanyCountGuardrail({})
        
        # Create a mock TaskOutput with content attribute
        mock_task_output = MagicMock(spec=TaskOutput)
        mock_task_output.content = "task output content"
        
        result = guardrail._get_output_text(mock_task_output)
        assert result == "task output content"
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_task_output_raw_output(self, mock_logger):
        """Test _get_output_text with TaskOutput object with raw_output."""
        guardrail = CompanyCountGuardrail({})
        
        # Create a mock TaskOutput with raw_output attribute
        mock_task_output = MagicMock(spec=TaskOutput)
        mock_task_output.content = None
        mock_task_output.raw_output = "raw output content"
        
        result = guardrail._get_output_text(mock_task_output)
        assert result == "raw output content"
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_task_output_fallback_to_str(self, mock_logger):
        """Test _get_output_text with TaskOutput object falling back to str()."""
        guardrail = CompanyCountGuardrail({})
        
        # Create a mock TaskOutput without content/raw_output but with string representation
        mock_task_output = MagicMock(spec=TaskOutput)
        mock_task_output.content = None
        mock_task_output.raw_output = None
        mock_task_output.output = None
        mock_task_output.text = None
        mock_task_output.result = None
        mock_task_output.response = None
        mock_task_output.__str__.return_value = "string representation"
        
        result = guardrail._get_output_text(mock_task_output)
        assert result == "string representation"
    
    def test_module_initialization_coverage(self):
        """Test module-level initialization code path."""
        # This test ensures the module-level initialization check exists
        # by importing the module and checking that the logger exists
        from src.engines.crewai.guardrails import company_count_guardrail
        
        # Verify that the module has the expected attributes after initialization
        assert hasattr(company_count_guardrail, 'logger_manager')
        assert hasattr(company_count_guardrail, 'logger')
        
        # The initialization code exists and has been executed during import
        # This covers the module-level initialization logic
        
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_dict(self, mock_logger):
        """Test _get_output_text with dictionary input."""
        guardrail = CompanyCountGuardrail({})
        
        # Test with dict containing content key
        dict_input = {"content": "dict content"}
        result = guardrail._get_output_text(dict_input)
        assert result == "dict content"
        
        # Test with dict containing raw_output key
        dict_input = {"raw_output": "raw dict content"}
        result = guardrail._get_output_text(dict_input)
        assert result == "raw dict content"
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_dict_json_fallback(self, mock_logger):
        """Test _get_output_text with dict falling back to JSON string."""
        guardrail = CompanyCountGuardrail({})
        
        # Test with dict without recognized keys
        dict_input = {"unknown_key": "value", "another_key": 123}
        result = guardrail._get_output_text(dict_input)
        assert '"unknown_key": "value"' in result
        assert '"another_key": 123' in result
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_unsupported_type(self, mock_logger):
        """Test _get_output_text with unsupported type."""
        guardrail = CompanyCountGuardrail({})
        
        # Test with list (unsupported type)
        result = guardrail._get_output_text([1, 2, 3])
        assert result == "[1, 2, 3]"
        
        # Test with number
        result = guardrail._get_output_text(123)
        assert result == "123"
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_none(self, mock_logger):
        """Test _get_output_text with None input."""
        guardrail = CompanyCountGuardrail({})
        
        result = guardrail._get_output_text(None)
        assert result == "None"
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_exception(self, mock_logger):
        """Test _get_output_text when str() raises exception."""
        guardrail = CompanyCountGuardrail({})
        
        # Create an object that raises exception on str()
        class BadObject:
            def __str__(self):
                raise Exception("Cannot convert to string")
        
        result = guardrail._get_output_text(BadObject())
        assert result is None
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_task_output_str_exception(self, mock_logger):
        """Test _get_output_text with TaskOutput object when str() fails."""
        guardrail = CompanyCountGuardrail({})
        
        # Create a mock TaskOutput that raises exception when converted to string
        mock_task_output = MagicMock(spec=TaskOutput)
        mock_task_output.content = None
        mock_task_output.raw_output = None
        mock_task_output.output = None
        mock_task_output.text = None
        mock_task_output.result = None
        mock_task_output.response = None
        mock_task_output.__str__.side_effect = Exception("String conversion failed")
        
        result = guardrail._get_output_text(mock_task_output)
        assert result is None
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_get_output_text_with_dict_json_exception(self, mock_logger):
        """Test _get_output_text with dict when JSON conversion fails."""
        guardrail = CompanyCountGuardrail({})
        
        # Create a dict that can't be serialized to JSON
        class UnserializableObject:
            pass
        
        dict_input = {"key": UnserializableObject()}
        
        # This should handle the JSON exception and return None
        result = guardrail._get_output_text(dict_input)
        assert result is None
    
    # Test _extract_companies method comprehensively
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_with_standard_suffixes(self, mock_logger):
        """Test _extract_companies with standard company suffixes."""
        guardrail = CompanyCountGuardrail({})
        
        text = "Apple Inc., Microsoft Corporation, Google LLC, Amazon Ltd., Facebook Company"
        companies = guardrail._extract_companies(text)
        
        assert len(companies) > 0
        # Check that at least some companies are extracted
        assert any("Apple" in company for company in companies)
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_with_swiss_companies(self, mock_logger):
        """Test _extract_companies with Swiss company identifiers."""
        guardrail = CompanyCountGuardrail({})
        
        text = "Here are some Swiss companies: Nestlé, Novartis, Roche, UBS"
        companies = guardrail._extract_companies(text)
        
        assert "Nestlé" in companies
        assert "Novartis" in companies
        assert "Roche" in companies
        assert "UBS" in companies
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_with_uid_pattern(self, mock_logger):
        """Test _extract_companies with UID pattern."""
        guardrail = CompanyCountGuardrail({})
        
        text = "CHE-123.456.789: Example Swiss Company AG\nCHE987654321 Another Company Ltd"
        companies = guardrail._extract_companies(text)
        
        assert len(companies) > 0
        assert any("Example Swiss Company AG" in company for company in companies)
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_with_quoted_names(self, mock_logger):
        """Test _extract_companies with quoted company names."""
        guardrail = CompanyCountGuardrail({})
        
        text = 'Companies include "Quoted Company Name" and "Another Quoted Corp"'
        companies = guardrail._extract_companies(text)
        
        assert len(companies) > 0
        assert any("Quoted Company Name" in company for company in companies)
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_filters_common_words(self, mock_logger):
        """Test _extract_companies filters out common words."""
        guardrail = CompanyCountGuardrail({})
        
        text = "The Company Corporation is not a real company. This is a test."
        companies = guardrail._extract_companies(text)
        
        # Should not include common words like "The", "This", "Company", "Corporation"
        assert "The" not in companies
        assert "This" not in companies
        assert "Company" not in companies
        assert "Corporation" not in companies
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_filters_short_names(self, mock_logger):
        """Test _extract_companies filters out very short names."""
        guardrail = CompanyCountGuardrail({})
        
        text = "Companies: A, AB, ABC Inc., ABCD Corporation"
        companies = guardrail._extract_companies(text)
        
        # Should not include very short names (less than 3 characters)
        assert "A" not in companies
        assert "AB" not in companies
        # But should include longer names
        assert any("ABC" in company for company in companies)
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_with_empty_text(self, mock_logger):
        """Test _extract_companies with empty text."""
        guardrail = CompanyCountGuardrail({})
        
        companies = guardrail._extract_companies("")
        assert len(companies) == 0
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_with_tuple_matches(self, mock_logger):
        """Test _extract_companies with regex patterns that return tuples."""
        guardrail = CompanyCountGuardrail({})
        
        # Test by temporarily modifying the regex patterns to return tuples  
        import re
        original_findall = re.findall
        
        def mock_findall(pattern, text):
            # For one of the patterns, return tuples to test tuple handling
            if '"Microsoft Inc"' in text:
                # Return a tuple to test the tuple handling code path
                return [("Microsoft Inc", "extra_group")]  
            return original_findall(pattern, text)
        
        with patch('re.findall', side_effect=mock_findall):
            text = 'Apple Corporation and "Microsoft Inc" are companies.'
            companies = guardrail._extract_companies(text)
            
            # Should handle tuple matches correctly - taking first element of tuple
            assert len(companies) > 0
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_extract_companies_filters_short_and_common_words(self, mock_logger):
        """Test _extract_companies filters short names and common words."""
        guardrail = CompanyCountGuardrail({})
        
        # Text with very short names, empty matches, and common words that should be filtered
        # Use pattern that will generate empty matches and common words
        text = "   \n\nThe This That These Those Their There They Company Corporation\n  \t AB C"
        companies = guardrail._extract_companies(text)
        
        # Should filter out single letters, short names, empty strings, and common words
        filtered_words = ["A", "B", "C", "Company", "Corporation", "The", "This", "That", "These", "Those", "Their", "There", "They"]
        for word in filtered_words:
            assert word not in companies
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_with_none_output(self, mock_logger):
        """Test validate method with None output."""
        guardrail = CompanyCountGuardrail({"min_companies": 2})
        
        result = guardrail.validate(None)
        
        assert result["valid"] is False
        # None gets converted to "None" string, so it doesn't trigger the empty content check
        # Instead it goes through the normal validation path
        assert "only includes 0 companies" in result["feedback"]
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_with_dict_output(self, mock_logger):
        """Test validate method with dictionary output."""
        guardrail = CompanyCountGuardrail({"min_companies": 2})
        
        dict_output = {"content": "Apple Inc., Microsoft Corporation, Google LLC"}
        result = guardrail.validate(dict_output)
        
        assert result["valid"] is True
        assert result["feedback"] == ""
    
    @patch('src.engines.crewai.guardrails.company_count_guardrail.logger')
    def test_validate_with_task_output(self, mock_logger):
        """Test validate method with TaskOutput object."""
        guardrail = CompanyCountGuardrail({"min_companies": 2})
        
        mock_task_output = MagicMock(spec=TaskOutput)
        mock_task_output.content = "Apple Inc., Microsoft Corporation, Google LLC"
        
        result = guardrail.validate(mock_task_output)
        
        assert result["valid"] is True
        assert result["feedback"] == ""