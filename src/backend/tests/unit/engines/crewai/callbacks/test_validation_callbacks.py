"""
Comprehensive test suite for validation callbacks to achieve 100% coverage.
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch, Mock
import json
import re

# Add the backend src to path
sys.path.insert(0, '/Users/nehme.tohme/workspace/kasal/src/backend/src')

from engines.crewai.callbacks.validation_callbacks import (
    SchemaValidator,
    ContentValidator,
    TypeValidator
)


class TestSchemaValidator:
    """Comprehensive test suite for SchemaValidator to achieve 100% coverage."""
    
    def test_schema_validator_initialization(self):
        """Test SchemaValidator initialization."""
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        validator = SchemaValidator(schema, max_retries=5, task_key="test_task")
        
        assert validator.schema == schema
        assert validator.max_retries == 5
        assert validator.task_key == "test_task"
        assert validator.retry_count == 0
        assert validator.metadata == {}
    
    def test_schema_validator_initialization_kwargs_only(self):
        """Test SchemaValidator initialization with only kwargs."""
        schema = {"type": "string"}
        validator = SchemaValidator(schema)
        
        assert validator.schema == schema
        assert validator.max_retries == 3  # default
        assert validator.task_key is None  # default
    
    @patch('jsonschema.validate')
    def test_execute_valid_dict_output(self, mock_validate):
        """Test execute with valid dictionary output."""
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        validator = SchemaValidator(schema)
        
        output = {"name": "test"}
        result = validator.execute(output)
        
        assert result is True
        mock_validate.assert_called_once_with(instance=output, schema=schema)
    
    @patch('jsonschema.validate')
    def test_execute_with_dict_method(self, mock_validate):
        """Test execute with output that has a dict() method."""
        schema = {"type": "object"}
        validator = SchemaValidator(schema)
        
        class MockOutput:
            def dict(self):
                return {"name": "test"}
        
        output = MockOutput()
        result = validator.execute(output)
        
        assert result is True
        mock_validate.assert_called_once_with(instance={"name": "test"}, schema=schema)
    
    @patch('jsonschema.validate')
    def test_execute_with_dict_attribute(self, mock_validate):
        """Test execute with output that has __dict__ attribute."""
        schema = {"type": "object"}
        validator = SchemaValidator(schema)
        
        class MockOutput:
            def __init__(self):
                self.name = "test"
                self.value = 42
        
        output = MockOutput()
        result = validator.execute(output)
        
        assert result is True
        expected_data = {"name": "test", "value": 42}
        mock_validate.assert_called_once_with(instance=expected_data, schema=schema)
    
    @patch('jsonschema.validate')
    def test_execute_with_dict_method_priority(self, mock_validate):
        """Test execute prioritizes dict() method over __dict__ attribute."""
        schema = {"type": "object"}
        validator = SchemaValidator(schema)
        
        class MockOutput:
            def __init__(self):
                self.name = "from_dict_attr"
            
            def dict(self):
                return {"name": "from_dict_method"}
        
        output = MockOutput()
        result = validator.execute(output)
        
        assert result is True
        mock_validate.assert_called_once_with(instance={"name": "from_dict_method"}, schema=schema)
    
    @patch('jsonschema.validate')
    def test_execute_with_plain_data(self, mock_validate):
        """Test execute with plain data (no dict method or __dict__)."""
        schema = {"type": "string"}
        validator = SchemaValidator(schema)
        
        output = "test string"
        result = validator.execute(output)
        
        assert result is True
        mock_validate.assert_called_once_with(instance=output, schema=schema)
    
    @patch('jsonschema.validate')
    def test_execute_validation_error(self, mock_validate):
        """Test execute when validation fails."""
        schema = {"type": "object", "required": ["name"]}
        validator = SchemaValidator(schema)
        
        mock_validate.side_effect = Exception("Validation failed")
        
        output = {"age": 25}  # missing required 'name' field
        
        with pytest.raises(Exception, match="Validation failed"):
            validator.execute(output)
        
        assert validator.metadata['validation_error'] == "Validation failed"
        mock_validate.assert_called_once_with(instance=output, schema=schema)
    
    def test_execute_import_error_handling(self):
        """Test execute when jsonschema import fails."""
        schema = {"type": "string"}
        validator = SchemaValidator(schema)
        
        # Mock the import to raise an exception
        with patch('builtins.__import__', side_effect=ImportError("jsonschema not found")):
            with pytest.raises(ImportError, match="jsonschema not found"):
                validator.execute("test")
    
    @patch('jsonschema.validate')
    def test_execute_generic_exception(self, mock_validate):
        """Test execute with generic exception during validation."""
        schema = {"type": "object"}
        validator = SchemaValidator(schema)
        
        mock_validate.side_effect = ValueError("Invalid schema")
        
        with pytest.raises(ValueError, match="Invalid schema"):
            validator.execute({})
        
        assert validator.metadata['validation_error'] == "Invalid schema"
    
    def test_execute_without_jsonschema_library(self):
        """Test execute when jsonschema library is not available."""
        schema = {"type": "string"}
        validator = SchemaValidator(schema)
        
        # Mock the import to fail by patching the module import
        import sys
        original_modules = sys.modules.copy()
        
        try:
            # Remove jsonschema from sys.modules if it exists
            if 'jsonschema' in sys.modules:
                del sys.modules['jsonschema']
            
            def mock_import(name, *args, **kwargs):
                if name == 'jsonschema':
                    raise ImportError("No module named 'jsonschema'")
                return __import__(name, *args, **kwargs)
            
            with patch('builtins.__import__', side_effect=mock_import):
                with pytest.raises(ImportError):
                    validator.execute("test")
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)


class TestContentValidator:
    """Comprehensive test suite for ContentValidator to achieve 100% coverage."""
    
    def test_content_validator_initialization_default(self):
        """Test ContentValidator initialization with default parameters."""
        validator = ContentValidator()
        
        assert validator.required_fields == []
        assert validator.min_length is None
        assert validator.max_length is None
        assert validator.pattern is None
        assert validator.custom_validator is None
        assert validator.max_retries == 3
        assert validator.task_key is None
    
    def test_content_validator_initialization_all_params(self):
        """Test ContentValidator initialization with all parameters."""
        def custom_func(x):
            return True
        
        validator = ContentValidator(
            required_fields=["name", "age"],
            min_length=10,
            max_length=100,
            pattern=r"^test",
            custom_validator=custom_func,
            max_retries=5,
            task_key="test_task"
        )
        
        assert validator.required_fields == ["name", "age"]
        assert validator.min_length == 10
        assert validator.max_length == 100
        assert validator.pattern == r"^test"
        assert validator.custom_validator == custom_func
        assert validator.max_retries == 5
        assert validator.task_key == "test_task"
    
    def test_content_validator_initialization_none_required_fields(self):
        """Test ContentValidator with None required_fields becomes empty list."""
        validator = ContentValidator(required_fields=None)
        
        assert validator.required_fields == []
    
    def test_execute_simple_string_success(self):
        """Test execute with simple string that passes all validations."""
        validator = ContentValidator()
        
        result = validator.execute("test string")
        
        assert result is True
    
    def test_execute_required_fields_success(self):
        """Test execute with required fields validation success."""
        validator = ContentValidator(required_fields=["name", "age"])
        
        class MockOutput:
            def __init__(self):
                self.name = "John"
                self.age = 30
                self.extra = "field"
        
        output = MockOutput()
        result = validator.execute(output)
        
        assert result is True
    
    def test_execute_required_fields_missing(self):
        """Test execute with missing required fields."""
        validator = ContentValidator(required_fields=["name", "age", "email"])
        
        class MockOutput:
            def __init__(self):
                self.name = "John"
                # missing age and email
        
        output = MockOutput()
        
        with pytest.raises(ValueError, match="Missing required fields: \\['age', 'email'\\]"):
            validator.execute(output)
        
        assert validator.metadata['missing_fields'] == ["age", "email"]
    
    def test_execute_required_fields_no_dict_attr(self):
        """Test execute with required fields but output has no __dict__."""
        validator = ContentValidator(required_fields=["name"])
        
        # String output doesn't have __dict__ with the required field
        result = validator.execute("test string")
        
        # Should pass because the check only applies to objects with __dict__
        assert result is True
    
    def test_execute_min_length_success(self):
        """Test execute with minimum length validation success."""
        validator = ContentValidator(min_length=5)
        
        result = validator.execute("hello world")
        
        assert result is True
    
    def test_execute_min_length_failure(self):
        """Test execute with minimum length validation failure."""
        validator = ContentValidator(min_length=10)
        
        with pytest.raises(ValueError, match="Content length 4 is less than minimum 10"):
            validator.execute("test")
    
    def test_execute_max_length_success(self):
        """Test execute with maximum length validation success."""
        validator = ContentValidator(max_length=10)
        
        result = validator.execute("hello")
        
        assert result is True
    
    def test_execute_max_length_failure(self):
        """Test execute with maximum length validation failure."""
        validator = ContentValidator(max_length=5)
        
        with pytest.raises(ValueError, match="Content length 11 exceeds maximum 5"):
            validator.execute("hello world")
    
    def test_execute_pattern_success(self):
        """Test execute with pattern validation success."""
        validator = ContentValidator(pattern=r"^test")
        
        result = validator.execute("test string")
        
        assert result is True
    
    def test_execute_pattern_failure(self):
        """Test execute with pattern validation failure."""
        validator = ContentValidator(pattern=r"^hello")
        
        with pytest.raises(ValueError, match="Content does not match pattern: \\^hello"):
            validator.execute("test string")
    
    def test_execute_custom_validator_success(self):
        """Test execute with custom validator success."""
        def custom_func(output):
            return "test" in str(output)
        
        validator = ContentValidator(custom_validator=custom_func)
        
        result = validator.execute("test string")
        
        assert result is True
    
    def test_execute_custom_validator_failure(self):
        """Test execute with custom validator failure."""
        def custom_func(output):
            return "hello" in str(output)
        
        validator = ContentValidator(custom_validator=custom_func)
        
        with pytest.raises(ValueError, match="Custom validation failed"):
            validator.execute("test string")
    
    def test_execute_all_validations_success(self):
        """Test execute with all validations passing."""
        def custom_func(output):
            return len(str(output)) > 5
        
        validator = ContentValidator(
            min_length=10,
            max_length=50,
            pattern=r"test",
            custom_validator=custom_func
        )
        
        result = validator.execute("test string with content")
        
        assert result is True
    
    def test_execute_min_length_zero(self):
        """Test execute with min_length of 0."""
        validator = ContentValidator(min_length=0)
        
        result = validator.execute("")
        
        assert result is True
    
    def test_execute_max_length_zero(self):
        """Test execute with max_length of 0 and empty string."""
        validator = ContentValidator(max_length=0)
        
        result = validator.execute("")
        
        assert result is True
    
    def test_execute_max_length_zero_edge_case(self):
        """Test execute with max_length of 0 and non-empty string."""
        # Note: max_length=0 is falsy, so the check won't trigger
        # This tests the edge case behavior
        validator = ContentValidator(max_length=0)
        
        # This should actually pass because 0 is falsy and the check is skipped
        result = validator.execute("a")
        
        assert result is True
    
    def test_execute_max_length_one_failure(self):
        """Test execute with max_length of 1 and longer string."""
        validator = ContentValidator(max_length=1)
        
        with pytest.raises(ValueError, match="Content length 2 exceeds maximum 1"):
            validator.execute("ab")
    
    def test_execute_pattern_with_complex_regex(self):
        """Test execute with complex regex pattern."""
        validator = ContentValidator(pattern=r"\d{3}-\d{2}-\d{4}")  # SSN pattern
        
        result = validator.execute("123-45-6789")
        
        assert result is True
    
    def test_execute_pattern_with_complex_regex_failure(self):
        """Test execute with complex regex pattern failure."""
        validator = ContentValidator(pattern=r"\d{3}-\d{2}-\d{4}")  # SSN pattern
        
        with pytest.raises(ValueError):
            validator.execute("invalid-ssn")
    
    def test_execute_custom_validator_with_object(self):
        """Test execute with custom validator receiving object."""
        def custom_func(output):
            return hasattr(output, 'name')
        
        validator = ContentValidator(custom_validator=custom_func)
        
        class MockOutput:
            def __init__(self):
                self.name = "test"
        
        result = validator.execute(MockOutput())
        
        assert result is True
    
    def test_execute_custom_validator_with_object_failure(self):
        """Test execute with custom validator receiving object that fails."""
        def custom_func(output):
            return hasattr(output, 'nonexistent_attr')
        
        validator = ContentValidator(custom_validator=custom_func)
        
        class MockOutput:
            def __init__(self):
                self.name = "test"
        
        with pytest.raises(ValueError, match="Custom validation failed"):
            validator.execute(MockOutput())
    
    def test_execute_string_conversion_with_number(self):
        """Test execute with numeric input (converted to string)."""
        validator = ContentValidator(min_length=2, pattern=r"\d+")
        
        result = validator.execute(123)
        
        assert result is True
    
    def test_execute_string_conversion_with_boolean(self):
        """Test execute with boolean input (converted to string)."""
        validator = ContentValidator(min_length=4, pattern=r"True")
        
        result = validator.execute(True)
        
        assert result is True


class TestTypeValidator:
    """Comprehensive test suite for TypeValidator to achieve 100% coverage."""
    
    def test_type_validator_initialization_default(self):
        """Test TypeValidator initialization with default parameters."""
        validator = TypeValidator(str)
        
        assert validator.expected_type == str
        assert validator.allow_none is False
        assert validator.max_retries == 3
        assert validator.task_key is None
    
    def test_type_validator_initialization_all_params(self):
        """Test TypeValidator initialization with all parameters."""
        validator = TypeValidator(
            expected_type=int,
            allow_none=True,
            max_retries=5,
            task_key="test_task"
        )
        
        assert validator.expected_type == int
        assert validator.allow_none is True
        assert validator.max_retries == 5
        assert validator.task_key == "test_task"
    
    def test_execute_correct_type_success(self):
        """Test execute with correct type."""
        validator = TypeValidator(str)
        
        result = validator.execute("test string")
        
        assert result is True
    
    def test_execute_wrong_type_failure(self):
        """Test execute with wrong type."""
        validator = TypeValidator(str)
        
        with pytest.raises(TypeError, match="Expected type str, got int"):
            validator.execute(123)
    
    def test_execute_none_value_allowed(self):
        """Test execute with None value when allow_none is True."""
        validator = TypeValidator(str, allow_none=True)
        
        result = validator.execute(None)
        
        assert result is True
    
    def test_execute_none_value_not_allowed(self):
        """Test execute with None value when allow_none is False."""
        validator = TypeValidator(str, allow_none=False)
        
        with pytest.raises(ValueError, match="Output is None but allow_none is False"):
            validator.execute(None)
    
    def test_execute_none_value_default_not_allowed(self):
        """Test execute with None value with default allow_none (False)."""
        validator = TypeValidator(str)
        
        with pytest.raises(ValueError, match="Output is None but allow_none is False"):
            validator.execute(None)
    
    def test_execute_int_type_success(self):
        """Test execute with int type validation."""
        validator = TypeValidator(int)
        
        result = validator.execute(42)
        
        assert result is True
    
    def test_execute_float_type_success(self):
        """Test execute with float type validation."""
        validator = TypeValidator(float)
        
        result = validator.execute(3.14)
        
        assert result is True
    
    def test_execute_bool_type_success(self):
        """Test execute with bool type validation."""
        validator = TypeValidator(bool)
        
        result = validator.execute(True)
        
        assert result is True
    
    def test_execute_list_type_success(self):
        """Test execute with list type validation."""
        validator = TypeValidator(list)
        
        result = validator.execute([1, 2, 3])
        
        assert result is True
    
    def test_execute_dict_type_success(self):
        """Test execute with dict type validation."""
        validator = TypeValidator(dict)
        
        result = validator.execute({"key": "value"})
        
        assert result is True
    
    def test_execute_custom_class_type_success(self):
        """Test execute with custom class type validation."""
        class CustomClass:
            pass
        
        validator = TypeValidator(CustomClass)
        
        result = validator.execute(CustomClass())
        
        assert result is True
    
    def test_execute_custom_class_type_failure(self):
        """Test execute with custom class type validation failure."""
        class CustomClass:
            pass
        
        class OtherClass:
            pass
        
        validator = TypeValidator(CustomClass)
        
        with pytest.raises(TypeError, match="Expected type CustomClass, got OtherClass"):
            validator.execute(OtherClass())
    
    def test_execute_inheritance_success(self):
        """Test execute with inheritance (subclass validation)."""
        class BaseClass:
            pass
        
        class DerivedClass(BaseClass):
            pass
        
        validator = TypeValidator(BaseClass)
        
        result = validator.execute(DerivedClass())
        
        assert result is True
    
    def test_execute_tuple_type_success(self):
        """Test execute with tuple type validation."""
        validator = TypeValidator(tuple)
        
        result = validator.execute((1, 2, 3))
        
        assert result is True
    
    def test_execute_set_type_success(self):
        """Test execute with set type validation."""
        validator = TypeValidator(set)
        
        result = validator.execute({1, 2, 3})
        
        assert result is True
    
    def test_execute_multiple_wrong_types(self):
        """Test execute with various wrong types to ensure good error messages."""
        validator = TypeValidator(str)
        
        test_cases = [
            (123, "Expected type str, got int"),
            (3.14, "Expected type str, got float"),
            (True, "Expected type str, got bool"),
            ([1, 2, 3], "Expected type str, got list"),
            ({"key": "value"}, "Expected type str, got dict"),
            ((1, 2, 3), "Expected type str, got tuple"),
            ({1, 2, 3}, "Expected type str, got set")
        ]
        
        for value, expected_message in test_cases:
            with pytest.raises(TypeError, match=expected_message):
                validator.execute(value)
    
    def test_execute_none_with_different_expected_types(self):
        """Test execute with None for different expected types."""
        type_validators = [
            TypeValidator(str, allow_none=True),
            TypeValidator(int, allow_none=True),
            TypeValidator(float, allow_none=True),
            TypeValidator(bool, allow_none=True),
            TypeValidator(list, allow_none=True),
            TypeValidator(dict, allow_none=True),
            TypeValidator(tuple, allow_none=True),
            TypeValidator(set, allow_none=True)
        ]
        
        for validator in type_validators:
            result = validator.execute(None)
            assert result is True


class TestValidationCallbacksIntegration:
    """Integration tests to ensure all validators work together and edge cases."""
    
    def test_all_validators_have_common_methods(self):
        """Test that all validators have execute method."""
        validators = [
            SchemaValidator({"type": "string"}),
            ContentValidator(),
            TypeValidator(str)
        ]
        
        for validator in validators:
            assert hasattr(validator, 'execute')
            assert callable(getattr(validator, 'execute'))
    
    def test_validators_metadata_attribute(self):
        """Test that all validators have metadata attribute."""
        validators = [
            SchemaValidator({"type": "string"}),
            ContentValidator(),
            TypeValidator(str)
        ]
        
        for validator in validators:
            assert hasattr(validator, 'metadata')
            assert isinstance(validator.metadata, dict)
    
    def test_validators_with_complex_data(self):
        """Test validators with complex nested data structures."""
        schema = {
            "type": "object",
            "properties": {
                "users": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "age": {"type": "integer"}
                        }
                    }
                }
            }
        }
        
        schema_validator = SchemaValidator(schema)
        content_validator = ContentValidator(min_length=20)
        type_validator = TypeValidator(dict)
        
        complex_data = {
            "users": [
                {"name": "John", "age": 30},
                {"name": "Jane", "age": 25}
            ]
        }
        
        # All should pass
        with patch('jsonschema.validate'):
            assert schema_validator.execute(complex_data) is True
        assert content_validator.execute(complex_data) is True
        assert type_validator.execute(complex_data) is True
    
    def test_error_handling_consistency(self):
        """Test that all validators handle errors consistently."""
        validators_and_bad_inputs = [
            (SchemaValidator({"type": "string"}), 123),  # Will cause validation error
            (ContentValidator(min_length=100), "short"),  # Will cause length error
            (TypeValidator(str), 123)  # Will cause type error
        ]
        
        for validator, bad_input in validators_and_bad_inputs:
            with pytest.raises(Exception):  # Each will raise different exception types
                if isinstance(validator, SchemaValidator):
                    with patch('jsonschema.validate', 
                              side_effect=Exception("Schema validation failed")):
                        validator.execute(bad_input)
                else:
                    validator.execute(bad_input)