import pytest
from pydantic import ValidationError

from src.engines.crewai.tools.schemas import (
    GoogleSlidesToolOutput,
    PythonPPTXToolOutput
)


class TestGoogleSlidesToolOutput:
    """Test suite for GoogleSlidesToolOutput schema."""
    
    def test_valid_creation(self):
        """Test creating valid GoogleSlidesToolOutput."""
        output = GoogleSlidesToolOutput(success=True, message="Slides created successfully")
        
        assert output.success is True
        assert output.message == "Slides created successfully"
    
    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError) as exc_info:
            GoogleSlidesToolOutput()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'success' in required_fields
        assert 'message' in required_fields
    
    def test_field_types(self):
        """Test field type validation."""
        # Valid types
        output = GoogleSlidesToolOutput(success=True, message="test")
        assert isinstance(output.success, bool)
        assert isinstance(output.message, str)
        
        # Invalid success type should be coerced or raise error
        with pytest.raises(ValidationError):
            GoogleSlidesToolOutput(success="not_bool", message="test")
    
    def test_json_serialization(self):
        """Test JSON serialization."""
        output = GoogleSlidesToolOutput(success=False, message="Failed to create")
        json_data = output.model_dump()
        
        expected = {
            "success": False, 
            "message": "Failed to create",
            "presentation_id": None,
            "presentation_url": None
        }
        assert json_data == expected
    
    def test_valid_creation_minimal(self):
        """Test creating GoogleSlidesToolOutput with minimal required fields."""
        output = GoogleSlidesToolOutput(success=True, message="Success")
        
        assert output.success is True
        assert output.message == "Success"
        assert output.presentation_id is None
        assert output.presentation_url is None
    
    def test_valid_creation_full(self):
        """Test creating GoogleSlidesToolOutput with all fields."""
        output = GoogleSlidesToolOutput(
            success=True,
            message="Presentation created successfully",
            presentation_id="12345",
            presentation_url="https://docs.google.com/presentation/d/12345"
        )
        
        assert output.success is True
        assert output.message == "Presentation created successfully"
        assert output.presentation_id == "12345"
        assert output.presentation_url == "https://docs.google.com/presentation/d/12345"
    
    def test_optional_fields(self):
        """Test that presentation_id and presentation_url are optional."""
        output = GoogleSlidesToolOutput(success=False, message="Failed")
        assert output.presentation_id is None
        assert output.presentation_url is None


class TestPythonPPTXToolOutput:
    """Test suite for PythonPPTXToolOutput schema."""
    
    def test_valid_creation(self):
        """Test creating valid PythonPPTXToolOutput."""
        output = PythonPPTXToolOutput(
            success=True,
            message="Presentation created successfully",
            file_path="/path/to/file.pptx",
            relative_path="file.pptx",
            content="Test content",
            title="Test Presentation"
        )
        
        assert output.success is True
        assert output.message == "Presentation created successfully"
        assert output.file_path == "/path/to/file.pptx"
        assert output.relative_path == "file.pptx"
        assert output.content == "Test content"
        assert output.title == "Test Presentation"
    
    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError) as exc_info:
            PythonPPTXToolOutput()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'success' in required_fields
        assert 'message' in required_fields
        assert 'file_path' in required_fields
        assert 'relative_path' in required_fields
        assert 'content' in required_fields
        assert 'title' in required_fields
    
    def test_field_types(self):
        """Test field type validation."""
        output = PythonPPTXToolOutput(
            success=True,
            message="Test",
            file_path="/test.pptx",
            relative_path="test.pptx",
            content="Content",
            title="Title"
        )
        
        assert isinstance(output.success, bool)
        assert isinstance(output.message, str)
        assert isinstance(output.file_path, str)
        assert isinstance(output.relative_path, str)
        assert isinstance(output.content, str)
        assert isinstance(output.title, str)
    
    def test_json_serialization(self):
        """Test JSON serialization."""
        output = PythonPPTXToolOutput(
            success=False,
            message="Failed to create",
            file_path="",
            relative_path="",
            content="",
            title=""
        )
        
        json_data = output.model_dump()
        expected = {
            "success": False,
            "message": "Failed to create",
            "file_path": "",
            "relative_path": "",
            "content": "",
            "title": ""
        }
        assert json_data == expected


class TestSchemaInteroperability:
    """Test suite for schema interoperability and edge cases."""
    
    def test_schema_inheritance(self):
        """Test that all schemas inherit from BaseModel."""
        from pydantic import BaseModel
        
        schemas = [
            GoogleSlidesToolOutput,
            PythonPPTXToolOutput
        ]
        
        for schema_class in schemas:
            assert issubclass(schema_class, BaseModel)
    
    def test_field_descriptions(self):
        """Test that fields have descriptions."""
        # Test a sample schema
        fields = GoogleSlidesToolOutput.model_fields
        
        for field_name, field_info in fields.items():
            assert hasattr(field_info, 'description')
            assert field_info.description is not None
            assert len(field_info.description) > 0
    
    def test_model_validation_errors(self):
        """Test validation error handling."""
        with pytest.raises(ValidationError) as exc_info:
            GoogleSlidesToolOutput(success="not_bool", message="test")  # Invalid success type
        
        errors = exc_info.value.errors()
        assert len(errors) > 0
    
    def test_json_schema_generation(self):
        """Test that schemas can generate JSON schemas."""
        schema = GoogleSlidesToolOutput.model_json_schema()
        
        assert 'properties' in schema
        assert 'success' in schema['properties']
        assert 'message' in schema['properties']
        assert schema['properties']['success']['type'] == 'boolean'
        assert schema['properties']['message']['type'] == 'string'
    
    def test_model_copy(self):
        """Test model copying functionality."""
        original = GoogleSlidesToolOutput(success=True, message="Original")
        copy = original.model_copy(update={"message": "Updated"})
        
        assert original.message == "Original"
        assert copy.message == "Updated"
        assert original.success == copy.success
    
    def test_model_dump_json(self):
        """Test JSON string serialization."""
        output = GoogleSlidesToolOutput(success=True, message="test")
        json_str = output.model_dump_json()
        
        import json
        parsed = json.loads(json_str)
        assert parsed["success"] is True
        assert parsed["message"] == "test"