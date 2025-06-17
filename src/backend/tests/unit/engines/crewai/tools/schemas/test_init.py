import pytest
from pydantic import ValidationError

from src.engines.crewai.tools.schemas import (
    SendPulseEmailOutput,
    EmailContent,
    EmailSender,
    EmailRecipient,
    GoogleSlidesToolOutput,
    PythonPPTXToolOutput
)


class TestSendPulseEmailOutput:
    """Test suite for SendPulseEmailOutput schema."""
    
    def test_valid_creation(self):
        """Test creating valid SendPulseEmailOutput."""
        output = SendPulseEmailOutput(success=True, message="Email sent successfully")
        
        assert output.success is True
        assert output.message == "Email sent successfully"
    
    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError) as exc_info:
            SendPulseEmailOutput()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'success' in required_fields
        assert 'message' in required_fields
    
    def test_field_types(self):
        """Test field type validation."""
        # Valid types
        output = SendPulseEmailOutput(success=True, message="test")
        assert isinstance(output.success, bool)
        assert isinstance(output.message, str)
        
        # Invalid success type should be coerced or raise error
        with pytest.raises(ValidationError):
            SendPulseEmailOutput(success="not_bool", message="test")
    
    def test_json_serialization(self):
        """Test JSON serialization."""
        output = SendPulseEmailOutput(success=False, message="Failed to send")
        json_data = output.model_dump()
        
        assert json_data == {"success": False, "message": "Failed to send"}


class TestEmailContent:
    """Test suite for EmailContent schema."""
    
    def test_valid_creation_minimal(self):
        """Test creating EmailContent with minimal required fields."""
        content = EmailContent(subject="Test Subject", html="<h1>Test</h1>")
        
        assert content.subject == "Test Subject"
        assert content.html == "<h1>Test</h1>"
        assert content.text is None
    
    def test_valid_creation_full(self):
        """Test creating EmailContent with all fields."""
        content = EmailContent(
            subject="Test Subject",
            html="<h1>Test</h1>",
            text="Test"
        )
        
        assert content.subject == "Test Subject"
        assert content.html == "<h1>Test</h1>"
        assert content.text == "Test"
    
    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError) as exc_info:
            EmailContent()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'subject' in required_fields
        assert 'html' in required_fields
    
    def test_optional_text_field(self):
        """Test that text field is optional."""
        content = EmailContent(subject="Test", html="<p>HTML</p>")
        assert content.text is None
        
        content_with_text = EmailContent(subject="Test", html="<p>HTML</p>", text="Plain text")
        assert content_with_text.text == "Plain text"


class TestEmailSender:
    """Test suite for EmailSender schema."""
    
    def test_valid_creation(self):
        """Test creating valid EmailSender."""
        sender = EmailSender(name="John Doe", email="john@example.com")
        
        assert sender.name == "John Doe"
        assert sender.email == "john@example.com"
    
    def test_required_fields(self):
        """Test that both fields are required."""
        with pytest.raises(ValidationError) as exc_info:
            EmailSender()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'name' in required_fields
        assert 'email' in required_fields
    
    def test_field_types(self):
        """Test field type validation."""
        sender = EmailSender(name="Test User", email="test@example.com")
        assert isinstance(sender.name, str)
        assert isinstance(sender.email, str)


class TestEmailRecipient:
    """Test suite for EmailRecipient schema."""
    
    def test_valid_creation_minimal(self):
        """Test creating EmailRecipient with minimal required fields."""
        recipient = EmailRecipient(email="recipient@example.com")
        
        assert recipient.email == "recipient@example.com"
        assert recipient.name is None
    
    def test_valid_creation_full(self):
        """Test creating EmailRecipient with all fields."""
        recipient = EmailRecipient(name="Jane Doe", email="jane@example.com")
        
        assert recipient.name == "Jane Doe"
        assert recipient.email == "jane@example.com"
    
    def test_required_email_field(self):
        """Test that email field is required."""
        with pytest.raises(ValidationError) as exc_info:
            EmailRecipient()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'email' in required_fields
    
    def test_optional_name_field(self):
        """Test that name field is optional."""
        recipient = EmailRecipient(email="test@example.com")
        assert recipient.name is None
        
        recipient_with_name = EmailRecipient(name="Test User", email="test@example.com")
        assert recipient_with_name.name == "Test User"


class TestGoogleSlidesToolOutput:
    """Test suite for GoogleSlidesToolOutput schema."""
    
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
    
    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError) as exc_info:
            GoogleSlidesToolOutput()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'success' in required_fields
        assert 'message' in required_fields
    
    def test_optional_fields(self):
        """Test that presentation_id and presentation_url are optional."""
        output = GoogleSlidesToolOutput(success=False, message="Failed")
        assert output.presentation_id is None
        assert output.presentation_url is None
    
    def test_boolean_success_field(self):
        """Test success field type validation."""
        output = GoogleSlidesToolOutput(success=True, message="test")
        assert isinstance(output.success, bool)
        
        output_false = GoogleSlidesToolOutput(success=False, message="test")
        assert isinstance(output_false.success, bool)


class TestPythonPPTXToolOutput:
    """Test suite for PythonPPTXToolOutput schema."""
    
    def test_valid_creation(self):
        """Test creating valid PythonPPTXToolOutput."""
        output = PythonPPTXToolOutput(
            success=True,
            message="Presentation created",
            file_path="/absolute/path/to/presentation.pptx",
            relative_path="presentations/presentation.pptx",
            content="Slide content",
            title="My Presentation"
        )
        
        assert output.success is True
        assert output.message == "Presentation created"
        assert output.file_path == "/absolute/path/to/presentation.pptx"
        assert output.relative_path == "presentations/presentation.pptx"
        assert output.content == "Slide content"
        assert output.title == "My Presentation"
    
    def test_required_fields(self):
        """Test that all fields are required."""
        with pytest.raises(ValidationError) as exc_info:
            PythonPPTXToolOutput()
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        expected_fields = {'success', 'message', 'file_path', 'relative_path', 'content', 'title'}
        assert required_fields == expected_fields
    
    def test_field_types(self):
        """Test field type validation."""
        output = PythonPPTXToolOutput(
            success=True,
            message="test",
            file_path="/path",
            relative_path="rel/path",
            content="content",
            title="title"
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
            SendPulseEmailOutput,
            EmailContent,
            EmailSender,
            EmailRecipient,
            GoogleSlidesToolOutput,
            PythonPPTXToolOutput
        ]
        
        for schema_class in schemas:
            assert issubclass(schema_class, BaseModel)
    
    def test_field_descriptions(self):
        """Test that fields have descriptions."""
        # Test a sample schema
        output = SendPulseEmailOutput(success=True, message="test")
        fields = output.model_fields
        
        for field_name, field_info in fields.items():
            assert hasattr(field_info, 'description')
            assert field_info.description is not None
            assert len(field_info.description) > 0
    
    def test_model_validation_errors(self):
        """Test validation error handling."""
        with pytest.raises(ValidationError) as exc_info:
            EmailSender(name=123, email="invalid")  # Invalid name type
        
        errors = exc_info.value.errors()
        assert len(errors) > 0
        assert any(error['type'] == 'string_type' for error in errors)
    
    def test_json_schema_generation(self):
        """Test that schemas can generate JSON schemas."""
        schema = SendPulseEmailOutput.model_json_schema()
        
        assert 'properties' in schema
        assert 'success' in schema['properties']
        assert 'message' in schema['properties']
        assert schema['properties']['success']['type'] == 'boolean'
        assert schema['properties']['message']['type'] == 'string'
    
    def test_model_copy(self):
        """Test model copying functionality."""
        original = EmailSender(name="Original", email="original@example.com")
        copy = original.model_copy(update={"name": "Updated"})
        
        assert original.name == "Original"
        assert copy.name == "Updated"
        assert original.email == copy.email
    
    def test_model_dump_json(self):
        """Test JSON string serialization."""
        output = SendPulseEmailOutput(success=True, message="test")
        json_str = output.model_dump_json()
        
        import json
        parsed = json.loads(json_str)
        assert parsed["success"] is True
        assert parsed["message"] == "test"
    
    def test_schema_with_empty_strings(self):
        """Test schemas with empty string values."""
        content = EmailContent(subject="", html="")
        assert content.subject == ""
        assert content.html == ""
        
        output = PythonPPTXToolOutput(
            success=True,
            message="",
            file_path="",
            relative_path="",
            content="",
            title=""
        )
        assert output.message == ""
        assert output.file_path == ""
    
    def test_schema_field_aliases(self):
        """Test that schemas work with their defined field names."""
        # Verify field names are as expected
        sender_fields = EmailSender.model_fields.keys()
        assert 'name' in sender_fields
        assert 'email' in sender_fields
        
        recipient_fields = EmailRecipient.model_fields.keys()
        assert 'name' in recipient_fields
        assert 'email' in recipient_fields