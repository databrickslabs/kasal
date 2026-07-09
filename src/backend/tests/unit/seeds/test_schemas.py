"""
Unit tests for schemas seeding module.

Tests the functionality of seeding schema definitions into the database.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from datetime import datetime
from src.seeds.schemas import (
    SAMPLE_SCHEMAS,
    seed_async,
    seed
)


@pytest.fixture
def mock_session():
    """Create mock database session."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()
    return session


@pytest.fixture
def mock_schema_model():
    """Create mock Schema model."""
    schema = MagicMock()
    schema.name = "TestSchema"
    schema.description = "Test description"
    schema.schema_type = "schema"
    schema.schema_definition = {"type": "object"}
    schema.created_at = datetime.now()
    schema.updated_at = datetime.now()
    return schema


@pytest.fixture
def sample_schema_data():
    """Create sample schema data."""
    return {
        "name": "TestSchema",
        "description": "Test schema description",
        "schema_type": "schema",
        "schema_definition": {
            "type": "object",
            "properties": {
                "title": {"type": "string"}
            },
            "required": ["title"]
        }
    }


class TestSampleSchemas:
    """Test sample schema definitions."""

    def test_sample_schemas_structure(self):
        """Test that SAMPLE_SCHEMAS has correct structure."""
        assert isinstance(SAMPLE_SCHEMAS, list)
        assert len(SAMPLE_SCHEMAS) == 16  # the business-outcome catalog

        for schema in SAMPLE_SCHEMAS:
            assert "name" in schema
            assert "description" in schema
            assert "schema_type" in schema
            assert "schema_definition" in schema

            # Validate schema_definition is valid JSON
            assert isinstance(schema["schema_definition"], dict)
            assert "type" in schema["schema_definition"]

    def test_required_fields_exist_in_properties(self):
        """Every schema's `required` list only names declared properties."""
        for schema in SAMPLE_SCHEMAS:
            definition = schema["schema_definition"]
            properties = definition.get("properties", {})
            for field in definition.get("required", []):
                assert field in properties, \
                    f"'{schema['name']}' requires undeclared field '{field}'"

    def test_operation_result_schema(self):
        """Test OperationResult schema definition (action outcomes)."""
        op_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "OperationResult"),
            None
        )

        assert op_schema is not None
        assert op_schema["schema_type"] == "schema"
        assert "success" in op_schema["schema_definition"]["properties"]
        assert "status" in op_schema["schema_definition"]["properties"]
        assert "status" in op_schema["schema_definition"]["required"]
        assert "success" in op_schema["schema_definition"]["required"]

    def test_data_load_result_schema(self):
        """Test DataLoadResult schema definition (ETL row counts)."""
        load_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "DataLoadResult"),
            None
        )

        assert load_schema is not None
        assert load_schema["schema_type"] == "schema"
        assert "table" in load_schema["schema_definition"]["properties"]
        assert "rows_inserted" in load_schema["schema_definition"]["properties"]
        assert "rows_inserted" in load_schema["schema_definition"]["required"]

    def test_support_ticket_triage_schema(self):
        """Test SupportTicketTriage schema definition (classification/routing)."""
        triage_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "SupportTicketTriage"),
            None
        )

        assert triage_schema is not None
        assert triage_schema["schema_type"] == "schema"
        assert "category" in triage_schema["schema_definition"]["properties"]
        assert "priority" in triage_schema["schema_definition"]["properties"]
        assert "requires_human" in triage_schema["schema_definition"]["properties"]

    def test_sentiment_analysis_schema(self):
        """Test SentimentAnalysis schema definition."""
        sentiment_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "SentimentAnalysis"),
            None
        )

        assert sentiment_schema is not None
        assert sentiment_schema["schema_type"] == "schema"
        assert "sentiment" in sentiment_schema["schema_definition"]["properties"]
        assert "score" in sentiment_schema["schema_definition"]["properties"]
        assert "sentiment" in sentiment_schema["schema_definition"]["required"]

    def test_intent_classification_schema(self):
        """Test IntentClassification schema definition."""
        intent_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "IntentClassification"),
            None
        )

        assert intent_schema is not None
        assert intent_schema["schema_type"] == "schema"
        assert "intent" in intent_schema["schema_definition"]["properties"]
        assert "confidence" in intent_schema["schema_definition"]["properties"]
        assert "intent" in intent_schema["schema_definition"]["required"]

    def test_evaluation_schema(self):
        """Test Evaluation schema definition."""
        eval_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "Evaluation"),
            None
        )

        assert eval_schema is not None
        assert eval_schema["schema_type"] == "schema"
        assert "subject" in eval_schema["schema_definition"]["properties"]
        assert "score" in eval_schema["schema_definition"]["properties"]
        assert "verdict" in eval_schema["schema_definition"]["properties"]

    def test_all_schema_names(self):
        """Test all expected schema names are present."""
        expected_names = [
            "OperationResult", "DataLoadResult", "SupportTicketTriage",
            "SentimentAnalysis", "IntentClassification", "CustomerFeedback",
            "WebSearchResult", "ApprovalDecision", "LeadQualification",
            "ResumeScreening", "Evaluation", "RiskAssessment",
            "ContentModeration", "FraudCheck", "ExpenseApproval", "InvoiceData",
        ]

        actual_names = [s["name"] for s in SAMPLE_SCHEMAS]

        for name in expected_names:
            assert name in actual_names, f"Missing schema: {name}"


class TestAsyncSeeding:
    """Test async schema seeding functionality."""

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    @patch('src.seeds.schemas.Schema')
    @patch('src.seeds.schemas.datetime')
    async def test_seed_async_new_schemas(self, mock_datetime, mock_schema_class, mock_select, mock_session_factory, mock_session, sample_schema_data):
        """Test async seeding with new schemas."""
        # Mock datetime
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now

        # Mock session factory context manager
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context

        # Mock query result - no existing schema
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        # Mock Schema model creation
        mock_schema_instance = MagicMock()
        mock_schema_class.return_value = mock_schema_instance

        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()

        # Verify schema was added
        mock_session.add.assert_called_once_with(mock_schema_instance)
        mock_session.commit.assert_called()

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    async def test_seed_async_existing_schemas(self, mock_select, mock_session_factory, mock_session, mock_schema_model, sample_schema_data):
        """Test async seeding with existing schemas."""
        # Mock session factory context manager
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context

        # Mock query result - schema exists
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_schema_model
        mock_session.execute.return_value = mock_result

        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()

        # Verify schema was updated (not added)
        assert mock_schema_model.description == sample_schema_data["description"]
        assert mock_schema_model.schema_type == sample_schema_data["schema_type"]
        mock_session.add.assert_not_called()
        mock_session.commit.assert_called()

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    async def test_seed_async_error_handling(self, mock_select, mock_session_factory, mock_session, sample_schema_data):
        """Test async seeding with error handling."""
        # Mock session factory context manager
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context

        # Mock execute to raise exception
        mock_session.execute.side_effect = Exception("Database error")

        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            # Should not raise - errors are logged
            await seed_async()

        # Verify error was handled (no crash)


class TestMainSeedFunction:
    """Test main seed function."""

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.seed_async')
    async def test_seed_success(self, mock_seed_async):
        """Test successful seed execution."""
        mock_seed_async.return_value = None

        await seed()

        mock_seed_async.assert_called_once()

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.seed_async')
    async def test_seed_error(self, mock_seed_async):
        """Test seed execution with error."""
        mock_seed_async.side_effect = Exception("Seeding error")

        # Should not raise exception (errors are logged)
        await seed()

        mock_seed_async.assert_called_once()


class TestSchemaValidation:
    """Test schema definition validation."""

    def test_all_schemas_have_required_fields(self):
        """Test that all schemas have required fields."""
        required_fields = ["name", "description", "schema_type", "schema_definition"]

        for schema in SAMPLE_SCHEMAS:
            for field in required_fields:
                assert field in schema, f"Schema '{schema.get('name', 'unknown')}' missing required field '{field}'"

    def test_schema_definitions_are_valid_json_schema(self):
        """Test that schema definitions are valid JSON schema format."""
        for schema in SAMPLE_SCHEMAS:
            schema_def = schema["schema_definition"]

            # Must have type
            assert "type" in schema_def, f"Schema '{schema['name']}' definition missing 'type'"

            # Must be object type
            assert schema_def["type"] == "object", f"Schema '{schema['name']}' must be object type"

            # Must have properties
            assert "properties" in schema_def, f"Schema '{schema['name']}' missing 'properties'"
            assert isinstance(schema_def["properties"], dict)

            # Each property should have a type
            for prop_name, prop_def in schema_def["properties"].items():
                assert "type" in prop_def, f"Property '{prop_name}' in schema '{schema['name']}' missing type"

    def test_schema_types_are_valid(self):
        """Test that all schema types are 'schema'."""
        for schema in SAMPLE_SCHEMAS:
            assert schema["schema_type"] == "schema", f"Schema '{schema['name']}' should have type 'schema'"

    def test_required_fields_exist_in_properties(self):
        """Test that required fields exist in properties."""
        for schema in SAMPLE_SCHEMAS:
            schema_def = schema["schema_definition"]
            if "required" in schema_def:
                properties = schema_def.get("properties", {})
                for required_field in schema_def["required"]:
                    assert required_field in properties, \
                        f"Required field '{required_field}' not in properties of '{schema['name']}'"

    def test_array_properties_have_items(self):
        """Test that array properties have items definition."""
        for schema in SAMPLE_SCHEMAS:
            schema_def = schema["schema_definition"]
            properties = schema_def.get("properties", {})

            for prop_name, prop_def in properties.items():
                if prop_def.get("type") == "array":
                    assert "items" in prop_def, \
                        f"Array property '{prop_name}' in '{schema['name']}' missing 'items'"


class TestSchemaCount:
    """Test schema count and naming."""

    def test_exactly_sixteen_schemas(self):
        """Test that the catalog carries exactly the 16 seeded schemas."""
        assert len(SAMPLE_SCHEMAS) == 16

    def test_unique_schema_names(self):
        """Test that all schema names are unique."""
        names = [s["name"] for s in SAMPLE_SCHEMAS]
        assert len(names) == len(set(names)), "Duplicate schema names found"

    def test_schema_descriptions_not_empty(self):
        """Test that all schemas have non-empty descriptions."""
        for schema in SAMPLE_SCHEMAS:
            assert schema["description"], f"Schema '{schema['name']}' has empty description"
            assert len(schema["description"]) > 10, f"Schema '{schema['name']}' description too short"
