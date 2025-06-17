"""
Unit tests for schemas seeding module.

Tests the functionality of seeding schema definitions into the database.
"""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from datetime import datetime
from src.seeds.schemas import (
    SAMPLE_SCHEMAS,
    seed_async,
    seed_sync,
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
def mock_sync_session():
    """Create mock sync database session."""
    session = MagicMock()
    session.execute = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.add = MagicMock()
    return session


@pytest.fixture
def mock_schema_model():
    """Create mock Schema model."""
    schema = MagicMock()
    schema.name = "TestSchema"
    schema.description = "Test description"
    schema.schema_type = "data_model"
    schema.schema_definition = {"type": "object"}
    schema.field_descriptions = {}
    schema.keywords = []
    schema.tools = []
    schema.example_data = {}
    schema.created_at = datetime.now()
    schema.updated_at = datetime.now()
    return schema


@pytest.fixture
def sample_schema_data():
    """Create sample schema data."""
    return {
        "name": "TestSchema",
        "description": "Test schema description",
        "schema_type": "data_model",
        "schema_definition": {
            "type": "object",
            "properties": {
                "title": {"type": "string"}
            }
        },
        "field_descriptions": {"title": "The title field"},
        "keywords": ["test", "schema"],
        "tools": ["test_tool"],
        "example_data": {"title": "Example Title"}
    }


class TestSampleSchemas:
    """Test sample schema definitions."""
    
    def test_sample_schemas_structure(self):
        """Test that SAMPLE_SCHEMAS has correct structure."""
        assert isinstance(SAMPLE_SCHEMAS, list)
        assert len(SAMPLE_SCHEMAS) > 0
        
        for schema in SAMPLE_SCHEMAS:
            assert "name" in schema
            assert "description" in schema
            assert "schema_type" in schema
            assert "schema_definition" in schema
            assert "keywords" in schema
            assert "tools" in schema
            
            # Validate schema_definition is valid JSON
            assert isinstance(schema["schema_definition"], dict)
            assert "type" in schema["schema_definition"]
    
    def test_research_report_schema(self):
        """Test ResearchReport schema definition."""
        research_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "ResearchReport"), 
            None
        )
        
        assert research_schema is not None
        assert research_schema["schema_type"] == "data_model"
        assert "findings" in research_schema["schema_definition"]["properties"]
        assert "sources" in research_schema["schema_definition"]["properties"]
        assert "recommendations" in research_schema["schema_definition"]["properties"]
    
    def test_product_requirements_schema(self):
        """Test ProductRequirements schema definition."""
        product_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "ProductRequirements"), 
            None
        )
        
        assert product_schema is not None
        assert product_schema["schema_type"] == "data_model"
        assert "features" in product_schema["schema_definition"]["properties"]
        assert "stakeholders" in product_schema["schema_definition"]["properties"]
    
    def test_web_scraping_config_schema(self):
        """Test WebScrapingConfig schema definition."""
        scraping_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "WebScrapingConfig"), 
            None
        )
        
        assert scraping_schema is not None
        assert scraping_schema["schema_type"] == "tool_config"
        assert "target_url" in scraping_schema["schema_definition"]["properties"]
        assert "elements_to_extract" in scraping_schema["schema_definition"]["properties"]
    
    def test_output_model_schemas(self):
        """Test output model schemas."""
        output_schemas = [
            s for s in SAMPLE_SCHEMAS 
            if s["schema_type"] == "output_model"
        ]
        
        assert len(output_schemas) > 0
        
        # Test specific output models
        email_schema = next(
            (s for s in output_schemas if s["name"] == "EmailContent"), 
            None
        )
        assert email_schema is not None
        
        serper_schema = next(
            (s for s in output_schemas if s["name"] == "SerperDevToolOutput"), 
            None
        )
        assert serper_schema is not None


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
        
        # Mock session factory
        async def mock_session_context(self):
            return mock_session
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context
        
        # Mock first query (get existing names) - empty result
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        # Mock second query (check for race condition) - no existing schema
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result  # First query
            else:
                return mock_check_result  # Second query
        
        mock_session.execute.side_effect = side_effect
        
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
        # Mock session factory
        async def mock_session_context(self):
            return mock_session
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context
        
        # Mock first query (get existing names) - schema exists
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sample_schema_data["name"]]
        
        # Mock second query (get existing schema for update)
        mock_update_result = MagicMock()
        mock_update_result.scalars.return_value.first.return_value = mock_schema_model
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result  # First query
            else:
                return mock_update_result  # Second query
        
        mock_session.execute.side_effect = side_effect
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()
        
        # Verify schema was updated
        assert mock_schema_model.description == sample_schema_data["description"]
        assert mock_schema_model.schema_type == sample_schema_data["schema_type"]
        mock_session.commit.assert_called()
    
    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    async def test_seed_async_race_condition(self, mock_select, mock_session_factory, mock_session, mock_schema_model, sample_schema_data):
        """Test async seeding with race condition (schema appears during processing)."""
        # Mock session factory
        async def mock_session_context(self):
            return mock_session
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context
        
        # Mock first query (get existing names) - schema doesn't exist
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        
        # Mock second query (check for race condition) - schema now exists
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = mock_schema_model
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result  # First query
            else:
                return mock_check_result  # Second query
        
        mock_session.execute.side_effect = side_effect
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()
        
        # Verify schema was updated (not added)
        assert mock_schema_model.description == sample_schema_data["description"]
        mock_session.add.assert_not_called()
        mock_session.commit.assert_called()
    
    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    async def test_seed_async_commit_error(self, mock_select, mock_session_factory, mock_session, sample_schema_data):
        """Test async seeding with commit error."""
        # Mock session factory
        async def mock_session_context(self):
            return mock_session
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context
        
        # Mock queries
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result
            else:
                return mock_check_result
        
        mock_session.execute.side_effect = side_effect
        
        # Mock commit error
        mock_session.commit.side_effect = Exception("UNIQUE constraint failed")
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()
        
        # Verify rollback was called
        mock_session.rollback.assert_called()
    
    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    async def test_seed_async_general_error(self, mock_select, mock_session_factory, mock_session, sample_schema_data):
        """Test async seeding with general error."""
        # Mock session factory to raise error on session creation
        async def mock_session_context_error(self):
            raise Exception("Database connection error")
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context_error
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            # Should raise exception when initial connection fails
            with pytest.raises(Exception, match="Database connection error"):
                await seed_async()


class TestSyncSeeding:
    """Test sync schema seeding functionality."""
    
    @patch('src.seeds.schemas.SessionLocal')
    @patch('src.seeds.schemas.select')
    @patch('src.seeds.schemas.Schema')
    @patch('src.seeds.schemas.datetime')
    def test_seed_sync_new_schemas(self, mock_datetime, mock_schema_class, mock_select, mock_session_local, mock_sync_session, sample_schema_data):
        """Test sync seeding with new schemas."""
        # Mock datetime
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock session factory
        mock_session_local.return_value.__enter__.return_value = mock_sync_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock first query (get existing names) - empty result
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_sync_session.execute.return_value = mock_result
        
        # Mock second query (check for race condition) - no existing schema
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result  # First query
            else:
                return mock_check_result  # Second query
        
        mock_sync_session.execute.side_effect = side_effect
        
        # Mock Schema model creation
        mock_schema_instance = MagicMock()
        mock_schema_class.return_value = mock_schema_instance
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            seed_sync()
        
        # Verify schema was added
        mock_sync_session.add.assert_called_once_with(mock_schema_instance)
        mock_sync_session.commit.assert_called()
    
    @patch('src.seeds.schemas.SessionLocal')
    @patch('src.seeds.schemas.select')
    def test_seed_sync_existing_schemas(self, mock_select, mock_session_local, mock_sync_session, mock_schema_model, sample_schema_data):
        """Test sync seeding with existing schemas."""
        # Mock session factory
        mock_session_local.return_value.__enter__.return_value = mock_sync_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock first query (get existing names) - schema exists
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [sample_schema_data["name"]]
        
        # Mock second query (get existing schema for update)
        mock_update_result = MagicMock()
        mock_update_result.scalars.return_value.first.return_value = mock_schema_model
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result  # First query
            else:
                return mock_update_result  # Second query
        
        mock_sync_session.execute.side_effect = side_effect
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            seed_sync()
        
        # Verify schema was updated
        assert mock_schema_model.description == sample_schema_data["description"]
        assert mock_schema_model.schema_type == sample_schema_data["schema_type"]
        mock_sync_session.commit.assert_called()
    
    @patch('src.seeds.schemas.SessionLocal')
    @patch('src.seeds.schemas.select')
    def test_seed_sync_commit_error(self, mock_select, mock_session_local, mock_sync_session, sample_schema_data):
        """Test sync seeding with commit error."""
        # Mock session factory
        mock_session_local.return_value.__enter__.return_value = mock_sync_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock queries
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result
            else:
                return mock_check_result
        
        mock_sync_session.execute.side_effect = side_effect
        
        # Mock commit error
        mock_sync_session.commit.side_effect = Exception("UNIQUE constraint failed")
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            seed_sync()
        
        # Verify rollback was called
        mock_sync_session.rollback.assert_called()


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
        required_fields = ["name", "description", "schema_type", "schema_definition", "keywords", "tools"]
        
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
            
            # If has properties, they should be valid
            if "properties" in schema_def:
                assert isinstance(schema_def["properties"], dict)
                
                for prop_name, prop_def in schema_def["properties"].items():
                    # Skip properties that use $ref (valid in JSON schema)
                    if "$ref" not in prop_def:
                        assert "type" in prop_def, f"Property '{prop_name}' in schema '{schema['name']}' missing type"
    
    def test_schema_types_are_valid(self):
        """Test that schema types are from allowed values."""
        allowed_types = ["data_model", "tool_config", "output_model"]
        
        for schema in SAMPLE_SCHEMAS:
            assert schema["schema_type"] in allowed_types, f"Schema '{schema['name']}' has invalid type '{schema['schema_type']}'"
    
    def test_keywords_and_tools_are_lists(self):
        """Test that keywords and tools are lists."""
        for schema in SAMPLE_SCHEMAS:
            assert isinstance(schema["keywords"], list), f"Schema '{schema['name']}' keywords must be a list"
            assert isinstance(schema["tools"], list), f"Schema '{schema['name']}' tools must be a list"
    
    def test_example_data_when_present(self):
        """Test example data validity when present."""
        for schema in SAMPLE_SCHEMAS:
            if "example_data" in schema and schema["example_data"]:
                # Example data should be a dict
                assert isinstance(schema["example_data"], dict), f"Schema '{schema['name']}' example_data must be a dict"
                
                # For data_model schemas, example should match some properties
                if schema["schema_type"] == "data_model" and "properties" in schema["schema_definition"]:
                    schema_props = schema["schema_definition"]["properties"]
                    example_data = schema["example_data"]
                    
                    # At least some properties should be present in example
                    common_props = set(schema_props.keys()) & set(example_data.keys())
                    assert len(common_props) > 0, f"Schema '{schema['name']}' example_data should contain some schema properties"


class TestSpecificSchemas:
    """Test specific schema implementations."""
    
    def test_genie_output_schema(self):
        """Test Databricks Genie output schema."""
        genie_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "GenieOutput"),
            None
        )
        
        assert genie_schema is not None
        assert genie_schema["schema_type"] == "output_model"
        
        # Check required structure
        props = genie_schema["schema_definition"]["properties"]
        assert "conversation" in props
        assert "message" in props
        
        # Check conversation properties
        conv_props = props["conversation"]["properties"]
        assert "id" in conv_props
        assert "space_id" in conv_props
        assert "user_id" in conv_props
        
        # Check example data
        assert "example_data" in genie_schema
        example = genie_schema["example_data"]
        assert "conversation" in example
        assert "message" in example
    
    def test_python_pptx_schema(self):
        """Test Python PPTX schema."""
        pptx_schema = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "PythonPPTX"),
            None
        )
        
        assert pptx_schema is not None
        assert pptx_schema["schema_type"] == "data_model"
        
        # Check required structure
        props = pptx_schema["schema_definition"]["properties"]
        assert "title" in props
        assert "slides" in props
        
        # Check slides array structure
        slides_def = props["slides"]
        assert slides_def["type"] == "array"
        assert "items" in slides_def
        
        slide_props = slides_def["items"]["properties"]
        assert "title" in slide_props
        assert "content" in slide_props
        assert "bullet_points" in slide_props
    
    def test_arxiv_schemas(self):
        """Test arXiv related schemas."""
        arxiv_paper = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "ArxivPaper"),
            None
        )
        arxiv_search = next(
            (s for s in SAMPLE_SCHEMAS if s["name"] == "ArxivSearchResult"),
            None
        )
        
        assert arxiv_paper is not None
        assert arxiv_search is not None
        
        # Check paper schema
        paper_props = arxiv_paper["schema_definition"]["properties"]
        assert "id" in paper_props
        assert "title" in paper_props
        assert "abstract" in paper_props
        assert "authors" in paper_props
        
        # Check search result schema
        search_props = arxiv_search["schema_definition"]["properties"]
        assert "papers" in search_props
        assert "total_results" in search_props

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    @patch('src.seeds.schemas.Schema')
    @patch('src.seeds.schemas.datetime')
    async def test_seed_async_update_existing_schema(self, mock_datetime, mock_schema_class, mock_select, mock_session_factory, mock_session, sample_schema_data):
        """Test async seeding with updating existing schemas."""
        # Mock datetime
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock session factory
        async def mock_session_context(self):
            return mock_session
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context
        mock_context.__aexit__ = AsyncMock(return_value=None)
        # Mock session factory to return the same session for all calls
        mock_session_factory.return_value = mock_context
        
        # Mock first query (get existing names) - schema exists
        mock_result = MagicMock()
        # Mock scalars().all() to return rows where row[0] is schema name
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [(sample_schema_data["name"],)]  # This becomes existing_names = {schema_name}
        mock_result.scalars.return_value = mock_scalars
        
        # Mock existing schema for update
        existing_schema = Mock()
        existing_schema.description = "old description"
        existing_schema.schema_type = "old_type"
        existing_schema.schema_definition = {"old": "definition"}
        existing_schema.field_descriptions = {}
        existing_schema.keywords = []
        existing_schema.tools = []
        existing_schema.example_data = {}
        existing_schema.updated_at = datetime.now()
        
        mock_update_result = MagicMock()
        mock_update_result.scalars.return_value.first.return_value = existing_schema
        
        # Set up call counting to handle different query calls
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_result  # First query for existing names
            else:
                return mock_update_result  # Update query
        
        mock_session.execute.side_effect = side_effect
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()
        
        # Verify schema was updated
        assert existing_schema.description == sample_schema_data["description"]
        assert existing_schema.schema_type == sample_schema_data["schema_type"]
        assert existing_schema.schema_definition == sample_schema_data["schema_definition"]
        mock_session.commit.assert_called()

    @patch('src.seeds.schemas.SessionLocal')
    @patch('src.seeds.schemas.select')
    @patch('src.seeds.schemas.Schema')
    @patch('src.seeds.schemas.datetime')
    def test_seed_sync_update_existing_schema(self, mock_datetime, mock_schema_class, mock_select, mock_session_local, mock_sync_session, sample_schema_data):
        """Test sync seeding with updating existing schemas."""
        # Mock datetime
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock session factory
        mock_session_local.return_value.__enter__.return_value = mock_sync_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock first query (get existing names) - schema exists
        mock_result = MagicMock()
        # Mock scalars().all() to return rows where row[0] is schema name
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [(sample_schema_data["name"],)]  # This becomes existing_names = {schema_name}
        mock_result.scalars.return_value = mock_scalars
        
        # Mock existing schema for update
        existing_schema = Mock()
        existing_schema.description = "old description"
        existing_schema.schema_type = "old_type"
        existing_schema.schema_definition = {"old": "definition"}
        existing_schema.field_descriptions = {}
        existing_schema.keywords = []
        existing_schema.tools = []
        existing_schema.example_data = {}
        existing_schema.updated_at = datetime.now()
        
        mock_update_result = MagicMock()
        mock_update_result.scalars.return_value.first.return_value = existing_schema
        
        # Set up call counting to handle different query calls
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_result  # First query for existing names
            else:
                return mock_update_result  # Update query
        
        mock_sync_session.execute.side_effect = side_effect
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            seed_sync()
        
        # Verify schema was updated
        assert existing_schema.description == sample_schema_data["description"]
        assert existing_schema.schema_type == sample_schema_data["schema_type"]
        assert existing_schema.schema_definition == sample_schema_data["schema_definition"]
        mock_sync_session.commit.assert_called()

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    async def test_seed_async_commit_error_non_unique(self, mock_select, mock_session_factory, mock_session, sample_schema_data):
        """Test async seeding with non-unique constraint commit error."""
        # Mock session factory
        async def mock_session_context(self):
            return mock_session
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context
        
        # Mock queries
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result
            else:
                return mock_check_result
        
        mock_session.execute.side_effect = side_effect
        
        # Mock commit error - non-unique constraint
        mock_session.commit.side_effect = Exception("Foreign key constraint failed")
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()
        
        # Verify rollback was called
        mock_session.rollback.assert_called()

    @patch('src.seeds.schemas.SessionLocal')
    @patch('src.seeds.schemas.select')
    def test_seed_sync_commit_error_non_unique(self, mock_select, mock_session_local, mock_sync_session, sample_schema_data):
        """Test sync seeding with non-unique constraint commit error."""
        # Mock session factory
        mock_session_local.return_value.__enter__.return_value = mock_sync_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock queries
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        def side_effect(*args, **kwargs):
            if "Schema.name" in str(args[0]):
                return mock_result
            else:
                return mock_check_result
        
        mock_sync_session.execute.side_effect = side_effect
        
        # Mock commit error - non-unique constraint
        mock_sync_session.commit.side_effect = Exception("Foreign key constraint failed")
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            seed_sync()
        
        # Verify rollback was called
        mock_sync_session.rollback.assert_called()

    def test_main_module_execution(self):
        """Test __main__ block execution."""
        # Test the __main__ block execution by running the module as __main__
        # Note: The __main__ block in schemas.py calls asyncio.run(seed()) TWICE
        import runpy
        import sys
        from unittest.mock import patch
        
        with patch('src.seeds.schemas.seed', new_callable=AsyncMock) as mock_seed:
            with patch('asyncio.run') as mock_asyncio_run:
                # Temporarily modify sys.argv to simulate command line execution
                original_argv = sys.argv[:]
                try:
                    sys.argv = ['src/seeds/schemas.py']
                    
                    # Run the module as __main__ which will execute the __main__ block
                    runpy.run_module('src.seeds.schemas', run_name='__main__')
                    
                    # Verify that asyncio.run was called twice (as per the __main__ block)
                    assert mock_asyncio_run.call_count == 2
                    
                finally:
                    sys.argv = original_argv

    @pytest.mark.asyncio
    @patch('src.seeds.schemas.async_session_factory')
    @patch('src.seeds.schemas.select')
    async def test_seed_async_general_processing_error(self, mock_select, mock_session_factory, mock_session, sample_schema_data):
        """Test async seeding with general processing error."""
        # Mock session factory
        async def mock_session_context(self):
            return mock_session
        mock_context = Mock()
        mock_context.__aenter__ = mock_session_context
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_context
        
        # Mock initial query success
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []  # No existing schemas
        mock_result.scalars.return_value = mock_scalars
        
        # Mock execute to raise exception during schema processing
        call_count = [0]
        async def mock_execute(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_result  # First call succeeds
            else:
                # Second call (schema processing) raises exception
                raise Exception("Schema processing error")
        
        mock_session.execute.side_effect = mock_execute
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            await seed_async()
        
        # Should have handled the error gracefully

    @patch('src.seeds.schemas.SessionLocal')
    @patch('src.seeds.schemas.select')
    def test_seed_sync_general_processing_error(self, mock_select, mock_session_local, mock_sync_session, sample_schema_data):
        """Test sync seeding with general processing error."""
        # Mock session factory
        mock_session_local.return_value.__enter__.return_value = mock_sync_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock initial query success
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []  # No existing schemas
        mock_result.scalars.return_value = mock_scalars
        
        # Mock execute to raise exception during schema processing
        call_count = [0]
        def mock_execute(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_result  # First call succeeds
            else:
                # Second call (schema processing) raises exception
                raise Exception("Schema processing error")
        
        mock_sync_session.execute.side_effect = mock_execute
        
        # Patch SAMPLE_SCHEMAS with our test data
        with patch('src.seeds.schemas.SAMPLE_SCHEMAS', [sample_schema_data]):
            seed_sync()
        
        # Should have handled the error gracefully