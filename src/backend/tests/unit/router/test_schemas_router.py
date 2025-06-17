"""
Unit tests for SchemasRouter.

Tests the functionality of schema management endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi.testclient import TestClient


@pytest.fixture
def app():
    """Create a FastAPI app."""
    from fastapi import FastAPI
    from src.api.schemas_router import router
    
    app = FastAPI()
    app.include_router(router)
    
    return app



@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    from src.models.enums import UserRole, UserStatus
    from datetime import datetime
    
    class MockUser:
        def __init__(self):
            self.id = "current-user-123"
            self.username = "testuser"
            self.email = "test@example.com"
            self.role = UserRole.REGULAR
            self.status = UserStatus.ACTIVE
            self.created_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()
    
    return MockUser()


@pytest.fixture
def client(app, mock_current_user):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user

    return TestClient(app)


class TestSchemasRouter:
    """Test cases for schema management endpoints."""
    
    @patch('src.api.schemas_router.SchemaService')
    def test_get_schemas_success(self, mock_service_class, client):
        """Test successful schemas retrieval."""
        from datetime import datetime
        from unittest.mock import MagicMock
        
        # Create a mock response object with the right attributes
        mock_response = MagicMock()
        mock_response.count = 2
        mock_response.schemas = [
            {
                "id": 1,
                "name": "user_schema",
                "description": "Schema for user data",
                "schema_type": "data_model",
                "schema_definition": {"type": "object"},
                "field_descriptions": {},
                "keywords": ["user"],
                "tools": [],
                "example_data": None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "id": 2,
                "name": "task_schema", 
                "description": "Schema for task data",
                "schema_type": "data_model",
                "schema_definition": {"type": "object"},
                "field_descriptions": {},
                "keywords": ["task"],
                "tools": [],
                "example_data": None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        ]
        
        async def mock_get_all_schemas():
            return mock_response
        
        mock_service_class.get_all_schemas = mock_get_all_schemas
        
        response = client.get("/schemas")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["schemas"]) == 2
        assert data["schemas"][0]["name"] == "user_schema"
    
    @patch('src.api.schemas_router.SchemaService')
    def test_create_schema_success(self, mock_service_class, client):
        """Test successful schema creation."""
        from datetime import datetime
        async def mock_create_schema(schema_data):
            from unittest.mock import MagicMock
            mock_schema = MagicMock()
            mock_schema.name = "new_schema"
            mock_schema.id = 3
            mock_schema.description = "A new schema"
            mock_schema.schema_type = "data_model"
            mock_schema.schema_definition = {"type": "object", "properties": {"field1": {"type": "string"}}}
            mock_schema.field_descriptions = {"field1": "A string field"}
            mock_schema.keywords = ["new"]
            mock_schema.tools = []
            mock_schema.example_data = {"field1": "example"}
            mock_schema.created_at = datetime.now()
            mock_schema.updated_at = datetime.now()
            return mock_schema
        
        mock_service_class.create_schema = mock_create_schema
        
        schema_data = {
            "name": "new_schema",
            "description": "A new schema",
            "schema_type": "data_model",
            "schema_definition": {"type": "object", "properties": {"field1": {"type": "string"}}},
            "field_descriptions": {"field1": "A string field"},
            "keywords": ["new"],
            "tools": [],
            "example_data": {"field1": "example"}
        }
        
        response = client.post("/schemas", json=schema_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "new_schema"
    
    @patch('src.api.schemas_router.SchemaService')
    def test_get_schema_by_name_success(self, mock_service_class, client):
        """Test successful schema retrieval by name."""
        from datetime import datetime
        async def mock_get_schema_by_name(schema_name):
            from unittest.mock import MagicMock
            mock_schema = MagicMock()
            mock_schema.name = "user_schema"
            mock_schema.id = 1
            mock_schema.description = "Schema for user data"
            mock_schema.schema_type = "data_model"
            mock_schema.schema_definition = {"type": "object"}
            mock_schema.field_descriptions = {}
            mock_schema.keywords = ["user"]
            mock_schema.tools = []
            mock_schema.example_data = None
            mock_schema.created_at = datetime.now()
            mock_schema.updated_at = datetime.now()
            return mock_schema
        
        mock_service_class.get_schema_by_name = mock_get_schema_by_name
        
        response = client.get("/schemas/user_schema")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "user_schema"
    
    @patch('src.api.schemas_router.SchemaService')
    def test_get_schema_by_name_not_found(self, mock_service_class, client):
        """Test schema retrieval by name when not found."""
        from fastapi import HTTPException
        
        async def mock_get_schema_by_name(schema_name):
            raise HTTPException(status_code=404, detail="Schema not found")
        
        mock_service_class.get_schema_by_name = mock_get_schema_by_name
        
        response = client.get("/schemas/nonexistent_schema")
        
        assert response.status_code == 404
        data = response.json()
        assert "Schema not found" in data["detail"]
    
    @patch('src.api.schemas_router.SchemaService')
    def test_get_schemas_by_type_success(self, mock_service_class, client):
        """Test successful schemas retrieval by type."""
        from datetime import datetime
        from unittest.mock import MagicMock
        
        # Create a mock response object
        mock_response = MagicMock()
        mock_response.count = 1
        mock_response.schemas = [
            {
                "id": 1,
                "name": "user_schema",
                "description": "Schema for user data",
                "schema_type": "data_model",
                "schema_definition": {"type": "object"},
                "field_descriptions": {},
                "keywords": ["user"],
                "tools": [],
                "example_data": None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        ]
        
        async def mock_get_schemas_by_type(schema_type):
            return mock_response
        
        mock_service_class.get_schemas_by_type = mock_get_schemas_by_type
        
        response = client.get("/schemas/by-type/data_model")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert len(data["schemas"]) == 1
        assert data["schemas"][0]["schema_type"] == "data_model"
    
    @patch('src.api.schemas_router.SchemaService')
    def test_create_schema_failure(self, mock_service_class, client):
        """Test schema creation failure."""
        from fastapi import HTTPException
        
        async def mock_create_schema(schema_data):
            raise HTTPException(status_code=400, detail="Schema already exists")
        
        mock_service_class.create_schema = mock_create_schema
        
        schema_data = {
            "name": "existing_schema",
            "description": "A schema that already exists",
            "schema_type": "data_model",
            "schema_definition": {"type": "object"},
            "field_descriptions": {},
            "keywords": [],
            "tools": [],
            "example_data": None
        }
        
        response = client.post("/schemas", json=schema_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "Schema already exists" in data["detail"]
    
    @patch('src.api.schemas_router.SchemaService')
    def test_update_schema_success(self, mock_service_class, client):
        """Test successful schema update."""
        from datetime import datetime
        
        async def mock_update_schema(schema_name, schema_data):
            from unittest.mock import MagicMock
            mock_schema = MagicMock()
            mock_schema.name = "updated_schema"
            mock_schema.id = 1
            mock_schema.description = "Updated description"
            mock_schema.schema_type = "data_model"
            mock_schema.schema_definition = {"type": "object", "properties": {"field1": {"type": "string"}}}
            mock_schema.field_descriptions = {"field1": "Updated field description"}
            mock_schema.keywords = ["updated"]
            mock_schema.tools = []
            mock_schema.example_data = {"field1": "updated_example"}
            mock_schema.created_at = datetime.now()
            mock_schema.updated_at = datetime.now()
            return mock_schema
        
        mock_service_class.update_schema = mock_update_schema
        
        update_data = {
            "description": "Updated description",
            "schema_definition": {"type": "object", "properties": {"field1": {"type": "string"}}},
            "field_descriptions": {"field1": "Updated field description"},
            "keywords": ["updated"],
            "example_data": {"field1": "updated_example"}
        }
        
        response = client.put("/schemas/updated_schema", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "updated_schema"
        assert data["description"] == "Updated description"
    
    @patch('src.api.schemas_router.SchemaService')
    def test_update_schema_failure(self, mock_service_class, client):
        """Test schema update failure."""
        from fastapi import HTTPException
        
        async def mock_update_schema(schema_name, schema_data):
            raise HTTPException(status_code=404, detail="Schema not found")
        
        mock_service_class.update_schema = mock_update_schema
        
        update_data = {
            "description": "Updated description"
        }
        
        response = client.put("/schemas/nonexistent_schema", json=update_data)
        
        assert response.status_code == 404
        data = response.json()
        assert "Schema not found" in data["detail"]
    
    @patch('src.api.schemas_router.SchemaService')
    def test_delete_schema_success(self, mock_service_class, client):
        """Test successful schema deletion."""
        async def mock_delete_schema(schema_name):
            return None  # Successful deletion returns None
        
        mock_service_class.delete_schema = mock_delete_schema
        
        response = client.delete("/schemas/test_schema")
        
        assert response.status_code == 204
        assert response.content == b""  # No content for 204 status
    
    @patch('src.api.schemas_router.SchemaService')
    def test_delete_schema_failure(self, mock_service_class, client):
        """Test schema deletion failure."""
        from fastapi import HTTPException
        
        async def mock_delete_schema(schema_name):
            raise HTTPException(status_code=404, detail="Schema not found")
        
        mock_service_class.delete_schema = mock_delete_schema
        
        response = client.delete("/schemas/nonexistent_schema")
        
        assert response.status_code == 404
        data = response.json()
        assert "Schema not found" in data["detail"]
    
    @patch('src.api.schemas_router.SchemaService')
    def test_get_schemas_by_type_empty_result(self, mock_service_class, client):
        """Test schemas retrieval by type with empty result."""
        from unittest.mock import MagicMock
        
        # Create a mock response object with no schemas
        mock_response = MagicMock()
        mock_response.count = 0
        mock_response.schemas = []
        
        async def mock_get_schemas_by_type(schema_type):
            return mock_response
        
        mock_service_class.get_schemas_by_type = mock_get_schemas_by_type
        
        response = client.get("/schemas/by-type/nonexistent_type")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert len(data["schemas"]) == 0
    
    @patch('src.api.schemas_router.SchemaService')
    def test_get_all_schemas_empty_result(self, mock_service_class, client):
        """Test all schemas retrieval with empty result."""
        from unittest.mock import MagicMock
        
        # Create a mock response object with no schemas
        mock_response = MagicMock()
        mock_response.count = 0
        mock_response.schemas = []
        
        async def mock_get_all_schemas():
            return mock_response
        
        mock_service_class.get_all_schemas = mock_get_all_schemas
        
        response = client.get("/schemas")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert len(data["schemas"]) == 0
    
    @patch('src.api.schemas_router.SchemaService')
    def test_create_schema_with_minimal_data(self, mock_service_class, client):
        """Test schema creation with minimal required data."""
        from datetime import datetime
        
        async def mock_create_schema(schema_data):
            from unittest.mock import MagicMock
            mock_schema = MagicMock()
            mock_schema.name = "minimal_schema"
            mock_schema.id = 4
            mock_schema.description = "Minimal schema"
            mock_schema.schema_type = "basic"
            mock_schema.schema_definition = {"type": "object"}
            mock_schema.field_descriptions = {}
            mock_schema.keywords = []
            mock_schema.tools = []
            mock_schema.example_data = None
            mock_schema.created_at = datetime.now()
            mock_schema.updated_at = datetime.now()
            return mock_schema
        
        mock_service_class.create_schema = mock_create_schema
        
        schema_data = {
            "name": "minimal_schema",
            "description": "Minimal schema",
            "schema_type": "basic",
            "schema_definition": {"type": "object"}
        }
        
        response = client.post("/schemas", json=schema_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "minimal_schema"
    
    @patch('src.api.schemas_router.SchemaService')
    def test_update_schema_with_minimal_data(self, mock_service_class, client):
        """Test schema update with minimal data."""
        from datetime import datetime
        
        async def mock_update_schema(schema_name, schema_data):
            from unittest.mock import MagicMock
            mock_schema = MagicMock()
            mock_schema.name = "minimal_updated"
            mock_schema.id = 1
            mock_schema.description = "Minimally updated"
            mock_schema.schema_type = "basic"
            mock_schema.schema_definition = {"type": "object"}
            mock_schema.field_descriptions = {}
            mock_schema.keywords = []
            mock_schema.tools = []
            mock_schema.example_data = None
            mock_schema.created_at = datetime.now()
            mock_schema.updated_at = datetime.now()
            return mock_schema
        
        mock_service_class.update_schema = mock_update_schema
        
        update_data = {
            "description": "Minimally updated"
        }
        
        response = client.put("/schemas/minimal_updated", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "minimal_updated"
        assert data["description"] == "Minimally updated"