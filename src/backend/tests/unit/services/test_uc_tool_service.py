"""
Unit tests for UCToolService.

Tests the functionality of Unity Catalog tools operations including
tool listing, configuration validation, error handling, and integration.
"""
import pytest
import os
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.uc_tool_service import UCToolService
from src.schemas.uc_tool import UCToolSchema, UCToolListResponse


class MockDatabricksConfig:
    """Mock Databricks configuration for testing."""
    
    def __init__(self, is_enabled=True, catalog="test_catalog", schema="test_schema", 
                 workspace_url="https://test.databricks.com", warehouse_id="test_warehouse"):
        self.is_enabled = is_enabled
        self.catalog = catalog
        self.schema = schema
        self.workspace_url = workspace_url
        self.warehouse_id = warehouse_id


class MockApiKey:
    """Mock API key for testing."""
    
    def __init__(self, name="DATABRICKS_TOKEN", value="test_token"):
        self.name = name
        self.value = value


class MockFunction:
    """Mock Unity Catalog function for testing."""
    
    def __init__(self, name="test_function", comment="Test function"):
        self.name = name
        self.comment = comment


class MockFunctionDetails:
    """Mock function details for testing."""
    
    def __init__(self, return_type="string", input_params=None):
        self.return_type = return_type
        self.input_params = input_params or []


class MockParameter:
    """Mock function parameter for testing."""
    
    def __init__(self, name="param1", param_type="string", nullable=False):
        self.name = name
        self.type = MagicMock()
        # Mock the get method to return param_type for 'type_name' key
        self.type.get = MagicMock(side_effect=lambda key, default='unknown': param_type if key == 'type_name' else default)
        self.type.nullable = nullable


@pytest.fixture
def mock_session():
    """Create a mock AsyncSession."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def uc_tool_service(mock_session):
    """Create a UCToolService with mocked dependencies."""
    return UCToolService(session=mock_session)


@pytest.fixture
def mock_databricks_config():
    """Create a mock Databricks configuration."""
    return MockDatabricksConfig()


@pytest.fixture
def mock_api_key():
    """Create a mock API key."""
    return MockApiKey()


class TestUCToolService:
    """Test cases for UCToolService."""
    
    def test_uc_tool_service_initialization(self, mock_session):
        """Test UCToolService initialization."""
        service = UCToolService(session=mock_session)
        
        assert service.session == mock_session
    
    @pytest.mark.asyncio
    async def test_check_databricks_token_exists_regular_token(self, uc_tool_service, mock_session):
        """Test checking for regular DATABRICKS_TOKEN existence."""
        # Mock query results
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = MockApiKey("DATABRICKS_TOKEN")
        mock_session.execute.return_value = mock_result
        
        result = await uc_tool_service._check_databricks_token_exists()
        
        assert result is True
        assert mock_session.execute.call_count == 2  # Called for both token types
    
    @pytest.mark.asyncio
    async def test_check_databricks_token_exists_personal_token(self, uc_tool_service, mock_session):
        """Test checking for DATABRICKS_PERSONAL_ACCESS_TOKEN existence."""
        # Mock query results - first call returns None, second returns token
        mock_result_none = MagicMock()
        mock_result_none.scalars.return_value.first.return_value = None
        
        mock_result_token = MagicMock()
        mock_result_token.scalars.return_value.first.return_value = MockApiKey("DATABRICKS_PERSONAL_ACCESS_TOKEN")
        
        mock_session.execute.side_effect = [mock_result_none, mock_result_token]
        
        result = await uc_tool_service._check_databricks_token_exists()
        
        assert result is True
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_check_databricks_token_exists_no_tokens(self, uc_tool_service, mock_session):
        """Test checking for tokens when none exist."""
        # Mock query results - both return None
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result
        
        result = await uc_tool_service._check_databricks_token_exists()
        
        assert result is False
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_success(self, uc_tool_service, mock_databricks_config):
        """Test successful retrieval of UC tools."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client
            mock_client_instance = MagicMock()
            mock_function = MockFunction("test_func", "Test function")
            mock_client_instance.list_functions.return_value = [mock_function]
            
            mock_param = MockParameter("param1", "string", False)
            mock_details = MockFunctionDetails("string", [mock_param])
            mock_client_instance.get_function_details.return_value = mock_details
            mock_client.return_value = mock_client_instance
            
            result = await uc_tool_service.get_all_uc_tools()
            
            assert isinstance(result, UCToolListResponse)
            assert result.count == 1
            assert len(result.tools) == 1
            assert result.tools[0].name == "test_func"
            assert result.tools[0].full_name == "test_catalog.test_schema.test_func"
            assert result.tools[0].catalog == "test_catalog"
            assert result.tools[0].db_schema == "test_schema"
            assert len(result.tools[0].input_params) == 1
            assert result.tools[0].input_params[0].name == "param1"
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_no_config(self, uc_tool_service):
        """Test UC tools retrieval when no configuration exists."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = None
            mock_repo.return_value = mock_repo_instance
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 400
            assert "configuration not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_databricks_disabled(self, uc_tool_service):
        """Test UC tools retrieval when Databricks is disabled."""
        disabled_config = MockDatabricksConfig(is_enabled=False)
        
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo:
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = disabled_config
            mock_repo.return_value = mock_repo_instance
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 400
            assert "integration is disabled" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_no_token(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval when no token exists."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=False):
            
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 400
            assert "No Databricks token found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_token_setup_failed(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval when token setup fails."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service:
            
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = False
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 400
            assert "Failed to set up Databricks token" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_no_token_after_setup(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval when no token is available after setup."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch.dict(os.environ, {}, clear=True):  # Clear environment variables
            
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 400
            assert "token is not available after setup" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_no_workspace_url(self, uc_tool_service):
        """Test UC tools retrieval when workspace URL is missing."""
        config_no_url = MockDatabricksConfig(workspace_url="")
        
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = config_no_url
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 400
            assert "workspace URL is not provided" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_no_functions(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval when no functions are found."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client with no functions
            mock_client_instance = MagicMock()
            mock_client_instance.list_functions.return_value = []
            mock_client.return_value = mock_client_instance
            
            result = await uc_tool_service.get_all_uc_tools()
            
            assert isinstance(result, UCToolListResponse)
            assert result.count == 0
            assert len(result.tools) == 0
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_function_processing_error(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval when function processing fails."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client
            mock_client_instance = MagicMock()
            mock_function = MockFunction("test_func")
            mock_client_instance.list_functions.return_value = [mock_function]
            mock_client_instance.get_function_details.side_effect = Exception("Details error")
            mock_client.return_value = mock_client_instance
            
            result = await uc_tool_service.get_all_uc_tools()
            
            # Should continue processing despite error
            assert isinstance(result, UCToolListResponse)
            assert result.count == 0  # Function was skipped due to error
            assert len(result.tools) == 0
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_databricks_sdk_missing(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval with Databricks SDK missing error."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client to raise specific error
            mock_client.side_effect = Exception("ModuleNotFoundError: No module named 'databricks'")
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 500
            assert "Databricks SDK is not installed" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_authentication_error(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval with authentication error."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client to raise authentication error
            mock_client_instance = MagicMock()
            mock_client_instance.list_functions.side_effect = Exception("Unauthorized")
            mock_client.return_value = mock_client_instance
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 500
            assert "Authentication failed" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_connection_error(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval with connection error."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client to raise connection error
            mock_client_instance = MagicMock()
            mock_client_instance.list_functions.side_effect = Exception("ConnectionError")
            mock_client.return_value = mock_client_instance
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 500
            assert "Failed to connect to Databricks" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_config_key_error(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval with configuration key error."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client to raise key error
            mock_client_instance = MagicMock()
            mock_client_instance.list_functions.side_effect = KeyError("No such key")
            mock_client.return_value = mock_client_instance
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 500
            assert "Missing key in configuration" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_uc_tools_generic_error(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval with generic error."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client to raise generic error
            mock_client_instance = MagicMock()
            mock_client_instance.list_functions.side_effect = Exception("Generic error")
            mock_client.return_value = mock_client_instance
            
            with pytest.raises(HTTPException) as exc_info:
                await uc_tool_service.get_all_uc_tools()
            
            assert exc_info.value.status_code == 500
            assert "Generic error" in str(exc_info.value.detail)
    
    def test_service_attributes(self, uc_tool_service, mock_session):
        """Test that service has correct attributes."""
        assert hasattr(uc_tool_service, 'session')
        assert uc_tool_service.session == mock_session
    
    @pytest.mark.asyncio
    async def test_function_with_complex_parameters(self, uc_tool_service, mock_databricks_config):
        """Test UC tools retrieval with complex function parameters."""
        with patch('src.services.uc_tool_service.DatabricksConfigRepository') as mock_repo, \
             patch.object(uc_tool_service, '_check_databricks_token_exists', return_value=True), \
             patch('src.services.uc_tool_service.UnitOfWork') as mock_uow, \
             patch('src.services.uc_tool_service.DatabricksService') as mock_db_service, \
             patch('src.services.uc_tool_service.UCClient') as mock_client, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}):
            
            # Setup mocks
            mock_repo_instance = AsyncMock()
            mock_repo_instance.get_active_config.return_value = mock_databricks_config
            mock_repo.return_value = mock_repo_instance
            
            mock_uow_instance = MagicMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            mock_db_service_instance = AsyncMock()
            mock_db_service_instance.setup_token.return_value = True
            mock_db_service.from_unit_of_work = AsyncMock(return_value=mock_db_service_instance)
            
            # Mock UC client with complex parameters
            mock_client_instance = MagicMock()
            mock_function = MockFunction("complex_func", "Function with complex params")
            mock_client_instance.list_functions.return_value = [mock_function]
            
            # Create multiple parameters with different types
            param1 = MockParameter("required_param", "string", False)
            param2 = MockParameter("optional_param", "int", True)
            param3 = MockParameter("array_param", "array<string>", False)
            
            mock_details = MockFunctionDetails("map<string,string>", [param1, param2, param3])
            mock_client_instance.get_function_details.return_value = mock_details
            mock_client.return_value = mock_client_instance
            
            result = await uc_tool_service.get_all_uc_tools()
            
            assert isinstance(result, UCToolListResponse)
            assert result.count == 1
            assert len(result.tools) == 1
            tool = result.tools[0]
            assert tool.name == "complex_func"
            assert tool.return_type == "map<string,string>"
            assert len(tool.input_params) == 3
            
            # Check parameter details
            params = tool.input_params
            assert params[0].name == "required_param"
            assert params[0].type == "string"
            assert params[0].required is True
            
            assert params[1].name == "optional_param"
            assert params[1].type == "int"
            assert params[1].required is False
            
            assert params[2].name == "array_param"
            assert params[2].type == "array<string>"
            assert params[2].required is True