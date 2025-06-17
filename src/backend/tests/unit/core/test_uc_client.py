"""
Unit tests for uc_client module.

Tests the functionality of the Unity Catalog client including
initialization, function listing, and mock mode operations.
"""
import pytest
import os
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import List

from src.core.uc_client import UCClient, MockFunction
from unittest.mock import AsyncMock


class TestMockFunction:
    """Test cases for MockFunction class."""
    
    def test_mock_function_creation(self):
        """Test MockFunction creation with name and comment."""
        # Act
        func = MockFunction("test_function", "Test comment")
        
        # Assert
        assert func.name == "test_function"
        assert func.comment == "Test comment"
        assert func.return_type == "string"
        assert func.input_params == []
    
    def test_mock_function_creation_without_comment(self):
        """Test MockFunction creation without comment."""
        # Act
        func = MockFunction("test_function")
        
        # Assert
        assert func.name == "test_function"
        assert func.comment is None
        assert func.return_type == "string"
        assert func.input_params == []


class TestUCClient:
    """Test cases for UCClient class."""
    
    def setup_method(self):
        """Set up test environment before each test."""
        # Clear environment variables
        if 'UC_MOCK_MODE' in os.environ:
            del os.environ['UC_MOCK_MODE']
        if 'DATABRICKS_HOST' in os.environ:
            del os.environ['DATABRICKS_HOST']
        if 'DATABRICKS_TOKEN' in os.environ:
            del os.environ['DATABRICKS_TOKEN']
    
    def test_init_mock_mode_explicit(self):
        """Test UCClient initialization with explicit mock mode."""
        # Act
        client = UCClient(mock_mode=True)
        
        # Assert
        assert client.mock_mode is True
        assert not hasattr(client, 'client')
    
    @patch.dict(os.environ, {'UC_MOCK_MODE': 'true'})
    def test_init_mock_mode_env_var(self):
        """Test UCClient initialization with mock mode from environment."""
        # Act
        client = UCClient()
        
        # Assert
        assert client.mock_mode is True
        assert not hasattr(client, 'client')
    
    @patch.dict(os.environ, {'UC_MOCK_MODE': 'false'})
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_init_live_mode(self, mock_init_client):
        """Test UCClient initialization in live mode."""
        # Arrange
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        
        # Act
        client = UCClient()
        
        # Assert
        assert client.mock_mode is False
        assert client.client == mock_client
        mock_init_client.assert_called_once_with(None, None)
    
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_init_with_host_and_token(self, mock_init_client):
        """Test UCClient initialization with provided host and token."""
        # Arrange
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        host = "https://test.databricks.com"
        token = "test-token"
        
        # Act
        client = UCClient(host=host, token=token)
        
        # Assert
        assert client.mock_mode is False
        assert client.client == mock_client
        mock_init_client.assert_called_once_with(host, token)
    
    @patch('src.core.uc_client.WorkspaceClient')
    def test_initialize_uc_client_with_params(self, mock_workspace_client):
        """Test initialize_uc_client with provided host and token."""
        # Arrange
        mock_client = MagicMock()
        mock_workspace_client.return_value = mock_client
        host = "https://test.databricks.com"
        token = "test-token"
        client = UCClient(mock_mode=True)  # Start in mock mode to avoid auto-init
        
        # Act
        result = client.initialize_uc_client(host, token)
        
        # Assert
        assert result == mock_client
        mock_workspace_client.assert_called_once_with(
            host=host, 
            token=token, 
            auth_type="pat"
        )
    
    @patch.dict(os.environ, {'DATABRICKS_TOKEN': 'env-token'})
    @patch('src.core.uc_client.SessionLocal')
    @patch('src.core.uc_client.get_databricks_config', new_callable=lambda: MagicMock())
    @patch('src.core.uc_client.setup_databricks_token', new_callable=lambda: MagicMock())
    @patch('src.core.uc_client.WorkspaceClient')
    def test_initialize_uc_client_from_database(self, mock_workspace_client, 
                                               mock_setup_token, mock_get_config, 
                                               mock_session_local):
        """Test initialize_uc_client using database configuration."""
        # Arrange
        mock_db = MagicMock()
        mock_session_local.return_value = mock_db
        
        mock_config = MagicMock()
        mock_config.workspace_url = "https://db.databricks.com"
        mock_get_config.return_value = mock_config
        mock_setup_token.return_value = None  # Mock the sync call
        
        mock_client = MagicMock()
        mock_workspace_client.return_value = mock_client
        
        client = UCClient(mock_mode=True)
        
        # Act
        result = client.initialize_uc_client()
        
        # Assert
        assert result == mock_client
        mock_session_local.assert_called_once()
        mock_get_config.assert_called_once_with(mock_db)
        mock_setup_token.assert_called_once_with(mock_db)
        mock_db.close.assert_called_once()
        mock_workspace_client.assert_called_once_with(
            host="https://db.databricks.com",
            token="env-token",
            auth_type="pat"
        )
    
    @patch.dict(os.environ, {'DATABRICKS_HOST': 'env-host', 'DATABRICKS_TOKEN': 'env-token'})
    @patch('src.core.uc_client.SessionLocal')
    @patch('src.core.uc_client.get_databricks_config')
    @patch('src.core.uc_client.setup_databricks_token', return_value=None)
    @patch('src.core.uc_client.WorkspaceClient')
    def test_initialize_uc_client_from_env_vars(self, mock_workspace_client, 
                                               mock_setup_token, mock_get_config, mock_session_local):
        """Test initialize_uc_client using environment variables."""
        # Arrange
        mock_db = MagicMock()
        mock_session_local.return_value = mock_db
        mock_get_config.return_value = None  # No database config
        
        mock_client = MagicMock()
        mock_workspace_client.return_value = mock_client
        
        client = UCClient(mock_mode=True)
        
        # Act
        result = client.initialize_uc_client()
        
        # Assert
        assert result == mock_client
        mock_workspace_client.assert_called_once_with(
            host="env-host",
            token="env-token",
            auth_type="pat"
        )
    
    def test_initialize_uc_client_missing_credentials(self):
        """Test initialize_uc_client raises error when credentials are missing."""
        # Arrange
        with patch('src.core.uc_client.SessionLocal') as mock_session_local:
            mock_db = MagicMock()
            mock_session_local.return_value = mock_db
            
            with patch('src.core.uc_client.get_databricks_config', return_value=None):
                with patch('src.core.uc_client.setup_databricks_token', return_value=None):
                    client = UCClient(mock_mode=True)
                    
                    # Act & Assert
                    with pytest.raises(ValueError, match="Databricks host and token must be provided"):
                        client.initialize_uc_client()
    
    @patch('src.core.uc_client.WorkspaceClient')
    def test_initialize_uc_client_workspace_client_error(self, mock_workspace_client):
        """Test initialize_uc_client handles WorkspaceClient errors."""
        # Arrange
        mock_workspace_client.side_effect = Exception("Connection failed")
        client = UCClient(mock_mode=True)
        
        # Act & Assert
        with pytest.raises(Exception, match="Connection failed"):
            client.initialize_uc_client("host", "token")
    
    def test_list_functions_mock_mode(self):
        """Test list_functions in mock mode."""
        # Arrange
        client = UCClient(mock_mode=True)
        
        # Act
        result = client.list_functions("test_catalog", "test_schema")
        
        # Assert
        assert len(result) == 2
        assert all(isinstance(func, MockFunction) for func in result)
        assert result[0].name == "example_function_1"
        assert result[1].name == "example_function_2"
    
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_list_functions_live_mode(self, mock_init_client):
        """Test list_functions in live mode."""
        # Arrange
        mock_client = MagicMock()
        mock_functions = MagicMock()
        mock_functions_list = [MagicMock(name="func1", group_id="group-123"), MagicMock(name="func2", group_id="group-123")]
        
        mock_client.functions.list.return_value = mock_functions
        mock_functions.__iter__ = Mock(return_value=iter(mock_functions_list))
        
        mock_init_client.return_value = mock_client
        
        client = UCClient()
        
        # Act
        result = client.list_functions("catalog", "schema")
        
        # Assert
        assert len(result) == 2
        mock_client.functions.list.assert_called_once_with(
            catalog_name="catalog",
            schema_name="schema"
        )
    
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_list_functions_live_mode_error(self, mock_init_client):
        """Test list_functions handles errors in live mode."""
        # Arrange
        mock_client = MagicMock()
        mock_client.functions.list.side_effect = Exception("API Error")
        mock_init_client.return_value = mock_client
        
        client = UCClient()
        
        # Act & Assert
        with pytest.raises(Exception, match="API Error"):
            client.list_functions("catalog", "schema")
    
    def test_get_function_details_mock_mode_found(self):
        """Test get_function_details in mock mode when function is found."""
        # Arrange
        client = UCClient(mock_mode=True)
        
        # Act
        result = client.get_function_details("catalog", "schema", "example_function_1")
        
        # Assert
        assert isinstance(result, MockFunction)
        assert result.name == "example_function_1"
        assert result.comment == "This is an example function"
    
    def test_get_function_details_mock_mode_not_found(self):
        """Test get_function_details in mock mode when function is not found."""
        # Arrange
        client = UCClient(mock_mode=True)
        
        # Act & Assert
        with pytest.raises(ValueError, match="Function nonexistent_function not found"):
            client.get_function_details("catalog", "schema", "nonexistent_function")
    
    @patch('src.core.uc_client.UCClient.list_functions')
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_get_function_details_live_mode_found(self, mock_init_client, mock_list_functions):
        """Test get_function_details in live mode when function is found."""
        # Arrange
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        
        mock_func = MagicMock()
        mock_func.name = "target_function"
        mock_list_functions.return_value = [mock_func]
        
        client = UCClient()
        
        # Act
        result = client.get_function_details("catalog", "schema", "target_function")
        
        # Assert
        assert result == mock_func
        mock_list_functions.assert_called_once_with("catalog", "schema")
    
    @patch('src.core.uc_client.UCClient.list_functions')
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_get_function_details_live_mode_not_found(self, mock_init_client, mock_list_functions):
        """Test get_function_details in live mode when function is not found."""
        # Arrange
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        
        mock_func = MagicMock()
        mock_func.name = "other_function"
        mock_list_functions.return_value = [mock_func]
        
        client = UCClient()
        
        # Act & Assert
        with pytest.raises(ValueError, match="Function target_function not found in catalog.schema"):
            client.get_function_details("catalog", "schema", "target_function")
    
    @patch('src.core.uc_client.UCClient.list_functions')
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_get_function_details_live_mode_list_error(self, mock_init_client, mock_list_functions):
        """Test get_function_details handles errors from list_functions."""
        # Arrange
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        mock_list_functions.side_effect = Exception("List error")
        
        client = UCClient()
        
        # Act & Assert
        with pytest.raises(Exception, match="List error"):
            client.get_function_details("catalog", "schema", "function")
    
    @patch('src.core.uc_client.UCClient.list_functions')
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_get_function_details_reraises_value_error(self, mock_init_client, mock_list_functions):
        """Test that ValueError is re-raised correctly."""
        # Arrange
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        mock_list_functions.return_value = []  # Empty list
        
        client = UCClient()
        
        # Act & Assert
        with pytest.raises(ValueError, match="Function test_func not found in cat.schema"):
            client.get_function_details("cat", "schema", "test_func")
    
    @patch.dict(os.environ, {'UC_MOCK_MODE': 'TRUE'})
    def test_mock_mode_case_insensitive(self):
        """Test that UC_MOCK_MODE environment variable is case insensitive."""
        # Act
        client = UCClient()
        
        # Assert
        assert client.mock_mode is True
    
    @patch.dict(os.environ, {'UC_MOCK_MODE': 'False'})
    @patch('src.core.uc_client.UCClient.initialize_uc_client')
    def test_mock_mode_false_env_var(self, mock_init_client):
        """Test that UC_MOCK_MODE=False enables live mode."""
        # Arrange
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        
        # Act
        client = UCClient()
        
        # Assert
        assert client.mock_mode is False
    
    def test_mock_mode_parameter_overrides_env(self):
        """Test that mock_mode parameter overrides environment variable."""
        with patch.dict(os.environ, {'UC_MOCK_MODE': 'false'}):
            with patch('src.core.uc_client.UCClient.initialize_uc_client'):
                # Act
                client = UCClient(mock_mode=True)
                
                # Assert
                assert client.mock_mode is True