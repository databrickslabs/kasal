import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import Optional, Dict, Any

# Test MLflowService - based on actual code inspection

from src.services.mlflow_service import MLflowService


class TestMLflowServiceInit:
    """Test MLflowService initialization"""

    def test_mlflow_service_init_valid_group_id(self):
        """Test MLflowService __init__ with valid group_id"""
        mock_session = Mock()
        group_id = "test-group-id"
        
        service = MLflowService(mock_session, group_id)
        
        assert service.session == mock_session
        assert service.group_id == group_id
        assert hasattr(service, 'repo')
        assert hasattr(service, 'exec_repo')
        assert hasattr(service, 'model_config_service')
        assert service._user_token is None

    def test_mlflow_service_init_none_group_id(self):
        """Test MLflowService __init__ raises ValueError for None group_id"""
        mock_session = Mock()
        
        with pytest.raises(ValueError, match="SECURITY: group_id is REQUIRED"):
            MLflowService(mock_session, None)

    def test_mlflow_service_init_empty_group_id(self):
        """Test MLflowService __init__ raises ValueError for empty group_id"""
        mock_session = Mock()
        
        with pytest.raises(ValueError, match="SECURITY: group_id is REQUIRED"):
            MLflowService(mock_session, "")

    def test_mlflow_service_init_whitespace_group_id(self):
        """Test MLflowService __init__ accepts whitespace group_id (truthy in Python)"""
        mock_session = Mock()

        # Whitespace strings are truthy in Python, so this should not raise
        service = MLflowService(mock_session, "   ")
        assert service.group_id == "   "


class TestMLflowServiceBasicMethods:
    """Test basic MLflowService methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.group_id = "test-group-id"
        self.service = MLflowService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_is_enabled(self):
        """Test is_enabled method"""
        self.service.repo.is_enabled = AsyncMock(return_value=True)
        
        result = await self.service.is_enabled()
        
        assert result is True
        self.service.repo.is_enabled.assert_called_once_with(group_id=self.group_id)

    @pytest.mark.asyncio
    async def test_is_enabled_false(self):
        """Test is_enabled method returns False"""
        self.service.repo.is_enabled = AsyncMock(return_value=False)
        
        result = await self.service.is_enabled()
        
        assert result is False
        self.service.repo.is_enabled.assert_called_once_with(group_id=self.group_id)

    @pytest.mark.asyncio
    async def test_set_enabled_true(self):
        """Test set_enabled method with True"""
        self.service.repo.set_enabled = AsyncMock(return_value=True)
        
        result = await self.service.set_enabled(True)
        
        assert result is True
        self.service.repo.set_enabled.assert_called_once_with(enabled=True, group_id=self.group_id)

    @pytest.mark.asyncio
    async def test_set_enabled_false(self):
        """Test set_enabled method with False"""
        self.service.repo.set_enabled = AsyncMock(return_value=False)
        
        result = await self.service.set_enabled(False)
        
        assert result is False
        self.service.repo.set_enabled.assert_called_once_with(enabled=False, group_id=self.group_id)

    @pytest.mark.asyncio
    async def test_is_evaluation_enabled(self):
        """Test is_evaluation_enabled method"""
        self.service.repo.is_evaluation_enabled = AsyncMock(return_value=True)
        
        result = await self.service.is_evaluation_enabled()
        
        assert result is True
        self.service.repo.is_evaluation_enabled.assert_called_once_with(group_id=self.group_id)

    @pytest.mark.asyncio
    async def test_set_evaluation_enabled_true(self):
        """Test set_evaluation_enabled method with True"""
        self.service.repo.set_evaluation_enabled = AsyncMock(return_value=True)
        
        result = await self.service.set_evaluation_enabled(True)
        
        assert result is True
        self.service.repo.set_evaluation_enabled.assert_called_once_with(enabled=True, group_id=self.group_id)

    @pytest.mark.asyncio
    async def test_set_evaluation_enabled_false(self):
        """Test set_evaluation_enabled method with False"""
        self.service.repo.set_evaluation_enabled = AsyncMock(return_value=False)
        
        result = await self.service.set_evaluation_enabled(False)
        
        assert result is False
        self.service.repo.set_evaluation_enabled.assert_called_once_with(enabled=False, group_id=self.group_id)


class TestMLflowServiceUserToken:
    """Test MLflowService user token methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.group_id = "test-group-id"
        self.service = MLflowService(self.mock_session, self.group_id)

    def test_set_user_token_valid_string(self):
        """Test set_user_token with valid string"""
        token = "valid-token"
        
        self.service.set_user_token(token)
        
        assert self.service._user_token == token

    def test_set_user_token_none(self):
        """Test set_user_token with None"""
        self.service.set_user_token(None)
        
        assert self.service._user_token is None

    def test_set_user_token_empty_string(self):
        """Test set_user_token with empty string"""
        self.service.set_user_token("")
        
        assert self.service._user_token is None

    def test_set_user_token_whitespace_string(self):
        """Test set_user_token with whitespace string"""
        self.service.set_user_token("   ")
        
        assert self.service._user_token is None

    def test_set_user_token_non_string(self):
        """Test set_user_token with non-string value"""
        self.service.set_user_token(123)
        
        assert self.service._user_token is None

    def test_set_user_token_exception_handling(self):
        """Test set_user_token handles exceptions gracefully"""
        # Mock isinstance to raise an exception
        with patch('builtins.isinstance', side_effect=Exception("Test error")):
            self.service.set_user_token("test-token")
            
            assert self.service._user_token is None


class TestMLflowServiceSetupAuth:
    """Test MLflowService _setup_mlflow_auth method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.group_id = "test-group-id"
        self.service = MLflowService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    async def test_setup_mlflow_auth_success(self, mock_get_auth_context):
        """Test _setup_mlflow_auth with successful authentication"""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_auth.auth_method = "pat"
        mock_get_auth_context.return_value = mock_auth
        
        result = await self.service._setup_mlflow_auth()
        
        assert result == mock_auth
        mock_get_auth_context.assert_called_once_with(user_token=None)

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    async def test_setup_mlflow_auth_with_user_token(self, mock_get_auth_context):
        """Test _setup_mlflow_auth with user token"""
        self.service._user_token = "test-token"
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_auth.auth_method = "obo"
        mock_get_auth_context.return_value = mock_auth
        
        result = await self.service._setup_mlflow_auth()
        
        assert result == mock_auth
        mock_get_auth_context.assert_called_once_with(user_token="test-token")

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    async def test_setup_mlflow_auth_no_auth(self, mock_get_auth_context):
        """Test _setup_mlflow_auth when no authentication available"""
        mock_get_auth_context.return_value = None
        
        result = await self.service._setup_mlflow_auth()
        
        assert result is None

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    async def test_setup_mlflow_auth_no_workspace_url(self, mock_get_auth_context):
        """Test _setup_mlflow_auth when auth has no workspace_url"""
        mock_auth = Mock()
        mock_auth.workspace_url = None
        mock_get_auth_context.return_value = mock_auth
        
        result = await self.service._setup_mlflow_auth()
        
        assert result is None

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    async def test_setup_mlflow_auth_exception(self, mock_get_auth_context):
        """Test _setup_mlflow_auth handles exceptions"""
        mock_get_auth_context.side_effect = Exception("Test error")
        
        result = await self.service._setup_mlflow_auth()
        
        assert result is None


class TestMLflowServiceAttributes:
    """Test MLflowService attribute access"""

    def test_service_has_required_attributes(self):
        """Test that service has all required attributes after initialization"""
        mock_session = Mock()
        group_id = "test-group-id"
        
        service = MLflowService(mock_session, group_id)
        
        # Check all required attributes exist
        assert hasattr(service, 'session')
        assert hasattr(service, 'group_id')
        assert hasattr(service, 'repo')
        assert hasattr(service, 'exec_repo')
        assert hasattr(service, 'model_config_service')
        assert hasattr(service, '_user_token')
        
        # Check attribute types
        assert service.session == mock_session
        assert service.group_id == group_id
        assert service._user_token is None


class TestMLflowServiceAsyncMethods:
    """Test MLflowService async methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.group_id = "test-group-id"
        self.service = MLflowService(self.mock_session, self.group_id)

    @pytest.mark.asyncio
    async def test_is_enabled_true(self):
        """Test is_enabled returns True"""
        with patch.object(self.service.repo, 'is_enabled', return_value=True):
            result = await self.service.is_enabled()

            assert result is True

    @pytest.mark.asyncio
    async def test_is_enabled_false(self):
        """Test is_enabled returns False"""
        with patch.object(self.service.repo, 'is_enabled', return_value=False):
            result = await self.service.is_enabled()

            assert result is False

    @pytest.mark.asyncio
    async def test_set_enabled_true(self):
        """Test set_enabled with True"""
        with patch.object(self.service.repo, 'set_enabled', return_value=True):
            result = await self.service.set_enabled(True)

            assert result is True

    @pytest.mark.asyncio
    async def test_set_enabled_false(self):
        """Test set_enabled with False"""
        with patch.object(self.service.repo, 'set_enabled', return_value=True):
            result = await self.service.set_enabled(False)

            assert result is True

    @pytest.mark.asyncio
    async def test_is_evaluation_enabled_true(self):
        """Test is_evaluation_enabled returns True"""
        with patch.object(self.service.repo, 'is_evaluation_enabled', return_value=True):
            result = await self.service.is_evaluation_enabled()

            assert result is True

    @pytest.mark.asyncio
    async def test_is_evaluation_enabled_false(self):
        """Test is_evaluation_enabled returns False"""
        with patch.object(self.service.repo, 'is_evaluation_enabled', return_value=False):
            result = await self.service.is_evaluation_enabled()

            assert result is False

    @pytest.mark.asyncio
    async def test_set_evaluation_enabled_true(self):
        """Test set_evaluation_enabled with True"""
        with patch.object(self.service.repo, 'set_evaluation_enabled', return_value=True):
            result = await self.service.set_evaluation_enabled(True)

            assert result is True

    @pytest.mark.asyncio
    async def test_set_evaluation_enabled_false(self):
        """Test set_evaluation_enabled with False"""
        with patch.object(self.service.repo, 'set_evaluation_enabled', return_value=True):
            result = await self.service.set_evaluation_enabled(False)

            assert result is True

    @pytest.mark.asyncio
    async def test_resolve_judge_model_with_configured_model(self):
        """Test _resolve_judge_model with configured model"""
        configured_model = "test-judge-model"

        with patch.object(self.service.model_config_service, 'get_model_config') as mock_get_config:
            mock_get_config.return_value = {"key": "test-judge-model"}

            result = await self.service._resolve_judge_model(configured_model)

            assert result == "test-judge-model"
