import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.mlflow_service import MLflowService


class TestMLflowServiceInit:
    """Test MLflowService initialization."""

    def test_init_success(self):
        """Test successful initialization with group_id."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        assert service.session == session
        assert service.group_id == "test-group"
        assert service._user_token is None

    def test_init_without_group_id_raises_error(self):
        """Test initialization without group_id raises ValueError."""
        session = AsyncMock(spec=AsyncSession)
        
        with pytest.raises(ValueError, match="SECURITY: group_id is REQUIRED"):
            MLflowService(session=session, group_id="")
        
        with pytest.raises(ValueError, match="SECURITY: group_id is REQUIRED"):
            MLflowService(session=session, group_id=None)


class TestMLflowServiceEnableDisable:
    """Test MLflow enable/disable functionality."""

    @pytest.mark.asyncio
    async def test_is_enabled_true(self):
        """Test checking if MLflow is enabled returns True."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.is_enabled = AsyncMock(return_value=True)
        
        result = await service.is_enabled()
        assert result is True
        service.repo.is_enabled.assert_called_once_with(group_id="test-group")

    @pytest.mark.asyncio
    async def test_is_enabled_false(self):
        """Test checking if MLflow is enabled returns False."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.is_enabled = AsyncMock(return_value=False)
        
        result = await service.is_enabled()
        assert result is False

    @pytest.mark.asyncio
    async def test_set_enabled_true(self):
        """Test enabling MLflow."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.set_enabled = AsyncMock(return_value=True)
        
        result = await service.set_enabled(True)
        assert result is True
        service.repo.set_enabled.assert_called_once_with(enabled=True, group_id="test-group")

    @pytest.mark.asyncio
    async def test_set_enabled_false(self):
        """Test disabling MLflow."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.set_enabled = AsyncMock(return_value=True)
        
        result = await service.set_enabled(False)
        assert result is True
        service.repo.set_enabled.assert_called_once_with(enabled=False, group_id="test-group")


class TestMLflowServiceEvaluation:
    """Test MLflow evaluation functionality."""

    @pytest.mark.asyncio
    async def test_is_evaluation_enabled_true(self):
        """Test checking if evaluation is enabled returns True."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.is_evaluation_enabled = AsyncMock(return_value=True)
        
        result = await service.is_evaluation_enabled()
        assert result is True
        service.repo.is_evaluation_enabled.assert_called_once_with(group_id="test-group")

    @pytest.mark.asyncio
    async def test_is_evaluation_enabled_false(self):
        """Test checking if evaluation is enabled returns False."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.is_evaluation_enabled = AsyncMock(return_value=False)
        
        result = await service.is_evaluation_enabled()
        assert result is False

    @pytest.mark.asyncio
    async def test_set_evaluation_enabled_true(self):
        """Test enabling evaluation."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.set_evaluation_enabled = AsyncMock(return_value=True)
        
        result = await service.set_evaluation_enabled(True)
        assert result is True
        service.repo.set_evaluation_enabled.assert_called_once_with(enabled=True, group_id="test-group")

    @pytest.mark.asyncio
    async def test_set_evaluation_enabled_false(self):
        """Test disabling evaluation."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.repo.set_evaluation_enabled = AsyncMock(return_value=True)
        
        result = await service.set_evaluation_enabled(False)
        assert result is True
        service.repo.set_evaluation_enabled.assert_called_once_with(enabled=False, group_id="test-group")


class TestMLflowServiceUserToken:
    """Test user token management."""

    def test_set_user_token_valid(self):
        """Test setting a valid user token."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.set_user_token("valid-token")
        assert service._user_token == "valid-token"

    def test_set_user_token_empty_string(self):
        """Test setting empty string as user token."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.set_user_token("")
        assert service._user_token is None

    def test_set_user_token_whitespace(self):
        """Test setting whitespace as user token."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.set_user_token("   ")
        assert service._user_token is None

    def test_set_user_token_none(self):
        """Test setting None as user token."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.set_user_token(None)
        assert service._user_token is None

    def test_set_user_token_invalid_type(self):
        """Test setting invalid type as user token."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service.set_user_token(12345)
        assert service._user_token is None


class TestMLflowServiceAuth:
    """Test MLflow authentication setup."""

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_success(self):
        """Test successful MLflow authentication setup."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_auth = Mock()
        mock_auth.workspace_url = "https://test.databricks.com"
        mock_auth.auth_method = "PAT"
        mock_auth.token = "test-token"

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result == mock_auth
            mock_get_auth.assert_called_once_with(user_token=None)

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_with_user_token(self):
        """Test MLflow authentication setup with user token."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        service.set_user_token("user-token")

        mock_auth = Mock()
        mock_auth.workspace_url = "https://test.databricks.com"
        mock_auth.auth_method = "OBO"
        mock_auth.token = "obo-token"

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result == mock_auth
            mock_get_auth.assert_called_once_with(user_token="user-token")

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_no_auth(self):
        """Test MLflow authentication setup when no auth available."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = None

            result = await service._setup_mlflow_auth()

            assert result is None

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_no_workspace_url(self):
        """Test MLflow authentication setup when workspace URL is missing."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_auth = Mock()
        mock_auth.workspace_url = None

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result is None

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_exception(self):
        """Test MLflow authentication setup when exception occurs."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.side_effect = Exception("Auth error")

            result = await service._setup_mlflow_auth()

            assert result is None


class TestMLflowServiceExperimentInfo:
    """Test getting MLflow experiment info."""

    @pytest.mark.asyncio
    async def test_get_experiment_info_no_auth(self):
        """Test getting experiment info when authentication fails."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")
        
        service._setup_mlflow_auth = AsyncMock(return_value=None)
        
        with pytest.raises(RuntimeError, match="Failed to configure MLflow authentication"):
            await service.get_experiment_info()

