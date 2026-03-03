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


class TestMLflowServiceAuth:
    """Test MLflow authentication setup with SPN -> PAT priority."""

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_pat_fallback_success(self):
        """Test successful PAT fallback when SPN env vars are not set."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_auth = Mock()
        mock_auth.workspace_url = "https://test.databricks.com"
        mock_auth.auth_method = "PAT"
        mock_auth.token = "test-token"

        with patch.dict('os.environ', {}, clear=False), \
             patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            # Ensure SPN env vars are not set
            import os
            os.environ.pop("DATABRICKS_CLIENT_ID", None)
            os.environ.pop("DATABRICKS_CLIENT_SECRET", None)

            mock_get_auth.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result == mock_auth
            mock_get_auth.assert_called_once_with(user_token=None)

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_spn_success(self):
        """Test SPN authentication when env vars are set."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_auth_context_cls = Mock()

        mock_dummy_request = Mock()
        mock_dummy_request.headers = {"Authorization": "Bearer spn-token-123"}

        mock_workspace_client = Mock()
        mock_authenticate = Mock(side_effect=lambda req: None)
        mock_workspace_client.config.authenticate.return_value = mock_authenticate

        with patch.dict('os.environ', {
            "DATABRICKS_CLIENT_ID": "test-client-id",
            "DATABRICKS_CLIENT_SECRET": "test-client-secret",
            "DATABRICKS_HOST": "https://test.databricks.com",
        }), \
             patch('src.utils.databricks_auth.AuthContext') as mock_ac, \
             patch('databricks.sdk.WorkspaceClient', return_value=mock_workspace_client), \
             patch('requests.Request') as mock_req_cls:
            mock_req_instance = Mock()
            mock_req_instance.headers = {"Authorization": "Bearer spn-token-123"}
            mock_req_cls.return_value = mock_req_instance

            mock_auth = Mock()
            mock_auth.workspace_url = "https://test.databricks.com"
            mock_auth.token = "spn-token-123"
            mock_auth.auth_method = "service_principal"
            mock_ac.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result is not None
            assert result.auth_method == "service_principal"

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_spn_fails_falls_back_to_pat(self):
        """Test SPN auth failure falls back to PAT."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_auth = Mock()
        mock_auth.workspace_url = "https://test.databricks.com"
        mock_auth.auth_method = "PAT"
        mock_auth.token = "pat-token"

        with patch.dict('os.environ', {
            "DATABRICKS_CLIENT_ID": "test-client-id",
            "DATABRICKS_CLIENT_SECRET": "test-client-secret",
            "DATABRICKS_HOST": "https://test.databricks.com",
        }), \
             patch('databricks.sdk.WorkspaceClient', side_effect=Exception("SPN failed")), \
             patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result == mock_auth
            assert result.auth_method == "PAT"
            mock_get_auth.assert_called_once_with(user_token=None)

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_no_auth(self):
        """Test MLflow authentication setup when no auth available."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        with patch.dict('os.environ', {}, clear=False), \
             patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            import os
            os.environ.pop("DATABRICKS_CLIENT_ID", None)
            os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
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

        with patch.dict('os.environ', {}, clear=False), \
             patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            import os
            os.environ.pop("DATABRICKS_CLIENT_ID", None)
            os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
            mock_get_auth.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result is None

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_exception(self):
        """Test MLflow authentication setup when exception occurs."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        with patch.dict('os.environ', {}, clear=False), \
             patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            import os
            os.environ.pop("DATABRICKS_CLIENT_ID", None)
            os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
            mock_get_auth.side_effect = Exception("Auth error")

            result = await service._setup_mlflow_auth()

            assert result is None

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_spn_host_without_scheme(self):
        """Test SPN auth prepends https:// when host has no scheme."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_workspace_client = Mock()
        mock_authenticate = Mock(side_effect=lambda req: None)
        mock_workspace_client.config.authenticate.return_value = mock_authenticate

        with patch.dict('os.environ', {
            "DATABRICKS_CLIENT_ID": "test-client-id",
            "DATABRICKS_CLIENT_SECRET": "test-client-secret",
            "DATABRICKS_HOST": "test.databricks.com",  # no scheme
        }), \
             patch('src.utils.databricks_auth.AuthContext') as mock_ac, \
             patch('databricks.sdk.WorkspaceClient', return_value=mock_workspace_client), \
             patch('requests.Request') as mock_req_cls:
            mock_req_instance = Mock()
            mock_req_instance.headers = {"Authorization": "Bearer spn-tok"}
            mock_req_cls.return_value = mock_req_instance

            mock_auth = Mock()
            mock_auth.workspace_url = "https://test.databricks.com"
            mock_auth.token = "spn-tok"
            mock_auth.auth_method = "service_principal"
            mock_ac.return_value = mock_auth

            result = await service._setup_mlflow_auth()

            assert result is not None
            # Verify AuthContext was called with https:// prefix
            call_kwargs = mock_ac.call_args
            assert call_kwargs[1]["workspace_url"] == "https://test.databricks.com"

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_spn_no_bearer_prefix(self):
        """Test SPN auth falls back to PAT when Authorization header has no Bearer prefix."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_workspace_client = Mock()
        mock_authenticate = Mock(side_effect=lambda req: None)
        mock_workspace_client.config.authenticate.return_value = mock_authenticate

        mock_pat_auth = Mock()
        mock_pat_auth.workspace_url = "https://test.databricks.com"
        mock_pat_auth.auth_method = "PAT"
        mock_pat_auth.token = "pat-token"

        with patch.dict('os.environ', {
            "DATABRICKS_CLIENT_ID": "test-client-id",
            "DATABRICKS_CLIENT_SECRET": "test-client-secret",
            "DATABRICKS_HOST": "https://test.databricks.com",
        }), \
             patch('databricks.sdk.WorkspaceClient', return_value=mock_workspace_client), \
             patch('requests.Request') as mock_req_cls, \
             patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_req_instance = Mock()
            mock_req_instance.headers = {"Authorization": "Basic some-cred"}  # no Bearer
            mock_req_cls.return_value = mock_req_instance

            mock_get_auth.return_value = mock_pat_auth

            result = await service._setup_mlflow_auth()

            # Should fall through SPN (no "Bearer " prefix) to PAT fallback
            assert result == mock_pat_auth
            assert result.auth_method == "PAT"


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

