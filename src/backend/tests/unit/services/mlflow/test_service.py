from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.mlflow.service import MLflowService


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
        service.repo.set_enabled.assert_called_once_with(
            enabled=True, group_id="test-group"
        )

    @pytest.mark.asyncio
    async def test_set_enabled_false(self):
        """Test disabling MLflow."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        service.repo.set_enabled = AsyncMock(return_value=True)

        result = await service.set_enabled(False)
        assert result is True
        service.repo.set_enabled.assert_called_once_with(
            enabled=False, group_id="test-group"
        )


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
        service.repo.is_evaluation_enabled.assert_called_once_with(
            group_id="test-group"
        )

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
        service.repo.set_evaluation_enabled.assert_called_once_with(
            enabled=True, group_id="test-group"
        )

    @pytest.mark.asyncio
    async def test_set_evaluation_enabled_false(self):
        """Test disabling evaluation."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        service.repo.set_evaluation_enabled = AsyncMock(return_value=True)

        result = await service.set_evaluation_enabled(False)
        assert result is True
        service.repo.set_evaluation_enabled.assert_called_once_with(
            enabled=False, group_id="test-group"
        )


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

        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock
            ) as mock_get_auth,
        ):
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

        # ``config.authenticate()`` returns a HEADERS DICT, not a callable — see
        # sp_auth.derive_sp_bearer, which exists because an earlier version called
        # the result and got "'dict' object is not callable", swallowed it, and
        # silently fell back to the ambient PAT (a 403 in production). Mocking it
        # as a callable reproduces that dead contract, so the token derivation
        # fails and _setup_mlflow_auth falls through to PAT.
        mock_workspace_client = Mock()
        mock_workspace_client.config.authenticate.return_value = {
            "Authorization": "Bearer spn-token-123"
        }

        with (
            patch.dict(
                "os.environ",
                {
                    "DATABRICKS_CLIENT_ID": "test-client-id",
                    "DATABRICKS_CLIENT_SECRET": "test-client-secret",
                    "DATABRICKS_HOST": "https://test.databricks.com",
                },
            ),
            patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace_client),
        ):
            result = await service._setup_mlflow_auth()

            assert result is not None
            assert result.auth_method == "service_principal"
            # The bearer prefix is stripped off the header value.
            assert result.token == "spn-token-123"

    @pytest.mark.asyncio
    async def test_setup_mlflow_auth_spn_fails_falls_back_to_pat(self):
        """Test SPN auth failure falls back to PAT."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="test-group")

        mock_auth = Mock()
        mock_auth.workspace_url = "https://test.databricks.com"
        mock_auth.auth_method = "PAT"
        mock_auth.token = "pat-token"

        with (
            patch.dict(
                "os.environ",
                {
                    "DATABRICKS_CLIENT_ID": "test-client-id",
                    "DATABRICKS_CLIENT_SECRET": "test-client-secret",
                    "DATABRICKS_HOST": "https://test.databricks.com",
                },
            ),
            patch(
                "databricks.sdk.WorkspaceClient", side_effect=Exception("SPN failed")
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock
            ) as mock_get_auth,
        ):
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

        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock
            ) as mock_get_auth,
        ):
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

        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock
            ) as mock_get_auth,
        ):
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

        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock
            ) as mock_get_auth,
        ):
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

        # authenticate() returns a headers DICT — see the note in the SPN success
        # test above.
        mock_workspace_client = Mock()
        mock_workspace_client.config.authenticate.return_value = {
            "Authorization": "Bearer spn-tok"
        }

        with (
            patch.dict(
                "os.environ",
                {
                    "DATABRICKS_CLIENT_ID": "test-client-id",
                    "DATABRICKS_CLIENT_SECRET": "test-client-secret",
                    "DATABRICKS_HOST": "test.databricks.com",  # no scheme
                },
            ),
            patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace_client),
        ):
            result = await service._setup_mlflow_auth()

            assert result is not None
            # The bare host gets an https:// prefix — MLflow needs an absolute URL.
            assert result.workspace_url == "https://test.databricks.com"

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

        with (
            patch.dict(
                "os.environ",
                {
                    "DATABRICKS_CLIENT_ID": "test-client-id",
                    "DATABRICKS_CLIENT_SECRET": "test-client-secret",
                    "DATABRICKS_HOST": "https://test.databricks.com",
                },
            ),
            patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace_client),
            patch("requests.Request") as mock_req_cls,
            patch(
                "src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock
            ) as mock_get_auth,
        ):
            mock_req_instance = Mock()
            mock_req_instance.headers = {
                "Authorization": "Basic some-cred"
            }  # no Bearer
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

        with pytest.raises(
            RuntimeError, match="Failed to configure MLflow authentication"
        ):
            await service.get_experiment_info()


class TestEnsureExperimentCreated:
    """Saving the experiment name creates it on Databricks, so an admin can
    attach it as the app's MLflow resource (which grants the SP MLflow access)."""

    @pytest.mark.asyncio
    async def test_noop_when_no_databricks_backend(self):
        """Local/OSS backends create the experiment lazily — nothing to do here."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="grp")
        service._configured_workspace_url = AsyncMock(return_value=None)
        service._setup_mlflow_auth = AsyncMock()

        await service._ensure_experiment_created()

        service._setup_mlflow_auth.assert_not_called()

    @pytest.mark.asyncio
    async def test_creates_experiment_at_shared_path(self):
        """With Databricks configured, create /Shared/<resolved-name>-uc — the
        SAME dedicated UC experiment the tracer/judges/GEPA use, so the admin
        attaches the one traces actually land in."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="grp")
        service._configured_workspace_url = AsyncMock(
            return_value="https://ws.example.com"
        )
        service._setup_mlflow_auth = AsyncMock(return_value=MagicMock())
        service.repo.get_experiment_name = AsyncMock(return_value="my-exp")
        service._teamspace_name = AsyncMock(return_value="Acme")

        with patch(
            "src.services.mlflow.experiment_setup.create_databricks_experiment",
            return_value={"experiment_id": "123", "experiment_name": "x"},
        ) as create:
            await service._ensure_experiment_created()

        # Explicit configured name wins; created under /Shared/ with the -uc
        # suffix (Databricks backend) so it matches where traces are written.
        _auth, path = create.call_args.args
        assert path == "/Shared/my-exp-uc"

    @pytest.mark.asyncio
    async def test_create_failure_does_not_raise(self):
        """A create failure must not block saving the name."""
        session = AsyncMock(spec=AsyncSession)
        service = MLflowService(session=session, group_id="grp")
        service._configured_workspace_url = AsyncMock(
            return_value="https://ws.example.com"
        )
        service._setup_mlflow_auth = AsyncMock(return_value=MagicMock())
        service.repo.get_experiment_name = AsyncMock(return_value=None)
        service._teamspace_name = AsyncMock(return_value="Acme")

        with patch(
            "src.services.mlflow.experiment_setup.create_databricks_experiment",
            side_effect=Exception("PERMISSION_DENIED"),
        ):
            # Must not raise.
            await service._ensure_experiment_created()
