import pytest
import os
import httpx
import warnings
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from src.services.databricks_service import DatabricksService
from src.core.exceptions import KasalError
from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse


# ---------------------------------------------------------------------------
# Helper: build a DatabricksService with mocked internals
# ---------------------------------------------------------------------------

def _make_service(group_id=None):
    """Return a DatabricksService with mocked repository."""
    with patch('src.services.databricks_service.DatabricksConfigRepository') as MockRepo:
        MockRepo.return_value = AsyncMock()
        service = DatabricksService(session=MagicMock(), group_id=group_id)
        service.repository = AsyncMock()
    return service


def _make_service_and_config():
    """Return a (service, mock_config) tuple ready for check_databricks_connection tests."""
    service = _make_service()

    mock_config = MagicMock()
    mock_config.is_enabled = True
    mock_config.workspace_url = "https://example.com"
    mock_config.warehouse_id = "wh1"
    mock_config.catalog = "main"
    mock_config.schema = "default"
    service.repository.get_active_config = AsyncMock(return_value=mock_config)

    return service, mock_config


def _make_full_config_mock():
    """Return a MagicMock that has all the attributes used by DatabricksConfigResponse."""
    cfg = MagicMock()
    cfg.workspace_url = "https://example.com"
    cfg.warehouse_id = "wh1"
    cfg.catalog = "main"
    cfg.schema = "default"
    cfg.is_enabled = True
    cfg.is_active = True
    cfg.mlflow_enabled = True
    cfg.mlflow_experiment_name = "test-experiment"
    cfg.evaluation_enabled = True
    cfg.evaluation_judge_model = "judge-model"
    cfg.volume_enabled = True
    cfg.volume_path = "/volumes/test"
    cfg.volume_file_format = "parquet"
    cfg.volume_create_date_dirs = False
    cfg.knowledge_volume_enabled = True
    cfg.knowledge_volume_path = "/volumes/knowledge"
    cfg.knowledge_chunk_size = 500
    cfg.knowledge_chunk_overlap = 100
    return cfg


def _make_config_input(enabled=True):
    """Create a DatabricksConfigCreate-like mock with all required attributes."""
    config_in = MagicMock(spec=[])
    config_in.workspace_url = "https://example.com"
    config_in.warehouse_id = "wh1"
    config_in.catalog = "main"
    config_in.db_schema = "default"
    config_in.enabled = enabled
    config_in.mlflow_enabled = True
    config_in.mlflow_experiment_name = "test-experiment"
    config_in.evaluation_enabled = False
    config_in.evaluation_judge_model = None
    config_in.volume_enabled = False
    config_in.volume_path = None
    config_in.volume_file_format = "json"
    config_in.volume_create_date_dirs = True
    config_in.knowledge_volume_enabled = False
    config_in.knowledge_volume_path = None
    config_in.knowledge_chunk_size = 1000
    config_in.knowledge_chunk_overlap = 200
    return config_in


# ===========================================================================
# __init__ and properties (lines 39-40)
# ===========================================================================

class TestInit:
    """Tests for DatabricksService constructor and properties."""

    def test_init_sets_session_and_group_id(self):
        """Lines 28-32: init stores session, repository, group_id, and _secrets_service=None."""
        mock_session = MagicMock()
        with patch('src.services.databricks_service.DatabricksConfigRepository') as MockRepo:
            MockRepo.return_value = AsyncMock()
            service = DatabricksService(session=mock_session, group_id="grp-123")

        assert service.session is mock_session
        assert service.group_id == "grp-123"
        assert service._secrets_service is None

    def test_init_default_group_id_is_none(self):
        """group_id defaults to None when not provided."""
        with patch('src.services.databricks_service.DatabricksConfigRepository') as MockRepo:
            MockRepo.return_value = AsyncMock()
            service = DatabricksService(session=MagicMock())

        assert service.group_id is None

    def test_secrets_service_lazy_init(self):
        """Lines 39-40: First access creates DatabricksSecretsService; second returns same."""
        service = _make_service()
        mock_secrets = MagicMock()

        with patch(
            'src.services.databricks_secrets_service.DatabricksSecretsService',
            return_value=mock_secrets,
        ):
            result = service.secrets_service

        assert result is mock_secrets
        assert service._secrets_service is mock_secrets

    def test_secrets_service_cached_after_first_access(self):
        """After first init, subsequent accesses return the cached instance."""
        service = _make_service()
        cached = MagicMock()
        service._secrets_service = cached

        assert service.secrets_service is cached


# ===========================================================================
# set_databricks_config (lines 54-112)
# ===========================================================================

class TestSetDatabricksConfig:
    """Tests for set_databricks_config method."""

    @pytest.mark.asyncio
    async def test_set_config_success_enabled(self):
        """Lines 54-109: Successful config creation with enabled=True."""
        service = _make_service(group_id="grp-1")
        config_in = _make_config_input(enabled=True)
        new_config = _make_full_config_mock()
        service.repository.create_config = AsyncMock(return_value=new_config)

        result = await service.set_databricks_config(config_in, created_by_email="user@example.com")

        assert result["status"] == "success"
        assert "enabled" in result["message"]
        assert isinstance(result["config"], DatabricksConfigResponse)
        assert result["config"].workspace_url == "https://example.com"
        assert result["config"].mlflow_enabled is True
        assert result["config"].evaluation_enabled is True
        assert result["config"].volume_enabled is True
        assert result["config"].knowledge_volume_enabled is True

    @pytest.mark.asyncio
    async def test_set_config_success_disabled(self):
        """Message says 'disabled' when config_in.enabled is False."""
        service = _make_service()
        config_in = _make_config_input(enabled=False)
        new_config = _make_full_config_mock()
        service.repository.create_config = AsyncMock(return_value=new_config)

        result = await service.set_databricks_config(config_in)

        assert result["status"] == "success"
        assert "disabled" in result["message"]

    @pytest.mark.asyncio
    async def test_set_config_uses_getattr_defaults(self):
        """Lines 63-66: When config_in lacks optional attrs, getattr defaults are used."""
        service = _make_service()
        config_in = MagicMock(spec=[])
        config_in.workspace_url = "https://example.com"
        config_in.warehouse_id = "wh1"
        config_in.catalog = "main"
        config_in.db_schema = "default"
        config_in.enabled = True
        config_in.volume_enabled = False
        config_in.volume_path = None
        config_in.volume_file_format = "json"
        config_in.volume_create_date_dirs = True
        config_in.knowledge_volume_enabled = False
        config_in.knowledge_volume_path = None
        config_in.knowledge_chunk_size = 1000
        config_in.knowledge_chunk_overlap = 200
        # Intentionally do NOT add mlflow_enabled, mlflow_experiment_name,
        # evaluation_enabled, evaluation_judge_model so getattr falls back

        new_config = _make_full_config_mock()
        service.repository.create_config = AsyncMock(return_value=new_config)

        result = await service.set_databricks_config(config_in)

        # Verify create_config was called with the correct data
        call_args = service.repository.create_config.call_args[0][0]
        assert call_args["mlflow_enabled"] is False  # getattr default
        assert call_args["mlflow_experiment_name"] == "kasal-crew-execution-traces"
        assert call_args["evaluation_enabled"] is False
        assert call_args["evaluation_judge_model"] is None

    @pytest.mark.asyncio
    async def test_set_config_new_config_missing_hasattr(self):
        """Lines 94-107: When new_config lacks optional attrs, hasattr returns False."""
        service = _make_service()
        config_in = _make_config_input(enabled=True)

        # Create a config object where hasattr returns False for optional fields
        new_config = MagicMock(spec=["workspace_url", "warehouse_id", "catalog", "schema", "is_enabled"])
        new_config.workspace_url = "https://example.com"
        new_config.warehouse_id = "wh1"
        new_config.catalog = "main"
        new_config.schema = "default"
        new_config.is_enabled = True
        service.repository.create_config = AsyncMock(return_value=new_config)

        result = await service.set_databricks_config(config_in)

        assert result["status"] == "success"
        cfg_resp = result["config"]
        assert cfg_resp.mlflow_enabled is False
        assert cfg_resp.mlflow_experiment_name == "kasal-crew-execution-traces"
        assert cfg_resp.evaluation_enabled is False
        assert cfg_resp.evaluation_judge_model is None
        assert cfg_resp.volume_enabled is False
        assert cfg_resp.volume_path is None
        assert cfg_resp.volume_file_format == "json"
        assert cfg_resp.volume_create_date_dirs is True
        assert cfg_resp.knowledge_volume_enabled is False
        assert cfg_resp.knowledge_volume_path is None
        assert cfg_resp.knowledge_chunk_size == 1000
        assert cfg_resp.knowledge_chunk_overlap == 200

    @pytest.mark.asyncio
    async def test_set_config_exception_raises_kasal_error(self):
        """Lines 110-112: Generic exception wraps into KasalError."""
        service = _make_service()
        config_in = _make_config_input()
        service.repository.create_config = AsyncMock(side_effect=RuntimeError("db failed"))

        with pytest.raises(KasalError, match="Error setting Databricks configuration"):
            await service.set_databricks_config(config_in)


# ===========================================================================
# get_databricks_config (lines 114-155, missing: 125, 152)
# ===========================================================================

class TestGetDatabricksConfig:
    """Tests for get_databricks_config method."""

    @pytest.mark.asyncio
    async def test_get_config_returns_config(self):
        """Lines 121-150: Returns DatabricksConfigResponse when config exists."""
        service = _make_service(group_id="grp-1")
        mock_config = _make_full_config_mock()
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.get_databricks_config()

        assert isinstance(result, DatabricksConfigResponse)
        assert result.workspace_url == "https://example.com"
        assert result.mlflow_enabled is True
        assert result.volume_enabled is True
        service.repository.get_active_config.assert_awaited_once_with(group_id="grp-1")

    @pytest.mark.asyncio
    async def test_get_config_returns_none_when_no_config(self):
        """Line 125: Returns None when no active config found."""
        service = _make_service(group_id="grp-1")
        service.repository.get_active_config = AsyncMock(return_value=None)

        result = await service.get_databricks_config()

        assert result is None

    @pytest.mark.asyncio
    async def test_get_config_reraises_kasal_error(self):
        """Line 152: KasalError is re-raised without wrapping."""
        service = _make_service()
        service.repository.get_active_config = AsyncMock(
            side_effect=KasalError(detail="original error")
        )

        with pytest.raises(KasalError, match="original error"):
            await service.get_databricks_config()

    @pytest.mark.asyncio
    async def test_get_config_wraps_generic_exception(self):
        """Lines 153-155: Non-KasalError exceptions are wrapped."""
        service = _make_service()
        service.repository.get_active_config = AsyncMock(
            side_effect=ValueError("unexpected")
        )

        with pytest.raises(KasalError, match="Error getting Databricks configuration"):
            await service.get_databricks_config()

    @pytest.mark.asyncio
    async def test_get_config_hasattr_defaults(self):
        """Config missing optional attrs falls back to defaults via hasattr."""
        service = _make_service()
        mock_config = MagicMock(spec=["workspace_url", "warehouse_id", "catalog", "schema", "is_enabled"])
        mock_config.workspace_url = "https://example.com"
        mock_config.warehouse_id = "wh1"
        mock_config.catalog = "main"
        mock_config.schema = "default"
        mock_config.is_enabled = True
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.get_databricks_config()

        assert isinstance(result, DatabricksConfigResponse)
        assert result.mlflow_enabled is False
        assert result.volume_enabled is False
        assert result.knowledge_volume_enabled is False


# ===========================================================================
# check_personal_token_required (lines 164-197)
# ===========================================================================

class TestCheckPersonalTokenRequired:
    """Tests for check_personal_token_required method."""

    @pytest.mark.asyncio
    async def test_no_config(self):
        """Lines 167-171: No config returns personal_token_required=False."""
        service = _make_service()
        service.repository.get_active_config = AsyncMock(return_value=None)

        result = await service.check_personal_token_required()

        assert result["personal_token_required"] is False
        assert "not configured" in result["message"]

    @pytest.mark.asyncio
    async def test_disabled_config(self):
        """Lines 174-178: Disabled config returns personal_token_required=False."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = False
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.check_personal_token_required()

        assert result["personal_token_required"] is False
        assert "disabled" in result["message"]

    @pytest.mark.asyncio
    async def test_missing_required_field(self):
        """Lines 181-188: Missing required field returns personal_token_required=True."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.warehouse_id = "wh1"
        mock_config.catalog = ""  # empty => missing
        mock_config.schema = "default"
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.check_personal_token_required()

        assert result["personal_token_required"] is True
        assert "missing catalog" in result["message"]

    @pytest.mark.asyncio
    async def test_all_fields_present(self):
        """Lines 191-194: All fields present returns personal_token_required=False."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.warehouse_id = "wh1"
        mock_config.catalog = "main"
        mock_config.schema = "default"
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.check_personal_token_required()

        assert result["personal_token_required"] is False
        assert "unified authentication" in result["message"]

    @pytest.mark.asyncio
    async def test_exception_raises_kasal_error(self):
        """Lines 195-197: Exception wraps into KasalError."""
        service = _make_service()
        service.repository.get_active_config = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(KasalError, match="Error checking personal token requirement"):
            await service.check_personal_token_required()


# ===========================================================================
# check_apps_configuration (lines 209-224)
# ===========================================================================

class TestCheckAppsConfiguration:
    """Tests for check_apps_configuration method."""

    @pytest.mark.asyncio
    async def test_no_config_returns_false(self):
        """Lines 211-212: No config returns (False, '')."""
        service = _make_service()
        service.repository.get_active_config = AsyncMock(return_value=None)

        result = await service.check_apps_configuration()

        assert result == (False, "")

    @pytest.mark.asyncio
    async def test_enabled_with_token(self):
        """Lines 215-219: Enabled config with personal token returns (True, token)."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = True
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        mock_secrets = AsyncMock()
        mock_secrets.get_personal_access_token = AsyncMock(return_value="pat-token")
        service._secrets_service = mock_secrets

        result = await service.check_apps_configuration()

        assert result == (True, "pat-token")

    @pytest.mark.asyncio
    async def test_enabled_no_token(self):
        """Lines 218-221: Enabled but no token returns (False, '')."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = True
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        mock_secrets = AsyncMock()
        mock_secrets.get_personal_access_token = AsyncMock(return_value=None)
        service._secrets_service = mock_secrets

        result = await service.check_apps_configuration()

        assert result == (False, "")

    @pytest.mark.asyncio
    async def test_disabled_returns_false(self):
        """Line 221: Config not enabled returns (False, '')."""
        service = _make_service()
        mock_config = MagicMock(spec=["is_active"])  # no is_enabled attribute
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.check_apps_configuration()

        assert result == (False, "")

    @pytest.mark.asyncio
    async def test_exception_returns_false(self):
        """Lines 222-224: Exception returns (False, '') gracefully."""
        service = _make_service()
        service.repository.get_active_config = AsyncMock(side_effect=RuntimeError("fail"))

        result = await service.check_apps_configuration()

        assert result == (False, "")


# ===========================================================================
# setup_endpoint static method (lines 242-272)
# ===========================================================================

class TestSetupEndpoint:
    """Tests for setup_endpoint static method."""

    def test_valid_config_sets_env_vars(self):
        """Lines 249-266: Valid config sets DATABRICKS_API_BASE and DATABRICKS_ENDPOINT."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://example.com/"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = DatabricksService.setup_endpoint(mock_config)

        assert result is True
        assert os.environ["DATABRICKS_API_BASE"] == "https://example.com/serving-endpoints"
        assert os.environ["DATABRICKS_ENDPOINT"] == "https://example.com/serving-endpoints"

    def test_url_already_has_serving_endpoints(self):
        """Lines 259-262: URL ending with /serving-endpoints is preserved."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://example.com/serving-endpoints"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = DatabricksService.setup_endpoint(mock_config)

        assert result is True
        assert os.environ["DATABRICKS_ENDPOINT"] == "https://example.com/serving-endpoints"

    def test_no_config_returns_false(self):
        """Lines 267-269: None config returns False."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = DatabricksService.setup_endpoint(None)

        assert result is False

    def test_no_workspace_url_returns_false(self):
        """Lines 267-269: Config with empty workspace_url returns False."""
        mock_config = MagicMock()
        mock_config.workspace_url = ""

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = DatabricksService.setup_endpoint(mock_config)

        assert result is False

    def test_config_without_workspace_url_attr_returns_false(self):
        """Lines 267-269: Config without workspace_url attribute returns False."""
        mock_config = MagicMock(spec=[])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = DatabricksService.setup_endpoint(mock_config)

        assert result is False

    def test_exception_returns_false(self):
        """Lines 270-272: Exception during setup returns False."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://example.com"
        # Make rstrip raise to trigger the except branch
        type(mock_config).workspace_url = PropertyMock(side_effect=RuntimeError("fail"))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = DatabricksService.setup_endpoint(mock_config)

        assert result is False

    def test_deprecation_warning_emitted(self):
        """Lines 242-248: DeprecationWarning is emitted."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://example.com"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            DatabricksService.setup_endpoint(mock_config)

        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1
        assert "deprecated" in str(deprecation_warnings[0].message).lower()


# ===========================================================================
# from_session (line 296 -- with api_keys_service)
# ===========================================================================

class TestFromSession:
    """Tests for from_session class method."""

    def test_from_session_basic(self):
        """Lines 286-298: from_session without api_keys_service."""
        mock_session = MagicMock()
        with patch('src.services.databricks_service.DatabricksConfigRepository') as MockRepo:
            MockRepo.return_value = AsyncMock()
            service = DatabricksService.from_session(mock_session)

        assert isinstance(service, DatabricksService)
        assert service.session is mock_session

    def test_from_session_with_api_keys_service(self):
        """Line 296: from_session sets api_keys_service on secrets_service."""
        mock_session = MagicMock()
        mock_api_keys = MagicMock()
        mock_secrets = MagicMock()

        with patch('src.services.databricks_service.DatabricksConfigRepository') as MockRepo:
            MockRepo.return_value = AsyncMock()
            with patch.object(
                DatabricksService, 'secrets_service', new_callable=PropertyMock, return_value=mock_secrets
            ):
                service = DatabricksService.from_session(mock_session, api_keys_service=mock_api_keys)

        mock_secrets.set_api_keys_service.assert_called_once_with(mock_api_keys)


# ===========================================================================
# check_databricks_connection (lines 300-442)
# ===========================================================================

class TestCheckDatabricksConnection:
    """Tests for check_databricks_connection method."""

    @pytest.mark.asyncio
    async def test_no_config(self):
        """Lines 309-314: No config returns error."""
        service = _make_service()
        service.repository.get_active_config = AsyncMock(return_value=None)

        result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "not found" in result["message"]

    @pytest.mark.asyncio
    async def test_disabled_config(self):
        """Lines 316-321: Disabled config returns disabled status."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = False
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.check_databricks_connection()

        assert result["status"] == "disabled"
        assert result["connected"] is False
        assert "disabled" in result["message"]

    @pytest.mark.asyncio
    async def test_missing_required_fields(self):
        """Lines 328-338: Missing required fields returns error with field names."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.warehouse_id = ""
        mock_config.catalog = ""
        mock_config.schema = "default"
        mock_config.workspace_url = "https://example.com"
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "warehouse_id" in result["message"]
        assert "catalog" in result["message"]

    @pytest.mark.asyncio
    async def test_no_workspace_url_and_no_env(self):
        """Lines 344-351: No workspace_url and no DATABRICKS_HOST env returns error."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.warehouse_id = "wh1"
        mock_config.catalog = "main"
        mock_config.schema = "default"
        mock_config.workspace_url = ""
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        with patch.dict(os.environ, {}, clear=True):
            # Ensure DATABRICKS_HOST is not set
            os.environ.pop("DATABRICKS_HOST", None)
            result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "workspace_url not configured" in result["message"]

    @pytest.mark.asyncio
    async def test_url_without_https_prefix(self):
        """Line 353: URL without https:// gets the prefix added."""
        service, mock_config = _make_service_and_config()
        mock_config.workspace_url = "example.com"

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "success"
        assert result["config"]["workspace_url"] == "https://example.com"

    @pytest.mark.asyncio
    async def test_url_with_trailing_slash(self):
        """Line 355: Trailing slash is removed from workspace URL."""
        service, mock_config = _make_service_and_config()
        mock_config.workspace_url = "https://example.com/"

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "success"
        assert result["config"]["workspace_url"] == "https://example.com"

    @pytest.mark.asyncio
    async def test_auth_returns_none(self):
        """Lines 366-371: get_auth_context returns None."""
        service, _ = _make_service_and_config()

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = None

            result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "No authentication credentials" in result["message"]

    @pytest.mark.asyncio
    async def test_auth_exception(self):
        """Lines 375-381: get_auth_context raises exception."""
        service, _ = _make_service_and_config()

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = RuntimeError("auth boom")

            result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Authentication error" in result["message"]

    @pytest.mark.asyncio
    async def test_no_headers_from_auth(self):
        """Lines 383-388: Auth context returns empty/None headers."""
        service, _ = _make_service_and_config()

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
            mock_auth_ctx = MagicMock()
            mock_auth_ctx.get_headers.return_value = {}
            mock_auth.return_value = mock_auth_ctx

            result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "No authentication credentials" in result["message"]

    @pytest.mark.asyncio
    async def test_success_response(self):
        """Lines 394-405: HTTP 200 returns success with config details."""
        service, mock_config = _make_service_and_config()

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "success"
        assert result["connected"] is True
        assert result["config"]["workspace_url"] == "https://example.com"

    @pytest.mark.asyncio
    async def test_unauthorized_response(self):
        """Lines 406-411: HTTP 401 returns authentication failed error."""
        service, _ = _make_service_and_config()

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_client.get.return_value = mock_response

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Authentication failed" in result["message"]

    @pytest.mark.asyncio
    async def test_forbidden_response(self):
        """Lines 412-417: HTTP 403 returns access forbidden error."""
        service, _ = _make_service_and_config()

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_client.get.return_value = mock_response

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Access forbidden" in result["message"]

    @pytest.mark.asyncio
    async def test_other_error_response(self):
        """Lines 418-423: Other HTTP status codes return status-specific error."""
        service, _ = _make_service_and_config()

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_client.get.return_value = mock_response

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Connection failed with status 500" in result["message"]

    @pytest.mark.asyncio
    async def test_connect_error(self):
        """Lines 425-430: httpx.ConnectError returns connection failure."""
        service, _ = _make_service_and_config()

        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.ConnectError("fail")

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Failed to connect" in result["message"]

    @pytest.mark.asyncio
    async def test_timeout_error(self):
        """Lines 431-435: httpx.TimeoutException returns timeout error."""
        service, _ = _make_service_and_config()

        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Connection timeout" in result["message"]

    @pytest.mark.asyncio
    async def test_general_exception(self):
        """Lines 437-442: General exception during connection test."""
        service, _ = _make_service_and_config()

        mock_client = AsyncMock()
        mock_client.get.side_effect = RuntimeError("unexpected failure")

        with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                mock_auth_ctx = MagicMock()
                mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
                mock_auth.return_value = mock_auth_ctx

                result = await service.check_databricks_connection()

        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Connection test failed" in result["message"]

    @pytest.mark.asyncio
    async def test_workspace_url_from_env_fallback(self):
        """Line 344: Falls back to DATABRICKS_HOST env var when config has no workspace_url."""
        service = _make_service()
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.warehouse_id = "wh1"
        mock_config.catalog = "main"
        mock_config.schema = "default"
        mock_config.workspace_url = ""
        service.repository.get_active_config = AsyncMock(return_value=mock_config)

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response

        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://env-host.example.com"}):
            with patch('src.services.databricks_service.httpx.AsyncClient') as MockAsyncClient:
                MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
                MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

                with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_auth:
                    mock_auth_ctx = MagicMock()
                    mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test"}
                    mock_auth.return_value = mock_auth_ctx

                    result = await service.check_databricks_connection()

        assert result["status"] == "success"
        assert result["config"]["workspace_url"] == "https://env-host.example.com"
