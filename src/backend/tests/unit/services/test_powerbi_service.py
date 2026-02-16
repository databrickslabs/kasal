"""
Unit tests for PowerBIService.

Tests the core functionality of Power BI integration operations including
DAX query execution, authentication, and result processing.
"""
import sys
import types
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.core.exceptions import KasalError, NotFoundError, BadRequestError, UnauthorizedError

# Ensure azure.identity is mockable even when not installed
if 'azure' not in sys.modules:
    azure_mock = types.ModuleType('azure')
    azure_mock.identity = types.ModuleType('azure.identity')
    azure_mock.identity.UsernamePasswordCredential = MagicMock
    azure_mock.identity.DeviceCodeCredential = MagicMock
    sys.modules['azure'] = azure_mock
    sys.modules['azure.identity'] = azure_mock.identity

from src.services.powerbi_service import PowerBIService
from src.schemas.powerbi_config import DAXQueryRequest, DAXQueryResponse


# Mock models
class MockPowerBIConfig:
    def __init__(self, id=1, tenant_id="test-tenant", client_id="test-client",
                 semantic_model_id="test-model", workspace_id="test-workspace",
                 is_enabled=True, is_active=True, auth_method=None,
                 created_at=None, updated_at=None):
        self.id = id
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.semantic_model_id = semantic_model_id
        self.workspace_id = workspace_id
        self.is_enabled = is_enabled
        self.is_active = is_active
        self.auth_method = auth_method
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    return AsyncMock()


@pytest.fixture
def mock_repository():
    """Create a mock PowerBIConfigRepository."""
    return AsyncMock()


@pytest.fixture
def powerbi_service(mock_session, mock_repository):
    """Create a PowerBIService instance with mocks."""
    with patch('src.services.powerbi_service.PowerBIConfigRepository') as MockRepo:
        MockRepo.return_value = mock_repository
        service = PowerBIService(mock_session, group_id="test-group")
        service.repository = mock_repository
        return service


@pytest.fixture
def mock_powerbi_config():
    """Create a mock Power BI config."""
    return MockPowerBIConfig()


@pytest.fixture
def valid_dax_query_request():
    """Create a valid DAX query request."""
    return DAXQueryRequest(
        dax_query="EVALUATE 'Sales'",
        semantic_model_id="test-model",
        workspace_id="test-workspace"
    )


@pytest.fixture
def mock_power_bi_api_response():
    """Create a mock Power BI API response."""
    return {
        "results": [{
            "tables": [{
                "rows": [
                    {"Region": "East", "Total": 1000},
                    {"Region": "West", "Total": 2000}
                ]
            }]
        }]
    }


class TestPowerBIServiceInitialization:
    """Test cases for PowerBIService initialization."""

    def test_powerbi_service_initialization(self, powerbi_service, mock_session, mock_repository):
        """Test PowerBIService initialization."""
        assert powerbi_service.session == mock_session
        assert powerbi_service.repository == mock_repository
        assert powerbi_service.group_id == "test-group"
        assert hasattr(powerbi_service, 'secrets_service')

    def test_powerbi_service_no_group_id(self, mock_session):
        """Test PowerBIService initialization without group_id."""
        with patch('src.services.powerbi_service.PowerBIConfigRepository'):
            service = PowerBIService(mock_session)
            assert service.group_id is None


class TestPowerBIServiceExecuteDAXQuery:
    """Test cases for execute_dax_query method."""

    @pytest.mark.asyncio
    async def test_execute_dax_query_success(self, powerbi_service, mock_powerbi_config,
                                             valid_dax_query_request, mock_power_bi_api_response):
        """Test successful DAX query execution."""
        # Mock repository to return config
        powerbi_service.repository.get_active_config.return_value = mock_powerbi_config

        # Mock token generation
        with patch.object(powerbi_service, '_generate_token', return_value="mock-token"):
            # Mock API call
            with patch.object(powerbi_service, '_execute_query', return_value=mock_power_bi_api_response["results"]):
                result = await powerbi_service.execute_dax_query(valid_dax_query_request)

                assert isinstance(result, DAXQueryResponse)
                assert result.status == "success"
                assert result.row_count == 2
                assert len(result.data) == 2
                assert result.columns == ["Region", "Total"]
                assert result.execution_time_ms >= 0

    @pytest.mark.asyncio
    async def test_execute_dax_query_no_config(self, powerbi_service, valid_dax_query_request):
        """Test DAX query execution when no config exists."""
        powerbi_service.repository.get_active_config.return_value = None

        with pytest.raises(NotFoundError) as exc_info:
            await powerbi_service.execute_dax_query(valid_dax_query_request)

        assert exc_info.value.status_code == 404
        assert "No active Power BI configuration" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_execute_dax_query_disabled_config(self, powerbi_service, mock_powerbi_config, valid_dax_query_request):
        """Test DAX query execution with disabled configuration."""
        mock_powerbi_config.is_enabled = False
        powerbi_service.repository.get_active_config.return_value = mock_powerbi_config

        with pytest.raises(BadRequestError) as exc_info:
            await powerbi_service.execute_dax_query(valid_dax_query_request)

        assert exc_info.value.status_code == 400
        assert "disabled" in str(exc_info.value.detail).lower()

    @pytest.mark.asyncio
    async def test_execute_dax_query_no_semantic_model(self, powerbi_service, mock_powerbi_config):
        """Test DAX query execution without semantic model ID."""
        mock_powerbi_config.semantic_model_id = None
        powerbi_service.repository.get_active_config.return_value = mock_powerbi_config

        query_request = DAXQueryRequest(dax_query="EVALUATE 'Sales'")

        with pytest.raises(BadRequestError) as exc_info:
            await powerbi_service.execute_dax_query(query_request)

        assert exc_info.value.status_code == 400
        assert "Semantic model ID is required" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_execute_dax_query_uses_default_model(self, powerbi_service, mock_powerbi_config, mock_power_bi_api_response):
        """Test DAX query uses default semantic model from config."""
        powerbi_service.repository.get_active_config.return_value = mock_powerbi_config

        query_request = DAXQueryRequest(dax_query="EVALUATE 'Sales'")  # No model ID in request

        with patch.object(powerbi_service, '_generate_token', return_value="mock-token"):
            with patch.object(powerbi_service, '_execute_query', return_value=mock_power_bi_api_response["results"]) as mock_execute:
                result = await powerbi_service.execute_dax_query(query_request)

                assert result.status == "success"
                # Verify default model was used
                mock_execute.assert_called_once()
                call_args = mock_execute.call_args
                assert call_args[1]['semantic_model_id'] == "test-model"

    @pytest.mark.asyncio
    async def test_execute_dax_query_error_handling(self, powerbi_service, mock_powerbi_config, valid_dax_query_request):
        """Test DAX query execution error handling."""
        powerbi_service.repository.get_active_config.return_value = mock_powerbi_config

        with patch.object(powerbi_service, '_generate_token', side_effect=Exception("Auth failed")):
            result = await powerbi_service.execute_dax_query(valid_dax_query_request)

            assert result.status == "error"
            assert "Auth failed" in result.error
            assert result.data is None


class TestPowerBIServiceTokenGeneration:
    """Test cases for _generate_token method."""

    @pytest.mark.asyncio
    async def test_generate_token_success(self, powerbi_service, mock_powerbi_config):
        """Test successful token generation."""
        with patch('azure.identity.UsernamePasswordCredential') as MockCred:
            mock_credential = MagicMock()
            mock_token = MagicMock()
            mock_token.token = "test-token-123"
            mock_credential.get_token.return_value = mock_token
            MockCred.return_value = mock_credential

            with patch.dict('os.environ', {
                'POWERBI_USERNAME': 'test@example.com',
                'POWERBI_PASSWORD': 'test-password'
            }):
                token = await powerbi_service._generate_token(mock_powerbi_config)

                assert token == "test-token-123"
                MockCred.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_token_missing_credentials(self, powerbi_service, mock_powerbi_config):
        """Test token generation with missing credentials."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(UnauthorizedError) as exc_info:
                await powerbi_service._generate_token(mock_powerbi_config)

            assert exc_info.value.status_code == 401
            assert "credentials" in str(exc_info.value.detail).lower()

    @pytest.mark.asyncio
    async def test_generate_token_authentication_error(self, powerbi_service, mock_powerbi_config):
        """Test token generation authentication error."""
        with patch('azure.identity.UsernamePasswordCredential') as MockCred:
            MockCred.side_effect = Exception("Auth failed")

            with patch.dict('os.environ', {
                'POWERBI_USERNAME': 'test@example.com',
                'POWERBI_PASSWORD': 'test-password'
            }):
                with pytest.raises(UnauthorizedError) as exc_info:
                    await powerbi_service._generate_token(mock_powerbi_config)

                assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_generate_token_device_code_branch(self, powerbi_service):
        """Test _generate_token routes to device_code method when auth_method is device_code."""
        config = MockPowerBIConfig(auth_method="device_code")

        with patch.object(
            powerbi_service, '_generate_token_device_code', return_value="device-code-token"
        ) as mock_dc:
            token = await powerbi_service._generate_token(config)

            assert token == "device-code-token"
            mock_dc.assert_called_once_with("test-tenant", "test-client")


class TestPowerBIServiceDeviceCodeAuth:
    """Test cases for _generate_token_device_code method."""

    @pytest.mark.asyncio
    async def test_generate_token_device_code_success(self, powerbi_service):
        """Test successful device code authentication flow."""
        with patch('azure.identity.DeviceCodeCredential') as MockDeviceCred:
            mock_credential = MagicMock()
            mock_token = MagicMock()
            mock_token.token = "device-code-token-abc"
            mock_credential.get_token.return_value = mock_token
            MockDeviceCred.return_value = mock_credential

            token = await powerbi_service._generate_token_device_code(
                tenant_id="test-tenant",
                client_id="test-client"
            )

            assert token == "device-code-token-abc"
            MockDeviceCred.assert_called_once_with(
                client_id="test-client",
                tenant_id="test-tenant",
            )
            mock_credential.get_token.assert_called_once_with(
                "https://analysis.windows.net/powerbi/api/.default"
            )

    @pytest.mark.asyncio
    async def test_generate_token_device_code_failure(self, powerbi_service):
        """Test device code authentication failure re-raises the exception."""
        with patch('azure.identity.DeviceCodeCredential') as MockDeviceCred:
            MockDeviceCred.side_effect = Exception("Device code auth failed")

            with pytest.raises(Exception, match="Device code auth failed"):
                await powerbi_service._generate_token_device_code(
                    tenant_id="test-tenant",
                    client_id="test-client"
                )

    @pytest.mark.asyncio
    async def test_generate_token_device_code_get_token_failure(self, powerbi_service):
        """Test device code flow when get_token raises an exception."""
        with patch('azure.identity.DeviceCodeCredential') as MockDeviceCred:
            mock_credential = MagicMock()
            mock_credential.get_token.side_effect = Exception("Token request failed")
            MockDeviceCred.return_value = mock_credential

            with pytest.raises(Exception, match="Token request failed"):
                await powerbi_service._generate_token_device_code(
                    tenant_id="test-tenant",
                    client_id="test-client"
                )


class TestPowerBIServiceUsernamePasswordSecretsService:
    """Test cases for _generate_token_username_password with secrets service."""

    @pytest.mark.asyncio
    async def test_generate_token_username_password_from_secrets_service(self, powerbi_service):
        """Test token generation retrieves credentials from secrets service."""
        mock_secrets = AsyncMock()
        mock_secrets.get_api_key = AsyncMock(side_effect=lambda key: {
            "POWERBI_USERNAME": "secret-user@example.com",
            "POWERBI_PASSWORD": "secret-password",
            "POWERBI_CLIENT_SECRET": "secret-client-secret",
        }.get(key))

        # Set _secrets_service to a truthy value so the code enters the if block
        powerbi_service._secrets_service = mock_secrets

        config = MockPowerBIConfig()

        with patch('azure.identity.UsernamePasswordCredential') as MockCred:
            mock_credential = MagicMock()
            mock_token = MagicMock()
            mock_token.token = "secrets-service-token"
            mock_credential.get_token.return_value = mock_token
            MockCred.return_value = mock_credential

            token = await powerbi_service._generate_token_username_password(
                tenant_id="test-tenant",
                client_id="test-client",
                config=config
            )

            assert token == "secrets-service-token"
            # Verify secrets service was queried
            assert mock_secrets.get_api_key.call_count == 3
            # Verify credential was created with secrets service values
            MockCred.assert_called_once_with(
                client_id="test-client",
                username="secret-user@example.com",
                password="secret-password",
                tenant_id="test-tenant",
                client_secret="secret-client-secret",
            )

    @pytest.mark.asyncio
    async def test_generate_token_username_password_secrets_service_exception(self, powerbi_service):
        """Test token generation falls back to env vars when secrets service raises."""
        mock_secrets = AsyncMock()
        mock_secrets.get_api_key = AsyncMock(side_effect=Exception("Secrets service unavailable"))

        # Set _secrets_service to a truthy value so the code enters the if block
        powerbi_service._secrets_service = mock_secrets

        config = MockPowerBIConfig()

        with patch('azure.identity.UsernamePasswordCredential') as MockCred:
            mock_credential = MagicMock()
            mock_token = MagicMock()
            mock_token.token = "env-fallback-token"
            mock_credential.get_token.return_value = mock_token
            MockCred.return_value = mock_credential

            with patch.dict('os.environ', {
                'POWERBI_USERNAME': 'env-user@example.com',
                'POWERBI_PASSWORD': 'env-password'
            }):
                token = await powerbi_service._generate_token_username_password(
                    tenant_id="test-tenant",
                    client_id="test-client",
                    config=config
                )

                assert token == "env-fallback-token"
                # Verify the credential was created with env var values after secrets failure
                MockCred.assert_called_once_with(
                    client_id="test-client",
                    username="env-user@example.com",
                    password="env-password",
                    tenant_id="test-tenant",
                    client_secret=None,
                )


class TestPowerBIServiceExecuteQuery:
    """Test cases for _execute_query method."""

    @pytest.mark.asyncio
    async def test_execute_query_success(self, powerbi_service, mock_power_bi_api_response):
        """Test successful Power BI API query execution."""
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_power_bi_api_response
        mock_client.post.return_value = mock_response

        with patch('src.services.powerbi_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await powerbi_service._execute_query(
                token="test-token",
                semantic_model_id="test-model",
                dax_query="EVALUATE 'Sales'"
            )

            assert result == mock_power_bi_api_response["results"]
            mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_query_api_error(self, powerbi_service):
        """Test Power BI API error handling."""
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_client.post.return_value = mock_response

        with patch('src.services.powerbi_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(KasalError) as exc_info:
                await powerbi_service._execute_query(
                    token="test-token",
                    semantic_model_id="test-model",
                    dax_query="INVALID DAX"
                )

            assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_execute_query_timeout(self, powerbi_service):
        """Test Power BI API timeout handling."""
        import httpx

        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.TimeoutException("Request timeout")

        with patch('src.services.powerbi_service.httpx.AsyncClient') as MockAsyncClient:
            MockAsyncClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockAsyncClient.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(KasalError) as exc_info:
                await powerbi_service._execute_query(
                    token="test-token",
                    semantic_model_id="test-model",
                    dax_query="EVALUATE 'Sales'"
                )

            assert exc_info.value.status_code == 500


class TestPowerBIServicePostprocessData:
    """Test cases for _postprocess_data method."""

    def test_postprocess_data_success(self, powerbi_service, mock_power_bi_api_response):
        """Test successful data postprocessing."""
        results = mock_power_bi_api_response["results"]

        data = powerbi_service._postprocess_data(results)

        assert len(data) == 2
        assert data[0] == {"Region": "East", "Total": 1000}
        assert data[1] == {"Region": "West", "Total": 2000}

    def test_postprocess_data_empty_results(self, powerbi_service):
        """Test postprocessing with empty results."""
        results = []

        data = powerbi_service._postprocess_data(results)

        assert data == []

    def test_postprocess_data_no_tables(self, powerbi_service):
        """Test postprocessing with no tables in results."""
        results = [{"tables": []}]

        data = powerbi_service._postprocess_data(results)

        assert data == []

    def test_postprocess_data_no_rows(self, powerbi_service):
        """Test postprocessing with no rows in table."""
        results = [{"tables": [{"rows": []}]}]

        data = powerbi_service._postprocess_data(results)

        assert data == []


class TestPowerBIServiceMultiTenancy:
    """Test cases for multi-tenant functionality."""

    @pytest.mark.asyncio
    async def test_service_uses_group_id(self, mock_session):
        """Test service properly uses group_id."""
        with patch('src.services.powerbi_service.PowerBIConfigRepository') as MockRepo:
            mock_repo = AsyncMock()
            MockRepo.return_value = mock_repo

            service = PowerBIService(mock_session, group_id="test-group")

            # Mock config
            mock_config = MockPowerBIConfig()
            mock_repo.get_active_config.return_value = mock_config

            query_request = DAXQueryRequest(dax_query="EVALUATE 'Sales'", semantic_model_id="test-model")

            with patch.object(service, '_generate_token', return_value="mock-token"):
                with patch.object(service, '_execute_query', return_value=[]):
                    await service.execute_dax_query(query_request)

                    # Verify get_active_config was called with group_id
                    mock_repo.get_active_config.assert_called_with(group_id="test-group")
