"""
Unit tests for converters/formats/uc_metrics/connector.py

Comprehensive tests for DatabricksConnector class.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.services.converters.formats.uc_metrics.connector import DatabricksConnector


class TestDatabricksConnectorInit:
    """Tests for DatabricksConnector initialization."""

    def test_init_with_api_key(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )
        assert conn.workspace_url == "https://example.databricks.com"
        assert conn.auth_service is not None

    def test_init_strips_trailing_slash(self):
        conn = DatabricksConnector(workspace_url="https://example.databricks.com/")
        assert conn.workspace_url == "https://example.databricks.com"

    def test_init_with_service_principal(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            client_id="cid",
            client_secret="csecret",
        )
        assert conn.auth_service.client_id == "cid"

    def test_init_with_extra_kwargs(self):
        """Extra kwargs should be accepted without error."""
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_xyz",
            unknown_param="ignored",
        )
        assert conn.workspace_url == "https://example.databricks.com"


class TestValidateConnection:
    """Tests for DatabricksConnector.validate_connection."""

    @pytest.fixture
    def connector(self):
        return DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )

    def test_returns_true_on_200(self, connector):
        mock_response = MagicMock()
        mock_response.status_code = 200
        with patch("requests.get", return_value=mock_response):
            assert connector.validate_connection() is True

    def test_returns_false_on_non_200(self, connector):
        mock_response = MagicMock()
        mock_response.status_code = 401
        with patch("requests.get", return_value=mock_response):
            assert connector.validate_connection() is False

    def test_returns_false_on_exception(self, connector):
        with patch("requests.get", side_effect=Exception("network error")):
            assert connector.validate_connection() is False

    def test_returns_false_on_auth_error(self, connector):
        with patch.object(
            connector.auth_service, "get_headers", side_effect=ValueError("no creds")
        ):
            assert connector.validate_connection() is False


class TestDeployUcMetrics:
    """Tests for DatabricksConnector.deploy_uc_metrics."""

    def test_raises_not_implemented(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )
        with pytest.raises(NotImplementedError):
            conn.deploy_uc_metrics("catalog", "schema", "yaml_def")


class TestContextManager:
    """Tests for DatabricksConnector context manager protocol."""

    def test_enter_raises_on_connection_failure(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )
        with patch.object(conn, "validate_connection", return_value=False):
            with pytest.raises(ConnectionError):
                conn.__enter__()

    def test_enter_returns_self_on_success(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )
        with patch.object(conn, "validate_connection", return_value=True):
            result = conn.__enter__()
        assert result is conn

    def test_exit_does_not_raise(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )
        # Should not raise
        conn.__exit__(None, None, None)
