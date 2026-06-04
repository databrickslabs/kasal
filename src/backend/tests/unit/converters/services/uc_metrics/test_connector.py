"""
Unit tests for converters/services/uc_metrics/connector.py

Comprehensive tests for DatabricksConnector class.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.converters.services.uc_metrics.connector import DatabricksConnector


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
        with patch.object(connector.auth_service, "get_headers", side_effect=ValueError("no creds")):
            assert connector.validate_connection() is False


class TestGetCatalogs:
    """Tests for DatabricksConnector.get_catalogs."""

    @pytest.fixture
    def connector(self):
        return DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )

    def test_returns_catalog_list(self, connector):
        mock_response = MagicMock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"catalogs": [{"name": "main"}, {"name": "hive"}]}
        with patch("requests.get", return_value=mock_response):
            result = connector.get_catalogs()
        assert len(result) == 2
        assert result[0]["name"] == "main"

    def test_returns_empty_list_when_no_catalogs_key(self, connector):
        mock_response = MagicMock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {}
        with patch("requests.get", return_value=mock_response):
            result = connector.get_catalogs()
        assert result == []

    def test_raises_on_http_error(self, connector):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("HTTP 403")
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception):
                connector.get_catalogs()


class TestGetSchemas:
    """Tests for DatabricksConnector.get_schemas."""

    @pytest.fixture
    def connector(self):
        return DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )

    def test_returns_schema_list(self, connector):
        mock_response = MagicMock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"schemas": [{"name": "default"}, {"name": "analytics"}]}
        with patch("requests.get", return_value=mock_response):
            result = connector.get_schemas("main")
        assert len(result) == 2

    def test_raises_on_http_error(self, connector):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("HTTP 500")
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception):
                connector.get_schemas("main")


class TestDeployUcMetrics:
    """Tests for DatabricksConnector.deploy_uc_metrics."""

    def test_raises_not_implemented(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )
        with pytest.raises(NotImplementedError):
            conn.deploy_uc_metrics("catalog", "schema", "yaml_def")


class TestGetMetricDefinitions:
    """Tests for DatabricksConnector.get_metric_definitions."""

    def test_raises_not_implemented(self):
        conn = DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )
        with pytest.raises(NotImplementedError):
            conn.get_metric_definitions("catalog", "schema")


class TestValidateMetricDefinition:
    """Tests for DatabricksConnector.validate_metric_definition."""

    @pytest.fixture
    def connector(self):
        return DatabricksConnector(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc",
        )

    def test_valid_yaml(self, connector):
        yaml_def = """
version: "0.1"
measures:
  - name: revenue
    expr: SUM(amount)
"""
        result = connector.validate_metric_definition(yaml_def)
        assert result["valid"] is True
        assert result["errors"] == []

    def test_missing_version_field(self, connector):
        yaml_def = """
measures:
  - name: revenue
    expr: SUM(amount)
"""
        result = connector.validate_metric_definition(yaml_def)
        assert result["valid"] is False
        assert any("version" in e for e in result["errors"])

    def test_missing_measures_field(self, connector):
        yaml_def = """
version: "0.1"
"""
        result = connector.validate_metric_definition(yaml_def)
        assert result["valid"] is False
        assert any("measures" in e for e in result["errors"])

    def test_measures_not_list(self, connector):
        yaml_def = """
version: "0.1"
measures: not_a_list
"""
        result = connector.validate_metric_definition(yaml_def)
        assert result["valid"] is False

    def test_measure_missing_name(self, connector):
        yaml_def = """
version: "0.1"
measures:
  - expr: SUM(amount)
"""
        result = connector.validate_metric_definition(yaml_def)
        assert result["valid"] is False
        assert any("name" in e for e in result["errors"])

    def test_measure_missing_expr(self, connector):
        yaml_def = """
version: "0.1"
measures:
  - name: revenue
"""
        result = connector.validate_metric_definition(yaml_def)
        assert result["valid"] is False
        assert any("expr" in e for e in result["errors"])

    def test_invalid_yaml_syntax(self, connector):
        result = connector.validate_metric_definition("{{invalid: yaml: syntax:")
        assert result["valid"] is False
        assert any("YAML" in e or "yaml" in e.lower() for e in result["errors"])


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
