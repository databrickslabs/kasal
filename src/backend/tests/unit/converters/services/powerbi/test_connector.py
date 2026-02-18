"""
Unit tests for converters/services/powerbi/connector.py

Tests Power BI connector for extracting measures from Power BI datasets via REST API
including authentication, connection management, and measure extraction.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from src.converters.services.powerbi.connector import PowerBIConnector
from src.converters.base.connectors import ConnectorType
from src.converters.base.models import KPI


class TestPowerBIConnector:
    """Tests for PowerBIConnector class"""

    @pytest.fixture
    def connector_with_token(self):
        """Create PowerBIConnector with access token"""
        return PowerBIConnector(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="test_token"
        )

    @pytest.fixture
    def connector_with_credentials(self):
        """Create PowerBIConnector with Service Principal credentials"""
        return PowerBIConnector(
            semantic_model_id="model123",
            group_id="workspace456",
            tenant_id="tenant789",
            client_id="client_abc",
            client_secret="secret_xyz"
        )

    # ========== Initialization Tests ==========

    def test_initialization_with_token(self, connector_with_token):
        """Test PowerBIConnector initializes with access token"""
        assert connector_with_token.semantic_model_id == "model123"
        assert connector_with_token.group_id == "workspace456"
        assert connector_with_token._access_token == "test_token"
        assert connector_with_token.info_table_name == "Info Measures"

    def test_initialization_with_credentials(self, connector_with_credentials):
        """Test PowerBIConnector initializes with credentials"""
        assert connector_with_credentials.semantic_model_id == "model123"
        assert connector_with_credentials.group_id == "workspace456"
        assert connector_with_credentials.aad_service is not None

    def test_initialization_with_custom_table_name(self):
        """Test PowerBIConnector initializes with custom info table name"""
        connector = PowerBIConnector(
            semantic_model_id="model",
            group_id="workspace",
            access_token="token",
            info_table_name="Custom Info Table"
        )
        assert connector.info_table_name == "Custom Info Table"

    def test_initialization_creates_dax_parser(self, connector_with_token):
        """Test PowerBIConnector creates DAX parser"""
        assert connector_with_token.dax_parser is not None

    def test_initialization_with_all_parameters(self):
        """Test PowerBIConnector initializes with all parameters"""
        connector = PowerBIConnector(
            semantic_model_id="model",
            group_id="workspace",
            tenant_id="tenant",
            client_id="client",
            client_secret="secret",
            access_token="token",
            project_id="proj123",
            use_database=True,
            info_table_name="Info Table"
        )

        assert connector.semantic_model_id == "model"
        assert connector.group_id == "workspace"
        assert connector.info_table_name == "Info Table"

    def test_api_base_constant(self):
        """Test API_BASE constant is set"""
        assert PowerBIConnector.API_BASE == "https://api.powerbi.com/v1.0/myorg"

    # ========== Connection Tests ==========

    @patch.object(PowerBIConnector, '_get_access_token')
    def test_connect_success(self, mock_get_token, connector_with_token):
        """Test successful connection"""
        mock_get_token.return_value = "new_token"

        connector_with_token.connect()

        assert connector_with_token._connected is True
        assert connector_with_token._access_token == "new_token"
        mock_get_token.assert_called_once()

    @patch.object(PowerBIConnector, '_get_access_token')
    def test_connect_already_connected(self, mock_get_token, connector_with_token):
        """Test connecting when already connected"""
        connector_with_token._connected = True

        connector_with_token.connect()

        # Should not call get_access_token again
        mock_get_token.assert_not_called()

    @patch.object(PowerBIConnector, '_get_access_token')
    def test_connect_failure(self, mock_get_token, connector_with_token):
        """Test connection failure"""
        mock_get_token.side_effect = Exception("Auth failed")

        with pytest.raises(ConnectionError, match="Failed to obtain access token"):
            connector_with_token.connect()

        assert connector_with_token._connected is False

    def test_disconnect(self, connector_with_token):
        """Test disconnection"""
        connector_with_token._connected = True

        connector_with_token.disconnect()

        assert connector_with_token._connected is False

    # ========== Get Access Token Tests ==========

    def test_get_access_token_delegates_to_aad_service(self, connector_with_credentials):
        """Test _get_access_token delegates to AadService"""
        with patch.object(connector_with_credentials.aad_service, 'get_access_token', return_value="service_token"):
            token = connector_with_credentials._get_access_token()

            assert token == "service_token"
            connector_with_credentials.aad_service.get_access_token.assert_called_once()

    # ========== Execute DAX Query Tests ==========

    @patch('src.converters.services.powerbi.connector.requests.post')
    def test_execute_dax_query_success(self, mock_post, connector_with_token):
        """Test successful DAX query execution"""
        connector_with_token._connected = True
        connector_with_token._access_token = "test_token"

        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{
                "tables": [{
                    "rows": [
                        {"[Name]": "Measure1", "[Expression]": "SUM(Table[Column])"}
                    ]
                }]
            }]
        }
        mock_post.return_value = mock_response

        result = connector_with_token._execute_dax_query("EVALUATE 'Info Measures'")

        assert len(result) == 1
        assert result[0]["[Name]"] == "Measure1"
        mock_post.assert_called_once()

    @patch('src.converters.services.powerbi.connector.requests.post')
    def test_execute_dax_query_not_connected(self, mock_post, connector_with_token):
        """Test DAX query execution when not connected"""
        connector_with_token._connected = False

        with pytest.raises(RuntimeError, match="Not connected"):
            connector_with_token._execute_dax_query("SELECT * FROM Table")

        mock_post.assert_not_called()

    @patch('src.converters.services.powerbi.connector.requests.post')
    def test_execute_dax_query_api_error(self, mock_post, connector_with_token):
        """Test DAX query execution with API error"""
        connector_with_token._connected = True
        connector_with_token._access_token = "test_token"

        # Mock error response
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response

        with pytest.raises(RuntimeError, match="Query failed"):
            connector_with_token._execute_dax_query("INVALID QUERY")

    @patch('src.converters.services.powerbi.connector.requests.post')
    def test_execute_dax_query_no_tables(self, mock_post, connector_with_token):
        """Test DAX query with no tables in response"""
        connector_with_token._connected = True
        connector_with_token._access_token = "test_token"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}
        mock_post.return_value = mock_response

        result = connector_with_token._execute_dax_query("QUERY")

        assert result == []

    @patch('src.converters.services.powerbi.connector.requests.post')
    def test_execute_dax_query_authorization_header(self, mock_post, connector_with_token):
        """Test DAX query sends correct authorization header"""
        connector_with_token._connected = True
        connector_with_token._access_token = "my_token_123"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"tables": [{"rows": []}]}]}
        mock_post.return_value = mock_response

        connector_with_token._execute_dax_query("QUERY")

        # Verify authorization header
        call_kwargs = mock_post.call_args[1]
        assert call_kwargs['headers']['Authorization'] == "Bearer my_token_123"

    # ========== Extract Measures Tests ==========

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_success(self, mock_execute, connector_with_token):
        """Test extracting measures successfully"""
        connector_with_token._connected = True

        mock_execute.return_value = [
            {
                "[ID]": "1",
                "[Name]": "Total Sales",
                "[Table]": "Sales",
                "[Description]": "Sum of all sales",
                "[Expression]": "SUM(Sales[Amount])",
                "[IsHidden]": False,
                "[State]": "Ready",
                "[DisplayFolder]": "Metrics"
            }
        ]

        kpis = connector_with_token.extract_measures()

        assert len(kpis) == 1
        assert isinstance(kpis[0], KPI)
        assert kpis[0].technical_name == "total_sales"
        assert kpis[0].aggregation_type == "SUM"

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_not_connected(self, mock_execute, connector_with_token):
        """Test extracting measures when not connected"""
        connector_with_token._connected = False

        with pytest.raises(RuntimeError, match="Not connected"):
            connector_with_token.extract_measures()

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_exclude_hidden(self, mock_execute, connector_with_token):
        """Test extracting measures excludes hidden by default"""
        connector_with_token._connected = True

        mock_execute.return_value = [
            {
                "[Name]": "Visible",
                "[Expression]": "SUM(Table[Col])",
                "[IsHidden]": False
            },
            {
                "[Name]": "Hidden",
                "[Expression]": "SUM(Table[Col2])",
                "[IsHidden]": True
            }
        ]

        kpis = connector_with_token.extract_measures(include_hidden=False)

        assert len(kpis) == 1
        assert kpis[0].technical_name == "visible"

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_include_hidden(self, mock_execute, connector_with_token):
        """Test extracting measures includes hidden when requested"""
        connector_with_token._connected = True

        mock_execute.return_value = [
            {
                "[Name]": "Visible",
                "[Expression]": "SUM(Table[Col])",
                "[IsHidden]": False
            },
            {
                "[Name]": "Hidden",
                "[Expression]": "SUM(Table[Col2])",
                "[IsHidden]": True
            }
        ]

        kpis = connector_with_token.extract_measures(include_hidden=True)

        assert len(kpis) == 2

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_with_filter_pattern(self, mock_execute, connector_with_token):
        """Test extracting measures with filter pattern"""
        connector_with_token._connected = True

        mock_execute.return_value = [
            {
                "[Name]": "Sales_Total",
                "[Expression]": "SUM(Sales[Amount])",
                "[IsHidden]": False
            },
            {
                "[Name]": "Revenue_Total",
                "[Expression]": "SUM(Revenue[Amount])",
                "[IsHidden]": False
            }
        ]

        kpis = connector_with_token.extract_measures(filter_pattern=r"Sales.*")

        assert len(kpis) == 1
        assert kpis[0].technical_name == "sales_total"

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_technical_name_generation(self, mock_execute, connector_with_token):
        """Test technical name generation from measure name"""
        connector_with_token._connected = True

        mock_execute.return_value = [
            {
                "[Name]": "Total Sales-Amount",
                "[Expression]": "SUM(Sales[Amount])",
                "[IsHidden]": False
            }
        ]

        kpis = connector_with_token.extract_measures()

        # Spaces and hyphens converted to underscores, lowercased
        assert kpis[0].technical_name == "total_sales_amount"

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_stores_advanced_parsing(self, mock_execute, connector_with_token):
        """Test extract_measures stores advanced parsing metadata"""
        connector_with_token._connected = True

        mock_execute.return_value = [
            {
                "[Name]": "Measure",
                "[Expression]": "SUM(Table[Col])",
                "[IsHidden]": False
            }
        ]

        kpis = connector_with_token.extract_measures()

        # Check advanced parsing metadata is attached
        assert hasattr(kpis[0], '_advanced_parsing')
        assert 'transpiled_sql' in kpis[0]._advanced_parsing
        assert 'is_transpilable' in kpis[0]._advanced_parsing

    # ========== Get Metadata Tests ==========

    def test_get_metadata_not_connected(self, connector_with_token):
        """Test getting metadata when not connected"""
        connector_with_token._connected = False

        metadata = connector_with_token.get_metadata()

        assert metadata.connector_type == ConnectorType.POWERBI
        assert metadata.source_id == "model123"
        assert metadata.connected is False
        assert metadata.measure_count is None

    @patch.object(PowerBIConnector, 'extract_measures')
    def test_get_metadata_connected(self, mock_extract, connector_with_token):
        """Test getting metadata when connected"""
        connector_with_token._connected = True

        # Mock extract_measures to return 3 measures
        mock_extract.return_value = [Mock(), Mock(), Mock()]

        metadata = connector_with_token.get_metadata()

        assert metadata.connector_type == ConnectorType.POWERBI
        assert metadata.connected is True
        assert metadata.measure_count == 3
        assert metadata.additional_info["info_table_name"] == "Info Measures"
        assert metadata.additional_info["group_id"] == "workspace456"

    @patch.object(PowerBIConnector, 'extract_measures')
    def test_get_metadata_extraction_fails(self, mock_extract, connector_with_token):
        """Test getting metadata when extraction fails"""
        connector_with_token._connected = True
        mock_extract.side_effect = Exception("Extraction failed")

        metadata = connector_with_token.get_metadata()

        # Should still return metadata without measure count
        assert metadata.measure_count is None

    # ========== Context Manager Tests ==========

    @patch.object(PowerBIConnector, 'connect')
    @patch.object(PowerBIConnector, 'disconnect')
    def test_context_manager_success(self, mock_disconnect, mock_connect, connector_with_token):
        """Test using connector as context manager"""
        with connector_with_token as conn:
            assert conn == connector_with_token
            mock_connect.assert_called_once()

        mock_disconnect.assert_called_once()

    @patch.object(PowerBIConnector, 'connect')
    @patch.object(PowerBIConnector, 'disconnect')
    def test_context_manager_with_exception(self, mock_disconnect, mock_connect, connector_with_token):
        """Test context manager disconnects even on exception"""
        try:
            with connector_with_token:
                raise ValueError("Test exception")
        except ValueError:
            pass

        mock_connect.assert_called_once()
        mock_disconnect.assert_called_once()

    # ========== Integration Tests ==========

    @patch('src.converters.services.powerbi.connector.requests.post')
    @patch.object(PowerBIConnector, '_get_access_token')
    def test_full_extraction_workflow(self, mock_get_token, mock_post, connector_with_token):
        """Test complete workflow from connect to extract"""
        # Setup mocks
        mock_get_token.return_value = "workflow_token"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{
                "tables": [{
                    "rows": [
                        {
                            "[Name]": "Revenue",
                            "[Expression]": "SUM(Sales[Amount])",
                            "[IsHidden]": False,
                            "[Table]": "Sales"
                        }
                    ]
                }]
            }]
        }
        mock_post.return_value = mock_response

        # Execute workflow
        connector_with_token.connect()
        kpis = connector_with_token.extract_measures()
        connector_with_token.disconnect()

        # Verify
        assert connector_with_token._connected is False
        assert len(kpis) == 1
        assert kpis[0].technical_name == "revenue"

    # ========== Edge Cases ==========

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_empty_result(self, mock_execute, connector_with_token):
        """Test extracting measures with empty result"""
        connector_with_token._connected = True
        mock_execute.return_value = []

        kpis = connector_with_token.extract_measures()

        assert kpis == []

    @patch.object(PowerBIConnector, '_execute_dax_query')
    def test_extract_measures_missing_fields(self, mock_execute, connector_with_token):
        """Test extracting measures with missing optional fields"""
        connector_with_token._connected = True

        mock_execute.return_value = [
            {
                "[Name]": "BasicMeasure",
                "[Expression]": "SUM(Table[Col])"
                # Missing IsHidden, Description, Table, etc.
            }
        ]

        kpis = connector_with_token.extract_measures()

        # Should still create KPI with defaults
        assert len(kpis) == 1
        assert kpis[0].technical_name == "basicmeasure"

    def test_initialization_sets_connection_params(self, connector_with_token):
        """Test initialization stores connection parameters"""
        params = connector_with_token.connection_params

        assert params["semantic_model_id"] == "model123"
        assert params["group_id"] == "workspace456"
        assert params["access_token"] == "test_token"

    @patch('src.converters.services.powerbi.connector.requests.post')
    def test_execute_dax_query_builds_correct_url(self, mock_post, connector_with_token):
        """Test _execute_dax_query builds correct API URL"""
        connector_with_token._connected = True
        connector_with_token._access_token = "token"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"tables": [{"rows": []}]}]}
        mock_post.return_value = mock_response

        connector_with_token._execute_dax_query("QUERY")

        # Verify URL construction
        call_args = mock_post.call_args[0]
        expected_url = f"{PowerBIConnector.API_BASE}/groups/workspace456/datasets/model123/executeQueries"
        assert call_args[0] == expected_url
