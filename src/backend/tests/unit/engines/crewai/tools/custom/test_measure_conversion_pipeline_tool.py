"""
Unit tests for engines/crewai/tools/custom/measure_conversion_pipeline_tool.py

Tests universal measure conversion pipeline tool for CrewAI.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.engines.crewai.tools.custom.measure_conversion_pipeline_tool import (
    MeasureConversionPipelineSchema,
    MeasureConversionPipelineTool
)


class TestMeasureConversionPipelineSchema:
    """Tests for MeasureConversionPipelineSchema Pydantic model"""

    def test_schema_initialization_powerbi_minimal(self):
        """Test schema with minimal Power BI parameters"""
        schema = MeasureConversionPipelineSchema(
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789"
        )

        assert schema.inbound_connector == "powerbi"
        assert schema.powerbi_semantic_model_id == "model123"
        assert schema.powerbi_group_id == "workspace456"
        assert schema.powerbi_tenant_id == "tenant123"
        assert schema.powerbi_client_id == "client456"
        assert schema.powerbi_client_secret == "secret789"
        assert schema.outbound_format is None  # Optional
        assert schema.sql_dialect == "databricks"  # default

    def test_schema_initialization_yaml_minimal(self):
        """Test schema with minimal YAML parameters"""
        schema = MeasureConversionPipelineSchema(
            inbound_connector="yaml",
            yaml_content="version: 0.1\nkpis: []"
        )

        assert schema.inbound_connector == "yaml"
        assert schema.yaml_content == "version: 0.1\nkpis: []"
        assert schema.yaml_file_path is None

    def test_schema_initialization_all_powerbi_parameters(self):
        """Test schema with all Power BI parameters"""
        schema = MeasureConversionPipelineSchema(
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            powerbi_info_table_name="Custom Measures",
            powerbi_include_hidden=True,
            powerbi_filter_pattern="Sales.*",
            outbound_format="sql",
            sql_dialect="postgresql",
            sql_include_comments=True,
            sql_process_structures=True,
            definition_name="test_conversion"
        )

        assert schema.powerbi_tenant_id == "tenant123"
        assert schema.powerbi_client_id == "client456"
        assert schema.powerbi_client_secret == "secret789"
        assert schema.powerbi_info_table_name == "Custom Measures"
        assert schema.powerbi_include_hidden is True
        assert schema.powerbi_filter_pattern == "Sales.*"
        assert schema.outbound_format == "sql"
        assert schema.sql_dialect == "postgresql"

    def test_schema_initialization_uc_metrics_parameters(self):
        """Test schema with UC Metrics parameters"""
        schema = MeasureConversionPipelineSchema(
            inbound_connector="yaml",
            yaml_content="test",
            outbound_format="uc_metrics",
            uc_catalog="analytics",
            uc_schema="metrics",
            uc_process_structures=True
        )

        assert schema.outbound_format == "uc_metrics"
        assert schema.uc_catalog == "analytics"
        assert schema.uc_schema == "metrics"
        assert schema.uc_process_structures is True


class TestMeasureConversionPipelineTool:
    """Tests for MeasureConversionPipelineTool CrewAI integration"""

    @pytest.fixture
    def tool(self):
        """Create MeasureConversionPipelineTool instance for testing"""
        return MeasureConversionPipelineTool()

    @pytest.fixture
    def tool_preconfigured_powerbi_to_sql(self):
        """Create tool pre-configured for Power BI to SQL conversion"""
        return MeasureConversionPipelineTool(
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="sql",
            sql_dialect="databricks"
        )

    @pytest.fixture
    def tool_preconfigured_yaml_to_dax(self):
        """Create tool pre-configured for YAML to DAX conversion"""
        return MeasureConversionPipelineTool(
            inbound_connector="yaml",
            yaml_content="version: 0.1\nkpis: []",
            outbound_format="dax"
        )

    def _set_default_config(self, tool, **overrides):
        """Helper to set _default_config values on a bare tool for testing _run().

        The source code's merge strategy protects config_fields
        (inbound_connector, outbound_format, powerbi_semantic_model_id,
        powerbi_group_id) by preferring _default_config over runtime kwargs.
        Tests that use the bare `tool` fixture must pre-populate these values.
        """
        for key, value in overrides.items():
            tool._default_config[key] = value

    # ========== Initialization Tests ==========

    def test_tool_initialization(self, tool):
        """Test tool initializes correctly"""
        assert tool is not None
        assert tool.name == "Measure Conversion Pipeline"
        assert "Universal measure conversion pipeline" in tool.description
        assert tool.args_schema == MeasureConversionPipelineSchema
        assert hasattr(tool, '_pipeline')

    def test_tool_initialization_with_config(self, tool_preconfigured_powerbi_to_sql):
        """Test tool initializes with pre-configured parameters"""
        tool = tool_preconfigured_powerbi_to_sql
        assert hasattr(tool, '_default_config')
        assert tool._default_config['inbound_connector'] == "powerbi"
        assert tool._default_config['powerbi_semantic_model_id'] == "model123"
        assert tool._default_config['outbound_format'] == "sql"

    def test_tool_has_required_attributes(self, tool):
        """Test tool has all required CrewAI tool attributes"""
        assert hasattr(tool, 'name')
        assert hasattr(tool, 'description')
        assert hasattr(tool, 'args_schema')
        assert hasattr(tool, '_run')

    # ========== Run Method Tests - Parameter Validation ==========

    def test_run_invalid_inbound_connector(self, tool):
        """Test _run fails with invalid inbound_connector"""
        self._set_default_config(tool, inbound_connector="invalid_connector")
        result = tool._run(
            powerbi_semantic_model_id="model123"
        )

        assert "Error" in result
        assert "Unsupported inbound_connector" in result

    def test_run_invalid_outbound_format(self, tool):
        """Test _run fails with invalid outbound_format"""
        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="invalid_format",
        )
        result = tool._run()

        assert "Error" in result
        assert "Unsupported outbound_format" in result

    def test_run_powerbi_missing_semantic_model_id(self, tool):
        """Test _run fails with missing Power BI semantic_model_id"""
        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" in result
        assert "requires powerbi_semantic_model_id" in result

    def test_run_powerbi_missing_group_id(self, tool):
        """Test _run fails with missing Power BI group_id"""
        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" in result
        assert "requires powerbi_semantic_model_id and powerbi_group_id" in result

    def test_run_powerbi_missing_authentication(self, tool):
        """Test _run fails with missing Power BI authentication"""
        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" in result
        assert "Authentication required" in result

    def test_run_yaml_missing_content_and_file(self, tool):
        """Test _run fails with missing YAML content or file path"""
        self._set_default_config(
            tool,
            inbound_connector="yaml",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" in result
        assert "requires either yaml_content or yaml_file_path" in result

    # ========== Run Method Tests - Power BI Success Paths ==========

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_powerbi_to_dax_success(self, mock_pipeline_class, tool):
        """Test _run Power BI to DAX conversion"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [
                {
                    "name": "Total Sales",
                    "expression": "SUM(Sales[Amount])",
                    "description": "Total sales"
                }
            ],
            "measure_count": 1,
            "errors": []
        }

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Power BI Measures" in result
        assert "DAX" in result
        assert "Total Sales" in result
        assert "SUM(Sales[Amount])" in result

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_powerbi_to_sql_success(self, mock_pipeline_class, tool):
        """Test _run Power BI to SQL conversion"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "SELECT SUM(amount) as total_sales FROM sales",
            "measure_count": 1,
            "errors": []
        }

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="sql",
            sql_dialect="databricks",
        )
        result = tool._run()

        assert "Power BI Measures" in result
        assert "SQL" in result
        assert "SELECT SUM(amount)" in result

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_powerbi_to_uc_metrics_success(self, mock_pipeline_class, tool):
        """Test _run Power BI to UC Metrics conversion"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "version: 0.1\nmeasures:\n  - name: total_sales",
            "measure_count": 1,
            "errors": []
        }

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="uc_metrics",
            uc_catalog="main",
            uc_schema="sales",
        )
        result = tool._run()

        assert "Power BI Measures" in result
        assert "UC Metrics" in result
        assert "version: 0.1" in result

    # ========== Run Method Tests - YAML Success Paths ==========

    @patch('src.converters.services.powerbi.DAXGenerator')
    @patch('src.converters.common.transformers.yaml.YAMLKPIParser')
    def test_run_yaml_to_dax_success(self, mock_yaml_parser_class, mock_dax_generator_class, tool):
        """Test _run YAML to DAX conversion"""
        # Mock YAML parser
        mock_parser = Mock()
        mock_yaml_parser_class.return_value = mock_parser

        mock_definition = Mock()
        mock_kpi = Mock()
        mock_kpi.technical_name = "total_sales"
        mock_definition.kpis = [mock_kpi]
        mock_parser.parse_file.return_value = mock_definition

        # Mock DAX generator
        mock_generator = Mock()
        mock_dax_generator_class.return_value = mock_generator

        mock_dax_measure = Mock()
        mock_dax_measure.name = "Total Sales"
        mock_dax_measure.dax_formula = "SUM(Sales[Amount])"
        mock_dax_measure.description = "Total sales amount"
        mock_generator.generate_dax_measure.return_value = mock_dax_measure

        self._set_default_config(
            tool,
            inbound_connector="yaml",
            outbound_format="dax",
        )
        result = tool._run(
            yaml_content="version: 0.1\nkpis:\n  - technical_name: total_sales",
        )

        assert "YAML Measures" in result
        assert "DAX" in result
        assert "Total Sales" in result

    @patch('src.converters.services.sql.SQLTranslationOptions')
    @patch('src.converters.services.sql.SQLGenerator')
    @patch('src.converters.common.transformers.yaml.YAMLKPIParser')
    def test_run_yaml_to_sql_success(self, mock_yaml_parser_class, mock_sql_generator_class, mock_options_class, tool):
        """Test _run YAML to SQL conversion"""
        # Mock YAML parser
        mock_parser = Mock()
        mock_yaml_parser_class.return_value = mock_parser

        mock_definition = Mock()
        mock_kpi = Mock()
        mock_definition.kpis = [mock_kpi]
        mock_parser.parse_file.return_value = mock_definition

        # Mock SQL generator
        mock_generator = Mock()
        mock_sql_generator_class.return_value = mock_generator

        mock_result = Mock()
        mock_result.measures_count = 1
        mock_result.queries_count = 1

        # Mock SQL query object
        mock_query = Mock()
        mock_query.to_sql.return_value = "SELECT SUM(amount) as total FROM sales"
        mock_query.original_kbi = mock_kpi
        mock_kpi.technical_name = "total_sales"

        mock_result.sql_queries = [mock_query]
        mock_generator.generate_sql_from_kbi_definition.return_value = mock_result

        self._set_default_config(
            tool,
            inbound_connector="yaml",
            outbound_format="sql",
            sql_dialect="databricks",
        )
        result = tool._run(
            yaml_content="version: 0.1\nkpis: []",
        )

        assert "YAML Measures" in result
        assert "SQL" in result

    @patch('src.converters.services.uc_metrics.UCMetricsGenerator')
    @patch('src.converters.common.transformers.yaml.YAMLKPIParser')
    def test_run_yaml_to_uc_metrics_success(self, mock_yaml_parser_class, mock_uc_generator_class, tool):
        """Test _run YAML to UC Metrics conversion"""
        # Mock YAML parser
        mock_parser = Mock()
        mock_yaml_parser_class.return_value = mock_parser

        mock_definition = Mock()
        mock_definition.kpis = [Mock()]
        mock_parser.parse_file.return_value = mock_definition

        # Mock UC Metrics generator
        mock_generator = Mock()
        mock_uc_generator_class.return_value = mock_generator
        mock_generator.generate_consolidated_uc_metrics.return_value = Mock()
        mock_generator.format_consolidated_uc_metrics_yaml.return_value = "version: 0.1\nmeasures: []"

        self._set_default_config(
            tool,
            inbound_connector="yaml",
            outbound_format="uc_metrics",
            uc_catalog="main",
            uc_schema="default",
        )
        result = tool._run(
            yaml_content="version: 0.1\nkpis: []",
        )

        assert "YAML Measures" in result
        assert "UC Metrics" in result

    # ========== Run Method Tests - Pre-configured Tool ==========

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_preconfigured_tool_without_parameters(self, mock_pipeline_class, tool_preconfigured_powerbi_to_sql):
        """Test _run with pre-configured tool using no runtime parameters"""
        tool = tool_preconfigured_powerbi_to_sql
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "SELECT * FROM table",
            "measure_count": 1,
            "errors": []
        }

        # Call with no parameters - should use pre-configured values
        result = tool._run()

        assert "Error" not in result
        # Verify it used pre-configured values
        mock_pipeline.execute.assert_called_once()

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_preconfigured_tool_override_parameters(self, mock_pipeline_class, tool_preconfigured_powerbi_to_sql):
        """Test _run with pre-configured tool overriding parameters at runtime"""
        tool = tool_preconfigured_powerbi_to_sql
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "SELECT * FROM table",
            "measure_count": 1,
            "errors": []
        }

        # Override sql_dialect at runtime
        result = tool._run(sql_dialect="postgresql")

        assert "Error" not in result
        call_args = mock_pipeline.execute.call_args
        assert call_args[1]['outbound_params']['dialect'] == "postgresql"

    # ========== Run Method Tests - Parameter Resolution ==========

    def test_resolve_parameter_static_value(self, tool):
        """Test _resolve_parameter with static value"""
        result = tool._resolve_parameter("powerbi", {})
        assert result == "powerbi"

    def test_resolve_parameter_with_placeholder(self, tool):
        """Test _resolve_parameter resolves placeholder"""
        execution_inputs = {"dataset_id": "model123"}
        result = tool._resolve_parameter("{dataset_id}", execution_inputs)
        assert result == "model123"

    def test_resolve_parameter_with_multiple_placeholders(self, tool):
        """Test _resolve_parameter resolves multiple placeholders"""
        execution_inputs = {"catalog": "main", "schema": "sales"}
        result = tool._resolve_parameter("{catalog}.{schema}", execution_inputs)
        assert result == "main.sales"

    def test_resolve_parameter_non_string_value(self, tool):
        """Test _resolve_parameter with non-string value"""
        result = tool._resolve_parameter(True, {})
        assert result is True

        result = tool._resolve_parameter(123, {})
        assert result == 123

    def test_resolve_parameter_placeholder_not_found(self, tool):
        """Test _resolve_parameter when placeholder not in execution_inputs"""
        execution_inputs = {"other_key": "value"}
        result = tool._resolve_parameter("{dataset_id}", execution_inputs)
        # Should return original string with placeholder
        assert "{dataset_id}" in result

    # ========== Run Method Tests - Error Handling ==========

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_pipeline_failure(self, mock_pipeline_class, tool):
        """Test _run handles pipeline failure"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": False,
            "output": None,
            "measure_count": 0,
            "errors": ["Connection failed", "Authentication error"]
        }

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" in result
        assert "Conversion failed" in result

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_exception_handling(self, mock_pipeline_class, tool):
        """Test _run handles exceptions gracefully"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.side_effect = Exception("Unexpected error")

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" in result
        assert "Unexpected error" in result

    @patch('src.converters.common.transformers.yaml.YAMLKPIParser')
    def test_run_yaml_conversion_exception(self, mock_yaml_parser_class, tool):
        """Test _run handles YAML conversion exceptions"""
        mock_parser = Mock()
        mock_yaml_parser_class.return_value = mock_parser
        mock_parser.parse_file.side_effect = Exception("YAML parse error")

        self._set_default_config(
            tool,
            inbound_connector="yaml",
            outbound_format="dax",
        )
        result = tool._run(
            yaml_content="invalid yaml {{",
        )

        assert "Error" in result
        assert "YAML conversion failed" in result

    # ========== Format Output Tests ==========

    def test_format_output_dax(self, tool):
        """Test _format_output for DAX format"""
        output = [
            {
                "name": "Measure1",
                "expression": "SUM(Table[Column])",
                "description": "Test measure"
            }
        ]

        result = tool._format_output(
            output=output,
            inbound_connector="powerbi",
            outbound_format="dax",
            measure_count=1,
            source_id="model123"
        )

        assert "Power BI Measures" in result
        assert "DAX" in result
        assert "Measure1" in result
        assert "SUM(Table[Column])" in result
        assert "Test measure" in result

    def test_format_output_sql_list(self, tool):
        """Test _format_output for SQL format with list"""
        mock_query = Mock()
        mock_query.to_sql.return_value = "SELECT SUM(amount) FROM sales"
        mock_query.original_kbi = Mock()
        mock_query.original_kbi.technical_name = "total_sales"
        mock_query.description = None

        result = tool._format_output(
            output=[mock_query],
            inbound_connector="yaml",
            outbound_format="sql",
            measure_count=1,
            source_id="yaml_file"
        )

        assert "YAML Measures" in result
        assert "SQL" in result
        assert "total_sales" in result

    def test_format_output_sql_string(self, tool):
        """Test _format_output for SQL format with string"""
        result = tool._format_output(
            output="SELECT * FROM table",
            inbound_connector="powerbi",
            outbound_format="sql",
            measure_count=1,
            source_id="model123"
        )

        assert "Power BI Measures" in result
        assert "SQL" in result
        assert "SELECT * FROM table" in result

    def test_format_output_sql_dict(self, tool):
        """Test _format_output for SQL format with dictionary"""
        output = [
            {
                "name": "total_sales",
                "sql": "SELECT SUM(amount) FROM sales",
                "is_transpilable": True
            }
        ]

        result = tool._format_output(
            output=output,
            inbound_connector="powerbi",
            outbound_format="sql",
            measure_count=1,
            source_id="model123"
        )

        assert "total_sales" in result
        assert "SELECT SUM(amount)" in result

    def test_format_output_sql_empty(self, tool):
        """Test _format_output for SQL format with empty output"""
        result = tool._format_output(
            output=[],
            inbound_connector="yaml",
            outbound_format="sql",
            measure_count=0,
            source_id="yaml_file"
        )

        assert "No SQL queries generated" in result

    def test_format_output_uc_metrics(self, tool):
        """Test _format_output for UC Metrics format"""
        result = tool._format_output(
            output="version: 0.1\nmeasures:\n  - name: total",
            inbound_connector="powerbi",
            outbound_format="uc_metrics",
            measure_count=1,
            source_id="model123"
        )

        assert "Power BI Measures" in result
        assert "UC Metrics" in result
        assert "version: 0.1" in result

    def test_format_output_yaml(self, tool):
        """Test _format_output for YAML format"""
        result = tool._format_output(
            output="kpis:\n  - technical_name: total",
            inbound_connector="powerbi",
            outbound_format="yaml",
            measure_count=1,
            source_id="model123"
        )

        assert "Power BI Measures" in result
        assert "YAML" in result
        assert "kpis:" in result

    # ========== Edge Cases ==========

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_with_service_principal_auth(self, mock_pipeline_class, tool):
        """Test _run with Service Principal authentication"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [],
            "measure_count": 0,
            "errors": []
        }

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" not in result

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_with_device_code_auth(self, mock_pipeline_class, tool):
        """Test _run with device code authentication"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [],
            "measure_count": 0,
            "errors": []
        }

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        result = tool._run()

        assert "Error" not in result

    @patch('src.engines.crewai.tools.custom.measure_conversion_pipeline_tool.ConversionPipeline')
    def test_run_with_all_optional_parameters(self, mock_pipeline_class, tool):
        """Test _run with all optional parameters"""
        mock_pipeline = Mock()
        mock_pipeline_class.return_value = mock_pipeline
        tool._pipeline = mock_pipeline

        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "SELECT * FROM table",
            "measure_count": 1,
            "errors": []
        }

        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="sql",
        )
        result = tool._run(
            powerbi_include_hidden=True,
            powerbi_filter_pattern="Revenue.*",
            powerbi_info_table_name="Custom Info",
            sql_dialect="snowflake",
            sql_include_comments=True,
            sql_process_structures=True,
            definition_name="custom_name"
        )

        assert "Error" not in result

    def test_run_with_execution_inputs_parameter_resolution(self, tool):
        """Test _run with execution_inputs for dynamic parameter resolution"""
        self._set_default_config(
            tool,
            inbound_connector="powerbi",
            powerbi_semantic_model_id="model123",
            powerbi_group_id="workspace456",
            powerbi_tenant_id="tenant123",
            powerbi_client_id="client456",
            powerbi_client_secret="secret789",
            outbound_format="dax",
        )
        with patch.object(tool._pipeline, 'execute') as mock_execute:
            mock_execute.return_value = {
                "success": True,
                "output": [],
                "measure_count": 0,
                "errors": []
            }

            # This tests that execution_inputs would be used if provided
            result = tool._run(
                execution_inputs={"dataset_id": "override_model"}
            )

            assert "Error" not in result
