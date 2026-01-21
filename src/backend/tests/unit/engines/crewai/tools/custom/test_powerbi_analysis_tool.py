"""
Unit tests for engines/crewai/tools/custom/powerbi_analysis_tool.py

Tests CrewAI integration tool for complex Power BI analysis via Databricks jobs.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from src.engines.crewai.tools.custom.powerbi_analysis_tool import (
    PowerBIAnalysisToolSchema,
    PowerBIAnalysisTool,
    _run_async_in_sync_context
)


class TestPowerBIAnalysisToolSchema:
    """Tests for PowerBIAnalysisToolSchema Pydantic model"""

    def test_schema_initialization_minimal(self):
        """Test schema with minimal required parameters"""
        schema = PowerBIAnalysisToolSchema(
            dashboard_id="model123",
            questions=["What is total revenue?"]
        )

        assert schema.dashboard_id == "model123"
        assert schema.questions == ["What is total revenue?"]
        assert schema.workspace_id is None
        assert schema.dax_statement is None
        assert schema.job_id is None
        assert schema.additional_params is None

    def test_schema_initialization_all_parameters(self):
        """Test schema with all parameters"""
        schema = PowerBIAnalysisToolSchema(
            dashboard_id="model123",
            questions=["What is total revenue?"],
            workspace_id="workspace456",
            dax_statement="EVALUATE 'Sales'",
            job_id=12345,
            additional_params={
                "tenant_id": "tenant-id",
                "client_id": "client-id",
                "auth_method": "service_principal"
            }
        )

        assert schema.dashboard_id == "model123"
        assert schema.questions == ["What is total revenue?"]
        assert schema.workspace_id == "workspace456"
        assert schema.dax_statement == "EVALUATE 'Sales'"
        assert schema.job_id == 12345
        assert schema.additional_params["tenant_id"] == "tenant-id"
        assert schema.additional_params["client_id"] == "client-id"

    def test_schema_validation_requires_questions_or_dax(self):
        """Test schema validation requires either questions or dax_statement"""
        # Should succeed with questions
        schema = PowerBIAnalysisToolSchema(
            dashboard_id="model123",
            questions=["What is revenue?"]
        )
        assert schema is not None

        # Should succeed with dax_statement
        schema = PowerBIAnalysisToolSchema(
            dashboard_id="model123",
            questions=[],
            dax_statement="EVALUATE 'Sales'"
        )
        assert schema is not None

    def test_schema_validation_fails_without_questions_and_dax(self):
        """Test schema validation fails without questions or dax_statement"""
        with pytest.raises(ValueError, match="Either 'questions' or 'dax_statement' must be provided"):
            PowerBIAnalysisToolSchema(
                dashboard_id="model123",
                questions=[]
            )


class TestPowerBIAnalysisTool:
    """Tests for PowerBIAnalysisTool CrewAI integration"""

    @pytest.fixture
    def tool(self):
        """Create PowerBIAnalysisTool instance for testing"""
        return PowerBIAnalysisTool(
            group_id="test_group",
            databricks_job_id=12345,
            tenant_id="tenant123",
            client_id="client456"
        )

    @pytest.fixture
    def tool_no_job_id(self):
        """Create PowerBIAnalysisTool without pre-configured job ID"""
        return PowerBIAnalysisTool()

    # ========== Initialization Tests ==========

    def test_tool_initialization(self, tool):
        """Test tool initializes correctly"""
        assert tool is not None
        assert tool.name == "Power BI Analysis (Databricks)"
        assert "Execute complex Power BI analysis" in tool.description
        assert tool.args_schema == PowerBIAnalysisToolSchema
        assert tool._group_id == "test_group"
        assert tool._databricks_job_id == 12345
        assert tool._tenant_id == "tenant123"
        assert tool._client_id == "client456"

    def test_tool_initialization_with_defaults(self):
        """Test tool initializes with default values"""
        tool = PowerBIAnalysisTool()

        assert tool._group_id is None
        assert tool._databricks_job_id is None
        assert tool._tenant_id is None
        assert tool._client_id is None
        assert tool._auth_method == "service_principal"

    def test_tool_initialization_with_all_parameters(self):
        """Test tool initializes with all parameters"""
        tool = PowerBIAnalysisTool(
            group_id="test_group",
            databricks_job_id=12345,
            tenant_id="tenant123",
            client_id="client456",
            workspace_id="workspace789",
            semantic_model_id="model999",
            auth_method="device_code"
        )

        assert tool._workspace_id == "workspace789"
        assert tool._semantic_model_id == "model999"
        assert tool._auth_method == "device_code"

    # ========== Run Method Tests ==========

    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context')
    def test_run_calls_execute_analysis(self, mock_async_wrapper, tool):
        """Test _run calls _execute_analysis via async wrapper"""
        mock_async_wrapper.return_value = "Analysis complete"

        result = tool._run(
            dashboard_id="model123",
            questions=["What is revenue?"]
        )

        assert result == "Analysis complete"
        mock_async_wrapper.assert_called_once()

    @patch.object(PowerBIAnalysisTool, '_execute_analysis')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context')
    def test_run_passes_parameters_to_execute_analysis(self, mock_async_wrapper, mock_execute, tool):
        """Test _run passes all parameters to _execute_analysis"""
        async def mock_coroutine(**kwargs):
            return "Result"

        mock_async_wrapper.side_effect = lambda coro: asyncio.run(coro)
        mock_execute.return_value = mock_coroutine()

        tool._run(
            dashboard_id="model123",
            questions=["What is revenue?"],
            workspace_id="workspace456",
            job_id=99999,
            additional_params={"tenant_id": "tenant"}
        )

        # Verify parameters were passed
        assert mock_async_wrapper.called

    # ========== Execute Analysis Tests ==========

    @pytest.mark.asyncio
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.DatabricksJobsTool')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.get_auth_context')
    async def test_execute_analysis_no_job_id_returns_instructions(self, mock_auth, mock_databricks_tool, tool_no_job_id):
        """Test _execute_analysis returns setup instructions when no job_id"""
        mock_auth.return_value = None

        result = await tool_no_job_id._execute_analysis(
            dashboard_id="model123",
            questions=["What is revenue?"]
        )

        assert "Setup Required" in result
        assert "Create a Databricks job" in result

    @pytest.mark.asyncio
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.DatabricksJobsTool')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.get_auth_context')
    async def test_execute_analysis_with_job_id_triggers_job(self, mock_auth, mock_databricks_tool_class, tool):
        """Test _execute_analysis triggers Databricks job when job_id configured"""
        # Mock auth context
        mock_auth_context = Mock()
        mock_auth_context.workspace_url = "https://example.databricks.com"
        mock_auth.return_value = mock_auth_context

        # Mock DatabricksJobsTool
        mock_databricks_tool = Mock()
        mock_databricks_tool_class.return_value = mock_databricks_tool
        mock_databricks_tool._run.return_value = "✅ Job run started successfully\nRun ID: 67890\nStatus: RUNNING"
        mock_databricks_tool._make_api_call = AsyncMock()

        # Mock task status check - return SUCCESS immediately
        async def mock_check_task_status(tool_instance, run_id, task_key):
            return "SUCCESS"

        with patch.object(PowerBIAnalysisTool, '_check_task_status', new=mock_check_task_status):
            # Mock notebook output extraction
            async def mock_get_notebook_output(tool_instance, run_id, task_key):
                return {
                    'status': 'success',
                    'execution_time': '5.2s',
                    'generated_dax': 'EVALUATE Sales',
                    'rows_returned': 10,
                    'columns': ['Amount'],
                    'result_data': [{'Amount': 1000}]
                }

            with patch.object(PowerBIAnalysisTool, '_get_notebook_output', new=mock_get_notebook_output):
                result = await tool._execute_analysis(
                    dashboard_id="model123",
                    questions=["What is revenue?"]
                )

        assert "Analysis Complete" in result
        assert "model123" in result

    # ========== Extract Run ID Tests ==========

    def test_extract_run_id_success(self, tool):
        """Test _extract_run_id extracts run ID correctly"""
        result_text = "✅ Job run started successfully\nRun ID: 12345\nStatus: RUNNING"
        run_id = tool._extract_run_id(result_text)

        assert run_id == 12345

    def test_extract_run_id_not_found(self, tool):
        """Test _extract_run_id returns None when no run ID"""
        result_text = "Job started but no run ID"
        run_id = tool._extract_run_id(result_text)

        assert run_id is None

    def test_extract_run_id_multiple_numbers(self, tool):
        """Test _extract_run_id extracts correct run ID with multiple numbers"""
        result_text = "Job 123 started\nRun ID: 45678\nTimeout: 300"
        run_id = tool._extract_run_id(result_text)

        assert run_id == 45678

    # ========== Check Task Status Tests ==========

    @pytest.mark.asyncio
    async def test_check_task_status_multi_task_success(self, tool):
        """Test _check_task_status with multi-task job returning SUCCESS"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [
                {
                    "task_key": "pbi_e2e_pipeline",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS"
                    }
                }
            ]
        })

        status = await tool._check_task_status(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert status == "SUCCESS"

    @pytest.mark.asyncio
    async def test_check_task_status_multi_task_running(self, tool):
        """Test _check_task_status with multi-task job still running"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [
                {
                    "task_key": "pbi_e2e_pipeline",
                    "state": {
                        "life_cycle_state": "RUNNING",
                        "result_state": ""
                    }
                }
            ]
        })

        status = await tool._check_task_status(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert status == "RUNNING"

    @pytest.mark.asyncio
    async def test_check_task_status_multi_task_failed(self, tool):
        """Test _check_task_status with multi-task job failed"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [
                {
                    "task_key": "pbi_e2e_pipeline",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED"
                    }
                }
            ]
        })

        status = await tool._check_task_status(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert status == "FAILED"

    @pytest.mark.asyncio
    async def test_check_task_status_single_task_job(self, tool):
        """Test _check_task_status with single-task job"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [],  # No tasks means single-task job
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS"
            }
        })

        status = await tool._check_task_status(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert status == "SUCCESS"

    @pytest.mark.asyncio
    async def test_check_task_status_task_not_found(self, tool):
        """Test _check_task_status when task key not found"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [
                {
                    "task_key": "other_task",
                    "state": {"life_cycle_state": "RUNNING", "result_state": ""}
                }
            ],
            "state": {
                "life_cycle_state": "RUNNING",
                "result_state": ""
            }
        })

        status = await tool._check_task_status(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        # Should fall back to main run status
        assert status == "RUNNING"

    @pytest.mark.asyncio
    async def test_check_task_status_error_handling(self, tool):
        """Test _check_task_status handles errors gracefully"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(side_effect=Exception("API error"))

        status = await tool._check_task_status(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert status == "ERROR"

    # ========== Get Notebook Output Tests ==========

    @pytest.mark.asyncio
    async def test_get_notebook_output_multi_task_success(self, tool):
        """Test _get_notebook_output extracts output from multi-task job"""
        mock_databricks_tool = Mock()

        # Mock run details API call
        mock_databricks_tool._make_api_call = AsyncMock()
        mock_databricks_tool._make_api_call.side_effect = [
            # First call: run details
            {
                "tasks": [
                    {
                        "task_key": "pbi_e2e_pipeline",
                        "run_id": 99999
                    }
                ]
            },
            # Second call: task output
            {
                "notebook_output": {
                    "result": '{"status": "success", "execution_time": "5s", "pipeline_steps": {"step_3_execution": {"rows_returned": 5, "columns": ["Amount"], "result_data": [{"Amount": 100}]}}}'
                }
            }
        ]

        result = await tool._get_notebook_output(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert result is not None
        assert result['status'] == 'success'
        assert result['rows_returned'] == 5
        assert len(result['result_data']) == 1

    @pytest.mark.asyncio
    async def test_get_notebook_output_no_result_text(self, tool):
        """Test _get_notebook_output returns None when no result text"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [],
            "notebook_output": {
                "result": ""
            }
        })

        result = await tool._get_notebook_output(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_notebook_output_invalid_json(self, tool):
        """Test _get_notebook_output handles invalid JSON"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [],
            "notebook_output": {
                "result": "not valid json {{"
            }
        })

        result = await tool._get_notebook_output(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_notebook_output_with_notebook_exited_pattern(self, tool):
        """Test _get_notebook_output extracts JSON from 'Notebook exited:' pattern"""
        mock_databricks_tool = Mock()
        mock_databricks_tool._make_api_call = AsyncMock(return_value={
            "tasks": [],
            "notebook_output": {
                "result": 'Notebook exited: {"status": "success", "pipeline_steps": {"step_3_execution": {"rows_returned": 3, "columns": ["Value"], "result_data": [{"Value": 42}]}}}'
            }
        })

        result = await tool._get_notebook_output(mock_databricks_tool, 12345, "pbi_e2e_pipeline")

        assert result is not None
        assert result['status'] == 'success'
        assert result['rows_returned'] == 3

    # ========== Format Analysis Result Tests ==========

    def test_format_analysis_result_complete(self, tool):
        """Test _format_analysis_result formats complete result"""
        result_data = {
            'status': 'success',
            'execution_time': '5.2s',
            'generated_dax': 'EVALUATE Sales',
            'rows_returned': 2,
            'columns': ['Amount', 'Date'],
            'result_data': [
                {'Amount': 100, 'Date': '2024-01-01'},
                {'Amount': 200, 'Date': '2024-01-02'}
            ]
        }

        formatted = tool._format_analysis_result("model123", "What is revenue?", result_data)

        assert "Analysis Complete" in formatted
        assert "model123" in formatted
        assert "What is revenue?" in formatted
        assert "5.2s" in formatted
        assert "EVALUATE Sales" in formatted
        assert "2" in formatted  # rows_returned
        assert "Amount" in formatted
        assert "Date" in formatted

    def test_format_analysis_result_no_dax(self, tool):
        """Test _format_analysis_result without generated DAX"""
        result_data = {
            'status': 'success',
            'execution_time': '3s',
            'rows_returned': 1,
            'columns': ['Value'],
            'result_data': [{'Value': 42}]
        }

        formatted = tool._format_analysis_result("model123", "Test question", result_data)

        assert "Analysis Complete" in formatted
        assert "3s" in formatted
        assert "Value" in formatted

    def test_format_analysis_result_no_data(self, tool):
        """Test _format_analysis_result with no result data"""
        result_data = {
            'status': 'success',
            'execution_time': '1s',
            'rows_returned': 0,
            'columns': [],
            'result_data': []
        }

        formatted = tool._format_analysis_result("model123", "Test", result_data)

        assert "No result data returned" in formatted

    # ========== Format Setup Instructions Tests ==========

    def test_format_setup_instructions(self, tool_no_job_id):
        """Test _format_setup_instructions returns setup guide"""
        result = tool_no_job_id._format_setup_instructions(
            "model123",
            "What is revenue?",
            {"question": "What is revenue?", "semantic_model_id": "model123"}
        )

        assert "Setup Required" in result
        assert "Create a Databricks job" in result
        assert "model123" in result
        assert "What is revenue?" in result

    # ========== Async Context Helper Tests ==========

    def test_run_async_in_sync_context_no_loop(self):
        """Test _run_async_in_sync_context creates new loop when none exists"""
        async def test_coroutine():
            return "test_result"

        result = _run_async_in_sync_context(test_coroutine())

        assert result == "test_result"

    # ========== Integration Tests ==========

    @pytest.mark.asyncio
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.DatabricksJobsTool')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.get_auth_context')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.ApiKeysService')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.async_session_factory')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.EncryptionUtils')
    async def test_execute_analysis_full_workflow(
        self, mock_encryption, mock_session_factory, mock_api_keys_service,
        mock_auth, mock_databricks_tool_class, tool
    ):
        """Test complete _execute_analysis workflow"""
        # Mock auth context
        mock_auth_context = Mock()
        mock_auth_context.workspace_url = "https://example.databricks.com"
        mock_auth.return_value = mock_auth_context

        # Mock session and API keys
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_service = Mock()
        mock_api_keys_service.return_value = mock_service
        mock_service.find_by_name = AsyncMock(return_value=None)

        # Mock DatabricksJobsTool
        mock_databricks_tool = Mock()
        mock_databricks_tool_class.return_value = mock_databricks_tool
        mock_databricks_tool._run.return_value = "✅ Job run started successfully\nRun ID: 99999\nStatus: RUNNING"

        # Mock task status - return SUCCESS
        async def mock_check_task_status(tool_instance, run_id, task_key):
            return "SUCCESS"

        with patch.object(PowerBIAnalysisTool, '_check_task_status', new=mock_check_task_status):
            # Mock notebook output
            async def mock_get_notebook_output(tool_instance, run_id, task_key):
                return {
                    'status': 'success',
                    'execution_time': '10s',
                    'rows_returned': 5,
                    'columns': ['Revenue'],
                    'result_data': [{'Revenue': 1000}]
                }

            with patch.object(PowerBIAnalysisTool, '_get_notebook_output', new=mock_get_notebook_output):
                result = await tool._execute_analysis(
                    dashboard_id="model123",
                    questions=["What is total revenue?"],
                    additional_params={
                        "tenant_id": "tenant123",
                        "client_id": "client456"
                    }
                )

        assert "Analysis Complete" in result
        assert "model123" in result

    # ========== Error Handling Tests ==========

    @pytest.mark.asyncio
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.DatabricksJobsTool')
    @patch('src.engines.crewai.tools.custom.powerbi_analysis_tool.get_auth_context')
    async def test_execute_analysis_exception_handling(self, mock_auth, mock_databricks_tool_class, tool):
        """Test _execute_analysis handles exceptions"""
        mock_auth.side_effect = Exception("Unexpected error")

        result = await tool._execute_analysis(
            dashboard_id="model123",
            questions=["Test"]
        )

        assert "Error executing analysis" in result
        assert "Unexpected error" in result
