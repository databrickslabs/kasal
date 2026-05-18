import unittest
import asyncio
from unittest.mock import MagicMock, Mock, patch, AsyncMock, call
import json
import aiohttp
import base64
from datetime import datetime
import os

from src.engines.crewai.tools.custom.databricks_jobs_tool import (
    DatabricksJobsTool, 
    DatabricksJobsToolSchema
)


class TestDatabricksJobsToolSchema(unittest.TestCase):
    """Unit tests for DatabricksJobsToolSchema with 100% coverage"""

    def test_valid_list_action(self):
        """Test creating a valid schema for list action"""
        schema = DatabricksJobsToolSchema(
            action="list",
            limit=10
        )
        
        self.assertEqual(schema.action, "list")
        self.assertEqual(schema.limit, 10)
        self.assertIsNone(schema.job_id)
        self.assertIsNone(schema.run_id)
        self.assertIsNone(schema.job_config)

    def test_valid_list_my_jobs_action(self):
        """Test creating a valid schema for list_my_jobs action"""
        schema = DatabricksJobsToolSchema(
            action="list_my_jobs",
            limit=15,
            name_filter="test"
        )
        
        self.assertEqual(schema.action, "list_my_jobs")
        self.assertEqual(schema.limit, 15)
        self.assertEqual(schema.name_filter, "test")

    def test_valid_get_action(self):
        """Test creating a valid schema for get action"""
        schema = DatabricksJobsToolSchema(
            action="get",
            job_id=123
        )
        
        self.assertEqual(schema.action, "get")
        self.assertEqual(schema.job_id, 123)

    def test_valid_get_notebook_action(self):
        """Test creating a valid schema for get_notebook action"""
        schema = DatabricksJobsToolSchema(
            action="get_notebook",
            job_id=456
        )
        
        self.assertEqual(schema.action, "get_notebook")
        self.assertEqual(schema.job_id, 456)

    def test_valid_run_action(self):
        """Test creating a valid schema for run action"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=789,
            job_params={"key": "value"}
        )
        
        self.assertEqual(schema.action, "run")
        self.assertEqual(schema.job_id, 789)
        self.assertEqual(schema.job_params, {"key": "value"})

    def test_valid_monitor_action(self):
        """Test creating a valid schema for monitor action"""
        schema = DatabricksJobsToolSchema(
            action="monitor",
            run_id=999
        )
        
        self.assertEqual(schema.action, "monitor")
        self.assertEqual(schema.run_id, 999)

    def test_valid_create_action(self):
        """Test creating a valid schema for create action"""
        job_config = {
            "name": "Test Job",
            "tasks": [{"task_key": "task1", "notebook_task": {"notebook_path": "/test"}}]
        }
        
        schema = DatabricksJobsToolSchema(
            action="create",
            job_config=job_config
        )
        
        self.assertEqual(schema.action, "create")
        self.assertEqual(schema.job_config, job_config)

    def test_case_insensitive_action(self):
        """Test that action validation is case insensitive"""
        # Test uppercase
        schema = DatabricksJobsToolSchema(action="LIST", limit=5)
        self.assertEqual(schema.action, "LIST")
        
        # Test mixed case
        schema = DatabricksJobsToolSchema(action="LiSt_My_JoBs")
        self.assertEqual(schema.action, "LiSt_My_JoBs")

    def test_invalid_action(self):
        """Test schema validation with invalid action"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="invalid")
        
        self.assertIn("Invalid action 'invalid'", str(cm.exception))

    def test_get_action_missing_job_id(self):
        """Test schema validation for get action without job_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="get")
        
        self.assertIn("job_id is required for action 'get'", str(cm.exception))

    def test_get_notebook_action_missing_job_id(self):
        """Test get_notebook action without job_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="get_notebook")
        
        self.assertIn("job_id is required for action 'get_notebook'", str(cm.exception))

    def test_run_action_missing_job_id(self):
        """Test schema validation for run action without job_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="run")
        
        self.assertIn("job_id is required for action 'run'", str(cm.exception))

    def test_monitor_action_missing_run_id(self):
        """Test schema validation for monitor action without run_id"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="monitor")
        
        self.assertIn("run_id is required for action 'monitor'", str(cm.exception))

    def test_create_action_missing_job_config(self):
        """Test schema validation for create action without job_config"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(action="create")
        
        self.assertIn("job_config is required for action 'create'", str(cm.exception))

    def test_schema_default_limit(self):
        """Test schema uses default limit when not specified"""
        schema = DatabricksJobsToolSchema(action="list")
        self.assertEqual(schema.limit, 20)

    def test_job_params_validation_dict(self):
        """Test job_params validation with dict"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params={"param1": "value1", "param2": 123}
        )
        self.assertEqual(schema.job_params, {"param1": "value1", "param2": 123})

    def test_job_params_validation_list(self):
        """Test job_params validation with list"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params=["--arg1", "value1", "--arg2", "value2"]
        )
        self.assertEqual(schema.job_params, ["--arg1", "value1", "--arg2", "value2"])

    def test_job_params_validation_invalid_type(self):
        """Test job_params validation with invalid type"""
        with self.assertRaises(ValueError) as cm:
            DatabricksJobsToolSchema(
                action="run",
                job_id=123,
                job_params="invalid_string"
            )
        # Pydantic v2 gives a different error message
        self.assertIn("validation error", str(cm.exception).lower())

    def test_job_params_validation_none(self):
        """Test job_params validation with None (should pass)"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params=None
        )
        self.assertIsNone(schema.job_params)

    def test_job_params_validation_empty_dict(self):
        """Test job_params validation with empty dict"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params={}
        )
        self.assertEqual(schema.job_params, {})

    def test_job_params_validation_empty_list(self):
        """Test job_params validation with empty list"""
        schema = DatabricksJobsToolSchema(
            action="run",
            job_id=123,
            job_params=[]
        )
        self.assertEqual(schema.job_params, [])

    def test_name_filter_with_list(self):
        """Test name_filter parameter with list action"""
        schema = DatabricksJobsToolSchema(
            action="list",
            name_filter="search_term"
        )
        self.assertEqual(schema.name_filter, "search_term")

    def test_name_filter_with_list_my_jobs(self):
        """Test name_filter parameter with list_my_jobs action"""
        schema = DatabricksJobsToolSchema(
            action="list_my_jobs",
            name_filter="my_job"
        )
        self.assertEqual(schema.name_filter, "my_job")

    def test_all_optional_fields_none(self):
        """Test schema with all optional fields as None"""
        schema = DatabricksJobsToolSchema(
            action="list",
            job_id=None,
            run_id=None,
            job_config=None,
            limit=None,
            name_filter=None,
            job_params=None
        )
        self.assertEqual(schema.action, "list")
        self.assertIsNone(schema.job_id)
        self.assertIsNone(schema.run_id)
        self.assertIsNone(schema.job_config)
        self.assertIsNone(schema.limit)  # Explicitly set to None
        self.assertIsNone(schema.name_filter)
        self.assertIsNone(schema.job_params)


class TestDatabricksJobsTool(unittest.TestCase):
    """Unit tests for DatabricksJobsTool with 100% coverage"""

    def setUp(self):
        """Set up test fixtures"""
        self.tool_config = {
            "DATABRICKS_HOST": "test-workspace.cloud.databricks.com",
            "DATABRICKS_API_KEY": "test-api-key"
        }

    def tearDown(self):
        """Clean up after tests"""
        # Reset any environment variables that might have been set
        for key in ['DATABRICKS_HOST', 'DATABRICKS_API_KEY', 'DATABRICKS_TOKEN']:
            if key in os.environ:
                del os.environ[key]

    def test_tool_initialization_with_config(self):
        """Test DatabricksJobsTool initialization with config"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "test-api-key")
        # Test single execution control attributes
        self.assertIsNone(tool.max_usage_count)
        self.assertEqual(tool.current_usage_count, 0)
        # Global tracking is tested separately in single_execution tests

    def test_tool_initialization_empty_config(self):
        """Test initialization with empty config"""
        tool = DatabricksJobsTool(tool_config={})
        self.assertIsNotNone(tool)

    def test_tool_initialization_none_config(self):
        """Test initialization with None config"""
        tool = DatabricksJobsTool(tool_config=None)
        self.assertIsNotNone(tool)

    def test_tool_initialization_with_parameter(self):
        """Test initialization with databricks_host parameter"""
        tool = DatabricksJobsTool(
            databricks_host="param-workspace.cloud.databricks.com",
            tool_config={"DATABRICKS_API_KEY": "test-key"}
        )
        
        self.assertEqual(tool._host, "param-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "test-key")

    def test_tool_initialization_lowercase_host_key(self):
        """Test initialization with lowercase databricks_host in config"""
        tool = DatabricksJobsTool(
            tool_config={
                "databricks_host": "lowercase-host.databricks.com",
                "DATABRICKS_API_KEY": "test-key"
            }
        )
        
        self.assertEqual(tool._host, "lowercase-host.databricks.com")

    def test_tool_initialization_token_key(self):
        """Test initialization with 'token' key in config"""
        tool = DatabricksJobsTool(
            tool_config={
                "DATABRICKS_HOST": "test.databricks.com",
                "token": "test-token-key"
            }
        )
        
        self.assertEqual(tool._token, "test-token-key")

    def test_initialization_with_pat_config(self):
        """Test initialization with PAT token in config"""
        tool_config = {
            "DATABRICKS_HOST": "test-workspace.cloud.databricks.com",
            "DATABRICKS_API_KEY": "test-pat-token"
        }
        
        tool = DatabricksJobsTool(tool_config=tool_config)
        
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "test-pat-token")

    def test_host_processing_https(self):
        """Test host URL processing with https prefix"""
        tool = DatabricksJobsTool(databricks_host="https://test-workspace.cloud.databricks.com/")
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")

    def test_host_processing_http(self):
        """Test host URL processing with http prefix"""
        tool = DatabricksJobsTool(databricks_host="http://test-workspace.cloud.databricks.com")
        self.assertEqual(tool._host, "test-workspace.cloud.databricks.com")

    def test_host_processing_list(self):
        """Test host processing when provided as list"""
        tool_config = {
            "DATABRICKS_HOST": ["workspace1.cloud.databricks.com", "workspace2.cloud.databricks.com"],
            "DATABRICKS_API_KEY": "test-key"
        }
        tool = DatabricksJobsTool(tool_config=tool_config)
        self.assertEqual(tool._host, "workspace1.cloud.databricks.com")

    def test_host_processing_empty_list(self):
        """Test host processing with empty list"""
        tool_config = {
            "DATABRICKS_HOST": [],
            "DATABRICKS_API_KEY": "test-key"
        }
        tool = DatabricksJobsTool(tool_config=tool_config)
        # Should fall back to default
        self.assertEqual(tool._host, "your-workspace.cloud.databricks.com")

    def test_token_masking_short_token(self):
        """Test token masking with short token"""
        tool = DatabricksJobsTool(
            tool_config={
                "DATABRICKS_HOST": "test.com",
                "DATABRICKS_API_KEY": "short"
            }
        )
        self.assertEqual(tool._token, "short")


    def test_environment_variable_fallback(self):
        """Test authentication via tool_config"""
        # Provide authentication via tool_config (simulating how the tool is actually used)
        tool_config = {
            'DATABRICKS_HOST': 'env-workspace.cloud.databricks.com',
            'DATABRICKS_API_KEY': 'env-api-key'
        }

        tool = DatabricksJobsTool(tool_config=tool_config)

        self.assertEqual(tool._host, "env-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "env-api-key")

    def test_environment_variable_databricks_token(self):
        """Test authentication via tool_config with DATABRICKS_TOKEN"""
        # Provide authentication via tool_config (simulating how the tool is actually used)
        tool_config = {
            'DATABRICKS_HOST': 'env-workspace.cloud.databricks.com',
            'token': 'env-token'  # Using 'token' key as per the tool's __init__ logic
        }

        tool = DatabricksJobsTool(tool_config=tool_config)

        self.assertEqual(tool._host, "env-workspace.cloud.databricks.com")
        self.assertEqual(tool._token, "env-token")

    def test_authentication_validation_no_auth(self):
        """Test authentication validation with no auth"""
        tool = DatabricksJobsTool(token_required=True)
        tool._token = None
        
        result = tool._run(action="list")
        
        self.assertIn("no authentication available", result)

    def test_authentication_validation_with_token_required_false(self):
        """Test with token_required=False"""
        tool = DatabricksJobsTool(token_required=False)
        # Should not show warning
        self.assertIsNotNone(tool)

    def test_invalid_action_error(self):
        """Test handling of invalid actions"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._run(action="invalid_action")
        self.assertIn("Invalid action 'invalid_action'", result)

    def test_run_with_validation_error(self):
        """Test _run with validation error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Missing required job_id for get action
        result = tool._run(action="get")
        self.assertIn("job_id is required", result)

    def test_run_with_exception_in_action(self):
        """Test _run with exception during action execution"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        with patch.object(tool, '_list_jobs', side_effect=Exception("Test error")):
            result = tool._run(action="list")
            self.assertIn("Error executing Databricks Jobs action", result)
            self.assertIn("Test error", result)

    def test_run_with_unknown_action_after_validation(self):
        """Test _run with action that fails validation"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._run(action="fake_action")
        self.assertIn("Error executing Databricks Jobs action", result)
        self.assertIn("Invalid action 'fake_action'", result)

    def test_run_with_timing_over_2_seconds(self):
        """Test that timing info is added when execution takes > 2 seconds"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock a slow operation
        async def slow_list(*args, **kwargs):
            return "Results"
        
        with patch.object(tool, '_list_jobs', new_callable=AsyncMock, return_value="Results"):
            # Patch time.time in the tool module: first call returns 0 (start),
            # all subsequent calls return 2.5 (simulates >2s elapsed).
            _calls = [0]
            def _mock_time():
                val = _calls[0]
                _calls[0] = 2.5
                return val
            with patch('src.engines.crewai.tools.custom.databricks_jobs_tool.time.time',
                       side_effect=_mock_time):
                result = tool._run(action="list")
                self.assertIn("⏱️ Performance: Action took", result)
                self.assertIn("Results", result)

    def test_run_all_actions(self):
        """Test _run with all valid actions"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock all async methods
        async def mock_async_method(*args, **kwargs):
            return "Mocked result"
        
        with patch.object(tool, '_list_jobs', side_effect=mock_async_method):
            result = tool._run(action="list")
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_list_my_jobs', side_effect=mock_async_method):
            result = tool._run(action="list_my_jobs")
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_get_job', side_effect=mock_async_method):
            result = tool._run(action="get", job_id=123)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_get_notebook_content', side_effect=mock_async_method):
            result = tool._run(action="get_notebook", job_id=123)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_run_job', side_effect=mock_async_method):
            result = tool._run(action="run", job_id=123)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_monitor_run', side_effect=mock_async_method):
            result = tool._run(action="monitor", run_id=456)
            self.assertIn("Mocked result", result)
        
        with patch.object(tool, '_create_job', side_effect=mock_async_method):
            result = tool._run(action="create", job_config={"name": "test"})
            self.assertIn("Mocked result", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_get_auth_headers_with_pat(self, mock_session):
        """Test _get_auth_headers with PAT token"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Run async method
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        self.assertEqual(headers["Authorization"], "Bearer test-api-key")
        self.assertEqual(headers["Content-Type"], "application/json")



    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    @patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock)
    @patch('src.core.unit_of_work.UnitOfWork')
    def test_get_auth_headers_no_token_error(self, mock_uow, mock_get_api_key, mock_session):
        """Test _get_auth_headers with no token raises error"""
        tool = DatabricksJobsTool()
        tool._token = None
        
        # Mock the API Keys Service to return None (no API key found)
        mock_get_api_key.return_value = None
        mock_uow.return_value.__aenter__.return_value = MagicMock()
        
        # Run async method and expect exception
        loop = asyncio.new_event_loop()
        with self.assertRaises(Exception) as cm:
            loop.run_until_complete(tool._get_auth_headers())
        loop.close()
        
        self.assertIn("No authentication token available", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_success(self, mock_session_class):
        """Test successful API call"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"result": "success"})
        mock_response.text = AsyncMock(return_value='{"result": "success"}')
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        result = asyncio.run(tool._make_api_call("GET", "/api/2.1/jobs/list"))
        
        self.assertEqual(result, {"result": "success"})
        mock_session.request.assert_called_once()

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_with_data(self, mock_session_class):
        """Test API call with data parameter"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"result": "success"})
        mock_response.text = AsyncMock(return_value='{"result": "success"}')
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        test_data = {"job_id": 123}
        
        # Run the async method
        result = asyncio.run(tool._make_api_call("POST", "/api/2.1/jobs/run-now", data=test_data))
        
        self.assertEqual(result, {"result": "success"})
        # Verify data was passed
        call_args = mock_session.request.call_args
        self.assertEqual(call_args[1]['json'], test_data)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_error(self, mock_session_class):
        """Test API call with error response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock error response
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.text = AsyncMock(return_value='{"error_code": "INVALID_REQUEST", "message": "Bad request"}')
        mock_response.headers = {"Content-Type": "application/json"}  # Regular dict, not AsyncMock
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("POST", "/api/2.1/jobs/run-now", {"job_id": 123}))
        
        self.assertIn("API call failed with status 400", str(cm.exception))
        self.assertIn("INVALID_REQUEST", str(cm.exception))
        self.assertIn("Bad request", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_error_invalid_json(self, mock_session_class):
        """Test API call with error response that has invalid JSON"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock error response with invalid JSON
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value='Internal Server Error')
        mock_response.headers = {"Content-Type": "text/plain"}  # Regular dict, not AsyncMock
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("GET", "/api/2.1/jobs/list"))
        
        self.assertIn("API call failed with status 500", str(cm.exception))
        self.assertIn("Internal Server Error", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession')
    def test_make_api_call_timeout(self, mock_session_class):
        """Test API call timeout"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock session that times out
        mock_session = AsyncMock()
        mock_session.request = MagicMock(side_effect=asyncio.TimeoutError())
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        mock_session_class.return_value = mock_session
        
        # Run the async method
        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("GET", "/api/2.1/jobs/list", timeout=1))
        
        self.assertIn("API call timed out", str(cm.exception))

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_success(self, mock_api_call):
        """Test successful list jobs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {
                        "name": "Test Job 1",
                        "tasks": [{"notebook_task": {"notebook_path": "/test1"}}],
                        "schedule": {"quartz_cron_expression": "0 0 * * *"}
                    },
                    "creator_user_name": "user1@example.com",
                    "created_time": 1640995200000
                },
                {
                    "job_id": 456,
                    "settings": {
                        "name": "Test Job 2",
                        "tasks": [
                            {"python_task": {"python_file": "test.py"}},
                            {"sql_task": {"warehouse_id": "warehouse123"}}
                        ]
                    },
                    "creator_user_name": "user2@example.com",
                    "created_time": 1641081600000
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertIn("Found 2 jobs", result)
        self.assertIn("Test Job 1", result)
        self.assertIn("Test Job 2", result)
        self.assertIn("ID: 123", result)
        self.assertIn("ID: 456", result)
        self.assertIn("Schedule: 0 0 * * *", result)
        # Check for task types in either order since set() doesn't guarantee order
        self.assertTrue(
            "2 task(s) (Python, SQL)" in result or "2 task(s) (SQL, Python)" in result,
            f"Expected task types not found in result: {result}"
        )

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_with_unknown_task_type(self, mock_api_call):
        """Test list jobs with unknown task type"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {
                        "name": "Job with Unknown Task",
                        "tasks": [{"unknown_task": {"some_field": "value"}}]
                    },
                    "creator_user_name": "user@example.com",
                    "created_time": None  # Test None created_time
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertIn("1 task(s) (Other)", result)
        self.assertIn("Created: Unknown", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_with_filter(self, mock_api_call):
        """Test list jobs with name filter"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {"name": "Production Job"},
                    "creator_user_name": "user@example.com",
                    "created_time": 1640995200000
                },
                {
                    "job_id": 456,
                    "settings": {"name": "Test Job"},
                    "creator_user_name": "user@example.com",
                    "created_time": 1640995200000
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10, name_filter="production"))
        
        self.assertIn("Found 1 jobs", result)
        self.assertIn("Production Job", result)
        self.assertNotIn("Test Job", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_filter_by_id(self, mock_api_call):
        """Test list jobs filtering by job ID"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {"name": "Job 123"},
                    "creator_user_name": "user@example.com"
                },
                {
                    "job_id": 456,
                    "settings": {"name": "Job 456"},
                    "creator_user_name": "user@example.com"
                }
            ]
        }
        
        result = asyncio.run(tool._list_jobs(limit=10, name_filter="123"))
        
        self.assertIn("Found 1 jobs", result)
        self.assertIn("Job 123", result)
        self.assertNotIn("Job 456", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_empty(self, mock_api_call):
        """Test list jobs with no results"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {"jobs": []}
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertEqual(result, "No jobs found in workspace.")

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_no_jobs_key(self, mock_api_call):
        """Test list jobs when response has no 'jobs' key"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {}
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertEqual(result, "No jobs found in workspace.")

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_invalid_created_time(self, mock_api_call):
        """Test list jobs with invalid created_time"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {"name": "Job"},
                    "creator_user_name": "user@example.com",
                    "created_time": "invalid"  # Invalid timestamp
                }
            ]
        }

        result = asyncio.run(tool._list_jobs(limit=10))

        # Invalid string timestamp causes TypeError on division (str / int),
        # which is not caught by the (ValueError, OSError, OverflowError) handler
        # in _format_job_list, so it propagates to the outer exception handler
        self.assertIn("Error listing jobs:", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_jobs_error(self, mock_api_call):
        """Test list jobs with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._list_jobs(limit=10))
        
        self.assertIn("Error listing jobs: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_success(self, mock_api_call):
        """Test successful list my jobs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call: get current user
            {"userName": "current.user@example.com"},
            # Second call: list all jobs
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "My Job", "tasks": []},
                        "creator_user_name": "current.user@example.com",
                        "created_time": 1640995200000
                    },
                    {
                        "job_id": 456,
                        "settings": {"name": "Other User Job", "tasks": []},
                        "creator_user_name": "other.user@example.com",
                        "created_time": 1640995200000
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Found 1 jobs created by current.user@example.com", result)
        self.assertIn("My Job", result)
        self.assertNotIn("Other User Job", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_with_emails(self, mock_api_call):
        """Test list my jobs when user info has emails instead of userName"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call: get current user with emails
            {"emails": [{"value": "current.user@example.com"}]},
            # Second call: list all jobs
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "My Job"},
                        "creator_user_name": "current.user@example.com"
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Found 1 jobs created by current.user@example.com", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_no_current_user(self, mock_api_call):
        """Test list my jobs when can't determine current user"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call fails
            Exception("Can't get user"),
            # Second call: list all jobs
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "Job 1"},
                        "creator_user_name": "user1@example.com",
                        "created_time": 1640995200000
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Found 1 jobs:", result)
        self.assertIn("Job 1", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_with_filter(self, mock_api_call):
        """Test list my jobs with name filter"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {"userName": "user@example.com"},
            {
                "jobs": [
                    {
                        "job_id": 123,
                        "settings": {"name": "My Production Job"},
                        "creator_user_name": "user@example.com"
                    },
                    {
                        "job_id": 456,
                        "settings": {"name": "My Test Job"},
                        "creator_user_name": "user@example.com"
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10, name_filter="production"))
        
        self.assertIn("Found 1 jobs", result)
        self.assertIn("My Production Job", result)
        self.assertNotIn("My Test Job", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_no_jobs(self, mock_api_call):
        """Test list my jobs with no jobs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {"userName": "user@example.com"},
            {"jobs": []}
        ]
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("No jobs found created by user@example.com", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_list_my_jobs_error(self, mock_api_call):
        """Test list my jobs with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._list_my_jobs(limit=10))
        
        self.assertIn("Error listing my jobs: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_success(self, mock_api_call):
        """Test successful get job details"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Mock responses
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {
                    "name": "Detailed Job",
                    "tasks": [
                        {
                            "task_key": "notebook_task",
                            "notebook_task": {"notebook_path": "/path/to/notebook"}
                        },
                        {
                            "task_key": "python_task",
                            "python_task": {"python_file": "script.py"}
                        },
                        {
                            "task_key": "sql_task",
                            "sql_task": {"warehouse_id": "warehouse123"}
                        }
                    ],
                    "job_clusters": [
                        {
                            "job_cluster_key": "cluster1",
                            "new_cluster": {
                                "node_type_id": "i3.xlarge",
                                "num_workers": 2
                            }
                        }
                    ],
                    "schedule": {
                        "quartz_cron_expression": "0 0 * * *",
                        "timezone_id": "UTC"
                    }
                },
                "creator_user_name": "creator@example.com",
                "created_time": 1640995200000
            },
            # Second call: list recent runs
            {
                "runs": [
                    {
                        "run_id": 999,
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "SUCCESS"
                        },
                        "start_time": 1641081600000
                    },
                    {
                        "run_id": 998,
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "FAILED"
                        },
                        "start_time": None  # Test None start_time
                    }
                ]
            }
        ]
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Job Details:", result)
        self.assertIn("Detailed Job", result)
        self.assertIn("Job ID: 123", result)
        self.assertIn("notebook_task (Notebook: /path/to/notebook)", result)
        self.assertIn("python_task (Python: script.py)", result)
        self.assertIn("sql_task (SQL: warehouse warehouse123)", result)
        self.assertIn("cluster1: i3.xlarge (2 workers)", result)
        self.assertIn("Schedule: 0 0 * * * (UTC)", result)
        self.assertIn("🟢 Run 999: TERMINATED (SUCCESS)", result)
        self.assertIn("🔴 Run 998: TERMINATED (FAILED)", result)
        self.assertIn("Unknown", result)  # For run 998 with None start_time

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_no_optional_fields(self, mock_api_call):
        """Test get job with minimal fields"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # Job with no optional fields
            {
                "job_id": 123,
                "settings": {"name": "Simple Job"},
                "creator_user_name": "creator@example.com"
            },
            {"runs": []}
        ]
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Simple Job", result)
        self.assertIn("Created: Unknown", result)  # No created_time
        self.assertIn("Recent Runs: No runs found", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_invalid_start_time(self, mock_api_call):
        """Test get job with invalid start time in runs"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {"name": "Job"},
                "creator_user_name": "user@example.com"
            },
            {
                "runs": [
                    {
                        "run_id": 999,
                        "state": {"life_cycle_state": "RUNNING"},
                        "start_time": "invalid"  # Invalid timestamp
                    }
                ]
            }
        ]

        result = asyncio.run(tool._get_job(123))

        # Invalid string timestamp causes TypeError on division, which propagates
        # to the runs fetch exception handler, resulting in "Unable to fetch"
        self.assertIn("Recent Runs: Unable to fetch", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_runs_error(self, mock_api_call):
        """Test get job when fetching runs fails"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {"name": "Job"},
                "creator_user_name": "creator@example.com"
            },
            # Second call fails
            Exception("Can't get runs")
        ]
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Recent Runs: Unable to fetch", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_job_error(self, mock_api_call):
        """Test get job with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._get_job(123))
        
        self.assertIn("Error getting job details: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_success(self, mock_api_call):
        """Test successful get notebook content"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Create test notebook content
        notebook_content = """# Databricks notebook
dbutils.widgets.text("search_id", "")
search_id = dbutils.widgets.get("search_id")
search_id = getArgument("search_id")

import json
params = json.loads(dbutils.widgets.get("job_params"))
api_key = dbutils.widgets.get("api_key")
"""
        encoded_content = base64.b64encode(notebook_content.encode()).decode()
        
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {
                            "task_key": "analyze_task",
                            "notebook_task": {"notebook_path": "/path/to/search_notebook"}
                        }
                    ]
                }
            },
            # Second call: export notebook
            {"content": encoded_content}
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("Notebook Analysis for Job 123", result)
        self.assertIn("analyze_task", result)
        self.assertIn("/path/to/search_notebook", result)
        self.assertIn("✅ Notebook content retrieved", result)
        self.assertIn("Found parameter-related patterns", result)
        self.assertIn("dbutils.widgets", result)
        self.assertIn("getArgument", result)
        self.assertIn("json.loads", result)
        self.assertIn("api_key", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_no_patterns(self, mock_api_call):
        """Test get notebook content with no parameter patterns"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Notebook without parameter patterns
        notebook_content = """# Simple notebook
print("Hello World")
spark.sql("SELECT * FROM table")
"""
        encoded_content = base64.b64encode(notebook_content.encode()).decode()
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/simple/notebook"}}
                    ]
                }
            },
            {"content": encoded_content}
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("No obvious parameter patterns found", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_many_patterns(self, mock_api_call):
        """Test get notebook content with many parameter patterns"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Create notebook with many patterns
        lines = []
        for i in range(15):
            lines.append(f'dbutils.widgets.get("param{i}")')
        notebook_content = "\n".join(lines)
        encoded_content = base64.b64encode(notebook_content.encode()).decode()
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/many/params"}}
                    ]
                }
            },
            {"content": encoded_content}
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("... and 5 more", result)  # Should show "and X more"

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_no_notebooks(self, mock_api_call):
        """Test get notebook content with no notebook tasks"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "job_id": 123,
            "settings": {
                "tasks": [
                    {"task_key": "python_task", "python_task": {"python_file": "test.py"}}
                ]
            }
        }
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("does not contain any notebook tasks", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_no_notebook_path(self, mock_api_call):
        """Test get notebook content with missing notebook path"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "job_id": 123,
            "settings": {
                "tasks": [
                    {
                        "task_key": "task1",
                        "notebook_task": {}  # No notebook_path
                    }
                ]
            }
        }
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("❌ No notebook path found", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_empty_content(self, mock_api_call):
        """Test get notebook content with empty content response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/empty/notebook"}}
                    ]
                }
            },
            {"content": ""}  # Empty content
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("❌ No content returned from export", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_decode_error(self, mock_api_call):
        """Test get notebook content with base64 decode error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/bad/notebook"}}
                    ]
                }
            },
            {"content": "invalid_base64!!!"}  # Invalid base64
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("❌ Failed to decode content", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_export_error(self, mock_api_call):
        """Test get notebook content with export error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: get job details
            {
                "job_id": 123,
                "settings": {
                    "tasks": [
                        {"notebook_task": {"notebook_path": "/path/to/notebook"}}
                    ]
                }
            },
            # Second call fails
            Exception("Export failed")
        ]
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("Failed to export notebook: Export failed", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_get_notebook_content_api_error(self, mock_api_call):
        """Test get notebook content with initial API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._get_notebook_content(123))
        
        self.assertIn("Error getting notebook content: API Error", result)

    def test_analyze_notebook_parameters_search_job(self):
        """Test analyze notebook parameters for search job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/gmaps_search_notebook.py",
            {}
        )
        
        self.assertIn("search/pagination job", result)
        self.assertIn("search_id", result)
        self.assertIn("latitude", result)
        self.assertIn("longitude", result)

    def test_analyze_notebook_parameters_google_maps(self):
        """Test analyze notebook parameters for google maps job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/google_maps_pagination.py",
            {}
        )
        
        self.assertIn("search/pagination job", result)
        self.assertIn("zoom", result)
        self.assertIn("language", result)
        self.assertIn("country", result)

    def test_analyze_notebook_parameters_etl_job(self):
        """Test analyze notebook parameters for ETL job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/etl_transform_notebook.py",
            {}
        )
        
        self.assertIn("ETL job", result)
        self.assertIn("source_path", result)
        self.assertIn("target_path", result)
        self.assertIn("batch_size", result)

    def test_analyze_notebook_parameters_extract_job(self):
        """Test analyze notebook parameters for extract job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/data_extract_job.py",
            {}
        )
        
        self.assertIn("ETL job", result)
        self.assertIn("date_range", result)

    def test_analyze_notebook_parameters_generic(self):
        """Test analyze notebook parameters for generic job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        result = tool._analyze_notebook_parameters(
            "/path/to/generic_notebook.py",
            {}
        )
        
        self.assertIn("General parameter guidelines", result)
        self.assertIn("dbutils.widgets.get()", result)
        self.assertIn("getArgument()", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_success(self, mock_api_call):
        """Test successful job run"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: run job
            {"run_id": 456},
            # Second call: get run status
            {
                "state": {
                    "life_cycle_state": "PENDING",
                    "result_state": ""
                }
            }
        ]
        
        result = asyncio.run(tool._run_job(123, {"param1": "value1"}))
        
        self.assertIn("✅ Successfully triggered job 123", result)
        self.assertIn("Run ID: 456", result)
        self.assertIn("Status: PENDING", result)
        self.assertIn("Parameters passed:", result)
        self.assertIn('"param1": "value1"', result)
        self.assertIn("Monitor progress with: action='monitor', run_id=456", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_without_params(self, mock_api_call):
        """Test run job without parameters"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            {"run_id": 456},
            {"state": {"life_cycle_state": "RUNNING"}}
        ]
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("✅ Successfully triggered job 123", result)
        self.assertNotIn("Parameters passed:", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_no_run_id(self, mock_api_call):
        """Test run job with no run_id in response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {}
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("Error: No run_id returned", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_status_check_fails(self, mock_api_call):
        """Test run job when status check fails"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = [
            # First call: run job
            {"run_id": 456},
            # Second call fails
            Exception("Can't get status")
        ]
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("✅ Successfully triggered job 123", result)
        self.assertIn("Run ID: 456", result)
        self.assertIn("Status: Unable to check initial status", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_run_job_error(self, mock_api_call):
        """Test run job with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._run_job(123))
        
        self.assertIn("Error triggering job run: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_success(self, mock_api_call):
        """Test successful run monitoring"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "Run completed successfully"
            },
            "start_time": 1641081600000,
            "end_time": 1641081900000,
            "tasks": [
                {
                    "task_key": "task1",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS"
                    }
                }
            ]
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("Run Status for 456", result)
        self.assertIn("✅ Job ID: 123", result)
        self.assertIn("Status: TERMINATED (SUCCESS)", result)
        self.assertIn("Message: Run completed successfully", result)
        self.assertIn("Duration: 300.0s", result)
        self.assertIn("✅ task1: TERMINATED (SUCCESS)", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_running(self, mock_api_call):
        """Test monitoring running job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "RUNNING",
                "result_state": ""
            },
            "start_time": 1641081600000
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("🔄 Job ID: 123", result)
        self.assertIn("Status: RUNNING", result)
        self.assertIn("Ended: Running", result)
        self.assertIn("Duration: In progress", result)
        self.assertIn("Job is still running", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_pending(self, mock_api_call):
        """Test monitoring pending job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "PENDING",
                "result_state": ""
            }
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("🔄 Job ID: 123", result)
        self.assertIn("Status: PENDING", result)
        self.assertIn("Started: Not started", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_failed(self, mock_api_call):
        """Test monitoring failed job"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Task failed with error"
            },
            "start_time": 1641081600000,
            "end_time": 1641081700000
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("❌ Job ID: 123", result)
        self.assertIn("Status: TERMINATED (FAILED)", result)
        self.assertIn("Message: Task failed with error", result)
        self.assertIn("Job failed. Get output with: action='get_output', run_id=456", result)
        self.assertIn("Check logs in Databricks UI for job 123, run 456", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_unknown_state(self, mock_api_call):
        """Test monitoring with unknown state"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "UNKNOWN",
                "result_state": ""
            }
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("🟡 Job ID: 123", result)  # Default emoji
        self.assertIn("Status: UNKNOWN", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_invalid_timestamps(self, mock_api_call):
        """Test monitor run with invalid timestamps"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {"life_cycle_state": "TERMINATED"},
            "start_time": "invalid",
            "end_time": "invalid"
        }

        result = asyncio.run(tool._monitor_run(456))

        # Invalid string timestamps cause TypeError on division (str / int),
        # which is not caught by the (ValueError, OSError, OverflowError) handler,
        # so it propagates to the outer exception handler
        self.assertIn("Error monitoring run:", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_with_failed_task(self, mock_api_call):
        """Test monitor run with failed tasks"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"},
            "tasks": [
                {
                    "task_key": "task1",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED"
                    }
                },
                {
                    "task_key": "task2",
                    "state": {
                        "life_cycle_state": "RUNNING",
                        "result_state": ""
                    }
                }
            ]
        }
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("❌ task1: TERMINATED (FAILED)", result)
        self.assertIn("🔄 task2: RUNNING", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_monitor_run_error(self, mock_api_call):
        """Test monitor run with API error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("API Error")
        
        result = asyncio.run(tool._monitor_run(456))
        
        self.assertIn("Error monitoring run: API Error", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_success(self, mock_api_call):
        """Test successful job creation"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {"job_id": 789}
        
        job_config = {
            "name": "New Test Job",
            "tasks": [
                {
                    "task_key": "notebook_task",
                    "notebook_task": {"notebook_path": "/test/notebook"}
                },
                {
                    "task_key": "python_task",
                    "python_task": {"python_file": "script.py"}
                },
                {
                    "task_key": "sql_task",
                    "sql_task": {"query": "SELECT * FROM table"}
                },
                {
                    "task_key": "other_task",
                    "other_task_type": {"field": "value"}
                }
            ],
            "schedule": {"quartz_cron_expression": "0 0 * * *"}
        }
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("✅ Successfully created job 'New Test Job'", result)
        self.assertIn("Job ID: 789", result)
        self.assertIn("Tasks: 4 task(s) configured", result)
        self.assertIn("notebook_task: Notebook (/test/notebook)", result)
        self.assertIn("python_task: Python (script.py)", result)
        self.assertIn("sql_task: SQL Task", result)
        self.assertIn("other_task: Other", result)
        self.assertIn("Schedule: 0 0 * * *", result)
        self.assertIn("Run now: action='run', job_id=789", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_minimal(self, mock_api_call):
        """Test create job with minimal configuration"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {"job_id": 789}
        
        job_config = {
            "name": "Minimal Job",
            "tasks": [{"task_key": "task1"}]
        }
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("✅ Successfully created job 'Minimal Job'", result)
        self.assertNotIn("Schedule:", result)  # No schedule

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_missing_name(self, mock_api_call):
        """Test create job with missing name"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        job_config = {"tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: Job configuration must include 'name' field", result)
        mock_api_call.assert_not_called()

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_missing_tasks(self, mock_api_call):
        """Test create job with missing tasks"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        job_config = {"name": "Job Without Tasks"}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: Job configuration must include 'tasks' field", result)
        mock_api_call.assert_not_called()

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_no_job_id(self, mock_api_call):
        """Test create job with no job_id in response"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.return_value = {}
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: No job_id returned", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_already_exists(self, mock_api_call):
        """Test create job when name already exists"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("Job with name 'Test Job' already exists")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: A job with the name 'Test Job' already exists", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_permission_error(self, mock_api_call):
        """Test create job with permission error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("User does not have permission to create jobs")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: You don't have permission to create jobs", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_invalid_config(self, mock_api_call):
        """Test create job with invalid configuration"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("Invalid job configuration: missing required field")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error: Invalid job configuration", result)

    @patch('src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call')
    def test_create_job_generic_error(self, mock_api_call):
        """Test create job with generic error"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        mock_api_call.side_effect = Exception("Generic error")
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        result = asyncio.run(tool._create_job(job_config))
        
        self.assertIn("Error creating job: Generic error", result)

    def test_tool_description_and_name(self):
        """Test tool has proper name and description"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        self.assertEqual(tool.name, "Databricks Jobs Manager")
        self.assertIn("REST API 2.2", tool.description)
        self.assertIn("list all jobs", tool.description)
        self.assertIn("get_notebook", tool.description)
        self.assertIn("IMPORTANT:", tool.description)

    def test_args_schema(self):
        """Test tool has correct args schema"""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        self.assertEqual(tool.args_schema, DatabricksJobsToolSchema)

    def test_all_initialization_paths(self):
        """Test all possible initialization paths"""
        # Test with PAT in config
        tool = DatabricksJobsTool(
            tool_config={
                "DATABRICKS_API_KEY": "pat-token"
            }
        )
        self.assertEqual(tool._token, "pat-token")
        
        # Test with parameter taking precedence over config
        tool = DatabricksJobsTool(
            databricks_host="param-host.com",
            tool_config={"DATABRICKS_HOST": "config-host.com"}
        )
        self.assertEqual(tool._host, "param-host.com")

    def test_edge_cases(self):
        """Test various edge cases"""
        # Test with None as various parameters
        tool = DatabricksJobsTool(
            databricks_host=None,
            tool_config=None,
            token_required=False,
            user_token=None
        )
        self.assertIsNotNone(tool)
        
        # Test with empty strings
        tool = DatabricksJobsTool(
            databricks_host="",
            tool_config={"DATABRICKS_API_KEY": ""}
        )
        self.assertIsNotNone(tool)

    # Single Execution Control Tests
    def test_single_execution_run_duplicate_prevention(self):
        """Test that duplicate run actions are prevented"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # First run should succeed
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456\nStatus: RUNNING"):
            result1 = tool._run(action="run", job_id=123, job_params={"test": "value"})
            self.assertIn("Successfully triggered job 123", result1)
            self.assertIn("📊 Action Usage: run 1/1", result1)
        
        # Verify global tracking
        stats = DatabricksJobsTool.get_execution_stats()
        self.assertEqual(stats['tracked_runs'], 1)
        
        # Second identical run should be prevented by action limit
        result2 = tool._run(action="run", job_id=123, job_params={"test": "value"})
        self.assertIn("⚠️ ACTION LIMIT REACHED", result2)
        self.assertIn("usage limit of 1", result2)

    def test_single_execution_run_different_params_blocked(self):
        """Test that different params for same job are blocked after first run"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # First run with params
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456\nStatus: RUNNING"):
            result1 = tool._run(action="run", job_id=123, job_params={"test": "value1"})
            self.assertIn("Successfully triggered job 123", result1)
        
        # Second run with different params should hit usage limit
        result2 = tool._run(action="run", job_id=123, job_params={"test": "value2"})
        self.assertIn("⚠️ ACTION LIMIT REACHED", result2)
        self.assertIn("usage limit of 1", result2)

    def test_single_execution_create_duplicate_prevention(self):
        """Test that duplicate create actions are prevented"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        # First create should succeed
        with patch.object(tool, '_create_job', return_value="✅ Successfully created job 'Test Job'\nJob ID: 789"):
            result1 = tool._run(action="create", job_config=job_config)
            self.assertIn("Successfully created job", result1)
            self.assertIn("📊 Action Usage: create 1/1", result1)
        
        # Verify global tracking
        stats = DatabricksJobsTool.get_execution_stats()
        self.assertEqual(stats['tracked_creates'], 1)
        
        # Second identical create should be prevented by action limit
        result2 = tool._run(action="create", job_config=job_config)
        self.assertIn("⚠️ ACTION LIMIT REACHED", result2)
        self.assertIn("usage limit of 1", result2)

    def test_single_execution_create_different_config_blocked(self):
        """Test that different job configs are blocked after first create"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # First create
        job_config1 = {"name": "Test Job 1", "tasks": [{"task_key": "task1"}]}
        with patch.object(tool, '_create_job', return_value="✅ Successfully created job 'Test Job 1'\nJob ID: 789"):
            result1 = tool._run(action="create", job_config=job_config1)
            self.assertIn("Successfully created job", result1)
        
        # Second create with different config should hit usage limit
        job_config2 = {"name": "Test Job 2", "tasks": [{"task_key": "task2"}]}
        result2 = tool._run(action="create", job_config=job_config2)
        self.assertIn("⚠️ ACTION LIMIT REACHED", result2)
        self.assertIn("usage limit of 1", result2)

    def test_single_execution_other_actions_unlimited(self):
        """Test that other actions (list, get, monitor) are not limited"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Simulate using up the single execution limit with a run
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456"):
            tool._run(action="run", job_id=123)
        
        # Other actions should still work
        with patch.object(tool, '_list_jobs', return_value="Jobs listed"):
            result = tool._run(action="list")
            self.assertIn("Jobs listed", result)
        
        with patch.object(tool, '_get_job', return_value="Job details"):
            result = tool._run(action="get", job_id=123)
            self.assertIn("Job details", result)
        
        with patch.object(tool, '_monitor_run', return_value="Run status"):
            result = tool._run(action="monitor", run_id=456)
            self.assertIn("Run status", result)

    def test_single_execution_across_multiple_instances(self):
        """Test that duplicate runs are prevented across multiple tool instances"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        # Create first tool instance
        tool1 = DatabricksJobsTool(tool_config=self.tool_config)
        
        # First run with tool1 should succeed
        with patch.object(tool1, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456\nStatus: RUNNING"):
            result1 = tool1._run(action="run", job_id=123, job_params={"test": "value"})
            self.assertIn("Successfully triggered job 123", result1)
        
        # Create second tool instance (simulating what happens in workflow)
        tool2 = DatabricksJobsTool(tool_config=self.tool_config)
        
        # Second run with tool2 should be prevented due to global tracking
        result2 = tool2._run(action="run", job_id=123, job_params={"test": "value"})
        self.assertIn("⚠️ DUPLICATE RUN PREVENTED", result2)
        self.assertIn("Previous run_id: 456", result2)
        self.assertIn("Global tracking stats: 1 runs", result2)
        
        # Verify global tracking persists across instances
        stats = DatabricksJobsTool.get_execution_stats()
        self.assertEqual(stats['tracked_runs'], 1)
        self.assertEqual(stats['tracked_creates'], 0)

    def test_single_execution_run_without_params(self):
        """Test single execution control with run action without parameters"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # First run without params
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456"):
            result1 = tool._run(action="run", job_id=123)
            self.assertIn("Successfully triggered job 123", result1)
        
        # Second run without params should be prevented
        result2 = tool._run(action="run", job_id=123)
        self.assertIn("⚠️ ACTION LIMIT REACHED", result2)

    def test_single_execution_failed_run_not_tracked(self):
        """Test that failed runs are not tracked for duplicate prevention"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        # First run fails
        with patch.object(tool, '_run_job', return_value="Error: Job failed to start"):
            result1 = tool._run(action="run", job_id=123, job_params={"test": "value"})
            self.assertIn("Error: Job failed to start", result1)
            self.assertNotIn("📊 Usage Count", result1)
        
        # Verify nothing was tracked globally
        stats = DatabricksJobsTool.get_execution_stats()
        self.assertEqual(stats['tracked_runs'], 0)
        
        # Second run with same params should be allowed (since first failed)
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456"):
            result2 = tool._run(action="run", job_id=123, job_params={"test": "value"})
            self.assertIn("Successfully triggered job 123", result2)

    def test_single_execution_failed_create_not_tracked(self):
        """Test that failed creates are not tracked for duplicate prevention"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        
        job_config = {"name": "Test Job", "tasks": [{"task_key": "task1"}]}
        
        # First create fails
        with patch.object(tool, '_create_job', return_value="Error: Job creation failed"):
            result1 = tool._run(action="create", job_config=job_config)
            self.assertIn("Error: Job creation failed", result1)
            self.assertNotIn("📊 Usage Count", result1)
        
        # Verify nothing was tracked globally
        stats = DatabricksJobsTool.get_execution_stats()
        self.assertEqual(stats['tracked_creates'], 0)
        
        # Second create with same config should be allowed (since first failed)
        with patch.object(tool, '_create_job', return_value="✅ Successfully created job 'Test Job'\nJob ID: 789"):
            result2 = tool._run(action="create", job_config=job_config)
            self.assertIn("Successfully created job", result2)

    def test_deterministic_hash_consistency(self):
        """Test that the deterministic hash produces consistent results"""
        # Same data should produce same hash
        data1 = {"search_id": "abc", "params": {"query": "test", "city": "zurich"}}
        data2 = {"params": {"city": "zurich", "query": "test"}, "search_id": "abc"}  # Different order
        
        hash1 = DatabricksJobsTool._deterministic_hash(data1)
        hash2 = DatabricksJobsTool._deterministic_hash(data2)
        
        self.assertEqual(hash1, hash2)  # Should be equal despite different ordering
        self.assertEqual(len(hash1), 16)  # Hash should be 16 characters
        
        # Different data should produce different hash
        data3 = {"search_id": "xyz", "params": {"query": "test", "city": "zurich"}}
        hash3 = DatabricksJobsTool._deterministic_hash(data3)
        
        self.assertNotEqual(hash1, hash3)
        
        # Test with nested structures
        complex_data1 = {
            "job_params": {
                "search_params": {"city": "zurich", "query": "gym"},
                "api_key": "test-key",
                "unique_identifier": "test-uuid"
            }
        }
        complex_data2 = {
            "job_params": {
                "unique_identifier": "test-uuid",
                "search_params": {"query": "gym", "city": "zurich"},  # Different order
                "api_key": "test-key"
            }
        }
        
        complex_hash1 = DatabricksJobsTool._deterministic_hash(complex_data1)
        complex_hash2 = DatabricksJobsTool._deterministic_hash(complex_data2)
        
        self.assertEqual(complex_hash1, complex_hash2)  # Should be equal despite different ordering

    def test_per_action_limits_initialization(self):
        """Test per-action limits initialization"""
        # Test with default limits
        tool1 = DatabricksJobsTool(tool_config=self.tool_config)
        self.assertEqual(tool1._action_limits['run'], 1)
        self.assertEqual(tool1._action_limits['create'], 1)
        self.assertIsNone(tool1._action_limits['list'])
        self.assertIsNone(tool1._action_limits['get'])
        
        # Test with custom limits
        custom_limits = {
            'run': 3,
            'create': 2,
            'list': 10,
            'get': None
        }
        tool2 = DatabricksJobsTool(
            tool_config=self.tool_config,
            action_limits=custom_limits
        )
        self.assertEqual(tool2._action_limits['run'], 3)
        self.assertEqual(tool2._action_limits['create'], 2)
        self.assertEqual(tool2._action_limits['list'], 10)
        self.assertIsNone(tool2._action_limits['get'])

    def test_per_action_limits_enforcement(self):
        """Test that per-action limits are properly enforced"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        # Create tool with custom limits
        tool = DatabricksJobsTool(
            tool_config=self.tool_config,
            action_limits={
                'run': 2,      # Allow 2 runs
                'create': 1,   # Allow 1 create
                'get': 3,      # Allow 3 gets
                'list': None   # Unlimited lists
            }
        )
        
        # Test run action (limit of 2)
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456"):
            # First run should work
            result1 = tool._run(action="run", job_id=123)
            self.assertIn("Successfully triggered job", result1)
            self.assertIn("Action Usage: run 1/2", result1)
            
            # Second run should work
            result2 = tool._run(action="run", job_id=124)
            self.assertIn("Successfully triggered job", result2)
            self.assertIn("Action Usage: run 2/2", result2)
            
            # Third run should be blocked
            result3 = tool._run(action="run", job_id=125)
            self.assertIn("⚠️ ACTION LIMIT REACHED", result3)
            self.assertIn("'run' action has reached its usage limit of 2", result3)
        
        # Test get action (limit of 3)
        with patch.object(tool, '_get_job', return_value="Job details"):
            for i in range(3):
                result = tool._run(action="get", job_id=100+i)
                self.assertIn("Job details", result)
                self.assertEqual(tool._action_usage_counts['get'], i+1)
            
            # Fourth get should be blocked
            result = tool._run(action="get", job_id=104)
            self.assertIn("⚠️ ACTION LIMIT REACHED", result)
            self.assertIn("'get' action has reached its usage limit of 3", result)
        
        # Test list action (unlimited)
        with patch.object(tool, '_list_jobs', return_value="Jobs listed"):
            for i in range(10):  # Should all work
                result = tool._run(action="list")
                self.assertIn("Jobs listed", result)
                self.assertEqual(tool._action_usage_counts['list'], i+1)

    def test_per_action_limits_with_failures(self):
        """Test that only successful run/create actions count against limits"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(
            tool_config=self.tool_config,
            action_limits={'run': 2}  # Allow 2 runs
        )
        
        # Failed run doesn't count against limit (for run/create actions)
        with patch.object(tool, '_run_job', return_value="Error: Failed to trigger job"):
            result1 = tool._run(action="run", job_id=123)
            self.assertIn("Error", result1)
            self.assertEqual(tool._action_usage_counts['run'], 0)  # Not incremented for failures
            
        # First successful run should work
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456"):
            result2 = tool._run(action="run", job_id=123)
            self.assertIn("Successfully triggered job", result2)
            self.assertEqual(tool._action_usage_counts['run'], 1)
            
        # Second successful run should work (within limit of 2)
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 124\nRun ID: 457"):
            result3 = tool._run(action="run", job_id=124)
            self.assertIn("Successfully triggered job", result3)
            self.assertEqual(tool._action_usage_counts['run'], 2)
            
        # Third run should hit the limit
        result4 = tool._run(action="run", job_id=125)
        self.assertIn("⚠️ ACTION LIMIT REACHED", result4)
        
        # Test that other actions (list, get) always increment even on failure
        tool2 = DatabricksJobsTool(
            tool_config=self.tool_config,
            action_limits={'get': 1}
        )
        
        with patch.object(tool2, '_get_job', return_value="Error: Job not found"):
            result = tool2._run(action="get", job_id=999)
            self.assertIn("Error", result)
            self.assertEqual(tool2._action_usage_counts['get'], 1)  # Incremented even on error
            
        # Second get should hit limit
        result = tool2._run(action="get", job_id=998)
        self.assertIn("⚠️ ACTION LIMIT REACHED", result)

    def test_per_action_reset_execution_state(self):
        """Test that reset_execution_state resets per-action counters"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(
            tool_config=self.tool_config,
            action_limits={
                'run': 2,
                'create': 1,
                'get': 3
            }
        )
        
        # Use up some actions
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456"):
            tool._run(action="run", job_id=123)
            tool._run(action="run", job_id=124)
        
        with patch.object(tool, '_get_job', return_value="Job details"):
            tool._run(action="get", job_id=100)
            tool._run(action="get", job_id=101)
        
        # Check usage before reset
        self.assertEqual(tool._action_usage_counts['run'], 2)
        self.assertEqual(tool._action_usage_counts['get'], 2)
        
        # Reset execution state
        reset_result = tool.reset_execution_state()
        self.assertIn("Action usage counts reset:", reset_result)
        self.assertIn("run: 2/2 → 0/2", reset_result)
        self.assertIn("get: 2/3 → 0/3", reset_result)
        
        # Verify counters are reset
        self.assertEqual(tool._action_usage_counts['run'], 0)
        self.assertEqual(tool._action_usage_counts['get'], 0)
        
        # Should be able to run again
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 125\nRun ID: 457"):
            result = tool._run(action="run", job_id=125)
            self.assertIn("Successfully triggered job", result)
            self.assertIn("Action Usage: run 1/2", result)

    def test_action_limits_summary_in_error(self):
        """Test that usage summary is shown when limit is reached"""
        # Clear global tracking before test
        DatabricksJobsTool.clear_execution_tracking()
        
        tool = DatabricksJobsTool(
            tool_config=self.tool_config,
            action_limits={
                'run': 1,
                'create': 2,
                'get': 3,
                'list': None,
                'monitor': 5
            }
        )
        
        # Use up the run limit
        with patch.object(tool, '_run_job', return_value="✅ Successfully triggered job 123\nRun ID: 456"):
            tool._run(action="run", job_id=123)
        
        # Try to run again - should show full usage summary
        result = tool._run(action="run", job_id=124)
        self.assertIn("⚠️ ACTION LIMIT REACHED", result)
        self.assertIn("Current action usage:", result)
        self.assertIn("- run: 1/1", result)
        self.assertIn("- create: 0/2", result)
        self.assertIn("- get: 0/3", result)
        self.assertIn("- list: 0/unlimited", result)
        self.assertIn("- monitor: 0/5", result)


class TestDatabricksJobsToolAdditionalCoverage(unittest.TestCase):
    """Additional tests targeting uncovered lines."""

    def setUp(self):
        self.tool_config = {
            "DATABRICKS_HOST": "test-workspace.cloud.databricks.com",
            "DATABRICKS_API_KEY": "test-api-key",
        }

    # ── _run_async_in_sync_context: running-loop branch ──────────────────

    def test_run_async_in_sync_context_no_running_loop(self):
        """Executes normally when no event loop is running."""
        from src.engines.crewai.tools.custom.databricks_jobs_tool import (
            _run_async_in_sync_context,
        )

        async def coro():
            return "result"

        result = _run_async_in_sync_context(coro())
        self.assertEqual(result, "result")

    # ── Schema: submit action validation ─────────────────────────────────

    def test_schema_submit_action_requires_tasks(self):
        """submit action raises ValueError when tasks is None."""
        with self.assertRaises(ValueError):
            DatabricksJobsToolSchema(action="submit")

    def test_schema_get_output_requires_run_id(self):
        """get_output action raises ValueError when run_id is None."""
        with self.assertRaises(ValueError):
            DatabricksJobsToolSchema(action="get_output")

    # ── _make_api_call: GET with data-as-params, query appending ─────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession")
    def test_make_api_call_get_data_as_params(self, mock_session_class):
        """GET request with data but no params — data becomes query params."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"jobs": []})
        mock_response.text = AsyncMock(return_value='{"jobs": []}')
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session_class.return_value = mock_session

        result = asyncio.run(
            tool._make_api_call("GET", "/api/2.2/jobs/list", data={"limit": 10})
        )
        self.assertEqual(result, {"jobs": []})
        # Verify the URL contains the query string
        call_args = mock_session.request.call_args
        self.assertIn("limit=10", call_args[1]["url"])

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession")
    def test_make_api_call_url_already_has_query(self, mock_session_class):
        """Query params are appended with & when URL already contains ?."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={})
        mock_response.text = AsyncMock(return_value="{}")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session_class.return_value = mock_session

        result = asyncio.run(
            tool._make_api_call(
                "GET", "/api/2.2/jobs/list?foo=bar", params={"limit": 5}
            )
        )
        call_args = mock_session.request.call_args
        self.assertIn("&limit=5", call_args[1]["url"])

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession")
    def test_make_api_call_401_error(self, mock_session_class):
        """401 Unauthorized response raises with auth error message."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_response = AsyncMock()
        mock_response.status = 401
        mock_response.text = AsyncMock(return_value="Unauthorized")
        mock_response.headers = {}
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session_class.return_value = mock_session

        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("GET", "/api/2.2/jobs/list"))
        self.assertIn("401", str(cm.exception))

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession")
    def test_make_api_call_403_error(self, mock_session_class):
        """403 Forbidden response raises."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_response = AsyncMock()
        mock_response.status = 403
        mock_response.text = AsyncMock(return_value="Forbidden")
        mock_response.headers = {}
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session_class.return_value = mock_session

        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("GET", "/api/2.2/jobs/list"))
        self.assertIn("403", str(cm.exception))

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession")
    def test_make_api_call_404_error(self, mock_session_class):
        """404 Not Found response raises."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.text = AsyncMock(return_value="Not Found")
        mock_response.headers = {}
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session_class.return_value = mock_session

        with self.assertRaises(Exception) as cm:
            asyncio.run(tool._make_api_call("GET", "/api/2.2/jobs/list"))
        self.assertIn("404", str(cm.exception))

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.aiohttp.ClientSession")
    def test_make_api_call_json_parse_error(self, mock_session_class):
        """200 response with invalid JSON raises."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="not json at all")
        mock_response.json = AsyncMock(side_effect=Exception("JSON decode error"))
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.request = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session_class.return_value = mock_session

        with self.assertRaises(Exception):
            asyncio.run(tool._make_api_call("GET", "/api/2.2/jobs/list"))

    # ── list_jobs: pagination with has_more ───────────────────────────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_list_jobs_pagination(self, mock_api_call):
        """List jobs follows pagination tokens until has_more=False."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        page1_jobs = [
            {
                "job_id": i,
                "settings": {"name": f"Job {i}", "tasks": []},
                "creator_user_name": "user@example.com",
            }
            for i in range(1, 4)
        ]
        page2_jobs = [
            {
                "job_id": i,
                "settings": {"name": f"Job {i}", "tasks": []},
                "creator_user_name": "user@example.com",
            }
            for i in range(4, 6)
        ]

        mock_api_call.side_effect = [
            {"jobs": page1_jobs, "has_more": True, "next_page_token": "tok1"},
            {"jobs": page2_jobs, "has_more": False},
        ]

        result = asyncio.run(tool._list_jobs(limit=100))
        self.assertIn("Found 5 jobs", result)

    # ── list_my_jobs: pagination with has_more ────────────────────────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_list_my_jobs_pagination(self, mock_api_call):
        """list_my_jobs follows pagination."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        page1_jobs = [
            {
                "job_id": i,
                "settings": {"name": f"Job {i}", "tasks": []},
                "creator_user_name": "user@example.com",
            }
            for i in range(1, 4)
        ]
        page2_jobs = [
            {
                "job_id": i,
                "settings": {"name": f"Job {i}", "tasks": []},
                "creator_user_name": "user@example.com",
            }
            for i in range(4, 6)
        ]

        mock_api_call.side_effect = [
            {"userName": "user@example.com"},
            {"jobs": page1_jobs, "has_more": True, "next_page_token": "tok1"},
            {"jobs": page2_jobs, "has_more": False},
        ]

        result = asyncio.run(tool._list_my_jobs(limit=100))
        self.assertIn("Found 5 jobs created by user@example.com", result)

    # ── _format_job_list: spark_jar, pipeline, dbt task types ────────────

    def test_format_job_list_spark_jar_task(self):
        """Spark JAR tasks are formatted with (Spark JAR: <class>)."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        jobs = [
            {
                "job_id": 1,
                "settings": {
                    "name": "Spark Job",
                    "tasks": [{"spark_jar_task": {"main_class_name": "com.example.Main"}}],
                },
                "creator_user_name": "u@ex.com",
            }
        ]
        result = tool._format_job_list(jobs)
        self.assertIn("Spark JAR", result)

    def test_format_job_list_pipeline_task(self):
        """Pipeline tasks are labelled Pipeline."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        jobs = [
            {
                "job_id": 1,
                "settings": {
                    "name": "Pipeline Job",
                    "tasks": [{"pipeline_task": {"pipeline_id": "p-123"}}],
                },
                "creator_user_name": "u@ex.com",
            }
        ]
        result = tool._format_job_list(jobs)
        self.assertIn("Pipeline", result)

    def test_format_job_list_dbt_task(self):
        """dbt tasks are labelled dbt."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        jobs = [
            {
                "job_id": 1,
                "settings": {
                    "name": "dbt Job",
                    "tasks": [{"dbt_task": {}}],
                },
                "creator_user_name": "u@ex.com",
            }
        ]
        result = tool._format_job_list(jobs)
        self.assertIn("dbt", result)

    def test_format_job_list_without_creator(self):
        """format_job_list with include_creator=False omits creator column."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        jobs = [
            {
                "job_id": 1,
                "settings": {"name": "Job", "tasks": []},
                "creator_user_name": "u@ex.com",
            }
        ]
        result = tool._format_job_list(jobs, include_creator=False)
        self.assertNotIn("Creator:", result)
        self.assertIn("ID: 1", result)

    # ── _get_job: spark_jar, pipeline, dbt task type branches ────────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_job_spark_jar_pipeline_dbt_tasks(self, mock_api_call):
        """Get job with spark_jar, pipeline, and dbt tasks reports them correctly."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.side_effect = [
            {
                "job_id": 1,
                "settings": {
                    "name": "Multi Task Job",
                    "tasks": [
                        {"task_key": "jar_task", "spark_jar_task": {"main_class_name": "Main"}},
                        {"task_key": "pipe_task", "pipeline_task": {"pipeline_id": "p1"}},
                        {"task_key": "dbt_task", "dbt_task": {}},
                        {"task_key": "other_task"},  # unknown type
                    ],
                },
                "creator_user_name": "u@ex.com",
            },
            {"runs": []},
        ]

        result = asyncio.run(tool._get_job(1))
        self.assertIn("jar_task (Spark JAR: Main)", result)
        self.assertIn("pipe_task (Pipeline: p1)", result)
        self.assertIn("dbt_task (dbt)", result)
        self.assertIn("other_task", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_job_with_canceled_run(self, mock_api_call):
        """Monitor run cancelled emoji is shown for CANCELED result_state."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.side_effect = [
            {"job_id": 1, "settings": {"name": "Job"}, "creator_user_name": "u@ex.com"},
            {
                "runs": [
                    {
                        "run_id": 10,
                        "state": {"life_cycle_state": "TERMINATED", "result_state": "CANCELED"},
                        "start_time": 1640995200000,
                    }
                ]
            },
        ]

        result = asyncio.run(tool._get_job(1))
        # CANCELED result: emoji is 🟡 (not SUCCESS/FAILED)
        self.assertIn("Run 10: TERMINATED (CANCELED)", result)

    # ── _monitor_run: CANCELED state ─────────────────────────────────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_monitor_run_canceled(self, mock_api_call):
        """Canceled run shows 🚫 emoji."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "CANCELED",
                "state_message": "",
            },
            "start_time": 1641081600000,
            "end_time": 1641081700000,
        }

        result = asyncio.run(tool._monitor_run(456))
        self.assertIn("🚫 Job ID: 123", result)
        self.assertIn("CANCELED", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_monitor_run_task_canceled(self, mock_api_call):
        """Task with CANCELED result gets 🚫 emoji."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "run_id": 456,
            "job_id": 123,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "CANCELED"},
            "tasks": [
                {
                    "task_key": "t1",
                    "state": {"life_cycle_state": "TERMINATED", "result_state": "CANCELED"},
                }
            ],
        }

        result = asyncio.run(tool._monitor_run(456))
        self.assertIn("🚫 t1: TERMINATED (CANCELED)", result)

    # ── _run_job: list params ─────────────────────────────────────────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_run_job_with_list_params(self, mock_api_call):
        """run_job with list params uses python_params key."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.side_effect = [
            {"run_id": 789},
            {"state": {"life_cycle_state": "RUNNING"}},
        ]

        result = asyncio.run(tool._run_job(1, ["--arg1", "val1"]))
        self.assertIn("Successfully triggered job 1", result)
        # python_params path
        call_payload = mock_api_call.call_args_list[0][0][2]  # third positional arg
        self.assertIn("python_params", call_payload)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_run_job_with_result_state(self, mock_api_call):
        """run_job shows result_state in output if returned."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.side_effect = [
            {"run_id": 100},
            {
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                }
            },
        ]

        result = asyncio.run(tool._run_job(1, {"key": "val"}))
        self.assertIn("TERMINATED (SUCCESS)", result)
        self.assertIn("Parameters passed:", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_run_job_status_check_fails_with_params(self, mock_api_call):
        """run_job status-check-failed path still shows params."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.side_effect = [
            {"run_id": 100},
            Exception("status check failed"),
        ]

        result = asyncio.run(tool._run_job(1, {"key": "val"}))
        self.assertIn("Parameters passed:", result)

    # ── _get_run_output ───────────────────────────────────────────────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_notebook_output(self, mock_api_call):
        """get_run_output shows notebook result and truncation warning."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "notebook_output": {"result": "my result", "truncated": True}
        }

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("Notebook Output:", result)
        self.assertIn("my result", result)
        self.assertIn("truncated", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_notebook_empty_result(self, mock_api_call):
        """get_run_output shows (empty) when notebook result is empty string."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "notebook_output": {"result": "", "truncated": False}
        }

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("(empty)", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_error_and_trace(self, mock_api_call):
        """get_run_output shows error and truncated trace."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        long_trace = "Traceback " + "x" * 2100
        mock_api_call.return_value = {
            "error": "Something went wrong",
            "error_trace": long_trace,
        }

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("Error: Something went wrong", result)
        self.assertIn("Error Trace:", result)
        self.assertIn("truncated", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_sql_output(self, mock_api_call):
        """get_run_output shows SQL output sections."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "sql_output": {
                "query_output": {
                    "query_text": "SELECT 1",
                    "warehouse_id": "wh-123",
                    "output_link": "https://example.com",
                },
                "dashboard_output": {"widgets": [{"id": "w1"}]},
                "alert_output": {"alert_state": "TRIGGERED"},
            }
        }

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("SQL Output:", result)
        self.assertIn("SELECT 1", result)
        self.assertIn("wh-123", result)
        self.assertIn("https://example.com", result)
        self.assertIn("Dashboard Widgets: 1", result)
        self.assertIn("Alert State: TRIGGERED", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_run_job_output(self, mock_api_call):
        """get_run_output shows run_job_output section."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {"run_job_output": {"run_id": 999}}

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("Run Job Output:", result)
        self.assertIn("Triggered Run ID: 999", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_metadata(self, mock_api_call):
        """get_run_output shows metadata run state."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {
            "notebook_output": {"result": "ok"},
            "metadata": {
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                    "state_message": "All done",
                }
            },
        }

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("Run State: TERMINATED (SUCCESS)", result)
        self.assertIn("Message: All done", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_no_output(self, mock_api_call):
        """get_run_output shows 'No output data available' when empty."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        mock_api_call.return_value = {}

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("No output data available", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_get_run_output_error(self, mock_api_call):
        """get_run_output handles API exception."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.side_effect = Exception("API Error")

        result = asyncio.run(tool._get_run_output(1))
        self.assertIn("Error getting run output: API Error", result)

    # ── _submit_run ───────────────────────────────────────────────────────

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_success_notebook(self, mock_api_call):
        """submit_run with notebook task returns run info."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.return_value = {"run_id": 555}

        tasks = [{"task_key": "t1", "notebook_task": {"notebook_path": "/test"}}]
        result = asyncio.run(tool._submit_run(tasks, run_name="My Run"))

        self.assertIn("Successfully submitted one-time run", result)
        self.assertIn("'My Run'", result)
        self.assertIn("Run ID: 555", result)
        self.assertIn("t1: Notebook (/test)", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_python_task(self, mock_api_call):
        """submit_run with python task."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.return_value = {"run_id": 555}

        tasks = [{"task_key": "t1", "python_task": {"python_file": "script.py"}}]
        result = asyncio.run(tool._submit_run(tasks))
        self.assertIn("t1: Python (script.py)", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_sql_task(self, mock_api_call):
        """submit_run with SQL task."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.return_value = {"run_id": 555}

        tasks = [{"task_key": "t1", "sql_task": {}}]
        result = asyncio.run(tool._submit_run(tasks))
        self.assertIn("t1: SQL Task", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_other_task(self, mock_api_call):
        """submit_run with unknown task type falls back to Other."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.return_value = {"run_id": 555}

        tasks = [{"task_key": "t1"}]
        result = asyncio.run(tool._submit_run(tasks))
        self.assertIn("t1: Other", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_with_dict_params(self, mock_api_call):
        """submit_run with dict params wraps as job_parameters."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.return_value = {"run_id": 555}

        tasks = [{"task_key": "t1"}]
        result = asyncio.run(tool._submit_run(tasks, job_params={"k": "v"}))
        self.assertIn("Parameters passed:", result)
        payload = mock_api_call.call_args[0][2]
        self.assertIn("job_parameters", payload)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_with_list_params(self, mock_api_call):
        """submit_run with list params uses python_params."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.return_value = {"run_id": 555}

        tasks = [{"task_key": "t1"}]
        result = asyncio.run(tool._submit_run(tasks, job_params=["--a", "b"]))
        payload = mock_api_call.call_args[0][2]
        self.assertIn("python_params", payload)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_no_run_id(self, mock_api_call):
        """submit_run returns error when no run_id in response."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.return_value = {}

        tasks = [{"task_key": "t1"}]
        result = asyncio.run(tool._submit_run(tasks))
        self.assertIn("Error: No run_id returned", result)

    @patch("src.engines.crewai.tools.custom.databricks_jobs_tool.DatabricksJobsTool._make_api_call")
    def test_submit_run_api_error(self, mock_api_call):
        """submit_run handles API exception."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        mock_api_call.side_effect = Exception("API Error")

        tasks = [{"task_key": "t1"}]
        result = asyncio.run(tool._submit_run(tasks))
        self.assertIn("Error submitting one-time run: API Error", result)

    # ── _run: submit action via _run dispatch ─────────────────────────────

    def test_run_submit_action(self):
        """_run with submit action dispatches to _submit_run."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)
        tasks = [{"task_key": "t1", "notebook_task": {"notebook_path": "/p"}}]

        with patch.object(tool, "_submit_run", return_value="submitted") as mock_submit:
            result = tool._run(action="submit", tasks=tasks)
            self.assertIn("submitted", result)
            mock_submit.assert_called_once()

    def test_run_get_output_action(self):
        """_run with get_output action dispatches to _get_run_output."""
        tool = DatabricksJobsTool(tool_config=self.tool_config)

        with patch.object(tool, "_get_run_output", return_value="output") as mock_output:
            result = tool._run(action="get_output", run_id=42)
            self.assertIn("output", result)
            mock_output.assert_called_once_with(42)


if __name__ == "__main__":
    unittest.main()