"""
Tests for MetricViewDeployerTool.

Updated for app-modes refactoring:
- Tool now uses DDL via SQL Statement API instead of REST metric-views endpoint
- Authentication via _authenticate() / get_auth_context(), not databricks_token param
- sql_specs_json parameter was removed from the schema
- view_name format changed to "{cat}.{sch}.{safe_key}" (no _uc_metric_view suffix)
- summary no longer has 'updated' key (no PUT/update logic)
- live deployment requires warehouse_id, auth context, and a whitelisted workspace URL
"""
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from src.engines.crewai.tools.custom.metric_view_deployer_tool import MetricViewDeployerTool


class TestDryRun:
    """Tests for dry_run=True (validation only, no deployment)."""

    def test_dry_run_validates_single_view(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test\nsource: tbl\nmeasures:\n  - name: x\n    expr: SUM(source.x)'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['summary']['validated'] == 1
        assert data['summary']['total'] == 1
        assert data['deployment_results']['fact_test']['status'] == 'validated'
        assert data['deployment_results']['fact_test']['dry_run'] is True

    def test_dry_run_multiple_tables(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {
            'fact_a': 'version: 1.1\nsource: cat.sch.a',
            'fact_b': 'version: 1.1\nsource: cat.sch.b',
        }
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['summary']['total'] == 2
        assert data['summary']['validated'] == 2
        assert data['deployment_results']['fact_a']['status'] == 'validated'
        assert data['deployment_results']['fact_b']['status'] == 'validated'

    def test_dry_run_view_name_format(self):
        """View name uses safe_cat.safe_sch.safe_key (no _uc_metric_view suffix)."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=True,
            catalog='main',
            schema_name='default',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['view_name'] == 'main.default.fact_test'

    def test_dry_run_reports_yaml_lines(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'t': 'line1\nline2\nline3'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['deployment_results']['t']['yaml_lines'] == 3

    def test_dry_run_no_errors(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'t': 'yaml content'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['summary']['errors'] == 0

    def test_yaml_key_is_included_in_output(self):
        """The output includes a 'yaml' key for downstream flow injection."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact': 'yaml: content'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert 'yaml' in data
        assert data['yaml']['fact'] == 'yaml: content'


class TestLivePathErrors:
    """Tests for live deployment error cases (no actual network calls)."""

    def test_live_path_missing_warehouse(self):
        """Empty warehouse_id → error result."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=False,
            warehouse_id='',  # Empty
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'error'
        assert 'warehouse_id' in data['deployment_results']['fact_test']['message']

    def test_live_path_empty_yaml_content(self):
        """Empty YAML content → error result."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': ''}  # Empty YAML
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            dry_run=False,
            warehouse_id='wh-123',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'error'
        assert 'Empty YAML content' in data['deployment_results']['fact_test']['message']

    def test_live_path_no_yaml_specs(self):
        """No yaml_specs → top-level error dict."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json='{}',
            dry_run=False,
        )
        data = json.loads(result)
        assert 'error' in data

    def test_invalid_json_input(self):
        """Invalid yaml_specs_json → top-level error."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json='not valid json{{{',
        )
        data = json.loads(result)
        assert 'error' in data
        assert 'Invalid JSON' in data['error']

    def test_live_path_authentication_error(self):
        """Auth failure (exception from _authenticate) → error result."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact': 'yaml content'}

        with patch.object(tool, '_authenticate', side_effect=Exception('Auth failed')):
            result = tool._run(
                yaml_specs_json=json.dumps(yaml_specs),
                dry_run=False,
                warehouse_id='wh',
            )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert 'Auth failed' in data['deployment_results']['fact']['message']

    def test_live_path_auth_returns_none(self):
        """None auth context → authentication failed error."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact': 'yaml content'}

        with patch.object(tool, '_authenticate', return_value=None):
            result = tool._run(
                yaml_specs_json=json.dumps(yaml_specs),
                dry_run=False,
                warehouse_id='wh',
            )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert 'Authentication failed' in data['deployment_results']['fact']['message']

    def test_live_path_untrusted_host(self):
        """Non-whitelisted workspace_url → error result."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact': 'yaml content'}

        mock_auth = MagicMock()
        mock_auth.get_headers.return_value = {'Authorization': 'Bearer tok'}
        mock_auth.workspace_url = 'https://untrusted-host.example.com'

        with patch.object(tool, '_authenticate', return_value=mock_auth):
            result = tool._run(
                yaml_specs_json=json.dumps(yaml_specs),
                dry_run=False,
                warehouse_id='wh-123',
            )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert 'Untrusted host' in data['deployment_results']['fact']['message']


class TestLivePathSuccess:
    """Tests for successful live deployment (mocking auth + SQL Statement API)."""

    def _make_mock_auth(self, workspace_url='https://myworkspace.cloud.databricks.com'):
        mock_auth = MagicMock()
        mock_auth.get_headers.return_value = {'Authorization': 'Bearer test-token'}
        mock_auth.workspace_url = workspace_url
        return mock_auth

    def _make_sql_response(self, state='SUCCEEDED', statement_id='stmt-1'):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            'statement_id': statement_id,
            'status': {'state': state},
        }
        return mock_resp

    @patch('requests.post')
    def test_successful_deploy_via_sql_api(self, mock_post):
        """Successful DDL execution → status=deployed."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test\nmeasures:\n  - name: x'}

        mock_auth = self._make_mock_auth()
        sql_resp = self._make_sql_response()
        mock_post.return_value = sql_resp

        with patch.object(tool, '_authenticate', return_value=mock_auth):
            with patch('src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter._check_dangerous_sql', return_value=True):
                result = tool._run(
                    yaml_specs_json=json.dumps(yaml_specs),
                    dry_run=False,
                    warehouse_id='wh-123',
                    catalog='main',
                    schema_name='default',
                )

        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'deployed'
        assert data['deployment_results']['fact_test']['view_name'] == 'main.default.fact_test'

    @patch('requests.post')
    def test_sql_api_endpoint_is_used(self, mock_post):
        """Verify SQL Statement API (not the old metric-views REST API) is called."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'tbl': 'yaml_body'}

        mock_auth = self._make_mock_auth()
        mock_post.return_value = self._make_sql_response()

        with patch.object(tool, '_authenticate', return_value=mock_auth):
            with patch('src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter._check_dangerous_sql', return_value=True):
                tool._run(
                    yaml_specs_json=json.dumps(yaml_specs),
                    dry_run=False,
                    warehouse_id='wh-abc',
                )

        # Should call /api/2.0/sql/statements
        assert mock_post.called
        call_url = mock_post.call_args[0][0]
        assert '/api/2.0/sql/statements' in call_url
        # Should NOT call the old metric-views endpoint
        assert 'unity-catalog/metric-views' not in call_url

    @patch('requests.post')
    def test_view_name_uses_safe_key(self, mock_post):
        """View name safely encodes the table_key (no _uc_metric_view suffix)."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'Fact-Sales': 'yaml'}

        mock_auth = self._make_mock_auth()
        mock_post.return_value = self._make_sql_response()

        with patch.object(tool, '_authenticate', return_value=mock_auth):
            with patch('src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter._check_dangerous_sql', return_value=True):
                result = tool._run(
                    yaml_specs_json=json.dumps(yaml_specs),
                    dry_run=False,
                    warehouse_id='wh',
                    catalog='CAT',
                    schema_name='SCH',
                )

        data = json.loads(result)
        # safe_key replaces non-alphanumeric with _
        view_name = data['deployment_results']['Fact-Sales']['view_name']
        assert 'CAT' in view_name
        assert 'SCH' in view_name
        # No _uc_metric_view suffix in the new API
        assert '_uc_metric_view' not in view_name

    @patch('requests.post')
    def test_sql_api_failure_reports_error(self, mock_post):
        """SQL Statement API HTTP error → error result."""
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact': 'yaml content'}

        mock_auth = self._make_mock_auth()
        fail_resp = MagicMock()
        fail_resp.status_code = 500
        fail_resp.text = 'Internal Server Error'
        mock_post.return_value = fail_resp

        with patch.object(tool, '_authenticate', return_value=mock_auth):
            with patch('src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter._check_dangerous_sql', return_value=True):
                result = tool._run(
                    yaml_specs_json=json.dumps(yaml_specs),
                    dry_run=False,
                    warehouse_id='wh',
                )

        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert data['deployment_results']['fact']['http_status'] == 500

    def test_network_error_caught(self):
        """Network exception during DDL execution → error result.

        Patch _execute_sql_sync since it's called for both schema creation and DDL.
        Schema creation must succeed but DDL fails.
        """
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact': 'yaml content'}

        mock_auth = self._make_mock_auth()
        # First call (CREATE SCHEMA) succeeds; second call (DDL) raises
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"success": True}  # schema creation
            raise Exception('Connection timeout')

        with patch.object(tool, '_authenticate', return_value=mock_auth):
            with patch.object(tool, '_execute_sql_sync', side_effect=side_effect):
                with patch('src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter._check_dangerous_sql', return_value=True):
                    result = tool._run(
                        yaml_specs_json=json.dumps(yaml_specs),
                        dry_run=False,
                        warehouse_id='wh',
                    )

        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert 'Connection timeout' in data['deployment_results']['fact']['message']


class TestToolConfig:
    """Tests for tool initialization and configuration."""

    def test_default_config_passthrough(self):
        """Catalog and schema_name defaults from __init__ are used in dry-run."""
        tool = MetricViewDeployerTool(
            catalog='init_cat',
            schema_name='init_sch',
        )
        result = tool._run(
            yaml_specs_json=json.dumps({'t': 'yaml'}),
            dry_run=True,
        )
        data = json.loads(result)
        view_name = data['deployment_results']['t']['view_name']
        assert 'init_cat' in view_name
        assert 'init_sch' in view_name

    def test_summary_structure(self):
        """Summary dict has expected keys."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({'t': 'yaml'}),
            dry_run=True,
        )
        data = json.loads(result)
        assert 'total' in data['summary']
        assert 'validated' in data['summary']
        assert 'deployed' in data['summary']
        assert 'errors' in data['summary']
        assert 'dry_run' in data['summary']
