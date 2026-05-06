"""Tests for the deployer tool — both dry-run and live path."""
import json
import pytest
from unittest.mock import patch, MagicMock
from src.engines.crewai.tools.custom.metric_view_deployer_tool import MetricViewDeployerTool


class TestDeployerLivePath:
    def test_dry_run_validates(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test\nsource: tbl\nmeasures:\n  - name: x\n    expr: SUM(source.x)'}
        sql_specs = {'fact_test': '-- Deploy instructions'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['summary']['validated'] == 1

    def test_dry_run_multiple_tables(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {
            'fact_a': 'version: 1.1\nsource: cat.sch.a',
            'fact_b': 'version: 1.1\nsource: cat.sch.b',
        }
        sql_specs = {'fact_a': '-- SQL A', 'fact_b': '-- SQL B'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['summary']['total'] == 2
        assert data['summary']['validated'] == 2

    def test_live_path_missing_host_and_token(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test'}
        sql_specs = {'fact_test': '-- SQL'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=False,
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'error'
        assert 'required' in data['deployment_results']['fact_test']['message']

    def test_live_path_missing_warehouse(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test'}
        sql_specs = {'fact_test': '-- SQL'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='token123',
            warehouse_id='',  # Empty
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'error'
        assert 'warehouse_id' in data['deployment_results']['fact_test']['message']

    def test_live_path_empty_yaml_content(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': ''}  # Empty YAML
        sql_specs = {'fact_test': '-- Deploy'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='token123',
            warehouse_id='wh-123',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'error'
        assert 'Empty YAML content' in data['deployment_results']['fact_test']['message']

    @patch('requests.post')
    def test_live_path_successful_deploy(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {'name': 'test_view', 'status': 'ACTIVE'}
        mock_post.return_value = mock_resp

        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test\nmeasures:\n  - name: x'}
        sql_specs = {'fact_test': '-- Deploy'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='token123',
            warehouse_id='wh-123',
            catalog='main',
            schema_name='default',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'deployed'
        assert data['deployment_results']['fact_test']['view_name'] == 'main.default.fact_test_uc_metric_view'

        # Verify the API was called with correct payload
        call_args = mock_post.call_args
        assert 'unity-catalog/metric-views' in call_args[0][0] or 'unity-catalog/metric-views' in call_args.kwargs.get('url', call_args[0][0])
        payload = call_args.kwargs.get('json') or call_args[1].get('json')
        assert payload['name'] == 'main.default.fact_test_uc_metric_view'
        assert payload['yaml_body'] == 'name: test\nmeasures:\n  - name: x'

    @patch('requests.post')
    def test_live_path_200_also_succeeds(self, mock_post):
        """Status code 200 should also count as deployed."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {'name': 'test_view'}
        mock_post.return_value = mock_resp

        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({'fact': 'version: 1.1'}),
            sql_specs_json=json.dumps({'fact': '-- SQL'}),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='tok',
            warehouse_id='wh',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'deployed'

    @patch('requests.put')
    @patch('requests.post')
    def test_live_path_conflict_triggers_update(self, mock_post, mock_put):
        """409 Conflict should trigger a PUT update."""
        # POST returns 409
        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 409
        mock_post.return_value = mock_post_resp

        # PUT returns 200
        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 200
        mock_put_resp.json.return_value = {'name': 'updated_view'}
        mock_put.return_value = mock_put_resp

        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({'fact': 'version: 1.1\nsource: tbl'}),
            sql_specs_json=json.dumps({'fact': '-- SQL'}),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='tok',
            warehouse_id='wh',
            catalog='cat',
            schema_name='sch',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'updated'
        assert data['summary']['updated'] == 1

        # Verify PUT was called with correct URL
        put_call = mock_put.call_args
        put_url = put_call[0][0] if put_call[0] else put_call.kwargs.get('url', '')
        assert 'cat.sch.fact_uc_metric_view' in put_url

    @patch('requests.put')
    @patch('requests.post')
    def test_live_path_conflict_update_fails(self, mock_post, mock_put):
        """409 + failed PUT should report error."""
        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 409
        mock_post.return_value = mock_post_resp

        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 500
        mock_put_resp.text = 'Internal Server Error'
        mock_put.return_value = mock_put_resp

        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({'fact': 'yaml content'}),
            sql_specs_json=json.dumps({'fact': '-- SQL'}),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='tok',
            warehouse_id='wh',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert data['deployment_results']['fact']['http_status'] == 500

    @patch('requests.post')
    def test_live_path_server_error(self, mock_post):
        """Non-200/201/409 should report error with status code."""
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = 'Forbidden: insufficient permissions'
        mock_post.return_value = mock_resp

        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({'fact': 'yaml content'}),
            sql_specs_json=json.dumps({'fact': '-- SQL'}),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='tok',
            warehouse_id='wh',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert data['deployment_results']['fact']['http_status'] == 403
        assert 'Forbidden' in data['deployment_results']['fact']['message']

    @patch('requests.post', side_effect=Exception('Connection timeout'))
    def test_live_path_network_error(self, mock_post):
        """Network errors should be caught and reported."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({'fact': 'yaml content'}),
            sql_specs_json=json.dumps({'fact': '-- SQL'}),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='tok',
            warehouse_id='wh',
        )
        data = json.loads(result)
        assert data['deployment_results']['fact']['status'] == 'error'
        assert 'Connection timeout' in data['deployment_results']['fact']['message']

    def test_invalid_json_input(self):
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json='not valid json{{{',
            sql_specs_json='{}',
        )
        data = json.loads(result)
        assert 'error' in data
        assert 'Invalid JSON' in data['error']

    def test_summary_updated_counter(self):
        """Verify the summary includes the updated counter."""
        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json='{}',
            sql_specs_json='{}',
            dry_run=True,
        )
        data = json.loads(result)
        assert 'updated' in data['summary']
        assert data['summary']['updated'] == 0

    @patch('requests.post')
    def test_live_path_uses_correct_api_endpoint(self, mock_post):
        """Verify the tool hits the UC Metric View API, not the SQL Statement API."""
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {}
        mock_post.return_value = mock_resp

        tool = MetricViewDeployerTool()
        tool._run(
            yaml_specs_json=json.dumps({'tbl': 'yaml_body'}),
            sql_specs_json=json.dumps({'tbl': '-- sql'}),
            dry_run=False,
            databricks_host='myhost.cloud.databricks.com',
            databricks_token='dapi123',
            warehouse_id='wh-abc',
        )
        call_url = mock_post.call_args[0][0]
        assert '/api/2.0/unity-catalog/metric-views' in call_url
        assert '/api/2.0/sql/statements' not in call_url

    @patch('requests.post')
    def test_live_path_view_name_lowercase(self, mock_post):
        """View name should use lowercase table_key."""
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.json.return_value = {}
        mock_post.return_value = mock_resp

        tool = MetricViewDeployerTool()
        result = tool._run(
            yaml_specs_json=json.dumps({'Fact_Sales': 'yaml'}),
            sql_specs_json=json.dumps({'Fact_Sales': '-- sql'}),
            dry_run=False,
            databricks_host='myworkspace.cloud.databricks.com',
            databricks_token='tok',
            warehouse_id='wh',
            catalog='CAT',
            schema_name='SCH',
        )
        data = json.loads(result)
        assert data['deployment_results']['Fact_Sales']['view_name'] == 'CAT.SCH.fact_sales_uc_metric_view'

    def test_default_config_passthrough(self):
        """Test that __init__ defaults are used when _run() kwargs are absent."""
        tool = MetricViewDeployerTool(
            catalog='init_cat',
            schema_name='init_sch',
        )
        result = tool._run(
            yaml_specs_json=json.dumps({'t': 'yaml'}),
            sql_specs_json=json.dumps({'t': '-- sql'}),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['deployment_results']['t']['catalog'] == 'init_cat'
        assert data['deployment_results']['t']['schema'] == 'init_sch'
