"""Tests for MetricViewDeployerTool."""
import json
import pytest
from src.engines.crewai.tools.custom.metric_view_deployer_tool import MetricViewDeployerTool


class TestMetricViewDeployerTool:
    def test_initialization(self):
        tool = MetricViewDeployerTool()
        assert tool.name == "Metric View Deployer"

    def test_dry_run(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test_view\nsource: cat.sch.tbl'}
        sql_specs = {'fact_test': 'CREATE METRIC VIEW test_view AS SELECT * FROM tbl;'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=True,
        )
        data = json.loads(result)
        assert data['summary']['dry_run'] is True
        assert data['summary']['validated'] == 1
        assert data['deployment_results']['fact_test']['status'] == 'validated'

    def test_deploy_without_credentials(self):
        tool = MetricViewDeployerTool()
        yaml_specs = {'fact_test': 'name: test'}
        sql_specs = {'fact_test': 'CREATE METRIC VIEW x;'}
        result = tool._run(
            yaml_specs_json=json.dumps(yaml_specs),
            sql_specs_json=json.dumps(sql_specs),
            dry_run=False,
        )
        data = json.loads(result)
        assert data['deployment_results']['fact_test']['status'] == 'error'
        assert 'required' in data['deployment_results']['fact_test']['message']

    def test_empty_input(self):
        tool = MetricViewDeployerTool()
        result = tool._run(yaml_specs_json='{}', sql_specs_json='{}', dry_run=True)
        data = json.loads(result)
        assert data['summary']['total'] == 0
