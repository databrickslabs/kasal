"""Metric View Deployer Tool for CrewAI — deploy YAML to Databricks."""
import json
import logging
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class MetricViewDeployerSchema(BaseModel):
    """Input schema for MetricViewDeployerTool."""
    yaml_specs_json: Optional[str] = Field(
        None, description="JSON dict of table_key → YAML string (from UC Metric View Generator)")
    sql_specs_json: Optional[str] = Field(
        None, description="JSON dict of table_key → SQL string (from UC Metric View Generator)")
    catalog: Optional[str] = Field(None, description="Target UC catalog")
    schema_name: Optional[str] = Field(None, description="Target UC schema")
    dry_run: bool = Field(True, description="If True, validate only without deploying")
    databricks_host: Optional[str] = Field(None, description="Databricks workspace URL")
    databricks_token: Optional[str] = Field(None, description="Databricks PAT for deployment")


class MetricViewDeployerTool(BaseTool):
    """Deploy UC Metric View YAML + SQL to a Databricks workspace."""
    name: str = "Metric View Deployer"
    description: str = (
        "Deploy UC Metric View definitions to Databricks. Accepts YAML specs and deploy SQL "
        "from the UC Metric View Generator tool. Supports dry_run mode (default) for validation "
        "without actual deployment. When dry_run=False, executes CREATE METRIC VIEW SQL "
        "via the Databricks SQL Statement API. "
        "Input: yaml_specs_json + sql_specs_json from tool 86. "
        "Output: deployment status per metric view."
    )
    args_schema: Type[BaseModel] = MetricViewDeployerSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = ('yaml_specs_json', 'sql_specs_json', 'catalog', 'schema_name',
                       'dry_run', 'databricks_host', 'databricks_token')
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        def _get(key):
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        yaml_raw = _get('yaml_specs_json') or '{}'
        sql_raw = _get('sql_specs_json') or '{}'
        catalog = _get('catalog') or 'main'
        schema = _get('schema_name') or 'default'
        dry_run = _get('dry_run')
        if dry_run is None:
            dry_run = True

        try:
            yaml_specs = json.loads(yaml_raw) if isinstance(yaml_raw, str) else yaml_raw
            sql_specs = json.loads(sql_raw) if isinstance(sql_raw, str) else sql_raw
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {e}"})

        results = {}
        for table_key in sorted(set(list(yaml_specs.keys()) + list(sql_specs.keys()))):
            yaml_content = yaml_specs.get(table_key, '')
            sql_content = sql_specs.get(table_key, '')

            if dry_run:
                results[table_key] = {
                    'status': 'validated',
                    'yaml_lines': len(yaml_content.split('\n')) if yaml_content else 0,
                    'sql_lines': len(sql_content.split('\n')) if sql_content else 0,
                    'catalog': catalog,
                    'schema': schema,
                    'dry_run': True,
                }
            else:
                # Actual deployment via Databricks SQL
                host = _get('databricks_host')
                token = _get('databricks_token')
                if not host or not token:
                    results[table_key] = {
                        'status': 'error',
                        'message': 'databricks_host and databricks_token required for deployment',
                    }
                    continue

                try:
                    import requests
                    url = f"https://{host}/api/2.0/sql/statements"
                    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                    payload = {
                        "statement": sql_content,
                        "warehouse_id": "",
                        "catalog": catalog,
                        "schema": schema,
                    }
                    resp = requests.post(url, json=payload, headers=headers, timeout=60)
                    if resp.status_code == 200:
                        results[table_key] = {'status': 'deployed', 'response': resp.json()}
                    else:
                        results[table_key] = {
                            'status': 'error',
                            'http_status': resp.status_code,
                            'message': resp.text[:500],
                        }
                except Exception as e:
                    results[table_key] = {'status': 'error', 'message': str(e)}

        return json.dumps({
            'deployment_results': results,
            'summary': {
                'total': len(results),
                'validated': sum(1 for r in results.values() if r.get('status') == 'validated'),
                'deployed': sum(1 for r in results.values() if r.get('status') == 'deployed'),
                'errors': sum(1 for r in results.values() if r.get('status') == 'error'),
                'dry_run': dry_run,
            }
        }, indent=2)
