"""Metric View Deployer Tool for CrewAI — deploy YAML to Databricks."""
import json
import logging
import re as _re
import urllib.parse
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
    databricks_token: Optional[str] = Field(None, description="Databricks PAT for deployment", repr=False)
    warehouse_id: Optional[str] = Field(None, description="Databricks SQL warehouse ID for deployment")


class MetricViewDeployerTool(BaseTool):
    """Deploy UC Metric View YAML + SQL to a Databricks workspace."""
    name: str = "Metric View Deployer"
    description: str = (
        "Deploy UC Metric View definitions to Databricks. Accepts YAML specs and deploy SQL "
        "from the UC Metric View Generator tool. Supports dry_run mode (default) for validation "
        "without actual deployment. When dry_run=False, deploys via the UC Metric View REST API "
        "(YAML-based creation). "
        "Input: yaml_specs_json + sql_specs_json from tool 86. "
        "Output: deployment status per metric view."
    )
    args_schema: Type[BaseModel] = MetricViewDeployerSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    _ALLOWED_HOST_SUFFIXES = (
        '.cloud.databricks.com', '.azuredatabricks.net', '.gcp.databricks.com',
        '.databricks.azure.cn', '.databricksapps.com',
    )

    @staticmethod
    def _mask_secret(value: str | None) -> str:
        """Mask a secret value for logging."""
        if not value:
            return 'none'
        if len(value) <= 8:
            return '***'
        return f'{value[:4]}...{value[-4:]}'

    def __init__(self, **kwargs: Any) -> None:
        config_keys = ('yaml_specs_json', 'sql_specs_json', 'catalog', 'schema_name',
                       'dry_run', 'databricks_host', 'databricks_token', 'warehouse_id')
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
                # Actual deployment via UC Metric View REST API (YAML-based)
                host = _get('databricks_host')
                token = _get('databricks_token')
                warehouse_id = _get('warehouse_id') or ''
                if not host or not token:
                    results[table_key] = {
                        'status': 'error',
                        'message': 'databricks_host and databricks_token required for deployment',
                    }
                    continue
                if not warehouse_id:
                    results[table_key] = {
                        'status': 'error',
                        'message': 'warehouse_id required for deployment',
                    }
                    continue

                if not yaml_content or not yaml_content.strip():
                    results[table_key] = {
                        'status': 'error',
                        'message': 'Empty YAML content',
                    }
                    continue

                # Validate databricks_host (SSRF prevention)
                parsed_host = urllib.parse.urlparse(
                    f'https://{host}' if not host.startswith('http') else host)
                if not parsed_host.hostname:
                    results[table_key] = {'status': 'error', 'message': 'Invalid databricks_host URL'}
                    continue
                if not any(parsed_host.hostname.endswith(s) for s in self._ALLOWED_HOST_SUFFIXES):
                    results[table_key] = {
                        'status': 'error',
                        'message': f'Untrusted Databricks host: {parsed_host.hostname}. '
                                   f'Must be *.cloud.databricks.com or similar.',
                    }
                    continue

                # YAML content validation (dangerous SQL check)
                from src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter import (
                    _check_dangerous_sql,
                )
                if not _check_dangerous_sql(yaml_content):
                    results[table_key] = {
                        'status': 'error',
                        'message': 'YAML contains dangerous SQL patterns',
                    }
                    continue

                # Deploy via UC Metric View API (YAML-based creation)
                try:
                    import requests as _requests

                    # UC Metric View API endpoint
                    actual_host = parsed_host.hostname
                    mv_url = f"https://{actual_host}/api/2.0/unity-catalog/metric-views"
                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    }

                    # Validate metric view name (prevent injection via PBI table names)
                    safe_table_key = _re.sub(r'[^a-zA-Z0-9_]', '_', table_key.lower())
                    safe_catalog = _re.sub(r'[^a-zA-Z0-9_]', '_', catalog)
                    safe_schema = _re.sub(r'[^a-zA-Z0-9_]', '_', schema)
                    view_name = f"{safe_catalog}.{safe_schema}.{safe_table_key}_uc_metric_view"

                    payload = {
                        "name": view_name,
                        "yaml_body": yaml_content,
                    }

                    resp = _requests.post(mv_url, json=payload, headers=headers, timeout=120)

                    if resp.status_code in (200, 201):
                        results[table_key] = {
                            'status': 'deployed',
                            'view_name': view_name,
                            'response': resp.json(),
                        }
                    elif resp.status_code == 409:
                        # Already exists — try PUT to update
                        update_url = f"{mv_url}/{view_name}"
                        resp2 = _requests.put(
                            update_url,
                            json={"yaml_body": yaml_content},
                            headers=headers,
                            timeout=120,
                        )
                        if resp2.status_code == 200:
                            results[table_key] = {
                                'status': 'updated',
                                'view_name': view_name,
                                'response': resp2.json(),
                            }
                        else:
                            results[table_key] = {
                                'status': 'error',
                                'http_status': resp2.status_code,
                                'message': resp2.text[:500],
                            }
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
                'updated': sum(1 for r in results.values() if r.get('status') == 'updated'),
                'errors': sum(1 for r in results.values() if r.get('status') == 'error'),
                'dry_run': dry_run,
            }
        }, indent=2)
