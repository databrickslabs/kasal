"""Metric View Deployer Tool for CrewAI — deploy YAML to Databricks via SQL Statement API."""

import asyncio
import json
import logging
import re as _re
import time
import urllib.parse
from typing import Any, Optional, Type

from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool

logger = logging.getLogger(__name__)


class MetricViewDeployerSchema(BaseModel):
    """Input schema for MetricViewDeployerTool."""

    ucmv_output: Optional[str] = Field(
        None,
        description="Full JSON output from UC Metric View Generator (auto-injected by flow)",
    )
    yaml_specs_json: Optional[str] = Field(
        None,
        description="JSON dict of table_key → YAML string (from UC Metric View Generator)",
    )
    catalog: Optional[str] = Field(None, description="Target UC catalog")
    schema_name: Optional[str] = Field(None, description="Target UC schema")
    dry_run: bool = Field(False, description="If True, validate only without deploying")
    databricks_host: Optional[str] = Field(
        None, description="Override workspace URL (optional)"
    )
    warehouse_id: Optional[str] = Field(None, description="Databricks SQL warehouse ID")
    catalog_remap: Optional[str] = Field(
        None,
        description='JSON dict of source catalog replacements, e.g. {"dc_datalake_prod_001": "david_test_metrics"}',
    )


class MetricViewDeployerTool(BaseTool):
    """Deploy UC Metric View YAML definitions to Databricks via the SQL Statement API."""

    name: str = "Metric View Deployer"
    description: str = (
        "Deploy UC Metric View definitions to Databricks. Accepts the full ucmv_output from "
        "the UC Metric View Generator tool (auto-injected in flow). "
        "Executes CREATE OR REPLACE METRIC VIEW DDL via the Databricks SQL Statement API. "
        "Output: deployment status per metric view."
    )
    args_schema: Type[BaseModel] = MetricViewDeployerSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    _ALLOWED_HOST_SUFFIXES = (
        ".cloud.databricks.com",
        ".azuredatabricks.net",
        ".gcp.databricks.com",
        ".databricks.azure.cn",
        ".databricksapps.com",
    )

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            "ucmv_output",
            "yaml_specs_json",
            "catalog",
            "schema_name",
            "dry_run",
            "databricks_host",
            "warehouse_id",
            "catalog_remap",
        )
        # Always seed ucmv_output so flow_methods.py injection check fires
        default_config: dict = {"ucmv_output": None}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _authenticate(self, host_override: Optional[str] = None):
        """Obtain AuthContext synchronously (OBO → PAT → SPN)."""
        import concurrent.futures

        from src.utils.databricks_auth import get_auth_context

        def _run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(get_auth_context())
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            auth = executor.submit(_run_in_thread).result(timeout=30)

        if auth is not None and host_override:
            url = host_override.strip().rstrip("/")
            if not url.startswith("https://"):
                url = f"https://{url}"
            auth.workspace_url = url
        return auth

    @staticmethod
    def _yaml_to_ddl(yaml_content: str, view_name: str) -> str:
        """Wrap UC Metric View YAML in CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML DDL."""
        # Correct Databricks syntax for metric views (DBR 17.2+):
        # CREATE OR REPLACE VIEW <name> WITH METRICS LANGUAGE YAML AS $$ <yaml> $$
        return (
            f"CREATE OR REPLACE VIEW {view_name}\n"
            f"WITH METRICS\n"
            f"LANGUAGE YAML\n"
            f"AS $$\n"
            f"{yaml_content.strip()}\n"
            f"$$"
        )

    @staticmethod
    def _execute_sql_sync(
        sql: str, workspace_url: str, warehouse_id: str, headers: dict
    ) -> dict:
        """Execute SQL via Databricks SQL Statement API (synchronous with polling)."""
        import requests as _requests

        url = f"{workspace_url}/api/2.0/sql/statements"
        payload = {
            "statement": sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        }

        resp = _requests.post(url, json=payload, headers=headers, timeout=60)
        if resp.status_code not in (200, 201):
            return {
                "success": False,
                "http_status": resp.status_code,
                "error": resp.text[:500],
            }

        data = resp.json()
        statement_id = data.get("statement_id")
        state = data.get("status", {}).get("state", "")

        # Poll until terminal state
        polls = 0
        while state in ("RUNNING", "PENDING") and polls < 60:
            time.sleep(2)
            pr = _requests.get(f"{url}/{statement_id}", headers=headers, timeout=30)
            data = pr.json()
            state = data.get("status", {}).get("state", "")
            polls += 1

        if state == "SUCCEEDED":
            # Carry the raw payload so callers that need the result set (e.g. the
            # post-deploy DESCRIBE guard rail) can read result.data_array.
            return {"success": True, "state": state, "data": data}

        err = data.get("status", {}).get("error", {})
        return {
            "success": False,
            "state": state,
            "error": err.get("message", f"State: {state}"),
        }

    # ── Post-deploy schema guard rail ────────────────────────────────────────
    # The migration playbook warns that a truncated YAML deploys with no error as
    # a metric view exposing NO measures. After each deploy we DESCRIBE the view
    # and confirm the measures/dimensions declared in the YAML actually landed.

    @staticmethod
    def _declared_names_from_yaml(yaml_content: str) -> tuple:
        """Return (measure_names, dimension_names) declared in a metric-view YAML,
        lower-cased for case-insensitive comparison with DESCRIBE output.

        Returns empty sets on unparseable YAML — the caller then skips the check
        rather than raising."""
        import yaml as _yaml

        try:
            spec = _yaml.safe_load(yaml_content)
        except Exception:  # noqa: BLE001 — bad YAML → skip check, never break deploy
            return set(), set()
        if not isinstance(spec, dict):
            return set(), set()

        def _names(items) -> set:
            out: set = set()
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        nm = it.get("name")
                        if isinstance(nm, str) and nm.strip():
                            out.add(nm.strip().lower())
            return out

        return _names(spec.get("measures")), _names(spec.get("dimensions"))

    @staticmethod
    def _parse_describe_json_columns(data: dict) -> Optional[tuple]:
        """Parse a ``DESCRIBE TABLE EXTENDED <view> AS JSON`` result into
        (deployed_measure_names, deployed_dimension_names), lower-cased.

        The SQL Statement API returns the JSON document as a single string cell in
        result.data_array; metric-view measure columns carry ``"is_measure": true``.
        Returns None if the shape is not what we expect (caller skips the check)."""
        try:
            rows = (data or {}).get("result", {}).get("data_array") or []
            if not rows or not rows[0] or not rows[0][0]:
                return None
            doc = json.loads(rows[0][0])
        except Exception:  # noqa: BLE001
            return None
        cols = doc.get("columns")
        if not isinstance(cols, list):
            return None
        measures: set = set()
        dims: set = set()
        for c in cols:
            if not isinstance(c, dict):
                continue
            nm = c.get("name")
            if not isinstance(nm, str) or not nm.strip():
                continue
            (measures if c.get("is_measure") else dims).add(nm.strip().lower())
        return measures, dims

    @staticmethod
    def _compare_schema(
        declared_measures: set,
        declared_dimensions: set,
        deployed_measures: set,
        deployed_dimensions: set,
    ) -> dict:
        """Compare declared (YAML) vs deployed (DESCRIBE) names. Flags the
        truncation failure: declared measures exist but NONE landed in the view."""
        missing_measures = sorted(declared_measures - deployed_measures)
        missing_dimensions = sorted(declared_dimensions - deployed_dimensions)
        truncated = bool(declared_measures) and not (
            declared_measures & deployed_measures
        )
        result: dict = {
            "schema_verified": not missing_measures and not missing_dimensions,
            "declared_measures": len(declared_measures),
            "deployed_measures": len(deployed_measures),
            "declared_dimensions": len(declared_dimensions),
            "deployed_dimensions": len(deployed_dimensions),
        }
        if missing_measures:
            result["missing_measures"] = missing_measures
        if missing_dimensions:
            result["missing_dimensions"] = missing_dimensions
        if truncated:
            result["truncated"] = True
        return result

    def _verify_deployed_schema(
        self,
        yaml_content: str,
        view_name: str,
        workspace_url: str,
        warehouse_id: str,
        headers: dict,
    ) -> Optional[dict]:
        """DESCRIBE the just-created metric view and confirm its declared
        measures/dimensions landed. Returns a schema-check dict, or None if the
        check could not run. Never raises — a failed check must not fail an
        otherwise-successful deploy."""
        declared_m, declared_d = self._declared_names_from_yaml(yaml_content)
        if not declared_m and not declared_d:
            return None  # nothing to verify (or unparseable YAML)
        describe = self._execute_sql_sync(
            f"DESCRIBE TABLE EXTENDED {view_name} AS JSON",
            workspace_url,
            warehouse_id,
            headers,
        )
        if not describe.get("success"):
            return {
                "schema_verified": None,
                "note": "DESCRIBE failed; schema not verified",
            }
        parsed = self._parse_describe_json_columns(describe.get("data", {}))
        if parsed is None:
            return {
                "schema_verified": None,
                "note": "DESCRIBE output not parseable; schema not verified",
            }
        deployed_m, deployed_d = parsed
        return self._compare_schema(declared_m, declared_d, deployed_m, deployed_d)

    def _run(self, **kwargs: Any) -> str:  # noqa: C901
        def _get(key):
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        catalog = _get("catalog") or "main"
        schema = _get("schema_name") or "default"
        dry_run = _get("dry_run")
        if dry_run is None:
            dry_run = False
        host_override = _get("databricks_host") or None
        warehouse_id = _get("warehouse_id") or ""

        # ── Parse yaml specs — manual upload wins over flow injection ────────
        yaml_specs: dict = {}

        # 1. Manual upload takes priority (explicit override)
        yaml_raw = _get("yaml_specs_json") or "{}"
        if yaml_raw and yaml_raw != "{}":
            try:
                parsed = json.loads(yaml_raw) if isinstance(yaml_raw, str) else yaml_raw
                if isinstance(parsed, dict) and parsed:
                    yaml_specs = parsed
                    logger.info(
                        f"[MVDeployer] Using manual yaml_specs_json: {len(yaml_specs)} views"
                    )
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid JSON in yaml_specs_json: {e}"})

        # 2. Fall back to flow-injected ucmv_output
        if not yaml_specs:
            ucmv_raw = _get("ucmv_output")
            if ucmv_raw:
                try:
                    ucmv = (
                        json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
                    )
                    yaml_dict = ucmv.get("yaml", {})
                    if isinstance(yaml_dict, dict):
                        yaml_specs = yaml_dict
                    logger.info(
                        f"[MVDeployer] Using flow ucmv_output: {len(yaml_specs)} views"
                    )
                except (json.JSONDecodeError, AttributeError) as e:
                    logger.warning(f"[MVDeployer] Could not parse ucmv_output: {e}")

        if not yaml_specs:
            return json.dumps(
                {
                    "error": "No metric view specs found — ucmv_output not injected and no yaml_specs_json provided"
                }
            )

        # ── Apply catalog remapping to every YAML spec ────────────────────────
        remap_raw = _get("catalog_remap")
        if remap_raw:
            try:
                remap = (
                    json.loads(remap_raw) if isinstance(remap_raw, str) else remap_raw
                )
                if isinstance(remap, dict):
                    # SEC #7: validate both sides are plain (optionally dotted)
                    # identifiers before a blind substring replace into the YAML —
                    # config/agent-supplied new_cat must not inject arbitrary text.
                    _ident = _re.compile(r"^[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)*$")
                    safe_remap = {
                        old: new
                        for old, new in remap.items()
                        if isinstance(old, str)
                        and isinstance(new, str)
                        and _ident.match(old)
                        and _ident.match(new)
                    }
                    dropped = set(remap) - set(safe_remap)
                    if dropped:
                        logger.warning(
                            f"[MVDeployer] Ignoring catalog_remap entries with "
                            f"non-identifier values: {sorted(dropped)}"
                        )
                    remapped = {}
                    for k, v in yaml_specs.items():
                        for old_cat, new_cat in safe_remap.items():
                            v = v.replace(old_cat, new_cat)
                        remapped[k] = v
                    yaml_specs = remapped
                    logger.info(f"[MVDeployer] Applied catalog remap: {safe_remap}")
            except Exception as e:
                logger.warning(f"[MVDeployer] Could not apply catalog_remap: {e}")

        results = {}
        schema_ensured = False  # create schema once before first deployment

        for table_key in sorted(yaml_specs.keys()):
            yaml_content = yaml_specs.get(table_key, "")

            if dry_run:
                safe_key = _re.sub(r"[^a-zA-Z0-9_]", "_", table_key.lower())
                view_name = f"{catalog}.{schema}.{safe_key}"
                results[table_key] = {
                    "status": "validated",
                    "view_name": view_name,
                    "yaml_lines": len(yaml_content.split("\n")),
                    "dry_run": True,
                }
                continue

            # ── Actual deployment ─────────────────────────────────────────────
            if not warehouse_id:
                results[table_key] = {
                    "status": "error",
                    "message": "warehouse_id is required",
                }
                continue

            if not yaml_content or not yaml_content.strip():
                results[table_key] = {
                    "status": "error",
                    "message": "Empty YAML content",
                }
                continue

            # Generate DDL
            try:
                safe_key = _re.sub(r"[^a-zA-Z0-9_]", "_", table_key.lower())
                safe_cat = _re.sub(r"[^a-zA-Z0-9_]", "_", catalog)
                safe_sch = _re.sub(r"[^a-zA-Z0-9_]", "_", schema)
                view_name = f"{safe_cat}.{safe_sch}.{safe_key}"
                ddl = self._yaml_to_ddl(yaml_content, view_name)
            except Exception as e:
                results[table_key] = {
                    "status": "error",
                    "message": f"DDL generation failed: {e}",
                }
                continue

            # Auth
            try:
                auth = self._authenticate(host_override=host_override)
                if not auth:
                    results[table_key] = {
                        "status": "error",
                        "message": "Authentication failed",
                    }
                    continue
                headers = auth.get_headers()
                from src.utils.telemetry import KasalProduct, get_user_agent_header

                headers.update(get_user_agent_header(KasalProduct.POWERBI))
                workspace_url = (auth.workspace_url or "").rstrip("/")
            except Exception as e:
                results[table_key] = {
                    "status": "error",
                    "message": f"Authentication error: {e}",
                }
                continue

            if not workspace_url:
                results[table_key] = {
                    "status": "error",
                    "message": "workspace_url not configured",
                }
                continue

            # SSRF check — shared source of truth (SEC #5), fails closed.
            try:
                from src.utils.url_security import is_trusted_databricks_host

                _host_ok = is_trusted_databricks_host(workspace_url)
            except Exception:  # noqa: BLE001 — no helper → do not trust
                _host_ok = False
            if not _host_ok:
                parsed_host = urllib.parse.urlparse(workspace_url)
                results[table_key] = {
                    "status": "error",
                    "message": f"Untrusted host: {parsed_host.hostname}",
                }
                continue

            # Ensure schema exists (once per run, after SSRF validation)
            if not schema_ensured:
                safe_cat_s = _re.sub(r"[^a-zA-Z0-9_]", "_", catalog)
                safe_sch_s = _re.sub(r"[^a-zA-Z0-9_]", "_", schema)
                schema_result = self._execute_sql_sync(
                    f"CREATE SCHEMA IF NOT EXISTS {safe_cat_s}.{safe_sch_s}",
                    workspace_url,
                    warehouse_id,
                    headers,
                )
                if not schema_result["success"]:
                    logger.warning(
                        f"[MVDeployer] Could not ensure schema: {schema_result.get('error')}"
                    )
                else:
                    logger.info(
                        f"[MVDeployer] Schema {safe_cat_s}.{safe_sch_s} ensured"
                    )
                schema_ensured = True

            # YAML safety check
            from src.services.tools.metric_view_utils.yaml_emitter import (
                _check_dangerous_sql,
            )

            if not _check_dangerous_sql(yaml_content):
                results[table_key] = {
                    "status": "error",
                    "message": "YAML contains dangerous SQL patterns",
                }
                continue

            # Execute DDL via SQL Statement API
            try:
                result = self._execute_sql_sync(
                    ddl, workspace_url, warehouse_id, headers
                )
                if result["success"]:
                    entry: dict = {"status": "deployed", "view_name": view_name}
                    try:
                        schema_check = self._verify_deployed_schema(
                            yaml_content,
                            view_name,
                            workspace_url,
                            warehouse_id,
                            headers,
                        )
                    except (
                        Exception
                    ) as e:  # noqa: BLE001 — guard rail must never break a good deploy
                        schema_check = {
                            "schema_verified": None,
                            "note": f"schema check error: {e}",
                        }
                    if schema_check is not None:
                        entry["schema_check"] = schema_check
                        if schema_check.get("truncated"):
                            entry["status"] = "deployed_incomplete"
                            entry["message"] = (
                                f"Metric view deployed but exposes 0 of "
                                f"{schema_check['declared_measures']} declared measures "
                                f"— YAML likely truncated"
                            )
                    results[table_key] = entry
                else:
                    results[table_key] = {
                        "status": "error",
                        "view_name": view_name,
                        "message": result.get("error", "Unknown error"),
                        "http_status": result.get("http_status"),
                    }
            except Exception as e:
                results[table_key] = {"status": "error", "message": str(e)}

        return json.dumps(
            {
                "deployment_results": results,
                "summary": {
                    "total": len(results),
                    "validated": sum(
                        1 for r in results.values() if r.get("status") == "validated"
                    ),
                    "deployed": sum(
                        1 for r in results.values() if r.get("status") == "deployed"
                    ),
                    "deployed_incomplete": sum(
                        1
                        for r in results.values()
                        if r.get("status") == "deployed_incomplete"
                    ),
                    "errors": sum(
                        1 for r in results.values() if r.get("status") == "error"
                    ),
                    "dry_run": dry_run,
                },
                # Pass yaml_specs through so downstream steps (Genie Config Generator)
                # can pick them up via the 'yaml' key in flow injection
                "yaml": yaml_specs,
            },
            indent=2,
        )
