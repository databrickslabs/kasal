"""Metric View Deployer Tool for CrewAI — deploy YAML to Databricks via SQL Statement API.

Modes:
- ``deploy`` (default): CREATE OR REPLACE VIEW per YAML spec. Before a redeploy the direct
  grants are read and re-applied after a successful deploy (``preserve_grants``). After each
  deploy the live definition is DESCRIBEd and compared with the YAML.
- ``verify``: read-only. Compares the deployed definition with the YAML field by field.
- ``check``: read-only. Runs every measure as a count (optionally grouped by a dimension,
  optionally timed).
"""

import json
import logging
import re as _re
import time
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Type

from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.metric_view_deployer_utils.definition import (
    definition_report,
    parse_yaml_spec,
    verify_view_definition,
)
from src.services.tools.metric_view_deployer_utils.grants import (
    DirectGrant,
    GrantReadError,
    read_direct_grants,
    restore_grants,
)
from src.services.tools.metric_view_deployer_utils.inspection import (
    check_one,
    deploy_summary,
    inspection_summary,
)
from src.services.tools.metric_view_deployer_utils.sql_helpers import (
    SqlRunner,
    first_cell,
    sanitize_identifier,
)
from src.services.tools.metric_view_deployer_utils.yaml_safety import yaml_is_safe
from src.services.tools.metric_view_utils.raw_fields import as_map, opt_str

if TYPE_CHECKING:
    from src.utils.databricks_auth import AuthContext

logger = logging.getLogger(__name__)

_MODES = ("deploy", "verify", "check")
_DEFAULT_SLOW_S = 30.0
_EMPTY_YAML = "no metric view: empty YAML (a table without measures)"


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
        description='JSON dict of source catalog replacements, e.g. {"acme_prod": "main"}',
    )
    mode: Optional[str] = Field(
        None,
        description="deploy (default) | verify: deployed definition vs YAML | "
        "check: every measure executes. verify and check are read-only.",
    )
    preserve_grants: bool = Field(
        True,
        description="Re-apply direct grants after a redeploy (CREATE OR REPLACE drops them)",
    )
    group_by: Optional[str] = Field(
        None, description="check mode: also run each measure grouped by this dimension"
    )
    timing: bool = Field(
        False, description="check mode: report per-measure seconds and a SLOW flag"
    )
    slow_threshold_s: float = Field(
        _DEFAULT_SLOW_S, description="check mode: seconds above which a measure is SLOW"
    )


@dataclass(frozen=True)
class _RunOptions:
    catalog: str
    schema: str
    host_override: Optional[str]
    warehouse_id: str
    preserve_grants: bool
    group_by: Optional[str]
    timing: bool
    slow_s: float


@dataclass
class _DeployState:
    schema_ensured: bool = False


def _view_parts(table_key: str, catalog: str, schema: str) -> tuple[str, str, str]:
    """Sanitised (catalog, schema, view) for a table key."""
    return (
        sanitize_identifier(catalog),
        sanitize_identifier(schema),
        sanitize_identifier(table_key.lower()),
    )


def _seconds(value: object, default: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return default
    try:
        seconds = float(value)
    except ValueError:
        return default
    return seconds if seconds > 0 else default


class MetricViewDeployerTool(BaseTool):
    """Deploy UC Metric View YAML definitions to Databricks via the SQL Statement API."""

    name: str = "Metric View Deployer"
    description: str = (
        "Deploy UC Metric View definitions to Databricks. Accepts the full ucmv_output from "
        "the UC Metric View Generator tool (auto-injected in flow). "
        "Executes CREATE OR REPLACE METRIC VIEW DDL via the Databricks SQL Statement API, "
        "re-applies direct grants, and checks the deployed definition against the YAML. "
        "mode='verify' compares the deployed definition with the YAML; mode='check' runs "
        "every measure as a count (read-only). Output: status per metric view."
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

    def __init__(self, **kwargs: object) -> None:
        config_keys = (
            "ucmv_output",
            "yaml_specs_json",
            "catalog",
            "schema_name",
            "dry_run",
            "databricks_host",
            "warehouse_id",
            "catalog_remap",
            "mode",
            "preserve_grants",
            "group_by",
            "timing",
            "slow_threshold_s",
        )
        # Always seed ucmv_output so flow_methods.py injection check fires
        default_config: dict = {"ucmv_output": None}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _authenticate(
        self, host_override: Optional[str] = None
    ) -> Optional["AuthContext"]:
        """Obtain an AuthContext synchronously (OBO → PAT → SPN).

        ``host_override`` (the ``databricks_host`` input) may only re-spell the
        credential's own workspace; any other host is refused (audit N1/F3a).
        Kept as a method so tests can patch it.
        """
        from src.services.tools.databricks_tool_utils import resolve_tool_auth

        return resolve_tool_auth(host_override)

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

        resp = _requests.post(  # host checked by is_trusted_databricks_host in _run — nosemgrep: tmp.databricks.ssrf.requests-user-url
            url, json=payload, headers=headers, timeout=60
        )
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
            pr = _requests.get(  # same trusted host; statement_id from the API — nosemgrep: tmp.databricks.ssrf.requests-user-url
                f"{url}/{statement_id}", headers=headers, timeout=30
            )
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
    # a metric view exposing NO measures. After each deploy we DESCRIBE the view,
    # confirm the declared names landed, and compare the live definition with the YAML.

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

        def _names(items: object) -> set[str]:
            out: set[str] = set()
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        nm = it.get("name")
                        if isinstance(nm, str) and nm.strip():
                            out.add(nm.strip().lower())
            return out

        return _names(spec.get("measures")), _names(spec.get("dimensions"))

    @staticmethod
    def _parse_describe_json_columns(data: object) -> Optional[tuple]:
        """Parse a ``DESCRIBE TABLE EXTENDED <view> AS JSON`` result into
        (deployed_measure_names, deployed_dimension_names), lower-cased.

        The SQL Statement API returns the JSON document as a single string cell in
        result.data_array; metric-view measure columns carry ``"is_measure": true``.
        Returns None if the shape is not what we expect (caller skips the check)."""
        cell = first_cell(data)
        if not cell:
            return None
        try:
            doc = json.loads(cell)
        except ValueError:
            return None
        if not isinstance(doc, dict):
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
        self, yaml_content: str, view_name: str, run_sql: SqlRunner
    ) -> Optional[dict]:
        """DESCRIBE the just-created metric view once. Checks that the declared
        measures/dimensions landed (``schema_check`` fields) and that the live
        definition equals the YAML (``definition``). Returns None when the YAML is not
        a mapping. Never raises — a failed check must not fail an otherwise-successful
        deploy."""
        spec = parse_yaml_spec(yaml_content)
        if spec is None:
            return None
        describe = run_sql(f"DESCRIBE TABLE EXTENDED {view_name} AS JSON")
        if not describe.get("success"):
            return {
                "schema_verified": None,
                "note": "DESCRIBE failed; schema not verified",
                "definition": {"verified": None, "note": "DESCRIBE failed"},
            }
        result: dict = {"definition": definition_report(spec, describe.get("data"))}
        declared_m, declared_d = self._declared_names_from_yaml(yaml_content)
        if not declared_m and not declared_d:
            return result
        parsed = self._parse_describe_json_columns(describe.get("data"))
        if parsed is None:
            result["schema_verified"] = None
            result["note"] = "DESCRIBE output not parseable; schema not verified"
            return result
        deployed_m, deployed_d = parsed
        return {
            **result,
            **self._compare_schema(declared_m, declared_d, deployed_m, deployed_d),
        }

    @staticmethod
    def _resolve_yaml_specs(
        yaml_raw: object, ucmv_raw: object
    ) -> tuple[dict[str, object], Optional[str]]:
        """Pick the YAML specs to deploy: a manual ``yaml_specs_json`` upload wins
        over the flow-injected ``ucmv_output``. Returns ``(specs, error)``."""
        yaml_specs: dict[str, object] = {}
        # 1. Manual upload takes priority (explicit override)
        if yaml_raw and yaml_raw != "{}":
            try:
                parsed = json.loads(yaml_raw) if isinstance(yaml_raw, str) else yaml_raw
                if isinstance(parsed, dict) and parsed:
                    yaml_specs = dict(parsed)
                    logger.info(
                        "[MVDeployer] Using manual yaml_specs_json: %s views",
                        len(yaml_specs),
                    )
            except json.JSONDecodeError as e:
                return {}, f"Invalid JSON in yaml_specs_json: {e}"

        # 2. Fall back to flow-injected ucmv_output
        if not yaml_specs and ucmv_raw:
            try:
                ucmv = json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
            except json.JSONDecodeError as e:
                logger.warning("[MVDeployer] Could not parse ucmv_output: %s", e)
            else:
                if isinstance(ucmv, Mapping):
                    yaml_dict = as_map(ucmv).get("yaml")
                    if isinstance(yaml_dict, dict):
                        yaml_specs = dict(yaml_dict)
                    logger.info(
                        "[MVDeployer] Using flow ucmv_output: %s views",
                        len(yaml_specs),
                    )
                else:
                    logger.warning(
                        "[MVDeployer] Could not parse ucmv_output: not a JSON object"
                    )
        return yaml_specs, None

    @staticmethod
    def _apply_catalog_remap(
        yaml_specs: dict[str, object], remap_raw: object
    ) -> dict[str, object]:
        """Replace source catalog names in every YAML spec (best-effort: a failure
        logs a warning and leaves the specs untouched)."""
        if not remap_raw:
            return yaml_specs
        try:
            remap = json.loads(remap_raw) if isinstance(remap_raw, str) else remap_raw
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
                        "[MVDeployer] Ignoring catalog_remap entries with "
                        "non-identifier values: %s",
                        sorted(dropped),
                    )
                remapped: dict[str, object] = {}
                for k, v in yaml_specs.items():
                    text = v
                    for old_cat, new_cat in safe_remap.items():
                        if not isinstance(text, str):
                            raise TypeError(f"YAML spec {k!r} is not text")
                        text = text.replace(old_cat, new_cat)
                    remapped[k] = text
                logger.info("[MVDeployer] Applied catalog remap: %s", safe_remap)
                return remapped
        except Exception as e:
            logger.warning(
                "[MVDeployer] Could not apply catalog_remap: %s", e, exc_info=True
            )
        return yaml_specs

    # ── Run steps ────────────────────────────────────────────────────────────

    def _open_session(
        self, opts: _RunOptions
    ) -> tuple[Optional[SqlRunner], Optional[str]]:
        """Authenticate, check the workspace host (SEC #5, fails closed) and return a
        SQL runner bound to it, or ``(None, error message)``."""
        try:
            auth = self._authenticate(host_override=opts.host_override)
            if not auth:
                return None, "Authentication failed"
            headers = auth.get_headers()
            from src.utils.telemetry import KasalProduct, get_user_agent_header

            headers.update(get_user_agent_header(KasalProduct.POWERBI))
            workspace_url = (auth.workspace_url or "").rstrip("/")
        except Exception as e:
            logger.warning("[MVDeployer] Authentication error: %s", e, exc_info=True)
            return None, f"Authentication error: {e}"
        if not workspace_url:
            return None, "workspace_url not configured"
        try:
            from src.utils.url_security import is_trusted_databricks_host

            host_ok = is_trusted_databricks_host(workspace_url)
        except Exception:
            logger.warning("[MVDeployer] Host check unavailable", exc_info=True)
            host_ok = False
        if not host_ok:
            hostname = urllib.parse.urlparse(workspace_url).hostname
            return None, f"Untrusted host: {hostname}"
        warehouse_id = opts.warehouse_id

        def run_sql(sql: str) -> dict:
            return self._execute_sql_sync(sql, workspace_url, warehouse_id, headers)

        return run_sql, None

    def _validate_views(
        self, yaml_specs: dict[str, object], opts: _RunOptions
    ) -> dict[str, dict[str, object]]:
        """Dry run: no SQL, only the view names and line counts."""
        results: dict[str, dict[str, object]] = {}
        for table_key in sorted(yaml_specs):
            raw_yaml = yaml_specs.get(table_key)
            yaml_content = raw_yaml if isinstance(raw_yaml, str) else ""
            safe_key = _re.sub(r"[^a-zA-Z0-9_]", "_", table_key.lower())
            results[table_key] = {
                "status": "validated",
                "view_name": f"{opts.catalog}.{opts.schema}.{safe_key}",
                "yaml_lines": len(yaml_content.split("\n")),
                "dry_run": True,
            }
        return results

    def _deploy_views(
        self, yaml_specs: dict[str, object], opts: _RunOptions
    ) -> dict[str, dict[str, object]]:
        results: dict[str, dict[str, object]] = {}
        state = _DeployState()
        for table_key in sorted(yaml_specs):
            raw_yaml = yaml_specs.get(table_key)
            yaml_content = raw_yaml if isinstance(raw_yaml, str) else ""
            results[table_key] = self._deploy_one(table_key, yaml_content, opts, state)
        return results

    def _deploy_one(
        self, table_key: str, yaml_content: str, opts: _RunOptions, state: _DeployState
    ) -> dict[str, object]:
        if not opts.warehouse_id:
            return {"status": "error", "message": "warehouse_id is required"}
        if not yaml_content or not yaml_content.strip():
            return {"status": "skipped", "message": _EMPTY_YAML}

        cat, sch, view = _view_parts(table_key, opts.catalog, opts.schema)
        view_name = f"{cat}.{sch}.{view}"
        try:
            ddl = self._yaml_to_ddl(yaml_content, view_name)
        except Exception as e:
            logger.warning("[MVDeployer] DDL generation failed: %s", e, exc_info=True)
            return {"status": "error", "message": f"DDL generation failed: {e}"}

        runner, error = self._open_session(opts)
        if runner is None:
            return {"status": "error", "message": error or "Authentication failed"}

        if not yaml_is_safe(yaml_content):
            return {
                "status": "error",
                "message": "YAML contains dangerous SQL patterns",
            }
        try:
            if not state.schema_ensured:
                self._ensure_schema(runner, cat, sch)
                state.schema_ensured = True
            return self._execute_deploy(
                runner, ddl, yaml_content, view_name, (cat, sch, view), opts
            )
        except Exception as e:
            logger.warning("[MVDeployer] deploy failed: %s", e, exc_info=True)
            return {"status": "error", "message": str(e)}

    @staticmethod
    def _ensure_schema(runner: SqlRunner, cat: str, sch: str) -> None:
        """Create the target schema once per run (failure is logged, not fatal)."""
        result = runner(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
        if not result.get("success"):
            logger.warning(
                "[MVDeployer] Could not ensure schema: %s", result.get("error")
            )
        else:
            logger.info("[MVDeployer] Schema %s.%s ensured", cat, sch)

    def _execute_deploy(
        self,
        runner: SqlRunner,
        ddl: str,
        yaml_content: str,
        view_name: str,
        parts: tuple[str, str, str],
        opts: _RunOptions,
    ) -> dict[str, object]:
        """Read grants, run the DDL, re-apply grants, then run the post-deploy checks."""
        cat, sch, view = parts
        grants_before: list[DirectGrant] = []
        if opts.preserve_grants:
            try:
                grants_before, _ = read_direct_grants(runner, cat, sch, view)
            except GrantReadError as e:
                return {
                    "status": "error",
                    "view_name": view_name,
                    "message": (
                        f"could not read direct grants before redeploy; view left as is ({e}). "
                        "Grant SELECT on information_schema.table_privileges, or set "
                        "preserve_grants=false to redeploy without preserving grants"
                    ),
                    "http_status": e.http_status,
                }

        outcome = runner(ddl)
        if not outcome.get("success"):
            return {
                "status": "error",
                "view_name": view_name,
                "message": outcome.get("error", "Unknown error"),
                "http_status": outcome.get("http_status"),
            }

        entry: dict[str, object] = {"status": "deployed", "view_name": view_name}
        grants_ok = True
        if opts.preserve_grants:
            grants = (
                restore_grants(runner, view_name, cat, sch, view, grants_before)
                if grants_before
                else {"before": 0, "after": 0, "preserved": True, "failures": []}
            )
            entry["grants"] = grants
            grants_ok = bool(grants.get("preserved"))

        problems: list[str] = []
        try:
            schema_check = self._verify_deployed_schema(yaml_content, view_name, runner)
        except Exception as e:
            logger.warning("[MVDeployer] schema check error: %s", e, exc_info=True)
            schema_check = {"schema_verified": None, "note": f"schema check error: {e}"}
        if schema_check is not None:
            entry["schema_check"] = schema_check
            if schema_check.get("truncated"):
                entry["status"] = "deployed_incomplete"
                problems.append(
                    f"Metric view deployed but exposes 0 of "
                    f"{schema_check['declared_measures']} declared measures "
                    f"— YAML likely truncated"
                )
            definition = schema_check.get("definition")
            if isinstance(definition, dict) and definition.get("verified") is False:
                problems.append(
                    f"live definition differs from the YAML in "
                    f"{definition.get('difference_count')} place(s); see schema_check.definition"
                )
        if not grants_ok:
            if entry["status"] == "deployed":
                entry["status"] = "deployed_grants_incomplete"
            problems.append(
                "direct grants not fully re-applied after redeploy; see grants"
            )
        if problems:
            entry["message"] = "; ".join(problems)
        return entry

    def _inspect_views(
        self, mode: str, yaml_specs: dict[str, object], opts: _RunOptions
    ) -> str:
        """verify / check modes: read-only inspection of the deployed views."""
        if not opts.warehouse_id:
            return json.dumps({"error": f"warehouse_id is required for mode {mode!r}"})
        runner, error = self._open_session(opts)
        if runner is None:
            return json.dumps({"error": error or "Authentication failed"})
        results: dict[str, dict[str, object]] = {}
        for table_key in sorted(yaml_specs):
            raw_yaml = yaml_specs.get(table_key)
            spec = parse_yaml_spec(raw_yaml) if isinstance(raw_yaml, str) else None
            cat, sch, view = _view_parts(table_key, opts.catalog, opts.schema)
            view_name = f"{cat}.{sch}.{view}"
            if spec is None:
                results[table_key] = {
                    "status": "error",
                    "view_name": view_name,
                    "message": "YAML is not a mapping",
                }
                continue
            try:
                if mode == "verify":
                    results[table_key] = verify_view_definition(runner, view_name, spec)
                else:
                    results[table_key] = check_one(
                        runner, view_name, spec, opts.group_by, opts.timing, opts.slow_s
                    )
            except Exception as e:
                logger.warning("[MVDeployer] %s failed: %s", mode, e, exc_info=True)
                results[table_key] = {
                    "status": "error",
                    "view_name": view_name,
                    "message": str(e),
                }
        return json.dumps(
            {
                "mode": mode,
                "results": results,
                "summary": inspection_summary(results),
                "yaml": yaml_specs,
            },
            indent=2,
        )

    def _run(self, **kwargs: object) -> str:
        def _get(key: str) -> object:
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        mode = str(_get("mode") or "deploy").strip().lower()
        if mode not in _MODES:
            return json.dumps(
                {
                    "error": f"Unknown mode {mode!r}; expected one of: {', '.join(_MODES)}"
                }
            )
        dry_run = bool(_get("dry_run"))
        opts = _RunOptions(
            catalog=str(_get("catalog") or "main"),
            schema=str(_get("schema_name") or "default"),
            host_override=opt_str(_get("databricks_host")) or None,
            warehouse_id=str(_get("warehouse_id") or ""),
            preserve_grants=_get("preserve_grants") is not False,
            group_by=opt_str(_get("group_by")) or None,
            timing=bool(_get("timing")),
            slow_s=_seconds(_get("slow_threshold_s"), _DEFAULT_SLOW_S),
        )

        # ── Parse yaml specs — manual upload wins over flow injection ────────
        yaml_specs, spec_error = self._resolve_yaml_specs(
            _get("yaml_specs_json") or "{}", _get("ucmv_output")
        )
        if spec_error:
            return json.dumps({"error": spec_error})
        if not yaml_specs:
            return json.dumps(
                {
                    "error": "No metric view specs found — ucmv_output not injected and no yaml_specs_json provided"
                }
            )

        # ── Apply catalog remapping to every YAML spec ────────────────────────
        yaml_specs = self._apply_catalog_remap(yaml_specs, _get("catalog_remap"))

        if mode != "deploy":
            return self._inspect_views(mode, yaml_specs, opts)
        if dry_run:
            results = self._validate_views(yaml_specs, opts)
        else:
            results = self._deploy_views(yaml_specs, opts)
        return json.dumps(
            {
                "deployment_results": results,
                "summary": deploy_summary(results, dry_run),
                # Pass yaml_specs through so downstream steps (Genie Config Generator)
                # can pick them up via the 'yaml' key in flow injection
                "yaml": yaml_specs,
            },
            indent=2,
        )
