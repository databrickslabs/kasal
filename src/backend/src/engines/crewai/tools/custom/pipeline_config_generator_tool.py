"""Pipeline Config Generator Tool — calls PBI APIs directly, no LLM intermediation.

Wraps generate_config.py logic as a CrewAI tool so it can be used in the Kasal UI
with its own config form (including both SP credential sets).
"""
import json
import logging
import os
import re
import sys
from collections import defaultdict
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class PipelineConfigGeneratorSchema(BaseModel):
    """Input schema — drives the Kasal UI form."""

    # ── PBI Configuration ──
    workspace_id: Optional[str] = Field(
        None, description="[PBI] Workspace ID (GUID)")
    dataset_id: Optional[str] = Field(
        None, description="[PBI] Dataset / Semantic Model ID (GUID)")
    report_id: Optional[str] = Field(
        None, description="[PBI] Optional Report ID (GUID) for visual metadata")

    # ── Non-Admin credentials (Execute Queries API — data / mapping extraction) ──
    # Provide EITHER a Service Principal (client_id + client_secret) OR a
    # Service Account (client_id + username + password). SA is used when
    # username + password are supplied.
    tenant_id: Optional[str] = Field(
        None, description="[Auth] Azure AD Tenant ID (shared by all credentials)")
    client_id: Optional[str] = Field(
        None, description="[Auth - Non-Admin] Client ID (SP or SA app; workspace member, SemanticModel.ReadWrite.All)")
    client_secret: Optional[str] = Field(
        None, description="[Auth - Non-Admin SP] Client Secret (Service Principal). Optional for a Service Account.")
    username: Optional[str] = Field(
        None, description="[Auth - Non-Admin SA] Service Account username/UPN (enables password grant)")
    password: Optional[str] = Field(
        None, description="[Auth - Non-Admin SA] Service Account password")
    access_token: Optional[str] = Field(
        None, description="[Auth - Non-Admin] Pre-obtained OAuth access token (bypasses SP/SA for the data path)")
    auth_method: Optional[str] = Field(
        None, description="[Auth] Optional explicit method: 'service_principal' or 'service_account'. Auto-detected from the fields provided when unset (SP-first, matching the shared AadService).")

    # ── Admin credentials (Admin Scanner API) ──
    # Also accepts SP (admin_client_id + admin_client_secret) or SA
    # (admin_client_id + admin_username + admin_password).
    admin_client_id: Optional[str] = Field(
        None, description="[Auth - Admin] Client ID (SP or SA app; Tenant.Read.All / Power BI Admin)")
    admin_client_secret: Optional[str] = Field(
        None, description="[Auth - Admin SP] Client Secret (Service Principal). Optional for a Service Account.")
    admin_username: Optional[str] = Field(
        None, description="[Auth - Admin SA] Service Account username/UPN (enables password grant)")
    admin_password: Optional[str] = Field(
        None, description="[Auth - Admin SA] Service Account password")

    # ── Target UC Namespace ──
    catalog: Optional[str] = Field(
        None, description="[Target] Unity Catalog name (default: main)")
    schema_name: Optional[str] = Field(
        None, description="[Target] UC Schema name (default: default)")


class PipelineConfigGeneratorTool(BaseTool):
    """Generate pipeline_config.json by calling 4 PBI APIs directly."""

    name: str = "Pipeline Config Generator"
    description: str = (
        "Generate pipeline_config.json by calling 4 Power BI APIs directly — "
        "no LLM intermediation, no data truncation. Produces all 26 config keys "
        "with auto-filled values and TODO markers where human input is needed. "
        "Requires two credential sets — non-admin (Execute Queries API) and "
        "admin (Admin Scanner API) — each of which may be a Service Principal "
        "(client_id + client_secret) or a Service Account (client_id + username "
        "+ password). Output is the full config JSON ready for the UC Metric "
        "View Generator (Tool 86)."
    )
    args_schema: Type[BaseModel] = PipelineConfigGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            "workspace_id", "dataset_id", "report_id",
            "tenant_id", "client_id", "client_secret",
            "username", "password", "access_token", "auth_method",
            "admin_client_id", "admin_client_secret",
            "admin_username", "admin_password",
            "catalog", "schema_name",
        )
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        """Execute the pipeline config generation."""
        def _get(key: str) -> Any:
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        workspace_id = _get("workspace_id")
        dataset_id = _get("dataset_id")
        report_id = _get("report_id")
        tenant_id = _get("tenant_id")
        client_id = _get("client_id")
        client_secret = _get("client_secret")
        username = _get("username")
        password = _get("password")
        access_token = _get("access_token")
        auth_method = _get("auth_method")
        admin_client_id = _get("admin_client_id")
        admin_client_secret = _get("admin_client_secret")
        admin_username = _get("admin_username")
        admin_password = _get("admin_password")
        catalog = _get("catalog") or "main"
        schema = _get("schema_name") or "default"

        # A credential set is valid as a Service Principal (client_id +
        # client_secret), a Service Account (client_id + username + password),
        # or a pre-obtained OAuth access_token (non-admin only).
        def _creds_ok(cid, secret, user, pw) -> bool:
            has_sp = bool(cid and secret)
            has_sa = bool(cid and user and pw)
            return has_sp or has_sa

        # Validate required fields
        if not workspace_id or not dataset_id:
            return json.dumps({"error": "workspace_id and dataset_id are required."})
        if not access_token and not tenant_id:
            return json.dumps({"error": "tenant_id is required (unless a pre-obtained access_token is supplied)."})
        if not access_token and not _creds_ok(client_id, client_secret, username, password):
            return json.dumps({"error": (
                "Non-admin credentials required: a Service Principal "
                "(client_id + client_secret), a Service Account "
                "(client_id + username + password), or a pre-obtained access_token."
            )})
        if not _creds_ok(admin_client_id, admin_client_secret, admin_username, admin_password):
            return json.dumps({"error": (
                "Admin credentials required: either a Service Principal "
                "(admin_client_id + admin_client_secret) or a Service Account "
                "(admin_client_id + admin_username + admin_password)."
            )})

        try:
            # Import generate_config module
            gen = self._import_generate_config()

            # Resolve both tokens through the shared AadService — the SAME auth
            # path the Semantic Model Fetcher and UC Metric View Generator use.
            # AadService supports Service Principal, Service Account, and User
            # OAuth, with explicit auth_method honoured and SP-first auto-detect.
            logger.info("[PipelineConfigGen] Acquiring tokens via shared AadService...")
            token = self._resolve_token(
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret,
                username=username, password=password, access_token=access_token,
                auth_method=auth_method, label="non-admin",
            )
            admin_token = self._resolve_token(
                tenant_id=tenant_id, client_id=admin_client_id, client_secret=admin_client_secret,
                username=admin_username, password=admin_password, access_token=None,
                auth_method=auth_method, label="admin",
            )
            warnings: list[str] = []

            # API 1: Relationships (graceful — XMLA may be disabled)
            relationships: list[dict] = []
            try:
                logger.info("[PipelineConfigGen] API 1: INFO.VIEW.RELATIONSHIPS()...")
                relationships = gen.extract_relationships(token, workspace_id, dataset_id)
                logger.info(f"[PipelineConfigGen]   → {len(relationships)} relationships")
            except Exception as e:
                msg = f"API 1 (Relationships) failed: {e}"
                logger.warning(f"[PipelineConfigGen] {msg}")
                warnings.append(msg)

            # API 2: Measures (graceful — XMLA may be disabled)
            measures: list[dict] = []
            try:
                logger.info("[PipelineConfigGen] API 2: $SYSTEM.MDSCHEMA_MEASURES...")
                measures = gen.extract_measures(token, workspace_id, dataset_id)
                logger.info(f"[PipelineConfigGen]   → {len(measures)} measures")
            except Exception as e:
                msg = f"API 2 (Measures) failed: {e}"
                logger.warning(f"[PipelineConfigGen] {msg}")
                warnings.append(msg)

            # API 3: Admin Scanner (required — uses admin token, no XMLA needed)
            logger.info("[PipelineConfigGen] API 3: Admin Scanner (workspace scan)...")
            scan_result = gen.trigger_admin_scan(admin_token, workspace_id)
            admin_tables = gen.parse_admin_tables(scan_result, dataset_id=dataset_id)
            logger.info(f"[PipelineConfigGen]   → {len(admin_tables)} tables")

            # If API 1+2 failed but admin scan has measures, extract from scan
            if not measures and admin_tables:
                logger.info("[PipelineConfigGen] Extracting measures from admin scan (XMLA fallback)...")
                for tbl_name, tbl_info in admin_tables.items():
                    for m in tbl_info.get("measures", []):
                        measures.append({
                            "measure_name": m.get("name", ""),
                            "table_name": tbl_name,
                            "expression": m.get("expression", ""),
                            "description": m.get("description", ""),
                        })
                logger.info(f"[PipelineConfigGen]   → {len(measures)} measures from admin scan")

            # API 4: Report Definition (optional)
            report_def = None
            if report_id:
                logger.info("[PipelineConfigGen] API 4: Report Definition...")
                report_def = gen.extract_report_definition(
                    token, workspace_id, report_id,
                    tenant_id=tenant_id, client_id=client_id, client_secret=client_secret,
                )

            # Build config
            logger.info("[PipelineConfigGen] Building config...")
            config = gen.build_config(
                relationships, measures, admin_tables, report_def,
                catalog=catalog, schema=schema,
            )

            # ── Auto-enrich: scan DAX to populate filter_sets, switch_decompositions,
            # and measure_resolutions that the UCMV tool depends on ──
            enriched = self._enrich_config_from_dax(config, measures)
            if enriched:
                logger.info(
                    f"[PipelineConfigGen] DAX enrichment: "
                    f"{len(config.get('filter_sets', {}))} filter sets, "
                    f"{len(config.get('switch_decompositions', {}))} switch tables, "
                    f"{len(config.get('measure_resolutions', {}))} measure resolutions"
                )

            # Summary stats
            config_json = json.dumps(config, default=str)
            auto_count = 0
            todo_count = 0
            for val in config.values():
                val_str = json.dumps(val, default=str)
                if "TODO" in val_str:
                    todo_count += 1
                elif val:
                    auto_count += 1

            output = {
                "proposed_config": config,
                "summary": {
                    "total_keys": len(config),
                    "auto_filled": auto_count,
                    "needs_human_review": todo_count,
                    "empty_null": len(config) - auto_count - todo_count,
                    "total_todo_markers": config_json.count("TODO"),
                    "relationships_extracted": len(relationships),
                    "measures_extracted": len(measures),
                    "admin_tables_scanned": len(admin_tables),
                },
            }

            logger.info(
                f"[PipelineConfigGen] Done: {auto_count} auto-filled, "
                f"{todo_count} need review, {config_json.count('TODO')} TODO markers"
            )
            return json.dumps(output, indent=2, default=str)

        except Exception as e:
            logger.exception("[PipelineConfigGen] Failed")
            return json.dumps({"error": str(e)})

    @staticmethod
    def _enrich_config_from_dax(config: dict, measures: list[dict]) -> bool:
        """Scan DAX expressions to auto-populate filter_sets, switch_decompositions,
        and measure_resolutions.

        Mutates config in place. Returns True if anything was added.
        """
        if not measures:
            return False

        changed = False
        filter_sets: dict[str, list[str]] = config.get('filter_sets', {})
        switch_decompositions: dict[str, dict] = config.get('switch_decompositions', {})
        measure_resolutions: dict[str, dict] = config.get('measure_resolutions', {})

        # ── 1. Extract filter_sets from CALCULATE(..., Table[col] = "val") patterns ──
        # Collects all literal value sets used in filter conditions per column.
        col_values: dict[str, set[str]] = defaultdict(set)
        for m in measures:
            dax = m.get('expression', '') or ''
            # Table[col] = "val" in CALCULATE filter arguments
            for hit in re.finditer(r'(\w+)\[(\w+)\]\s*=\s*"([^"]*)"', dax):
                col_name = hit.group(2)
                col_values[col_name].add(hit.group(3))
            # Table[col] IN {"a","b","c"}
            for hit in re.finditer(r'(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}', dax, re.IGNORECASE):
                col_name = hit.group(2)
                vals = re.findall(r'"([^"]*)"', hit.group(3))
                for v in vals:
                    col_values[col_name].add(v)
            # var CWC_List = {"APET","CAN",...}
            for hit in re.finditer(r'var\s+(\w+)\s*=\s*\{([^}]+)\}', dax, re.IGNORECASE):
                var_name = hit.group(1)
                vals = re.findall(r'"([^"]*)"', hit.group(2))
                if vals and len(vals) >= 2:
                    fs_key = var_name.upper()
                    if fs_key not in filter_sets:
                        filter_sets[fs_key] = sorted(set(vals))
                        changed = True

        # Promote column value sets into named filter sets when they have 3+ values
        for col, vals in col_values.items():
            if len(vals) >= 3:
                fs_key = f'{col.upper()}_FILTER'
                if fs_key not in filter_sets:
                    filter_sets[fs_key] = sorted(vals)
                    changed = True

        # ── 2. Detect SWITCH(TRUE(), ...) decompositions ──
        switch_re = re.compile(
            r'SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,\s*(.*?)\s*\)\s*$',
            re.IGNORECASE | re.DOTALL,
        )
        for m in measures:
            dax = m.get('expression', '') or ''
            table_name = m.get('table_name', '')
            measure_name = m.get('measure_name', '')
            sm = switch_re.search(dax)
            if not sm:
                continue

            body = sm.group(1)
            # Parse SWITCH branches: condition, expression pairs
            branches: dict[str, dict] = {}
            # Split on top-level commas (respecting parens)
            parts: list[str] = []
            depth = 0
            buf: list[str] = []
            for ch in body:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                elif ch == ',' and depth == 0:
                    parts.append(''.join(buf).strip())
                    buf = []
                    continue
                buf.append(ch)
            if buf:
                parts.append(''.join(buf).strip())

            # Pair up: (condition, expression), (condition, expression), ...
            for idx in range(0, len(parts) - 1, 2):
                cond_text = parts[idx].strip()
                expr_text = parts[idx + 1].strip()
                # Extract a readable branch name from the condition
                cond_match = re.search(r'SELECTEDVALUE\s*\(\s*\w+\[(\w+)\]\s*\)\s*=\s*"([^"]*)"', cond_text, re.IGNORECASE)
                if cond_match:
                    branch_name = cond_match.group(2)
                    branches[branch_name] = {
                        'condition': cond_text,
                        'sql_expr': '',  # needs manual fill or further translation
                    }

            if branches:
                if table_name not in switch_decompositions:
                    switch_decompositions[table_name] = {}
                switch_decompositions[table_name][measure_name] = branches
                changed = True

        # ── 3. Detect SELECTEDVALUE dimensions for measure_resolutions ──
        for m in measures:
            dax = m.get('expression', '') or ''
            table_name = m.get('table_name', '')
            measure_name = m.get('measure_name', '')
            for hit in re.finditer(r'SELECTEDVALUE\s*\(\s*(\w+)\[(\w+)\]\s*\)', dax, re.IGNORECASE):
                dim_table = hit.group(1)
                dim_col = hit.group(2)
                key = f'{dim_table}.{dim_col}'
                if key not in measure_resolutions:
                    measure_resolutions[key] = {
                        'table': dim_table,
                        'column': dim_col,
                        'used_by': [],
                    }
                    changed = True
                users = measure_resolutions[key].get('used_by', [])
                ref = f'{table_name}.{measure_name}'
                if ref not in users:
                    users.append(ref)

        if changed:
            config['filter_sets'] = filter_sets
            config['switch_decompositions'] = switch_decompositions
            config['measure_resolutions'] = measure_resolutions
        return changed

    @staticmethod
    @staticmethod
    def _resolve_token(
        tenant_id: Optional[str],
        client_id: Optional[str],
        client_secret: Optional[str],
        username: Optional[str],
        password: Optional[str],
        access_token: Optional[str],
        auth_method: Optional[str],
        label: str,
    ) -> str:
        """Resolve a Power BI token via the shared AadService.

        Same auth path as the Semantic Model Fetcher and UC Metric View
        Generator: honours an explicit ``auth_method``; otherwise auto-detects
        Service Principal first, then Service Account. ``AadService`` is
        synchronous, so it is safe to call directly from this sync tool.
        """
        from src.converters.services.powerbi.authentication import AadService

        service = AadService(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            access_token=access_token,
            username=username,
            password=password,
            auth_method=auth_method,
        )
        detected = service._determine_auth_method() if not access_token else "user_oauth"
        logger.info(f"[PipelineConfigGen] {label} token via: {detected or 'UNKNOWN'}")
        return service.get_access_token()

    @staticmethod
    def _import_generate_config():
        """Import the generate_config module.

        Search order:
        1. Same directory as this tool file (bundled for deployed apps).
        2. examples/uc_metric_view_migration/ relative to the repo root
           (local dev convenience).
        """
        this_dir = os.path.dirname(os.path.abspath(__file__))

        # Priority 1: bundled alongside the tool (deployed app path)
        candidates = [this_dir]

        # Priority 2: examples directory (local dev)
        project_root = os.path.abspath(
            os.path.join(this_dir, "..", "..", "..", "..", "..", "..", "..")
        )
        candidates.append(os.path.join(project_root, "examples", "uc_metric_view_migration"))

        gen_config_dir = None
        for candidate in candidates:
            if os.path.isfile(os.path.join(candidate, "generate_config.py")):
                gen_config_dir = candidate
                break

        if gen_config_dir is None:
            checked = ", ".join(candidates)
            raise ImportError(
                f"generate_config.py not found in any of: [{checked}]. "
                f"Resolved from {this_dir} (project_root={project_root})"
            )

        if gen_config_dir not in sys.path:
            sys.path.insert(0, gen_config_dir)

        import generate_config  # noqa: E402
        return generate_config
