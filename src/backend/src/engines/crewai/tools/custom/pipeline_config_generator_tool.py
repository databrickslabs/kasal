"""Pipeline Config Generator Tool — calls PBI APIs directly, no LLM intermediation.

Wraps generate_config.py logic as a CrewAI tool so it can be used in the Kasal UI
with its own config form (including both SP credential sets).
"""
import json
import logging
import os
import sys
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

    # ── Non-Admin Service Principal (Execute Queries API) ──
    tenant_id: Optional[str] = Field(
        None, description="[Auth] Azure AD Tenant ID (shared by both SPs)")
    client_id: Optional[str] = Field(
        None, description="[Auth - Non-Admin SP] Client ID (workspace member, SemanticModel.ReadWrite.All)")
    client_secret: Optional[str] = Field(
        None, description="[Auth - Non-Admin SP] Client Secret")

    # ── Admin Service Principal (Admin Scanner API) ──
    admin_client_id: Optional[str] = Field(
        None, description="[Auth - Admin SP] Client ID (Tenant.Read.All / Power BI Admin)")
    admin_client_secret: Optional[str] = Field(
        None, description="[Auth - Admin SP] Client Secret")

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
        "Requires two Service Principals: non-admin (Execute Queries API) and "
        "admin (Admin Scanner API). Output is the full config JSON ready for "
        "the UC Metric View Generator (Tool 86)."
    )
    args_schema: Type[BaseModel] = PipelineConfigGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            "workspace_id", "dataset_id", "report_id",
            "tenant_id", "client_id", "client_secret",
            "admin_client_id", "admin_client_secret",
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
        admin_client_id = _get("admin_client_id")
        admin_client_secret = _get("admin_client_secret")
        catalog = _get("catalog") or "main"
        schema = _get("schema_name") or "default"

        # Validate required fields
        if not workspace_id or not dataset_id:
            return json.dumps({"error": "workspace_id and dataset_id are required."})
        if not all([tenant_id, client_id, client_secret]):
            return json.dumps({"error": "Non-admin SP credentials required: tenant_id, client_id, client_secret."})
        if not all([admin_client_id, admin_client_secret]):
            return json.dumps({"error": "Admin SP credentials required: admin_client_id, admin_client_secret."})

        try:
            # Import generate_config module
            gen = self._import_generate_config()

            logger.info("[PipelineConfigGen] Acquiring tokens...")
            token = gen.get_token(tenant_id, client_id, client_secret)
            admin_token = gen.get_token(tenant_id, admin_client_id, admin_client_secret)
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
    def _import_generate_config():
        """Import the generate_config module from examples directory."""
        # Find the generate_config.py relative to this file
        # Path: src/backend/src/engines/crewai/tools/custom/ → 7 levels up → kasal/
        this_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(
            os.path.join(this_dir, "..", "..", "..", "..", "..", "..", "..")
        )
        gen_config_dir = os.path.join(project_root, "examples", "uc_metric_view_migration")

        if not os.path.isfile(os.path.join(gen_config_dir, "generate_config.py")):
            raise ImportError(
                f"generate_config.py not found at {gen_config_dir}. "
                f"Resolved from {this_dir} (project_root={project_root})"
            )

        if gen_config_dir not in sys.path:
            sys.path.insert(0, gen_config_dir)

        import generate_config  # noqa: E402
        return generate_config
