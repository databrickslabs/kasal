"""Genie Space Generator Tool for CrewAI — deploy Genie Spaces from UC Metric Views."""
import asyncio
import json
import logging
import urllib.parse
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class GenieSpaceGeneratorSchema(BaseModel):
    """Input schema for GenieSpaceGeneratorTool."""
    # From flow (auto-injected by flow_methods.py)
    ucmv_output: Optional[str] = Field(
        None, description="UCMV Generator output (yaml/sql/stats keys) — auto-injected by flow")

    # Core config (set in tool task form)
    space_title: Optional[str] = Field(None, description="Genie space display name")
    catalog: Optional[str] = Field(None, description="UC catalog for metric views")
    schema_name: Optional[str] = Field(None, description="UC schema for metric views")
    warehouse_id: Optional[str] = Field(None, description="SQL warehouse ID")
    databricks_host: Optional[str] = Field(
        None,
        description=(
            "Databricks workspace URL override (e.g. https://adb-123.azuredatabricks.net). "
            "If omitted, workspace URL is resolved from Kasal Settings / DATABRICKS_HOST env var / SDK auto-detection."
        ),
    )

    # Additional tables beyond metric views
    additional_tables: Optional[str] = Field(
        None, description="Newline-separated list of extra table FQNs (e.g. catalog.schema.dim_customer)")

    # Genie space content
    text_instructions: Optional[str] = Field(None, description="Plain text general instructions for the Genie space")
    join_specs_json: Optional[str] = Field(
        None, description="JSON array of join spec objects [{left_table, right_table, join_condition}]")
    sample_questions: Optional[str] = Field(
        None, description="Newline-separated or JSON array of sample questions")
    sql_expressions_json: Optional[str] = Field(
        None, description="JSON array of {display_name, sql} SQL expression snippets")
    sql_measures_json: Optional[str] = Field(
        None, description="JSON array of {display_name, sql, instruction} SQL measure snippets")
    sql_filters_json: Optional[str] = Field(
        None, description="JSON array of {display_name, sql} SQL filter snippets")
    example_sqls_json: Optional[str] = Field(
        None, description="JSON array of {question, sql} example SQL queries")


class GenieSpaceGeneratorTool(BaseTool):
    """Create or update a Databricks Genie Space from deployed UC Metric Views."""
    name: str = "Genie Space Generator"
    description: str = (
        "Creates or updates a Databricks Genie Space from deployed UC Metric Views. "
        "Configures instructions, join specs, sample questions, and SQL snippets. "
        "Idempotent — patches existing space or creates new one. "
        "Input: ucmv_output from UCMV Generator (auto-injected in flow) + space config. "
        "Output: {space_id, url, operation, table_count, question_count}."
    )
    args_schema: Type[BaseModel] = GenieSpaceGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    _ALLOWED_HOST_SUFFIXES = (
        '.cloud.databricks.com', '.azuredatabricks.net', '.gcp.databricks.com',
        '.databricks.azure.cn', '.databricksapps.com',
    )

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            'ucmv_output', 'space_title', 'catalog', 'schema_name', 'warehouse_id',
            'databricks_host',
            'additional_tables', 'text_instructions', 'join_specs_json', 'sample_questions',
            'sql_expressions_json', 'sql_measures_json', 'sql_filters_json', 'example_sqls_json',
        )
        # Always seed ucmv_output so flow_methods.py injection check
        # (`'ucmv_output' in tool._default_config`) matches this tool.
        default_config: dict = {'ucmv_output': None}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _authenticate(self, host_override: Optional[str] = None):
        """Obtain an AuthContext synchronously. Override in tests for easy mocking.

        Args:
            host_override: If provided, patches the returned AuthContext's workspace_url
                           so the tool targets a specific workspace rather than the one
                           resolved from Kasal Settings / env vars.
        """
        from src.utils.databricks_auth import get_auth_context
        loop = asyncio.new_event_loop()
        try:
            auth = loop.run_until_complete(get_auth_context())
            if auth is not None and host_override:
                # Normalise the override URL and inject it into the auth context
                url = host_override.strip().rstrip('/')
                if not url.startswith('https://'):
                    url = f'https://{url}'
                auth.workspace_url = url
            return auth
        finally:
            loop.close()

    def _run(self, **kwargs: Any) -> str:  # noqa: C901
        def _get(key):
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        space_title = _get('space_title') or 'Genie Space'
        catalog = _get('catalog') or 'main'
        schema = _get('schema_name') or 'default'
        warehouse_id = _get('warehouse_id') or ''
        host_override = _get('databricks_host') or None  # explicit workspace URL override
        ucmv_raw = _get('ucmv_output')
        additional_tables_raw = _get('additional_tables') or ''
        text_instructions = _get('text_instructions') or ''
        join_specs_raw = _get('join_specs_json') or '[]'
        sample_questions_raw = _get('sample_questions') or ''
        sql_expressions_raw = _get('sql_expressions_json') or '[]'
        sql_measures_raw = _get('sql_measures_json') or '[]'
        sql_filters_raw = _get('sql_filters_json') or '[]'
        example_sqls_raw = _get('example_sqls_json') or '[]'

        if not warehouse_id:
            return json.dumps({"error": "warehouse_id is required"})

        # ── 1. Parse metric views from ucmv_output ──────────────────────────────
        metric_view_tables: list[str] = []
        if ucmv_raw:
            try:
                ucmv = json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
                # ucmv_output has a "yaml" key which is a dict of table_key → yaml_str
                yaml_dict = ucmv.get('yaml', {})
                if isinstance(yaml_dict, dict):
                    for table_key in sorted(yaml_dict.keys()):
                        # Sanitise to alphanumeric + underscore
                        import re as _re
                        safe_key = _re.sub(r'[^a-zA-Z0-9_]', '_', table_key.lower())
                        safe_cat = _re.sub(r'[^a-zA-Z0-9_]', '_', catalog)
                        safe_sch = _re.sub(r'[^a-zA-Z0-9_]', '_', schema)
                        fqn = f"{safe_cat}.{safe_sch}.{safe_key}_uc_metric_view"
                        metric_view_tables.append(fqn)
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"[GenieSpace] Could not parse ucmv_output: {e}")

        # ── 2. Parse additional_tables ───────────────────────────────────────────
        additional_tables: list[str] = []
        for line in additional_tables_raw.splitlines():
            stripped = line.strip()
            if stripped and stripped.count('.') >= 2:
                additional_tables.append(stripped)

        all_tables = metric_view_tables + additional_tables

        # ── 3. Parse JSON fields ─────────────────────────────────────────────────
        def _parse_json_list(raw: str, field_name: str) -> list:
            if not raw or raw.strip() in ('', '[]', 'null'):
                return []
            try:
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError as e:
                logger.warning(f"[GenieSpace] Could not parse {field_name}: {e}")
                return []

        join_specs = _parse_json_list(join_specs_raw, 'join_specs_json')
        sql_expressions = _parse_json_list(sql_expressions_raw, 'sql_expressions_json')
        sql_measures = _parse_json_list(sql_measures_raw, 'sql_measures_json')
        sql_filters = _parse_json_list(sql_filters_raw, 'sql_filters_json')
        example_sqls = _parse_json_list(example_sqls_raw, 'example_sqls_json')

        # Parse sample questions (newline-sep or JSON array)
        sample_questions: list[str] = []
        if sample_questions_raw.strip():
            if sample_questions_raw.strip().startswith('['):
                try:
                    sample_questions = json.loads(sample_questions_raw)
                except json.JSONDecodeError:
                    sample_questions = [q.strip() for q in sample_questions_raw.splitlines() if q.strip()]
            else:
                sample_questions = [q.strip() for q in sample_questions_raw.splitlines() if q.strip()]

        # ── 4. Authenticate ──────────────────────────────────────────────────────
        try:
            auth = self._authenticate(host_override=host_override)
            if auth is None:
                return json.dumps({"error": "Authentication failed: no auth context returned"})
            workspace_url = auth.workspace_url  # e.g. "https://my.cloud.databricks.com"
            headers = auth.get_headers()
        except Exception as e:
            return json.dumps({"error": f"Authentication failed: {e}"})

        if not workspace_url:
            return json.dumps({"error": "No Databricks workspace URL configured"})

        # ── 5. SSRF protection ────────────────────────────────────────────────────
        parsed_host = urllib.parse.urlparse(workspace_url)
        if not parsed_host.hostname:
            return json.dumps({"error": "Invalid Databricks workspace URL"})
        if not any(parsed_host.hostname.endswith(s) for s in self._ALLOWED_HOST_SUFFIXES):
            return json.dumps({
                "error": f"Untrusted Databricks host: {parsed_host.hostname}. "
                         "Must be *.cloud.databricks.com or similar."
            })
        actual_host = parsed_host.hostname

        # ── 6. Build Genie space payload ─────────────────────────────────────────
        import uuid

        def _new_id() -> str:
            return uuid.uuid4().hex

        # data_sources
        data_sources: dict = {}
        if all_tables:
            data_sources["tables"] = [{"name": t} for t in all_tables]

        # instructions block
        instructions: list[dict] = []
        if text_instructions.strip():
            instructions.append({
                "id": _new_id(),
                "content": text_instructions.strip(),
            })

        # join specs
        join_spec_entries: list[dict] = []
        for js in join_specs:
            join_spec_entries.append({
                "id": _new_id(),
                "left_table": js.get("left_table", ""),
                "right_table": js.get("right_table", ""),
                "join_condition": js.get("join_condition", ""),
            })

        # SQL snippets
        sql_snippets: list[dict] = []
        for expr in sql_expressions:
            sql_snippets.append({
                "id": _new_id(),
                "type": "EXPRESSION",
                "title": expr.get("display_name", ""),
                "content": expr.get("sql", ""),
            })
        for meas in sql_measures:
            sql_snippets.append({
                "id": _new_id(),
                "type": "MEASURE",
                "title": meas.get("display_name", ""),
                "content": meas.get("sql", ""),
                "description": meas.get("instruction", ""),
            })
        for filt in sql_filters:
            sql_snippets.append({
                "id": _new_id(),
                "type": "FILTER",
                "title": filt.get("display_name", ""),
                "content": filt.get("sql", ""),
            })

        # example question SQLs
        example_question_sqls: list[dict] = []
        for eq in example_sqls:
            example_question_sqls.append({
                "id": _new_id(),
                "question": eq.get("question", ""),
                "sql": eq.get("sql", ""),
            })

        # sample questions list for the space
        sample_question_list = [{"id": _new_id(), "question": q} for q in sample_questions]

        space_payload: dict = {
            "display_name": space_title,
            "warehouse_id": warehouse_id,
        }
        if data_sources:
            space_payload["data_sources"] = data_sources
        if instructions:
            space_payload["instructions"] = instructions
        if join_spec_entries:
            space_payload["join_specs"] = join_spec_entries
        if sql_snippets:
            space_payload["sql_snippets"] = sql_snippets
        if example_question_sqls:
            space_payload["example_question_sqls"] = example_question_sqls
        if sample_question_list:
            space_payload["sample_questions"] = sample_question_list

        # ── 7. Idempotent deploy (GET → find by title → PATCH; else POST) ────────
        try:
            import requests as _requests

            base_url = f"https://{actual_host}/api/2.0/genie/spaces"

            # List existing spaces
            list_resp = _requests.get(base_url, headers=headers, timeout=30)
            existing_space_id: Optional[str] = None
            if list_resp.status_code == 200:
                spaces_data = list_resp.json()
                spaces = spaces_data.get("spaces", spaces_data.get("items", []))
                for sp in spaces:
                    if sp.get("display_name") == space_title or sp.get("title") == space_title:
                        existing_space_id = sp.get("space_id") or sp.get("id")
                        break

            if existing_space_id:
                # PATCH existing space
                patch_url = f"{base_url}/{existing_space_id}"
                resp = _requests.patch(patch_url, json=space_payload, headers=headers, timeout=60)
                operation = "updated"
            else:
                # POST new space
                resp = _requests.post(base_url, json=space_payload, headers=headers, timeout=60)
                operation = "created"

            if resp.status_code not in (200, 201):
                return json.dumps({
                    "error": f"Genie API returned HTTP {resp.status_code}",
                    "detail": resp.text[:500],
                    "payload_sent": space_payload,
                })

            resp_data = resp.json()
            space_id = (
                resp_data.get("space_id")
                or resp_data.get("id")
                or existing_space_id
                or "unknown"
            )
            space_url = f"{workspace_url}/genie/spaces/{space_id}"

            return json.dumps({
                "space_id": space_id,
                "url": space_url,
                "operation": operation,
                "table_count": len(all_tables),
                "metric_view_count": len(metric_view_tables),
                "additional_table_count": len(additional_tables),
                "question_count": len(sample_questions),
                "sql_snippet_count": len(sql_snippets),
                "example_sql_count": len(example_question_sqls),
            }, indent=2)

        except Exception as e:
            return json.dumps({"error": f"Genie Space deploy failed: {e}"})
