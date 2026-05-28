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

        Runs the async get_auth_context() in a dedicated thread so this method is
        safe to call from within a running asyncio event loop (which CrewAI always
        provides). Using asyncio.new_event_loop().run_until_complete() in the *same*
        thread raises "Cannot run the event loop while another loop is running".

        Args:
            host_override: If provided, patches the returned AuthContext's workspace_url
                           so the tool targets a specific workspace rather than the one
                           resolved from Kasal Settings / env vars.
        """
        import concurrent.futures
        from src.utils.databricks_auth import get_auth_context

        def _run_in_thread():
            """Run the coroutine in a fresh thread that owns its own event loop."""
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
            url = host_override.strip().rstrip('/')
            if not url.startswith('https://'):
                url = f'https://{url}'
            auth.workspace_url = url
        return auth

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
                import re as _re

                # Prefer actual deployed view names from deployment_results
                # (these have the exact names used when deploying, no suffix guessing needed)
                deployment_results = ucmv.get('deployment_results', {})
                if isinstance(deployment_results, dict):
                    for result in deployment_results.values():
                        view_name = result.get('view_name', '')
                        if view_name and view_name.count('.') == 2:
                            metric_view_tables.append(view_name)
                    if metric_view_tables:
                        logger.info(f"[GenieSpace] Using {len(metric_view_tables)} view names from deployment_results")

                # Fallback: derive from yaml dict keys (legacy path)
                if not metric_view_tables:
                    yaml_dict = ucmv.get('yaml', {})
                    if isinstance(yaml_dict, dict):
                        for table_key in sorted(yaml_dict.keys()):
                            safe_key = _re.sub(r'[^a-zA-Z0-9_]', '_', table_key.lower())
                            safe_cat = _re.sub(r'[^a-zA-Z0-9_]', '_', catalog)
                            safe_sch = _re.sub(r'[^a-zA-Z0-9_]', '_', schema)
                            # Don't append _uc_metric_view — Deployer now omits this suffix
                            fqn = f"{safe_cat}.{safe_sch}.{safe_key}"
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

        # ── 6. Build serialized_space (version 2 — required by Genie REST API) ────
        import uuid

        def _new_id() -> str:
            return uuid.uuid4().hex

        # ── data_sources ──────────────────────────────────────────────────────────
        # metric views and plain tables are separate sections in the API.
        # metric_view_tables come from ucmv_output; additional_tables are dimensions.
        mv_data = [{"identifier": t} for t in metric_view_tables]
        tbl_data = [{"identifier": t} for t in additional_tables]

        # Fallback: if there was no ucmv_output, treat all tables as plain tables.
        if not mv_data and not tbl_data:
            tbl_data = [{"identifier": t} for t in all_tables]

        data_sources: dict = {}
        if tbl_data:
            data_sources["tables"] = tbl_data
        if mv_data:
            data_sources["metric_views"] = mv_data

        # ── sample questions (config section) — IDs sorted alphabetically ─────────
        sample_question_list = sorted(
            [{"id": _new_id(), "question": [q]} for q in sample_questions],
            key=lambda x: x["id"],
        )

        # ── text instructions — max one block, ID sorted ───────────────────────────
        text_instruction_list = []
        if text_instructions.strip():
            text_instruction_list = sorted(
                [{"id": _new_id(), "content": [text_instructions.strip()]}],
                key=lambda x: x["id"],
            )

        # ── join specs — serialized_space format (left/right objects, sql list) ────
        join_spec_entries = []
        for js in join_specs:
            left = js.get("left_table", "")
            right = js.get("right_table", "")
            cond = js.get("join_condition", "")
            left_alias = left.split(".")[-1] if left else ""
            right_alias = right.split(".")[-1] if right else ""
            join_spec_entries.append({
                "id": _new_id(),
                "left":  {"identifier": left,  "alias": left_alias},
                "right": {"identifier": right, "alias": right_alias},
                "sql": [cond, "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
            })
        join_spec_entries = sorted(join_spec_entries, key=lambda x: x["id"])

        # ── SQL snippets — each list item has sql as a list, IDs sorted ───────────
        expressions_list = sorted(
            [{"id": _new_id(), "display_name": e.get("display_name", ""),
              "sql": [e.get("sql", "")]}
             for e in sql_expressions],
            key=lambda x: x["id"],
        )
        measures_list = sorted(
            [{"id": _new_id(), "display_name": m.get("display_name", ""),
              "sql": [m.get("sql", "")],
              "instruction": [m.get("instruction", "")]}
             for m in sql_measures],
            key=lambda x: x["id"],
        )
        filters_list = sorted(
            [{"id": _new_id(), "display_name": f.get("display_name", ""),
              "sql": [f.get("sql", "")]}
             for f in sql_filters],
            key=lambda x: x["id"],
        )

        # ── example question SQLs ─────────────────────────────────────────────────
        example_question_sqls_list = sorted(
            [{"id": _new_id(),
              "question": [eq.get("question", "")],
              "sql":      [eq.get("sql", "")]}
             for eq in example_sqls],
            key=lambda x: x["id"],
        )

        # ── assemble serialized_space dict and JSON-encode it ─────────────────────
        serialized_space_dict: dict = {
            "version": 2,
            "config": {
                "sample_questions": sample_question_list,
            },
            "data_sources": data_sources,
            "instructions": {
                "text_instructions": text_instruction_list,
                "join_specs": join_spec_entries,
                "sql_snippets": {
                    "expressions": expressions_list,
                    "measures":    measures_list,
                    "filters":     filters_list,
                },
                "example_question_sqls": example_question_sqls_list,
            },
        }
        serialized_space_str = json.dumps(serialized_space_dict)

        # ── outer request payload (title + warehouse_id + serialized_space) ───────
        space_payload: dict = {
            "title":            space_title,
            "warehouse_id":     warehouse_id,
            "description":      f"Genie Space for {space_title} — deployed by Kasal",
            "serialized_space": serialized_space_str,
        }

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
                    if sp.get("title") == space_title or sp.get("display_name") == space_title:
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
            space_url = f"{workspace_url}/genie/rooms/{space_id}"

            return json.dumps({
                "space_id": space_id,
                "url": space_url,
                "operation": operation,
                "table_count": len(all_tables),
                "metric_view_count": len(metric_view_tables),
                "additional_table_count": len(additional_tables),
                "question_count": len(sample_questions),
                "sql_snippet_count": len(expressions_list) + len(measures_list) + len(filters_list),
                "example_sql_count": len(example_question_sqls_list),
            }, indent=2)

        except Exception as e:
            return json.dumps({"error": f"Genie Space deploy failed: {e}"})
