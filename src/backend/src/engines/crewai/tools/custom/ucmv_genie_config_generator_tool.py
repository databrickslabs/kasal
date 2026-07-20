"""UCMV Genie Space Config Generator Tool for CrewAI.

Reads deployed UC Metric View definitions (from flow-injected ucmv_output)
and uses an LLM to auto-generate a Genie Space configuration:
  - text_instructions   : business-friendly description of the data model
  - sample_questions    : natural-language questions a user might ask
  - example_sqls_json   : SQL examples using MEASURE() syntax
  - join_specs_json     : join relationships derived from UCMV joins
  - additional_tables   : dimension tables referenced in joins

If genie_config_override is provided (manually uploaded JSON) the LLM step
is skipped and the override is returned as-is (merged with connection params).

The output JSON keys match the GenieSpaceGeneratorTool schema fields so they
are automatically injected by the flow engine into the next crew step.
"""
import asyncio
import json
import logging
import re
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class UCMVGenieConfigGeneratorSchema(BaseModel):
    """Input schema."""
    ucmv_output: Optional[str] = Field(
        None,
        description="Full UCMV Generator / Validator output JSON (auto-injected by flow)"
    )
    genie_config_override: Optional[str] = Field(
        None,
        description="Manually uploaded GenieSpaceConfig JSON — skips auto-generation when provided"
    )
    space_title: Optional[str] = Field(None, description="Genie space display name")
    catalog: Optional[str] = Field(None, description="UC catalog where metric views are deployed")
    schema_name: Optional[str] = Field(None, description="UC schema where metric views are deployed")
    warehouse_id: Optional[str] = Field(None, description="SQL warehouse ID for the Genie space")
    databricks_host: Optional[str] = Field(None, description="Override workspace URL (optional)")
    llm_model: Optional[str] = Field(
        None,
        description="LLM model for config generation (defaults to configured Databricks model)"
    )


class UCMVGenieConfigGeneratorTool(BaseTool):
    """Generate Genie Space configuration from deployed UC Metric Views."""

    name: str = "UCMV Genie Space Config Generator"
    description: str = (
        "Generates a complete Genie Space configuration from deployed UC Metric Views. "
        "Reads ucmv_output (auto-injected from the flow) and uses an LLM to produce "
        "text_instructions, sample_questions, example SQL queries with MEASURE() syntax, "
        "and join specs. Output fields match the Genie Space Generator schema so they are "
        "automatically injected into the next flow step. "
        "If genie_config_override is provided, the LLM step is skipped."
    )
    args_schema: Type[BaseModel] = UCMVGenieConfigGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            'ucmv_output', 'genie_config_override',
            'space_title', 'catalog', 'schema_name', 'warehouse_id',
            'databricks_host', 'llm_model',
        )
        default_config: dict = {'ucmv_output': None}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _authenticate(self, host_override: Optional[str] = None):
        """Obtain AuthContext synchronously."""
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
            url = host_override.strip().rstrip('/')
            if not url.startswith('https://'):
                url = f'https://{url}'
            auth.workspace_url = url
        return auth

    # ──────────────────────────────────────────────────────────────────────────
    # UCMV parsing helpers
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_yaml_specs(ucmv_raw: str) -> dict:
        """Return {table_key: yaml_str} from ucmv_output."""
        try:
            data = json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
            return data.get('yaml', {}) if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _parse_yaml_spec(yaml_str: str) -> dict:
        """Parse a single YAML spec string into a structured dict."""
        try:
            import yaml as _yaml
            return _yaml.safe_load(yaml_str) or {}
        except Exception:
            return {}

    def _build_mv_summaries(self, yaml_specs: dict, catalog: str, schema: str) -> list:
        """Build a list of concise metric view summaries for the LLM prompt."""
        summaries = []
        for key, yaml_str in yaml_specs.items():
            if not yaml_str or not yaml_str.strip():
                continue
            spec = self._parse_yaml_spec(yaml_str)
            safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', key.lower())
            view_name = f"{catalog}.{schema}.{safe_key}"
            dims = [
                f"  - {d.get('name','')} ({d.get('comment') or d.get('display_name','')}) = {d.get('expr','')}"
                for d in (spec.get('dimensions') or [])[:15]
            ]
            meas = [
                f"  - {m.get('name','')} ({m.get('comment') or m.get('display_name','')}) = {m.get('expr','')}"
                for m in (spec.get('measures') or [])[:15]
            ]
            comment = spec.get('comment', '').split('\n')[0]  # first line only
            summaries.append({
                'view_name': view_name,
                'key': key,
                'comment': comment,
                'dimensions': dims,
                'measures': meas,
                'joins': spec.get('joins', []),
            })
        return summaries

    def _extract_dimension_tables(self, yaml_specs: dict) -> list:
        """Collect dimension table names from join sources."""
        tables = set()
        for yaml_str in yaml_specs.values():
            if not yaml_str:
                continue
            spec = self._parse_yaml_spec(yaml_str)
            for join in (spec.get('joins') or []):
                src = join.get('source', '')
                if isinstance(src, str) and '.' in src and 'SELECT' not in src.upper():
                    tables.add(src.strip())
        return sorted(tables)

    # ──────────────────────────────────────────────────────────────────────────
    # LLM call
    # ──────────────────────────────────────────────────────────────────────────

    def _call_llm(self, prompt: str, model: str) -> str:
        """Call the LLM and return the response text."""
        from src.core.llm_manager import LLMManager
        from src.engines.crewai.tools.async_bridge import run_async_with_context
        from src.utils.telemetry import get_user_agent_header, KasalProduct

        async def _run():
            return await LLMManager.completion(
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a data analytics expert helping configure a Databricks Genie Space. "
                            "Genie allows business users to ask natural language questions about data. "
                            "Your job is to generate clear, business-friendly configuration based on "
                            "UC Metric View definitions."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                model=model,
                temperature=0.3,
                max_tokens=4000,
                extra_headers=get_user_agent_header(KasalProduct.POWERBI),
            )

        # ContextVars (UserContext group/token) must survive the sync→async
        # bridge — LLMManager.completion requires group_id from context.
        return run_async_with_context(_run(), timeout=300)

    def _build_prompt(self, summaries: list, space_title: str) -> str:
        """Build the LLM prompt from metric view summaries."""
        mv_text = ""
        for s in summaries[:10]:  # limit to avoid token overflow
            mv_text += f"\n### Metric View: {s['view_name']}\n"
            if s['comment']:
                mv_text += f"Description: {s['comment']}\n"
            if s['dimensions']:
                mv_text += "Dimensions:\n" + "\n".join(s['dimensions'][:10]) + "\n"
            if s['measures']:
                mv_text += "Measures:\n" + "\n".join(s['measures'][:10]) + "\n"

        return f"""I have the following UC Metric Views deployed as a Genie Space called "{space_title}":

{mv_text}

Please generate a Genie Space configuration JSON with these exact keys:

{{
  "text_instructions": "<A comprehensive paragraph (200-400 words) describing what data this Genie Space contains, key metrics available, business context, and guidance on how to query it. Mention key dimension and measure names. Explain what the MEASURE() function does in queries.>",

  "sample_questions": "<10-15 natural language questions a business user might ask, one per line. Make them specific to the actual measures and dimensions available. Examples: 'What was the total production output by plant last quarter?' or 'Which workcenter has the highest SLE efficiency?'>",

  "example_sqls_json": "<JSON array of 5-8 example SQL queries using MEASURE() syntax. Each entry: {{\"question\": \"...\", \"sql\": \"SELECT dim1, MEASURE(measure_name) FROM view_name GROUP BY dim1\"}}. Use the actual view names and real measure/dimension names. All measures MUST use MEASURE() function.>"
}}

Rules:
- Use ONLY the metric view names, measure names, and dimension names from the definitions above
- SQL queries must use MEASURE() syntax: SELECT dimension, MEASURE(measure_name) FROM catalog.schema.view GROUP BY dimension
- Make sample questions business-friendly, not technical
- Return ONLY the JSON object, no markdown, no extra text
"""

    def _parse_llm_response(self, response_text: str) -> dict:
        """Extract JSON from LLM response."""
        text = response_text.strip()
        # Strip markdown code blocks if present
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'\s*```$', '', text, flags=re.MULTILINE)
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to extract JSON object
            match = re.search(r'\{[\s\S]+\}', text)
            if match:
                try:
                    return json.loads(match.group())
                except Exception:
                    pass
        return {}

    # ──────────────────────────────────────────────────────────────────────────
    # Main _run
    # ──────────────────────────────────────────────────────────────────────────

    def _run(self, **kwargs: Any) -> str:  # noqa: C901
        def _get(key):
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        catalog = _get('catalog') or 'main'
        schema = _get('schema_name') or 'default'
        warehouse_id = _get('warehouse_id') or ''
        databricks_host = _get('databricks_host') or None
        space_title = _get('space_title') or 'Genie Space'
        llm_model = _get('llm_model') or 'databricks/databricks-claude-sonnet-4-5'
        genie_config_override = _get('genie_config_override')

        # ── 1. Manual override — skip auto-generation ────────────────────────
        if genie_config_override and genie_config_override.strip() not in ('{}', ''):
            try:
                override = json.loads(genie_config_override) if isinstance(genie_config_override, str) else genie_config_override
                # Merge connection params if not already set
                override.setdefault('catalog', catalog)
                override.setdefault('schema_name', schema)
                override.setdefault('warehouse_id', warehouse_id)
                override.setdefault('space_title', space_title)
                if databricks_host:
                    override.setdefault('databricks_host', databricks_host)
                logger.info("[GenieConfigGen] Using manual genie_config_override")
                return json.dumps(override, indent=2)
            except Exception as e:
                logger.warning(f"[GenieConfigGen] Could not parse genie_config_override: {e}")

        # ── 2. Parse ucmv_output ─────────────────────────────────────────────
        ucmv_raw = _get('ucmv_output')
        if not ucmv_raw:
            # DB fallback (same as the UCMV Validator): allows running the
            # Genie-space flow standalone after a previous UCMV Generator run.
            try:
                from src.engines.crewai.tools.custom.metric_view_validator_tool import MetricViewValidatorTool
                latest = MetricViewValidatorTool._fetch_latest_ucmv_from_db()
                if isinstance(latest, dict) and latest.get('yaml'):
                    logger.info("[GenieConfigGen] ucmv_output not injected — using latest UCMV Generator output from DB")
                    ucmv_raw = json.dumps(latest)
            except Exception as e:
                logger.warning(f"[GenieConfigGen] DB fallback for ucmv_output failed: {e}")
        if not ucmv_raw:
            return json.dumps({"error": "No ucmv_output available — run the UC Metric View Generator first (flow injection or a prior run in this workspace)"})

        yaml_specs = self._extract_yaml_specs(ucmv_raw)
        if not yaml_specs:
            return json.dumps({"error": "Could not extract YAML specs from ucmv_output"})

        logger.info(f"[GenieConfigGen] Generating config for {len(yaml_specs)} metric views")

        # ── 3. Build metric view summaries ───────────────────────────────────
        summaries = self._build_mv_summaries(yaml_specs, catalog, schema)
        dim_tables = self._extract_dimension_tables(yaml_specs)

        # ── 4. Authenticate ──────────────────────────────────────────────────
        try:
            auth = self._authenticate(host_override=databricks_host)
            if not auth:
                return json.dumps({"error": "Authentication failed"})
        except Exception as e:
            return json.dumps({"error": f"Authentication error: {e}"})

        # ── 5. Call LLM ──────────────────────────────────────────────────────
        try:
            # Resolve LLM model from workspace config if needed
            if not llm_model.startswith('databricks/'):
                llm_model = f"databricks/{llm_model}"

            prompt = self._build_prompt(summaries, space_title)
            logger.info(f"[GenieConfigGen] Calling LLM ({llm_model}) for config generation")
            llm_response = self._call_llm(prompt, llm_model)
            generated = self._parse_llm_response(llm_response)
            logger.info(f"[GenieConfigGen] LLM generation complete — keys: {list(generated.keys())}")
        except Exception as e:
            logger.error(f"[GenieConfigGen] LLM call failed: {e}")
            # Fall back to structural-only config (no natural language)
            generated = {}

        # ── 6. Assemble output config ─────────────────────────────────────────
        # Build view names list for the Genie space
        view_names = [s['view_name'] for s in summaries]

        # Extract join specs from UCMV YAMLs
        join_specs = []
        for s in summaries:
            for j in s.get('joins', []):
                if isinstance(j.get('source'), str) and '.' in j.get('source', '') and 'SELECT' not in j.get('source', '').upper():
                    join_specs.append({
                        "left_table": j['source'].strip(),
                        "right_table": s['view_name'],
                        "join_condition": j.get('on', '')
                    })

        # Dimension tables from joins — exclude any that look like sanitised UCMV table names
        # (those are already embedded in the metric view definition, not extra tables)
        real_dim_tables = [
            t for t in dim_tables
            if not any(skip in t for skip in ['dc_datalake_prod_001__', 'udm_cchbc', 'udm_datamart'])
        ]

        output = {
            # Connection params
            "space_title": space_title,
            "catalog": catalog,
            "schema_name": schema,
            "warehouse_id": warehouse_id,

            # LLM-generated content (with fallbacks)
            "text_instructions": generated.get("text_instructions", ""),
            "sample_questions": generated.get("sample_questions", ""),
            "example_sqls_json": generated.get("example_sqls_json", "[]"),

            # Structural content derived from UCMVs
            "join_specs_json": json.dumps(join_specs) if join_specs else "[]",
            "additional_tables": "\n".join(real_dim_tables) if real_dim_tables else "",

            # Pass ucmv_output through so the Genie Space Generator knows
            # which metric views to add to the space
            "ucmv_output": ucmv_raw,
        }

        if databricks_host:
            output["databricks_host"] = databricks_host

        logger.info(
            f"[GenieConfigGen] Config generated for {len(summaries)} views, "
            f"{len(join_specs)} joins, {len(dim_tables)} dimension tables"
        )
        return json.dumps(output, indent=2)
