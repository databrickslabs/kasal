"""PBI Visual-UCMV Mapper Tool for CrewAI.

Takes Power BI report visuals (from tool 78 output) and deployed UCMV definitions
(flow-injected ucmv_output) and uses an LLM to map each visual to a UC Metric View
metric view with correct measures, dimensions, and generates executable SQL using
MEASURE() syntax.

Output JSON feeds directly into the Databricks Dashboard Creator (tool 95).
"""
import asyncio
import json
import logging
import re
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class PBIVisualUCMVMapperSchema(BaseModel):
    """Input schema for PBIVisualUCMVMapperTool."""
    report_references_override: Optional[str] = Field(
        None,
        description="Manually uploaded tool 78 JSON output — skips live PBI extraction when provided"
    )
    report_references_json: Optional[str] = Field(
        None,
        description="Tool 78 JSON output with reports/pages/visuals (auto-injected by flow)"
    )
    ucmv_output: Optional[str] = Field(
        None,
        description="Deployed UCMV definitions with yaml + deployment_results (auto-injected by flow)"
    )
    measures_json: Optional[str] = Field(
        None,
        description="DAX measures with translations from tool 73 (optional)"
    )
    catalog: Optional[str] = Field(None, description="UC catalog where metric views are deployed")
    schema_name: Optional[str] = Field(None, description="UC schema")
    dashboard_title: Optional[str] = Field(None, description="Title for the resulting dashboard")
    # NOTE: connection / LLM plumbing (databricks_host, llm_model) is
    # deliberately NOT part of this schema. Those values are injected at
    # tool-construction time from tool_configs (see __init__) — exposing them
    # as LLM-fillable parameters bloats every LLM call.


class PBIVisualUCMVMapperTool(BaseTool):
    """Map Power BI report visuals to deployed UC Metric View metric views using LLM."""

    name: str = "PBI Visual-UCMV Mapper"
    description: str = (
        "Maps Power BI report visuals to deployed UC Metric View metric views. "
        "Takes Power BI Report References (tool 78) output and ucmv_output (from flow injection) "
        "and uses an LLM to match each visual's PBI measures to UCMV SQL measures, identify the "
        "correct metric view, determine grouping dimensions, and generate executable Databricks SQL "
        "with MEASURE() syntax. Output feeds directly into the Dashboard Creator (tool 95). "
        "Part of the CI/CD dashboard pipeline: Report References → UCMV Mapper → Dashboard Creator."
    )
    args_schema: Type[BaseModel] = PBIVisualUCMVMapperSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            'report_references_override', 'report_references_json', 'ucmv_output', 'measures_json',
            'catalog', 'schema_name', 'dashboard_title', 'databricks_host', 'llm_model',
        )
        # Seed ucmv_output so flow_methods.py injection check fires
        default_config: dict = {'ucmv_output': None}
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
            url = host_override.strip().rstrip('/')
            if not url.startswith('https://'):
                url = f'https://{url}'
            auth.workspace_url = url
        return auth

    # ──────────────────────────────────────────────────────────────────────────
    # Parsing helpers
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_report_references(raw: str) -> dict:
        """Parse tool 78 JSON output."""
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _parse_ucmv_output(raw: str) -> dict:
        """Parse ucmv_output to extract yaml specs and deployment results."""
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _parse_yaml_spec(yaml_str: str) -> dict:
        """Parse a single YAML spec string."""
        try:
            import yaml as _yaml
            return _yaml.safe_load(yaml_str) or {}
        except Exception:
            return {}

    def _build_ucmv_summaries(self, ucmv_data: dict, catalog: str, schema: str) -> list:
        """Build concise metric view summaries for the LLM prompt."""
        summaries = []
        yaml_specs = ucmv_data.get('yaml', {})
        deployment_results = ucmv_data.get('deployment_results', {})

        for key, yaml_str in yaml_specs.items():
            if not yaml_str or not yaml_str.strip():
                continue
            spec = self._parse_yaml_spec(yaml_str)
            safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', key.lower())

            # Prefer actual deployed view name from deployment_results
            view_name = f"{catalog}.{schema}.{safe_key}"
            dep_result = deployment_results.get(key, {})
            if dep_result.get('view_name'):
                view_name = dep_result['view_name']

            dims = [
                d.get('name', '') for d in (spec.get('dimensions') or [])[:20]
                if d.get('name')
            ]
            meas = [
                m.get('name', '') for m in (spec.get('measures') or [])[:20]
                if m.get('name')
            ]
            comment = (spec.get('comment') or '').split('\n')[0]
            summaries.append({
                'view_name': view_name,
                'key': key,
                'comment': comment,
                'dimensions': dims,
                'measures': meas,
            })
        return summaries

    def _extract_visuals(self, report_data: dict) -> list:
        """Extract all visuals from tool 78 report references output.

        Handles two formats:
        A) Real tool 78 output: {"report": {...}, "visuals": [...flat...], "pages": [...]}
        B) Demo/expected format: {"reports": [{"pages": [{"visuals": [...]}]}]}
        """
        visuals = []

        # ── Format A: real tool 78 output — flat visuals list at top level ──────
        if 'visuals' in report_data and isinstance(report_data['visuals'], list):
            # Build page_name lookup from pages list
            pages = report_data.get('pages') or []
            # pages in real format: [{"page_name": "...", "page_url": "...", "visual_count": N}]
            default_page = pages[0].get('page_name', 'Page 1') if pages else 'Page 1'
            for visual in report_data['visuals']:
                measures = visual.get('measures', [])
                tables = visual.get('tables', [])
                # Skip slicers/filters with no real measures
                if not measures or visual.get('visual_type') == 'slicer':
                    continue
                visuals.append({
                    'visual_id': visual.get('visual_id', ''),
                    'page_name': visual.get('page_name') or default_page,
                    'visual_type': visual.get('visual_type', 'tableEx'),
                    'measures': measures,
                    'tables': tables,
                })
            if visuals:
                logger.info(f"[PBIVisualMapper] Parsed {len(visuals)} visuals (real tool 78 format)")
                return visuals

        # ── Format B: demo/wrapped format — reports > pages > visuals ────────────
        for report in (report_data.get('reports') or []):
            for page in (report.get('pages') or []):
                page_name = page.get('page_display_name') or page.get('page_name') or ''
                for visual in (page.get('visuals') or []):
                    if visual.get('visual_type') == 'slicer':
                        continue
                    visuals.append({
                        'visual_id': visual.get('visual_id', ''),
                        'page_name': page_name,
                        'visual_type': visual.get('visual_type', 'tableEx'),
                        'measures': visual.get('measures', []),
                        'tables': visual.get('tables', []),
                    })

        return visuals

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
                            "You are a data engineering expert who maps Power BI report visuals "
                            "to Databricks UC Metric Views. You match PBI measure names to UCMV "
                            "SQL measure names and generate SQL using the MEASURE() syntax. "
                            "You output only valid JSON arrays."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                model=model,
                temperature=0.1,
                max_tokens=8000,
                extra_headers=get_user_agent_header(KasalProduct.POWERBI),
            )

        # ContextVars (UserContext group/token) must survive the sync→async
        # bridge — LLMManager.completion requires group_id from context.
        return run_async_with_context(_run(), timeout=300)

    def _build_prompt(
        self,
        visuals: list,
        ucmv_summaries: list,
        measures_data: Optional[dict],
        dashboard_title: str
    ) -> str:
        """Build the LLM mapping prompt."""
        # Format UCMV summaries
        ucmv_text = ""
        for s in ucmv_summaries[:15]:
            ucmv_text += f"\n### Metric View: {s['view_name']}\n"
            if s['comment']:
                ucmv_text += f"Description: {s['comment']}\n"
            if s['dimensions']:
                ucmv_text += f"Dimensions: {', '.join(s['dimensions'][:15])}\n"
            if s['measures']:
                ucmv_text += f"Measures: {', '.join(s['measures'][:15])}\n"

        # Format PBI visuals
        visual_text = ""
        for v in visuals[:50]:
            visual_text += (
                f"\n- visual_id: {v['visual_id']}, page: {v['page_name']}, "
                f"type: {v['visual_type']}, "
                f"pbi_measures: {v['measures']}, "
                f"pbi_tables: {v['tables']}\n"
            )

        # Optional DAX → SQL translations for measure name hints
        measure_hints = ""
        if measures_data:
            try:
                measures_list = measures_data if isinstance(measures_data, list) else []
                if isinstance(measures_data, dict):
                    measures_list = measures_data.get('measures', [])
                for m in measures_list[:30]:
                    pbi_name = m.get('measure_name') or m.get('name', '')
                    proposed_table = m.get('proposed_allocation') or m.get('fact_table', '')
                    if pbi_name:
                        measure_hints += f"  - PBI '{pbi_name}' → table '{proposed_table}'\n"
            except Exception:
                pass

        hint_section = ""
        if measure_hints:
            hint_section = f"\n## PBI Measure Allocations (hints for matching):\n{measure_hints}"

        return f"""I need to map Power BI report visuals to Databricks UC Metric View metric views.

## Deployed UC Metric Views:
{ucmv_text}
{hint_section}
## Power BI Visuals to Map:
{visual_text}

## Task:
For each PBI visual, find the best matching UC Metric View and generate a SQL query.

Rules:
1. Match PBI measure names to UCMV measure names using fuzzy/semantic matching
   (e.g. "Revenue" → "total_revenue", "Qty" → "production_quantity")
2. Pick the UCMV metric view whose measures best match the visual's PBI measures
3. For dimensions: pick suitable UCMV dimensions for GROUP BY based on visual type
   - barChart/lineChart: pick 1-2 categorical/temporal dimensions
   - tableEx/matrix: list up to 3 relevant dimensions
   - card/kpiVisual: no GROUP BY needed (aggregate only)
4. Generate SQL: SELECT [dims,] MEASURE(m1), MEASURE(m2) FROM view GROUP BY [dims]
5. For each visual generate a descriptive chart_title
6. If no good match exists, set ucmv_view to null and sql to null

Output a JSON array ONLY, no markdown, no extra text:
[
  {{
    "visual_id": "<from input>",
    "page_name": "<from input>",
    "visual_type": "<from input>",
    "chart_title": "<descriptive title>",
    "ucmv_view": "<catalog.schema.view_name or null>",
    "dimensions": ["dim1", "dim2"],
    "measures": ["measure_name1", "measure_name2"],
    "sql": "<SELECT ... FROM ... GROUP BY ... or null>"
  }}
]

Dashboard title: "{dashboard_title}"
Map ALL {len(visuals)} visuals. Return the complete JSON array.
"""

    def _parse_llm_response(self, response_text: str) -> list:
        """Extract JSON array from LLM response."""
        text = response_text.strip()
        # Strip markdown code blocks
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'\s*```$', '', text, flags=re.MULTILINE)
        text = text.strip()
        try:
            result = json.loads(text)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass
        # Try to extract JSON array
        match = re.search(r'\[[\s\S]+\]', text)
        if match:
            try:
                result = json.loads(match.group())
                if isinstance(result, list):
                    return result
            except Exception:
                pass
        return []

    def _fallback_mapping(self, visuals: list, ucmv_summaries: list) -> list:
        """Simple structural fallback when LLM is unavailable."""
        mappings = []
        # Use the first UCMV view as a blanket fallback
        default_view = ucmv_summaries[0]['view_name'] if ucmv_summaries else None
        default_measures = ucmv_summaries[0]['measures'][:2] if ucmv_summaries else []
        default_dims = ucmv_summaries[0]['dimensions'][:1] if ucmv_summaries else []

        for v in visuals:
            view = default_view
            measures = default_measures
            dims = default_dims
            sql = None
            if view and measures:
                if v['visual_type'] in ('card', 'kpiVisual'):
                    measure_expr = ', '.join(f'MEASURE({m})' for m in measures[:1])
                    sql = f"SELECT {measure_expr} FROM {view}"
                elif dims:
                    dim_expr = ', '.join(dims)
                    measure_expr = ', '.join(f'MEASURE({m})' for m in measures)
                    sql = f"SELECT {dim_expr}, {measure_expr} FROM {view} GROUP BY {dim_expr}"
            mappings.append({
                'visual_id': v['visual_id'],
                'page_name': v['page_name'],
                'visual_type': v['visual_type'],
                'chart_title': f"{v['page_name']} - {v['visual_type']}",
                'ucmv_view': view,
                'dimensions': dims,
                'measures': measures,
                'sql': sql,
            })
        return mappings

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
        dashboard_title = _get('dashboard_title') or 'PBI Dashboard'
        host_override = _get('databricks_host') or None
        llm_model = _get('llm_model') or 'databricks-claude-sonnet-4'

        # ── 1. Parse report_references — override wins over flow injection ──
        # Priority: report_references_override > report_references_json > ucmv_output (tool 78 injected)
        override = _get('report_references_override')
        if override and override.strip() not in ('{}', ''):
            logger.info("[PBIVisualMapper] Using report_references_override (manual upload)")
            report_raw = override
        else:
            report_raw = None

        # ── Falls back to ucmv_output field if tool 78 output was injected directly
        # (tool 78 outputs {"reports": [...]} which the flow injects as ucmv_output
        # when report_references_json key is missing)
        if not report_raw:
            report_raw = _get('report_references_json')
        if not report_raw:
            # Try ucmv_output — flow may have injected tool 78 output there
            candidate = _get('ucmv_output')
            if candidate:
                try:
                    parsed = json.loads(candidate) if isinstance(candidate, str) else candidate
                    if isinstance(parsed, dict) and 'reports' in parsed:
                        report_raw = candidate
                        logger.info("[PBIVisualMapper] Using ucmv_output as report_references_json (tool 78 output detected)")
                except Exception:
                    pass
        if not report_raw:
            return json.dumps({"error": "No report_references_json available — required for visual mapping"})

        report_data = self._parse_report_references(report_raw)
        visuals = self._extract_visuals(report_data)
        if not visuals:
            return json.dumps({"error": "No visuals found in report_references_json"})

        logger.info(f"[PBIVisualMapper] Found {len(visuals)} visuals to map")

        # ── 2. Parse ucmv_output ─────────────────────────────────────────────
        ucmv_raw = _get('ucmv_output')
        if not ucmv_raw:
            # DB fallback (same as the UCMV Validator / Genie config generator):
            # covers standalone runs and flows where multi-hop injection did not
            # deliver the deployer output across the intermediate crews.
            try:
                from src.engines.crewai.tools.custom.metric_view_validator_tool import MetricViewValidatorTool
                latest = MetricViewValidatorTool._fetch_latest_ucmv_from_db()
                if isinstance(latest, dict) and latest.get('yaml'):
                    logger.info("[PBIVisualMapper] ucmv_output not injected — using latest UCMV Generator output from DB")
                    ucmv_raw = json.dumps(latest)
            except Exception as e:
                logger.warning(f"[PBIVisualMapper] DB fallback for ucmv_output failed: {e}")
        if not ucmv_raw:
            return json.dumps({"error": "No ucmv_output available — run the UC Metric View Generator first (flow injection or a prior run in this workspace)"})

        ucmv_data = self._parse_ucmv_output(ucmv_raw)
        ucmv_summaries = self._build_ucmv_summaries(ucmv_data, catalog, schema)
        if not ucmv_summaries:
            return json.dumps({"error": "No deployed metric views found in ucmv_output"})

        logger.info(f"[PBIVisualMapper] Found {len(ucmv_summaries)} deployed metric views")

        # ── 3. Parse optional measures_json ──────────────────────────────────
        measures_data = None
        measures_raw = _get('measures_json')
        if measures_raw:
            try:
                measures_data = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
            except Exception as e:
                logger.warning(f"[PBIVisualMapper] Could not parse measures_json: {e}")

        # ── 4. Authenticate ──────────────────────────────────────────────────
        try:
            auth = self._authenticate(host_override=host_override)
            if not auth:
                logger.warning("[PBIVisualMapper] Authentication failed — using structural fallback")
                visual_mappings = self._fallback_mapping(visuals, ucmv_summaries)
                return self._build_output(visual_mappings, dashboard_title, catalog, schema)
        except Exception as e:
            logger.warning(f"[PBIVisualMapper] Auth error — using structural fallback: {e}")
            visual_mappings = self._fallback_mapping(visuals, ucmv_summaries)
            return self._build_output(visual_mappings, dashboard_title, catalog, schema)

        # ── 5. Call LLM ──────────────────────────────────────────────────────
        try:
            if not llm_model.startswith('databricks/'):
                llm_model = f"databricks/{llm_model}"

            prompt = self._build_prompt(visuals, ucmv_summaries, measures_data, dashboard_title)
            logger.info(f"[PBIVisualMapper] Calling LLM ({llm_model}) for {len(visuals)} visuals")
            llm_response = self._call_llm(prompt, llm_model)
            visual_mappings = self._parse_llm_response(llm_response)
            logger.info(f"[PBIVisualMapper] LLM returned {len(visual_mappings)} mappings")

            if not visual_mappings:
                logger.warning("[PBIVisualMapper] LLM returned no mappings — using structural fallback")
                visual_mappings = self._fallback_mapping(visuals, ucmv_summaries)

        except Exception as e:
            logger.error(f"[PBIVisualMapper] LLM call failed: {e} — using structural fallback")
            visual_mappings = self._fallback_mapping(visuals, ucmv_summaries)

        return self._build_output(visual_mappings, dashboard_title, catalog, schema)

    def _build_output(
        self,
        visual_mappings: list,
        dashboard_title: str,
        catalog: str,
        schema: str
    ) -> str:
        """Assemble the final output JSON."""
        mapped = sum(1 for m in visual_mappings if m.get('ucmv_view'))
        unmapped = len(visual_mappings) - mapped

        output = {
            "visual_mappings": visual_mappings,
            # Also include as JSON string so flow injection matches tool 95's
            # visual_mappings_json key in _default_config (key must match exactly)
            "visual_mappings_json": json.dumps(visual_mappings),
            "dashboard_title": dashboard_title,
            "catalog": catalog,
            "schema_name": schema,
            "summary": {
                "total_visuals": len(visual_mappings),
                "mapped": mapped,
                "unmapped": unmapped,
            }
        }
        logger.info(
            f"[PBIVisualMapper] Complete: {mapped} mapped, {unmapped} unmapped "
            f"out of {len(visual_mappings)} visuals"
        )
        return json.dumps(output, indent=2)
