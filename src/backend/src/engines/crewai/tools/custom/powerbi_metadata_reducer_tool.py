"""
Power BI Metadata Reducer Tool for CrewAI

Intelligently reduces semantic model metadata to only what's relevant for a
specific user question. Sits between the Fetcher (Tool 79) and DAX Generator
(Tool 80) in multi-step workflows:

    Fetcher → Metadata Reducer → DAX Generator

Pipeline steps:
1. Parse full model context from JSON
2. Fuzzy pre-screening (score all tables/measures against question)
3. LLM table + measure selection (with fuzzy hints)
4. Measure dependency resolution (auto-expand DAX dependencies)
5. Filter reduction (keep only relevant relationships/sample_data/slicers)
6. Value normalization (validate filter values against column metadata)
7. Build reduced output JSON

Author: Kasal Team
Date: 2026
"""

import asyncio
import contextvars
import logging
import json
import re
import time
from typing import Any, Optional, Type, Dict, List
from concurrent.futures import ThreadPoolExecutor

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.services.powerbi_semantic_model_cache_service import PowerBISemanticModelCacheService
from src.db.session import async_session_factory

from .metadata_reduction.fuzzy_scorer import FuzzyScorer
from .metadata_reduction.dependency_resolver import MeasureDependencyResolver
from .metadata_reduction.value_normalizer import ValueNormalizer
from .metadata_reduction.question_preprocessor import QuestionPreprocessor
from .metadata_reduction.measure_resolver import MeasureResolver
from .metadata_reduction.dax_skeleton_builder import DaxSkeletonBuilder
from .metadata_reduction.dimension_resolver import DimensionResolver

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_EXECUTOR = ThreadPoolExecutor(max_workers=3)


def _run_async_in_sync_context(coro):
    """
    Safely run an async coroutine from a synchronous context.
    Handles nested event loop scenarios (e.g., FastAPI).
    Propagates contextvars (like execution_id) to worker threads.
    """
    try:
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        future = _EXECUTOR.submit(ctx.run, asyncio.run, coro)
        return future.result()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


# ─── Input Schema ───────────────────────────────────────────────────────────

class PowerBIMetadataReducerSchema(BaseModel):
    """Input schema for PowerBIMetadataReducerTool.

    All parameters are pre-configured via tool_configs. The agent does NOT
    need to pass any arguments — just call the tool directly.
    """

    # The only optional override — agent can pass this but doesn't need to
    model_context_json: Optional[str] = Field(
        None,
        description=(
            "DO NOT pass this parameter. The tool automatically loads model context "
            "from the database cache. Only use this if explicitly instructed to override."
        ),
    )


# ─── Tool Class ─────────────────────────────────────────────────────────────

class PowerBIMetadataReducerTool(BaseTool):
    """
    Intelligently reduces Power BI semantic model metadata to only
    question-relevant elements using fuzzy matching, LLM selection,
    and measure dependency resolution.

    **Input**: Full model context JSON from the Fetcher tool + user question.
    **Output**: Reduced JSON with same schema, ready for the DAX Generator.
    """

    name: str = "Power BI Metadata Reducer"
    description: str = (
        "Reduces Power BI semantic model metadata to only question-relevant elements. "
        "IMPORTANT: Call this tool with NO arguments. All configuration (dataset_id, "
        "workspace_id, user_question, strategy) is pre-loaded from tool settings. "
        "The tool automatically loads the model context from the database cache. "
        "Do NOT pass model_context_json or any other parameter."
    )
    args_schema: Type[BaseModel] = PowerBIMetadataReducerSchema

    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        import uuid
        instance_id = str(uuid.uuid4())[:8]
        logger.info(f"[MetadataReducer.__init__] Instance ID: {instance_id}")

        default_config = {
            "user_question": kwargs.get("user_question", ""),
            "strategy": kwargs.get("strategy", "combined"),
            "synonym_threshold": kwargs.get("synonym_threshold", 70),
            "synonym_boost_min": kwargs.get("synonym_boost_min", 60.0),
            "max_tables": kwargs.get("max_tables", 15),
            "max_measures": kwargs.get("max_measures", 30),
            "enable_value_normalization": kwargs.get("enable_value_normalization", True),
            "dataset_id": kwargs.get("dataset_id"),
            "workspace_id": kwargs.get("workspace_id"),
            "group_id": kwargs.get("group_id", "default"),
            "report_id": kwargs.get("report_id"),
            "llm_workspace_url": kwargs.get("llm_workspace_url"),
            "llm_token": kwargs.get("llm_token"),
            "llm_model": kwargs.get("llm_model", "databricks-claude-sonnet-4"),
            "business_mappings": kwargs.get("business_mappings", {}),
            "field_synonyms": kwargs.get("field_synonyms", {}),
            "active_filters": kwargs.get("active_filters", {}),
            "business_terms": kwargs.get("business_terms", {}),
            "enrichment_data": kwargs.get("enrichment_data", {}),
            "reference_dax": kwargs.get("reference_dax", ""),
        }

        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        self._instance_id = instance_id
        self._default_config = default_config

    # ─── Entry Point ────────────────────────────────────────────────────

    def _run(self, **kwargs: Any) -> str:
        """Execute the metadata reduction pipeline."""
        try:
            logger.info(f"[MetadataReducer] ═══ _run() START (instance={self._instance_id}) ═══")
            logger.info(
                f"[MetadataReducer] Config: strategy={self._default_config.get('strategy')}, "
                f"question='{(self._default_config.get('user_question') or '')[:80]}...', "
                f"dataset={self._default_config.get('dataset_id')}, "
                f"workspace={self._default_config.get('workspace_id')}"
            )
            if kwargs:
                logger.info(f"[MetadataReducer] Agent kwargs: {list(kwargs.keys())}")

            # Merge configs: kwargs override defaults
            config = dict(self._default_config)
            for k, v in kwargs.items():
                if v is not None:
                    config[k] = v

            t0 = time.time()
            result = _run_async_in_sync_context(self._execute_reducer_pipeline(config))
            elapsed = time.time() - t0
            logger.info(f"[MetadataReducer] ═══ _run() DONE in {elapsed:.2f}s (output={len(result)} chars) ═══")
            return result

        except Exception as e:
            logger.error(f"[MetadataReducer] Pipeline error: {e}", exc_info=True)
            return json.dumps({"error": str(e)})

    # ─── Main Pipeline ──────────────────────────────────────────────────

    async def _execute_reducer_pipeline(self, config: Dict[str, Any]) -> str:
        """
        Full reduction pipeline:
        1. Parse model context
        2. Fuzzy pre-screening
        3. LLM or fuzzy-only table/measure selection
        4. Measure dependency resolution
        5. Filter reduction
        6. Value normalization
        7. Build output
        """
        pipeline_start = time.time()

        # Step 1: Parse model context (try JSON first, then cache fallback)
        t1 = time.time()
        model_context = await self._parse_model_context(config)
        logger.info(f"[MetadataReducer] Step 1 (parse/cache) done in {time.time()-t1:.2f}s")
        if "error" in model_context:
            return json.dumps(model_context)

        user_question = config.get("user_question", "")
        if not user_question:
            return json.dumps({"error": "user_question is required"})

        strategy = config.get("strategy", "intelligent")

        # Passthrough mode — return input unchanged
        if strategy == "passthrough":
            model_context["reduction_summary"] = {
                "strategy": "passthrough",
                "original_tables": len(model_context.get("tables", [])),
                "kept_tables": len(model_context.get("tables", [])),
                "original_measures": len(model_context.get("measures", [])),
                "kept_measures": len(model_context.get("measures", [])),
                "reduction_pct": 0.0,
                "reasoning": "Passthrough mode — no reduction applied.",
            }
            logger.info(f"[MetadataReducer] Passthrough — returning unchanged in {time.time()-pipeline_start:.2f}s")
            return json.dumps(model_context)

        tables = model_context.get("tables", [])
        measures = model_context.get("measures", [])
        relationships = model_context.get("relationships", [])
        sample_data = model_context.get("sample_data", {})
        slicers = model_context.get("slicers", [])
        columns = model_context.get("columns", [])

        # Step 1b: Merge enrichment data into model context (optional input)
        enrichment_data_raw = config.get("enrichment_data", {})
        if isinstance(enrichment_data_raw, str):
            try:
                enrichment_data = json.loads(enrichment_data_raw) if enrichment_data_raw else {}
            except (json.JSONDecodeError, TypeError):
                enrichment_data = {}
        else:
            enrichment_data = enrichment_data_raw or {}

        if enrichment_data:
            enriched_counts = self._merge_enrichment_data(tables, measures, columns, enrichment_data)
            logger.info(f"[MetadataReducer][ENRICHMENT] Merged enrichment data — {enriched_counts}")

        # Step 1b-dim: Resolve dimension keywords → explicit table-qualified column bindings
        # This runs before fuzzy scoring so the skeleton builder has concrete bindings.
        dimension_bindings: List = []

        # Step 1c: Parse reference DAX for force-include table/measure names
        reference_dax = config.get("reference_dax", "")
        if isinstance(reference_dax, str) and reference_dax.strip():
            ref_tables, ref_measures = self._parse_reference_dax(reference_dax, tables, measures)
            if ref_tables or ref_measures:
                logger.info(
                    f"[MetadataReducer][REFERENCE_DAX] Parsed → "
                    f"force-include tables={ref_tables}, measures={ref_measures}"
                )
        else:
            ref_tables, ref_measures = set(), set()

        original_table_count = len(tables)
        original_measure_count = len(measures)

        logger.info(
            f"[MetadataReducer] ── Pipeline: {original_table_count} tables, "
            f"{original_measure_count} measures, strategy={strategy}, "
            f"question='{user_question[:100]}'"
        )

        # Step 2 + 3: Selection based on strategy
        t2 = time.time()
        scorer = FuzzyScorer(
            synonym_threshold=config.get("synonym_threshold", 70),
            boost_min=config.get("synonym_boost_min", 60.0),
        )
        threshold = config.get("synonym_threshold", 70)
        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")

        # Parse business_terms: {"BU": ["Business Unit"], "CGR": ["Complete Good Receipt"]}
        business_terms_raw = config.get("business_terms", {})
        if isinstance(business_terms_raw, str):
            try:
                business_terms = json.loads(business_terms_raw)
            except (json.JSONDecodeError, TypeError):
                business_terms = {}
        else:
            business_terms = business_terms_raw or {}
        if business_terms:
            logger.info(f"[MetadataReducer] Business terms loaded: {list(business_terms.keys())}")

        # Step 2a: Question preprocessing — extract structured intent
        t2a = time.time()
        preprocessor = QuestionPreprocessor()
        known_measure_names = [m.get("name", "") for m in measures if m.get("name")]
        known_dimension_names = []
        for t in tables:
            for col in t.get("columns", []):
                cname = col.get("name", "") if isinstance(col, dict) else str(col)
                if cname:
                    known_dimension_names.append(cname)

        question_intent = preprocessor.preprocess(
            user_question,
            known_measures=known_measure_names,
            known_dimensions=known_dimension_names,
            llm_workspace_url=config.get("llm_workspace_url"),
            llm_token=config.get("llm_token"),
            llm_model=config.get("llm_model", "databricks-claude-sonnet-4"),
        )

        # Detect question split
        question_intent = preprocessor.detect_split(question_intent, known_measures=known_measure_names)
        logger.info(f"[MetadataReducer][PREPROCESSOR] Done in {time.time()-t2a:.3f}s")

        # Step 1b-dim (continued): now that question_intent is available, resolve dimensions
        dim_keywords = [
            kw for kw in (getattr(question_intent, "dimensions", []) or [])
            if kw and kw.lower() not in {m.lower() for m in known_measure_names}
        ]
        if dim_keywords:
            field_synonyms_raw = config.get("field_synonyms", {})
            dim_resolver = DimensionResolver()
            dimension_bindings = [
                b.to_dict()
                for b in dim_resolver.resolve(
                    keywords=dim_keywords,
                    selected_tables=tables,
                    sample_data=sample_data,
                    field_synonyms=field_synonyms_raw if isinstance(field_synonyms_raw, dict) else {},
                    threshold=0.70,
                )
            ]
            if dimension_bindings:
                logger.info(
                    f"[MetadataReducer][DIMENSION_RESOLVER] Resolved {len(dimension_bindings)} bindings: "
                    + ", ".join(
                        f"'{b['user_term']}'→'{b['resolved_table']}'['{b['resolved_column']}']"
                        for b in dimension_bindings
                    )
                )

        if strategy == "fuzzy":
            # ── Fuzzy-only: deterministic scoring, no LLM ──
            logger.info(f"[MetadataReducer][FUZZY_SCORING] Starting (threshold={threshold})...")
            ranked_tables = scorer.rank_tables(tables, user_question, sample_data=sample_data, business_terms=business_terms, question_intent=question_intent)
            ranked_measures = scorer.rank_measures(measures, user_question, business_terms=business_terms, question_intent=question_intent)

            # Log top fuzzy scores for visibility
            top_tables = [(r["table"]["name"], r["score"]) for r in ranked_tables[:8]]
            logger.info(f"[MetadataReducer][FUZZY_SCORING] Top table scores: {top_tables}")
            top_measures_scores = [(r["measure"]["name"], r["score"]) for r in ranked_measures[:8]]
            logger.info(f"[MetadataReducer][FUZZY_SCORING] Top measure scores: {top_measures_scores}")

            selected_table_names = [
                r["table"]["name"] for r in ranked_tables if r["score"] >= threshold
            ]
            selected_measure_names = [
                r["measure"]["name"] for r in ranked_measures if r["score"] >= threshold
            ]
            logger.info(
                f"[MetadataReducer][FUZZY_SCORING] Done in {time.time()-t2:.2f}s — "
                f"selected {len(selected_table_names)} tables, {len(selected_measure_names)} measures"
            )
            selection_reasoning = f"Fuzzy-only selection (threshold={threshold})"

        elif strategy == "llm":
            # ── LLM-only: send full catalog to LLM without fuzzy hints ──
            if not llm_workspace_url or not llm_token:
                return json.dumps({
                    "error": "LLM strategy requires llm_workspace_url and llm_token to be configured."
                })
            logger.info(f"[MetadataReducer][LLM_SELECTION] LLM-only selection (model={config.get('llm_model')})...")
            ranked_tables = scorer.rank_tables(tables, user_question, sample_data=sample_data, business_terms=business_terms, question_intent=question_intent)
            ranked_measures = scorer.rank_measures(measures, user_question, business_terms=business_terms, question_intent=question_intent)
            for entry in ranked_tables:
                entry["likely_relevant"] = False
            selected = await self._llm_select_tables_and_measures(
                user_question, ranked_tables, ranked_measures, config
            )
            selected_table_names = selected.get("tables", [])
            selected_measure_names = selected.get("measures", [])
            selection_reasoning = selected.get("reasoning", "LLM-only selection")
            logger.info(
                f"[MetadataReducer][LLM_SELECTION] Done in {time.time()-t2:.2f}s — "
                f"selected {len(selected_table_names)} tables: {selected_table_names}, "
                f"{len(selected_measure_names)} measures: {selected_measure_names}"
            )

        elif strategy == "combined":
            # ── Combined: fuzzy pre-screening + LLM with hints ──
            logger.info(f"[MetadataReducer][FUZZY_SCORING] Pre-screening for combined strategy...")
            ranked_tables = scorer.rank_tables(tables, user_question, sample_data=sample_data, business_terms=business_terms, question_intent=question_intent)
            ranked_measures = scorer.rank_measures(measures, user_question, business_terms=business_terms, question_intent=question_intent)
            fuzzy_time = time.time() - t2

            top_tables = [(r["table"]["name"], r["score"], r["likely_relevant"]) for r in ranked_tables[:8]]
            logger.info(f"[MetadataReducer][FUZZY_SCORING] Done in {fuzzy_time:.2f}s. Top tables: {top_tables}")

            if llm_workspace_url and llm_token:
                t3 = time.time()
                logger.info(f"[MetadataReducer][LLM_SELECTION] Starting (model={config.get('llm_model')})...")
                selected = await self._llm_select_tables_and_measures(
                    user_question, ranked_tables, ranked_measures, config
                )
                selected_table_names = selected.get("tables", [])
                selected_measure_names = selected.get("measures", [])
                selection_reasoning = selected.get("reasoning", "Combined fuzzy + LLM selection")
                logger.info(
                    f"[MetadataReducer][LLM_SELECTION] Done in {time.time()-t3:.2f}s — "
                    f"selected {len(selected_table_names)} tables: {selected_table_names}, "
                    f"{len(selected_measure_names)} measures: {selected_measure_names}"
                )
            else:
                logger.warning(
                    "[MetadataReducer] Combined strategy but LLM not configured — falling back to fuzzy-only"
                )
                selected_table_names = [
                    r["table"]["name"] for r in ranked_tables if r["score"] >= threshold
                ]
                selected_measure_names = [
                    r["measure"]["name"] for r in ranked_measures if r["score"] >= threshold
                ]
                selection_reasoning = f"Combined strategy fallback to fuzzy-only (LLM not configured, threshold={threshold})"
        else:
            return json.dumps({
                "error": f"Unknown strategy '{strategy}'. Use: fuzzy, llm, combined, or passthrough."
            })

        # Normalize measure names: LLM returns "[table] name" format but
        # the dependency resolver indexes by bare "name". Strip the prefix.
        selected_measure_names = [
            re.sub(r"^\[.*?\]\s*", "", name) for name in selected_measure_names
        ]

        # Force-include tables/measures from reference DAX
        if ref_tables:
            for rt in ref_tables:
                if rt not in selected_table_names:
                    selected_table_names.append(rt)
            logger.info(f"[MetadataReducer][REFERENCE_DAX] Force-included {len(ref_tables)} tables")
        if ref_measures:
            for rm in ref_measures:
                if rm not in selected_measure_names:
                    selected_measure_names.append(rm)
            logger.info(f"[MetadataReducer][REFERENCE_DAX] Force-included {len(ref_measures)} measures")

        # Dimension column anchor: if question_intent names a dimension (e.g. "Business Unit")
        # and a table has a column with that exact name, force-include it regardless of
        # fuzzy/LLM scoring. This is a hard guarantee that the GROUP BY table is always present.
        if question_intent and question_intent.dimensions:
            _dim_names_lower = {d.lower() for d in question_intent.dimensions}
            for _table in tables:
                _tname = _table["name"]
                if _tname in selected_table_names:
                    continue
                _table_col_names_lower = {
                    (c.get("name", "") if isinstance(c, dict) else str(c)).lower()
                    for c in _table.get("columns", [])
                }
                if _table_col_names_lower & _dim_names_lower:
                    selected_table_names.append(_tname)
                    logger.info(
                        f"[MetadataReducer][DIM_ANCHOR] Force-included '{_tname}' "
                        f"(column matches question dimension: {_table_col_names_lower & _dim_names_lower})"
                    )

        # Slicer anchor: if a slicer's column matches a question intent dimension or measure,
        # its table must be in context — slicers represent actively used filter axes in the report.
        if question_intent:
            _slicer_anchor_names = {d.lower() for d in (question_intent.dimensions or [])}
            _slicer_anchor_names |= {m.lower() for m in (question_intent.measures or [])}
            if _slicer_anchor_names:
                for _slicer in slicers:
                    _s_table = _slicer.get("table", "")
                    _s_column = _slicer.get("column", "").lower()
                    if _s_table and _s_column in _slicer_anchor_names and _s_table not in selected_table_names:
                        if any(t["name"] == _s_table for t in tables):
                            selected_table_names.append(_s_table)
                            logger.info(
                                f"[MetadataReducer][SLICER_ANCHOR] Force-included '{_s_table}' "
                                f"(slicer column '{_s_column}' matches question intent)"
                            )

        # Enforce max limits
        max_tables = config.get("max_tables", 15)
        max_measures = config.get("max_measures", 30)
        selected_table_names = selected_table_names[:max_tables]
        selected_measure_names = selected_measure_names[:max_measures]

        # Ensure at least 1 table if measures were selected
        if selected_measure_names and not selected_table_names:
            for m in measures:
                if m.get("name") in selected_measure_names:
                    t = m.get("table", "")
                    if t and t not in selected_table_names:
                        selected_table_names.append(t)

        # Step 4: Measure dependency resolution
        t4 = time.time()
        logger.info(f"[MetadataReducer][DEPENDENCY_RESOLUTION] Starting...")
        resolver = MeasureDependencyResolver(measures, tables)
        resolved_measures = resolver.resolve(selected_measure_names)
        resolved_measure_names = [m["name"] for m in resolved_measures]

        dep_tables = resolver.get_tables_for_measures(resolved_measure_names)
        added_dep_tables = [t for t in dep_tables if t not in selected_table_names]
        for t in added_dep_tables:
            selected_table_names.append(t)

        logger.info(
            f"[MetadataReducer][DEPENDENCY_RESOLUTION] Done in {time.time()-t4:.3f}s — "
            f"{len(resolved_measure_names)} measures (added {len(resolved_measure_names) - len(selected_measure_names)} deps), "
            f"added {len(added_dep_tables)} dep tables: {added_dep_tables}"
        )

        # Step 4.5: Measure resolution — classify as MODEL/FILTERED/COMPOSITE + expression analysis
        t45 = time.time()
        measure_resolver = MeasureResolver(measures, tables, sample_data=sample_data)
        resolved_measures = measure_resolver.resolve(resolved_measures)
        logger.info(f"[MetadataReducer][MEASURE_RESOLVER] Done in {time.time()-t45:.3f}s")

        # Step 4.6: Auto-include filter_table from FILTERED_MEASURE resolutions.
        # When a measure resolves as FILTERED_MEASURE (e.g. [Score] WHERE Quality Category = "Conformity"),
        # its filter_table (e.g. dim_Rules Inventory) must be in context for the CALCULATE to work.
        _all_table_names_set = {t["name"] for t in tables}
        for _m in resolved_measures:
            _filter_table = _m.get("_resolution", {}).get("filter_table", "")
            if _filter_table and _filter_table not in selected_table_names and _filter_table in _all_table_names_set:
                selected_table_names.append(_filter_table)
                logger.info(
                    f"[MetadataReducer][FILTERED_MEASURE_ANCHOR] Auto-included '{_filter_table}' "
                    f"as filter_table for measure '{_m.get('name', '')}'"
                )

        # Step 4b: MANDATORY — include all default_filter tables in reduced output
        # Default filters are report-level filters that MUST ALWAYS be applied.
        # They are excluded from fuzzy/LLM selection — they are unconditionally
        # included regardless of relevance scoring.
        # Strategy:
        #   1. If filter table exists in full model → ALWAYS include that table
        #   2. If filter table is phantom → remap column to an actual table
        #   3. If column not found anywhere → drop the filter
        default_filters = model_context.get("default_filters", {})
        if default_filters:
            all_table_names = {t["name"] for t in tables}

            # Build column→table lookup with normalized names for fuzzy matching
            # Normalization: lowercase, replace spaces/hyphens with underscores
            col_to_tables: Dict[str, List[tuple]] = {}  # normalized_col → [(original_col, table_name)]
            for t in tables:
                for col in t.get("columns", []):
                    normalized = col.lower().replace(" ", "_").replace("-", "_")
                    col_to_tables.setdefault(normalized, []).append((col, t["name"]))

            remapped_filters: Dict[str, Any] = {}
            filter_tables_added = []
            remapped_keys = []
            dropped_keys = []

            for filter_key, filter_value in default_filters.items():
                filter_table = filter_key.split("[")[0] if "[" in filter_key else ""
                filter_col = filter_key.split("[")[1].rstrip("]") if "[" in filter_key else ""

                if filter_table in all_table_names:
                    # Case 1: Table exists in the full model — include it
                    if filter_table not in selected_table_names:
                        selected_table_names.append(filter_table)
                        filter_tables_added.append(filter_table)
                    remapped_filters[filter_key] = filter_value
                elif filter_col:
                    # Case 2: Phantom table — try to remap column to an actual table
                    normalized_col = filter_col.lower().replace(" ", "_").replace("-", "_")
                    candidates = col_to_tables.get(normalized_col, [])

                    # Prefer tables already in the selected set
                    kept_candidates = [(c, t) for c, t in candidates if t in selected_table_names]
                    if kept_candidates:
                        actual_col, actual_table = kept_candidates[0]
                        new_key = f"{actual_table}[{actual_col}]"
                        remapped_filters[new_key] = filter_value
                        remapped_keys.append(f"{filter_key} → {new_key}")
                    elif candidates:
                        actual_col, actual_table = candidates[0]
                        if actual_table not in selected_table_names:
                            selected_table_names.append(actual_table)
                            filter_tables_added.append(actual_table)
                        new_key = f"{actual_table}[{actual_col}]"
                        remapped_filters[new_key] = filter_value
                        remapped_keys.append(f"{filter_key} → {new_key}")
                    else:
                        # Case 3: Column not found in any table — drop
                        dropped_keys.append(filter_key)
                else:
                    remapped_filters[filter_key] = filter_value

            model_context["default_filters"] = remapped_filters

            if filter_tables_added:
                logger.info(
                    f"[MetadataReducer][DEFAULT_FILTERS] Added {len(filter_tables_added)} tables "
                    f"for default_filters: {filter_tables_added}"
                )
            if remapped_keys:
                logger.info(
                    f"[MetadataReducer][DEFAULT_FILTERS] Remapped {len(remapped_keys)} filters: "
                    f"{remapped_keys}"
                )
            if dropped_keys:
                logger.warning(
                    f"[MetadataReducer][DEFAULT_FILTERS] Dropped {len(dropped_keys)} filters "
                    f"(column not found in any table): {dropped_keys}"
                )

        # Step 5: Filter reduction
        t5 = time.time()
        kept_table_names_set = set(selected_table_names)

        kept_tables = [t for t in tables if t["name"] in kept_table_names_set]
        kept_relationships = [
            r for r in relationships
            if r.get("from_table") in kept_table_names_set
            and r.get("to_table") in kept_table_names_set
        ]
        # Sample data keys use "table_name[column_name]" format — extract table name
        kept_sample_data = {}
        for k, v in sample_data.items():
            table_name = k.split("[")[0] if "[" in k else k
            if table_name in kept_table_names_set:
                kept_sample_data[k] = v
        kept_slicers = [
            s for s in slicers if s.get("table") in kept_table_names_set
        ]
        kept_columns = [
            c for c in columns if c.get("table") in kept_table_names_set
        ]
        logger.info(
            f"[MetadataReducer][FILTER_REDUCTION] Done in {time.time()-t5:.3f}s — "
            f"{len(kept_tables)} tables, {len(kept_relationships)} relationships, "
            f"{len(kept_sample_data)} sample_data, {len(kept_slicers)} slicers"
        )

        # Step 5b: Column-level reduction
        t5b = time.time()
        # Build set of columns referenced in relationships
        relationship_cols: set = set()
        for rel in kept_relationships:
            relationship_cols.add(rel.get("from_column", ""))
            relationship_cols.add(rel.get("to_column", ""))
        relationship_cols.discard("")

        # Build set of columns referenced in filters
        active_filter_keys = set(config.get("active_filters", {}).keys())
        default_filter_keys = set(model_context.get("default_filters", {}).keys())
        all_filter_keys = active_filter_keys | default_filter_keys
        filter_col_names: set = set()
        for fk in all_filter_keys:
            if "[" in fk:
                filter_col_names.add(fk.split("[")[1].rstrip("]"))

        # Detect time intelligence from question tokens
        _time_tokens = {"year", "quarter", "month", "week", "day", "yoy", "mom", "qoq", "wow",
                        "ytd", "mtd", "qtd", "annual", "monthly", "quarterly", "weekly", "daily"}
        question_tokens_for_col = scorer.extract_question_tokens(user_question, business_terms=business_terms)
        has_time_intel = bool(set(question_tokens_for_col) & _time_tokens)

        original_col_count = sum(len(t.get("columns", [])) for t in kept_tables)
        for table in kept_tables:
            table["columns"] = scorer.reduce_columns(
                table,
                question_tokens_for_col,
                sample_data=kept_sample_data,
                kept_relationship_cols=relationship_cols,
                filter_columns=filter_col_names,
                has_time_intelligence=has_time_intel,
                threshold=config.get("synonym_threshold", 70) * 0.7,  # lower threshold for columns
            )
        reduced_col_count = sum(len(t.get("columns", [])) for t in kept_tables)

        # Also reduce the flat columns list
        kept_col_names_by_table: dict = {}
        for table in kept_tables:
            tname = table.get("name", "")
            for col in table.get("columns", []):
                cname = col.get("name", "") if isinstance(col, dict) else str(col)
                kept_col_names_by_table.setdefault(tname, set()).add(cname)

        kept_columns = [
            c for c in kept_columns
            if c.get("name", "") in kept_col_names_by_table.get(c.get("table", ""), set())
        ]

        logger.info(
            f"[MetadataReducer][COLUMN_REDUCTION] Done in {time.time()-t5b:.3f}s — "
            f"{original_col_count}→{reduced_col_count} columns ({original_col_count - reduced_col_count} removed)"
        )

        # Step 6: Value normalization
        t6 = time.time()
        active_filters = config.get("active_filters", {})
        default_filters = model_context.get("default_filters", {})
        merged_filters = {**default_filters, **active_filters} if active_filters else default_filters

        if merged_filters and config.get("enable_value_normalization", True):
            normalizer = ValueNormalizer()
            normalized_filters = normalizer.normalize_filter_values(
                active_filters=merged_filters,
                sample_data=kept_sample_data,
                slicers=kept_slicers,
                columns=kept_columns,
            )
            normalization_log = normalized_filters.pop("_normalization_log", [])
            logger.info(
                f"[MetadataReducer][VALUE_NORMALIZATION] Done in {time.time()-t6:.3f}s — "
                f"{len(merged_filters)} filters, log={normalization_log}"
            )
        else:
            normalized_filters = merged_filters
            normalization_log = []
            logger.info(f"[MetadataReducer][VALUE_NORMALIZATION] Skipped (no filters or normalization disabled)")

        # Step 6b: Build DAX skeleton (optional)
        t6b = time.time()
        skeleton_builder = DaxSkeletonBuilder()
        dax_skeleton = skeleton_builder.build(
            resolved_measures=resolved_measures,
            relationships=kept_relationships,
            active_filters=normalized_filters,
            question_intent=question_intent.to_dict() if question_intent else None,
            tables=kept_tables,
            dimension_bindings=dimension_bindings if dimension_bindings else None,
        )
        if dax_skeleton.skeleton:
            logger.info(
                f"[MetadataReducer][DAX_SKELETON] Built in {time.time()-t6b:.3f}s — "
                f"can_skip_llm={dax_skeleton.can_skip_llm}, "
                f"placeholders={dax_skeleton.open_placeholders}, "
                f"notes={dax_skeleton.strategy_notes}"
            )
        else:
            logger.info(f"[MetadataReducer][DAX_SKELETON] No skeleton built (0 measures)")

        # Step 7: Build reduced output
        kept_table_count = len(kept_tables)
        kept_measure_count = len(resolved_measures)
        reduction_pct = 0.0
        if original_table_count > 0:
            reduction_pct = round(
                (1 - kept_table_count / original_table_count) * 100, 1
            )

        reduced_output = {
            "workspace_id": model_context.get("workspace_id", ""),
            "dataset_id": model_context.get("dataset_id", ""),
            "measures": resolved_measures,
            "relationships": kept_relationships,
            "tables": kept_tables,
            "columns": kept_columns,
            "sample_data": kept_sample_data,
            "default_filters": normalized_filters,
            "slicers": kept_slicers,
        }

        # Add dimension bindings if resolved
        if dimension_bindings:
            reduced_output["dimension_bindings"] = dimension_bindings

        # Add DAX skeleton if built
        if dax_skeleton.skeleton:
            reduced_output["dax_skeleton"] = dax_skeleton.to_dict()

        reduced_output["reduction_summary"] = {
            "strategy": strategy,
            "original_tables": original_table_count,
            "kept_tables": kept_table_count,
            "original_measures": original_measure_count,
            "kept_measures": kept_measure_count,
            "original_columns": original_col_count,
            "kept_columns": reduced_col_count,
            "reduction_pct": reduction_pct,
            "relevant_tables": selected_table_names,
            "reasoning": selection_reasoning,
            "normalization_log": normalization_log,
            "question_intent": question_intent.to_dict(),
        }

        # Add split warning if multi-measure question detected
        if question_intent.needs_split:
            reduced_output["reduction_summary"]["question_split"] = {
                "needs_split": True,
                "sub_questions": question_intent.sub_questions,
                "reason": f"Detected {len(question_intent.measures)} measures in question",
            }
            logger.warning(
                f"[MetadataReducer][QUESTION_SPLIT] Suggested: "
                f"{len(question_intent.measures)} measures → "
                f"{len(question_intent.sub_questions)} sub-questions"
            )

        total_time = time.time() - pipeline_start
        logger.info(
            f"[MetadataReducer] ══ REDUCTION COMPLETE in {total_time:.2f}s ══ "
            f"{original_table_count}→{kept_table_count} tables ({reduction_pct}% cut), "
            f"{original_measure_count}→{kept_measure_count} measures, "
            f"kept: {selected_table_names}"
        )

        # Save reduced output to cache so the DAX tool can pick it up
        # without relying on the agent to pass model_context_json.
        dataset_id = config.get("dataset_id")
        workspace_id = config.get("workspace_id")
        group_id = config.get("group_id", "default")
        cache_saved = False
        if dataset_id and workspace_id:
            try:
                async with async_session_factory() as session:
                    cache_service = PowerBISemanticModelCacheService(session)
                    await cache_service.save_metadata(
                        group_id=group_id,
                        dataset_id=dataset_id,
                        workspace_id=workspace_id,
                        metadata=reduced_output,
                        report_id="reduced",
                    )
                cache_saved = True
                logger.info(
                    f"[MetadataReducer] ✓ Saved reduced context to cache "
                    f"(report_id='reduced', group={group_id}, dataset={dataset_id})"
                )
            except Exception as e:
                logger.warning(f"[MetadataReducer] ✗ Failed to save reduced context to cache: {e}")

        if cache_saved:
            # Return a compact summary instead of the full JSON.
            # The full reduced context is already saved to cache (above).
            # The DAX tool reads from the reduced cache (report_id='reduced')
            # via _resolve_model_context Priority 2 — so the agent does NOT
            # need to pass 25K+ chars of JSON through the LLM. This avoids
            # a ~90s LLM processing delay between Reducer → DAX tool.
            summary = {
                "status": "success",
                "reduction_summary": reduced_output["reduction_summary"],
                "default_filters_count": len(normalized_filters),
                "cache_saved": True,
                "message": (
                    f"Reduced model context saved to cache. "
                    f"{original_table_count}→{kept_table_count} tables ({reduction_pct}% reduction), "
                    f"{original_measure_count}→{kept_measure_count} measures. "
                    f"The DAX Generator tool will automatically load this from cache."
                ),
            }
            return json.dumps(summary)
        else:
            # Cache save failed — fall back to returning full JSON so the
            # agent can still pass it to the DAX tool via model_context_json.
            logger.warning("[MetadataReducer] Cache save failed — returning full JSON as fallback")
            return json.dumps(reduced_output)

    # ─── Step 1: Parse Model Context ────────────────────────────────────

    async def _parse_model_context(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Parse model context from JSON string or load from cache.

        Priority:
        1. model_context_json (if provided by the agent)
        2. Cache lookup using dataset_id + workspace_id (from tool_configs)
        """
        raw = config.get("model_context_json")

        # Priority 1: Parse from provided JSON
        if raw:
            if isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    # Try to extract JSON from markdown code blocks
                    json_match = re.search(r"```(?:json)?\s*\n(.*?)\n```", raw, re.DOTALL)
                    if json_match:
                        try:
                            parsed = json.loads(json_match.group(1))
                        except json.JSONDecodeError:
                            return {"error": "Could not parse model_context_json — invalid JSON."}
                    else:
                        return {"error": "Could not parse model_context_json — invalid JSON."}
                # Handle double-encoded
                if isinstance(parsed, str):
                    try:
                        parsed = json.loads(parsed)
                    except json.JSONDecodeError:
                        return {"error": "model_context_json is double-encoded but inner JSON is invalid."}
            elif isinstance(raw, dict):
                parsed = raw
            else:
                return {"error": f"model_context_json has unexpected type: {type(raw).__name__}"}

            # Detect Fetcher compact summary (no tables/measures = just a summary,
            # not real model data). Fall through to cache lookup instead.
            has_tables = bool(parsed.get("tables") or parsed.get("schema", {}).get("tables"))
            has_measures = bool(parsed.get("measures"))
            if not has_tables and not has_measures:
                logger.info(
                    f"[MetadataReducer] model_context_json has no tables/measures "
                    f"(likely Fetcher compact summary), falling through to cache. "
                    f"Keys: {list(parsed.keys())}"
                )
            else:
                return parsed

        # Priority 2: Cache fallback using dataset_id + workspace_id
        dataset_id = config.get("dataset_id")
        workspace_id = config.get("workspace_id")

        if dataset_id and workspace_id:
            group_id = config.get("group_id", "default")
            logger.info(
                f"[MetadataReducer] Cache lookup: group_id={group_id}, "
                f"dataset={dataset_id}, workspace={workspace_id}"
            )
            try:
                async with async_session_factory() as session:
                    cache_service = PowerBISemanticModelCacheService(session)
                    # Use any_report_id=True because the Reducer doesn't care
                    # which report_id the cache was saved with — it just needs
                    # the model metadata (tables, measures, relationships, etc.)
                    cached = await cache_service.get_cached_metadata(
                        group_id=group_id,
                        dataset_id=dataset_id,
                        workspace_id=workspace_id,
                        any_report_id=True,
                    )
                if cached:
                    tables = cached.get("schema", {}).get("tables", [])
                    measures = cached.get("measures", [])
                    logger.info(
                        f"[MetadataReducer] Cache HIT: {len(tables)} tables, "
                        f"{len(measures)} measures"
                    )
                    return {
                        "workspace_id": workspace_id,
                        "dataset_id": dataset_id,
                        "measures": measures,
                        "relationships": cached.get("relationships", []),
                        "tables": tables,
                        "columns": cached.get("schema", {}).get("columns", []),
                        "sample_data": cached.get("sample_data", {}),
                        "default_filters": cached.get("default_filters", {}),
                        "slicers": cached.get("slicers", []),
                    }
                else:
                    logger.warning(
                        f"[MetadataReducer] No cache found for "
                        f"group_id={group_id}, dataset={dataset_id}, workspace={workspace_id}"
                    )
            except Exception as e:
                logger.warning(f"[MetadataReducer] Cache lookup failed: {e}")

        return {
            "error": (
                "No model context available. Either provide model_context_json directly, "
                "or configure dataset_id + workspace_id in tool_configs for cache lookup."
            )
        }

    # ─── Reference DAX Parsing ─────────────────────────────────────────

    @staticmethod
    def _parse_reference_dax(
        reference_dax: str,
        tables: List[Dict],
        measures: List[Dict],
    ) -> tuple:
        """Extract table and measure names from reference DAX queries.

        Parses:
        - 'TableName'[Column] → table name
        - [MeasureName] → measure name (if it matches a known measure)
        - "Label", [MeasureName] → measure name

        Returns (set of table names, set of measure names) that exist in the model.
        """
        known_tables = {t["name"] for t in tables}
        known_measures = {m["name"] for m in measures}

        found_tables: set = set()
        found_measures: set = set()

        # Match 'TableName'[...] — single-quoted table references
        for match in re.finditer(r"'([^']+)'\s*\[", reference_dax):
            table_name = match.group(1)
            if table_name in known_tables:
                found_tables.add(table_name)

        # Match [BracketedName] — could be measure or column
        for match in re.finditer(r"(?<!')\[([^\]]+)\]", reference_dax):
            name = match.group(1)
            if name in known_measures:
                found_measures.add(name)

        return found_tables, found_measures

    # ─── Enrichment Data Merge ─────────────────────────────────────────

    @staticmethod
    def _merge_enrichment_data(
        tables: List[Dict],
        measures: List[Dict],
        columns: List[Dict],
        enrichment_data: Dict[str, Any],
    ) -> Dict[str, int]:
        """Merge optional enrichment data into model context objects.

        Enrichment format:
        {
            "tables": {"table_name": {"purpose": "...", "grain": "..."}},
            "columns": {"table_name[col_name]": {"synonyms": [...], "description": "..."}},
            "measures": {"measure_name": {"synonyms": [...], "description": "..."}}
        }

        Only sets fields that are not already populated on the target object.
        Returns counts of enriched items.
        """
        counts = {"tables": 0, "columns": 0, "measures": 0}

        # Enrich tables
        table_enrichments = enrichment_data.get("tables", {})
        if table_enrichments:
            table_map = {t["name"]: t for t in tables}
            for table_name, fields in table_enrichments.items():
                target = table_map.get(table_name)
                if target and isinstance(fields, dict):
                    for key, val in fields.items():
                        if key in ("purpose", "grain", "description") and val and not target.get(key):
                            target[key] = val
                    counts["tables"] += 1

        # Enrich columns — format: "table_name[column_name]"
        col_enrichments = enrichment_data.get("columns", {})
        if col_enrichments:
            # Build lookup: "table_name[column_name]" → column dict
            col_map: Dict[str, Dict] = {}
            for col in columns:
                key = f"{col.get('table', '')}[{col.get('name', '')}]"
                col_map[key] = col
            # Also check columns embedded in tables
            for t in tables:
                for col in t.get("columns", []):
                    if isinstance(col, dict):
                        key = f"{t['name']}[{col.get('name', '')}]"
                        if key not in col_map:
                            col_map[key] = col

            for col_key, fields in col_enrichments.items():
                target = col_map.get(col_key)
                if target and isinstance(fields, dict):
                    if fields.get("description") and not target.get("description"):
                        target["description"] = fields["description"]
                    if fields.get("synonyms"):
                        existing = target.get("synonyms", [])
                        new_syns = [s for s in fields["synonyms"] if s not in existing]
                        target["synonyms"] = existing + new_syns
                    counts["columns"] += 1

        # Enrich measures
        measure_enrichments = enrichment_data.get("measures", {})
        if measure_enrichments:
            measure_map = {m["name"]: m for m in measures}
            for measure_name, fields in measure_enrichments.items():
                target = measure_map.get(measure_name)
                if target and isinstance(fields, dict):
                    if fields.get("description") and not target.get("description"):
                        target["description"] = fields["description"]
                    if fields.get("synonyms"):
                        existing = target.get("synonyms", [])
                        new_syns = [s for s in fields["synonyms"] if s not in existing]
                        target["synonyms"] = existing + new_syns
                    counts["measures"] += 1

        return counts

    # ─── Step 3: LLM Selection ──────────────────────────────────────────

    async def _llm_select_tables_and_measures(
        self,
        user_question: str,
        ranked_tables: List[Dict],
        ranked_measures: List[Dict],
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Use LLM to select relevant tables and measures.

        Provides fuzzy pre-screening hints to guide the LLM.
        Falls back to fuzzy-only if LLM call fails.
        """
        llm_workspace_url = config["llm_workspace_url"]
        llm_token = config["llm_token"]
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4")

        # Build table catalog for the prompt
        table_catalog_lines = []
        for i, entry in enumerate(ranked_tables):
            table = entry["table"]
            score = entry["score"]
            likely = entry["likely_relevant"]

            name = table.get("name", "Unknown")
            col_count = len(table.get("columns", []))
            measure_count = len(table.get("measures", []))
            purpose = table.get("purpose", table.get("description", ""))

            flag = " ⭐ LIKELY RELEVANT" if likely else ""
            purpose_text = f" — {purpose}" if purpose else ""
            table_catalog_lines.append(
                f"  {i+1}. {name} ({col_count} cols, {measure_count} measures){purpose_text}{flag}"
            )

        # Build measure catalog (top 50 by fuzzy score to keep prompt manageable)
        top_measures = ranked_measures[:50]
        measure_catalog_lines = []
        for entry in top_measures:
            m = entry["measure"]
            score = entry["score"]
            name = m.get("name", "Unknown")
            table = m.get("table", "")
            desc = m.get("description", "")
            flag = " ⭐" if score >= config.get("synonym_boost_min", 60.0) else ""
            desc_text = f" — {desc}" if desc else ""
            measure_catalog_lines.append(
                f"  - [{table}] {name}{desc_text}{flag}"
            )

        prompt = f"""You are a Power BI semantic model analyst. Given a user's business question, select ONLY the tables and measures needed to answer it.

## USER QUESTION
{user_question}

## TABLE CATALOG
Tables marked with ⭐ scored high on fuzzy matching and are LIKELY RELEVANT.
{chr(10).join(table_catalog_lines)}

## MEASURE CATALOG (top candidates)
Measures marked with ⭐ are likely relevant.
{chr(10).join(measure_catalog_lines)}

## INSTRUCTIONS
1. Select tables that contain data needed to answer the question
2. Select measures that directly answer or support answering the question
3. Include dimension tables needed for filtering/grouping
4. Include fact tables that contain the relevant measures
5. Do NOT include tables/measures unrelated to the question
6. Prefer tables marked ⭐ but use your judgment
7. Rate each selected item with a confidence score (0.0-1.0) indicating how relevant it is

## RESPONSE FORMAT
Return ONLY valid JSON (no markdown, no explanation):
{{"tables": [{{"name": "Table1", "confidence": 0.9}}, {{"name": "Table2", "confidence": 0.7}}], "measures": [{{"name": "Measure1", "confidence": 0.95}}, {{"name": "Measure2", "confidence": 0.6}}], "reasoning": "Brief explanation"}}
"""

        llm_temperature = config.get("llm_temperature", 0.1)
        llm_confidence_threshold = config.get("llm_confidence_threshold", 0.0)

        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {"Authorization": f"Bearer {llm_token}", "Content-Type": "application/json"}
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 2000,
            "temperature": llm_temperature,
        }

        logger.info(
            f"[MetadataReducer] LLM call: model={llm_model}, "
            f"prompt={len(prompt)} chars, {len(table_catalog_lines)} tables, "
            f"{len(measure_catalog_lines)} measures in catalog"
        )

        try:
            t_llm = time.time()
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                usage = result.get("usage", {})

                logger.info(
                    f"[MetadataReducer] LLM responded in {time.time()-t_llm:.2f}s — "
                    f"{len(content)} chars, tokens: {usage}"
                )

                parsed = self._extract_json_from_response(content)
                if parsed and "tables" in parsed:
                    # Normalize: handle both plain strings and {name, confidence} objects
                    parsed = self._normalize_llm_selection(parsed, llm_confidence_threshold)
                    return parsed

                logger.warning(
                    f"[MetadataReducer] LLM response did not contain valid selection JSON. "
                    f"Response preview: {content[:300]}"
                )

        except Exception as e:
            logger.warning(f"[MetadataReducer] LLM selection failed, falling back to fuzzy: {e}")

        # Fallback: fuzzy-only selection
        threshold = config.get("synonym_threshold", 70)
        return {
            "tables": [r["table"]["name"] for r in ranked_tables if r["score"] >= threshold],
            "measures": [r["measure"]["name"] for r in ranked_measures if r["score"] >= threshold],
            "reasoning": f"Fuzzy-only fallback (LLM unavailable), threshold={threshold}",
        }

    @staticmethod
    def _extract_json_from_response(content: str) -> Optional[Dict]:
        """Extract JSON object from LLM response, handling markdown code blocks."""
        # Try direct parse
        try:
            return json.loads(content.strip())
        except json.JSONDecodeError:
            pass

        # Try extracting from code block
        json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1).strip())
            except json.JSONDecodeError:
                pass

        # Try finding JSON object in the text
        brace_match = re.search(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", content, re.DOTALL)
        if brace_match:
            try:
                return json.loads(brace_match.group(0))
            except json.JSONDecodeError:
                pass

        return None

    @staticmethod
    def _normalize_llm_selection(
        parsed: Dict[str, Any], confidence_threshold: float
    ) -> Dict[str, Any]:
        """Normalize LLM selection: handle both plain strings and {name, confidence} objects.

        Supports two response formats:
          - Old: {"tables": ["A", "B"], "measures": ["M1"]}
          - New: {"tables": [{"name": "A", "confidence": 0.9}], "measures": [...]}

        Filters items below confidence_threshold (0.0 = keep all).
        Returns normalized dict with plain string lists.
        """
        for key in ("tables", "measures"):
            items = parsed.get(key, [])
            if not items:
                continue

            normalized = []
            for item in items:
                if isinstance(item, str):
                    # Old format — no confidence info, always keep
                    normalized.append(item)
                elif isinstance(item, dict) and "name" in item:
                    conf = item.get("confidence", 1.0)
                    if conf >= confidence_threshold:
                        normalized.append(item["name"])
                    else:
                        logger.info(
                            f"[MetadataReducer] LLM confidence filter: "
                            f"dropped {key[:-1]} '{item['name']}' "
                            f"(confidence={conf:.2f} < threshold={confidence_threshold:.2f})"
                        )

            parsed[key] = normalized

        return parsed
