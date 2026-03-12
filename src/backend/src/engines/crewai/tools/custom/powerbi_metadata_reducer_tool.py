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

        if strategy == "fuzzy":
            # ── Fuzzy-only: deterministic scoring, no LLM ──
            logger.info(f"[MetadataReducer] Step 2: Fuzzy scoring (threshold={threshold})...")
            ranked_tables = scorer.rank_tables(tables, user_question)
            ranked_measures = scorer.rank_measures(measures, user_question)

            # Log top fuzzy scores for visibility
            top_tables = [(r["table"]["name"], r["score"]) for r in ranked_tables[:8]]
            logger.info(f"[MetadataReducer]   Top table scores: {top_tables}")
            top_measures_scores = [(r["measure"]["name"], r["score"]) for r in ranked_measures[:8]]
            logger.info(f"[MetadataReducer]   Top measure scores: {top_measures_scores}")

            selected_table_names = [
                r["table"]["name"] for r in ranked_tables if r["score"] >= threshold
            ]
            selected_measure_names = [
                r["measure"]["name"] for r in ranked_measures if r["score"] >= threshold
            ]
            logger.info(
                f"[MetadataReducer] Step 2 done in {time.time()-t2:.2f}s — "
                f"selected {len(selected_table_names)} tables, {len(selected_measure_names)} measures"
            )
            selection_reasoning = f"Fuzzy-only selection (threshold={threshold})"

        elif strategy == "llm":
            # ── LLM-only: send full catalog to LLM without fuzzy hints ──
            if not llm_workspace_url or not llm_token:
                return json.dumps({
                    "error": "LLM strategy requires llm_workspace_url and llm_token to be configured."
                })
            logger.info(f"[MetadataReducer] Step 2+3: LLM-only selection (model={config.get('llm_model')})...")
            ranked_tables = scorer.rank_tables(tables, user_question)
            ranked_measures = scorer.rank_measures(measures, user_question)
            for entry in ranked_tables:
                entry["likely_relevant"] = False
            selected = await self._llm_select_tables_and_measures(
                user_question, ranked_tables, ranked_measures, config
            )
            selected_table_names = selected.get("tables", [])
            selected_measure_names = selected.get("measures", [])
            selection_reasoning = selected.get("reasoning", "LLM-only selection")
            logger.info(
                f"[MetadataReducer] Step 2+3 done in {time.time()-t2:.2f}s — "
                f"LLM selected {len(selected_table_names)} tables: {selected_table_names}, "
                f"{len(selected_measure_names)} measures: {selected_measure_names}"
            )

        elif strategy == "combined":
            # ── Combined: fuzzy pre-screening + LLM with hints ──
            logger.info(f"[MetadataReducer] Step 2: Fuzzy pre-screening...")
            ranked_tables = scorer.rank_tables(tables, user_question)
            ranked_measures = scorer.rank_measures(measures, user_question)
            fuzzy_time = time.time() - t2

            top_tables = [(r["table"]["name"], r["score"], r["likely_relevant"]) for r in ranked_tables[:8]]
            logger.info(f"[MetadataReducer]   Fuzzy done in {fuzzy_time:.2f}s. Top tables: {top_tables}")

            if llm_workspace_url and llm_token:
                t3 = time.time()
                logger.info(f"[MetadataReducer] Step 3: LLM selection (model={config.get('llm_model')})...")
                selected = await self._llm_select_tables_and_measures(
                    user_question, ranked_tables, ranked_measures, config
                )
                selected_table_names = selected.get("tables", [])
                selected_measure_names = selected.get("measures", [])
                selection_reasoning = selected.get("reasoning", "Combined fuzzy + LLM selection")
                logger.info(
                    f"[MetadataReducer] Step 3 done in {time.time()-t3:.2f}s — "
                    f"LLM selected {len(selected_table_names)} tables: {selected_table_names}, "
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
        logger.info(f"[MetadataReducer] Step 4: Resolving measure dependencies...")
        resolver = MeasureDependencyResolver(measures, tables)
        resolved_measures = resolver.resolve(selected_measure_names)
        resolved_measure_names = [m["name"] for m in resolved_measures]

        dep_tables = resolver.get_tables_for_measures(resolved_measure_names)
        added_dep_tables = [t for t in dep_tables if t not in selected_table_names]
        for t in added_dep_tables:
            selected_table_names.append(t)

        logger.info(
            f"[MetadataReducer] Step 4 done in {time.time()-t4:.3f}s — "
            f"{len(resolved_measure_names)} measures (added {len(resolved_measure_names) - len(selected_measure_names)} deps), "
            f"added {len(added_dep_tables)} dep tables: {added_dep_tables}"
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
            f"[MetadataReducer] Step 5 (filter) done in {time.time()-t5:.3f}s — "
            f"{len(kept_tables)} tables, {len(kept_relationships)} relationships, "
            f"{len(kept_sample_data)} sample_data, {len(kept_slicers)} slicers"
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
                f"[MetadataReducer] Step 6 (normalize) done in {time.time()-t6:.3f}s — "
                f"{len(merged_filters)} filters, log={normalization_log}"
            )
        else:
            normalized_filters = merged_filters
            normalization_log = []
            logger.info(f"[MetadataReducer] Step 6 skipped (no filters or normalization disabled)")

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
            "reduction_summary": {
                "strategy": strategy,
                "original_tables": original_table_count,
                "kept_tables": kept_table_count,
                "original_measures": original_measure_count,
                "kept_measures": kept_measure_count,
                "reduction_pct": reduction_pct,
                "relevant_tables": selected_table_names,
                "reasoning": selection_reasoning,
                "normalization_log": normalization_log,
            },
        }

        total_time = time.time() - pipeline_start
        logger.info(
            f"[MetadataReducer] ══ REDUCTION COMPLETE in {total_time:.2f}s ══ "
            f"{original_table_count}→{kept_table_count} tables ({reduction_pct}% cut), "
            f"{original_measure_count}→{kept_measure_count} measures, "
            f"kept: {selected_table_names}"
        )

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

## RESPONSE FORMAT
Return ONLY valid JSON (no markdown, no explanation):
{{"tables": ["Table1", "Table2"], "measures": ["Measure1", "Measure2"], "reasoning": "Brief explanation"}}
"""

        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {"Authorization": f"Bearer {llm_token}", "Content-Type": "application/json"}
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 2000,
            "temperature": 0.1,
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
