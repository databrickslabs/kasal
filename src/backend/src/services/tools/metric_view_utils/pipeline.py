"""MetricViewPipeline — orchestrator that ties all modules together.

Ported from the monolith's MetricViewPipeline (lines 5081-6792) with
exact output parity.  Unlike the source monolith (which writes files),
this version returns structured data (dict[str, MetricViewSpec] + stats)
for use as a Kasal tool.

Heavy lifting is delegated to:
  - table_processor.py  (per-table translation pipeline)
  - artifact_cascade.py (cross-table reclassification, grouping, counting)
  - switch_measure_builder.py, dax_var_chain.py, scan_columns.py (helpers)
  - pipeline_results.py (serialized results + metadata lint)
"""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from typing import Any

from .artifact_cascade import (
    collect_unassigned,
    count_artifacts,
    cross_table_artifact_cascade,
    group_by_table,
)
from .data_classes import MetricViewSpec, ScanTableInfo, TableInfo, TranslationResult
from .dax_translator import DaxTranslator
from .dax_var_chain import extract_divide_args, resolve_var_chain
from .join_detector import JoinDetector
from .m_transform_folder import MTransformFolder
from .metadata_generator import MetadataGenerator
from .pbi_parameter_resolver import PbiParameterResolver
from .pipeline_results import build_results
from .scan_columns import extract_columns_from_scan, extract_source_table_from_scan
from .sql_emitter import emit_deploy_sql
from .sql_post_processor import SqlPostProcessor
from .switch_measure_builder import build_switch_measure
from .table_processor import (
    _ARTIFACT_SKIP_KEYWORDS,
    TableProcessorContext,
    process_table,
)
from .utils import to_snake_case
from .yaml_emitter import emit_yaml

logger = logging.getLogger(__name__)


def _skip_stat(tinfo: TableInfo) -> dict:
    from .mquery_parser import classify_mquery_source

    """Build a skip stat that classifies WHY a table produced no view and
    carries the original M-query so it can be emitted as a note (mirrors
    how untranslatable measures are surfaced)."""
    raw = getattr(tinfo, "raw_transpiled_sql", "") or ""
    category, reason = classify_mquery_source(raw)
    return {
        "total": 0,
        "translated": 0,
        "untranslatable": 0,
        "cross_table": 0,
        "base": 0,
        "dax": 0,
        "skipped": True,
        "skip_reason": reason,
        "skip_category": category,
        # original M-query, capped, for the "not emitted" note in the report
        "original_mquery": raw[:2000],
    }


class MetricViewPipeline:
    """Orchestrate the full MQuery -> UC Metric View generation pipeline.

    Returns structured results (not files). Use emit_yaml/emit_deploy_sql
    to serialize the MetricViewSpec objects.
    """

    _ARTIFACT_SKIP_KEYWORDS = _ARTIFACT_SKIP_KEYWORDS

    # Quick-reject patterns for PBI display-only measures (not worth multi-allocating)
    _PBI_ARTIFACT_PATTERNS = re.compile(
        r"FORMAT\(|_COLOR\b|ISBLANK\(.*BLANK\(\)|SELECTEDVALUE\(|ISFILTERED\(",
        re.IGNORECASE,
    )

    def __init__(
        self,
        mapping: list[dict],
        mquery_tables: dict[str, TableInfo],
        config: dict | None = None,
        inner_dim_joins: bool = False,
        scan_data: dict | None = None,
        unflatten_tables: bool = False,
        relationships_enrichment: dict | None = None,
        llm_config: dict | None = None,
        inactive_relationships: list[dict] | None = None,
        m2n_relationships: list[dict] | None = None,
        refresh_policy_tables: list[dict] | None = None,
        no_summarize_columns: list[dict] | None = None,
        rls_tables: set[str] | None = None,
        mquery_expressions: dict | None = None,
    ):
        self.mapping = mapping
        self.mquery_tables = mquery_tables
        self.config = config or {}
        self.inner_dim_joins = inner_dim_joins
        self.scan_data = scan_data or {}
        # Raw Power Query M per table, for physical-name resolution (Gaps 1-3).
        # Prefer the explicit map; otherwise recover it from scan_data if present.
        self._mquery_expressions: dict[str, str] = dict(mquery_expressions or {})
        if not self._mquery_expressions and self.scan_data:
            for _k, _si in self.scan_data.items():
                _m = (
                    _si.get("raw_m_expression") or _si.get("m_expression")
                    if isinstance(_si, dict)
                    else getattr(_si, "raw_m_expression", None)
                )
                if _m:
                    self._mquery_expressions[_k] = _m
        # Fallback: config-gen ships the raw M per table in the config (no scan_data
        # needed). Enables physical-name resolution + generated-view SQL in the flow.
        if not self._mquery_expressions:
            _cfg_m = self.config.get("table_mquery_expressions")
            if isinstance(_cfg_m, dict):
                self._mquery_expressions = {k: v for k, v in _cfg_m.items() if v}
        self.unflatten_tables = unflatten_tables
        self.llm_config = llm_config or {}
        self._inactive_rels: list[dict] = inactive_relationships or []
        self._limitations: dict[str, Any] = {}
        if self._inactive_rels:
            self._limitations["inactive_relationships"] = self._inactive_rels
        if m2n_relationships:
            self._limitations["m2n_relationships"] = m2n_relationships
        if refresh_policy_tables:
            self._limitations["refresh_policies"] = refresh_policy_tables
        if no_summarize_columns:
            self._limitations["summarization_warnings"] = no_summarize_columns
        if rls_tables:
            self._limitations["rls_tables"] = sorted(rls_tables)

        # Calculation groups (Agent 8)
        self._calc_groups = self.config.get("calculation_groups", [])

        # Perspectives & field parameters (Agent 9)
        perspectives = self.config.get("perspectives", [])
        if perspectives:
            self._limitations["perspectives"] = perspectives

        field_parameters = self.config.get("field_parameters", [])
        if field_parameters:
            self._limitations["field_parameters"] = field_parameters

        self.translator = DaxTranslator(config=self.config)
        self.join_detector = JoinDetector(mquery_tables, config=self.config)

        # Merge auto-enrichment joins from PBI relationships.
        # Hardcoded _ENRICHMENT_JOINS take precedence -- auto entries are only added
        # when no existing hardcoded entry covers the same alias.
        # Deduplication against DAX-detected joins happens later in _process_table().
        self._enrichment_joins = dict(self.config.get("enrichment_joins", {}))
        if relationships_enrichment:

            def _base_table_name(source: str) -> str:
                """Normalise any source string to the leaf physical table name.

                Handles both simple dotted refs and inline SQL with FROM clauses,
                plus flattened '__'-separated names. Used for same-table dedup.
                """
                from_hits = re.findall(r"\bFROM\s+([\w.`]+)", source, re.IGNORECASE)
                tbl = from_hits[-1].strip("`") if from_hits else source.strip()
                tbl = tbl.rsplit(".", 1)[-1]  # last dot-segment
                tbl = tbl.rsplit("__", 1)[-1]  # last dunder-segment
                return tbl.lower()

            for tbl_key, auto_joins in relationships_enrichment.items():
                existing = self._enrichment_joins.setdefault(tbl_key, [])
                existing_aliases = {j["name"] for j in existing}
                existing_base_tables = {
                    _base_table_name(j["source"]) for j in existing if j.get("source")
                }
                for j in auto_joins:
                    if j["name"] in existing_aliases:
                        continue
                    if _base_table_name(j.get("source", "")) in existing_base_tables:
                        continue
                    existing.append(j)
                    existing_aliases.add(j["name"])
                    existing_base_tables.add(_base_table_name(j.get("source", "")))

        self._dimension_exclusions = self.config.get("dimension_exclusions", {})
        self._measure_metadata = self.config.get("measure_metadata", {})
        self._comment_overrides = self.config.get("comment_overrides", {})
        self._dimension_metadata = self.config.get("dimension_metadata", {})
        self._dimension_order = self.config.get("dimension_order", {})
        self.stats: dict[str, dict] = {}
        self.cross_table_measures: list[TranslationResult] = []
        self._filter_warnings: list[str] = []
        self.all_specs: dict[str, MetricViewSpec] = {}
        self._allocation_report: dict | None = None

        # Pipeline helper instances
        self.metadata_gen = MetadataGenerator(
            column_metadata=self.config.get("column_metadata", {}),
            measure_metadata=self._measure_metadata,
            dimension_metadata=self._dimension_metadata,
            comment_overrides=self._comment_overrides,
            dimension_exclusions=self._dimension_exclusions,
            dimension_order=self._dimension_order,
            name_prefixes_to_strip=tuple(self.config.get("name_prefixes_to_strip", ())),
        )
        self.sql_post_processor = SqlPostProcessor(
            unflatten_tables=unflatten_tables,
            expand_re_version=bool(
                self.config.get("parameter_defaults", {}).get("RE_Version_ranges")
            ),
        )
        self.m_transform_folder = MTransformFolder()
        self.pbi_parameter_resolver = PbiParameterResolver(
            parameter_defaults=self.config.get("parameter_defaults"),
        )

    def run(self) -> dict[str, MetricViewSpec]:
        """Execute the full pipeline and return specs per fact table.

        Returns:
            Dict mapping fact_table_key -> MetricViewSpec
        """
        self._allocate_by_columns()
        measure_groups = self._group_by_table()

        # Phase 1: Process all tables and collect specs
        self.all_specs = {}
        self._process_fact_tables(measure_groups)
        self._promote_measure_driven_facts(measure_groups)
        self._process_scan_only_tables(measure_groups)

        # Phase 2: Cross-table artifact cascade
        self._cross_table_artifact_cascade(self.all_specs)
        self._reclassify_secondary_allocations()
        self._resolve_generated_table_names()

        # Phase 2c: Rebuild YAML comment blocks to reflect updated skip_reasons
        for spec in self.all_specs.values():
            comment_override = self._comment_overrides.get(spec.fact_table_key)
            if comment_override:
                spec.comment = comment_override
            else:
                MetricViewPipeline._rebuild_comment(spec)
        self._collect_stats()

        # Handle unassigned measures
        unassigned = measure_groups.get("__unassigned__", [])
        if unassigned:
            self._collect_unassigned(unassigned)

        self._plan_cross_table_specs()
        return self.all_specs

    # ── Run phases (each step of run(), in execution order) ───────────

    def _allocate_by_columns(self) -> None:
        """Phase 0: home each measure on the fact whose columns its DAX references."""
        # ── Phase 0: column-based allocation (KASAL_FIXES M6/M7/M10) ──────
        # Home each measure on the fact whose COLUMNS its DAX references, before
        # grouping — so the existing routing places rescued/re-homed measures on
        # the right fact and keeps broken/cross-fact ones out of every fact.
        # Conservative: a correctly-homed measure is left untouched. Default on;
        # set allocate_by_columns=False for the exact prior behaviour.
        if self.config.get("allocate_by_columns", True):
            from .measure_allocator import reallocate_by_columns

            self._allocation_report = reallocate_by_columns(
                self.mapping, self.mquery_tables, config=self.config
            )
            _r = self._allocation_report
            if any(_r.get(k) for k in ("rescued", "rehomed", "broken", "cross_fact")):
                self._limitations["measure_allocation"] = _r
                logger.info(
                    "[MetricViewPipeline] column-based allocation: "
                    "%d rescued, %d re-homed, %d broken, %d cross-fact",
                    len(_r.get("rescued", [])),
                    len(_r.get("rehomed", [])),
                    len(_r.get("broken", [])),
                    len(_r.get("cross_fact", [])),
                )
        else:
            self._allocation_report = None

    def _process_fact_tables(self, measure_groups: dict[str, list[dict]]) -> None:
        """Phase 1: specs for SQL fact tables and mapping-only tables."""
        for table_key, table_info in self.mquery_tables.items():
            if not table_info.is_fact:
                # Non-fact / raw-M tables: record a classified skip (not a silent
                # drop) so the report can emit the original M-query with a reason.
                self.stats[table_key] = _skip_stat(table_info)
                continue

            if not table_info.source_table:
                self.stats[table_key] = _skip_stat(table_info)
                continue

            dax_measures = measure_groups.get(table_key, [])
            spec = self._process_table(table_key, table_info, dax_measures)
            self.all_specs[table_key] = spec

        # Handle mapping-only tables (measures in PBI mapping but no MQuery SQL)
        mapping_only_cfg = self.config.get("mapping_only_tables", {})
        for table_key, tbl_cfg in mapping_only_cfg.items():
            dax_measures = measure_groups.get(table_key, [])
            if not dax_measures:
                continue
            stub_info = TableInfo(
                table_name=table_key,
                source_table=tbl_cfg["source_table"],
                aggregate_columns=tbl_cfg.get("aggregate_columns", []),
                group_by_columns=tbl_cfg.get("dimensions", []),
                calculated_columns=[],
                is_fact=True,
                full_sql="",
            )
            spec = self._process_table(table_key, stub_info, dax_measures)
            self.all_specs[table_key] = spec

    def _promote_measure_driven_facts(
        self, measure_groups: dict[str, list[dict]]
    ) -> None:
        """Phase 1b: promote plain-source tables that carry allocated measures."""
        # Phase 1b: Measure-driven facts ──────────────────────────────────────
        # A table can parse WITH a source_table yet NOT be detected as a fact
        # because its M-Query resolved to a plain table read (no aggregate SQL /
        # GROUP BY) — typical of "click-together" Power Query sources. If such a
        # table has DAX measures allocated to it, the aggregation lives in the
        # MEASURES, not the source query, so it IS a fact. Promote it here.
        #
        # STRICTLY ADDITIVE + ISOLATED:
        #   - only touches tables NOT already in all_specs (Phase 1 SQL facts and
        #     mapping-only facts are never re-processed),
        #   - only tables the parser gave a real source_table (nothing invented),
        #   - only tables that actually have measures allocated,
        #   - gated by allow_measure_driven_facts (default on; set False to get
        #     the exact prior behaviour).
        # The only tables it can affect are ones that produce ZERO output today.
        if self.config.get("allow_measure_driven_facts", True):
            promoted = 0
            for table_key, table_info in self.mquery_tables.items():
                if table_key in self.all_specs:
                    continue  # Phase 1 / mapping-only owns it
                if table_info.is_fact:
                    continue  # real SQL fact — not ours
                if not table_info.source_table:
                    continue  # no source to point a view at
                dax_measures = measure_groups.get(table_key, [])
                if not dax_measures:
                    continue  # no measures → correctly skipped
                forced_info = replace(table_info, is_fact=True)
                spec = self._process_table(table_key, forced_info, dax_measures)
                self.all_specs[table_key] = spec
                promoted += 1
            if promoted:
                logger.info(
                    "[MetricViewPipeline] Promoted %d measure-driven fact table(s) "
                    "(plain source + allocated DAX measures, no aggregate SQL)",
                    promoted,
                )

    def _process_scan_only_tables(self, measure_groups: dict[str, list[dict]]) -> None:
        """Specs for PBI scan tables that have measures but no MQuery entry."""
        # Handle scan-data-only tables (in PBI scan + have DAX measures, but no MQuery/mapping entry)
        if self.scan_data:
            for table_key, scan_info in self.scan_data.items():
                if table_key in self.all_specs:
                    continue  # already processed
                dax_measures = measure_groups.get(table_key, [])
                if not dax_measures:
                    continue  # no measures -> skip
                agg_cols, grp_cols = self._extract_columns_from_scan(scan_info)
                source_table = self._extract_source_table_from_scan(scan_info)
                if not source_table:
                    source_table = f"scan_only.{table_key}"
                stub_info = TableInfo(
                    table_name=table_key,
                    source_table=source_table,
                    aggregate_columns=agg_cols,
                    group_by_columns=grp_cols,
                    calculated_columns=[],
                    is_fact=True,
                    full_sql="",
                )
                spec = self._process_table(table_key, stub_info, dax_measures)
                self.all_specs[table_key] = spec

    def _reclassify_secondary_allocations(self) -> None:
        """Phase 2b: reclassify secondary allocations covered on the primary table."""
        # Phase 2b: Reclassify secondary allocations that are covered on primary table
        global_covered: set[str] = set()
        for spec in self.all_specs.values():
            for m in spec.measures:
                global_covered.add(m.original_name)
                global_covered.add(to_snake_case(m.original_name))
            for m in spec.untranslatable:
                if any(
                    k in m.skip_reason
                    for k in MetricViewPipeline._ARTIFACT_SKIP_KEYWORDS
                ):
                    global_covered.add(m.original_name)
                    global_covered.add(to_snake_case(m.original_name))
        # Build mapping: measure_name -> primary table allocation
        measure_primary: dict[str, str] = {}
        for m_entry in self.mapping:
            for alloc in m_entry.get("all_allocations", []):
                if alloc["role"] == "primary":
                    measure_primary[m_entry["measure_name"]] = alloc["table"]
        kw = MetricViewPipeline._ARTIFACT_SKIP_KEYWORDS
        for spec in self.all_specs.values():
            for m in spec.untranslatable:
                if any(k in m.skip_reason for k in kw):
                    continue  # already an artifact
                primary_table = measure_primary.get(m.original_name)
                if primary_table and primary_table != spec.fact_table_key:
                    if (
                        m.original_name in global_covered
                        or to_snake_case(m.original_name) in global_covered
                    ):
                        m.skip_reason = (
                            "Covered on primary table (secondary allocation)"
                        )

    def _resolve_generated_table_names(self) -> None:
        """Phase 2b-fix: resolve physical table/column names from the Power Query M."""
        # Phase 2b-fix: resolve physical table/column names from the Power Query M
        # (KASAL_FIXES Gaps 1-3). Fail-open — unresolved identifiers are left as-is.
        if self._mquery_expressions:
            from .physical_name_resolver import resolve_physical_names

            _res = resolve_physical_names(
                self.all_specs, self.mquery_tables, self._mquery_expressions
            )
            if _res.get("generated_tables"):
                self._limitations["generated_tables"] = _res["generated_tables"]
                # Gap 3 materialization: emit CREATE VIEW SQL for generated calendars
                # (List.Dates etc.) so the reviewer can create the missing source.
                from .generated_table_emitter import emit_view_sql

                _gen_sql = {}
                for _t in _res["generated_tables"]:
                    _sql = emit_view_sql(
                        self._mquery_expressions.get(_t, ""),
                        f"{{catalog}}.{{schema}}.{to_snake_case(_t)}",
                    )
                    if _sql:
                        _gen_sql[_t] = _sql
                if _gen_sql:
                    self._limitations["generated_view_sql"] = _gen_sql
                    logger.info(
                        "[MetricViewPipeline] emitted CREATE VIEW SQL for %d generated "
                        "table(s): %s",
                        len(_gen_sql),
                        ", ".join(sorted(_gen_sql)),
                    )

    def _collect_stats(self) -> None:
        """Phase 3: emit each spec once to collect its per-table statistics."""
        # Phase 3: Collect stats (no file I/O in Kasal — emit is done by caller)
        for table_key, spec in self.all_specs.items():
            m_meta = self._measure_metadata.get(table_key, {})
            d_meta = self._dimension_metadata.get(table_key, {})
            d_order = self._dimension_order.get(table_key)
            yaml_content = emit_yaml(
                spec,
                measure_metadata=m_meta,
                dimension_metadata=d_meta,
                dimension_order=d_order,
                percentage_multiplier_patterns=self.config.get(
                    "percentage_multiplier_patterns"
                ),
            )

            if not yaml_content:
                self.stats[table_key] = {
                    "total": len(spec.untranslatable),
                    "translated": 0,
                    "untranslatable": len(spec.untranslatable),
                    "artifacts": self._count_artifacts(spec.untranslatable),
                    "cross_table": 0,
                    "base": 0,
                    "dax": 0,
                    "switch": 0,
                    "implicit_column": 0,
                    "manual_override": 0,
                    "skipped": True,
                    "skip_reason": "All measures dropped by validation (no deployable measures)",
                }
                continue

            self.stats[table_key] = {
                "total": len(spec.measures) + len(spec.untranslatable),
                "translated": len(spec.measures),
                "untranslatable": len(spec.untranslatable),
                "artifacts": self._count_artifacts(spec.untranslatable),
                "cross_table": sum(
                    1 for m in spec.untranslatable if m.category == "cross_table"
                ),
                "base": spec.base_measure_count,
                "dax": spec.dax_measure_count,
                "switch": spec.switch_measure_count,
                "implicit_column": spec.implicit_measure_count,
                "manual_override": sum(
                    1
                    for m in spec.measures
                    if getattr(m, "category", "") == "manual_override"
                ),
                "skipped": False,
            }

    def _plan_cross_table_specs(self) -> None:
        """Cross-fact measures: same-grain UNION combined sources (reconciliation-gated)."""
        # Cross-fact measures (span 2 facts): build same-grain UNION combined
        # sources and translate them via the normal path — but ONLY when
        # reconciliation is active to validate the numbers (see
        # cross_table_source_planner). Off → leave them collected, as before.
        if self.config.get("reconciliation_active"):
            try:
                from .cross_table_source_planner import plan_cross_table_sources

                _xspecs, _xlims = plan_cross_table_sources(self)
                for _k, _spec in _xspecs.items():
                    self.all_specs[_k] = _spec
                if _xlims:
                    self._limitations.setdefault("cross_table_deferred", []).extend(
                        _xlims
                    )
            except Exception as e:  # noqa: BLE001 — never break generation
                logger.warning(
                    "[MetricViewPipeline] cross-table planning skipped: %s", e
                )

    # ── _process_table() — delegates to table_processor module ────────

    def _process_table(
        self, table_key: str, table_info: TableInfo, dax_measures: list[dict]
    ) -> MetricViewSpec:
        ctx = TableProcessorContext(
            config=self.config,
            mquery_tables=self.mquery_tables,
            translator=self.translator,
            join_detector=self.join_detector,
            scan_data=self.scan_data,
            enrichment_joins=self._enrichment_joins,
            inactive_rels=self._inactive_rels,
            unflatten_tables=self.unflatten_tables,
            llm_config=self.llm_config,
            calc_groups=self._calc_groups,
            inner_dim_joins=self.inner_dim_joins,
            dimension_exclusions=self._dimension_exclusions,
            cross_table_measures=self.cross_table_measures,
            filter_warnings=self._filter_warnings,
            limitations=self._limitations,
        )
        spec = process_table(
            table_key,
            table_info,
            dax_measures,
            ctx,
            build_switch_measure_fn=self._build_switch_measure,
            resolve_var_chain_fn=MetricViewPipeline._resolve_var_chain,
            extract_divide_args_fn=MetricViewPipeline._extract_divide_args_static,
            clean_unresolved_vars_fn=MetricViewPipeline._clean_unresolved_vars,
            validate_filter_consistency_fn=MetricViewPipeline._validate_filter_consistency,
        )
        self._sanitize_spec_measures(spec)
        return spec

    def _sanitize_spec_measures(self, spec: MetricViewSpec) -> None:
        """P5 correctness batch + PROP-3a/4a silent-wrong guard.

        First applies the SQL sanitizer (no-op divisions, self-divisions, NULL-safe
        base aggregates). Then demotes any measure whose SQL would silently produce
        a WRONG or invalid result (empty ratio, malformed NULLIF, unresolved DAX
        ref, un-applied prior-year, untranslated SUMX/CALCULATE, dangling var) to
        untranslatable — an honest TODO comment beats a number that is quietly
        wrong or SQL that won't run.
        """
        from .sql_measure_sanitizer import (
            detect_lost_dax_component,
            detect_silent_wrong,
            sanitize_measure_sql,
        )

        kept = []
        demoted = []
        for m in getattr(spec, "measures", None) or []:
            is_base = getattr(m, "category", "") == "base"
            new_sql, note = sanitize_measure_sql(m.sql_expr, is_base=is_base)
            m.sql_expr = new_sql
            if note:
                _reason = getattr(m, "skip_reason", "") or ""
                if "self-division" not in _reason:
                    m.skip_reason = (f"{_reason} [{note}]").strip()
                self._filter_warnings.append(
                    f"{spec.fact_table_key}: {m.measure_name}: {note}"
                )
            # Base measures are simple SUM(COALESCE(...)) and are never demoted.
            # Two silent-wrong checks: (a) malformed/invalid SQL markers, and
            # (b) a DAX component (ratio denom, prior-year, exclusion) that the
            # SQL silently dropped — the latter needs the ORIGINAL dax to compare.
            bad = None
            if not is_base:
                bad = detect_silent_wrong(new_sql) or detect_lost_dax_component(
                    getattr(m, "dax_expression", "") or "", new_sql
                )
            if bad:
                m.is_translatable = False
                m.sql_expr = None
                m.skip_reason = (
                    f"TODO — not emitted (would be silently wrong/invalid: {bad}). "
                    f"Original DAX needs manual translation."
                )
                demoted.append(m)
                self._filter_warnings.append(
                    f"{spec.fact_table_key}: {m.measure_name}: demoted to TODO ({bad})"
                )
            else:
                kept.append(m)
        if demoted:
            spec.measures = kept
            existing = getattr(spec, "untranslatable", None)
            if existing is None:
                spec.untranslatable = demoted
            else:
                existing.extend(demoted)

    # ── Delegates to artifact_cascade module ─────────────────────────

    def _cross_table_artifact_cascade(
        self, all_specs: dict[str, MetricViewSpec]
    ) -> None:
        cross_table_artifact_cascade(
            all_specs, self.mapping, self._PBI_ARTIFACT_PATTERNS
        )

    @staticmethod
    def _count_artifacts(untranslatable: list) -> int:
        return count_artifacts(untranslatable)

    def _group_by_table(self) -> dict[str, list[dict]]:
        return group_by_table(
            self.mapping,
            self._PBI_ARTIFACT_PATTERNS,
            self.mquery_tables,
            self.config,
            self.scan_data,
        )

    def _collect_unassigned(self, measures: list[dict]) -> None:
        collect_unassigned(
            measures, self.translator, self.cross_table_measures, self.stats
        )

    # ── Helper methods (kept in pipeline.py — small, used locally) ───

    @staticmethod
    def _rebuild_comment(spec: MetricViewSpec) -> None:
        """Rebuild the YAML comment block from current spec state.

        Called after cross-table cascade to ensure skip_reasons in the comment
        match the (possibly updated) untranslatable list.
        """
        src = spec.source_table
        table_short = (
            spec.fact_table_key.replace("fact_", "")
            .replace("FT_", "")
            .replace("Fact_", "")
        )
        cross_ct = sum(
            1 for m in spec.measures if getattr(m, "category", "") == "cross_table"
        )
        lines = [
            f"{table_short.upper()} — UC Metric View ({spec.fact_table_key})",
            "Auto-generated from MQuery Conversion Report + DAX translation.",
            "",
            f'Source: {src.split(".")[-1]}',
            f'Target catalog/schema: {".".join(src.split(".")[:2])}',
            "",
            f"{spec.base_measure_count} base measures (from MQuery SUM columns)",
            f"{spec.dax_measure_count} DAX-translated measures"
            + (f" ({cross_ct} cross-table)" if cross_ct else ""),
        ]
        if spec.switch_measure_count:
            lines.append(f"{spec.switch_measure_count} SWITCH-decomposed measures")
        if spec.implicit_measure_count:
            lines.append(
                f"{spec.implicit_measure_count} aggregated-column measures "
                "(used in visuals, no named PBI measure)"
            )
        lines.append(
            f"{len(spec.untranslatable)} untranslatable DAX measures (documented below)"
        )
        if spec.untranslatable:
            lines.append("")
            lines.append("Untranslatable:")
            for r in spec.untranslatable:
                lines.append(f"  {r.original_name} — {r.skip_reason}")
        spec.comment = "\n".join(lines)

    # ── Scan-data helpers (scan_columns.py) ───────────────────────────

    @staticmethod
    def _extract_columns_from_scan(
        scan_info: ScanTableInfo,
    ) -> tuple[list[dict], list[str]]:
        """Extract aggregate and group-by columns from scan data SQL.

        Returns: (aggregate_columns, group_by_columns)
        """
        return extract_columns_from_scan(scan_info)

    @staticmethod
    def _extract_source_table_from_scan(scan_info: ScanTableInfo) -> str:
        """Extract source table name from scan data SQL's FROM clause."""
        return extract_source_table_from_scan(scan_info)

    @staticmethod
    def _clean_unresolved_vars(expr: str | None) -> str | None:
        """Strip unresolved scorecard variable artifacts like +a1, +b1, +res11."""
        if not expr:
            return expr
        expr = re.sub(r"\)\s*\+\s*[a-z]\w*\b", ")", expr)
        return expr

    @staticmethod
    def _validate_filter_consistency(measures: list[TranslationResult]) -> list[str]:
        """Check DIVIDE measures for mismatched filter sets on numerator vs denominator.

        Returns list of warning strings for measures where numerator and denominator
        use different FILTER clauses (which may cause NULL vs value discrepancies).
        """
        warnings = []
        for m in measures:
            expr = m.sql_expr or ""
            if "/ NULLIF(" not in expr:
                continue
            parts = expr.split("/ NULLIF(", 1)
            if len(parts) != 2:
                continue
            num_filters = set(re.findall(r"FILTER\s*\(WHERE\s+([^)]+)\)", parts[0]))
            den_filters = set(re.findall(r"FILTER\s*\(WHERE\s+([^)]+)\)", parts[1]))
            if num_filters and den_filters and num_filters != den_filters:
                warnings.append(
                    f"{m.measure_name}: numerator and denominator have different FILTER clauses — "
                    f"may produce NULL vs value discrepancies in LEFT JOIN scenarios"
                )
        return warnings

    # ── SWITCH and DAX-VAR helpers (switch_measure_builder.py, dax_var_chain.py)

    def _build_switch_measure(
        self,
        defn: dict,
        join_alias: str | None = None,
        join_col: str | None = None,
        filter_sets: dict | None = None,
    ) -> TranslationResult:
        """Build a TranslationResult from a SWITCH decomposition definition."""
        return build_switch_measure(
            defn, self.config, join_alias, join_col, filter_sets
        )

    @staticmethod
    def _resolve_var_chain(dax: str) -> str:
        """Resolve chained VAR assignments up to 5 levels deep (dax_var_chain.py)."""
        return resolve_var_chain(dax)

    @staticmethod
    def _extract_divide_args_static(raw: str) -> tuple[int, int, str, str] | None:
        """Return (start, end, numerator, denominator) of the first DIVIDE(...) in *raw*."""
        return extract_divide_args(raw)

    # ── Serialization ─────────────────────────────────────────────────────

    def get_results(self) -> dict:
        """Return pipeline results as a serializable dict."""
        return build_results(self)

    def _record_source_gaps(self) -> None:
        """Gaps the YAML does not show, for ``limitations`` and the report:
        dimension joins dropped for lack of a safe source, and Power Query
        type conversions the views do not apply."""
        dropped = self.join_detector.dropped_dims
        if dropped:
            self._limitations["dropped_dimension_joins"] = list(dropped)
        type_changes = {
            name: info.type_changes
            for name, info in self.mquery_tables.items()
            if info.type_changes
        }
        if type_changes:
            self._limitations["type_conversions_not_applied"] = type_changes

    @staticmethod
    def _resolve_placeholders_in_spec(
        spec: MetricViewSpec, catalog: str, schema: str
    ) -> None:
        """Substitute any surviving {catalog}/{schema} tokens in a spec's join
        sources + source table. Idempotent, mutates in place. Belt-and-suspenders
        net at the single emit choke point where catalog/schema are known —
        covers API/self-extract mode and externally-supplied configs, not just the
        config-gen path (which also resolves them at build_config time)."""

        def _sub(val: Any) -> Any:
            if isinstance(val, str) and ("{catalog}" in val or "{schema}" in val):
                return val.replace("{catalog}", catalog).replace("{schema}", schema)
            return val

        if getattr(spec, "source_table", None):
            spec.source_table = _sub(spec.source_table)
        for j in getattr(spec, "joins", None) or []:
            if "source" in j:
                j["source"] = _sub(j["source"])

    def _infer_source_catalog(self) -> "str | None":
        """The model's dominant SOURCE catalog, inferred from specs whose source
        is already a 3-level ``catalog.schema.table``.

        Used to qualify a source resolved from a ``Value.NativeQuery`` native-SQL
        ``FROM`` clause, which uses an implicit-catalog ``schema.table`` (e.g.
        ``acme_datamart_mtc.fm016_...``) and would otherwise emit a 2-part source
        that cannot be deployed. Placeholder ``main.default.*`` / ``TODO.*``
        sources are not real source catalogs and are ignored. Returns ``None``
        when there is no evidence — we never guess a catalog out of thin air."""
        from collections import Counter

        cats: Counter = Counter()
        for s in self.all_specs.values():
            st = getattr(s, "source_table", "") or ""
            low = st.lower()
            if st.count(".") == 2 and not (
                low.startswith("todo") or low.startswith("main.default.")
            ):
                cats[st.split(".", 1)[0]] += 1
        return cats.most_common(1)[0][0] if cats else None

    @staticmethod
    def _qualify_source_catalog(
        spec: MetricViewSpec, default_src_catalog: "str | None"
    ) -> None:
        """Prepend the inferred source catalog to a 2-part (``schema.table``)
        source so it becomes a 3-level ``catalog.schema.table``. Idempotent;
        a no-op without an inferred catalog, or on sources that are already
        3-level, bare, placeholders, or unresolved ``{token}`` templates."""
        if not default_src_catalog:
            return
        st = getattr(spec, "source_table", None)
        if not isinstance(st, str) or not st:
            return
        low = st.lower()
        if st.count(".") == 1 and not (low.startswith("todo") or st.startswith("{")):
            spec.source_table = f"{default_src_catalog}.{st}"

    def emit_all_yaml(
        self, catalog: str = "main", schema: str = "default"
    ) -> dict[str, str]:
        """Emit YAML for all specs. Returns dict[table_key -> yaml_string]."""
        result = {}
        default_src_catalog = self._infer_source_catalog()
        for table_key, spec in self.all_specs.items():
            self._resolve_placeholders_in_spec(spec, catalog, schema)
            self._qualify_source_catalog(spec, default_src_catalog)
            dim_meta = self._dimension_metadata.get(table_key, {})
            meas_meta = self._measure_metadata.get(table_key, {})
            dim_order = self.metadata_gen.get_dimension_order(table_key)
            result[table_key] = emit_yaml(
                spec,
                measure_metadata=meas_meta,
                dimension_metadata=dim_meta,
                dimension_order=dim_order,
                column_alias_map=getattr(self, "_column_alias_map", {}),
                known_missing_tables=getattr(self, "_known_missing_tables", set()),
                fact_join_map=getattr(self, "_fact_join_map", {}),
                percentage_multiplier_patterns=self.config.get(
                    "percentage_multiplier_patterns"
                ),
                budget_suffix=self.config.get("budget_suffix"),
            )
        return result

    def emit_all_sql(
        self, catalog: str = "main", schema: str = "default"
    ) -> dict[str, str]:
        """Emit deploy SQL for all specs. Returns dict[table_key -> sql_string]."""
        result = {}
        default_src_catalog = self._infer_source_catalog()
        for table_key, spec in self.all_specs.items():
            self._resolve_placeholders_in_spec(spec, catalog, schema)
            self._qualify_source_catalog(spec, default_src_catalog)
            result[table_key] = emit_deploy_sql(spec, catalog=catalog, schema=schema)
        return result
