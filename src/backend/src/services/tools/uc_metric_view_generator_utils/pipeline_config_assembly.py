"""Assembly of the pipeline config and of the raw Power Query M sources.

Merges the optional overlays (per-model naming vocabulary, implicit-column
measures, reconciliation carry-forward overrides) into the pipeline config, and
recovers raw Power Query M sources to SQL when LLM fallback is on.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from .shared_types import RunAsync, as_list, as_str, parse_json_input

logger = logging.getLogger(__name__)


def _bucket(config: dict[str, object], key: str) -> dict[str, object]:
    """``config[key]`` as a dict, created (or replaced if not a dict) as needed."""
    existing = config.get(key)
    if isinstance(existing, dict):
        return existing
    fresh: dict[str, object] = {}
    config[key] = fresh
    return fresh


def _apply_naming_config(config: dict[str, object], naming_raw: object) -> None:
    """Per-model naming/period vocabulary for the metadata lint and period folding.

    The reference model's values are the defaults; only other models supply it.
    Lands under config["naming_config"] where the pipeline (lint) and JoinDetector
    (period) read it.
    """
    if not naming_raw:
        return
    try:
        naming = parse_json_input(naming_raw, {})
        if isinstance(naming, dict) and naming:
            config["naming_config"] = naming
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("[UCMV] Failed to parse naming_config: %s", e)


def _merge_implicit_column_measures(
    config: dict[str, object], implicit_raw: object
) -> None:
    """Implicit visual-column measures ride inside ``config``, same bucket shape as
    switch_decompositions — merge rather than overwrite, in case config_json already
    carried one (e.g. a manually supplied config in a standalone/JSON-mode run).
    """
    if not implicit_raw:
        return
    try:
        implicit = parse_json_input(implicit_raw, {})
        if isinstance(implicit, dict) and implicit:
            bucket = _bucket(config, "implicit_column_measures")
            for table, entries in implicit.items():
                target = bucket.setdefault(table, [])
                if isinstance(target, list):
                    target.extend(as_list(entries))
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("[UCMV] Failed to parse implicit_column_measures: %s", e)


def _merge_manual_overrides(config: dict[str, object], overrides_raw: object) -> None:
    """Carry-forward overrides from the reconciliation loop.

    Measures that already reconciled in a PRIOR cycle arrive as manual_overrides,
    so table_processor reuses their SQL verbatim and drops them from the LLM batch.
    Only the measures that MISSED are re-translated on a refine cycle.
    """
    if not overrides_raw:
        return
    try:
        overrides = (
            parse_json_input(overrides_raw, {})
            if isinstance(overrides_raw, str)
            else overrides_raw
        )
        if isinstance(overrides, dict) and overrides:
            bucket = _bucket(config, "manual_overrides")
            for table, entries in overrides.items():
                if isinstance(entries, list):
                    target = bucket.setdefault(table, [])
                    if isinstance(target, list):
                        target.extend(entries)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("[UCMV] Failed to parse manual_overrides: %s", e)


def apply_config_overlays(
    config: dict[str, object],
    *,
    in_recon_loop: bool,
    naming_raw: object,
    implicit_column_measures_raw: object,
    manual_overrides_raw: object,
) -> None:
    """Merge the optional overlays into ``config`` (mutates it in place)."""
    # Cross-fact UNION sources are emitted ONLY inside the reconciliation loop,
    # where PBI validates the numbers cell-by-cell (see cross_table_source_planner).
    if in_recon_loop:
        config["reconciliation_active"] = True
    _apply_naming_config(config, naming_raw)
    _merge_implicit_column_measures(config, implicit_column_measures_raw)
    _merge_manual_overrides(config, manual_overrides_raw)


def capture_raw_sources(mquery_entries: list[dict]) -> dict[str, str]:
    """The ORIGINAL source expression per table, before any M→SQL recovery.

    Captured so the UI can show the FULL raw Power Query M (not just the derived
    Source SQL) and audit which transformation steps it carries. Keyed by PBI
    table_name; mapped to the emit table_key later.
    """
    raw_source_by_name: dict[str, str] = {}
    if isinstance(mquery_entries, list):
        for entry in mquery_entries:
            if isinstance(entry, dict) and entry.get("table_name"):
                raw_source_by_name[str(entry["table_name"])] = str(
                    entry.get("transpiled_sql") or ""
                )
    return raw_source_by_name


@dataclass(frozen=True)
class RawMRecovery:
    """Outcome of the raw Power Query M → SQL recovery."""

    entries: list[dict]
    raw_m_count: int
    raw_m_recovered: int


def recover_raw_m_sources(
    mquery_entries: list[dict],
    *,
    use_llm: bool,
    llm_model: str | None,
    run_async: RunAsync,
) -> RawMRecovery:
    """Rewrite raw-M table sources to Spark SQL the parser can read (opt-in).

    When a table's source is raw M (`let ... in ...`) with no embedded native SQL,
    MQueryParser cannot extract a FROM clause, so the table yields no view. With
    LLM fallback enabled, rewrite those entries' transpiled_sql to a SELECT. Fail-
    open: entries the LLM cannot translate are left as they were.

    The counts are returned so the post-emit safety net can tell "no raw-M
    tables" from "raw-M tables whose recovery produced nothing" (the shape of an
    LLM failure that otherwise reads as a silent 0 views).
    """
    raw_m_count = 0
    raw_m_recovered = 0
    entries = mquery_entries
    if use_llm and isinstance(entries, list) and entries:
        try:
            from src.services.tools.metric_view_utils.mquery_parser import (
                looks_like_raw_mquery,
            )

            raw_m_count = sum(
                1
                for e in entries
                if isinstance(e, dict)
                and looks_like_raw_mquery(as_str(e.get("transpiled_sql")))
            )
            if raw_m_count:
                from src.services.tools.metric_view_utils.mquery_llm_fallback import (
                    recover_sources_with_llm,
                )

                logger.info(
                    "[UCMV] %s raw M-Query table(s) detected; attempting M→SQL LLM recovery",
                    raw_m_count,
                )
                entries, raw_m_recovered = run_async(
                    recover_sources_with_llm(
                        entries,
                        model=(llm_model or "databricks-claude-sonnet-4-5"),
                    )
                )
                logger.info(
                    "[UCMV] M→SQL recovery: %s/%s table(s) recovered",
                    raw_m_recovered,
                    raw_m_count,
                )
        except Exception as m_err:
            logger.warning(
                "[UCMV] M→SQL LLM recovery skipped (non-fatal): %s",
                m_err,
                exc_info=True,
            )
    return RawMRecovery(
        entries=entries,
        raw_m_count=raw_m_count,
        raw_m_recovered=raw_m_recovered,
    )
