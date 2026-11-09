"""Parsing of the model inputs: M-Query tables, PBI relationships and scan data.

Each input is parsed fail-open: a malformed relationships or scan payload is
logged and skipped, and generation continues without it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .shared_types import as_dict, as_list, as_str

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.data_classes import (
        ScanTableInfo,
        TableInfo,
    )
    from src.services.tools.metric_view_utils.scan_data_parser import ScanDataParser

logger = logging.getLogger(__name__)


@dataclass
class ParsedModelInputs:
    """The parsed model inputs the pipeline is built from."""

    mquery_tables: dict[str, TableInfo]
    relationships_enrichment: dict
    m2n_relationships: list[dict]
    inactive_relationships: list[dict]
    scan_data: dict[str, ScanTableInfo]
    scan_parser: ScanDataParser


def _json_source(value: object) -> str | list | dict:
    """A value the parsers accept as a source; anything else is an empty model."""
    if isinstance(value, (str, list, dict)):
        return value
    return []


def _fact_keys_from_allocations(
    measures: list[dict],
    mquery_tables: dict[str, TableInfo],
    fact_keys: set[str],
) -> None:
    """Also enrich tables that have DAX measures allocated to them but are NOT
    aggregate-SQL facts (raw-grain / data-vault sources whose M is a plain SELECT).

    Phase 1b promotes these to facts *during* the run, but RelationshipsLoader only
    builds joins for tables in fact_keys, so without adding them here they get
    ZERO joins and their join-dependent measures decline. Gate on a real
    source_table (matches Phase 1b) so UI/selection/measure-holder tables aren't
    pulled in.
    """
    for measure in measures:
        allocations = [
            as_dict(a).get("table") for a in as_list(measure.get("all_allocations"))
        ] or [measure.get("proposed_allocation")]
        for table_value in allocations:
            table_name = as_str(table_value)
            table_info = mquery_tables.get(table_name)
            if (
                table_name
                and table_info is not None
                and getattr(table_info, "source_table", None)
            ):
                fact_keys.add(table_name)


def _parse_relationships(
    relationships_raw: object,
    measures: list[dict],
    mquery_tables: dict[str, TableInfo],
) -> tuple[dict, list[dict], list[dict]]:
    """(enrichment, skipped M:N relationships, inactive relationships)."""
    from src.services.tools.metric_view_utils.relationships_loader import (
        RelationshipsLoader,
    )

    try:
        rel_data = (
            json.loads(relationships_raw)
            if isinstance(relationships_raw, str)
            else relationships_raw
        )
        loader = RelationshipsLoader()
        fact_keys = {k for k, v in mquery_tables.items() if v.is_fact}
        _fact_keys_from_allocations(measures, mquery_tables, fact_keys)
        enrichment = loader.load(_json_source(rel_data), mquery_tables, fact_keys)
        return enrichment, loader.get_skipped_m2n(), loader.get_inactive_relationships()
    except Exception as e:
        logger.warning("Failed to parse relationships: %s", e, exc_info=True)
        return {}, [], []


def parse_model_inputs(
    *,
    mquery_entries: list[dict],
    measures: list[dict],
    relationships_raw: object,
    scan_raw: object,
) -> ParsedModelInputs:
    """Parse the M-Query tables, relationships and scan data (fail-open)."""
    from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
    from src.services.tools.metric_view_utils.scan_data_parser import ScanDataParser

    parser = MQueryParser()
    mquery_tables = parser.parse_json(_json_source(mquery_entries))

    relationships_enrichment: dict = {}
    m2n_relationships: list[dict] = []
    inactive_relationships: list[dict] = []
    if relationships_raw:
        relationships_enrichment, m2n_relationships, inactive_relationships = (
            _parse_relationships(relationships_raw, measures, mquery_tables)
        )

    scan_data: dict[str, ScanTableInfo] = {}
    scan_parser = ScanDataParser()
    if scan_raw:
        try:
            scan_obj = json.loads(scan_raw) if isinstance(scan_raw, str) else scan_raw
            scan_source = scan_obj if isinstance(scan_obj, (str, dict)) else {}
            scan_data = scan_parser.parse(scan_source)
        except Exception as e:
            logger.warning("Failed to parse scan data: %s", e, exc_info=True)

    return ParsedModelInputs(
        mquery_tables=mquery_tables,
        relationships_enrichment=relationships_enrichment,
        m2n_relationships=m2n_relationships,
        inactive_relationships=inactive_relationships,
        scan_data=scan_data,
        scan_parser=scan_parser,
    )
