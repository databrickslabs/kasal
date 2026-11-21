"""Read the real columns of the deployed source-layer views.

One ``information_schema.columns`` query over the layer's namespace, run through the deployer's
own session (auth, host check, Kasal User-Agent). It answers for views as well as tables, also
for a view over ``SELECT *`` (checked on the sandbox warehouse). A view that did not deploy has
no rows, so its columns stay unknown.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

from src.services.tools.metric_view_deployer_utils.sql_helpers import (
    result_rows,
    sql_name,
    string_literal,
)
from src.services.tools.ucmv_recon_source_layer import layer_session

logger = logging.getLogger(__name__)


def columns_query(catalog: str, schema: str, leaves: Sequence[str]) -> str:
    """The SQL listing the columns of the named views (lower-case leaf names) in one namespace."""
    names = ", ".join(string_literal(leaf.lower()) for leaf in leaves)
    return (
        "SELECT lower(table_name), column_name, data_type "
        f"FROM {sql_name(catalog)}.information_schema.columns "
        f"WHERE lower(table_schema) = lower({string_literal(schema)}) "
        f"AND lower(table_name) IN ({names}) "
        "ORDER BY lower(table_name), ordinal_position"
    )


def _truncated(data: object, rows: int) -> bool:
    """Whether the answer does not hold every row: more than one chunk, a link to the next chunk,
    or fewer rows than the statement reports."""
    if not isinstance(data, Mapping):
        return True
    manifest = data.get("manifest")
    result = data.get("result")
    manifest = manifest if isinstance(manifest, Mapping) else {}
    result = result if isinstance(result, Mapping) else {}
    chunks = manifest.get("total_chunk_count")
    total = manifest.get("total_row_count")
    if isinstance(chunks, int) and chunks > 1:
        return True
    if (
        result.get("next_chunk_internal_link")
        or result.get("next_chunk_index") is not None
    ):
        return True
    return isinstance(total, int) and total != rows


def read_source_columns(
    deployer: object, deploy_base: Mapping[str, object], leaves: Sequence[str]
) -> dict[str, list[dict[str, str]]]:
    """``{leaf name: [{name, type}]}`` for the views that exist. ``{}`` when the session or the
    query fails (logged): the generator then runs with the columns it can parse."""
    if not leaves:
        return {}
    catalog, schema, runner, error = layer_session(deployer, deploy_base)
    if runner is None:
        logger.warning("[UCMV] source columns not read: %s", error)
        return {}
    outcome = runner(columns_query(catalog, schema, leaves))
    if not outcome.get("success"):
        logger.warning(
            "[UCMV] source columns not read: %s", str(outcome.get("error") or "")[:300]
        )
        return {}
    rows = result_rows(outcome.get("data"))
    if _truncated(outcome.get("data"), len(rows)):
        # A partial listing would make real columns look missing: all or nothing.
        logger.warning("[UCMV] source columns not read: the answer was truncated")
        return {}
    out: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        if len(row) < 2 or row[0] is None or row[1] is None:
            continue
        dtype = "" if len(row) < 3 or row[2] is None else str(row[2])
        out.setdefault(str(row[0]), []).append({"name": str(row[1]), "type": dtype})
    return out
