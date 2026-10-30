"""SWITCH-decomposition measure builder.

Turns one ``switch_decomposition`` config entry (raw SQL, or a num/den pair
with per-KBI filter sets) into a ``TranslationResult``. Pure: the pipeline's
config is passed in explicitly.
"""

from __future__ import annotations

from .data_classes import TranslationResult
from .dax_condition_sql import _sql_in_list
from .join_detector import _sanitize_alias


def build_switch_measure(
    defn: dict,
    config: dict,
    join_alias: str | None = None,
    join_col: str | None = None,
    filter_sets: dict | None = None,
) -> TranslationResult:
    """Build a TranslationResult from a SWITCH decomposition definition."""
    join_alias = join_alias or config.get("switch_join_alias", "")
    join_col = join_col or config.get("switch_join_col", "")
    _fs = filter_sets or {}
    if _fs:  # config identifiers reach the deployed SQL unquoted
        join_alias, join_col = _sanitize_alias(join_alias), _sanitize_alias(join_col)

    # Filter-set values are model/warehouse data deployed as metric-view
    # SQL: escaped literals, never raw '{v}'.
    def _filter_clause(fs_key: str) -> str:
        quoted = _sql_in_list(_fs[fs_key])
        return f"FILTER (WHERE {join_alias}.{join_col} IN ({quoted}))"  # identifiers validated, values escaped — nosemgrep: tmp.databricks.sql-injection.fstring-where

    # Support raw_expr for pre-built SQL expressions
    if "raw_expr" in defn:
        return TranslationResult(
            measure_name=defn["name"],
            original_name=defn.get("original_name") or defn["name"],
            sql_expr=defn["raw_expr"],
            is_translatable=True,
            skip_reason=defn.get("comment", "SWITCH decomposition"),
            dax_expression="SWITCH decomposition",
            confidence="high",
            category="switch_decomposition",
            window_spec=defn.get("window"),
            # PBI-reconciliation provenance (priority 3) — present only
            # when the entry actually resolved (a TODO skeleton carries
            # none of these); see data_classes.py's field docstrings.
            pbi_kind=defn.get("pbi_kind"),
            pbi_sources=defn.get("pbi_sources") or [],
            pbi_operator=defn.get("pbi_operator"),
        )

    num = defn["num"]
    den = defn["den"]
    num_fs = defn["num_fs"]
    den_fs = defn["den_fs"]

    if num_fs is None:
        # Complex expression -- already has filter placeholders
        for fs_key, fs_vals in _fs.items():
            quoted = _sql_in_list(fs_vals)
            placeholder = "{" + fs_key.lower() + "}"
            fc = f"(WHERE {join_alias}.{join_col} IN ({quoted}))"  # identifiers validated, values escaped — nosemgrep: tmp.databricks.sql-injection.fstring-where
            num = num.replace(placeholder, fc)
        num_expr = num
    else:
        num_expr = f"SUM(source.{num}) {_filter_clause(num_fs)}"

    if den_fs is None:
        den_expr = den
    else:
        den_expr = f"SUM(source.{den}) {_filter_clause(den_fs)}"

    num_wrapped = f"({num_expr})" if "+" in num_expr or "-" in num_expr else num_expr
    sql_expr = f"{num_wrapped} / NULLIF({den_expr}, 0)"

    return TranslationResult(
        measure_name=defn["name"],
        original_name=defn.get("original_name") or defn["name"],
        sql_expr=sql_expr,
        is_translatable=True,
        skip_reason=defn.get("comment", "SWITCH decomposition"),
        dax_expression="SWITCH decomposition",
        confidence="high",
        category="switch_decomposition",
    )
