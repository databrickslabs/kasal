"""DAX calculated columns read from an Admin Scanner result.

The Admin Scanner (``getInfo?datasetSchema=true&datasetExpressions=true``, see
``pipeline_config.trigger_admin_scan``) returns, per table, ``columns[]``. A calculated
column is a column whose ``columnType`` is ``"Calculated"`` and that carries an
``expression`` (its DAX). This is the same test as
``SemanticModel``'s ``TableColumn.is_calculated`` in ``converters/formats/mquery/models.py``.

The output has the shape of ``tmdl_enrichment.extract_calculated_columns_from_tmdl`` so the
two sources are interchangeable: ``{table: [{name, expression, data_type}]}``.

Pure functions: no I/O.
"""

from __future__ import annotations

from collections.abc import Collection, Iterator, Mapping

#: Power BI column data types the translator can use, as DAX kinds. Others are left unknown.
_DAX_KIND_BY_DATA_TYPE = {
    "Int64": "integer",
    "Double": "number",
    "Decimal": "number",
    "String": "text",
    "Boolean": "boolean",
    "DateTime": "date",
    "Date": "date",
}


def _dataset_tables(
    scan_result: Mapping[str, object] | None, dataset_id: str | None
) -> Iterator[dict]:
    """Every table dict of the scanned datasets (``dataset_id`` filters, case-insensitive)."""
    if not isinstance(scan_result, Mapping):
        return
    for workspace in scan_result.get("workspaces") or []:
        if not isinstance(workspace, Mapping):
            continue
        for dataset in workspace.get("datasets") or []:
            if not isinstance(dataset, Mapping):
                continue
            if (
                dataset_id
                and str(dataset.get("id") or "").lower() != dataset_id.lower()
            ):
                continue
            for table in dataset.get("tables") or []:
                if isinstance(table, Mapping) and table.get("name"):
                    yield table


def _column_name(column: Mapping[str, object]) -> str:
    return str(column.get("columnName") or column.get("name") or "").strip()


def scan_calculated_columns(
    scan_result: Mapping[str, object] | None, dataset_id: str | None = None
) -> dict[str, list[dict]]:
    """``{table: [{name, expression, data_type}]}`` for the calculated columns of a scan.

    Tables with no calculated column are left out, as the TMDL extraction does.
    """
    out: dict[str, list[dict]] = {}
    for table in _dataset_tables(scan_result, dataset_id):
        found: list[dict] = []
        for column in table.get("columns") or []:
            if not isinstance(column, Mapping):
                continue
            expression = str(column.get("expression") or "").strip()
            name = _column_name(column)
            if column.get("columnType") != "Calculated" or not expression or not name:
                continue
            found.append(
                {
                    "name": name,
                    "expression": expression,
                    "data_type": str(column.get("dataType") or ""),
                }
            )
        if found:
            out.setdefault(str(table["name"]), []).extend(found)
    return out


def scan_column_kinds(
    scan_result: Mapping[str, object] | None,
    tables: Collection[str],
    dataset_id: str | None = None,
) -> dict[str, dict[str, str]]:
    """``{table: {column name: DAX kind}}`` for the plain columns of ``tables``.

    Only columns whose Power BI data type maps to a kind are included; the translator treats
    any other column as unknown and refuses the formulas that need its type.
    """
    wanted = set(tables)
    out: dict[str, dict[str, str]] = {}
    for table in _dataset_tables(scan_result, dataset_id):
        name = str(table["name"])
        if name not in wanted:
            continue
        kinds: dict[str, str] = {}
        for column in table.get("columns") or []:
            if (
                not isinstance(column, Mapping)
                or column.get("columnType") == "Calculated"
            ):
                continue
            kind = _DAX_KIND_BY_DATA_TYPE.get(str(column.get("dataType") or ""))
            column_name = _column_name(column)
            if kind and column_name:
                kinds[column_name] = kind
        if kinds:
            out.setdefault(name, {}).update(kinds)
    return out


def merge_calculated_columns(
    from_tmdl: Mapping[str, list[dict]], from_scan: Mapping[str, list[dict]]
) -> dict[str, list[dict]]:
    """TMDL wins for a table both sources describe; the scan fills the tables TMDL lacks.

    TMDL is the model's own definition file, so it is preferred when it has a table. The scan
    covers the same model through the Admin API, so it is used only where TMDL is silent (a
    failed or missing TMDL fetch) instead of appending a second copy of each column.
    """
    merged: dict[str, list[dict]] = {
        name: list(cols) for name, cols in from_scan.items()
    }
    for name, cols in from_tmdl.items():
        merged[name] = list(cols)
    return merged
