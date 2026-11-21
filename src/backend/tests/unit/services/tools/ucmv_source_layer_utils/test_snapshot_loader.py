"""Snapshot loader: DAX paging and limits, typed statements, TMDL decoding, row filters."""

import base64
import json
import zlib

import pytest

from src.services.tools.metric_view_utils.source_layer_emitter import RowFilter
from src.services.tools.ucmv_source_layer_utils import snapshot_loader as sl
from src.services.tools.ucmv_source_layer_utils.sql_types import SqlError, SqlResult

FILTER = RowFilter(function="main.gov.company_fn", columns=["company_code"])


def _spec(**kwargs):
    base = {"pbi_table": "Targets Input", "uc_table": "pbi_targets_input"}
    base.update(kwargs)
    return sl.parse_snapshot_specs([base])[0]


def _rows(n, **extra):
    return [
        {"Name": f"row {i}", "Value": float(i), "Flag": i % 2 == 0, **extra}
        for i in range(n)
    ]


class FakeDax:
    """Answers the snapshot's own queries; `rows` is the model's table."""

    def __init__(self, rows, *, cut_at=None):
        self.rows = rows
        self.cut_at = cut_at  # simulate a silent Power BI cut
        self.queries = []

    def __call__(self, query):
        self.queries.append(query)
        if query.startswith('EVALUATE ROW("n", COUNTROWS('):
            return [{"[n]": len(self.rows)}]
        if query.startswith("EVALUATE CALCULATETABLE("):
            value = query.split(" = ", 1)[1].rstrip(")")
            picked = [r for r in self.rows if f'"{r.get("Group")}"' == value]
            return [self._keyed(r) for r in picked]
        if query.startswith("EVALUATE VALUES("):
            groups = sorted({r["Group"] for r in self.rows})
            return [{"Targets Input[Group]": g} for g in groups]
        data = self.rows if self.cut_at is None else self.rows[: self.cut_at]
        return [self._keyed(r) for r in data]

    @staticmethod
    def _keyed(row):
        return {f"Targets Input[{k}]": v for k, v in row.items()}


class RecordingSql:
    def __init__(self, fail_on=None):
        self.statements = []
        self.fail_on = fail_on

    def __call__(self, statement):
        self.statements.append(statement)
        if self.fail_on and self.fail_on in statement:
            raise SqlError("warehouse said no")
        return SqlResult()


def _ctx(dax, sql, filters=None, tmdl=None):
    return sl.SnapshotContext(
        catalog="cat",
        schema="sch",
        dax=dax,
        sql=sql,
        filters=filters or {},
        loaded_at="2026-11-05 08:00:00",
        refreshed_at="2026-11-04 10:00:00",
        tmdl=tmdl,
    )


def test_parse_specs_defaults_uc_name_and_validates_types():
    spec = _spec()
    assert spec.uc_table == "pbi_targets_input"
    with pytest.raises(ValueError, match="types must be one of"):
        _spec(types={"Value": "MONEY"})
    with pytest.raises(ValueError, match="listed twice"):
        sl.parse_snapshot_specs([{"pbi_table": "A"}, {"pbi_table": "A"}])


def test_infer_sql_type_from_values():
    assert sl.infer_sql_type([True, None]) == "BOOLEAN"
    assert sl.infer_sql_type([1, 2]) == "BIGINT"
    assert sl.infer_sql_type([1, 2.5]) == "DOUBLE"
    assert sl.infer_sql_type(["2024-01-01T00:00:00"]) == "TIMESTAMP"
    assert sl.infer_sql_type(["abc", 1]) == "STRING"
    assert sl.infer_sql_type([None]) == "STRING"


def test_build_columns_uses_snake_names_map_and_type_overrides():
    spec = _spec(types={"Value": "decimal"}, column_map={"Name": "label"})
    rows = [{"Name": "a", "Value": 1.5, "Calendar Day": "2024-01-01T00:00:00"}]
    cols = sl.build_columns(spec, rows)
    assert [(c.name, c.sql_type) for c in cols] == [
        ("label", "STRING"),
        ("value", "DECIMAL"),
        ("calendar_day", "TIMESTAMP"),
    ]


def test_statements_are_create_then_batched_insert_with_escaping():
    spec = _spec()
    rows = [{"Name": "o'brien", "Value": 1.0}] + [{"Name": "x", "Value": 2.0}] * 1000
    cols = sl.build_columns(spec, rows)
    statements = sl.build_statements(
        "`cat`.`sch`.`pbi_targets_input`",
        cols,
        rows,
        comment="c",
        loaded_at="2026-11-05 08:00:00",
        refreshed_at="2026-11-04 10:00:00",
    )
    assert len(statements) == 2
    assert statements[0].startswith(
        "CREATE OR REPLACE TABLE `cat`.`sch`.`pbi_targets_input`"
    )
    assert "'o''brien'" in statements[0] or "'o\\'brien'" in statements[0]
    assert "CAST(`value` AS DOUBLE) AS `value`" in statements[0]
    assert (
        "AS loaded_at_utc" in statements[0]
        and "AS model_refreshed_at_utc" in statements[0]
    )
    assert statements[1].startswith("INSERT INTO `cat`.`sch`.`pbi_targets_input`")


def test_empty_result_refuses_to_replace_the_snapshot():
    with pytest.raises(sl.SnapshotError, match="existing snapshot is kept"):
        sl.build_statements(
            "`c`.`s`.`t`", [], [], comment="c", loaded_at="x", refreshed_at=None
        )


def test_small_table_read_in_one_query():
    dax = FakeDax(_rows(3))
    assert len(sl.fetch_rows(_spec(), dax)) == 3
    assert dax.queries[-1] == "EVALUATE 'Targets Input'"


def test_silent_cut_is_detected_and_refused():
    dax = FakeDax(_rows(5), cut_at=3)
    with pytest.raises(sl.SnapshotError, match="set page_by"):
        sl.fetch_rows(_spec(), dax)


def test_paged_read_collects_every_page():
    rows = [
        {"Group": g, "Name": f"n{i}"} for i, g in enumerate(["w1", "w1", "w2", "w3"])
    ]
    dax = FakeDax(rows)
    result = sl.fetch_rows(_spec(page_by="Group"), dax)
    assert len(result) == 4
    assert any(q.startswith("EVALUATE CALCULATETABLE(") for q in dax.queries)


def test_page_at_row_limit_is_refused(monkeypatch):
    monkeypatch.setattr(sl, "DAX_ROW_LIMIT", 3)
    rows = [{"Group": "w1"} for _ in range(3)]
    with pytest.raises(sl.SnapshotError, match="finer page_by"):
        sl.fetch_rows(_spec(page_by="Group"), FakeDax(rows))


def test_load_one_applies_row_filter_after_the_replace():
    sql = RecordingSql()
    filters = {"*": FILTER}
    outcome = sl.load_one(_spec(), _ctx(FakeDax(_rows(2)), sql, filters))
    assert outcome == {
        "pbi_table": "Targets Input",
        "uc_table": "cat.sch.pbi_targets_input",
        "rows": 2,
        "columns": 3,
        "row_filter": True,
        "status": "loaded",
    }
    assert sql.statements[-1] == (
        "ALTER TABLE `cat`.`sch`.`pbi_targets_input` SET ROW FILTER "
        "`main`.`gov`.`company_fn` ON (`company_code`)"
    )


def test_load_snapshots_reports_failure_and_keeps_going():
    specs = [_spec(), sl.parse_snapshot_specs([{"pbi_table": "Other"}])[0]]
    sql = RecordingSql(fail_on="pbi_other")
    dax = FakeDax(_rows(2))
    result = sl.load_snapshots(specs, _ctx(dax, sql))
    statuses = [s["status"] for s in result["snapshots"]]
    assert statuses == ["loaded", "failed"]
    assert result["all_loaded"] is False


def test_part_way_failure_is_explained():
    sql = RecordingSql(fail_on="INSERT INTO")
    rows = _rows(1500)
    with pytest.raises(sl.SnapshotError, match="part-way"):
        sl.load_one(_spec(), _ctx(FakeDax(rows), sql))


def test_tmdl_payload_round_trip():
    payload_rows = [
        {"CompanyCode": "0100", "Name": "Alpha"},
        {"CompanyCode": "0200", "Name": "Beta"},
    ]
    compressor = zlib.compressobj(wbits=-15)
    deflated = (
        compressor.compress(json.dumps(payload_rows).encode()) + compressor.flush()
    )
    encoded = base64.b64encode(deflated).decode()
    tmdl = f'expression CompanyCodesMapping =\n\t\tBinary.FromText("{encoded}", BinaryEncoding.Base64)\n'
    assert sl.decode_tmdl_rows(tmdl, "CompanyCodesMapping") == payload_rows


def test_tmdl_missing_expression_is_an_error():
    with pytest.raises(sl.SnapshotError, match="no Binary.FromText payload"):
        sl.decode_tmdl_rows("expression Other = 1", "CompanyCodesMapping")


def test_expressions_tmdl_part_is_extracted_from_definition():
    text = "expression X = 1"
    definition = {
        "definition": {
            "parts": [
                {
                    "path": "definition/model.tmdl",
                    "payload": base64.b64encode(b"model").decode(),
                },
                {
                    "path": "definition/expressions.tmdl",
                    "payload": base64.b64encode(text.encode()).decode(),
                },
            ]
        }
    }
    assert sl.expressions_tmdl_text(definition) == text
    with pytest.raises(sl.SnapshotError, match="no expressions.tmdl"):
        sl.expressions_tmdl_text({"definition": {"parts": []}})
