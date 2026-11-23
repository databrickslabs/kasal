"""Calculated columns read from an Admin Scanner result (``scan_calculated_columns``).

The payload is shaped like ``getInfo?datasetSchema=true&datasetExpressions=true`` returns it:
``workspaces[].datasets[].tables[].columns[]`` with ``columnType`` and, for a calculated
column, ``expression``. The shape is the one ``converters/formats/mquery/models.py`` reads
(``TableColumn.is_calculated``); no recorded Scanner response is in the repo.
"""

from src.services.powerbi.scan_calculated_columns import (
    merge_calculated_columns,
    scan_calculated_columns,
    scan_column_kinds,
)

DS_ID = "11111111-2222-3333-4444-555555555555"


def _scan(*datasets: dict) -> dict:
    return {"workspaces": [{"id": "ws", "datasets": list(datasets)}]}


def _dataset(name: str, tables: list[dict], ds_id: str = DS_ID) -> dict:
    return {"id": ds_id, "name": name, "tables": tables}


FACT = {
    "name": "Fact",
    "columns": [
        {"name": "posting_date", "dataType": "DateTime", "columnType": "Data"},
        {"name": "amount", "dataType": "Double", "columnType": "Data"},
        {
            "name": "date_key",
            "dataType": "Int64",
            "columnType": "Calculated",
            "expression": "YEAR(Fact[posting_date]) * 10000 + MONTH(Fact[posting_date]) * 100",
        },
        # A data column that carries an expression is not a calculated column.
        {"name": "note", "dataType": "String", "columnType": "Data", "expression": "x"},
        # A calculated column with no expression is not one either.
        {"name": "bare", "dataType": "String", "columnType": "Calculated"},
    ],
}


class TestScanCalculatedColumns:
    def test_calculated_column_with_expression_is_extracted(self):
        out = scan_calculated_columns(_scan(_dataset("M", [FACT])))
        assert out == {
            "Fact": [
                {
                    "name": "date_key",
                    "expression": "YEAR(Fact[posting_date]) * 10000 + MONTH(Fact[posting_date]) * 100",
                    "data_type": "Int64",
                }
            ]
        }

    def test_table_without_calculated_columns_is_omitted(self):
        plain = {"name": "Dim", "columns": [{"name": "k", "columnType": "Data"}]}
        assert scan_calculated_columns(_scan(_dataset("M", [plain]))) == {}

    def test_column_name_falls_back_to_column_name_key(self):
        table = {
            "name": "Fact",
            "columns": [
                {
                    "columnName": "plant_desc",
                    "dataType": "String",
                    "columnType": "Calculated",
                    "expression": "UPPER(Fact[plant])",
                }
            ],
        }
        out = scan_calculated_columns(_scan(_dataset("M", [table])))
        assert [c["name"] for c in out["Fact"]] == ["plant_desc"]

    def test_dataset_filter_is_case_insensitive(self):
        other = _dataset("O", [FACT], ds_id="aaaaaaaa-0000-0000-0000-000000000000")
        scan = _scan(_dataset("M", [FACT]), other)
        assert list(scan_calculated_columns(scan, DS_ID.upper())) == ["Fact"]
        assert (
            scan_calculated_columns(scan, "bbbbbbbb-0000-0000-0000-000000000000") == {}
        )

    def test_missing_or_malformed_scan_gives_nothing(self):
        assert scan_calculated_columns(None) == {}
        assert scan_calculated_columns({}) == {}
        assert (
            scan_calculated_columns({"workspaces": [{"datasets": [None, "x"]}]}) == {}
        )
        bad_columns = {"name": "Fact", "columns": ["not-a-dict", None]}
        assert scan_calculated_columns(_scan(_dataset("M", [bad_columns]))) == {}


class TestScanColumnKinds:
    def test_plain_columns_map_to_dax_kinds_for_the_asked_tables(self):
        scan = _scan(
            _dataset(
                "M",
                [
                    FACT,
                    {
                        "name": "Dim",
                        "columns": [
                            {
                                "name": "month",
                                "dataType": "String",
                                "columnType": "Data",
                            }
                        ],
                    },
                ],
            )
        )
        kinds = scan_column_kinds(scan, ["Fact"])
        assert kinds == {
            "Fact": {"posting_date": "date", "amount": "number", "note": "text"}
        }

    def test_calculated_columns_and_unmapped_types_are_left_out(self):
        table = {
            "name": "Fact",
            "columns": [
                {
                    "name": "date_key",
                    "dataType": "Int64",
                    "columnType": "Calculated",
                    "expression": "1",
                },
                {"name": "blob", "dataType": "Binary", "columnType": "Data"},
                {"name": "flag", "dataType": "Boolean", "columnType": "Data"},
            ],
        }
        assert scan_column_kinds(_scan(_dataset("M", [table])), ["Fact"]) == {
            "Fact": {"flag": "boolean"}
        }


class TestMergeCalculatedColumns:
    def test_tmdl_wins_for_a_table_both_describe(self):
        from_tmdl = {
            "Fact": [{"name": "date_key", "expression": "TMDL", "data_type": ""}]
        }
        from_scan = {
            "Fact": [{"name": "date_key", "expression": "SCAN", "data_type": "Int64"}],
            "Dim": [{"name": "label", "expression": "SCAN", "data_type": "String"}],
        }
        merged = merge_calculated_columns(from_tmdl, from_scan)
        assert merged["Fact"] == from_tmdl["Fact"]
        assert merged["Dim"] == from_scan["Dim"]

    def test_scan_fills_when_tmdl_is_empty(self):
        from_scan = {"Fact": [{"name": "date_key", "expression": "S", "data_type": ""}]}
        assert merge_calculated_columns({}, from_scan) == from_scan

    def test_inputs_are_not_mutated(self):
        from_scan = {"Fact": [{"name": "a", "expression": "1", "data_type": ""}]}
        merge_calculated_columns({"Dim": []}, from_scan)["Fact"].append({"name": "z"})
        assert from_scan == {
            "Fact": [{"name": "a", "expression": "1", "data_type": ""}]
        }
