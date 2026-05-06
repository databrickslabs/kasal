"""Tests for scan data parser."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser


def _make_scan_data(tables):
    """Helper to build a minimal scan JSON structure."""
    return {
        "workspaces": [{
            "datasets": [{
                "name": "test_dataset",
                "tables": tables,
            }]
        }]
    }


class TestScanDataParserBasic:
    def test_parse_with_native_query(self):
        parser = ScanDataParser()
        data = _make_scan_data([{
            "name": "fact_table",
            "columns": [{"name": "col1", "dataType": "String"}],
            "source": [{
                "expression": (
                    'let Source = Value.NativeQuery(conn, '
                    '"SELECT col1 FROM table", null, [EnableFolding=true]) in Source'
                )
            }]
        }])
        result = parser.parse(data)
        assert "fact_table" in result
        assert "SELECT col1 FROM table" in result["fact_table"].native_sql

    def test_parse_empty_workspaces(self):
        parser = ScanDataParser()
        result = parser.parse({"workspaces": []})
        assert result == {}

    def test_parse_empty_datasets(self):
        parser = ScanDataParser()
        result = parser.parse({"workspaces": [{"datasets": []}]})
        assert result == {}

    def test_parse_no_native_query(self):
        parser = ScanDataParser()
        data = _make_scan_data([{
            "name": "dim",
            "source": [{"expression": "Table.FromRows({})"}]
        }])
        result = parser.parse(data)
        assert "dim" not in result

    def test_parse_no_source(self):
        parser = ScanDataParser()
        data = _make_scan_data([{
            "name": "orphan",
        }])
        result = parser.parse(data)
        assert "orphan" not in result


class TestScanDataParserMultipleTables:
    def test_multiple_tables_parsed(self):
        parser = ScanDataParser()
        data = _make_scan_data([
            {
                "name": "fact_a",
                "source": [{
                    "expression": (
                        'let S = Value.NativeQuery(c, "SELECT a FROM tbl_a",'
                        ' null, [EnableFolding=true]) in S'
                    )
                }]
            },
            {
                "name": "fact_b",
                "source": [{
                    "expression": (
                        'let S = Value.NativeQuery(c, "SELECT b FROM tbl_b",'
                        ' null, [EnableFolding=true]) in S'
                    )
                }]
            },
        ])
        result = parser.parse(data)
        assert "fact_a" in result
        assert "fact_b" in result


class TestScanDataParserDatasetSelection:
    def test_specific_dataset_by_name(self):
        parser = ScanDataParser()
        data = {
            "workspaces": [{
                "datasets": [
                    {
                        "name": "ds_a",
                        "tables": [{
                            "name": "tbl",
                            "source": [{
                                "expression": (
                                    'Value.NativeQuery(c, "SELECT 1",'
                                    ' null, [EnableFolding=true])'
                                )
                            }]
                        }]
                    },
                    {
                        "name": "ds_b",
                        "tables": []
                    },
                ]
            }]
        }
        result = parser.parse(data, dataset_name="ds_a")
        assert "tbl" in result

    def test_specific_dataset_not_found(self):
        parser = ScanDataParser()
        data = {
            "workspaces": [{
                "datasets": [{"name": "other", "tables": []}]
            }]
        }
        result = parser.parse(data, dataset_name="nonexistent")
        assert result == {}


class TestScanDataParserNativeSqlExtraction:
    def test_has_union_flag(self):
        parser = ScanDataParser()
        data = _make_scan_data([{
            "name": "union_tbl",
            "source": [{
                "expression": (
                    'Value.NativeQuery(c, "SELECT a FROM t1 UNION ALL SELECT b FROM t2",'
                    ' null, [EnableFolding=true])'
                )
            }]
        }])
        result = parser.parse(data)
        assert result["union_tbl"].has_union is True

    def test_no_union_flag(self):
        parser = ScanDataParser()
        data = _make_scan_data([{
            "name": "simple_tbl",
            "source": [{
                "expression": (
                    'Value.NativeQuery(c, "SELECT a FROM t1",'
                    ' null, [EnableFolding=true])'
                )
            }]
        }])
        result = parser.parse(data)
        assert result["simple_tbl"].has_union is False


class TestScanDataParserMSteps:
    def test_m_steps_parsed(self):
        parser = ScanDataParser()
        m_expr = (
            'let Source = Value.NativeQuery(c, "SELECT 1", null, [EnableFolding=true]),\n'
            '#"Filtered Rows" = Table.SelectRows(Source, each [col] > 0),\n'
            '#"Renamed" = Table.RenameColumns(#"Filtered Rows", {{"a", "b"}})\n'
            'in #"Renamed"'
        )
        data = _make_scan_data([{
            "name": "stepped",
            "source": [{"expression": m_expr}]
        }])
        result = parser.parse(data)
        assert "stepped" in result
        steps = result["stepped"].m_steps
        assert len(steps) >= 1
        assert steps[0].step_type == "SelectRows"

    def test_m_steps_skip_firstn(self):
        parser = ScanDataParser()
        m_expr = (
            'let Source = Value.NativeQuery(c, "SELECT 1", null, [EnableFolding=true]),\n'
            '#"Limit" = Table.FirstN(Source, 100)\n'
            'in #"Limit"'
        )
        data = _make_scan_data([{
            "name": "limited",
            "source": [{"expression": m_expr}]
        }])
        result = parser.parse(data)
        assert "limited" in result
        steps = result["limited"].m_steps
        assert len(steps) == 0  # FirstN is skipped

    def test_pbi_columns_preserved(self):
        parser = ScanDataParser()
        cols = [
            {"name": "id", "dataType": "Int64"},
            {"name": "name", "dataType": "String"},
        ]
        data = _make_scan_data([{
            "name": "with_cols",
            "columns": cols,
            "source": [{
                "expression": (
                    'Value.NativeQuery(c, "SELECT id, name FROM t",'
                    ' null, [EnableFolding=true])'
                )
            }]
        }])
        result = parser.parse(data)
        assert len(result["with_cols"].pbi_columns) == 2
        assert result["with_cols"].pbi_columns[0]["name"] == "id"
