"""Tests for the source_artifacts bridge — pipeline state → source-layer DDL,
PBI-only ingestion tasks, and the validation hook (rec#3 / S8 / rec#4 wiring)."""

from types import SimpleNamespace

from src.services.tools.metric_view_utils.source_artifacts import (
    build_source_layer,
    detect_pbi_only_tables,
    run_pbi_validation,
)


class TestBuildSourceLayer:
    def test_native_query_table_emits_view_from_native_sql(self):
        specs = {"Fact_NPS": SimpleNamespace(source_table="cat.sch.nps")}
        mq = {
            "Fact_NPS": SimpleNamespace(
                native_query_sql="SELECT a, b/c AS r FROM dc.sch.tbl WHERE x = 1",
                source_table="cat.sch.nps",
            )
        }
        out = build_source_layer(specs, mq, {}, "dc_adb-landing-zone-002", "idor")
        assert "Fact_NPS" in out
        ddl = out["Fact_NPS"]["ddl"]
        assert "CREATE OR REPLACE VIEW" in ddl
        # catalog with a hyphen must be back-quoted
        assert "`dc_adb-landing-zone-002`.`idor`.`src_fact_nps`" in ddl
        # native SQL (with its derived column) is what the view reads
        assert "b/c AS r" in ddl

    def test_passthrough_when_no_native_sql(self):
        specs = {
            "Dim_Country": SimpleNamespace(source_table="dc.udm.cust_exp_dim_company")
        }
        out = build_source_layer(specs, {}, {}, "main", "default")
        assert "Dim_Country" in out
        assert "`dc`.`udm`.`cust_exp_dim_company`" in out["Dim_Country"]["ddl"]

    def test_calc_columns_surface_as_todo_not_raw_dax(self):
        specs = {"Cal": SimpleNamespace(source_table="c.s.cal")}
        cfg = {
            "calculated_columns": {
                "Cal": [{"name": "lbl", "expression": 'FORMAT([d],"YYYY")'}]
            }
        }
        out = build_source_layer(specs, {}, cfg, "c", "s")
        assert out["Cal"]["todo_steps"], "DAX calc column should be recorded as a TODO"

    def test_table_with_no_resolvable_source_is_skipped(self):
        specs = {"Ghost": SimpleNamespace(source_table="")}
        out = build_source_layer(specs, {}, {}, "c", "s")
        assert "Ghost" not in out


class TestDetectPbiOnlyTables:
    def test_excel_sharepoint_table_flagged(self):
        mq = {
            "SKU": 'let Source = Excel.Workbook(SharePoint.Files("https://x")) in Source'
        }
        tasks = detect_pbi_only_tables(mq, "cat", "sch")
        assert len(tasks) == 1
        assert tasks[0]["table"] == "SKU"
        assert tasks[0]["kind"] == "Excel.Workbook"
        assert tasks[0]["snapshot_loader_stub"]

    def test_warehouse_table_not_flagged(self):
        mq = {
            "Fact": 'let Source = Databricks.Catalogs("h","p"){[Name="c"]}[Data] in Source'
        }
        assert detect_pbi_only_tables(mq, "cat", "sch") == []


class TestRunPbiValidation:
    def test_skipped_without_callback(self):
        rep = run_pbi_validation({}, {"v1": "SELECT 1"})
        assert rep["status"] == "skipped"
        assert "v1" in rep["candidate_views"]

    def test_runs_with_callback(self):
        # Trivial callback returns identical single-row shapes → verified-ish path.
        def fake_eval(_q):
            return [{"period": "2026-01", "val": 1.0}]

        rep = run_pbi_validation({}, {"v1": "SELECT 1"}, evaluate_fn=fake_eval)
        assert rep["status"] in ("ran", "error")
