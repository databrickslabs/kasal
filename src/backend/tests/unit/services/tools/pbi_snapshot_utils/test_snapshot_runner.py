"""The snapshot runner, end to end over fakes: paging, caps, refusals, writes, the off switch."""

import base64
import zlib
from datetime import datetime, timezone


from src.services.tools.pbi_snapshot_utils import dax_paging
from src.services.tools.pbi_snapshot_utils import snapshot_runner as runner
from src.services.tools.pbi_snapshot_utils.limits import SnapshotSettings
from src.services.tools.pbi_snapshot_utils.snapshot_runner import (
    snapshot_leaf_name,
    snapshot_pbi_only_tables,
)
from tests.unit.services.tools.pbi_snapshot_utils.fakes import FakePbi, FakeSql

NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
ON = SnapshotSettings(enabled=True, max_rows=200_000)
CATALOG = "main_test"
SCHEMA = "snap_schema"


def _embedded_m(rows: list[list[str]]) -> str:
    """An 'enter data' table in M, built the way the model stores it."""
    import json

    payload = base64.b64encode(
        zlib.compress(json.dumps(rows).encode(), 9)[2:-4]
    ).decode()
    return (
        "let Source = Table.FromRows(Json.Document(Binary.Decompress("
        f'Binary.FromText("{payload}", BinaryEncoding.Base64), Compression.Deflate)), '
        "let _t = ((type nullable text) meta [Serialized.Text = true]) "
        "in type table [Tab = _t, Seq = _t])"
    )


async def _run(table, pbi, sql, **overrides):
    kwargs = dict(
        settings=ON,
        catalog=CATALOG,
        schema=SCHEMA,
        mquery_expressions={},
        rls_tables=(),
        columns_by_table={table: pbi.columns},
        pbi_query=pbi,
        run_sql=sql,
        now=NOW,
    )
    kwargs.update(overrides)
    return await snapshot_pbi_only_tables([table], **kwargs)


def _small_table():
    columns = [("Code", "int64"), ("Label", "string"), ("Active", "boolean")]
    rows = [
        {"Code": 1, "Label": "a", "Active": True},
        {"Code": 2, "Label": "it's \\ ok", "Active": False},
        {"Code": 3, "Label": None, "Active": None},
    ]
    return columns, rows


def _sets_writes(sql: FakeSql) -> list[str]:
    return [s for s in sql.writes() if not s.startswith("DROP")]


class TestOffSwitch:
    async def test_disabled_reads_and_writes_nothing(self):
        columns, rows = _small_table()
        pbi = FakePbi("Sales", columns, rows)
        sql = FakeSql()
        results = await snapshot_pbi_only_tables(
            ["Sales"],
            settings=SnapshotSettings(enabled=False),
            catalog=CATALOG,
            schema=SCHEMA,
            mquery_expressions={},
            rls_tables=(),
            columns_by_table={"Sales": columns},
            pbi_query=pbi,
            run_sql=sql,
            now=NOW,
        )
        assert results == []
        assert pbi.queries == []
        assert sql.statements == []


class TestWriteAndComment:
    async def test_single_query_table_is_written_with_marker_and_comment(self):
        columns, rows = _small_table()
        pbi = FakePbi("Sales Targets", columns, rows)
        sql = FakeSql()
        (result,) = await _run("Sales Targets", pbi, sql)

        assert result["status"] == "written"
        assert result["rows"] == 3
        assert result["strategy"] == "single_query"
        assert result["target"] == f"{CATALOG}.{SCHEMA}.snap_sales_targets"
        assert result["note"] == "snapshot, not refreshed"
        assert result["taken_at"] == "2026-01-02 03:04:05"

        create_replace = [
            s for s in sql.statements if s.startswith("CREATE OR REPLACE")
        ]
        assert len(create_replace) == 1
        ddl = create_replace[0]
        assert (
            "Snapshot of Power BI table Sales Targets, taken 2026-01-02 03:04:05 UTC"
            in ddl
        )
        assert "not refreshed" in ddl
        assert "'kasal.pbi_snapshot' = 'true'" in ddl
        assert "'kasal.pbi_snapshot.taken_at' = '2026-01-02 03:04:05'" in ddl

    async def test_values_are_escaped_and_typed_in_the_insert(self):
        columns, rows = _small_table()
        sql = FakeSql()
        await _run("Sales", FakePbi("Sales", columns, rows), sql)
        inserts = [s for s in sql.statements if s.startswith("INSERT INTO")]
        assert len(inserts) == 1
        assert "CAST('1' AS BIGINT)" in inserts[0]
        assert "'it''s \\\\ ok'" in inserts[0]
        assert "NULL, NULL" in inserts[0]
        assert "FALSE" in inserts[0]

    async def test_staging_table_is_dropped_after_success(self):
        columns, rows = _small_table()
        sql = FakeSql()
        await _run("Sales", FakePbi("Sales", columns, rows), sql)
        assert any(s.startswith("DROP TABLE IF EXISTS") for s in sql.statements)

    async def test_schema_is_never_created(self):
        columns, rows = _small_table()
        sql = FakeSql()
        await _run("Sales", FakePbi("Sales", columns, rows), sql)
        assert not any("CREATE SCHEMA" in s.upper() for s in sql.statements)


class TestPaging:
    async def test_keyset_pages_read_everything_once_in_key_order(self, monkeypatch):
        monkeypatch.setattr(runner, "PAGE_ROWS", 3)
        monkeypatch.setattr(dax_paging, "PAGE_ROWS", 3)
        columns = [("Id", "int64"), ("Name", "string")]
        rows = [{"Id": i * 10, "Name": f"n{i}"} for i in range(7)]
        pbi = FakePbi("Items", columns, rows)
        sql = FakeSql()
        (result,) = await _run("Items", pbi, sql)

        assert result["status"] == "written"
        assert result["strategy"] == "keyset_pages"
        assert result["pages"] == 3
        assert result["rows"] == 7
        # ids 0,10,...,60 read in pages of 3: first page unfiltered, then after 20, after 50
        page_queries = [q for q in pbi.queries if q.startswith("EVALUATE TOPN(")]
        assert "FILTER" not in page_queries[0]
        assert "> 20)" in page_queries[1]
        assert "> 50)" in page_queries[2]
        inserted = sum(
            s.count("\n(") for s in sql.statements if s.startswith("INSERT INTO")
        )
        assert inserted == 7

    async def test_short_pages_continue_from_the_last_key(self, monkeypatch):
        """A page cut short (the 15 MB cap) does not end the read: the next page
        starts after the last key read, so every row is read once."""
        monkeypatch.setattr(runner, "PAGE_ROWS", 4)
        monkeypatch.setattr(dax_paging, "PAGE_ROWS", 4)
        columns = [("Id", "int64"), ("Name", "string")]
        rows = [{"Id": i, "Name": "x"} for i in range(1, 9)]
        pbi = FakePbi("Items", columns, rows, cut_at=2)
        sql = FakeSql()
        (result,) = await _run("Items", pbi, sql)
        assert result["status"] == "written"
        assert result["rows"] == 8
        assert result["pages"] == 4

    async def test_a_page_that_stops_short_of_the_count_is_refused(self, monkeypatch):
        """If the service stops returning rows before the count, the table is refused."""
        monkeypatch.setattr(runner, "PAGE_ROWS", 4)
        monkeypatch.setattr(dax_paging, "PAGE_ROWS", 4)
        columns = [("Id", "int64")]
        rows = [{"Id": i} for i in range(1, 9)]
        pbi = FakePbi("Items", columns, rows)
        original = pbi.__call__

        async def stop_after_first(dax):
            if dax.startswith("EVALUATE TOPN(") and "FILTER" in dax:
                return []
            return await original(dax)

        sql = FakeSql()
        (result,) = await snapshot_pbi_only_tables(
            ["Items"],
            settings=ON,
            catalog=CATALOG,
            schema=SCHEMA,
            mquery_expressions={},
            rls_tables=(),
            columns_by_table={"Items": columns},
            pbi_query=stop_after_first,
            run_sql=sql,
            now=NOW,
        )
        assert result["status"] == "skipped"
        assert "read 4 rows but the model reports 8" in result["reason"]
        assert _sets_writes(sql) == []

    async def test_over_cap_is_skipped_with_reason_and_not_read(self):
        columns, rows = _small_table()
        pbi = FakePbi("Sales", columns, rows)
        sql = FakeSql()
        (result,) = await _run(
            "Sales", pbi, sql, settings=SnapshotSettings(enabled=True, max_rows=2)
        )
        assert result["status"] == "skipped"
        assert "pbi_snapshot_max_rows (2)" in result["reason"]
        assert all(not q.startswith("EVALUATE '") for q in pbi.queries)
        assert _sets_writes(sql) == []

    async def test_silent_truncation_is_detected_not_loaded(self):
        columns, rows = _small_table()
        pbi = FakePbi("Sales", columns, rows, cut_at=2)
        sql = FakeSql()
        (result,) = await _run("Sales", pbi, sql)
        assert result["status"] == "skipped"
        assert "read 2 rows but the model reports 3" in result["reason"]
        assert _sets_writes(sql) == []

    async def test_no_key_and_too_big_for_one_query_is_refused(self, monkeypatch):
        monkeypatch.setattr(dax_paging, "PBI_MAX_ROWS_PER_QUERY", 2)
        columns = [("Label", "string")]
        rows = [{"Label": f"x{i}"} for i in range(5)]
        pbi = FakePbi("Labels", columns, rows)
        sql = FakeSql()
        (result,) = await _run("Labels", pbi, sql)
        assert result["status"] == "skipped"
        assert "no unique whole-number column" in result["reason"]
        assert _sets_writes(sql) == []

    async def test_duplicate_int_key_is_not_used_for_paging(self, monkeypatch):
        monkeypatch.setattr(dax_paging, "PBI_MAX_ROWS_PER_QUERY", 2)
        columns = [("Code", "int64")]
        rows = [{"Code": i % 2} for i in range(5)]
        pbi = FakePbi("Codes", columns, rows)
        sql = FakeSql()
        (result,) = await _run("Codes", pbi, sql)
        assert result["status"] == "skipped"
        assert "no unique whole-number column" in result["reason"]


class TestRefusals:
    async def test_row_level_security_table_is_refused_before_any_read(self):
        columns, rows = _small_table()
        pbi = FakePbi("Sales", columns, rows)
        sql = FakeSql()
        (result,) = await _run("Sales", pbi, sql, rls_tables=["Sales"])
        assert result["status"] == "skipped"
        assert "row-level security" in result["reason"]
        assert pbi.queries == []
        assert sql.statements == []

    async def test_embedded_rows_are_not_copied_again(self):
        columns, rows = _small_table()
        pbi = FakePbi("Mapping", columns, rows)
        sql = FakeSql()
        (result,) = await _run(
            "Mapping",
            pbi,
            sql,
            mquery_expressions={"Mapping": _embedded_m([["a", "1"], ["b", "2"]])},
        )
        assert result["status"] == "skipped"
        assert "already emits" in result["reason"]
        assert pbi.queries == []

    async def test_missing_column_types_are_refused(self):
        columns, rows = _small_table()
        pbi = FakePbi("Sales", columns, rows)
        sql = FakeSql()
        (result,) = await _run("Sales", pbi, sql, columns_by_table={})
        assert result["status"] == "skipped"
        assert "no column types" in result["reason"]
        assert pbi.queries == []

    async def test_unmapped_column_type_is_refused(self):
        columns = [("Blob", "binary")]
        pbi = FakePbi("Files", columns, [{"Blob": None}])
        sql = FakeSql()
        (result,) = await _run("Files", pbi, sql)
        assert result["status"] == "skipped"
        assert "binary" in result["reason"]
        assert pbi.queries == []

    async def test_value_of_the_wrong_type_refuses_without_writing(self):
        columns = [("Code", "int64")]
        pbi = FakePbi("Codes", columns, [{"Code": "7"}])
        sql = FakeSql()
        (result,) = await _run("Codes", pbi, sql)
        assert result["status"] == "skipped"
        assert "non-integer" in result["reason"]
        assert _sets_writes(sql) == []

    async def test_extra_column_in_the_read_refuses(self):
        columns = [("Code", "int64")]
        pbi = FakePbi("Codes", columns, [{"Code": 1}])
        pbi.rows = [{"Code": 1, "Hidden": "x"}]
        sql = FakeSql()
        (result,) = await _run("Codes", pbi, sql)
        assert result["status"] == "skipped"
        assert "columns read differ" in result["reason"]
        assert _sets_writes(sql) == []

    async def test_foreign_table_with_same_name_is_never_replaced(self):
        columns, rows = _small_table()
        sql = FakeSql(existing={"snap_sales": "MANAGED"}, props={"snap_sales": {}})
        (result,) = await _run("Sales", FakePbi("Sales", columns, rows), sql)
        assert result["status"] == "skipped"
        assert "not created by a snapshot" in result["reason"]
        assert _sets_writes(sql) == []

    async def test_view_with_the_target_name_is_refused(self):
        columns, rows = _small_table()
        sql = FakeSql(existing={"snap_sales": "VIEW"})
        (result,) = await _run("Sales", FakePbi("Sales", columns, rows), sql)
        assert result["status"] == "skipped"
        assert "view" in result["reason"]
        assert _sets_writes(sql) == []

    async def test_our_earlier_snapshot_is_replaced(self):
        columns, rows = _small_table()
        sql = FakeSql(
            existing={"snap_sales": "MANAGED"},
            props={"snap_sales": {"kasal.pbi_snapshot": "true"}},
        )
        (result,) = await _run("Sales", FakePbi("Sales", columns, rows), sql)
        assert result["status"] == "written"
        assert any(s.startswith("CREATE OR REPLACE TABLE") for s in sql.statements)

    async def test_staged_count_mismatch_writes_nothing_and_drops_stage(self):
        columns, rows = _small_table()
        sql = FakeSql(count_delta=1)
        (result,) = await _run("Sales", FakePbi("Sales", columns, rows), sql)
        assert result["status"] == "skipped"
        assert "staged copy has 4 rows" in result["reason"]
        assert not any(s.startswith("CREATE OR REPLACE") for s in sql.statements)
        assert any(s.startswith("DROP TABLE IF EXISTS") for s in sql.statements)

    async def test_warehouse_failure_is_reported_and_stage_still_dropped(self):
        columns, rows = _small_table()
        sql = FakeSql(fail_on="INSERT INTO")
        (result,) = await _run("Sales", FakePbi("Sales", columns, rows), sql)
        assert result["status"] == "skipped"
        assert result["reason"] == "snapshot failed (RuntimeError)"
        assert any(s.startswith("DROP TABLE IF EXISTS") for s in sql.statements)

    async def test_pbi_failure_affects_only_its_table(self):
        columns, rows = _small_table()
        good = FakePbi("Good", columns, rows)
        bad = FakePbi("Bad", columns, rows, fail=True)

        async def router(dax):
            return await (bad if "'Bad'" in dax else good)(dax)

        results = await snapshot_pbi_only_tables(
            ["Bad", "Good"],
            settings=ON,
            catalog=CATALOG,
            schema=SCHEMA,
            mquery_expressions={},
            rls_tables=(),
            columns_by_table={"Bad": columns, "Good": columns},
            pbi_query=router,
            run_sql=FakeSql(),
            now=NOW,
        )
        assert [r["status"] for r in results] == ["skipped", "written"]


def test_leaf_name_is_snake_case_and_distinct_from_source_view_prefix():
    assert snapshot_leaf_name("Excel Table") == "snap_excel_table"
    assert not snapshot_leaf_name("Excel Table").startswith("src_")
