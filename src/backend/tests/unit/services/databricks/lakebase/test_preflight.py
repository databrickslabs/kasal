"""Unit tests for the Lakebase preflight diagnostic."""

import pytest

from src.services.databricks.lakebase import preflight as pf


class _Result:
    def __init__(self, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = rows or []

    def scalar(self):
        return self._scalar

    def fetchall(self):
        return self._rows


class _FakeConn:
    """Answers the four queries run_lakebase_preflight issues, by SQL text."""

    def __init__(self, user, has_vec, owners, cols):
        self.user = user
        self.has_vec = has_vec
        self.owners = owners  # list[(table, owner)]
        self.cols = cols  # list[(table, column)]

    async def execute(self, stmt, params=None):
        s = str(stmt)
        if "current_user" in s:
            return _Result(scalar=self.user)
        if "pg_extension" in s:
            return _Result(scalar=1 if self.has_vec else None)
        if "pg_tables" in s:
            return _Result(rows=self.owners)
        if "information_schema.columns" in s:
            return _Result(rows=self.cols)
        return _Result()


@pytest.fixture
def small_schema(monkeypatch):
    """Patch the expected schema so the test doesn't depend on the real models."""
    monkeypatch.setattr(
        pf,
        "_expected_schema",
        lambda: {"executionhistory": {"id", "harness"}, "agents": {"id", "name"}},
    )


@pytest.mark.asyncio
async def test_healthy_when_owned_and_complete(small_schema):
    conn = _FakeConn(
        user="sp1",
        has_vec=True,
        owners=[("executionhistory", "sp1"), ("agents", "sp1")],
        cols=[
            ("executionhistory", "id"),
            ("executionhistory", "harness"),
            ("agents", "id"),
            ("agents", "name"),
        ],
    )
    report = await pf.run_lakebase_preflight(conn)
    assert report["status"] == pf.STATUS_HEALTHY
    assert report["remediation"] is None
    assert report["current_user"] == "sp1"


@pytest.mark.asyncio
async def test_action_required_when_orphaned_table_missing_column(small_schema):
    # executionhistory is owned by a DIFFERENT (old) SP and is missing 'harness'.
    conn = _FakeConn(
        user="new_sp",
        has_vec=True,
        owners=[("executionhistory", "old_sp"), ("agents", "new_sp")],
        cols=[
            ("executionhistory", "id"),  # no 'harness'
            ("agents", "id"),
            ("agents", "name"),
        ],
    )
    report = await pf.run_lakebase_preflight(conn)
    assert report["status"] == pf.STATUS_ACTION_REQUIRED
    rem = report["remediation"]
    assert rem is not None
    assert "executionhistory" in rem["summary"]
    assert "old_sp" in rem["summary"] and "new_sp" in rem["summary"]
    # Reference SQL targets the orphaned owner and reassigns to the current SP.
    joined = "\n".join(rem["commands"])
    assert 'REASSIGN OWNED BY "old_sp" TO "new_sp"' in joined
    assert 'ALTER TABLE "executionhistory" OWNER TO "new_sp"' in joined


@pytest.mark.asyncio
async def test_auto_fixable_when_owned_table_missing_column(small_schema):
    # Missing column but the current SP OWNS the table -> self-heal can add it,
    # so it is NOT a blocker (status stays healthy).
    conn = _FakeConn(
        user="sp1",
        has_vec=True,
        owners=[("executionhistory", "sp1"), ("agents", "sp1")],
        cols=[
            ("executionhistory", "id"),  # missing 'harness' but owned by sp1
            ("agents", "id"),
            ("agents", "name"),
        ],
    )
    report = await pf.run_lakebase_preflight(conn)
    assert report["status"] == pf.STATUS_HEALTHY
    eh = next(t for t in report["tables"] if t["name"] == "executionhistory")
    assert eh["status"] == "auto_fixable"
    assert eh["missing_columns"] == ["harness"]


@pytest.mark.asyncio
async def test_connectivity_error_reported(small_schema):
    class _Boom:
        async def execute(self, *a, **k):
            raise RuntimeError("connection refused")

    report = await pf.run_lakebase_preflight(_Boom())
    assert report["status"] == pf.STATUS_ERROR
    assert any(not c["ok"] for c in report["checks"])
