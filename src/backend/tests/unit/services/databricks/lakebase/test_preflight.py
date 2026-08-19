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
    # The fix is the UI "Drop role → Reassign owned objects" flow, not SQL.
    steps = "\n".join(rem["steps"])
    assert "Drop role" in steps
    assert "Reassign owned" in steps
    assert "old_sp" in steps  # names the orphaned owner role to drop
    assert "new_sp" in steps  # reassign target = this app's SP
    # No SQL is suggested (every SQL-editor path fails on Lakebase here).
    assert rem["commands"] == []
    assert "REASSIGN OWNED" not in steps and "ALTER TABLE" not in steps


@pytest.mark.asyncio
async def test_action_required_user_owner_gets_sql_recipe(small_schema):
    # When the owner is a USER account (email) you CAN log in as, the remediation
    # is the SQL recipe (not the UI drop-role, which is for app-SP owners).
    conn = _FakeConn(
        user="new_sp",
        has_vec=True,
        owners=[("executionhistory", "person@databricks.com"), ("agents", "new_sp")],
        cols=[("executionhistory", "id"), ("agents", "id"), ("agents", "name")],
    )
    report = await pf.run_lakebase_preflight(conn)
    assert report["status"] == pf.STATUS_ACTION_REQUIRED
    rem = report["remediation"]
    joined = "\n".join(rem["commands"])
    assert "CREATE ROLE kasal_shared_owner" in joined
    assert 'GRANT kasal_shared_owner TO "new_sp" WITH INHERIT TRUE;' in joined
    assert "REASSIGN OWNED BY CURRENT_USER TO kasal_shared_owner;" in joined
    # Not the UI drop-role path for a human owner.
    assert "Drop role" not in "\n".join(rem["steps"])


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
async def test_enable_blocked_when_preflight_not_healthy(monkeypatch):
    """The connect gate: enable_lakebase must NOT flip 'enabled' when the preflight
    is not healthy — it returns success=False with the remediation instead."""
    from src.services.databricks.lakebase.service import LakebaseService

    svc = LakebaseService(session=None)

    saved = {}

    async def fake_get_config():
        return {"enabled": False}

    async def fake_save_config(cfg):
        saved.update(cfg)

    monkeypatch.setattr(svc, "get_config", fake_get_config)
    monkeypatch.setattr(svc, "save_config", fake_save_config)

    async def fake_preflight(service, instance_name=None):
        return {
            "status": pf.STATUS_ACTION_REQUIRED,
            "remediation": {"summary": "fix ownership"},
        }

    monkeypatch.setattr(pf, "preflight_via_service", fake_preflight)

    result = await svc.enable_lakebase("inst", "endpoint")
    assert result["success"] is False
    assert result["preflight"]["status"] == pf.STATUS_ACTION_REQUIRED
    assert saved == {}  # config was NOT saved — Lakebase not connected


@pytest.mark.asyncio
async def test_connectivity_error_reported(small_schema):
    class _Boom:
        async def execute(self, *a, **k):
            raise RuntimeError("connection refused")

    report = await pf.run_lakebase_preflight(_Boom())
    assert report["status"] == pf.STATUS_ERROR
    assert any(not c["ok"] for c in report["checks"])
