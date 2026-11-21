"""The drift check's semantic half: snapshot store, scope routing, ignores and LLM proposals (all faked)."""

import json

from src.services.powerbi.scope.semantic_sync import make
from src.services.tools.metric_view_utils.drift import semantic_proposals
from src.services.tools.metric_view_utils.drift.semantic_step import (
    run_semantic_sync,
    scope_rows,
)

VIEW_YAML = """version: 1.1
source: sales.fact
measures:
  - name: sales
    expr: SUM(source.sales)
"""


def _snapshot(dax="SUM(Fact[x])", loaded="20260101"):
    objects = [
        make("measure", "Fact", "Sales", dax),
        make("expression", "", "LoadDate", loaded),
    ]
    return {
        "taken_at_utc": "2026-11-04T05:00:00Z",
        "objects": [vars(o) for o in objects],
    }


SCOPE = [
    {
        "object_type": "measure",
        "pbi_table": "Fact",
        "name": "Sales",
        "decision": "keep",
        "mirrored_as": "ucm_fact.sales",
    }
]
DRIFT = {"views": [{"name": "ucm_fact", "baseline_yaml": VIEW_YAML}]}


class FakeSql:
    """A snapshot table in memory; records every statement."""

    def __init__(self, stored=None):
        self.statements = []
        self.stored = stored

    def __call__(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT"):
            rows = [[json.dumps(self.stored)]] if self.stored else []
            return {"success": True, "data": {"result": {"data_array": rows}}}
        return {"success": True, "data": {}}


def test_first_run_against_an_empty_table_stores_the_baseline():
    sql = FakeSql()
    out = run_semantic_sync(
        _snapshot(),
        sql_fn=sql,
        snapshot_table="main.ops.pbi_semantic_object",
        dataset_id="d1",
    )
    assert out["status"] == "baseline_stored" and out["changes"] == []
    assert sql.statements[0].startswith(
        "CREATE TABLE IF NOT EXISTS `main`.`ops`.`pbi_semantic_object`"
    )
    assert "WHERE dataset_id = 'd1'" in sql.statements[1]
    assert sql.statements[-1].startswith("INSERT INTO")


def test_changes_are_routed_ignored_and_proposed_and_the_new_snapshot_stored():
    sql = FakeSql(stored=_snapshot())
    prompts = []

    def complete(system, user):
        prompts.append(user)
        return json.dumps(
            {
                "verdict": "update",
                "summary": "x2",
                "proposed_definition": "SUM(source.sales) * 2",
            }
        )

    out = run_semantic_sync(
        _snapshot(dax="SUM(Fact[x]) * 2", loaded="20260202"),
        scope=SCOPE,
        ignore=json.dumps(
            [
                {
                    "object_type": "expression",
                    "name": "LoadDate",
                    "reason": "set by every load",
                }
            ]
        ),
        drift_report=DRIFT,
        complete_fn=complete,
        sql_fn=sql,
        snapshot_table="main.ops.pbi_semantic_object",
        dataset_id="d1",
    )
    by_name = {c["name"]: c for c in out["changes"]}
    assert by_name["Sales"]["action"] == "update the mirror: ucm_fact.sales"
    assert by_name["Sales"]["proposal"]["verdict"] == "update"
    assert by_name["LoadDate"]["action"] == "none (ignored: set by every load)"
    assert "proposal" not in by_name["LoadDate"]
    assert (
        len(prompts) == 1 and "expr: SUM(source.sales)" in prompts[0]
    )  # deployed counterpart
    assert out["counts"]["needing_action"] == 1
    assert sql.statements[-1].startswith("INSERT INTO")


def test_without_a_table_the_baseline_comes_in_and_the_snapshot_goes_back():
    out = run_semantic_sync(
        _snapshot(dax="SUM(Fact[y])"), baseline=json.dumps(_snapshot())
    )
    assert out["status"] == "compared"
    assert out["snapshot"]["objects"]
    assert out["changes"][0]["action"].startswith("triage: not in the scope table")


def test_without_a_signed_off_scope_todays_draft_routes_the_changes():
    out = run_semantic_sync(
        _snapshot(dax="SUM(Fact[y])"), baseline=_snapshot(), draft_scope={"rows": SCOPE}
    )
    assert out["changes"][0]["action"] == "update the mirror: ucm_fact.sales"
    assert any("scope draft" in w for w in out["warnings"])


def test_a_bad_table_name_and_a_missing_snapshot_are_warnings_not_errors():
    assert run_semantic_sync({})["status"] == "skipped"
    out = run_semantic_sync(
        _snapshot(), snapshot_table="main.ops.x; DROP TABLE y", sql_fn=FakeSql()
    )
    assert any("not a catalog.schema.table" in w for w in out["warnings"])


def test_scope_rows_read_csv_json_and_rows_objects():
    csv_text = "object_type,pbi_table,name,decision,mirrored_as\nmeasure,Fact,Sales,keep,ucm_fact.sales\n"
    assert scope_rows(csv_text)[0]["mirrored_as"] == "ucm_fact.sales"
    assert scope_rows(json.dumps(SCOPE)) == SCOPE
    assert scope_rows({"rows": SCOPE}) == SCOPE
    assert scope_rows(None) == []


def test_proposals_are_parsed_limited_and_never_raise():
    calls = []

    def complete(system, user):
        calls.append(user)
        if len(calls) == 2:
            raise RuntimeError("endpoint down")
        return "Here you go:\n" + json.dumps(
            {"verdict": "update", "summary": "s", "extra": "ignored"}
        )

    change = {
        "change_type": "changed",
        "object_type": "measure",
        "pbi_table": "Fact",
        "action": "update the mirror: x",
    }
    changes = [
        {**change, "name": "A"},
        {**change, "name": "B"},
        {**change, "name": "C", "action": "none (not mirrored)"},
        {**change, "name": "D"},
    ]
    semantic_proposals.propose(changes, {}, complete, limit=2)
    assert (
        changes[0]["proposal"]["verdict"] == "update"
        and "extra" not in changes[0]["proposal"]
    )
    assert (
        changes[1]["proposal"]["verdict"] == "needs_human"
        and "endpoint down" in changes[1]["proposal"]["summary"]
    )
    assert "proposal" not in changes[2] and "proposal" not in changes[3]
    assert (
        semantic_proposals.parse_proposal('{"verdict": "delete everything"}')["verdict"]
        == "needs_human"
    )
    assert semantic_proposals.parse_proposal("no json")["verdict"] == "needs_human"
