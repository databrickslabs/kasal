"""A flow that fails is stored and announced as FAILED, never as COMPLETED.

``FlowRunnerService.run_flow`` reports a failed flow by RETURNING
``{"status": "failed", "error": ...}`` rather than raising. The subprocess used
to announce COMPLETED for anything that was not a HITL pause, so a failed run
was stored COMPLETED with its error envelope as the answer, fired "completed"
triggers and told A2A subscribers and webhooks it had completed. The parent's
later FAILED write was then dropped as "already settled".

Each scenario follows a run across BOTH real interpreters
(services/execution/CLAUDE.md: in-process tests can pass while the spawned
interpreter cannot):

1. a fresh interpreter runs the real child entry,
   ``process_executor.run_flow_in_process``, against a SQLite file. Only the
   flow BODY (``_run_dynamic_flow``) is replaced, with one that does what the
   real body does: write its lowercase status onto the row, then return its
   outcome. ``run_flow``, the announcement and the status writer are real;
2. a second fresh interpreter runs the real parent finalisation,
   ``flow_execution_runner.run_flow_in_process``, handed what the child
   returned, against the same file.

What each half announced (SSE), notified (A2A / webhooks) and emitted
(event triggers) is recorded at the point it leaves Kasal.
"""

import base64
import json
import pathlib
import pickle
import subprocess
import sys
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import src.db.all_models  # noqa: F401  (registers every table on Base)
from src.db.base import Base
from src.models.execution_history import ExecutionHistory

# Imported here so a broken child entry fails as an import error, not as an
# opaque traceback from a spawned interpreter.
from src.services.flow_builder import process_executor  # noqa: F401
from src.utils.user_context import GroupContext

BACKEND = pathlib.Path(__file__).resolve().parents[4]
JOB_ID = "flow-failure-status"
WS = "ws-a"
ERROR = "Crew 'Research' timed out after 1200 seconds"

#: The status the real flow body writes onto the row, and what it returns.
OUTCOMES = {
    "failed": ("failed", {"success": False, "error": ERROR}),
    "completed": ("completed", {"success": True, "result": {"content": "answer"}}),
    "paused": (
        "waiting_for_approval",
        {
            "success": True,
            "paused_for_approval": True,
            "hitl_paused": True,
            "approval_id": 7,
            "gate_node_id": "gate-1",
            "message": "Approve the draft?",
        },
    ),
}

_RECORDERS = r"""
seen = {"announced": [], "notified": [], "emitted": []}

def notify(job_id, status, result=None):  # A2A subscribers and webhooks
    seen["notified"].append(status)

async def emit(**kwargs):  # event triggers
    seen["emitted"].append(kwargs["event_type"])

def row():
    from sqlalchemy import create_engine, text
    engine = create_engine("sqlite:///" + os.environ["SQLITE_DB_PATH"])
    with engine.connect() as conn:
        status, error, result = conn.execute(
            text("SELECT status, error, result FROM executionhistory WHERE job_id = :j"),
            {"j": execution_id},
        ).one()
    return {
        "status": status,
        "error": error,
        "result": json.loads(result) if isinstance(result, str) else result,
    }
"""

CHILD = (
    r"""
import base64, json, os, pickle, sys
from unittest.mock import patch

import src.services.flow_builder.process_executor as entry  # the child entry
from src.services.execution.event_pipe import EventPipeWriter
from src.services.flow_builder.flow_runner_service import FlowRunnerService

out_path = sys.argv[1]
execution_id, flow_config, inputs, group_context, outcome = pickle.loads(
    base64.b64decode(sys.stdin.read())
)
row_status, returned = outcome
"""
    + _RECORDERS
    + r"""
async def body(self, execution_id, job_id, config):
    await self.flow_execution_service.update_execution_status(
        execution_id=execution_id, status=row_status, error=returned.get("error")
    )
    return {**returned, "execution_id": execution_id}

def relay(self, frame):  # the live status the parent broadcasts on SSE
    if frame.get("kind") == "execution_update":
        seen["announced"].append(frame["status"])

with patch.object(FlowRunnerService, "_run_dynamic_flow", body), patch.object(
    EventPipeWriter, "_put", relay
), patch("src.services.external.notify.notify_state_change", notify), patch(
    "src.services.triggers.emit_for_completed_run", emit
):
    result = entry.run_flow_in_process(execution_id, flow_config, inputs, group_context)

seen["row"] = row()
seen["returned"] = result.get("status")
with open(out_path + ".result", "wb") as fh:
    pickle.dump(result, fh)
with open(out_path, "w") as fh:
    json.dump(seen, fh, default=str)
os._exit(0)  # as the real wrapper does: tracing threads must not hold exit
"""
)

PARENT = (
    r"""
import asyncio, json, os, pickle, sys
from unittest.mock import patch

from src.core.sse_manager import sse_manager
from src.services.flow_builder import flow_execution_runner as runner

child_path, out_path, execution_id = sys.argv[1:4]
with open(child_path, "rb") as fh:
    child_result = pickle.load(fh)
"""
    + _RECORDERS
    + r"""
async def isolated(**kwargs):  # the child has already run: hand back its result
    return child_result

async def broadcast(job_id, event, group_id=None):
    seen["announced"].append(event.data["status"])
    return 1

async def unchanged(result, **kwargs):  # A2UI composition is not under test
    return result

with patch.object(runner.process_flow_executor, "run_flow_isolated", isolated), patch.object(
    sse_manager, "broadcast_to_job", broadcast
), patch("src.services.a2ui.runner.wrap_result_with_surface", unchanged), patch(
    "src.services.external.notify.notify_state_change", notify
), patch(
    "src.services.triggers.emit_for_completed_run", emit
):
    asyncio.run(runner.run_flow_in_process(execution_id, {"inputs": {"topic": "t"}}, {}))

seen["row"] = row()
with open(out_path, "w") as fh:
    json.dump(seen, fh, default=str)
os._exit(0)
"""
)


def _child_args():
    """The arguments the parent hands the flow subprocess."""
    context = GroupContext(group_ids=[WS], group_email="a@example.com")
    flow_config = {
        "flow_id": None,
        "run_name": "Flow run",
        "nodes": [{"id": "n1", "type": "crewNode", "data": {"label": "c"}}],
        "edges": [],
        "flow_config": {},
        "inputs": {"topic": "t"},
        "model": None,
        "group_context": context,
        "group_id": WS,
        "execution_id": JOB_ID,
    }
    return JOB_ID, flow_config, {"topic": "t"}, context


def _database(path):
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        # The row the API writes before it queues the run.
        session.add(
            ExecutionHistory(
                job_id=JOB_ID,
                status="RUNNING",
                group_id=WS,
                execution_type="flow",
                created_at=datetime(2026, 10, 1, 12, 0, 0),
                inputs={"nodes": [{"id": "n1"}], "execution_type": "flow"},
            )
        )
        session.commit()
    engine.dispose()


def _env(workdir):
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": str(workdir),
        "DATABASE_TYPE": "sqlite",
        "SQLITE_DB_PATH": str(workdir / "app.db"),
        "LOG_DIR": str(workdir / "logs"),
        "PYTHONPATH": str(BACKEND),
    }


def _launch(script, args, workdir, stdin=None):
    return (
        subprocess.Popen(
            [sys.executable, "-c", script, *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=BACKEND,
            env=_env(workdir),
        ),
        stdin,
    )


def _finish(launched, out):
    process, stdin = launched
    try:
        _, stderr = process.communicate(input=stdin, timeout=240)
    except subprocess.TimeoutExpired:
        process.kill()  # never leave the spawned interpreter running
        process.communicate()
        raise
    assert out.exists(), (process.returncode, stderr[-3000:])
    return json.loads(out.read_text())


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    """Every scenario through the child, then the parent; run side by side."""
    work = {name: tmp_path_factory.mktemp(name) for name in OUTCOMES}
    for name, workdir in work.items():
        _database(workdir / "app.db")

    children = {}
    for name, workdir in work.items():
        payload = base64.b64encode(
            pickle.dumps((*_child_args(), OUTCOMES[name]))
        ).decode()
        out = workdir / "child.json"
        children[name] = (_launch(CHILD, [str(out)], workdir, payload), out)
    child_seen = {name: _finish(*children[name]) for name in work}

    parents = {}
    for name, workdir in work.items():
        out = workdir / "parent.json"
        args = [str(workdir / "child.json.result"), str(out), JOB_ID]
        parents[name] = (_launch(PARENT, args, workdir, ""), out)
    return {
        name: {"child": child_seen[name], "parent": _finish(*parents[name])}
        for name in work
    }


class TestAFailedFlow:
    def test_the_child_stores_it_as_failed_with_its_error(self, runs):
        row = runs["failed"]["child"]["row"]
        assert row["status"] == "FAILED"
        assert row["error"] == ERROR

    def test_its_error_envelope_is_not_stored_as_the_answer(self, runs):
        assert ERROR not in json.dumps(runs["failed"]["child"]["row"]["result"])

    def test_the_child_announces_failed(self, runs):
        assert runs["failed"]["child"]["announced"] == ["FAILED"]
        assert runs["failed"]["child"]["returned"] == "FAILED"

    def test_it_stays_failed_after_the_parent_finalises(self, runs):
        row = runs["failed"]["parent"]["row"]
        assert row["status"] == "FAILED"
        assert row["error"] == ERROR

    @pytest.mark.parametrize("half", ["child", "parent"])
    def test_no_completion_trigger_or_webhook_fires(self, runs, half):
        seen = runs["failed"][half]
        assert "completed" not in seen["emitted"]
        assert "COMPLETED" not in seen["notified"]
        assert "COMPLETED" not in seen["announced"]


class TestACompletedFlow:
    def test_it_is_still_stored_completed_with_its_answer(self, runs):
        for half in ("child", "parent"):
            row = runs["completed"][half]["row"]
            assert row["status"] == "COMPLETED", half
            assert "answer" in json.dumps(row["result"]), half

    def test_it_is_still_announced_completed(self, runs):
        assert runs["completed"]["child"]["announced"] == ["COMPLETED"]
        assert runs["completed"]["child"]["returned"] == "COMPLETED"


class TestAFlowPausedForApproval:
    def test_no_half_writes_a_status_over_the_pause(self, runs):
        for half in ("child", "parent"):
            assert runs["paused"][half]["row"]["status"] == "waiting_for_approval"

    def test_nothing_is_announced_notified_or_emitted(self, runs):
        for half in ("child", "parent"):
            seen = runs["paused"][half]
            assert seen["announced"] == seen["notified"] == seen["emitted"] == []

    def test_the_child_still_reports_the_pause(self, runs):
        assert runs["paused"]["child"]["returned"] == "WAITING_FOR_APPROVAL"
