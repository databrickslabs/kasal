"""The flow subprocess still runs as the caller, and stores no token doing it.

Follows a run across a real interpreter boundary (services/execution/CLAUDE.md:
in-process tests can pass while the spawned interpreter cannot). The parent
half is the real ``ProcessFlowExecutor._run_admitted``: it is stopped at the
spawn and its arguments are pickled, as ``multiprocessing`` spawn would do. A
fresh interpreter then runs the real child entry, ``run_flow_in_process``,
against a SQLite file. Only the flow BODY is replaced, to record what it saw;
everything before it (bootstrap, database, tracing setup, the flow runner and
the row write) is real.
"""

import base64
import json
import pathlib
import pickle
import subprocess
import sys
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import src.db.all_models  # noqa: F401  (registers every table on Base)
from src.db.base import Base
from src.models.execution_history import ExecutionHistory
from src.services.flow_builder.process_executor import ProcessFlowExecutor
from src.utils.user_context import GroupContext

BACKEND = pathlib.Path(__file__).resolve().parents[4]
TOKEN = "caller-obo-token-value"
JOB_ID = "flow-subprocess-token"
WS = "ws-a"

CHILD = r"""
import asyncio, base64, json, os, pickle, sys
from unittest.mock import patch

import src.services.flow_builder.process_executor as entry  # the child entry
from src.services.flow_builder.flow_runner_service import FlowRunnerService
from src.utils.user_context import UserContext

out_path = sys.argv[1]
execution_id, flow_config, inputs, group_context = pickle.loads(
    base64.b64decode(sys.stdin.read())
)
seen = {}

async def body(self, execution_id, job_id, config):
    seen["config_token"] = config.get("user_token")
    seen["context_token"] = UserContext.get_user_token()
    return {"success": True, "result": {"content": "ok"}}

with patch.object(FlowRunnerService, "_run_dynamic_flow", body):
    result = entry.run_flow_in_process(execution_id, flow_config, inputs, group_context)

from sqlalchemy import create_engine, text
engine = create_engine("sqlite:///" + os.environ["SQLITE_DB_PATH"])
with engine.connect() as conn:
    stored = conn.execute(
        text("SELECT inputs FROM executionhistory WHERE job_id = :j"),
        {"j": execution_id},
    ).scalar_one()
seen["stored"] = json.loads(stored) if isinstance(stored, str) else stored
seen["status"] = result.get("status") if isinstance(result, dict) else str(result)
with open(out_path, "w") as fh:
    json.dump(seen, fh)
os._exit(0)  # as the real wrapper does: tracing threads must not hold exit
"""


class _Spawned(Exception):
    """Stops the parent at the spawn, once it has built the child's arguments."""


def _spawn_args():
    """The arguments the real parent hands the flow subprocess."""
    executor = ProcessFlowExecutor()
    captured = {}

    def process(target, args, daemon):
        captured["args"] = args
        raise _Spawned()

    executor._ctx = MagicMock()
    executor._ctx.Process.side_effect = process
    flow_config = {
        "flow_id": None,
        "run_name": "Flow run",
        "nodes": [{"id": "n1", "type": "crewNode", "data": {"label": "c"}}],
        "edges": [],
        "flow_config": {},
        "inputs": {"topic": "t"},
        "model": None,
        "group_context": GroupContext(
            group_ids=[WS], group_email="a@example.com", access_token=TOKEN
        ),
        "group_id": WS,
    }
    ctx = flow_config["group_context"]
    return executor, flow_config, ctx, captured


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


@pytest.mark.asyncio
async def test_the_flow_subprocess_runs_on_its_token_and_stores_none(tmp_path):
    executor, flow_config, ctx, captured = _spawn_args()
    with pytest.raises(_Spawned):
        await executor._run_admitted(JOB_ID, flow_config, ctx, {"topic": "t"}, 60)
    execution_id, config, inputs, group_context = captured["args"][:4]
    assert config["user_token"] == TOKEN  # the parent hands it over
    payload = base64.b64encode(
        pickle.dumps((execution_id, config, inputs, group_context))
    ).decode()

    _database(tmp_path / "app.db")
    out = tmp_path / "seen.json"
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
        "DATABASE_TYPE": "sqlite",
        "SQLITE_DB_PATH": str(tmp_path / "app.db"),
        "LOG_DIR": str(tmp_path / "logs"),
        "PYTHONPATH": str(BACKEND),
    }
    child = subprocess.run(
        [sys.executable, "-c", CHILD, str(out)],
        input=payload,
        capture_output=True,
        text=True,
        cwd=BACKEND,
        env=env,
        timeout=240,
    )
    assert out.exists(), (child.returncode, child.stderr[-3000:])
    seen = json.loads(out.read_text())

    # The child still authenticates as the caller ...
    assert seen["config_token"] == TOKEN
    assert seen["context_token"] == TOKEN
    # ... and the row it wrote holds no credential.
    stored = seen["stored"]
    assert stored["inputs"] == {"topic": "t"}
    for key in ("user_token", "access_token", "group_context"):
        assert key not in stored, key
    assert TOKEN not in json.dumps(stored)
