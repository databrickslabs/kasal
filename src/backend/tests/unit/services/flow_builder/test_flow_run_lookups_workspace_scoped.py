"""The flow subprocess loads only crews, tasks and agents of the run's workspace.

A flow's ``flow_config`` names crews and tasks by id (startingPoints, listeners),
and a crew's canvas names its agents. The ids come from the client, or from a
flow or crew imported from another workspace, so they are not proof of
ownership. Inside the flow SUBPROCESS every one of them is read through the
data-access bundle; a row outside the run's workspace must read as "not found":

* a crew of another workspace is skipped, as a deleted crew is;
* a task or agent of another workspace named by one of the run's OWN crews falls
  back to that crew's embedded canvas data, as a deleted row does;
* no row of another workspace reaches the build: not its prompts, not its
  tool_configs, not its MCP servers.

Rows of the run's workspace load exactly as before.

Follows test_flow_failure_status.py: a fresh interpreter runs the real child
entry, ``process_executor.run_flow_in_process``, against a SQLite file. The flow
body is real down to the build: only the agent and task CONSTRUCTORS (which
would need a model) are replaced with recorders, and the build stops right
before the flow class is created, so no crew runs.
"""

import base64
import json
import pathlib
import pickle
import subprocess
import sys
import uuid
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import src.db.all_models  # noqa: F401  (registers every table on Base)
from src.db.base import Base
from src.models.agent import Agent
from src.models.crew import Crew
from src.models.execution_history import ExecutionHistory
from src.models.task import Task

# Imported here so a broken child entry fails as an import error, not as an
# opaque traceback from a spawned interpreter.
from src.services.flow_builder import process_executor  # noqa: F401
from src.utils.encryption_utils import EncryptionUtils
from src.utils.user_context import GroupContext

BACKEND = pathlib.Path(__file__).resolve().parents[4]
JOB_ID = "flow-run-lookups"
A, B = "ws-a", "ws-b"

A_CREW = uuid.UUID("aaaaaaaa-2222-4000-8000-00000000000a")
B_CREW = uuid.UUID("bbbbbbbb-2222-4000-8000-00000000000b")
# A crew of workspace A whose canvas still names B's task and agent — what an
# import from B leaves behind.
IMPORTED_CREW = uuid.UUID("aaaaaaaa-3333-4000-8000-00000000000a")
A_TASK = "aaaaaaaa-1111-4000-8000-00000000000a"
B_TASK = "bbbbbbbb-1111-4000-8000-00000000000b"
A_AGENT = "aaaaaaaa-0000-4000-8000-00000000000a"
B_AGENT = "bbbbbbbb-0000-4000-8000-00000000000b"
B_SECRET = "b-secret-key"
#: Policy the canvas keeps under a task node's data.config.
CANVAS_POLICY = {
    "guardrail": '{"type": "minimum_number"}',
    "llm_guardrail": {"description": "cites a source", "llm_model": "m"},
    "human_input": True,
    "max_retries": 2,
}

CHILD = r"""
import base64, json, os, pickle, sys
from types import SimpleNamespace
from unittest.mock import patch

import src.services.flow_builder.process_executor as entry  # the child entry
from src.services.flow_builder.modules.agent_adapter import AgentConfig
from src.services.flow_builder.modules.flow_builder import FlowBuilder
from src.services.flow_builder.modules.flow_config import FlowConfigManager
from src.services.flow_builder.modules.task_adapter import TaskConfig

out_path = sys.argv[1]
execution_id, flow_config, inputs, group_context = pickle.loads(
    base64.b64decode(sys.stdin.read())
)
seen = {"agents": [], "tasks": [], "mcp": None, "starts": [], "listeners": []}
POLICY = ("guardrail", "llm_guardrail", "human_input", "max_retries")


def _arg(args, kwargs, name):
    return kwargs.get(name, args[0] if args else None)


async def agent(*args, **kwargs):  # what reaches the agent build
    row = _arg(args, kwargs, "agent_data")
    seen["agents"].append({
        "id": str(getattr(row, "id", None)),
        "role": getattr(row, "role", None),
        "tool_configs": getattr(row, "tool_configs", None),
        "crew_tool_configs": kwargs.get("crew_tool_configs"),
    })
    return SimpleNamespace(role=getattr(row, "role", None))


async def task(*args, **kwargs):  # what reaches the task build
    row = _arg(args, kwargs, "task_data")
    spec = TaskConfig._task_data_to_spec(row)  # what the shared builder reads
    seen["tasks"].append({
        "id": str(getattr(row, "id", None)),
        "description": getattr(row, "description", None),
        "tool_configs": getattr(row, "tool_configs", None),
        "policy": {k: spec.get(k) for k in POLICY},
    })
    return SimpleNamespace(
        agent=kwargs.get("agent"), async_execution=False, context=None,
        description=str(getattr(row, "description", "")),
    )


real_mcp = FlowConfigManager.collect_agent_mcp_requirements


async def mcp(*args, **kwargs):
    seen["mcp"] = await real_mcp(*args, **kwargs)
    return seen["mcp"]


async def built(starts, listeners, *args, **kwargs):
    seen["starts"] = sorted(str(s[4].id) for s in starts)
    seen["listeners"] = sorted(str(l[1]) for l in listeners)
    raise RuntimeError("stop: the flow is built, no crew is under test")


with patch.object(AgentConfig, "configure_agent_and_tools", agent), patch.object(
    TaskConfig, "configure_task", task
), patch.object(
    FlowConfigManager, "collect_agent_mcp_requirements", mcp
), patch.object(FlowBuilder, "_create_dynamic_flow", built):
    result = entry.run_flow_in_process(execution_id, flow_config, inputs, group_context)

seen["returned"] = result.get("status")
with open(out_path, "w") as fh:
    json.dump(seen, fh, default=str)
os._exit(0)  # as the real wrapper does: tracing threads must not hold exit
"""


def _canvas(task_id, agent_id, label):
    return {
        "nodes": [
            {
                "id": f"task-{task_id}",
                "type": "taskNode",
                "data": {
                    "label": f"{label} task",
                    "description": f"{label} canvas description",
                    "expected_output": "o",
                    "config": CANVAS_POLICY,
                },
            },
            {
                "id": f"agent-{agent_id}",
                "type": "agentNode",
                "data": {"label": label, "role": f"{label} canvas role", "goal": "g"},
            },
        ],
        "edges": [{"source": f"agent-{agent_id}", "target": f"task-{task_id}"}],
    }


def _rows():
    def agent(agent_id, group_id, secret):
        return Agent(
            id=agent_id,
            name=f"{group_id} agent",
            role=f"{group_id} row role",
            goal="g",
            group_id=group_id,
            tools=[],
            tool_configs={"SerperDevTool": {"serper_api_key": secret}},
        )

    def task(task_id, agent_id, group_id, secret):
        return Task(
            id=task_id,
            name=f"{group_id} task",
            description=f"{group_id} row description",
            expected_output="o",
            agent_id=agent_id,
            group_id=group_id,
            tools=[],
            tool_configs={
                "SerperDevTool": {"serper_api_key": secret},
                "MCP_SERVERS": {"servers": [f"{group_id}-mcp"]},
            },
        )

    def crew(crew_id, group_id, task_id, agent_id, label, secret):
        return Crew(
            id=crew_id,
            name=label,
            group_id=group_id,
            agent_ids=[agent_id],
            task_ids=[task_id],
            tool_configs={"CrewTool": {"token": secret}},
            **_canvas(task_id, agent_id, label),
        )

    return [
        agent(A_AGENT, A, "a-own-key"),
        agent(B_AGENT, B, B_SECRET),
        task(A_TASK, A_AGENT, A, "a-own-key"),
        task(B_TASK, B_AGENT, B, B_SECRET),
        crew(A_CREW, A, A_TASK, A_AGENT, "A crew", "a-crew-token"),
        crew(B_CREW, B, B_TASK, B_AGENT, "B crew", B_SECRET),
        crew(IMPORTED_CREW, A, B_TASK, B_AGENT, "Imported", "a-crew-token"),
    ]


def _child_args(context):
    flow_config = {
        "flow_id": None,
        "run_name": "Flow run",
        "nodes": [{"id": "n1", "type": "crewNode", "data": {"label": "c"}}],
        "edges": [],
        "flow_config": {
            "startingPoints": [
                {"crewId": str(A_CREW), "taskId": A_TASK},
                {"crewId": str(B_CREW), "taskId": B_TASK},
                {"crewId": str(IMPORTED_CREW), "taskId": B_TASK},
            ],
            "listeners": [
                {
                    "crewId": str(B_CREW),
                    "listenToTaskIds": [A_TASK],
                    "conditionType": "NONE",
                    "tasks": [{"id": B_TASK}],
                }
            ],
        },
        "inputs": {"topic": "t"},
        "model": None,
        "group_context": context,
        "group_id": context.primary_group_id,
        "execution_id": JOB_ID,
    }
    return JOB_ID, flow_config, {"topic": "t"}, context


def _database(path):
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(_rows())
        session.add(  # the row the API writes before it queues the run
            ExecutionHistory(
                job_id=JOB_ID,
                status="RUNNING",
                group_id=A,
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


SCENARIOS = {
    "workspace_a": GroupContext(group_ids=[A], group_email="a@example.com"),
}


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    """Each scenario through the real child entry, side by side."""
    launched = {}
    for name, context in SCENARIOS.items():
        workdir = tmp_path_factory.mktemp(name)
        # Tool configs are encrypted on write: seed with the key pair the child
        # finds under its HOME, so it can decrypt what the parent stored.
        key_dir = workdir / ".backendcrew" / "keys"
        key_dir.mkdir(parents=True)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(EncryptionUtils, "get_key_directory", lambda: key_dir)
            _database(workdir / "app.db")
        out = workdir / "child.json"
        process = subprocess.Popen(
            [sys.executable, "-c", CHILD, str(out)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=BACKEND,
            env=_env(workdir),
        )
        payload = base64.b64encode(pickle.dumps(_child_args(context))).decode()
        launched[name] = (process, payload, out)

    seen = {}
    for name, (process, payload, out) in launched.items():
        try:
            _, stderr = process.communicate(input=payload, timeout=240)
        except subprocess.TimeoutExpired:
            process.kill()  # never leave the spawned interpreter running
            process.communicate()
            raise
        assert out.exists(), (process.returncode, stderr[-3000:])
        seen[name] = json.loads(out.read_text())
    return seen


class TestAnotherWorkspacesRows:
    def test_its_crew_is_skipped_as_a_starting_point_and_as_a_listener(self, runs):
        seen = runs["workspace_a"]
        assert seen["starts"] == sorted([str(A_CREW), str(IMPORTED_CREW)])
        assert seen["listeners"] == []

    def test_none_of_its_prompts_reach_the_build(self, runs):
        seen = runs["workspace_a"]
        roles = {a["role"] for a in seen["agents"]}
        descriptions = {t["description"] for t in seen["tasks"]}
        assert f"{B} row role" not in roles
        assert f"{B} row description" not in descriptions

    def test_none_of_its_tool_configs_or_mcp_servers_reach_the_build(self, runs):
        seen = runs["workspace_a"]
        assert B_SECRET not in json.dumps(seen)
        assert f"{B}-mcp" not in json.dumps(seen["mcp"])
        assert B_AGENT not in seen["mcp"]

    def test_an_own_crew_naming_them_falls_back_to_its_canvas(self, runs):
        seen = runs["workspace_a"]
        assert "Imported canvas role" in {a["role"] for a in seen["agents"]}
        assert "Imported canvas description" in {
            t["description"] for t in seen["tasks"]
        }

    def test_the_canvas_fallback_keeps_the_tasks_guardrails(self, runs):
        # Code guardrail, LLM guardrail, human input and retries live under
        # the node's data.config; a fallback without them runs unchecked.
        (fallback,) = [
            t
            for t in runs["workspace_a"]["tasks"]
            if t["description"] == "Imported canvas description"
        ]
        assert fallback["policy"] == CANVAS_POLICY


class TestTheRunsOwnWorkspace:
    def test_its_rows_load_as_before(self, runs):
        seen = runs["workspace_a"]
        own = [a for a in seen["agents"] if a["id"] == A_AGENT]
        assert own and own[0]["role"] == f"{A} row role"
        assert own[0]["tool_configs"] == {
            "SerperDevTool": {"serper_api_key": "a-own-key"}
        }
        assert own[0]["crew_tool_configs"]["CrewTool"] == {"token": "a-crew-token"}
        assert f"{A} row description" in {t["description"] for t in seen["tasks"]}
        assert seen["mcp"][A_AGENT] == [f"{A}-mcp"]
