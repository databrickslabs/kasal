"""A secret stored encrypted reaches the tool as plaintext, in a real child run.

Tool configs are encrypted in the database and decrypted by their column type
on load. Nothing else decrypts them: the Flow Builder child reads crew, task and
agent rows through repositories, and ToolFactory hands configs to tools as it
gets them. So the only proof that a Power BI tool still authenticates is a run
in a spawned interpreter (services/execution/CLAUDE.md), the same shape as
test_flow_failure_status.py:

- the test writes a crew whose task uses the Power BI DAX Executor with a
  service-principal secret, through the ORM, so the row holds ciphertext;
- a fresh interpreter runs the real child entry, the Flow Builder's
  ``run_flow_in_process`` or the Agent Builder's ``run_crew_in_process``, on the
  Kasal harness;
- only the edges are faked: the LLM (it calls the tool, as a model would), the
  Azure AD credential and the Power BI HTTP call. Both record what they got.

The crew run's config is built the way a saved crew is launched by id
(``build_crew_execution_config_by_id`` then ``adapt_config``), in this process.
"""

import asyncio
import base64
import json
import pathlib
import pickle
import subprocess
import sys
import uuid
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import src.db.all_models  # noqa: F401  (registers every table on Base)
from src.db.base import Base
from src.models.agent import Agent
from src.models.crew import Crew
from src.models.execution_history import ExecutionHistory
from src.models.group_tool import GroupTool
from src.models.task import Task
from src.models.tool import Tool

# Imported here so a broken child entry fails as an import error, not as an
# opaque traceback from a spawned interpreter.
from src.services.flow_builder import process_executor  # noqa: F401
from src.utils.encryption_utils import EncryptionUtils
from src.utils.user_context import GroupContext

BACKEND = pathlib.Path(__file__).resolve().parents[4]
WS = "ws-pbi"
SECRET = "sp-secret-value-1234567890"
TOOL = "Power BI DAX Executor"
CREW_ID = uuid.UUID("7d4f2a9e-1c3b-4e5f-8a6b-0c1d2e3f4a5b")
PBI = {
    TOOL: {
        "workspace_id": "pbi-workspace",
        "dataset_id": "pbi-dataset",
        "tenant_id": "tenant-1",
        "client_id": "client-1",
        "client_secret": SECRET,
        "auth_method": "service_principal",
        "dax_query": 'EVALUATE ROW("ok", 1)',
    }
}

#: Faked edges, shared by both children. ``seen`` is what they recorded.
_FAKES = r"""
from types import SimpleNamespace
from unittest.mock import patch

seen = {"client_secrets": [], "llm_calls": 0, "tool_outputs": [], "posts": 0}


class FakeCredential:  # azure.identity.ClientSecretCredential
    def __init__(self, tenant_id, client_id, client_secret, **_):
        seen["client_secrets"].append(client_secret)

    def get_token(self, *scopes, **_):
        return SimpleNamespace(token="fake-token", expires_on=0)


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"results": [{"tables": [{"rows": []}]}]}


async def fake_post(self, url, *args, **kwargs):  # the executeQueries call
    if "executeQueries" not in str(url):
        raise RuntimeError("unexpected network call in a test")
    seen["posts"] += 1
    return FakeResponse()


class FakeLLM:
    # The Kasal transport runs the tool loop inside call(): call the tool the
    # way a model's tool call would, then answer.
    model = "fake-model"

    def call(self, messages, tools=None, available_functions=None):
        seen["llm_calls"] += 1
        for name, run_tool in (available_functions or {}).items():
            if "dax" in name.lower():
                seen["tool_outputs"].append(str(run_tool())[:300])
        return "done"


async def fake_llm(*args, **kwargs):
    return FakeLLM()


def fakes():
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(
        patch(
            "src.services.converters.formats.powerbi.authentication."
            "ClientSecretCredential",
            FakeCredential,
        )
    )
    stack.enter_context(patch("httpx.AsyncClient.post", fake_post))
    stack.enter_context(
        patch("src.services.llm.manager.LLMManager.configure_kasal_llm", fake_llm)
    )
    return stack


def finish(out_path, result):
    seen["status"] = (result or {}).get("status")
    with open(out_path, "w") as fh:
        json.dump(seen, fh, default=str)
    os._exit(0)  # as the real wrapper does: tracing threads must not hold exit
"""

FLOW_CHILD = (
    r"""
import base64, json, os, pickle, sys

import src.services.flow_builder.process_executor as entry

out_path = sys.argv[1]
execution_id, flow_config, inputs, group_context = pickle.loads(
    base64.b64decode(sys.stdin.read())
)
"""
    + _FAKES
    + r"""
with fakes():
    result = entry.run_flow_in_process(execution_id, flow_config, inputs, group_context)
finish(out_path, result)
"""
)

CREW_CHILD = (
    r"""
import base64, json, os, pickle, sys

import src.services.agent_builder.process_executor as entry

out_path = sys.argv[1]
execution_id, crew_config, inputs, group_context = pickle.loads(
    base64.b64decode(sys.stdin.read())
)
"""
    + _FAKES
    + r"""
with fakes():
    result = entry.run_crew_in_process(execution_id, crew_config, inputs, group_context)
finish(out_path, result)
"""
)


def _context():
    return GroupContext(
        group_ids=[WS], group_email="a@example.com", email_domain="example.com"
    )


def _seed(path, job_ids):
    """The saved crew, written through the ORM: its secrets go in encrypted."""
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(engine)
    node_data = {"label": "extract", "tool_configs": PBI, "tools": []}
    with Session(engine) as session:
        tool = Tool(title=TOOL, description="d", icon="i", config={}, enabled=True)
        session.add(tool)
        session.flush()
        node_data["tools"] = [str(tool.id)]
        session.add_all(
            [
                GroupTool(tool_id=tool.id, group_id=WS, enabled=True, config={}),
                Agent(
                    id="a1",
                    name="analyst",
                    role="Analyst",
                    goal="Query the model",
                    backstory="b",
                    llm="fake-model",
                    tools=[],
                    memory=False,
                    group_id=WS,
                ),
                Task(
                    id="t1",
                    name="extract",
                    description="Run a DAX query against the model",
                    expected_output="A list",
                    agent_id="a1",
                    tools=[str(tool.id)],
                    tool_configs=PBI,
                    group_id=WS,
                ),
                Crew(
                    id=CREW_ID,
                    name="pbi crew",
                    agent_ids=["a1"],
                    task_ids=["t1"],
                    nodes=[
                        {
                            "id": "agent-a1",
                            "type": "agentNode",
                            "data": {"label": "analyst", "role": "Analyst"},
                        },
                        {"id": "task-t1", "type": "taskNode", "data": node_data},
                    ],
                    edges=[{"id": "e1", "source": "agent-a1", "target": "task-t1"}],
                    memory=False,
                    group_id=WS,
                ),
            ]
        )
        for job_id, kind in job_ids:
            session.add(
                ExecutionHistory(
                    job_id=job_id,
                    status="RUNNING",
                    group_id=WS,
                    execution_type=kind,
                    created_at=datetime(2026, 10, 1, 12, 0, 0),
                    inputs={},
                )
            )
        session.commit()
    with engine.connect() as conn:
        stored = {
            "task": conn.execute(text("SELECT tool_configs FROM tasks")).scalar_one(),
            "nodes": conn.execute(text("SELECT nodes FROM crews")).scalar_one(),
        }
    engine.dispose()
    return stored


def _flow_args():
    context = _context()
    starting_point = {
        "crewId": str(CREW_ID),
        "taskId": "t1",
        "crewName": "pbi crew",
        "taskName": "extract",
    }
    flow_config = {
        "flow_id": None,
        "run_name": "PBI flow",
        "nodes": [
            {
                "id": "crew-1",
                "type": "crewNode",
                "data": {"label": "pbi crew", "crewId": str(CREW_ID)},
            }
        ],
        "edges": [],
        "flow_config": {
            "startingPoints": [starting_point],
            "listeners": [],
            "actions": [],
        },
        "inputs": {},
        "model": "fake-model",
        "group_context": context,
        "group_id": WS,
        "execution_id": "flow-job",
        "_harness": "kasal",
    }
    return "flow-job", flow_config, {}, context


def _crew_args(path):
    """The saved crew's run config, as a run of a crew by id builds it."""
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import sessionmaker

    from src.schemas.execution import CrewConfig
    from src.services.catalog.crew_config import build_crew_execution_config_by_id
    from src.services.execution.config_adapter import adapt_config

    async def build():
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            built = await build_crew_execution_config_by_id(
                session, str(CREW_ID), _context()
            )
        await engine.dispose()
        return built

    agents_yaml, tasks_yaml = asyncio.run(build())
    crew_config = adapt_config(
        CrewConfig(
            agents_yaml=agents_yaml,
            tasks_yaml=tasks_yaml,
            inputs={},
            model="fake-model",
        )
    )
    crew_config["group_id"] = WS
    crew_config["_harness"] = "kasal"
    crew_config["crew"]["memory"] = False
    return "crew-job", crew_config, {}, _context()


def _env(workdir):
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": str(workdir),  # the key files live under it
        "DATABASE_TYPE": "sqlite",
        "SQLITE_DB_PATH": str(workdir / "app.db"),
        "LOG_DIR": str(workdir / "logs"),
        "PYTHONPATH": str(BACKEND),
        "KASAL_HARNESS": "kasal",
    }


def _run(script, args, workdir, out):
    payload = base64.b64encode(pickle.dumps(args)).decode()
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(out)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=BACKEND,
        env=_env(workdir),
    )
    try:
        _, stderr = process.communicate(input=payload, timeout=240)
    except subprocess.TimeoutExpired:
        process.kill()  # never leave the spawned interpreter running
        process.communicate()
        raise
    assert out.exists(), (process.returncode, stderr[-4000:])
    return json.loads(out.read_text())


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    workdir = tmp_path_factory.mktemp("pbi")
    key_dir = workdir / ".backendcrew" / "keys"  # Path.home() in the children
    key_dir.mkdir(parents=True)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(EncryptionUtils, "get_key_directory", lambda: key_dir)
        stored = _seed(workdir / "app.db", [("flow-job", "flow"), ("crew-job", "crew")])
        crew_args = _crew_args(workdir / "app.db")
    return {
        "stored": stored,
        "crew_config": crew_args[1],
        "flow": _run(FLOW_CHILD, _flow_args(), workdir, workdir / "flow.json"),
        "crew": _run(CREW_CHILD, crew_args, workdir, workdir / "crew.json"),
    }


def test_the_database_holds_only_ciphertext(runs):
    for column, value in runs["stored"].items():
        assert SECRET not in value, column
        assert "ENC:" in value, column


@pytest.mark.parametrize("path", ["flow", "crew"])
def test_the_tool_authenticates_with_the_plaintext_secret(runs, path):
    seen = runs[path]
    assert seen["llm_calls"] >= 1, seen
    assert seen["client_secrets"] == [SECRET], seen
    assert seen["posts"] == 1, seen
    assert not any(out.startswith("Error") for out in seen["tool_outputs"]), seen


def test_the_crew_run_is_handed_plaintext(runs):
    (task,) = runs["crew_config"]["tasks"]
    assert task["tool_configs"][TOOL]["client_secret"] == SECRET
