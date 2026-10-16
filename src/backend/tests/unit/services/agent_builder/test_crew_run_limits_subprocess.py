"""A real crew subprocess on the default harness, past 8 tool results and its clock.

The CrewAI harness is the default, and DatabricksRetryLLM serves most Databricks
chat models. Before this change such a crew could make at most 8 tool calls per
task (the handler stripped ``tools`` after 8 results), and a turn that ran out
of time failed the run: CrewAI's own ThreadPool timeout raised, or the budget
error was wrapped and the whole task re-ran through ``max_retry_limit``.

This runs the REAL child entry, ``process_executor.run_crew_in_process``, in a
fresh interpreter (services/execution/CLAUDE.md: in-process tests can pass while
the spawned interpreter cannot). Only two things are stand-ins:

* ``CrewPreparation`` — it reads agents, tools and models from the database;
  the stand-in builds the same objects through the run's harness binding, with
  a real ``DatabricksRetryLLM`` whose HTTP client is faked;
* the HTTP client — a model that asks for another search on every round, for
  ever, and can only answer in text when it is offered no tools.

Everything between is real: harness adoption from the payload, the event
bridge and turn deadlines, ``KasalMemoryCrew.kickoff_async``, the memory sink,
the status announcement and the result the parent receives.
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
from src.services.agent_builder import process_executor  # noqa: F401
from src.utils.user_context import GroupContext

BACKEND = pathlib.Path(__file__).resolve().parents[4]
JOB_ID = "crew-run-limits"
WS = "ws-a"
#: Long enough for well over 8 tool rounds (~0.06s each after a slow first
#: one), short enough that the clock, not max_iter=500, is what ends the turn.
CAP_SECONDS = 4

CHILD = r"""
import base64, json, os, pickle, sys, time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import src.services.agent_builder.process_executor as entry  # the child entry
from src.services.agent_builder import crew_preparation
from src.services.llm.handlers.databricks_retry_llm import DatabricksRetryLLM

out_path = sys.argv[1]
execution_id, crew_config, group_context, cap = pickle.loads(
    base64.b64decode(sys.stdin.read())
)
seen = {"requests": [], "tool_runs": 0, "turns": 0, "persisted": 0, "timeline": []}
USAGE = SimpleNamespace(total_tokens=1, prompt_tokens=1, completion_tokens=0,
                        prompt_tokens_details=None, completion_tokens_details=None)
T0 = time.monotonic()

def mark(what):  # diagnostics only: what happened, and when
    seen["timeline"].append((what, round(time.monotonic() - T0, 2)))

def create(**params):  # a model that never stops searching
    seen["requests"].append(bool(params.get("tools")))
    mark("request")
    time.sleep(0.05)
    if not params.get("tools"):
        message = SimpleNamespace(content="forced answer without tools",
                                  tool_calls=None, reasoning_content=None)
        reason = "stop"
    else:
        call = SimpleNamespace(
            id=f"call-{len(seen['requests'])}",
            function=SimpleNamespace(name="search", arguments='{"q": "x"}'),
        )
        message = SimpleNamespace(content=None, tool_calls=[call],
                                  reasoning_content=None)
        reason = "tool_calls"
    return SimpleNamespace(
        choices=[SimpleNamespace(message=message, finish_reason=reason)],
        usage=USAGE,
    )

CLIENT = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))

class FakePreparation:
    def __init__(self, config, tool_service, tool_factory, user_token=None):
        self.config, self.crew = config, None

    async def prepare(self):
        from pydantic import BaseModel, Field
        from src.services.execution.harnesses import active_harness
        from src.services.memory.run.persist import make_memory_output_sink
        from src.services.tools.base import BaseTool

        class Args(BaseModel):
            q: str = Field(description="query")

        class Search(BaseTool):
            name: str = "search"
            description: str = "Search"
            args_schema: type = Args

            def _run(self, q):
                seen["tool_runs"] += 1
                mark("tool")
                return "a result"

        harness = active_harness()
        seen["harness"] = harness.name.value
        agent = harness.build_agent(
            role="Researcher", goal="research", backstory="b",
            llm=DatabricksRetryLLM(model="databricks/databricks-claude-sonnet-4-6"),
            tools=[Search()], max_iter=500, max_execution_time=cap,
        )
        seen["agent_field"] = agent.max_execution_time
        task = harness.build_task(
            description="research everything", expected_output="text", agent=agent
        )
        self.crew = harness.build_crew(
            agents=[agent], tasks=[task], process=harness.process("sequential")
        )
        harness.wire_memory(self.crew, sink=make_memory_output_sink(MagicMock()))
        return True

from crewai import Agent as CrewAgent
original_execute = CrewAgent.execute_task

def counting(self, *args, **kwargs):
    seen["turns"] += 1
    mark("turn")
    return original_execute(self, *args, **kwargs)

def persisted(*args, **kwargs):
    seen["persisted"] += 1

started = time.monotonic()
with patch.object(crew_preparation, "CrewPreparation", FakePreparation), patch.object(
    DatabricksRetryLLM, "client", property(lambda self: CLIENT)
), patch.object(CrewAgent, "execute_task", counting), patch(
    "src.services.memory.run.persist.remember_async", persisted
):
    result = entry.run_crew_in_process(execution_id, crew_config, None, group_context)
seen["elapsed"] = time.monotonic() - started
seen["status"] = result.get("status")
seen["result"] = result.get("result")
seen["error"] = result.get("error")
with open(out_path, "w") as fh:
    json.dump(seen, fh, default=str)
os._exit(0)  # as the real wrapper does: tracing threads must not hold exit
"""


def _database(path):
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(  # the row the API writes before it queues the run
            ExecutionHistory(
                job_id=JOB_ID,
                status="RUNNING",
                group_id=WS,
                execution_type="crew",
                created_at=datetime(2026, 10, 1, 12, 0, 0),
                inputs={"execution_type": "crew"},
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


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    workdir = tmp_path_factory.mktemp("crew-run-limits")
    _database(workdir / "app.db")
    context = GroupContext(group_ids=[WS], group_email="a@example.com")
    crew_config = {
        "_harness": "crewai",
        "run_name": "Run limits",
        "group_id": WS,
        "agents": [{"id": "a1", "role": "Researcher"}],
        "tasks": [{"id": "t1", "description": "research", "agent": "a1"}],
    }
    payload = base64.b64encode(
        pickle.dumps((JOB_ID, crew_config, context, CAP_SECONDS))
    ).decode()
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
    try:
        _, stderr = process.communicate(input=payload, timeout=240)
    except subprocess.TimeoutExpired:
        process.kill()  # never leave the spawned interpreter running
        process.communicate()
        raise
    assert out.exists(), (process.returncode, stderr[-3000:])
    return json.loads(out.read_text())


class TestTheDefaultHarnessCrew:
    def test_it_ran_on_crewai(self, run):
        assert run["harness"] == "crewai"

    def test_the_run_completes(self, run):
        assert run["status"] == "COMPLETED", run["error"]

    def test_tools_are_kept_past_eight_results(self, run):
        assert run["tool_runs"] > 8
        assert all(run["requests"]), "a request went out without tools"

    def test_the_spent_clock_returns_the_truncated_partial(self, run):
        assert "Truncated: exceeded the execution budget" in run["result"]
        assert "forced answer" not in run["result"]

    def test_nothing_is_re_run(self, run):
        # A retry through max_retry_limit re-enters execute_task.
        assert run["turns"] == 1

    def test_max_execution_time_stays_off_crewai_but_bounds_the_turn(self, run):
        assert run["agent_field"] is None
        assert run["tool_runs"] < 500  # stopped by the clock, not by max_iter

    def test_the_degraded_answer_is_not_written_to_memory(self, run):
        assert run["persisted"] == 0
