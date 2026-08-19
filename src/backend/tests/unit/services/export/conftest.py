"""Shared fixtures for the export tests.

The exported app's own code (``agent_server/``) can only be imported from a
RENDERED bundle: its ``{{TOKEN}}`` placeholders make the template files invalid
Python, and ``kasal_runtime/`` does not exist until export time. So the tests
that execute app code — rather than string-matching the rendered text — render
the bundle to a temp dir and import from there.
"""

import sys

import pytest
import pytest_asyncio

from src.services.export.databricks_app_exporter import DatabricksAppExporter

SAMPLE_CREW = {
    "id": "crew-1",
    "name": "Research Crew",
    "agents": [
        {
            "id": "a1",
            "name": "Researcher",
            "role": "Researcher",
            "goal": "Find things",
            "backstory": "Seasoned",
        }
    ],
    "tasks": [
        {
            "id": "t1",
            "name": "research",
            "description": "Say hello",
            "expected_output": "a greeting",
            "agent_id": "a1",
        }
    ],
}


def purge_agent_server_modules():
    """Drop the app package from sys.modules and reset the globals it claims.

    Required both before and after: these tests import ``agent_server`` from a
    per-test temp dir, and a module cached from a previous temp dir would be
    silently reused (pointing at a directory that no longer exists).

    ``agent.py`` registers its handlers with MLflow's ``@invoke()`` and
    ``@stream()``, which keep PROCESS-GLOBAL functions and raise "... decorator
    can only be used once" on a second import. Clearing them is what makes
    agent.py importable per-test — in the real app it is imported exactly once,
    so this is a test harness concern only."""
    for name in list(sys.modules):
        if name == "agent_server" or name.startswith("agent_server."):
            del sys.modules[name]
    try:
        from mlflow.genai.agent_server import server as _mlflow_server

        _mlflow_server._invoke_function = None
        _mlflow_server._stream_function = None
    except Exception:  # noqa: BLE001 — older mlflow, or the attributes moved
        pass


async def _render_bundle(tmp_path, monkeypatch, options=None):
    result = await DatabricksAppExporter().export(dict(SAMPLE_CREW), options or {})
    for f in result["files"]:
        dest = tmp_path / f["path"]
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(f["content"], encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    purge_agent_server_modules()
    return tmp_path


@pytest_asyncio.fixture
async def app_bundle(tmp_path, monkeypatch):
    """Render the export to ``tmp_path`` and make ``agent_server`` importable."""
    yield await _render_bundle(tmp_path, monkeypatch)
    purge_agent_server_modules()


@pytest_asyncio.fixture
async def crewai_app_bundle(tmp_path, monkeypatch):
    """The same bundle, exported for the CrewAI runtime."""
    yield await _render_bundle(tmp_path, monkeypatch, {"runtime": "crewai"})
    purge_agent_server_modules()


@pytest.fixture
def fake_llm():
    """An object satisfying the runtime's duck-typed LLM contract.

    ``runtime/executor.call_llm`` forwards only the kwargs the signature
    declares, so these names are the contract — see the note in
    ``databricks_llm.DatabricksLLM.call``."""

    class _FakeLLM:
        def __init__(self, reply="Hello from the Kasal runtime."):
            self.reply = reply
            self.calls = []

        def call(
            self,
            messages,
            tools=None,
            available_functions=None,
            from_task=None,
            from_agent=None,
        ):
            self.calls.append(
                {"messages": messages, "tools": tools, "from_agent": from_agent}
            )
            return self.reply

    return _FakeLLM()
