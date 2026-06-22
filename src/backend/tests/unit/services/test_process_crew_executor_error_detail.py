"""Regression tests for the crew-preparation failure message in
``run_crew_in_process``.

When ``CrewPreparation.prepare()`` returns ``False`` the executor raises a
``RuntimeError`` that previously read only "Failed to prepare crew for <id>".
The diff under test appends an actionable suffix derived from ``crew_config``:

  * "" + " — crew has no tasks"   when ``crew_config["tasks"]`` is empty
  * "" + " — crew has no agents"  when tasks present but ``agents`` empty
  * "" (no suffix)                when both tasks and agents are present

``run_crew_in_process`` catches the RuntimeError and returns a FAILED result
whose ``error`` field is ``str(e)``, so we assert on that surfaced message.

The function is large with a deep import chain, so — mirroring
``TestOtelShutdownOnError`` in ``test_process_crew_executor_unit.py`` — we mock
the heavy subprocess imports and force ``CrewPreparation.prepare()`` to return
False to reach the exact branch under test.
"""

import contextlib
import io

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.process_crew_executor import run_crew_in_process


async def _fake_smart_db_session():
    """Async generator standing in for get_smart_db_session()."""
    yield MagicMock()


def _run_with_prepare_false(crew_config):
    """Invoke run_crew_in_process with CrewPreparation.prepare() -> False.

    Returns the FAILED result dict (its ``error`` carries the RuntimeError
    message we assert on). The function has a deep subprocess import chain, so
    we stub the heavy collaborators (MLflow setup, DB session, ToolFactory) and
    force CrewPreparation.prepare() to return False to reach the exact branch
    under test. The real OTel package is left importable — its setup blocks are
    wrapped in try/except and stay non-fatal.
    """
    mock_logging_config = MagicMock()
    mock_logging_config.suppress_stdout_stderr.return_value = (
        MagicMock(),
        MagicMock(),
        io.StringIO(),
    )
    mock_logging_config.configure_subprocess_logging.return_value = MagicMock()

    # CrewPreparation instance whose async prepare() resolves to False.
    mock_prep_instance = MagicMock()
    mock_prep_instance.prepare = AsyncMock(return_value=False)
    mock_crew_preparation_cls = MagicMock(return_value=mock_prep_instance)

    with contextlib.ExitStack() as stack:
        stack.enter_context(
            patch.dict(
                "sys.modules",
                {
                    "src.engines.crewai.logging_config": mock_logging_config,
                    "crewai": MagicMock(),
                    "crewai.llm": MagicMock(LLM_CONTEXT_WINDOW_SIZES={}),
                    "crewai.events": MagicMock(),
                    "crewai.utilities": MagicMock(),
                    "crewai.utilities.exceptions": MagicMock(),
                    "crewai.utilities.exceptions.context_window_exceeding_exception": MagicMock(
                        CONTEXT_LIMIT_ERRORS=[]
                    ),
                },
            )
        )
        stack.enter_context(patch("src.seeds.model_configs.MODEL_CONFIGS", {}))
        # Stub MLflow subprocess setup so it does no real work.
        stack.enter_context(
            patch(
                "src.services.otel_tracing.mlflow_setup.configure_mlflow_in_subprocess",
                new=AsyncMock(return_value=None),
            )
        )
        # DB session + tool factory: avoid real DB/tool wiring.
        stack.enter_context(
            patch(
                "src.db.database_router.get_smart_db_session",
                new=_fake_smart_db_session,
            )
        )
        stack.enter_context(patch("src.services.tool_service.ToolService", MagicMock()))
        stack.enter_context(
            patch("src.services.api_keys_service.ApiKeysService", MagicMock())
        )
        stack.enter_context(
            patch(
                "src.engines.crewai.tools.tool_factory.ToolFactory.create",
                new=AsyncMock(return_value=MagicMock()),
            )
        )
        stack.enter_context(
            patch(
                "src.engines.crewai.crew_preparation.CrewPreparation",
                mock_crew_preparation_cls,
            )
        )
        return run_crew_in_process(
            execution_id="e-prep-fail",
            crew_config=crew_config,
        )


class TestPrepareFailureMessageDetail:
    """The surfaced error explains *why* prepare() failed where it can."""

    def test_no_tasks_appends_no_tasks_suffix(self):
        crew_config = {
            "run_name": "test-crew",
            "version": "1.0",
            "agents": [{"id": "a1", "role": "Analyst"}],
            "tasks": [],
            "crew_config": {},
        }

        result = _run_with_prepare_false(crew_config)

        assert result["status"] == "FAILED"
        assert result["execution_id"] == "e-prep-fail"
        assert "Failed to prepare crew for e-prep-fail" in result["error"]
        assert result["error"].endswith(" — crew has no tasks")

    def test_no_agents_appends_no_agents_suffix(self):
        # tasks present, agents empty -> "no agents" branch
        crew_config = {
            "run_name": "test-crew",
            "version": "1.0",
            "agents": [],
            "tasks": [{"id": "t1", "name": "Do thing"}],
            "crew_config": {},
        }

        result = _run_with_prepare_false(crew_config)

        assert result["status"] == "FAILED"
        assert "Failed to prepare crew for e-prep-fail" in result["error"]
        assert result["error"].endswith(" — crew has no agents")

    def test_tasks_and_agents_present_has_no_suffix(self):
        # Both sections populated -> generic message, no suffix.
        crew_config = {
            "run_name": "test-crew",
            "version": "1.0",
            "agents": [{"id": "a1", "role": "Analyst"}],
            "tasks": [{"id": "t1", "name": "Do thing"}],
            "crew_config": {},
        }

        result = _run_with_prepare_false(crew_config)

        assert result["status"] == "FAILED"
        assert result["error"].endswith("Failed to prepare crew for e-prep-fail")
        assert "crew has no tasks" not in result["error"]
        assert "crew has no agents" not in result["error"]

    def test_no_tasks_takes_precedence_over_no_agents(self):
        # When BOTH tasks and agents are empty, the "no tasks" branch wins
        # (it is checked first in the source).
        crew_config = {
            "run_name": "test-crew",
            "version": "1.0",
            "agents": [],
            "tasks": [],
            "crew_config": {},
        }

        result = _run_with_prepare_false(crew_config)

        assert result["status"] == "FAILED"
        assert result["error"].endswith(" — crew has no tasks")
        assert "crew has no agents" not in result["error"]
