"""The blocking ask over the chat path.

Two properties matter here beyond "it returns an answer":

* the run is recorded like any other, stamped with the caller's origin, so the
  audit trail exists from the first external call rather than being retrofitted;
* nothing raises across the protocol boundary — an unhandled exception reaching
  another agent is an opaque transport error with no run id to follow up on.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.external.identity import ExternalCaller
from src.services.external.invocation import (
    InvocationResult,
    _build_ask_config,
    _to_result,
    ask,
)
from src.services.external.state import ExternalTaskState


class _Ctx:
    def __init__(self, group_ids=("acme_corp",)):
        self.group_ids = list(group_ids)
        self.group_email = "caller@example.com"

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


def _caller(protocol="mcp"):
    return ExternalCaller(
        group_context=_Ctx(), protocol=protocol, identifier="caller@example.com"
    )


def _patch_execution(result=None, side_effect=None):
    """Patch the execution layer at its import site inside ask()."""
    run = AsyncMock(return_value=result, side_effect=side_effect)
    create = AsyncMock(return_value=True)
    return (
        patch(
            "src.services.execution.service.ExecutionService.run_crew_execution",
            new=run,
        ),
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new=create,
        ),
        run,
        create,
    )


class TestAskConfig:
    def test_uses_the_chat_path(self):
        """execution_type='agent' is what makes this in-process and sub-second —
        the only Kasal shape that fits inside an ordinary tool-call timeout."""
        cfg = _build_ask_config("what is 6*7?", None)
        assert cfg.execution_type == "agent"

    def test_the_question_becomes_the_task(self):
        cfg = _build_ask_config("what is 6*7?", None)
        assert cfg.tasks_yaml["answer"]["description"] == "what is 6*7?"

    def test_exactly_one_agent_and_one_task(self):
        """A crew would not be sub-second, and this must not quietly become one."""
        cfg = _build_ask_config("q", None)
        assert len(cfg.agents_yaml) == 1
        assert len(cfg.tasks_yaml) == 1

    def test_model_override_is_carried(self):
        assert _build_ask_config("q", "databricks-claude").model == "databricks-claude"


class TestAsk:
    @pytest.mark.asyncio
    async def test_returns_the_answer(self):
        p_run, p_create, run, _create = _patch_execution(
            {"status": "COMPLETED", "result": "42"}
        )
        with p_run, p_create:
            result = await ask(_caller(), "what is 6*7?")

        assert result.state is ExternalTaskState.COMPLETED
        assert result.output == "42"

    @pytest.mark.asyncio
    async def test_runs_as_the_callers_group(self):
        p_run, p_create, run, _create = _patch_execution({"status": "COMPLETED"})
        with p_run, p_create:
            caller = _caller()
            await ask(caller, "q")

        assert run.await_args.kwargs["group_context"] is caller.group_context

    @pytest.mark.asyncio
    async def test_records_the_origin_on_the_execution(self):
        """One field, written at the only point every external invocation passes
        through — so "who called this, over which protocol" is answerable."""
        p_run, p_create, _run, create = _patch_execution({"status": "COMPLETED"})
        with p_run, p_create:
            await ask(_caller(protocol="a2a"), "q")

        data = create.await_args.kwargs["execution_data"]
        assert data["inputs"]["external_origin"] == "a2a:caller@example.com"

    @pytest.mark.asyncio
    async def test_failure_is_returned_not_raised(self):
        """An exception crossing the protocol boundary becomes an opaque
        transport error with no run id. The caller gets a result instead."""
        p_run, p_create, _run, _create = _patch_execution(
            side_effect=RuntimeError("engine exploded")
        )
        with p_run, p_create:
            result = await ask(_caller(), "q")

        assert result.state is ExternalTaskState.FAILED
        assert "engine exploded" in result.error
        assert result.run_id  # still followable

    @pytest.mark.asyncio
    async def test_empty_question_is_rejected_before_any_run_is_created(self):
        with pytest.raises(ValueError):
            await ask(_caller(), "   ")


class TestResultNormalisation:
    def test_kasal_status_becomes_the_canonical_state(self):
        assert _to_result("r", {"status": "RUNNING"}).state is ExternalTaskState.WORKING

    def test_unwraps_a_structured_output(self):
        r = _to_result("r", {"status": "COMPLETED", "result": {"raw": "the answer"}})
        assert r.output == "the answer"

    def test_survives_an_unexpected_return_shape(self):
        """The boundary where a change in the execution layer would otherwise
        surface to an external caller as a crash."""
        r = _to_result("r", "just a string")
        assert r.state is ExternalTaskState.COMPLETED
        assert r.output == "just a string"

    def test_as_dict_omits_absent_fields(self):
        payload = InvocationResult(
            run_id="r", state=ExternalTaskState.WORKING
        ).as_dict()
        assert payload == {"run_id": "r", "state": "working"}
