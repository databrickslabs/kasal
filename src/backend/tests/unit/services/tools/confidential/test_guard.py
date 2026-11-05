"""The guard on a tool instance: which calls it sees, what it redacts, what it raises."""

import asyncio

import pytest
from pydantic import BaseModel, Field

from src.services.tools.base import BaseTool
from src.services.tools.confidential.redactor import redact_text
from src.services.tools.confidential import guard as guard_module
from src.services.tools.confidential.guard import (
    GUARD_MARK,
    install_confidential_guard,
    is_value_free_mode_active,
    scrubbed_exception,
)
from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

TABLE = (
    "| Plant | Revenue |\n| --- | --- |\n| North | 1234.5 |\n| South | 7 |\n_2 rows._"
)


class _Schema(BaseModel):
    question: str = Field(default="")
    boom: str = Field(default="")


class _TableTool(BaseTool):
    name: str = "table_tool"
    description: str = "returns a table"
    args_schema: type[BaseModel] = _Schema

    def _run(self, **kwargs: object) -> str:
        if kwargs.get("boom"):
            raise ValueError("row 4 holds 12.75")
        return TABLE

    async def _arun(self, **kwargs: object) -> str:
        return TABLE


def _guarded(tool: BaseTool) -> BaseTool:
    assert install_confidential_guard(tool)
    return tool


def test_run_returns_redacted_output_and_trace_sees_no_value():
    tool = _guarded(_TableTool())
    out = tool.run(question="q")
    assert "1234.5" not in out
    assert "| North | <withheld> |" in out
    assert "| South | 7 |" in out


def test_direct_run_path_is_guarded_too():
    # to_structured_tool and BaseTool.run both reach _run directly.
    tool = _guarded(_TableTool())
    assert "1234.5" not in tool._run(question="q")


def test_output_is_redacted_once_not_twice():
    # Redacting an already-redacted result is not a no-op (its summary line
    # acquires a digit-bearing notice). The guard must equal ONE pass exactly.
    tool = _guarded(_TableTool())
    once = redact_text(TABLE)
    assert tool.run(question="q") == once
    assert redact_text(once) != once


def test_exception_message_is_scrubbed_and_not_chained():
    tool = _guarded(_TableTool())
    with pytest.raises(ValueError) as caught:
        tool.run(question="q", boom="x")
    assert str(caught.value) == "row <n> holds <n>"
    assert caught.value.__context__ is None
    assert caught.value.__suppress_context__ is True


def test_exception_without_numbers_is_the_original_object():
    error = ValueError("no numbers here")
    assert scrubbed_exception(error) is error


def test_exception_type_that_cannot_take_one_message_becomes_runtime_error():
    class Picky(Exception):
        def __init__(self, a: str, b: str) -> None:
            super().__init__(f"{a}{b}")

    rebuilt = scrubbed_exception(Picky("code 4", "x"))
    assert isinstance(rebuilt, RuntimeError)
    assert str(rebuilt) == "code <n>x"


def test_async_path_is_redacted():
    tool = _guarded(_TableTool())
    out = asyncio.run(tool.arun(question="q"))
    assert "1234.5" not in out
    assert "| North | <withheld> |" in out


def test_install_is_idempotent():
    tool = _TableTool()
    assert install_confidential_guard(tool)
    first = tool._run
    assert install_confidential_guard(tool)
    assert tool._run is first
    assert getattr(tool, GUARD_MARK) is True


def test_object_without_a_run_path_is_reported_not_guarded():
    class Inert:
        pass

    assert install_confidential_guard(Inert()) is False


def test_value_free_tool_gets_its_mode_forced_and_is_not_redacted(monkeypatch):
    calls: dict = {}

    def fake_run(self, **kwargs):
        calls.update(kwargs)
        return '{"measures": {"share_aligned": 0.97}, "statistics_only": true}'

    monkeypatch.setattr(UCMVReconciliationTool, "_run", fake_run)
    tool = UCMVReconciliationTool()
    assert install_confidential_guard(tool)

    out = tool._run(statistics_only=False, dry_run=True)

    assert "statistics_only" not in calls, "the LLM-side override must be dropped"
    assert calls == {"dry_run": True}
    assert tool._default_config["statistics_only"] is True
    assert is_value_free_mode_active(tool)
    assert "0.97" in out, "value-free output is the verdict; it must not be redacted"


def test_value_free_tool_still_scrubs_its_exceptions(monkeypatch):
    def fake_run(self, **kwargs):
        raise RuntimeError("view 3 failed at 12.5")

    monkeypatch.setattr(UCMVReconciliationTool, "_run", fake_run)
    tool = UCMVReconciliationTool()
    install_confidential_guard(tool)
    with pytest.raises(RuntimeError) as caught:
        tool._run()
    assert str(caught.value) == "view <n> failed at <n>"


def test_forced_mode_falls_back_to_redaction_when_config_is_missing(monkeypatch):
    class Renamed(_TableTool):
        pass

    Renamed.__name__ = "UCMVReconciliationTool"
    tool = Renamed()
    install_confidential_guard(tool)
    assert not is_value_free_mode_active(tool)
    assert "1234.5" not in tool._run(question="q")


def test_value_free_table_is_keyed_by_class_name():
    assert guard_module.VALUE_FREE_MODES == {
        "UCMVReconciliationTool": {"statistics_only": True},
        "GenieSpaceSyncTool": {"statistics_only": True},
    }
