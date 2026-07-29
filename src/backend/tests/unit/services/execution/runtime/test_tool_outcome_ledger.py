"""What a task's tools actually did, visible to something other than the model.

Nothing could see it before. A tool that RETURNS an error string — which most
first-party tools and every MCP server do — emits ToolUsageFinishedEvent and is
indistinguishable from success at every layer above.

The run that prompted this: every ``Parse_call_endpoint`` returned 404/503, the
agent therefore had no records, no INSERT ran, and the table stayed empty. The
task completed. The guardrail, seeing only the output text, rejected it three
times for "not defining named agents and an ordered task sequence" — advice no
rewrite could act on, because the data never existed. Then ``degrade`` accepted
it and the run read COMPLETED.

These pin the three seams that make that legible: the ledger records returned
failures as failures, the guardrail is told, and the degradation says why.
"""

import pytest

from src.services.execution.runtime.executor import (
    ToolOutcome,
    looks_like_failure,
    reset_tool_ledger,
    tool_failure_summary,
    tool_ledger,
    wholly_failed_tools,
    wrap_tool,
)
from src.services.tools.base import BaseTool


def _tool(name, result=None, raises=None):
    class _T(BaseTool):
        def _run(self, **kwargs):
            if raises:
                raise raises
            return result

    return _T(name=name, description=f"{name} tool")


@pytest.fixture(autouse=True)
def _fresh_ledger():
    reset_tool_ledger()
    yield
    reset_tool_ledger()


class TestClassifyingAFailure:
    @pytest.mark.parametrize(
        "output,failed",
        [
            ("Tool error: 503 upstream_error", True),
            ("Error: describe what the remote agent should do.", True),
            ("Error executing MCP tool foo", True),
            ("Error from Perplexity API: 429", True),
            # The literal message perplexity_tool emits on requests.Timeout. A
            # search that timed out is a dead source, not an answer, and the
            # tool has no way to say so except through this prefix.
            (
                "Error: Perplexity API did not respond within the timeout "
                "(10, 300) (connect, read) seconds: Read timed out.",
                True,
            ),
            # Not failures — the word appears, but the call worked.
            ("Found 3 errors in the build log", False),
            ("The error rate dropped to 0.2%", False),
            ('{"rows": [{"id": 1}]}', False),
            ("", False),
        ],
    )
    def test_only_a_result_that_reports_failure_counts(self, output, failed):
        """Matched at the START of the result and against a narrow marker set.
        A broad search for "error" would classify a successful search ABOUT
        errors as a failure, which is worse than missing one."""
        assert looks_like_failure(output) is failed

    def test_non_strings_are_never_failures(self):
        assert looks_like_failure({"ok": True}) is False
        assert looks_like_failure(None) is False


class TestTheLedgerRecordsWhatHappened:
    def test_a_returned_error_is_recorded_as_a_failure(self):
        """The whole point: this path emits ToolUsageFinishedEvent, so the
        event stream says success and only the ledger disagrees."""
        run = wrap_tool(_tool("Parse", result="Tool error: 503 upstream"))
        run()
        assert tool_ledger()["Parse"] == ToolOutcome(
            calls=1, failures=1, errors=["Tool error: 503 upstream"]
        )

    def test_a_raised_error_is_recorded_too(self):
        run = wrap_tool(_tool("Genie", raises=RuntimeError("boom")))
        with pytest.raises(RuntimeError):
            run()
        outcome = tool_ledger()["Genie"]
        assert (outcome.calls, outcome.failures) == (1, 1)

    def test_a_success_is_not_a_failure(self):
        run = wrap_tool(_tool("Genie", result='{"rows": []}'))
        run()
        outcome = tool_ledger()["Genie"]
        assert (outcome.calls, outcome.failures) == (1, 0)

    def test_wholly_failed_means_never_once_worked(self):
        """The distinction that matters: a tool that failed twice and then
        succeeded gave us the data; one that never succeeded did not."""
        dead = wrap_tool(_tool("Dead", result="Tool error: down"))
        dead()
        dead()
        flaky = wrap_tool(_tool("Flaky", result="Tool error: down"))
        flaky()
        wrap_tool(_tool("Flaky", result="ok"))()

        assert wholly_failed_tools() == ["Dead"]
        assert tool_ledger()["Flaky"].wholly_failed is False

    def test_only_three_error_samples_are_kept(self):
        """A looping agent can fail the same call dozens of times; the ledger
        is diagnostics, not a transcript."""
        run = wrap_tool(_tool("Noisy", result="Tool error: nope"))
        for _ in range(10):
            run()
        outcome = tool_ledger()["Noisy"]
        assert outcome.calls == 10 and len(outcome.errors) == 3

    def test_resetting_clears_the_previous_task(self):
        """Per task, not per run — a previous task's 503s must not be held
        against an answer they had nothing to do with."""
        wrap_tool(_tool("Parse", result="Tool error: down"))()
        assert wholly_failed_tools() == ["Parse"]
        reset_tool_ledger()
        assert wholly_failed_tools() == []

    def test_calls_outside_a_task_scope_are_not_an_error(self):
        """The ledger is opt-in per task; a tool run outside one just records
        nothing rather than blowing up."""
        from src.services.execution.runtime import executor

        executor._tool_ledger.set(None)
        wrap_tool(_tool("Loose", result="ok"))()  # must not raise
        assert tool_ledger() == {}


class TestTheSummaryAReaderGets:
    def test_nothing_failed_yields_nothing(self):
        wrap_tool(_tool("Genie", result="ok"))()
        assert tool_failure_summary() == ""

    def test_a_wholly_failed_tool_is_called_out(self):
        run = wrap_tool(_tool("Parse_call_endpoint", result="Tool error: 503"))
        run()
        run()
        summary = tool_failure_summary()
        assert "Parse_call_endpoint" in summary
        assert "2/2" in summary
        assert "ALL FAILED" in summary

    def test_a_partial_failure_is_distinguished(self):
        wrap_tool(_tool("Search", result="Tool error: rate limited"))()
        wrap_tool(_tool("Search", result="results"))()
        summary = tool_failure_summary()
        assert "1/2" in summary and "partly failed" in summary
        assert "ALL FAILED" not in summary
