"""Reading a flow result as failed, in every shape the flow path returns.

The end-to-end behaviour (stored, announced, not fired) is pinned across real
interpreters in ``test_flow_failure_status.py``; this pins the guard itself.
"""

import pytest

from src.schemas.flow_execution import FlowExecutionStatus
from src.services.flow_builder.completion_payload import is_paused_for_approval
from src.services.flow_builder.run_outcome import (
    DEFAULT_FAILURE_MESSAGE,
    early_announcement,
    failure_message,
    is_failed,
)

#: What FlowRunnerService.run_flow returns for a failed flow: no "success" key,
#: and a LOWERCASE status, so an uppercase-only comparison misses it.
RUN_FLOW_FAILURE = {
    "job_id": "j",
    "execution_id": 1,
    "status": FlowExecutionStatus.FAILED,
    "error": "Crew timed out",
    "message": "Flow execution failed: Crew timed out",
}

PAUSE = {
    "success": True,
    "status": "COMPLETED",
    "paused_for_approval": True,
    "hitl_paused": True,
    "approval_id": 7,
    "gate_node_id": "gate-1",
    "message": "Approve?",
}


class TestFailuresAreRecognised:
    def test_the_failure_run_flow_returns(self):
        assert is_failed(RUN_FLOW_FAILURE)

    def test_a_lowercase_failed_status_with_no_error(self):
        assert is_failed({"status": FlowExecutionStatus.FAILED, "error": None})
        assert is_failed({"status": "failed"})

    def test_the_uppercase_status_the_subprocess_returns(self):
        assert is_failed({"status": "FAILED"})

    def test_a_flow_body_that_reports_success_false(self):
        assert is_failed({"success": False})

    def test_any_error(self):
        assert is_failed({"error": "boom"})


class TestOtherResultsAreNotFailures:
    @pytest.mark.parametrize(
        "result",
        [
            {
                "job_id": "j",
                "execution_id": 1,
                "status": FlowExecutionStatus.COMPLETED,
                "result": {"content": "answer"},
                "message": "Flow execution completed",
            },
            {"success": True, "result": "answer"},
            {"status": "COMPLETED", "error": None},
            {},
        ],
        ids=["run-flow-success", "body-success", "null-error", "empty"],
    )
    def test_a_completed_result(self, result):
        assert not is_failed(result)

    def test_a_pause_for_approval(self):
        # The subprocess checks the pause first; the guard agrees anyway.
        assert is_paused_for_approval(PAUSE)
        assert not is_failed(PAUSE)

    @pytest.mark.parametrize("result", [None, "answer", ["a"], 0])
    def test_a_result_that_is_not_a_dict(self, result):
        assert not is_failed(result)


class TestTheFailureMessage:
    def test_the_error_wins(self):
        assert failure_message(RUN_FLOW_FAILURE) == "Crew timed out"

    def test_the_message_when_there_is_no_error(self):
        assert failure_message({"status": "failed", "message": "why"}) == "why"

    def test_a_default_when_there_is_neither(self):
        assert failure_message({"success": False}) == DEFAULT_FAILURE_MESSAGE
        assert failure_message({"error": "", "message": None}) == (
            DEFAULT_FAILURE_MESSAGE
        )
        assert failure_message(None) == DEFAULT_FAILURE_MESSAGE

    def test_a_non_string_error_is_stringified(self):
        assert failure_message({"error": ValueError("bad")}) == "bad"


class TestTheEarlyAnnouncement:
    def test_a_failure_is_announced_failed_with_its_error_and_no_answer(self):
        assert early_announcement(RUN_FLOW_FAILURE) == {
            "status": "FAILED",
            "message": "Crew timed out",
        }

    def test_a_success_is_announced_completed_with_its_answer(self):
        result = {"status": FlowExecutionStatus.COMPLETED, "result": "answer"}

        assert early_announcement(result) == {
            "status": "COMPLETED",
            "message": "Flow execution completed",
            "result": "answer",
        }

    def test_a_pause_is_not_announced(self):
        assert early_announcement(PAUSE) is None

    def test_a_paused_failure_shape_is_still_a_pause(self):
        # The pause is read first, exactly as before this guard existed.
        assert early_announcement({**PAUSE, "error": "irrelevant"}) is None
