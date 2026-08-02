"""What a finished flow announces alongside its COMPLETED status.

Two real failures came from announcing the status alone:

- A run persisted a 66,857-character answer that was never shown. The UI
  finalises a job exactly once, so it finalised on the empty announcement and
  ignored the parent's later write as a duplicate.
- A run whose subprocess died during teardown (aiosqlite CancelledError in pool
  reset) never reached the parent's status update at all — the only write that
  carried the result — and finished COMPLETED with an empty result.
"""

from src.services.flow_builder.completion_payload import (
    answer_from_result,
    is_paused_for_approval,
)


class TestTheAnswerIsFound:
    def test_the_usual_payload_shape(self):
        assert (
            answer_from_result({"success": True, "result": "# Catalog"}) == "# Catalog"
        )

    def test_a_plain_string_result(self):
        assert answer_from_result("# Catalog") == "# Catalog"

    def test_alternative_keys(self):
        assert answer_from_result({"output": "answer"}) == "answer"
        assert answer_from_result({"text": "answer"}) == "answer"

    def test_the_first_populated_key_wins(self):
        assert answer_from_result({"result": "a", "output": "b"}) == "a"

    def test_a_payload_with_no_recognised_key_is_passed_on_whole(self):
        # Better than dropping it; the parent's later write replaces it with the
        # composed envelope anyway.
        payload = {"unexpected": "shape"}

        assert answer_from_result(payload) == payload


class TestNothingIsAnnouncedWhenThereIsNothing:
    """None means "status only" — the old behaviour, still correct when empty.

    Crucially it must never be a falsy-but-present value: writing "" would put
    an empty result over whatever the parent or a retry had already stored,
    turning this fix into the very bug it exists to prevent.
    """

    def test_no_result(self):
        assert answer_from_result(None) is None

    def test_an_empty_string_is_not_announced(self):
        assert answer_from_result("") is None

    def test_an_empty_payload_is_not_announced(self):
        assert answer_from_result({}) is None

    def test_a_payload_whose_answer_is_empty_is_not_announced(self):
        assert answer_from_result({"success": True, "result": ""}) is None
        assert answer_from_result({"success": True, "result": None}) is None


class TestAPausedFlowHasNotFinished:
    """A flow stopped at a HITL gate returns success=True AND status=COMPLETED.

    So "did it finish?" cannot be read from the status. Announcing on the status
    alone marked a waiting run as done, and once the announcement carried a
    payload the approval bookkeeping — approval_id, gate_node_id, the reviewer
    prompt — was shown to the reader as their answer.

    Taken from a real run whose stored result was:
      {"success": true, "paused_for_approval": true, "approval_id": 1, ...}
    """

    PAUSE = {
        "success": True,
        "status": "COMPLETED",
        "paused_for_approval": True,
        "approval_id": 1,
        "gate_node_id": "crew-abc-def",
        "message": "Please review and approve to continue",
    }

    def test_a_pause_is_recognised_despite_saying_COMPLETED(self):
        assert is_paused_for_approval(self.PAUSE) is True

    def test_the_other_pause_markers_too(self):
        assert is_paused_for_approval({"hitl_paused": True}) is True
        assert is_paused_for_approval({"status": "WAITING_FOR_APPROVAL"}) is True

    def test_a_finished_run_is_not_a_pause(self):
        assert is_paused_for_approval({"status": "COMPLETED", "result": "x"}) is False
        assert is_paused_for_approval("a plain string answer") is False
        assert is_paused_for_approval(None) is False

    def test_a_pause_payload_is_never_announced_as_the_answer(self):
        # Defence in depth: the caller should not announce a paused run at all,
        # but if it does, this must not hand back the bookkeeping.
        assert answer_from_result(self.PAUSE) is None

    def test_a_falsy_pause_marker_does_not_count(self):
        # paused_for_approval=False is a FINISHED run that carries the key.
        finished = {"status": "COMPLETED", "paused_for_approval": False, "result": "x"}

        assert is_paused_for_approval(finished) is False
        assert answer_from_result(finished) == "x"
