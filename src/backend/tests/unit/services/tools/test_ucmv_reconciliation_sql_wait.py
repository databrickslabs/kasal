"""The comparison waits for a metric-view query that is still running.

The deployer's SQL call gives up after about three minutes (enough for a DDL). A wide view over
a heavy source takes longer; the comparison then reported "UCMV query returned no rows: State:
RUNNING" and the view was not compared at all."""

from unittest.mock import MagicMock, patch

import src.services.tools.ucmv_reconciliation_tool as tool


def _response(state: str, rows: list | None = None) -> MagicMock:
    response = MagicMock()
    response.json.return_value = {
        "status": {"state": state},
        "manifest": {
            "schema": {"columns": [{"name": "comp_code"}, {"name": "period"}]}
        },
        "result": {"data_array": rows or []},
    }
    return response


@patch.object(tool.time, "sleep")
@patch("requests.get")
def test_a_running_query_is_polled_until_it_succeeds(get, _sleep):
    get.side_effect = [
        _response("RUNNING"),
        _response("SUCCEEDED", [["0301", "2026001"]]),
    ]
    out = tool.wait_for_statement("https://example.com", "stmt-1", {})
    assert out["success"] is True and get.call_count == 2
    assert out["data"]["result"]["data_array"] == [["0301", "2026001"]]


@patch.object(tool.time, "sleep")
@patch("requests.post")
@patch("requests.get")
def test_a_query_that_never_finishes_is_cancelled_and_named(get, post, _sleep):
    get.return_value = _response("RUNNING")
    with patch.object(tool, "SQL_WAIT_SECONDS", 0.0):
        with patch.object(tool.time, "monotonic", side_effect=[0.0, 0.0, 1.0]):
            out = tool.wait_for_statement("https://example.com", "stmt-1", {})
    assert out["success"] is False and "did not finish" in str(out["error"])
    assert post.call_args[0][0].endswith("/statements/stmt-1/cancel")


@patch.object(tool.time, "sleep")
@patch("requests.get")
def test_a_failed_query_keeps_its_error(get, _sleep):
    failed = MagicMock()
    failed.json.return_value = {
        "status": {"state": "FAILED", "error": {"message": "boom"}}
    }
    get.return_value = failed
    out = tool.wait_for_statement("https://example.com", "stmt-1", {})
    assert out == {"success": False, "state": "FAILED", "error": "boom"}


def test_the_executor_waits_when_the_first_call_gave_up():
    executor = tool._SQLExecutor("https://example.com", "wh", {})
    executor._exec = lambda *a: {
        "success": False,
        "state": "RUNNING",
        "statement_id": "s",
    }
    with patch.object(
        tool, "wait_for_statement", return_value={"success": False, "error": "x"}
    ) as wait:
        frame, error = executor.execute("SELECT 1")
    assert wait.call_args[0][1] == "s" and frame.empty and error == "x"
