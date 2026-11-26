"""Genie conversation client and the 429 backoff, with a scripted fake HTTP."""

from src.services.tools.genie_space_sync_utils.eval.conversation import ask_genie
from src.services.tools.genie_space_sync_utils.eval.retry import RetryingHttp
from src.services.tools.genie_space_sync_utils.genie_api import GenieApiError

SPACE = "0123456789abcdef0123456789abcdef"
BASE = f"/{SPACE}"
MSG = f"{BASE}/conversations/c1/messages/m1"


class Script:
    """Answers each (method, path) from a queue; an exception in the queue is raised."""

    def __init__(self, routes):
        self.routes = {key: list(value) for key, value in routes.items()}
        self.calls = []

    def __call__(self, method, path, body, params):
        self.calls.append((method, path))
        queue = self.routes[(method, path)]
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        return item


def _no_wait(seconds):
    return None


def _clock(*times):
    values = list(times)
    last = [values[0]]

    def tick():
        if values:
            last[0] = values.pop(0)
        return last[0]

    return tick


def _complete_message(text="total is 42", status="COMPLETED"):
    return {
        "status": status,
        "attachments": [
            {"attachment_id": "t1", "text": {"content": text}},
            {
                "attachment_id": "a1",
                "query": {
                    "query": "SELECT 42",
                    "statement_id": "s1",
                    "description": "by total",
                },
            },
        ],
    }


def test_a_completed_question_collects_text_sql_statement_ids_and_the_result_table():
    result = {
        "statement_response": {
            "manifest": {
                "schema": {"columns": [{"name": "total", "type_name": "INT"}]}
            },
            "result": {"data_array": [["42"]]},
        }
    }
    http = Script(
        {
            ("POST", f"{BASE}/start-conversation"): [
                {"conversation_id": "c1", "message_id": "m1"}
            ],
            ("GET", MSG): [_complete_message()],
            ("GET", f"{MSG}/attachments/a1/query-result"): [result],
        }
    )
    answer = ask_genie(
        RetryingHttp(http, sleep=_no_wait), SPACE, "How many?", sleep=_no_wait
    )
    assert answer.status == "COMPLETED"
    assert "total is 42" in answer.text and "by total" in answer.text
    assert answer.sql == ("SELECT 42",)
    assert answer.statement_ids == ("s1",)
    assert answer.tables[0].rows == (("42",),)
    assert answer.message_id == "m1"


def test_polling_waits_until_the_message_is_done():
    http = Script(
        {
            ("POST", f"{BASE}/start-conversation"): [
                {"conversation_id": "c1", "message_id": "m1"}
            ],
            ("GET", MSG): [
                {"status": "EXECUTING_QUERY"},
                {"status": "EXECUTING_QUERY"},
                _complete_message(status="FAILED"),
            ],
            ("GET", f"{MSG}/attachments/a1/query-result"): [{"statement_response": {}}],
        }
    )
    waits = []
    answer = ask_genie(RetryingHttp(http), SPACE, "q", sleep=waits.append)
    assert answer.status == "FAILED"
    assert len(waits) == 2


def test_a_timeout_is_reported_as_timeout():
    http = Script(
        {
            ("POST", f"{BASE}/start-conversation"): [
                {"conversation_id": "c1", "message_id": "m1"}
            ],
            ("GET", MSG): [{"status": "EXECUTING_QUERY"}],
        }
    )
    answer = ask_genie(
        RetryingHttp(http),
        SPACE,
        "q",
        sleep=_no_wait,
        timeout_s=5,
        clock=_clock(0, 1, 9, 9),
    )
    assert answer.status == "TIMEOUT"


def test_a_refused_start_is_an_error_answer_not_an_exception():
    http = Script(
        {("POST", f"{BASE}/start-conversation"): [GenieApiError(403, "no access")]}
    )
    answer = ask_genie(RetryingHttp(http), SPACE, "q", sleep=_no_wait)
    assert answer.status == "ERROR"
    assert "403" in answer.error


def test_a_failed_result_fetch_is_reported_and_the_text_is_kept():
    http = Script(
        {
            ("POST", f"{BASE}/start-conversation"): [
                {"conversation_id": "c1", "message_id": "m1"}
            ],
            ("GET", MSG): [_complete_message()],
            ("GET", f"{MSG}/attachments/a1/query-result"): [GenieApiError(500, "gone")],
        }
    )
    answer = ask_genie(RetryingHttp(http), SPACE, "q", sleep=_no_wait)
    assert answer.status == "COMPLETED"
    assert answer.tables == ()
    assert "query result fetch failed" in answer.error


def test_429_is_retried_with_growing_capped_waits_and_counted():
    waits = []
    flaky = Script(
        {
            ("GET", "/x"): [
                GenieApiError(429, "slow down"),
                GenieApiError(429, "slow"),
                {"ok": True},
            ]
        }
    )
    http = RetryingHttp(flaky, attempts=5, base_s=2.0, cap_s=3.0, sleep=waits.append)
    assert http("GET", "/x", None, None) == {"ok": True}
    assert waits == [2.0, 3.0]
    assert http.retries == 2


def test_other_errors_are_not_retried_and_the_attempts_are_bounded():
    waits = []
    bad = Script({("GET", "/x"): [GenieApiError(404, "missing")]})
    try:
        RetryingHttp(bad, sleep=waits.append)("GET", "/x", None, None)
    except GenieApiError:
        pass
    assert waits == []
    always = Script({("GET", "/y"): [GenieApiError(503, "busy")]})
    try:
        RetryingHttp(always, attempts=3, sleep=waits.append)("GET", "/y", None, None)
    except GenieApiError as exc:
        assert exc.status == 503
    assert len(waits) == 2
