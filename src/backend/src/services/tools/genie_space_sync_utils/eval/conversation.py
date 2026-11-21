"""Ask a Genie space one question: start a conversation, poll the message, fetch results.

Uses the same Genie REST calls as the rest of the tool (``GenieHttp``, paths relative
to ``/api/2.0/genie/spaces``): ``POST /{space}/start-conversation``, then ``GET`` of the
message until it is done, then ``GET .../attachments/{id}/query-result`` for each query
attachment. Failures become an ``Answer`` with status ERROR or TIMEOUT, never an
exception, so one bad question does not stop a run.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from urllib.parse import quote

from src.services.tools.genie_space_sync_utils.eval.answer import ERROR, TIMEOUT, Answer
from src.services.tools.genie_space_sync_utils.eval.retry import RetryingHttp
from src.services.tools.genie_space_sync_utils.eval.tables import (
    ResultTable,
    statement_table,
)
from src.services.tools.genie_space_sync_utils.genie_api import GenieApiError
from src.services.tools.genie_space_sync_utils.verdicts import short

_DONE = frozenset({"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"})
_POLL_S = 2.0
_TIMEOUT_S = 600.0


def _id(value: object) -> str:
    return quote(value if isinstance(value, str) else "", safe="")


def ask_genie(
    http: RetryingHttp,
    space_id: str,
    question: str,
    *,
    poll_s: float = _POLL_S,
    timeout_s: float = _TIMEOUT_S,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Answer:
    """One question in a new conversation. The latency is question-to-complete-message, without downloads."""
    retries_before = http.retries
    started = clock()
    try:
        opened = http(
            "POST", f"/{space_id}/start-conversation", {"content": question}, None
        )
        conversation = _id(opened.get("conversation_id"))
        message_id = _id(opened.get("message_id"))
        if not conversation or not message_id:
            raise GenieApiError(
                0, "start-conversation returned no conversation or message id"
            )
        path = f"/{space_id}/conversations/{conversation}/messages/{message_id}"
        message = http("GET", path, None, None)
        while message.get("status") not in _DONE:
            if clock() - started > timeout_s:
                return _failed(
                    http,
                    started,
                    retries_before,
                    TIMEOUT,
                    message_id,
                    clock,
                    "timed out",
                )
            sleep(poll_s)
            message = http("GET", path, None, None)
        latency = round(clock() - started, 2)
        return _finished(http, path, message, message_id, latency, retries_before)
    except (GenieApiError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        return _failed(http, started, retries_before, ERROR, "", clock, short(exc))


def _failed(
    http: RetryingHttp,
    started: float,
    retries_before: int,
    status: str,
    message_id: str,
    clock: Callable[[], float],
    error: str,
) -> Answer:
    return Answer(
        agent="genie",
        status=status,
        error=error,
        latency_s=round(clock() - started, 2),
        message_id=message_id,
        rate_limit_retries=http.retries - retries_before,
    )


def _finished(
    http: RetryingHttp,
    path: str,
    message: dict[str, object],
    message_id: str,
    latency: float,
    retries_before: int,
) -> Answer:
    attachments = message.get("attachments")
    items = (
        [a for a in attachments if isinstance(a, dict)]
        if isinstance(attachments, list)
        else []
    )
    texts = [_text_of(a) for a in items]
    queries = [a for a in items if isinstance(a.get("query"), dict)]
    sql = tuple(_query_text(a) for a in queries)
    statement_ids = tuple(sid for sid in (_statement_id(a) for a in queries) if sid)
    tables, fetch_error = _fetch_tables(http, path, queries)
    error = _message_error(message)
    if fetch_error and not error:
        error = fetch_error
    return Answer(
        agent="genie",
        status=str(message.get("status") or ERROR),
        text="\n\n".join(t for t in texts if t),
        sql=sql,
        tables=tuple(tables),
        error=error,
        latency_s=latency,
        message_id=message_id,
        statement_ids=statement_ids,
        rate_limit_retries=http.retries - retries_before,
    )


def _text_of(attachment: dict[str, object]) -> str:
    text = attachment.get("text")
    content = text.get("content") if isinstance(text, dict) else None
    description = ""
    query = attachment.get("query")
    if isinstance(query, dict) and isinstance(query.get("description"), str):
        description = query["description"].strip()
    own = content.strip() if isinstance(content, str) else ""
    return "\n".join(part for part in (own, description) if part)


def _query_text(attachment: dict[str, object]) -> str:
    query = attachment.get("query")
    sql = query.get("query") if isinstance(query, dict) else None
    return sql if isinstance(sql, str) else ""


def _statement_id(attachment: dict[str, object]) -> str:
    query = attachment.get("query")
    sid = query.get("statement_id") if isinstance(query, dict) else None
    return sid if isinstance(sid, str) else ""


def _fetch_tables(
    http: RetryingHttp, path: str, queries: list[dict[str, object]]
) -> tuple[list[ResultTable], str]:
    """The result table of each query attachment; a failed fetch is reported, not raised."""
    tables: list[ResultTable] = []
    for attachment in queries:
        attachment_id = _id(attachment.get("attachment_id"))
        if not attachment_id:
            continue
        try:
            body = http(
                "GET", f"{path}/attachments/{attachment_id}/query-result", None, None
            )
        except GenieApiError as exc:
            return tables, f"query result fetch failed: {short(exc)}"
        payload = body.get("statement_response")
        tables.append(statement_table(payload if isinstance(payload, dict) else {}))
    return tables, ""


def _message_error(message: dict[str, object]) -> str:
    error = message.get("error")
    if isinstance(error, dict):
        return short(error.get("error") or error.get("message") or "")
    return short(error) if error else ""
