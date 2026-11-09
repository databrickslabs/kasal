"""The answer record both agents (Genie and Fabric) produce, so grading sees one shape."""

from __future__ import annotations

from dataclasses import dataclass

from src.services.tools.genie_space_sync_utils.eval.tables import ResultTable

COMPLETED = "COMPLETED"
ERROR = "ERROR"
TIMEOUT = "TIMEOUT"


@dataclass(frozen=True)
class Answer:
    agent: str
    status: str
    text: str = ""
    sql: tuple[str, ...] = ()
    tables: tuple[ResultTable, ...] = ()
    error: str = ""
    latency_s: float | None = None
    message_id: str = ""
    statement_ids: tuple[str, ...] = ()
    rate_limit_retries: int = 0
    total_tokens: int | None = None

    @property
    def completed(self) -> bool:
        return self.status == COMPLETED
