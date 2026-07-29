"""
Execution logs — the Logs tab, end to end.

    python logging  ->  db_handler  ->  queue  ->  writer  ->  execution_logs
    engine printer  ->  capture     ->  ^

Division of labour, so this does not sprawl again:
- ``src/core/logger.py`` owns LOGGERS (names, files, formatters). It knows
  nothing about executions.
- this package owns CAPTURE (getting a run's output into the database).
- the orchestrator owns neither; it calls ``subprocess_bootstrap`` and moves on.

Traces are a different pipeline entirely (OTel -> ``services/trace``); note that
``writer_task.py`` is named for history and drives the LOGS writer, not traces.
"""

from src.services.execution.logs.context import (
    ExecutionContextFormatter,
    clear_execution_context,
    execution_logging_context,
    set_execution_context,
)
from src.services.execution.logs.db_handler import ExecutionLogsDatabaseHandler

__all__ = [
    'ExecutionContextFormatter',
    'set_execution_context',
    'clear_execution_context',
    'execution_logging_context',
    'ExecutionLogsDatabaseHandler',
]
