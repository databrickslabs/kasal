"""
Execution-id logging context.

The contextvar every execution log line is stamped with, and the formatter that
reads it. A contextvar (not a thread-local) because the whole stack is async:
the id has to survive an await.
"""

import contextvars
import logging
import re
from contextlib import contextmanager
from typing import Optional

# Context variable for execution context (works with async/await)
_execution_context: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'execution_id', default=None
)


class ExecutionContextFormatter(logging.Formatter):
    """
    Custom formatter that adds execution ID to log messages.
    Preserves the prefix from the original format string.
    """

    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)
        # Extract prefix from original format (e.g., "[FLOW]" or "[CREW]")
        self._original_fmt = fmt or '[CREW] %(asctime)s - %(levelname)s - %(message)s'
        # Extract the prefix by finding the pattern [SOMETHING]
        import re
        match = re.match(r'(\[[\w]+\])', self._original_fmt)
        self._prefix = match.group(1) if match else '[CREW]'

    def format(self, record):
        # Get execution ID from context variable (works with async/await)
        execution_id = _execution_context.get()

        if execution_id:
            # Add execution ID to the format
            record.exec_id = f"[{execution_id[:8]}]"
        else:
            record.exec_id = ""

        # Use the original format with execution ID, preserving the prefix
        if record.exec_id:
            self._style._fmt = f'{self._prefix}%(exec_id)s %(asctime)s - %(levelname)s - %(message)s'
        else:
            self._style._fmt = f'{self._prefix} %(asctime)s - %(levelname)s - %(message)s'

        return super().format(record)


def set_execution_context(execution_id: str):
    """
    Set the execution ID for the current context (works with async/await).

    Args:
        execution_id: The execution ID to associate with logs
    """
    _execution_context.set(execution_id)


def clear_execution_context():
    """
    Clear the execution context for the current context (works with async/await).
    """
    _execution_context.set(None)


@contextmanager
def execution_logging_context(execution_id: str):
    """
    Context manager for execution-specific logging.

    Args:
        execution_id: The execution ID to use for logging
    """
    set_execution_context(execution_id)
    try:
        yield
    finally:
        clear_execution_context()

