"""
Unit tests for src/services/execution/logs/context.py
"""

import asyncio
import io
import logging
import os
import sys
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call, patch

import pytest

from src.services.execution.logs.context import (
    ExecutionContextFormatter,
    _execution_context,
    clear_execution_context,
    execution_logging_context,
    set_execution_context,
)


class TestExecutionContextFormatter:
    """Tests for ExecutionContextFormatter."""

    def test_format_with_execution_id(self):
        fmt = "[CREW] %(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        set_execution_context("abcdef1234567890")
        try:
            record = logging.LogRecord(
                name="crew",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="test message",
                args=(),
                exc_info=None,
            )
            result = formatter.format(record)
            assert "abcdef12" in result
            assert "test message" in result
        finally:
            clear_execution_context()

    def test_format_without_execution_id(self):
        fmt = "[CREW] %(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        clear_execution_context()
        record = logging.LogRecord(
            name="crew",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="no exec id",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert "no exec id" in result

    def test_format_flow_prefix(self):
        fmt = "[FLOW] %(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        assert formatter._prefix == "[FLOW]"

    def test_format_default_prefix(self):
        formatter = ExecutionContextFormatter()
        assert formatter._prefix == "[CREW]"

    def test_prefix_no_match(self):
        fmt = "%(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        # When no [SOMETHING] prefix, falls back to [CREW]
        assert formatter._prefix == "[CREW]"


# ---------------------------------------------------------------------------
# set/clear execution context
# ---------------------------------------------------------------------------


class TestExecutionContext:
    """Tests for context variable helpers."""

    def test_set_and_clear(self):
        set_execution_context("exec-1234")
        assert _execution_context.get() == "exec-1234"
        clear_execution_context()
        assert _execution_context.get() is None

    def test_execution_logging_context(self):
        with execution_logging_context("exec-abcd"):
            assert _execution_context.get() == "exec-abcd"
        assert _execution_context.get() is None

    def test_execution_logging_context_clears_on_exception(self):
        try:
            with execution_logging_context("exec-xyz"):
                raise ValueError("boom")
        except ValueError:
            pass
        assert _execution_context.get() is None


# ---------------------------------------------------------------------------
# suppress / restore stdout/stderr
# ---------------------------------------------------------------------------
