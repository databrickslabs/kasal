"""Shared types and narrowing helpers for the UC Metric View generator steps.

Values that arrive from other tools are untyped JSON. The helpers here narrow
``object`` to the shape a step needs; a value of the wrong shape becomes an
empty default instead of raising, so the step modules stay free of ``Any``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Protocol, TypedDict, TypeVar

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class RunAsync(Protocol):
    """Runs a coroutine to completion from synchronous tool code."""

    def __call__(self, coro: Coroutine[object, object, _T]) -> _T: ...


class UCMVDiagnostics(TypedDict, total=False):
    """Run-level diagnostics carried into the tool output as ``_diagnostics``."""

    preinject_measures_json_chars: int | None
    preinject_mquery_json_chars: int | None
    preinject_config_json_chars: int | None
    preinject_visual_usage_index_chars: int | None
    preinject_implicit_column_measures_chars: int | None
    db_fallback_fired_for: list[str]
    db_fallback_extraction_id: int | None
    db_fallback_error: str
    trace_context_seen: object


@dataclass
class RawInputs:
    """The JSON inputs as they arrived: JSON text, a decoded value, or absent."""

    measures: object
    mquery: object
    config: object
    relationships: object
    scan: object
    visual_usage: object
    implicit_column_measures: object


def as_dict(value: object) -> dict[str, object]:
    """``value`` itself when it is a dict, otherwise an empty dict."""
    return value if isinstance(value, dict) else {}


def as_list(value: object) -> list[object]:
    """``value`` itself when it is a list, otherwise an empty list."""
    return value if isinstance(value, list) else []


def as_str(value: object) -> str:
    """``value`` itself when it is a string, otherwise an empty string."""
    return value if isinstance(value, str) else ""


def parse_json_input(raw: object, default: object) -> object:
    """Parse a JSON input; a blank value yields the default, never an error."""
    if not isinstance(raw, str):
        return raw if raw is not None else default
    text = raw.strip()
    if not text:
        return default
    decoded: object = json.loads(text)
    return decoded


def execution_id_for(tool: object) -> str | None:
    """The tool's execution id, using the lookup the generator has always used."""
    try:
        from src.services.execution.kernel.trace_context import (
            resolve_tool_execution_id,
        )

        return resolve_tool_execution_id(tool)
    except Exception:
        logger.debug(
            "[UCMV] execution id lookup failed; falling back to trace_context",
            exc_info=True,
        )
        job_id = as_dict(getattr(tool, "trace_context", None)).get("job_id")
        return job_id if isinstance(job_id, str) else None
