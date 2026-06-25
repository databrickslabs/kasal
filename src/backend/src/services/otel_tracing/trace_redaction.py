"""Secret redaction for MLflow trace spans.

MLflow's CrewAI/LiteLLM autolog (``mlflow.crewai.autolog`` / ``mlflow.litellm.autolog``)
captures raw method inputs — e.g. the ``Task.execute_sync`` span input includes the full
CrewAI ``Agent`` object whose ``llm.api_key`` is the user's short-lived Databricks OBO access
token. Anyone with read access to the experiment could lift and replay it. This module
registers an MLflow span processor that scrubs credential-shaped values from every span's
inputs, outputs, and attributes BEFORE the trace is exported.

Registration is process-wide and idempotent — call :func:`enable_secret_redaction` from each
place that enables autolog (it only configures MLflow once per process).

The processor is intentionally defensive: it never raises, so a redaction bug can never break
tracing or crew execution.
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_REDACTED = "[REDACTED]"
_REDACTED_JWT = "[REDACTED_JWT]"

# Dict keys (case-insensitive substring match) whose VALUE is a credential and must be
# replaced wholesale, regardless of value type.
_SECRET_KEY_SUBSTRINGS = (
    "api_key",
    "apikey",
    "api-key",
    "access_token",
    "refresh_token",
    "auth_token",
    "authorization",
    "password",
    "passwd",
    "secret",          # covers client_secret, secret_key, ...
    "credential",
    "private_key",
    "bearer",
    "x-forwarded-access-token",
    "x-databricks-token",
)

# JWT / opaque-bearer token shapes embedded inside free-text string values
# (e.g. an "Authorization: Bearer eyJ..." header serialized into a span attribute).
_JWT_RE = re.compile(r"eyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}")
# Databricks PATs (dapi...) and OAuth secrets (dose...) as a backstop.
_DAPI_RE = re.compile(r"\bdap[ie][A-Za-z0-9]{16,}\b")

_MAX_DEPTH = 12  # guard against pathological/cyclic structures


def _is_secret_key(key: Any) -> bool:
    if not isinstance(key, str):
        return False
    k = key.lower()
    return any(s in k for s in _SECRET_KEY_SUBSTRINGS)


def _scrub_string(value: str) -> str:
    out = _JWT_RE.sub(_REDACTED_JWT, value)
    out = _DAPI_RE.sub(_REDACTED, out)
    return out


def scrub(value: Any, _depth: int = 0) -> Any:
    """Return a redacted copy of ``value`` (dict/list/str/scalar) — never mutates input."""
    if _depth > _MAX_DEPTH:
        return value
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            out[k] = _REDACTED if _is_secret_key(k) else scrub(v, _depth + 1)
        return out
    if isinstance(value, (list, tuple)):
        scrubbed = [scrub(v, _depth + 1) for v in value]
        return type(value)(scrubbed) if isinstance(value, tuple) else scrubbed
    if isinstance(value, str):
        return _scrub_string(value)
    return value


def redact_span(span: Any) -> None:
    """MLflow span processor: strip credential-shaped values from a span before export.

    Registered via ``mlflow.tracing.configure(span_processors=[redact_span])``. Receives a
    ``LiveSpan`` at span end. Defensive — any failure is logged and swallowed so tracing and
    crew execution are never broken by redaction.
    """
    try:
        inputs = span.inputs
        if inputs is not None:
            red = scrub(inputs)
            if red != inputs:
                span.set_inputs(red)

        outputs = span.outputs
        if outputs is not None:
            red = scrub(outputs)
            if red != outputs:
                span.set_outputs(red)

        attributes = span.attributes or {}
        changed = {}
        for key, val in attributes.items():
            # mlflow.spanInputs / mlflow.spanOutputs are already covered by
            # set_inputs/set_outputs above; this catches every other attribute.
            new_val = _REDACTED if _is_secret_key(key) else scrub(val)
            if new_val != val:
                changed[key] = new_val
        if changed:
            span.set_attributes(changed)
    except Exception as e:  # never break tracing
        logger.debug(f"[trace_redaction] redact_span failed (ignored): {e}")


_redaction_enabled = False


def enable_secret_redaction() -> bool:
    """Register :func:`redact_span` as an MLflow span processor (idempotent, process-wide).

    Returns True if redaction is active after the call, False if MLflow/tracing config is
    unavailable.
    """
    global _redaction_enabled
    if _redaction_enabled:
        return True
    try:
        import mlflow

        configure = getattr(getattr(mlflow, "tracing", None), "configure", None)
        if configure is None:
            logger.info("[trace_redaction] mlflow.tracing.configure unavailable — redaction not enabled")
            return False
        configure(span_processors=[redact_span])
        _redaction_enabled = True
        logger.info("✅ [trace_redaction] MLflow span secret-redaction enabled")
        return True
    except Exception as e:
        logger.warning(f"[trace_redaction] Could not enable span redaction: {e}")
        return False
