"""Read and write Configuration → Engines settings as one validated view.

Keeps the router thin and EngineConfigService free of the budget/Jev rules.
"""

from typing import Any, Dict, Optional
from urllib.parse import urlparse

from src.core.exceptions import BadRequestError
from src.services.decisions import connection as decision_connection
from src.services.execution.config.budget_profile import (
    budget_field_names,
    default_profiles,
)
from src.services.generation.crew.answer_mode import GATED_MODES
from src.services.settings import engine_settings as es
from src.services.settings.engine import EngineConfigService


def _applied_defaults() -> Dict[str, Dict[str, int]]:
    """Built-in budgets for the modes a run actually applies (only those are
    editable: a knob on a mode nothing enforces would be inert)."""
    return {m: f for m, f in default_profiles().items() if m in GATED_MODES}


def _effective(stored: Dict[str, str]) -> Dict[str, Any]:
    defaults = _applied_defaults()
    budgets = {
        mode: {
            field: _as_int(stored.get(es.budget_key(mode, field)), default)
            for field, default in fields.items()
        }
        for mode, fields in defaults.items()
    }
    # The effective decision connection: an old jev_api_base on OpenRouter
    # reads as the OpenRouter connection with that URL (see decisions.connection).
    connection = decision_connection.resolve(stored)
    return {
        "jev_api_base": connection.jev_api_base,
        "decision_connection": connection.kind,
        "openrouter_api_base": connection.openrouter_api_base,
        "agent_max_execution_time": _as_int(
            stored.get(es.AGENT_MAX_EXECUTION_TIME),
            es.DEFAULT_AGENT_MAX_EXECUTION_TIME,
            minimum=0,
        ),
        "agent_max_execution_time_default": es.DEFAULT_AGENT_MAX_EXECUTION_TIME,
        "budgets": budgets,
        "budget_defaults": defaults,
        "advanced": {key: _typed(key, stored.get(key)) for key in es.SETTINGS},
        "advanced_specs": {
            key: {
                "default": spec.default,
                "minimum": spec.minimum,
                "maximum": spec.maximum,
            }
            for key, spec in es.SETTINGS.items()
        },
    }


def _typed(key: str, raw: Optional[str]) -> Any:
    parsed = es.parse(key, raw)
    return es.SETTINGS[key].default if parsed is None else parsed


def _as_int(raw: Optional[str], default: int, minimum: int = 1) -> int:
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


async def get_view(service: EngineConfigService) -> Dict[str, Any]:
    return _effective(await service.get_settings())


async def update_view(
    service: EngineConfigService, sent: Dict[str, Any]
) -> Dict[str, Any]:
    """Validate and save the fields the caller sent (``exclude_unset`` dict)."""
    values: Dict[str, str] = {}
    for field, key, label in (
        ("jev_api_base", es.JEV_API_BASE, "Jev API"),
        ("openrouter_api_base", es.OPENROUTER_API_BASE, "OpenRouter"),
    ):
        if field in sent:
            values[key] = _http_url(sent[field], label)
    if "decision_connection" in sent:
        kind = (sent["decision_connection"] or "").strip().lower()
        if kind and kind not in decision_connection.CONNECTIONS:
            raise BadRequestError(
                "The decision model connection must be 'jev' or 'openrouter'"
            )
        values[es.DECISION_CONNECTION] = kind
    if "agent_max_execution_time" in sent:
        value = sent["agent_max_execution_time"]
        values[es.AGENT_MAX_EXECUTION_TIME] = "" if value is None else str(value)
    if sent.get("budgets"):
        defaults = _applied_defaults()
        allowed = set(budget_field_names())
        for mode, fields in sent["budgets"].items():
            if mode not in defaults:
                raise BadRequestError(f"No run budget applies to answer mode '{mode}'")
            for field, value in (fields or {}).items():
                if field not in allowed:
                    raise BadRequestError(f"Unknown budget field '{field}'")
                if value is not None and value < 1:
                    raise BadRequestError(f"{mode}.{field} must be at least 1")
                values[es.budget_key(mode, field)] = "" if value is None else str(value)
    values.update(_advanced_values(sent.get("advanced") or {}))
    return _effective(await service.save_settings(values))


def _http_url(raw: Optional[str], label: str) -> str:
    """``raw`` without a trailing slash; empty clears it. http(s) only."""
    url = (raw or "").strip().rstrip("/")
    parsed = urlparse(url)
    if url and (parsed.scheme not in ("http", "https") or not parsed.netloc):
        raise BadRequestError(f"The {label} URL must be an http:// or https:// address")
    return url


def _advanced_values(sent: Dict[str, Any]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for key, value in sent.items():
        spec = es.SETTINGS.get(key)
        if spec is None:
            raise BadRequestError(f"Unknown engine setting '{key}'")
        if value is None:
            values[key] = ""
            continue
        if isinstance(spec.default, bool) != isinstance(value, bool) or isinstance(
            spec.default, str
        ) != isinstance(value, str):
            raise BadRequestError(f"{key} has the wrong type")
        if es.parse(key, str(value)) is None:
            raise BadRequestError(
                f"{key} must be a "
                f"{'whole number' if isinstance(spec.default, int) else 'number'}"
                f" from {spec.minimum:g} to {spec.maximum:g}"
            )
        values[key] = str(value)
    return values
