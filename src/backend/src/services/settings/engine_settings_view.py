"""Read and write Configuration → Engines settings as one validated view.

Keeps the router thin and EngineConfigService free of the budget/Jev rules.
"""

from typing import Any, Dict, Optional

from src.core.exceptions import BadRequestError
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
    return {
        "jev_api_base": (stored.get(es.JEV_API_BASE) or "").strip() or None,
        "agent_max_execution_time": _as_int(
            stored.get(es.AGENT_MAX_EXECUTION_TIME),
            es.DEFAULT_AGENT_MAX_EXECUTION_TIME,
            minimum=0,
        ),
        "agent_max_execution_time_default": es.DEFAULT_AGENT_MAX_EXECUTION_TIME,
        "budgets": budgets,
        "budget_defaults": defaults,
    }


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
    if "jev_api_base" in sent:
        url = (sent["jev_api_base"] or "").strip().rstrip("/")
        if url and not url.startswith("https://"):
            raise BadRequestError("The Jev API URL must use https://")
        values[es.JEV_API_BASE] = url
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
    return _effective(await service.save_settings(values))
