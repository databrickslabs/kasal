"""The A2UI runtime settings a run uses: system defaults, workspace overrides.

A system administrator sets the defaults for every workspace (Configuration →
Output design, system scope; ``engine_settings`` rows). A workspace may override
any of :data:`OVERRIDABLE` in its own Output design (``ui_config.settings_json``).
They replaced the A2UI_* environment variables, which a Databricks App never sets.

``a2ui_enabled`` is deliberately NOT overridable: the system switch is a kill
switch (off means off everywhere), and each workspace already has its own on/off
(``ui_config.enabled``).
"""

import json
import logging
from typing import Any, Dict, Optional

from src.services.settings import engine_settings

logger = logging.getLogger(__name__)

OVERRIDABLE = (
    "a2ui_early",
    "a2ui_streaming",
    "a2ui_stream_interval_ms",
    "a2ui_compose_retries",
    "a2ui_compose_timeout",
)


def _parse(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def validate_overrides(raw: Optional[str]) -> Optional[str]:
    """Reject unknown keys and out-of-range values; ``None``/``""``/``{}`` clear."""
    if raw is None or raw.strip() in ("", "{}"):
        return None
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"settings_json is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("settings_json must be a JSON object")
    for key, value in data.items():
        if key not in OVERRIDABLE:
            raise ValueError(f"'{key}' cannot be set per workspace")
        spec = engine_settings.SETTINGS[key]
        if isinstance(spec.default, bool) != isinstance(value, bool):
            raise ValueError(f"'{key}' has the wrong type")
        if engine_settings.parse(key, str(value)) is None:
            raise ValueError(
                f"'{key}' must be from {spec.minimum:g} to {spec.maximum:g}"
            )
    return json.dumps(data, sort_keys=True) if data else None


def system_defaults() -> Dict[str, Any]:
    """The system-wide values of every workspace-overridable setting."""
    return {key: engine_settings.setting(key) for key in OVERRIDABLE}


def effective(settings_json: Optional[str] = None) -> Dict[str, Any]:
    """System defaults with this workspace's valid overrides applied."""
    values = {key: engine_settings.setting(key) for key in OVERRIDABLE}
    values["a2ui_enabled"] = engine_settings.setting("a2ui_enabled")
    for key, value in _parse(settings_json).items():
        if key in OVERRIDABLE:
            parsed = engine_settings.parse(key, str(value))
            if parsed is not None:
                values[key] = parsed
    return values


async def for_group(group_id: Optional[str]) -> Dict[str, Any]:
    """:func:`effective` for a workspace, reading its UI config. Never raises."""
    if not group_id:
        return effective()
    try:
        from src.db.session import routed_scoped_session
        from src.services.settings.ui import UIConfigService

        async with routed_scoped_session() as session:
            cfg = await UIConfigService(session, group_id=group_id).get_config()
        return effective(getattr(cfg, "settings_json", None))
    except Exception as exc:  # noqa: BLE001 — formatting must never break a run
        logger.warning("[a2ui] workspace settings unavailable (%s); defaults", exc)
        return effective()
