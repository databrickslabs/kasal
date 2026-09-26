"""A2UI runtime settings: system defaults a workspace may override.

They replaced the A2UI_* environment variables. The system switch is a kill
switch; the five runtime knobs are overridable per workspace.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.ui_config import UIConfigUpdate
from src.services.a2ui import settings as a2ui_settings


class TestEffective:
    def test_defaults_without_rows_or_overrides(self):
        values = a2ui_settings.effective()
        assert values["a2ui_enabled"] is True
        assert values["a2ui_compose_retries"] == 2
        assert values["a2ui_compose_timeout"] == 240.0

    def test_a_system_default_applies_to_every_workspace(self, engine_setting):
        engine_setting("a2ui_compose_retries", 5)
        assert a2ui_settings.effective()["a2ui_compose_retries"] == 5
        assert (
            a2ui_settings.effective('{"a2ui_streaming": false}')["a2ui_compose_retries"]
            == 5
        )

    def test_a_workspace_override_wins(self, engine_setting):
        engine_setting("a2ui_compose_retries", 5)
        values = a2ui_settings.effective(
            json.dumps({"a2ui_compose_retries": 3, "a2ui_streaming": False})
        )
        assert values["a2ui_compose_retries"] == 3
        assert values["a2ui_streaming"] is False

    def test_unusable_overrides_are_ignored(self):
        values = a2ui_settings.effective(
            json.dumps({"a2ui_compose_retries": 999, "a2ui_enabled": False})
        )
        assert values["a2ui_compose_retries"] == 2
        assert values["a2ui_enabled"] is True, "the kill switch is system-only"
        assert a2ui_settings.effective("not json")["a2ui_compose_retries"] == 2


class TestValidation:
    def test_clean_values_are_normalised(self):
        assert a2ui_settings.validate_overrides(None) is None
        assert a2ui_settings.validate_overrides("{}") is None
        assert (
            a2ui_settings.validate_overrides(
                '{"a2ui_streaming": false, "a2ui_compose_retries": 3}'
            )
            == '{"a2ui_compose_retries": 3, "a2ui_streaming": false}'
        )

    @pytest.mark.parametrize(
        "raw, message",
        [
            ("[1]", "JSON object"),
            ("{bad", "not valid JSON"),
            ('{"a2ui_enabled": false}', "cannot be set per workspace"),
            ('{"a2ui_compose_retries": 0}', "from 1 to 10"),
            ('{"a2ui_streaming": 1}', "wrong type"),
        ],
    )
    def test_the_schema_rejects(self, raw, message):
        with pytest.raises(ValueError, match=message):
            UIConfigUpdate(settings_json=raw)


class TestKillSwitch:
    @pytest.mark.asyncio
    async def test_system_off_beats_a_workspace_that_is_on(self, engine_setting):
        from src.services.a2ui import runner

        engine_setting("a2ui_enabled", False)
        cfg = SimpleNamespace(
            enabled=True,
            settings_json='{"a2ui_compose_retries": 4}',
            catalog_type="full",
            catalog_json=None,
            disabled_components=None,
            style_json=None,
        )
        service = MagicMock()
        service.get_config = AsyncMock(return_value=cfg)
        with (
            patch("src.services.settings.ui.UIConfigService", return_value=service),
            patch("src.db.session.routed_scoped_session") as session,
        ):
            session.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            session.return_value.__aexit__ = AsyncMock(return_value=None)
            enabled, _catalog, _guidance, settings = await runner._resolve_config(
                "g1", "show a dashboard"
            )
        assert enabled is False
        assert settings["a2ui_compose_retries"] == 4


class TestSystemDefaultsReachTheForm:
    @pytest.mark.asyncio
    async def test_get_config_carries_the_system_defaults(self, engine_setting):
        from src.services.settings.ui import UIConfigService

        engine_setting("a2ui_compose_timeout", 90)
        service = UIConfigService(AsyncMock(), group_id="g1")
        service.repository = MagicMock()
        service.repository.get_for_group = AsyncMock(return_value=None)
        response = await service.get_config()
        assert response.system_defaults["a2ui_compose_timeout"] == 90.0
        assert "a2ui_enabled" not in response.system_defaults
