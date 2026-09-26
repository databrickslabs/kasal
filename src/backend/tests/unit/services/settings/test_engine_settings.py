"""Configuration → Engines settings: the snapshot, the view and the save path."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.core.exceptions import BadRequestError
from src.services.execution.config.budget_profile import (
    default_profiles,
    resolve_budget_profile,
)
from src.services.settings import (
    engine_settings,
    engine_settings_loader,
    engine_settings_view,
)
from src.services.settings.engine import EngineConfigService


def _row(key, value, engine="kasal", enabled=True):
    return SimpleNamespace(
        engine_name=engine, config_key=key, config_value=value, enabled=enabled
    )


class TestSnapshot:
    def test_empty_snapshot_means_defaults(self):
        engine_settings.forget()
        assert engine_settings.value(engine_settings.JEV_API_BASE) is None
        assert engine_settings.agent_max_execution_time() == 900

    def test_replace_keeps_only_kasal_rows(self):
        engine_settings.replace(
            [
                _row("agent_max_execution_time", "120"),
                _row("agent_max_execution_time", "5", engine="other"),
                _row("jev_api_base", "https://example.com", enabled=False),
            ]
        )
        assert engine_settings.agent_max_execution_time() == 120
        assert engine_settings.value(engine_settings.JEV_API_BASE) is None

    def test_zero_turns_the_agent_limit_off(self):
        engine_settings.apply_row(_row("agent_max_execution_time", "0"))
        assert engine_settings.agent_max_execution_time() == 0

    @pytest.mark.parametrize("bad", ["bogus", "-1", ""])
    def test_unusable_value_falls_back_to_the_default(self, bad):
        engine_settings.apply_row(_row("agent_max_execution_time", bad))
        assert engine_settings.agent_max_execution_time() == 900

    def test_forget_one_key_keeps_the_rest(self):
        engine_settings.replace(
            [
                _row("jev_api_base", "https://example.com"),
                _row("budget_deep_max_iter", "9"),
            ]
        )
        engine_settings.forget("jev_api_base")
        assert engine_settings.value("jev_api_base") is None
        assert engine_settings.value("budget_deep_max_iter") == "9"

    @pytest.mark.asyncio
    async def test_load_never_raises(self):
        with patch(
            "src.db.session.routed_scoped_session", side_effect=RuntimeError("down")
        ):
            await engine_settings_loader.load()


class TestBudgetOverride:
    def test_configured_budget_applies_to_its_mode_only(self):
        engine_settings.apply_row(
            _row(engine_settings.budget_key("deep", "max_iter"), "77")
        )
        assert resolve_budget_profile("deep").max_iter == 77
        assert resolve_budget_profile("research").max_iter == 15


def _service(stored=None):
    service = AsyncMock(spec=EngineConfigService)
    service.get_settings.return_value = stored or {}
    service.save_settings.side_effect = lambda values: {
        k: v for k, v in values.items() if v
    }
    return service


class TestView:
    @pytest.mark.asyncio
    async def test_view_fills_defaults(self):
        view = await engine_settings_view.get_view(_service())
        assert view["jev_api_base"] is None
        assert view["agent_max_execution_time"] == 900
        # Only modes a run applies are editable (deep, today).
        assert view["budgets"] == {"deep": default_profiles()["deep"]}
        assert view["budget_defaults"] == view["budgets"]

    @pytest.mark.asyncio
    async def test_update_saves_only_what_was_sent(self):
        service = _service()
        view = await engine_settings_view.update_view(
            service,
            {
                "jev_api_base": " https://example.com/ ",
                "budgets": {"deep": {"max_iter": 40, "run_wall_clock": None}},
            },
        )
        service.save_settings.assert_awaited_once_with(
            {
                "jev_api_base": "https://example.com",
                "budget_deep_max_iter": "40",
                "budget_deep_run_wall_clock": "",
            }
        )
        assert view["jev_api_base"] == "https://example.com"
        assert view["budgets"]["deep"]["max_iter"] == 40

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sent, message",
        [
            ({"jev_api_base": "http://example.com"}, "https"),
            ({"budgets": {"turbo": {"max_iter": 3}}}, "No run budget"),
            ({"budgets": {"chat": {"max_iter": 3}}}, "No run budget"),
            ({"budgets": {"deep": {"bogus": 3}}}, "Unknown budget field"),
            ({"budgets": {"deep": {"max_iter": 0}}}, "at least 1"),
        ],
    )
    async def test_update_rejects_invalid_values(self, sent, message):
        service = _service()
        with pytest.raises(BadRequestError, match=message):
            await engine_settings_view.update_view(service, sent)
        service.save_settings.assert_not_awaited()


class TestSaveSettings:
    @pytest.mark.asyncio
    async def test_upserts_and_updates_the_snapshot(self):
        service = EngineConfigService(AsyncMock())
        service.repository = AsyncMock()
        existing = _row("jev_api_base", "https://old.example.com")
        service.repository.find_by_engine_and_key.side_effect = lambda engine, key: (
            existing if key == "jev_api_base" else None
        )

        async def update(engine, key, value):
            existing.config_value = value
            return True

        service.repository.update_config_value.side_effect = update
        service.repository.create.side_effect = lambda data: _row(
            data["config_key"], data["config_value"]
        )
        service.find_by_engine_type = AsyncMock(return_value=[])

        await service.save_settings(
            {"jev_api_base": "https://example.com", "budget_deep_max_iter": "12"}
        )

        service.repository.update_config_value.assert_awaited_once()
        service.repository.create.assert_awaited_once()
        assert engine_settings.value("jev_api_base") == "https://example.com"
        assert resolve_budget_profile("deep").max_iter == 12


class TestScalarSettings:
    """Server-wide memory/knowledge knobs (were KASAL_MEMORY_SWEEP*, KNOWLEDGE_*)."""

    def test_defaults_without_rows(self):
        engine_settings.forget()
        assert engine_settings.setting(engine_settings.MEMORY_SWEEP_ENABLED) is True
        assert engine_settings.setting(engine_settings.KNOWLEDGE_TTL_DAYS) == 7

    def test_typed_and_bounded(self):
        engine_settings.replace(
            [
                _row("memory_sweep_enabled", "false"),
                _row("knowledge_min_score", "0.5"),
                _row("memory_sweep_batch", "1000"),  # above its maximum
            ]
        )
        assert engine_settings.setting("memory_sweep_enabled") is False
        assert engine_settings.setting("knowledge_min_score") == 0.5
        assert engine_settings.setting("memory_sweep_batch") == 5

    def test_consumers_read_the_snapshot(self):
        from src.services.knowledge.search_guard import (
            KnowledgeSearchBudget,
            configured_min_score,
        )
        from src.services.memory.maintenance.sweep import sweep_enabled

        engine_settings.replace(
            [
                _row("memory_sweep_enabled", "false"),
                _row("knowledge_min_score", "0.6"),
                _row("knowledge_max_searches", "2"),
            ]
        )
        assert sweep_enabled() is False
        assert configured_min_score() == 0.6
        assert KnowledgeSearchBudget().max_searches == 2

    @pytest.mark.asyncio
    async def test_view_round_trips_advanced(self):
        service = _service()
        view = await engine_settings_view.update_view(
            service,
            {"advanced": {"knowledge_ttl_days": 30, "memory_sweep_enabled": None}},
        )
        service.save_settings.assert_awaited_once_with(
            {"knowledge_ttl_days": "30", "memory_sweep_enabled": ""}
        )
        assert view["advanced"]["knowledge_ttl_days"] == 30
        assert view["advanced"]["memory_sweep_enabled"] is True
        assert view["advanced_specs"]["knowledge_ttl_days"]["maximum"] == 3650

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sent, message",
        [
            ({"nope": 1}, "Unknown engine setting"),
            ({"knowledge_ttl_days": -1}, "whole number from 0 to 3650"),
            ({"knowledge_min_score": 2.0}, "number from 0 to 1"),
            ({"memory_sweep_enabled": 1}, "wrong type"),
        ],
    )
    async def test_advanced_rejects_invalid(self, sent, message):
        with pytest.raises(BadRequestError, match=message):
            await engine_settings_view.update_view(_service(), {"advanced": sent})
