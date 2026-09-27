"""Decision model connections: Jev API or OpenRouter.

The setting, the migration of an OpenRouter ``jev_api_base``, availability per
connection and key, Auto under each connection, and the other policies
abstaining under OpenRouter.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.exceptions import BadRequestError
from src.schemas.decision_config import DecisionConfigUpdate
from src.services.decisions import connection, model_selection, provider, runtime
from src.services.decisions.jev_router import select_router
from src.services.decisions.model_selection import ModelSelection, select_for_workspace
from src.services.decisions.policies import question
from src.services.decisions.settings import DecisionSettingsService
from src.services.settings import engine_settings as es
from src.services.settings import engine_settings_view

OPENROUTER_URL = "https://openrouter.ai/api/v1"


def configure(monkeypatch, **values):
    """Set engine settings in the snapshot; None clears one."""
    for key, value in values.items():
        if value is None:
            monkeypatch.delitem(es._snapshot, key, raising=False)
        else:
            monkeypatch.setitem(es._snapshot, key, value)


class TestResolve:
    def test_default_is_the_jev_connection_unconfigured(self):
        current = connection.resolve({})
        assert (current.kind, current.jev_api_base, current.configured) == (
            "jev",
            None,
            False,
        )
        assert current.key_name == "JEV_API_KEY"
        assert current.openrouter_api_base == OPENROUTER_URL

    def test_an_openrouter_jev_url_reads_as_the_openrouter_connection(self):
        """The migration: the old single URL pointed at OpenRouter."""
        current = connection.resolve({es.JEV_API_BASE: "https://openrouter.ai/api/v1/"})
        assert current.kind == "openrouter"
        assert current.jev_api_base is None
        assert current.openrouter_api_base == OPENROUTER_URL
        assert current.key_name == "OPENROUTER_API_KEY"
        assert current.configured

    def test_an_explicit_connection_wins_over_the_migration(self):
        current = connection.resolve(
            {es.DECISION_CONNECTION: "jev", es.JEV_API_BASE: "https://example.com"}
        )
        assert (current.kind, current.jev_api_base) == ("jev", "https://example.com")

    def test_openrouter_uses_its_own_url_when_saved(self):
        current = connection.resolve(
            {
                es.DECISION_CONNECTION: "openrouter",
                es.OPENROUTER_API_BASE: "https://gateway.example.com/v1/",
            }
        )
        assert current.openrouter_api_base == "https://gateway.example.com/v1"

    def test_a_non_openrouter_jev_url_stays_jev(self):
        current = connection.resolve({es.JEV_API_BASE: "https://jev.example.com"})
        assert current.kind == "jev" and current.configured


def _service(stored=None):
    service = AsyncMock()
    service.get_settings.return_value = stored or {}
    service.save_settings.side_effect = lambda values: {
        k: v for k, v in values.items() if v
    }
    return service


class TestEngineSettingsView:
    @pytest.mark.asyncio
    async def test_view_migrates_an_openrouter_jev_url(self):
        view = await engine_settings_view.get_view(
            _service({es.JEV_API_BASE: OPENROUTER_URL})
        )
        assert view["decision_connection"] == "openrouter"
        assert view["openrouter_api_base"] == OPENROUTER_URL
        assert view["jev_api_base"] is None

    @pytest.mark.asyncio
    async def test_saving_the_connection_writes_all_three_settings(self):
        service = _service()
        view = await engine_settings_view.update_view(
            service,
            {
                "decision_connection": "openrouter",
                "jev_api_base": None,
                "openrouter_api_base": " https://gateway.example.com/v1/ ",
            },
        )
        service.save_settings.assert_awaited_once_with(
            {
                es.JEV_API_BASE: "",
                es.OPENROUTER_API_BASE: "https://gateway.example.com/v1",
                es.DECISION_CONNECTION: "openrouter",
            }
        )
        assert view["decision_connection"] == "openrouter"
        assert view["openrouter_api_base"] == "https://gateway.example.com/v1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sent, message",
        [
            ({"decision_connection": "bedrock"}, "'jev' or 'openrouter'"),
            ({"openrouter_api_base": "ftp://example.com"}, "OpenRouter URL"),
            ({"openrouter_api_base": "openrouter.ai"}, "http:// or https://"),
            ({"jev_api_base": "ftp://example.com"}, "Jev API URL"),
        ],
    )
    async def test_rejects_invalid_values(self, sent, message):
        service = _service()
        with pytest.raises(BadRequestError, match=message):
            await engine_settings_view.update_view(service, sent)
        service.save_settings.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_connection_resets_to_the_default(self):
        service = _service()
        await engine_settings_view.update_view(service, {"decision_connection": ""})
        service.save_settings.assert_awaited_once_with({es.DECISION_CONNECTION: ""})


def _settings_service(enabled: bool, keys: set[str]) -> DecisionSettingsService:
    instance = DecisionSettingsService(AsyncMock(), "workspace-a")
    instance.repository = AsyncMock()
    instance.repository.get.return_value = SimpleNamespace(enabled=enabled)
    instance.api_keys = AsyncMock()
    instance.api_keys.find_by_name.side_effect = lambda name: (
        SimpleNamespace(encrypted_value="ciphertext") if name in keys else None
    )
    return instance


class TestAvailability:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "settings, keys, available, key_name",
        [
            # OpenRouter: its own key, and the URL defaults, so it is configured.
            ({es.DECISION_CONNECTION: "openrouter"}, {"OPENROUTER_API_KEY"}, True, "OPENROUTER_API_KEY"),
            ({es.DECISION_CONNECTION: "openrouter"}, {"JEV_API_KEY"}, False, "OPENROUTER_API_KEY"),
            # The migrated state behaves as OpenRouter.
            ({es.JEV_API_BASE: OPENROUTER_URL}, {"OPENROUTER_API_KEY"}, True, "OPENROUTER_API_KEY"),
            ({es.JEV_API_BASE: OPENROUTER_URL}, {"JEV_API_KEY"}, False, "OPENROUTER_API_KEY"),
            # Jev: needs its URL and JEV_API_KEY.
            ({es.JEV_API_BASE: "https://jev.example.com"}, {"JEV_API_KEY"}, True, "JEV_API_KEY"),
            ({es.JEV_API_BASE: "https://jev.example.com"}, {"OPENROUTER_API_KEY"}, False, "JEV_API_KEY"),
            ({es.DECISION_CONNECTION: "jev"}, {"JEV_API_KEY"}, False, "JEV_API_KEY"),
        ],
    )  # fmt: skip
    async def test_available_per_connection_and_key(
        self, monkeypatch, settings, keys, available, key_name
    ):
        configure(
            monkeypatch,
            **{es.DECISION_CONNECTION: None, es.JEV_API_BASE: None, **settings},
        )
        config = await _settings_service(True, keys).get()
        assert config.available is available
        assert config.api_key_name == key_name
        assert config.api_key_configured is (key_name in keys)
        assert "ciphertext" not in config.model_dump_json()

    @pytest.mark.asyncio
    async def test_enabling_under_openrouter_needs_the_openrouter_key(
        self, monkeypatch
    ):
        configure(monkeypatch, **{es.DECISION_CONNECTION: "openrouter"})
        service = _settings_service(False, {"JEV_API_KEY"})
        with pytest.raises(BadRequestError, match="OPENROUTER_API_KEY"):
            await service.save(DecisionConfigUpdate(enabled=True))
        service.repository.save.assert_not_awaited()
        service = _settings_service(False, {"OPENROUTER_API_KEY"})
        result = await service.save(DecisionConfigUpdate(enabled=True))
        assert result.available and result.connection == "openrouter"


QUESTIONS = {"q": question("Choose", {"yes": "Yes", "no": "No"})}


class TestPoliciesUnderOpenRouter:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "settings",
        [
            {es.DECISION_CONNECTION: "openrouter", es.JEV_API_BASE: "https://jev.example.com"},
            {es.JEV_API_BASE: OPENROUTER_URL},
        ],
    )  # fmt: skip
    async def test_every_policy_abstains_without_a_call(self, monkeypatch, settings):
        """No /v1/systemone on OpenRouter: the same silent abstain as "off"."""
        configure(monkeypatch, **settings)
        assert provider.api_base() is None and not provider.is_configured()
        with (
            patch(
                "src.services.decisions.credentials.decision_credential",
                new_callable=AsyncMock,
            ) as credential,
            patch(
                "src.services.decisions.provider.evaluate", new_callable=AsyncMock
            ) as evaluate,
        ):
            for policy in (
                "knowledge_ranking",
                "memory_classification",
                "model_selection",
            ):
                assert (
                    await runtime.decide(policy, {}, QUESTIONS, group_id="ws") is None
                )
        credential.assert_not_awaited()
        evaluate.assert_not_awaited()


def _context(group_id="ws-1"):
    return SimpleNamespace(primary_group_id=group_id, group_ids=[group_id])


class TestAutoUnderOpenRouter:
    @pytest.fixture
    def openrouter(self, monkeypatch):
        configure(monkeypatch, **{es.DECISION_CONNECTION: "openrouter"})

    @pytest.mark.asyncio
    async def test_auto_resolves_to_jev_router_without_a_decision_call(
        self, openrouter
    ):
        available = SimpleNamespace(available=True)
        catalogue = Mock(find_by_key=AsyncMock(return_value=SimpleNamespace()))
        with (
            patch(
                "src.services.decisions.settings.DecisionSettingsService.get",
                new=AsyncMock(return_value=available),
            ),
            patch(
                "src.services.settings.models.ModelConfigService",
                return_value=catalogue,
            ),
            patch.object(
                model_selection, "decide_with_reason", new=AsyncMock()
            ) as decide,
            patch(
                "src.services.decisions.provider.evaluate", new_callable=AsyncMock
            ) as evaluate,
        ):
            result = await select_for_workspace(Mock(), _context(), "hello")
        assert result.model == "jev-router" and result.status == "selected"
        assert result.model != "auto"
        catalogue.find_by_key.assert_awaited_once_with("jev-router")
        catalogue.find_enabled_models_for_group.assert_not_called()
        decide.assert_not_awaited()
        evaluate.assert_not_awaited()
        assert model_selection.current_selection.get() == result

    @pytest.mark.asyncio
    async def test_not_available_falls_back_to_the_default(self, openrouter):
        """Not opted in or no OPENROUTER_API_KEY: Auto's usual fallback."""
        enabled = Mock(
            find_enabled_models_for_group=AsyncMock(
                return_value=[SimpleNamespace(key="ws-model")]
            )
        )
        with (
            patch(
                "src.services.decisions.settings.DecisionSettingsService.get",
                new=AsyncMock(return_value=SimpleNamespace(available=False)),
            ),
            patch(
                "src.services.settings.models.ModelConfigService", return_value=enabled
            ),
            patch(
                "src.services.decisions.provider.evaluate", new_callable=AsyncMock
            ) as evaluate,
        ):
            result = await select_for_workspace(Mock(), _context(), "hello")
        assert result.model == "ws-model" and result.status == "fallback"
        evaluate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_router_row_falls_through(self, openrouter):
        with (
            patch(
                "src.services.decisions.settings.DecisionSettingsService.get",
                new=AsyncMock(return_value=SimpleNamespace(available=True)),
            ),
            patch(
                "src.services.settings.models.ModelConfigService",
                return_value=Mock(find_by_key=AsyncMock(return_value=None)),
            ),
        ):
            assert await select_router(Mock(), _context()) is None

    @pytest.mark.asyncio
    async def test_no_workspace_is_not_routed(self, openrouter):
        assert await select_router(Mock(), _context(None)) is None


class TestAutoUnderJev:
    @pytest.mark.asyncio
    async def test_router_is_not_consulted_and_jev_picks(self, monkeypatch):
        configure(
            monkeypatch,
            **{
                es.DECISION_CONNECTION: "jev",
                es.JEV_API_BASE: "https://jev.example.com",
            },
        )
        from src.services.decisions.contracts import Choice

        service = Mock(
            find_enabled_models_for_group=AsyncMock(
                return_value=[SimpleNamespace(key="ws-model", name="m")]
            )
        )
        answers = {
            "model": Choice(
                selected="0", confidence=0.95, probabilities={"0": 0.95, "none": 0.05}
            )
        }
        with (
            patch(
                "src.services.settings.models.ModelConfigService", return_value=service
            ),
            patch.object(
                model_selection,
                "decide_with_reason",
                new=AsyncMock(return_value=(answers, None)),
            ) as decide,
            patch(
                "src.services.decisions.settings.DecisionSettingsService.get",
                new=AsyncMock(),
            ) as availability,
        ):
            result = await select_for_workspace(Mock(), _context(), "hello")
        assert result == ModelSelection("ws-model", "selected", result.duration_ms)
        decide.assert_awaited_once()
        availability.assert_not_awaited()
