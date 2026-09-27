"""Decision model connections: Jev API or OpenRouter.

The setting, the migration of an OpenRouter ``jev_api_base``, availability per
connection and key, Auto under each connection, and the other policies
abstaining under OpenRouter.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.exceptions import BadRequestError
from src.schemas.decision_config import DecisionConfigUpdate
from src.services.decisions import connection, model_selection, provider, runtime
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
    async def test_every_policy_but_auto_abstains_without_a_call(
        self, monkeypatch, settings
    ):
        """Only Auto goes to OpenRouter; the rest stay as off as before."""
        configure(monkeypatch, **settings)
        assert provider.api_base() is None and not provider.is_configured()
        assert provider.api_base("model_selection") == OPENROUTER_URL
        with (
            patch(
                "src.services.decisions.credentials.decision_credential",
                new_callable=AsyncMock,
            ) as credential,
            patch(
                "src.services.decisions.provider.evaluate", new_callable=AsyncMock
            ) as evaluate,
        ):
            for policy in ("knowledge_ranking", "memory_classification"):
                assert (
                    await runtime.decide(policy, {}, QUESTIONS, group_id="ws") is None
                )
        credential.assert_not_awaited()
        evaluate.assert_not_awaited()


def _context(group_id="ws-1"):
    return SimpleNamespace(primary_group_id=group_id, group_ids=[group_id])


def _enabled(*models):
    return Mock(find_enabled_models_for_group=AsyncMock(return_value=list(models)))


def _answer(selected, options):
    probabilities = {option: 0.0 for option in options}
    probabilities[selected] = 1.0
    return {
        "answers": {
            "model": {
                "type": "choice",
                "choice": selected,
                "confidence": 0.99,
                "probabilities": probabilities,
            }
        }
    }


class TestAutoUnderOpenRouter:
    """Decide, then call: Jev through OpenRouter picks among the enabled models."""

    @pytest.fixture
    def openrouter(self, monkeypatch):
        configure(monkeypatch, **{es.DECISION_CONNECTION: "openrouter"})

    @pytest.mark.asyncio
    async def test_the_decision_goes_to_openrouters_system_one_route(
        self, openrouter, monkeypatch
    ):
        """The restriction is the payload: the enabled models are the options."""
        import httpx

        seen = []

        def handle(request):
            seen.append(request)
            return httpx.Response(200, json=_answer("1", ["0", "1", "none"]))

        client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        monkeypatch.setattr(provider.httpx, "AsyncClient", lambda **kwargs: client)
        enabled = _enabled(
            SimpleNamespace(key="ws-a", name="a", provider="databricks"),
            SimpleNamespace(key="claude-opus-5-5", name="o", provider="anthropic"),
            SimpleNamespace(key="jev-router", name="typesafe/jev-router"),
        )
        with (
            patch(
                "src.services.settings.models.ModelConfigService", return_value=enabled
            ),
            patch(
                "src.services.decisions.credentials.decision_credential",
                new=AsyncMock(return_value="or-key"),
            ),
            patch("src.services.decisions.telemetry.record_decision"),
        ):
            result = await select_for_workspace(Mock(), _context(), "hello")
        assert (result.model, result.status) == ("claude-opus-5-5", "selected")
        assert (result.connection, result.candidates, result.picked) == (
            "openrouter",
            2,
            "1",
        )
        assert result.summary() == "Auto (Jev via OpenRouter) → claude-opus-5-5"
        request = seen[0]
        assert str(request.url) == f"{OPENROUTER_URL}/systemone"
        assert request.headers["Authorization"] == "Bearer or-key"
        body = json.loads(request.content)
        assert body["model"] == "typesafe/jev-1.13"
        # Only the enabled, non-router models are offered, and never by key.
        assert set(body["questions"]["model"]["criteria"]) == {"0", "1", "none"}
        assert [m["name"] for m in body["state"]["models"]] == ["a", "o"]
        assert "jev-router" not in request.content.decode()

    @pytest.mark.asyncio
    async def test_never_resolves_to_jev_router(self, openrouter):
        """Only a router enabled: nothing to offer, and no router as the answer."""
        enabled = _enabled(
            SimpleNamespace(key="jev-router", name="typesafe/jev-router")
        )
        with (
            patch(
                "src.services.settings.models.ModelConfigService", return_value=enabled
            ),
            patch(
                "src.services.decisions.provider.evaluate", new_callable=AsyncMock
            ) as evaluate,
        ):
            result = await select_for_workspace(Mock(), _context(), "hello")
        assert result.model is None and result.reason == "no_models"
        evaluate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_not_opted_in_falls_back_to_the_default(self, openrouter):
        """No opt-in or no OPENROUTER_API_KEY: no call, the workspace default."""
        enabled = _enabled(SimpleNamespace(key="ws-model", name="m"))
        with (
            patch(
                "src.services.settings.models.ModelConfigService", return_value=enabled
            ),
            patch(
                "src.services.decisions.credentials.decision_credential",
                new=AsyncMock(return_value=None),
            ),
            patch(
                "src.services.decisions.provider.evaluate", new_callable=AsyncMock
            ) as evaluate,
        ):
            result = await select_for_workspace(Mock(), _context(), "hello")
        assert (result.model, result.status, result.reason) == (
            "ws-model",
            "fallback",
            "no_key",
        )
        evaluate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_openrouter_key_is_the_credential(self, openrouter):
        service = DecisionSettingsService(Mock(), "ws")
        service.repository = Mock(get=AsyncMock(return_value=Mock(enabled=True)))
        service.api_keys = Mock(find_by_name=AsyncMock(return_value=None))
        assert await service.credential() is None
        service.api_keys.find_by_name.assert_awaited_once_with("OPENROUTER_API_KEY")


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
