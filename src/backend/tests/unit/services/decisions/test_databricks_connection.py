"""The Databricks decision-model connection: Jev on the workspace's AI Gateway.

No URL setting and no API key: the host and bearer come from the workspace's
Databricks auth (OBO, PAT, service principal). The model is configurable and
defaults to the Databricks-hosted ``system.ai.openjev-qwen35-4b``. Every
policy is served, as under the Jev API.
"""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from src.core.exceptions import BadRequestError
from src.schemas.decision_config import DecisionConfigUpdate
from src.services.decisions import connection, credentials, provider, runtime
from src.services.decisions.model_selection import ModelSelection
from src.services.decisions.policies import question
from src.services.decisions.provider import WorkspaceCredential
from src.services.decisions.settings import DecisionSettingsService
from src.services.settings import engine_settings as es
from src.services.settings import engine_settings_view

# tests/unit/services/conftest.py patches decision_credential for every service
# test; keep the real function for the tests that exercise it.
_REAL_DECISION_CREDENTIAL = credentials.decision_credential

HOST = "https://example.com"
DEFAULT_MODEL = "system.ai.openjev-qwen35-4b"
QUESTIONS = {"q": question("Choose", {"yes": "Yes", "no": "No"})}
ACCEPTED = {
    "answers": {
        "q": {
            "type": "choice",
            "choice": "yes",
            "confidence": 0.95,
            "probabilities": {"yes": 0.95, "no": 0.05},
        }
    }
}


@pytest.fixture
def databricks(monkeypatch):
    """The Databricks connection, with no model saved."""
    monkeypatch.setitem(es._snapshot, es.DECISION_CONNECTION, "databricks")
    monkeypatch.delitem(es._snapshot, es.DATABRICKS_DECISION_MODEL, raising=False)
    monkeypatch.delitem(es._snapshot, es.JEV_API_BASE, raising=False)


def _mock_http(monkeypatch, response=ACCEPTED):
    seen = []

    def handle(request):
        seen.append(request)
        return httpx.Response(200, json=response)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    monkeypatch.setattr(provider.httpx, "AsyncClient", lambda **kwargs: client)
    return seen


class TestResolve:
    def test_databricks_needs_no_url_and_no_key(self):
        current = connection.resolve({es.DECISION_CONNECTION: "databricks"})
        assert current.kind == "databricks"
        assert current.key_name is None
        assert current.configured
        assert current.databricks_model == DEFAULT_MODEL

    def test_the_default_model_is_the_databricks_hosted_jev(self):
        assert connection.DATABRICKS_DEFAULT_MODEL == DEFAULT_MODEL

    @pytest.mark.parametrize("saved", ["", "   ", None])
    def test_an_empty_model_reads_as_the_default(self, saved):
        current = connection.resolve(
            {es.DECISION_CONNECTION: "databricks", es.DATABRICKS_DECISION_MODEL: saved}
        )
        assert current.databricks_model == DEFAULT_MODEL

    def test_a_saved_model_is_used(self):
        current = connection.resolve(
            {
                es.DECISION_CONNECTION: " Databricks ",
                es.DATABRICKS_DECISION_MODEL: " typesafe-jev-preview ",
            }
        )
        assert (current.kind, current.databricks_model) == (
            "databricks",
            "typesafe-jev-preview",
        )

    def test_the_other_connections_keep_their_keys(self):
        assert connection.resolve({}).key_name == "JEV_API_KEY"
        assert (
            connection.resolve({es.DECISION_CONNECTION: "openrouter"}).key_name
            == "OPENROUTER_API_KEY"
        )


class TestProvider:
    def test_every_policy_is_served(self, databricks):
        """Unlike OpenRouter, the state stays on Databricks: no policy abstains."""
        for policy in (None, "model_selection", "knowledge_ranking", "memory_ranking"):
            assert provider.is_configured(policy)
        # Nothing is saved for the host: it comes with the credential.
        assert provider.api_base("knowledge_ranking") is None

    def test_model_follows_the_setting(self, databricks, monkeypatch):
        assert provider.model_for() == DEFAULT_MODEL
        monkeypatch.setitem(es._snapshot, es.DATABRICKS_DECISION_MODEL, "custom-jev")
        assert provider.model_for() == "custom-jev"

    @pytest.mark.asyncio
    async def test_posts_to_the_workspace_ai_gateway(self, databricks, monkeypatch):
        seen = _mock_http(monkeypatch)
        credential = WorkspaceCredential(token="dapi-token", host=f"{HOST}/")
        result = await provider.evaluate(
            credential, {"request": "hi"}, QUESTIONS, "knowledge_ranking"
        )
        assert result == ACCEPTED
        request = seen[0]
        assert str(request.url) == f"{HOST}/ai-gateway/typesafe/v1/systemone"
        assert request.headers["Authorization"] == "Bearer dapi-token"
        from src.utils.telemetry import KasalProduct, get_user_agent_header

        expected = get_user_agent_header(KasalProduct.DECISIONS)["User-Agent"]
        assert request.headers["User-Agent"] == expected
        assert "decisions" in expected
        body = json.loads(request.content)
        assert body == {
            "model": DEFAULT_MODEL,
            "state": {"request": "hi"},
            "questions": QUESTIONS,
        }

    @pytest.mark.asyncio
    async def test_a_plain_api_key_is_refused(self, databricks):
        with pytest.raises(ValueError, match="workspace credential"):
            await provider.evaluate("jev-key", {}, QUESTIONS)

    @pytest.mark.asyncio
    async def test_a_workspace_credential_never_reaches_a_third_party(
        self, monkeypatch
    ):
        monkeypatch.setitem(es._snapshot, es.DECISION_CONNECTION, "jev")
        monkeypatch.setitem(es._snapshot, es.JEV_API_BASE, HOST)
        with pytest.raises(ValueError, match="only valid for Databricks"):
            await provider.evaluate(WorkspaceCredential("t", HOST), {}, QUESTIONS)

    @pytest.mark.asyncio
    async def test_the_third_party_connections_send_no_kasal_user_agent(
        self, monkeypatch
    ):
        monkeypatch.setitem(es._snapshot, es.DECISION_CONNECTION, "jev")
        monkeypatch.setitem(es._snapshot, es.JEV_API_BASE, HOST)
        seen = _mock_http(monkeypatch)
        await provider.evaluate("jev-key", {}, QUESTIONS)
        assert "kasal" not in seen[0].headers.get("User-Agent", "").lower()

    def test_the_credential_repr_hides_the_token(self):
        assert "secret" not in repr(WorkspaceCredential("secret", HOST))


def _isolated(service):
    @asynccontextmanager
    async def isolated():
        yield object()

    return (
        patch.object(credentials, "get_isolated_db_session", isolated),
        patch.object(credentials, "DecisionSettingsService", return_value=service),
    )


class TestCredential:
    @pytest.mark.asyncio
    async def test_not_opted_in_resolves_nothing(self, databricks):
        service = Mock(opted_in=AsyncMock(return_value=False), credential=AsyncMock())
        isolated, service_cls = _isolated(service)
        with (
            isolated,
            service_cls,
            patch(
                "src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock
            ) as auth,
        ):
            assert await _REAL_DECISION_CREDENTIAL("ws-a") is None
        auth.assert_not_awaited()
        service.credential.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_opted_in_uses_the_workspace_auth_chain(self, databricks):
        """OBO from the request's token; the PAT lookup is scoped to the workspace."""
        service = Mock(opted_in=AsyncMock(return_value=True), credential=AsyncMock())
        isolated, service_cls = _isolated(service)
        context = SimpleNamespace(token="dapi-obo", workspace_url=HOST)
        with (
            isolated,
            service_cls,
            patch(
                "src.utils.user_context.UserContext.get_user_token",
                return_value="user-obo",
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new=AsyncMock(return_value=context),
            ) as auth,
        ):
            result = await _REAL_DECISION_CREDENTIAL("ws-a")
        assert result == WorkspaceCredential(token="dapi-obo", host=HOST)
        auth.assert_awaited_once_with(user_token="user-obo", group_id="ws-a")
        service.credential.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_resolvable_workspace_is_no_credential(self, databricks):
        service = Mock(opted_in=AsyncMock(return_value=True))
        isolated, service_cls = _isolated(service)
        with (
            isolated,
            service_cls,
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new=AsyncMock(return_value=None),
            ),
        ):
            assert await _REAL_DECISION_CREDENTIAL("ws-a") is None

    @pytest.mark.asyncio
    async def test_the_keyed_connections_still_read_their_key(self, monkeypatch):
        monkeypatch.setitem(es._snapshot, es.DECISION_CONNECTION, "jev")
        service = Mock(credential=AsyncMock(return_value="jev-key"))
        isolated, service_cls = _isolated(service)
        with isolated, service_cls:
            assert await _REAL_DECISION_CREDENTIAL("ws-a") == "jev-key"


class TestRuntime:
    @pytest.mark.asyncio
    async def test_a_non_auto_policy_decides_on_databricks(
        self, databricks, monkeypatch
    ):
        seen = _mock_http(monkeypatch)
        with (
            patch(
                "src.services.decisions.credentials.decision_credential",
                new=AsyncMock(return_value=WorkspaceCredential("dapi", HOST)),
            ),
            patch("src.services.decisions.telemetry.record_decision") as trace,
        ):
            answers = await runtime.decide(
                "knowledge_ranking", {}, QUESTIONS, group_id="ws-a"
            )
        assert answers is not None and answers["q"].selected == "yes"
        assert str(seen[0].url) == f"{HOST}/ai-gateway/typesafe/v1/systemone"
        assert trace.call_args.args[:3] == (
            "knowledge_ranking",
            DEFAULT_MODEL,
            "accepted",
        )

    @pytest.mark.asyncio
    async def test_no_credential_falls_back_without_a_call(
        self, databricks, monkeypatch
    ):
        seen = _mock_http(monkeypatch)
        answers, reason = await runtime.decide_with_reason(
            "knowledge_ranking", {}, QUESTIONS, group_id="ws-a"
        )
        assert (answers, reason) == (None, runtime.NO_KEY)
        assert seen == []

    def test_auto_names_the_connection(self):
        selection = ModelSelection(
            "m", "selected", connection="databricks", confidence=1.0
        )
        assert selection.summary() == "Auto (Jev on Databricks) → m (1.00)"


def _settings_service(enabled: bool) -> DecisionSettingsService:
    service = DecisionSettingsService(AsyncMock(), "ws-a")
    service.repository = AsyncMock()
    service.repository.get.return_value = SimpleNamespace(enabled=enabled)
    service.api_keys = AsyncMock()
    return service


class TestWorkspaceOptIn:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("resolves", [True, False])
    async def test_ready_when_the_workspace_credential_resolves(
        self, databricks, resolves
    ):
        service = _settings_service(True)
        credential = WorkspaceCredential("dapi", HOST) if resolves else None
        with patch.object(
            credentials, "workspace_credential", new=AsyncMock(return_value=credential)
        ) as resolve:
            config = await service.get()
        resolve.assert_awaited_once_with("ws-a")
        assert config.connection == "databricks"
        assert config.api_key_name is None
        assert config.api_key_configured is resolves
        assert config.available is resolves
        service.api_keys.find_by_name.assert_not_awaited()
        assert "dapi" not in config.model_dump_json()

    @pytest.mark.asyncio
    async def test_enabling_needs_a_resolvable_workspace(self, databricks):
        service = _settings_service(False)
        with patch.object(
            credentials, "workspace_credential", new=AsyncMock(return_value=None)
        ):
            with pytest.raises(
                BadRequestError, match="Databricks workspace credential"
            ):
                await service.save(DecisionConfigUpdate(enabled=True))
        service.repository.save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_enabling_with_a_resolvable_workspace(self, databricks):
        service = _settings_service(False)
        with patch.object(
            credentials,
            "workspace_credential",
            new=AsyncMock(return_value=WorkspaceCredential("dapi", HOST)),
        ):
            result = await service.save(DecisionConfigUpdate(enabled=True))
        service.repository.save.assert_awaited_once_with("ws-a", True)
        assert result.available and result.connection == "databricks"

    @pytest.mark.asyncio
    async def test_turning_it_off_needs_nothing(self, databricks):
        service = _settings_service(True)
        with patch.object(
            credentials, "workspace_credential", new=AsyncMock(return_value=None)
        ):
            result = await service.save(DecisionConfigUpdate(enabled=False))
        assert result.enabled is False

    @pytest.mark.asyncio
    async def test_no_key_is_ever_read(self, databricks):
        service = _settings_service(True)
        assert await service.credential() is None
        service.api_keys.find_by_name.assert_not_awaited()


def _engine_service(stored=None):
    service = AsyncMock()
    service.get_settings.return_value = stored or {}
    service.save_settings.side_effect = lambda values: {
        k: v for k, v in values.items() if v
    }
    return service


class TestSystemSettings:
    @pytest.mark.asyncio
    async def test_the_view_shows_the_default_model(self):
        view = await engine_settings_view.get_view(
            _engine_service({es.DECISION_CONNECTION: "databricks"})
        )
        assert view["decision_connection"] == "databricks"
        assert view["databricks_decision_model"] == DEFAULT_MODEL

    @pytest.mark.asyncio
    async def test_saving_the_connection_and_a_model(self):
        service = _engine_service()
        view = await engine_settings_view.update_view(
            service,
            {
                "decision_connection": "databricks",
                "databricks_decision_model": " typesafe/jev-preview ",
            },
        )
        service.save_settings.assert_awaited_once_with(
            {
                es.DECISION_CONNECTION: "databricks",
                es.DATABRICKS_DECISION_MODEL: "typesafe/jev-preview",
            }
        )
        assert view["databricks_decision_model"] == "typesafe/jev-preview"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sent", ["", None])
    async def test_an_empty_model_resets_to_the_default(self, sent):
        service = _engine_service({es.DECISION_CONNECTION: "databricks"})
        view = await engine_settings_view.update_view(
            service, {"databricks_decision_model": sent}
        )
        service.save_settings.assert_awaited_once_with(
            {es.DATABRICKS_DECISION_MODEL: ""}
        )
        assert view["databricks_decision_model"] == DEFAULT_MODEL

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "model", ["two words", "https://example.com/x?y", "-leading", "x" * 201]
    )
    async def test_rejects_a_model_that_is_not_one_id(self, model):
        service = _engine_service()
        with pytest.raises(BadRequestError, match=DEFAULT_MODEL):
            await engine_settings_view.update_view(
                service, {"databricks_decision_model": model}
            )
        service.save_settings.assert_not_awaited()
