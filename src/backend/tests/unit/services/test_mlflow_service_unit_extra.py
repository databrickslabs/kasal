import os
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.services.mlflow_service import MLflowService


@pytest.mark.asyncio
async def test_set_user_token_and_setup_auth_paths(monkeypatch):
    svc = MLflowService(session=SimpleNamespace(), group_id="g1")

    # set_user_token normalization
    svc.set_user_token(" ")
    assert svc._user_token is None
    svc.set_user_token("tok")
    assert svc._user_token == "tok"

    # _setup_mlflow_auth -> None when no auth
    import sys
    # _setup_mlflow_auth -> None when no auth
    fake_mod = SimpleNamespace()
    async def no_auth(**kwargs):
        return None
    fake_mod.get_auth_context = no_auth
    monkeypatch.setitem(sys.modules, 'src.utils.databricks_auth', fake_mod)
    assert await svc._setup_mlflow_auth() is None

    # returns auth when available
    async def yes_auth(**kwargs):
        return SimpleNamespace(workspace_url="https://ws", token="t", auth_method="obo")
    fake_mod.get_auth_context = yes_auth
    monkeypatch.setitem(sys.modules, 'src.utils.databricks_auth', fake_mod)
    auth = await svc._setup_mlflow_auth()
    assert auth.workspace_url == "https://ws"


@pytest.mark.asyncio
async def test_get_trace_deeplink_builds_url(monkeypatch):
    import sys
    svc = MLflowService(session=SimpleNamespace(), group_id="g1")
    # Use DatabricksService fallback path (no auth) to avoid heavy mlflow import
    import sys
    # Ensure unified auth returns None immediately (no network/config attempts)
    fake_auth_mod = SimpleNamespace()
    async def no_auth(**kwargs):
        return None
    fake_auth_mod.get_auth_context = no_auth
    monkeypatch.setitem(sys.modules, 'src.utils.databricks_auth', fake_auth_mod)

    # Provide fake DatabricksService module where mlflow_service imports it
    fake_dbs_mod = SimpleNamespace()
    class FakeDBSvc:
        def __init__(self, session): pass
        async def get_databricks_config(self):
            return SimpleNamespace(workspace_url="acme.databricks.com")
    fake_dbs_mod.DatabricksService = FakeDBSvc
    # Use monkeypatch to avoid leaking sys.modules changes
    monkeypatch.setitem(sys.modules, 'src.services.databricks_service', fake_dbs_mod)

    # no job id
    out = await svc.get_trace_deeplink()
    assert out["url"].startswith("https://acme.databricks.com/ml/experiments")

    # with job id and trace id
    svc.exec_repo.get_execution_by_job_id = AsyncMock(return_value=SimpleNamespace(mlflow_trace_id=123))
    out2 = await svc.get_trace_deeplink(job_id="job-1")
    assert "selectedEvaluationId=123" in out2["url"]



@pytest.mark.asyncio
async def test_resolve_judge_model_paths(monkeypatch):
    svc = MLflowService(session=SimpleNamespace(), group_id="g1")
    # model_config_service mocked
    svc.repo = AsyncMock()
    svc.model_config_service = SimpleNamespace(get_model_config=AsyncMock(return_value={"provider": "databricks"}))

    # No configured model -> default databricks-claude-sonnet-4 -> adds databricks/
    svc.repo.get_evaluation_judge_model = AsyncMock(return_value=None)
    if "MLFLOW_EVAL_JUDGE_MODEL" in os.environ:
        del os.environ["MLFLOW_EVAL_JUDGE_MODEL"]
    model = await svc._resolve_judge_model()
    assert model.startswith("databricks/")

    # Non-databricks provider -> return normalized key without provider prefix
    svc.model_config_service.get_model_config = AsyncMock(return_value={"provider": "openai"})
    model2 = await svc._resolve_judge_model("endpoints://foo")
    assert model2 == "foo"




@pytest.mark.asyncio
async def test_get_trace_deeplink_with_auth_and_experiment_id(monkeypatch):
    # Arrange auth so workspace_id can be derived and avoid real mlflow via to_thread stub
    import sys, asyncio as aio
    svc = MLflowService(session=SimpleNamespace(), group_id="g1")

    fake_auth_mod = SimpleNamespace()
    async def yes_auth(**kwargs):
        return SimpleNamespace(workspace_url="https://acme.databricks.com", token="t", auth_method="obo")
    fake_auth_mod.get_auth_context = yes_auth
    monkeypatch.setitem(sys.modules, 'src.utils.databricks_auth', fake_auth_mod)

    # Stub asyncio.to_thread used to resolve experiment id
    async def fake_to_thread(func, *args, **kwargs):
        return "exp123"
    monkeypatch.setattr(__import__('asyncio'), 'to_thread', fake_to_thread, raising=True)

    # Act
    out = await svc.get_trace_deeplink()

    # Assert
    assert out["workspace_id"] == "acme"
    assert "/ml/experiments/exp123/" in out["url"]
    assert "o=acme" in out["url"]


@pytest.mark.asyncio
async def test_resolve_judge_model_fallback_on_exception(monkeypatch):
    svc = MLflowService(session=SimpleNamespace(), group_id="g1")
    svc.repo = AsyncMock()
    # Force model_config_service to raise so fallback kicks in
    svc.model_config_service = SimpleNamespace(get_model_config=AsyncMock(side_effect=Exception("x")))

    model = await svc._resolve_judge_model("foo/bar")
    assert model == "databricks/bar"
