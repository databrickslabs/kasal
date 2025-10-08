import pytest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from src.services.mlflow_service import MLflowService


@pytest.mark.asyncio
async def test_get_experiment_info_auth_failed_raises_runtimeerror():
    session = AsyncMock()
    svc = MLflowService(session, group_id="g1")
    with patch.object(MLflowService, "_setup_mlflow_auth", new=AsyncMock(return_value=None)):
        with pytest.raises(RuntimeError):
            await svc.get_experiment_info()


@pytest.mark.asyncio
async def test_get_trace_deeplink_with_auth_and_job_id_minimal():
    session = AsyncMock()
    svc = MLflowService(session, group_id="g1")

    # Mock unified auth and experiment id resolution
    with patch("src.utils.databricks_auth.get_auth_context", new=AsyncMock(return_value=SimpleNamespace(
        workspace_url="https://abc.cloud.databricks.com",
        token="t",
        auth_method="obo",
    ))), patch("asyncio.to_thread", new=AsyncMock(return_value="exp-1")):
        # Also mock execution repo to provide a trace id
        exec_obj = SimpleNamespace(mlflow_trace_id="trace-123")
        svc.exec_repo.get_execution_by_job_id = AsyncMock(return_value=exec_obj)

        out = await svc.get_trace_deeplink(job_id="job-xyz")
        assert isinstance(out, dict)
        assert out["workspace_url"].startswith("https://abc.cloud.databricks.com")
        assert out["experiment_id"] == "exp-1"
        assert out.get("trace_id") == "trace-123"
        assert out["url"].startswith("https://abc.cloud.databricks.com/ml/experiments/")



@pytest.mark.asyncio
async def test_resolve_judge_model_defaults_to_databricks_prefixed():
    session = AsyncMock()
    svc = MLflowService(session, group_id="g1")
    # repo returns None -> env missing -> default model key
    svc.repo.get_evaluation_judge_model = AsyncMock(return_value=None)
    # model config resolves provider as databricks
    svc.model_config_service.get_model_config = AsyncMock(return_value={"provider": "databricks"})
    out = await svc._resolve_judge_model(None)
    assert out.startswith("databricks/")
    assert "databricks-claude-sonnet-4" in out


@pytest.mark.asyncio
async def test_resolve_judge_model_non_databricks_as_is():
    session = AsyncMock()
    svc = MLflowService(session, group_id="g1")
    svc.model_config_service.get_model_config = AsyncMock(return_value={"provider": "openai"})
    out = await svc._resolve_judge_model("gpt-4o")
    assert out == "gpt-4o"
