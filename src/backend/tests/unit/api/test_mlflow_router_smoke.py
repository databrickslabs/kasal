import pytest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import HTTPException

from src.api.mlflow_router import (
    get_mlflow_status,
    set_mlflow_status,
    get_evaluation_status,
    set_evaluation_status,
    trigger_evaluation,
    get_mlflow_experiment_info,
    get_trace_deeplink,
)
from src.schemas.mlflow import MLflowConfigUpdate, MLflowEvaluateRequest


class Group:
    def __init__(self, gid):
        self.primary_group_id = gid


@pytest.mark.asyncio
async def test_get_mlflow_status_enabled():
    session = AsyncMock()
    group_ctx = Group('g1')
    with patch('src.api.mlflow_router.MLflowService') as svc_cls:
        svc = AsyncMock()
        svc.is_enabled = AsyncMock(return_value=True)
        svc_cls.return_value = svc
        resp = await get_mlflow_status(session=session, group_ctx=group_ctx)
        assert resp.enabled is True
        svc.is_enabled.assert_called_once()


@pytest.mark.asyncio
async def test_get_mlflow_status_requires_group():
    session = AsyncMock()
    with pytest.raises(HTTPException) as ei:
        await get_mlflow_status(session=session, group_ctx=None)
    # Router wraps into 500 since it catches general Exception
    assert ei.value.status_code == 500


@pytest.mark.asyncio
async def test_set_mlflow_status_true_false():
    session = AsyncMock()
    group_ctx = Group('g1')
    with patch('src.api.mlflow_router.MLflowService') as svc_cls:
        svc = AsyncMock()
        svc.set_enabled = AsyncMock(return_value=True)
        svc_cls.return_value = svc
        out = await set_mlflow_status(MLflowConfigUpdate(enabled=True), session=session, group_ctx=group_ctx)
        assert out.enabled is True
        
        svc.set_enabled = AsyncMock(return_value=False)
        with pytest.raises(HTTPException) as ei:
            await set_mlflow_status(MLflowConfigUpdate(enabled=False), session=session, group_ctx=group_ctx)
        assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_evaluation_status_get_set():
    session = AsyncMock()
    group_ctx = Group('g1')
    with patch('src.api.mlflow_router.MLflowService') as svc_cls:
        svc = AsyncMock()
        svc.is_evaluation_enabled = AsyncMock(return_value=False)
        svc.set_evaluation_enabled = AsyncMock(return_value=True)
        svc_cls.return_value = svc
        r1 = await get_evaluation_status(session=session, group_ctx=group_ctx)
        assert r1.enabled is False
        r2 = await set_evaluation_status(MLflowConfigUpdate(enabled=True), session=session, group_ctx=group_ctx)
        assert r2.enabled is True


@pytest.mark.asyncio
async def test_trigger_evaluation_requires_job():
    session = AsyncMock()
    group_ctx = Group('g1')
    with pytest.raises(HTTPException) as ei:
        await trigger_evaluation(MLflowEvaluateRequest(job_id=""), request=Mock(), session=session, group_ctx=group_ctx)
    assert ei.value.status_code == 400


@pytest.mark.asyncio
async def test_trigger_evaluation_success_and_user_token():
    session = AsyncMock()
    group_ctx = Group('g1')
    req = Mock()
    with patch('src.api.mlflow_router.MLflowService') as svc_cls, \
         patch('src.utils.databricks_auth.extract_user_token_from_request', return_value='tok'):
        svc = AsyncMock()
        svc.trigger_evaluation = AsyncMock(return_value={'run_id': '1'})
        svc_cls.return_value = svc
        out = await trigger_evaluation(MLflowEvaluateRequest(job_id='job1'), request=req, session=session, group_ctx=group_ctx)
        assert out.run_id == '1'
        svc.set_user_token.assert_called_once_with('tok')


@pytest.mark.asyncio
async def test_get_experiment_info_and_trace_deeplink():
    session = AsyncMock()
    group_ctx = Group('g1')
    req = Mock()
    with patch('src.api.mlflow_router.MLflowService') as svc_cls, \
         patch('src.utils.databricks_auth.extract_user_token_from_request', return_value=None):
        svc = AsyncMock()
        svc.get_experiment_info = AsyncMock(return_value={'experiment_id': 'exp'})
        svc.get_trace_deeplink = AsyncMock(return_value={'url': 'https://example.com'})
        svc_cls.return_value = svc

        info = await get_mlflow_experiment_info(request=req, session=session, group_ctx=group_ctx)
        assert info['experiment_id'] == 'exp'

        link = await get_trace_deeplink(request=req, session=session, group_ctx=group_ctx, job_id='jobx')
        assert link['url'].startswith('http')

