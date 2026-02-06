from fastapi import APIRouter, Depends, Request

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.schemas.mlflow import (
    MLflowConfigResponse,
    MLflowConfigUpdate,
    MLflowEvaluateRequest,
    MLflowEvaluateResponse,
)
from src.services.mlflow_service import MLflowService

router = APIRouter(prefix="/mlflow", tags=["mlflow"])


@router.get("/status", response_model=MLflowConfigResponse)
async def get_mlflow_status(
    session: SessionDep, group_ctx: GroupContextDep
) -> MLflowConfigResponse:
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    enabled = await svc.is_enabled()
    return MLflowConfigResponse(enabled=enabled)


@router.post("/status", response_model=MLflowConfigResponse)
async def set_mlflow_status(
    payload: MLflowConfigUpdate, session: SessionDep, group_ctx: GroupContextDep
) -> MLflowConfigResponse:
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    ok = await svc.set_enabled(payload.enabled)
    if not ok:
        raise NotFoundError("No Databricks configuration to attach MLflow setting to")
    return MLflowConfigResponse(enabled=payload.enabled)


# Evaluation toggles
@router.get("/evaluation-status", response_model=MLflowConfigResponse)
async def get_evaluation_status(
    session: SessionDep, group_ctx: GroupContextDep
) -> MLflowConfigResponse:
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    enabled = await svc.is_evaluation_enabled()
    return MLflowConfigResponse(enabled=enabled)


@router.post("/evaluation-status", response_model=MLflowConfigResponse)
async def set_evaluation_status(
    payload: MLflowConfigUpdate, session: SessionDep, group_ctx: GroupContextDep
) -> MLflowConfigResponse:
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    ok = await svc.set_evaluation_enabled(payload.enabled)
    if not ok:
        raise NotFoundError("No Databricks configuration to attach evaluation setting to")
    return MLflowConfigResponse(enabled=payload.enabled)


# Trigger minimal evaluation and return run info
@router.post("/evaluate", response_model=MLflowEvaluateResponse)
async def trigger_evaluation(
    payload: MLflowEvaluateRequest,
    request: Request,
    session: SessionDep,
    group_ctx: GroupContextDep,
) -> MLflowEvaluateResponse:
    if not payload.job_id:
        raise BadRequestError("job_id is required")
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    # Prefer OBO user token when present; fallback handled in service
    try:
        from src.utils.databricks_auth import extract_user_token_from_request

        user_token = extract_user_token_from_request(request)
        if user_token:
            svc.set_user_token(user_token)
    except Exception:
        pass
    info = await svc.trigger_evaluation(payload.job_id)
    return MLflowEvaluateResponse(**info)


from typing import Dict, Optional


@router.get("/experiment-info", response_model=Dict)
async def get_mlflow_experiment_info(
    request: Request, session: SessionDep, group_ctx: GroupContextDep
) -> Dict:
    """
    Return MLflow experiment info used for tracing UI deep links.
    - experiment_id: Numeric ID for the crew execution traces experiment
    - experiment_name: Name/path of the experiment
    """
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")

    svc = MLflowService(session, group_id=group_ctx.primary_group_id)

    # Prefer OBO user token when present; fallback handled in service
    try:
        from src.utils.databricks_auth import extract_user_token_from_request

        user_token = extract_user_token_from_request(request)
        if user_token:
            svc.set_user_token(user_token)
    except Exception:
        pass

    return await svc.get_experiment_info()


@router.get("/trace-link", response_model=Dict)
async def get_trace_deeplink(
    request: Request,
    session: SessionDep,
    group_ctx: GroupContextDep,
    job_id: Optional[str] = None,
) -> Dict:
    """
    Return a full MLflow deep link to the Traces tab, optionally selecting a specific trace
    for the provided job_id.

    Response example:
    {
      "url": "https://<workspace>/ml/experiments/<exp_id>/traces?o=<workspace_id>&selectedEvaluationId=tr-...",
      "experiment_id": "...",
      "trace_id": "tr-...",
      "workspace_url": "https://<workspace>",
      "workspace_id": "<numeric>"
    }
    """
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")

    svc = MLflowService(session, group_id=group_ctx.primary_group_id)

    # Prefer OBO user token when present; fallback handled in service
    try:
        from src.utils.databricks_auth import extract_user_token_from_request

        user_token = extract_user_token_from_request(request)
        if user_token:
            svc.set_user_token(user_token)
    except Exception:
        pass

    return await svc.get_trace_deeplink(job_id=job_id)
