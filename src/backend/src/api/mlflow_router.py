from fastapi import APIRouter

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.schemas.mlflow import (
    MLflowConfigResponse,
    MLflowConfigUpdate,
    MLflowEvaluateRequest,
    MLflowEvaluateResponse,
    MLflowSettings,
    MLflowSettingsUpdate,
)
from src.services.mlflow.service import MLflowService

router = APIRouter(prefix="/mlflow", tags=["mlflow"])


@router.get("/settings", response_model=MLflowSettings)
async def get_mlflow_settings(
    session: SessionDep, group_ctx: GroupContextDep
) -> MLflowSettings:
    """Everything the MLflow configuration section renders, in one call.

    One endpoint rather than four, because the section's fields are read
    together and the backend half is derived from the others.
    """
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    return MLflowSettings(**await svc.get_settings())


@router.patch("/settings", response_model=MLflowSettings)
async def update_mlflow_settings(
    payload: MLflowSettingsUpdate, session: SessionDep, group_ctx: GroupContextDep
) -> MLflowSettings:
    """Partial update; an omitted field is left alone. Returns the new state so
    the UI never has to guess what the backend resolved to."""
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    return MLflowSettings(
        **await svc.update_settings(
            enabled=payload.enabled,
            evaluation_enabled=payload.evaluation_enabled,
            experiment_name=payload.experiment_name,
        )
    )


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
        raise NotFoundError(
            "No Databricks configuration to attach evaluation setting to"
        )
    return MLflowConfigResponse(enabled=payload.enabled)


# Trigger minimal evaluation and return run info
@router.post("/evaluate", response_model=MLflowEvaluateResponse)
async def trigger_evaluation(
    payload: MLflowEvaluateRequest,
    session: SessionDep,
    group_ctx: GroupContextDep,
) -> MLflowEvaluateResponse:
    if not payload.job_id:
        raise BadRequestError("job_id is required")
    # SECURITY: group_id is REQUIRED for MLflowService
    if not group_ctx or not group_ctx.primary_group_id:
        raise ForbiddenError("Group context required for MLflow operations")
    svc = MLflowService(session, group_id=group_ctx.primary_group_id)
    info = await svc.trigger_evaluation(payload.job_id)
    return MLflowEvaluateResponse(**info)


from typing import Dict, Optional


@router.get("/experiment-info", response_model=Dict)
async def get_mlflow_experiment_info(
    session: SessionDep, group_ctx: GroupContextDep
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

    return await svc.get_experiment_info()


@router.get("/trace-link", response_model=Dict)
async def get_trace_deeplink(
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

    return await svc.get_trace_deeplink(job_id=job_id)
