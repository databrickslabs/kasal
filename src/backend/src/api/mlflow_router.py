import logging
from fastapi import APIRouter, Depends, HTTPException

from src.core.dependencies import SessionDep, GroupContextDep
from src.schemas.mlflow import (
    MLflowConfigUpdate,
    MLflowConfigResponse,
    MLflowEvaluateRequest,
    MLflowEvaluateResponse,
)
from src.services.mlflow_service import MLflowService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mlflow", tags=["mlflow"])


@router.get("/status", response_model=MLflowConfigResponse)
async def get_mlflow_status(session: SessionDep, group_ctx: GroupContextDep) -> MLflowConfigResponse:
    try:
        svc = MLflowService(session, group_id=group_ctx.primary_group_id if group_ctx else None)
        enabled = await svc.is_enabled()
        return MLflowConfigResponse(enabled=enabled)
    except Exception as e:
        logger.error(f"Failed to get MLflow status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/status", response_model=MLflowConfigResponse)
async def set_mlflow_status(payload: MLflowConfigUpdate, session: SessionDep, group_ctx: GroupContextDep) -> MLflowConfigResponse:
    try:
        svc = MLflowService(session, group_id=group_ctx.primary_group_id if group_ctx else None)
        ok = await svc.set_enabled(payload.enabled)
        if not ok:
            raise HTTPException(status_code=404, detail="No Databricks configuration to attach MLflow setting to")
        return MLflowConfigResponse(enabled=payload.enabled)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to set MLflow status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Evaluation toggles
@router.get("/evaluation-status", response_model=MLflowConfigResponse)
async def get_evaluation_status(session: SessionDep, group_ctx: GroupContextDep) -> MLflowConfigResponse:
    try:
        svc = MLflowService(session, group_id=group_ctx.primary_group_id if group_ctx else None)
        enabled = await svc.is_evaluation_enabled()
        return MLflowConfigResponse(enabled=enabled)
    except Exception as e:
        logger.error(f"Failed to get MLflow evaluation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/evaluation-status", response_model=MLflowConfigResponse)
async def set_evaluation_status(payload: MLflowConfigUpdate, session: SessionDep, group_ctx: GroupContextDep) -> MLflowConfigResponse:
    try:
        svc = MLflowService(session, group_id=group_ctx.primary_group_id if group_ctx else None)
        ok = await svc.set_evaluation_enabled(payload.enabled)
        if not ok:
            raise HTTPException(status_code=404, detail="No Databricks configuration to attach evaluation setting to")
        return MLflowConfigResponse(enabled=payload.enabled)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to set MLflow evaluation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Trigger minimal evaluation and return run info
@router.post("/evaluate", response_model=MLflowEvaluateResponse)
async def trigger_evaluation(payload: MLflowEvaluateRequest, session: SessionDep, group_ctx: GroupContextDep) -> MLflowEvaluateResponse:
    try:
        if not payload.job_id:
            raise HTTPException(status_code=400, detail="job_id is required")
        svc = MLflowService(session, group_id=group_ctx.primary_group_id if group_ctx else None)
        info = await svc.trigger_evaluation(payload.job_id)
        return MLflowEvaluateResponse(**info)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger MLflow evaluation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


from typing import Dict
import asyncio

@router.get("/experiment-info", response_model=Dict)
async def get_mlflow_experiment_info(session: SessionDep, group_ctx: GroupContextDep) -> Dict:
    """
    Return MLflow experiment info used for tracing UI deep links.
    - experiment_id: Numeric ID for the crew execution traces experiment
    - experiment_name: Name/path of the experiment
    """
    try:
        # Run blocking mlflow operations in a thread to keep API async
        def _resolve_experiment() -> Dict:
            import os
            import mlflow
            # Ensure Databricks tracking
            mlflow.set_tracking_uri("databricks")
            # Our standard experiment for crew execution traces
            exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")
            # set_experiment returns an Experiment object (creates if missing)
            exp = mlflow.set_experiment(exp_name)
            return {
                "experiment_id": str(getattr(exp, "experiment_id", "")),
                "experiment_name": exp_name,
            }

        result = await asyncio.to_thread(_resolve_experiment)
        if not result.get("experiment_id"):
            raise HTTPException(status_code=500, detail="Failed to resolve MLflow experiment ID")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get MLflow experiment info: {e}")
        raise HTTPException(status_code=500, detail=str(e))
