import logging
from fastapi import APIRouter, Depends, HTTPException, Request

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
async def trigger_evaluation(payload: MLflowEvaluateRequest, request: Request, session: SessionDep, group_ctx: GroupContextDep) -> MLflowEvaluateResponse:
    try:
        if not payload.job_id:
            raise HTTPException(status_code=400, detail="job_id is required")
        svc = MLflowService(session, group_id=group_ctx.primary_group_id if group_ctx else None)
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger MLflow evaluation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


from typing import Dict, Optional
import asyncio

@router.get("/experiment-info", response_model=Dict)
async def get_mlflow_experiment_info(session: SessionDep, group_ctx: GroupContextDep) -> Dict:
    """
    Return MLflow experiment info used for tracing UI deep links.
    - experiment_id: Numeric ID for the crew execution traces experiment
    - experiment_name: Name/path of the experiment
    """


@router.get("/trace-link", response_model=Dict)
async def get_trace_deeplink(job_id: Optional[str] = None, session: SessionDep = None, group_ctx: GroupContextDep = None) -> Dict:
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
    try:
        import os
        import mlflow
        from src.repositories.execution_history_repository import ExecutionHistoryRepository

        # Determine workspace URL and ID from environment (same as frontend does via /databricks/environment)
        workspace_url = os.getenv("DATABRICKS_HOST", "").strip()
        if workspace_url and not workspace_url.startswith("http"):
            workspace_url = f"https://{workspace_url}"
        workspace_url = workspace_url.rstrip("/") if workspace_url else ""
        workspace_id = os.getenv("DATABRICKS_WORKSPACE_ID")

        # Fallback: try to read workspace URL from stored Databricks configuration
        if not workspace_url:
            try:
                from src.services.databricks_service import DatabricksService
                svc = DatabricksService(session)
                cfg = await svc.get_databricks_config()
                if cfg and getattr(cfg, "workspace_url", None):
                    w = cfg.workspace_url.strip()
                    if w and not w.startswith("http"):
                        w = f"https://{w}"
                    workspace_url = w.rstrip("/")
            except Exception:
                pass

        # Resolve experiment id (crew execution traces)
        mlflow.set_tracking_uri("databricks")
        exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")
        exp = mlflow.get_experiment_by_name(exp_name)
        experiment_id = str(getattr(exp, "experiment_id", "")) if exp else ""

        # Try to extract trace id from the execution record when job_id is provided
        trace_id: Optional[str] = None
        if job_id:
            repo = ExecutionHistoryRepository(session)
            exec_obj = await repo.get_execution_by_job_id(job_id, group_ids=(group_ctx.group_ids if group_ctx else None))
            if exec_obj and getattr(exec_obj, "mlflow_trace_id", None):
                trace_id = exec_obj.mlflow_trace_id

        # Build URL
        if not workspace_url:
            # Best effort to obtain workspace URL via mlflow client
            # If not available, return minimal info so the client can fallback
            return {
                "url": None,
                "experiment_id": experiment_id,
                "trace_id": trace_id,
                "workspace_url": None,
                "workspace_id": workspace_id,
                "message": "Workspace URL not configured; set DATABRICKS_HOST to enable deep links"
            }

        base = f"{workspace_url}/ml/experiments/{experiment_id}/traces" if experiment_id else f"{workspace_url}/ml/experiments"
        params = []
        if workspace_id:
            params.append(f"o={workspace_id}")
        if trace_id:
            params.append(f"selectedEvaluationId={trace_id}")
        url = base + ("?" + "&".join(params) if params else "")

        return {
            "url": url,
            "experiment_id": experiment_id,
            "trace_id": trace_id,
            "workspace_url": workspace_url,
            "workspace_id": workspace_id,
        }
    except Exception as e:
        logger.error(f"Failed to build MLflow trace deep link: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
