from pydantic import BaseModel
from typing import Optional


class MLflowConfigUpdate(BaseModel):
    enabled: bool


class MLflowConfigResponse(BaseModel):
    enabled: bool

class MLflowEvaluateRequest(BaseModel):
    job_id: str


class MLflowEvaluateResponse(BaseModel):
    experiment_id: Optional[str] = None
    run_id: Optional[str] = None
    experiment_name: Optional[str] = None
