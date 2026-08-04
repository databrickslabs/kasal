from typing import List, Optional

from pydantic import BaseModel


class MLflowConfigUpdate(BaseModel):
    enabled: bool


class MLflowConfigResponse(BaseModel):
    enabled: bool


class MLflowBackend(BaseModel):
    """Where traces go — DERIVED, never chosen.

    A stored backend preference can only ever disagree with what is actually
    available (picking "Databricks" with no workspace configured is a setting
    that is wrong by definition), so the UI shows this rather than offering it.
    """

    #: "databricks" | "local" | "none"
    kind: str
    #: Whether this backend is actually usable right now — a Databricks workspace
    #: is configured, or a local server URI is set (and reachable). Lets the UI
    #: show every backend as a row and grey out the ones that are not available.
    available: bool = True
    #: Workspace URL or local server URI; None when there is no backend at all.
    uri: Optional[str] = None
    #: Whether a local server is actually answering. None for Databricks, where
    #: reachability is an auth question rather than a socket one.
    reachable: Optional[bool] = None
    #: The experiment as the backend will actually name it — the Databricks
    #: backend prefixes "/Shared/", the local one does not.
    experiment: Optional[str] = None
    #: Deep link to the experiment's traces, when one can be built.
    url: Optional[str] = None


class MLflowSettings(BaseModel):
    """Everything the MLflow configuration section renders."""

    enabled: bool
    evaluation_enabled: bool
    experiment_name: Optional[str] = None
    #: The backend a run WILL use — still derived, never chosen (Databricks when
    #: a workspace is configured, else a local server, else none).
    backend: MLflowBackend
    #: Every backend candidate the environment offers, so the UI can show
    #: Databricks / Local / None side by side with per-backend reachability. The
    #: ``kind`` matching ``backend.kind`` is the active one. Purely informational:
    #: which one a run uses is still ``backend``, not a selection made here.
    available: List[MLflowBackend] = []


class MLflowSettingsUpdate(BaseModel):
    """Partial update — an omitted field is left alone."""

    enabled: Optional[bool] = None
    evaluation_enabled: Optional[bool] = None
    experiment_name: Optional[str] = None


class MLflowEvaluateRequest(BaseModel):
    job_id: str


class MLflowEvaluateResponse(BaseModel):
    experiment_id: Optional[str] = None
    run_id: Optional[str] = None
    experiment_name: Optional[str] = None
