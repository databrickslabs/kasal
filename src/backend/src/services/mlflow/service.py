from typing import Any, Dict, Optional

from databricks.sdk.useragent import with_product
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.repositories.mlflow_repository import MLflowRepository
from src.services.settings.models import ModelConfigService
from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct

# Register User-Agent for Databricks SDK / MLflow calls (module-level)
with_product(
    f"{KASAL_BASE}_{KasalProduct.MLFLOW}", VERSION
)  # kasal_mlflow/0.1.0 User-Agent

# Route MLflowService logs to system.log for user visibility
logger = LoggerManager.get_instance().system


class MLflowService:
    """
    Service layer for MLflow enable/disable and status queries, plus evaluation triggers.
    """

    def __init__(self, session: AsyncSession, group_id: str):
        """
        Initialize MLflow service.

        Args:
            session: Database session
            group_id: Group ID for multi-tenant isolation (REQUIRED for security)

        Raises:
            ValueError: If group_id is None or empty
        """
        if not group_id:
            raise ValueError(
                "SECURITY: group_id is REQUIRED for MLflowService. "
                "All API key operations must be scoped to a group for multi-tenant isolation."
            )
        self.session = session
        self.group_id = group_id
        self.repo = MLflowRepository(session)
        self.exec_repo = ExecutionHistoryRepository(session)
        # SECURITY: Pass group_id for multi-tenant isolation
        self.model_config_service = ModelConfigService(session, group_id=group_id)

    async def is_enabled(self) -> bool:
        return await self.repo.is_enabled(group_id=self.group_id)

    async def set_enabled(self, enabled: bool) -> bool:
        ok = await self.repo.set_enabled(enabled=enabled, group_id=self.group_id)
        if ok:
            # Drop the memoized parent-process setup so the toggle takes effect
            # on the next dispatch instead of after the cache TTL.
            from src.services.otel_tracing.mlflow_parent_setup import (
                invalidate_parent_mlflow_cache,
            )

            invalidate_parent_mlflow_cache()
        return ok

    # Evaluation toggle
    async def is_evaluation_enabled(self) -> bool:
        return await self.repo.is_evaluation_enabled(group_id=self.group_id)

    async def set_evaluation_enabled(self, enabled: bool) -> bool:
        ok = await self.repo.set_evaluation_enabled(
            enabled=enabled, group_id=self.group_id
        )
        return ok

    async def get_settings(self) -> Dict[str, Any]:
        """Everything the MLflow configuration section renders.

        The BACKEND half is derived, never stored: Databricks when a workspace
        is configured, else a local OSS server, else nothing. A stored preference
        could only ever disagree with what is actually available.
        """
        from src.services.mlflow import local

        enabled = await self.repo.is_enabled(group_id=self.group_id)
        evaluation_enabled = await self.repo.is_evaluation_enabled(
            group_id=self.group_id
        )
        experiment_name = await self.repo.get_experiment_name(group_id=self.group_id)
        teamspace = await self._teamspace_name()

        experiment = local.local_experiment_name(experiment_name, teamspace)

        # ALWAYS describe both Databricks and local, marking each `available` or
        # not, so the UI can render every backend as a row (available ones live,
        # unavailable ones greyed out) — the user sees the full picture rather
        # than only whatever happens to exist. Which one a run uses is unchanged
        # (see `backend` below); this list is purely informational.
        workspace_url = await self._configured_workspace_url()
        databricks_available = bool(workspace_url)
        # Show the SAME experiment traces/judges/GEPA use — the -uc name on
        # Databricks — so what the admin attaches matches where traces land.
        databricks_experiment = (
            await self.configured_crew_traces_experiment()
            if databricks_available
            else None
        )
        databricks_backend = {
            "kind": "databricks",
            "available": databricks_available,
            "uri": workspace_url,
            # Reachability for Databricks is an auth question, not a socket one,
            # so it is deliberately not answered here.
            "reachable": None,
            "experiment": databricks_experiment,
            "url": f"{workspace_url}/ml/experiments" if databricks_available else None,
        }
        local_uri = local.local_tracking_uri()
        local_available = bool(local_uri)
        local_backend = {
            "kind": "local",
            "available": local_available,
            "uri": local_uri,
            "reachable": local.is_reachable(local_uri) if local_uri else None,
            "experiment": experiment if local_available else None,
            "url": f"{local_uri}/#/experiments" if local_available else None,
        }
        available = [databricks_backend, local_backend]

        # The ACTIVE backend — resolution unchanged: Databricks wins when a
        # workspace is configured, else a local server, else none.
        if databricks_available:
            backend = databricks_backend
        elif local_available:
            backend = local_backend
        else:
            backend = {"kind": "none", "available": False, "uri": None, "reachable": None}

        return {
            "enabled": enabled,
            "evaluation_enabled": evaluation_enabled,
            "experiment_name": experiment_name,
            "backend": backend,
            "available": available,
        }

    async def update_settings(
        self,
        enabled: Optional[bool] = None,
        evaluation_enabled: Optional[bool] = None,
        experiment_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Apply a partial update; an omitted field is left alone."""
        if enabled is not None:
            await self.set_enabled(enabled)
        if evaluation_enabled is not None:
            await self.repo.set_evaluation_enabled(
                enabled=evaluation_enabled, group_id=self.group_id
            )
        if experiment_name is not None:
            await self.repo.set_experiment_name(experiment_name, group_id=self.group_id)
            # Create the experiment on Databricks now, so an admin can attach it
            # to the app as an MLflow resource (which is what grants the app SP
            # MLflow access). Kasal otherwise creates it lazily on the first
            # traced run — too late to attach up front. Best-effort: a failure
            # here must not block saving the name, but it is surfaced so the UI
            # can tell the user the experiment could not be created.
            await self._ensure_experiment_created()
        return await self.get_settings()

    async def _ensure_experiment_created(self) -> None:
        """Create the configured experiment on Databricks (create-if-missing).

        No-op when Databricks is not configured (local/OSS backends create the
        experiment lazily and need no pre-attachment). Reuses the SPN/PAT auth
        from :meth:`_setup_mlflow_auth`; the blocking create runs in a thread.
        """
        import asyncio

        workspace_url = await self._configured_workspace_url()
        if not workspace_url:
            return  # local/OSS backend — nothing to pre-create

        auth = await self._setup_mlflow_auth()
        if not auth:
            logger.warning(
                "[MLflowService] Could not authenticate to create the experiment; "
                "it will be created on the first traced run instead."
            )
            return

        # Create the SAME experiment the tracer/judges/GEPA use (incl. the -uc
        # suffix on Databricks) so the admin attaches the one traces land in.
        exp_path = await self.configured_crew_traces_experiment()
        # Create WITH UC trace storage so the UC-only charts unlock and MLflow
        # doesn't refuse to add UC storage to this experiment later.
        uc_catalog, uc_schema, warehouse_id = await self._get_uc_trace_config()
        try:
            from src.services.mlflow.experiment_setup import (
                create_databricks_experiment,
            )

            result = await asyncio.to_thread(
                lambda: create_databricks_experiment(
                    auth,
                    exp_path,
                    uc_catalog=uc_catalog,
                    uc_schema=uc_schema,
                    warehouse_id=warehouse_id,
                )
            )
            logger.info(
                f"[MLflowService] Ensured experiment {exp_path} "
                f"(id={result.get('experiment_id')})"
            )
        except Exception as exc:  # noqa: BLE001 — saving the name must still succeed
            logger.warning(
                f"[MLflowService] Could not create experiment {exp_path}: {exc}"
            )

    async def configured_crew_traces_experiment(self) -> str:
        """The experiment crew traces, judges, GEPA runs, and eval all pin.

        THE single source of truth. The name comes from the MLflow configuration
        (Configuration.tsx) resolved via ``local.local_experiment_name``, and —
        crucially on Databricks — carries the SAME ``-uc`` suffix the OTel tracer
        applies (``otel_tracing.mlflow_setup.uc_experiment_name``). Without that
        suffix, crew-execution traces land in ``<base>-uc`` while judges/GEPA
        watched ``<base>``: two experiments for one teamspace (the exact bug this
        unifies). A UC trace destination permanently refuses to link to an
        experiment that already holds non-UC traces, which is why the tracer uses
        the dedicated ``-uc`` name and everyone else must match it.

        Falls back to the per-teamspace default when nothing is configured. NOT
        the old hardcoded ``/Shared/kasal-crew-execution-traces``.
        """
        from src.services.mlflow import local

        experiment_name = await self.repo.get_experiment_name(group_id=self.group_id)
        teamspace = await self._teamspace_name()
        base = f"/Shared/{local.local_experiment_name(experiment_name, teamspace)}"
        # On Databricks the tracer writes to the dedicated -uc experiment; match
        # it so judges/GEPA/eval see the same traces. Local/OSS uses base as-is.
        if await self._configured_workspace_url():
            from src.services.otel_tracing.mlflow_setup import uc_experiment_name

            return uc_experiment_name(base)
        return base

    async def _get_uc_trace_config(self) -> tuple:
        """(catalog, schema, warehouse_id) from the Databricks config, for
        creating an experiment WITH UC trace storage.

        A plain experiment (no trace_location) leaves the UC-only charts locked
        and MLflow then permanently refuses to attach UC storage to that name, so
        every create path resolves these together. Returns ``(None, None, None)``
        on any failure — the caller falls back to a plain experiment.
        """
        try:
            from src.services.databricks.workspace.service import DatabricksService

            db_config = await DatabricksService(
                self.session, group_id=self.group_id
            ).get_databricks_config()
            if not db_config:
                return (None, None, None)
            # schema field is `db_schema` (aliased "schema"); reading "schema"
            # returns BaseModel.schema (a method) -> MLflow error.
            return (
                getattr(db_config, "catalog", None),
                getattr(db_config, "db_schema", None),
                getattr(db_config, "warehouse_id", None),
            )
        except Exception as cfg_err:  # noqa: BLE001 — plain experiment is the fallback
            logger.debug(
                f"[MLflowService] Could not read UC config for experiment: {cfg_err}"
            )
            return (None, None, None)

    async def _trace_id_for(self, job_id: Optional[str]) -> Optional[str]:
        """The MLflow trace id recorded for a job, if any."""
        if not job_id:
            return None
        try:
            exec_obj = await self.exec_repo.get_execution_by_job_id(
                job_id, group_ids=[self.group_id]
            )
        except Exception as e:  # noqa: BLE001 — a deep link must never raise
            logger.warning(
                f"[MLflowService] Failed to get trace ID for job {job_id}: {e}"
            )
            return None
        value = getattr(exec_obj, "mlflow_trace_id", None) if exec_obj else None
        return str(value) if value else None

    def _local_deeplink(
        self, trace_id: Optional[str], teamspace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Deep link into the local MLflow UI. Runs in a thread — it does I/O."""
        from src.services.mlflow import local

        uri = local.local_tracking_uri()
        if not uri:
            return {
                "url": None,
                "experiment_id": "",
                "trace_id": trace_id,
                "workspace_url": None,
                "workspace_id": None,
                "message": (
                    "No MLflow backend is configured. Set a Databricks workspace, "
                    "or start a local MLflow server and launch Kasal with "
                    "MLFLOW_TRACKING_URI pointing at it."
                ),
            }

        name = local.local_experiment_name(teamspace=teamspace)
        exp_id = local.experiment_id(uri, name)
        return {
            "url": local.traces_url(uri, exp_id, trace_id),
            "experiment_id": exp_id,
            "trace_id": trace_id,
            # Named workspace_* for response-shape compatibility with the
            # Databricks branch: the frontend reads `url` and ignores the rest,
            # and diverging the shape by backend would give it a reason not to.
            "workspace_url": uri,
            "workspace_id": None,
        }

    async def _teamspace_name(self) -> Optional[str]:
        """The group's display name, which the default experiment is named for.

        The NAME rather than the id: it is what a person reads in the MLflow UI,
        and ``user_dev_localhost`` tells them less than "Acme Corporation" does.
        Falls back to None (and so to a slugless default) rather than raising —
        an experiment name must never be the thing that fails a run.
        """
        try:
            from src.repositories.group_repository import GroupRepository

            name = await GroupRepository(self.session).get_name(self.group_id)
            # Fall back to the group ID when no row names it. A synthetic
            # teamspace (the dev "user_dev_localhost", an auto-created group)
            # has no `groups` entry, and naming its experiment after the id it
            # DOES have beats the anonymous "kasal-traces" — one experiment
            # shared by every unnamed teamspace is exactly the collision the
            # per-teamspace default exists to avoid.
            return name or self.group_id
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"[MLflowService] Could not resolve teamspace name: {exc}")
            return self.group_id

    async def _configured_workspace_url(self) -> Optional[str]:
        """The workspace URL, or None when Databricks is not configured.

        Deliberately reads the STORED configuration rather than the auth chain:
        the auth chain falls back to ambient environment variables, which on a
        developer machine can report a workspace nobody configured and would
        route tracing away from the local server.
        """
        try:
            from src.services.databricks.workspace.service import DatabricksService

            cfg = await DatabricksService(self.session).get_databricks_config()
        except Exception as exc:  # noqa: BLE001 — absence is a normal dev state
            logger.debug(f"[MLflowService] No Databricks configuration: {exc}")
            return None
        url = (getattr(cfg, "workspace_url", "") or "").strip() if cfg else ""
        if not url:
            return None
        if not url.startswith("http"):
            url = f"https://{url}"
        return url.rstrip("/")

    async def _setup_mlflow_auth(self) -> Optional[Any]:
        """
        Setup MLflow authentication using SPN → PAT priority (matching Lakebase pattern).

        MLflow runs as a service-level operation (experiment tracking, tracing,
        evaluation) and does not need OBO tokens.  OBO tokens frequently lack
        MLflow scopes, causing unnecessary fallback retries.

        Authentication priority:
          1. SPN (Service Principal) — via DATABRICKS_CLIENT_ID + SECRET + HOST env vars
          2. PAT (Personal Access Token) — via get_auth_context(user_token=None)

        Returns:
            AuthContext if authentication was successful, None otherwise
        """
        import os

        try:
            # 1. Try SPN first via environment variables (preferred for deployed apps)
            client_id = os.environ.get("DATABRICKS_CLIENT_ID")
            client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
            host = os.environ.get("DATABRICKS_HOST")

            if client_id and client_secret and host:
                from src.services.mlflow.sp_auth import derive_sp_bearer
                from src.utils.databricks_auth import AuthContext

                # Shared derivation (mlflow/sp_auth.py) — the single, correct
                # implementation for the whole app. Returns None on any failure,
                # so we fall through to PAT.
                spn_token = derive_sp_bearer(host, client_id, client_secret)
                if spn_token:
                    workspace_url = host.rstrip("/")
                    if not workspace_url.startswith("http"):
                        workspace_url = f"https://{workspace_url}"
                    logger.info(
                        "[MLflowService] MLflow authentication configured using service_principal"
                    )
                    return AuthContext(
                        token=spn_token,
                        workspace_url=workspace_url,
                        auth_method="service_principal",
                    )
                logger.warning(
                    "[MLflowService] SPN auth unavailable, falling back to PAT"
                )

            # 2. Fall back to PAT via unified auth chain (skips OBO)
            from src.utils.databricks_auth import get_auth_context

            auth = await get_auth_context(user_token=None)

            if not auth or not auth.workspace_url:
                logger.error("[MLflowService] No authentication available for MLflow")
                return None

            logger.info(
                f"[MLflowService] MLflow authentication configured using {auth.auth_method}"
            )
            return auth

        except Exception as e:
            logger.error(f"[MLflowService] Failed to setup MLflow authentication: {e}")
            return None

    async def get_experiment_info(self) -> Dict[str, Any]:
        """{experiment_id, experiment_name} for crew traces; raises on auth or
        resolution failure."""
        import asyncio

        from src.services.mlflow.experiment_setup import create_databricks_experiment

        # Setup authentication first
        auth = await self._setup_mlflow_auth()
        if not auth:
            raise RuntimeError(
                "Failed to configure MLflow authentication. Please configure Databricks credentials."
            )

        # Resolve the SAME experiment the tracer/judges/GEPA use (the -uc name on
        # Databricks) via the single authority — NOT the old hardcoded getenv,
        # which resolved a different experiment. Pass the UC catalog/schema/
        # warehouse so create-if-missing links UC trace storage (a plain create
        # here would poison the name for UC tracing).
        exp_name = await self.configured_crew_traces_experiment()
        uc_catalog, uc_schema, warehouse_id = await self._get_uc_trace_config()
        try:
            # create-if-missing, in a thread (MLflow calls block); same auth
            # env-swap the settings-save path uses.
            result = await asyncio.to_thread(
                lambda: create_databricks_experiment(
                    auth,
                    exp_name,
                    uc_catalog=uc_catalog,
                    uc_schema=uc_schema,
                    warehouse_id=warehouse_id,
                )
            )

            if not result.get("experiment_id"):
                raise RuntimeError("Failed to resolve MLflow experiment ID")

            return result

        except Exception as e:
            logger.error(f"[MLflowService] Failed to get experiment info: {e}")
            raise

    async def get_trace_deeplink(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Build a deep link to MLflow traces UI, optionally for a specific job execution.

        Args:
            job_id: Optional job ID to link to specific trace

        Returns:
            Dict with url, experiment_id, trace_id, workspace_url, workspace_id
        """
        import asyncio

        from src.utils.databricks_auth import get_auth_context

        # Local backend FIRST: when no Databricks workspace is configured, every
        # step below resolves to nothing and this returned {"url": null,
        # "message": "please configure Databricks credentials"} — which the UI
        # turns into an href of "#", so clicking the MLflow Trace button did
        # nothing at all. A dev machine tracing to a local server has a perfectly
        # good link; it was simply never built.
        if not await self._configured_workspace_url():
            trace_id = await self._trace_id_for(job_id)
            teamspace = await self._teamspace_name()
            return await asyncio.to_thread(self._local_deeplink, trace_id, teamspace)

        # Get workspace URL and ID from unified auth
        workspace_url = ""
        workspace_id = None
        auth = None

        try:
            auth = await get_auth_context(user_token=None)
            if auth and auth.workspace_url:
                workspace_url = auth.workspace_url.rstrip("/")
                logger.info(
                    f"[MLflowService] Using workspace URL from {auth.auth_method} auth: {workspace_url}"
                )

                # Extract workspace ID from URL if available
                # Format: https://xxx.cloud.databricks.com or https://xxx.databricks.com
                if ".databricks.com" in workspace_url:
                    # Try to extract from URL
                    parts = workspace_url.replace("https://", "").split(".")
                    if parts:
                        workspace_id = parts[0]
        except Exception as e:
            logger.warning(f"[MLflowService] Failed to get auth context: {e}")

        # Fallback: try to read workspace URL from stored Databricks configuration
        if not workspace_url:
            try:
                from src.services.databricks.workspace.service import DatabricksService

                svc = DatabricksService(self.session)
                cfg = await svc.get_databricks_config()
                if cfg and getattr(cfg, "workspace_url", None):
                    w = cfg.workspace_url.strip()
                    if w and not w.startswith("http"):
                        w = f"https://{w}"
                    workspace_url = w.rstrip("/")
            except Exception as e:
                logger.warning(
                    f"[MLflowService] Failed to get workspace URL from config: {e}"
                )

        # Resolve experiment id (crew execution traces) - run in thread to avoid blocking.
        # Resolve the experiment NAME via the single authority (applies the -uc
        # suffix on Databricks) so the deep link points at the SAME experiment
        # traces land in — NOT the old hardcoded /Shared/kasal-crew-execution-traces
        # fallback, which sent this button to the wrong experiment.
        experiment_id = ""
        if auth:
            exp_name = await self.configured_crew_traces_experiment()

            def _get_experiment_id(auth_context, experiment_name: str) -> str:
                import mlflow

                from src.services.mlflow.sp_auth import single_auth_env

                try:
                    # Single-method auth (removes OAuth vars, pins auth_type=pat)
                    # so MLflow's SDK client doesn't hit "oauth and pat".
                    with single_auth_env(
                        host=auth_context.workspace_url, token=auth_context.token
                    ):
                        mlflow.set_tracking_uri("databricks")
                        exp = mlflow.get_experiment_by_name(experiment_name)
                        return str(getattr(exp, "experiment_id", "")) if exp else ""
                except Exception as e:
                    logger.warning(f"[MLflowService] Failed to get experiment ID: {e}")
                    return ""

            experiment_id = await asyncio.to_thread(_get_experiment_id, auth, exp_name)
        else:
            logger.warning(
                "[MLflowService] No auth available, cannot resolve experiment ID"
            )

        # Try to extract trace id from the execution record when job_id is provided
        trace_id: Optional[str] = None
        if job_id:
            try:
                exec_obj = await self.exec_repo.get_execution_by_job_id(
                    job_id, group_ids=[self.group_id]
                )
                if exec_obj and getattr(exec_obj, "mlflow_trace_id", None):
                    trace_id = str(exec_obj.mlflow_trace_id)
            except Exception as e:
                logger.warning(
                    f"[MLflowService] Failed to get trace ID for job {job_id}: {e}"
                )

        # Build URL
        if not workspace_url:
            return {
                "url": None,
                "experiment_id": experiment_id,
                "trace_id": trace_id,
                "workspace_url": None,
                "workspace_id": workspace_id,
                "message": "Workspace URL not configured; please configure Databricks credentials",
            }

        base = (
            f"{workspace_url}/ml/experiments/{experiment_id}/traces"
            if experiment_id
            else f"{workspace_url}/ml/experiments"
        )
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

    async def _resolve_judge_model(
        self, configured_judge_model: Optional[str] = None
    ) -> str:
        """
        Resolve the judge model using the model configuration system.
        This ensures proper provider prefixing and authentication setup.

        Args:
            configured_judge_model: Optional configured judge model key

        Returns:
            Properly formatted model name for LiteLLM (e.g., "databricks/databricks-claude-sonnet-4-5")
        """
        import os

        # Get configured judge model from database if not provided
        if not configured_judge_model:
            configured_judge_model = await self.repo.get_evaluation_judge_model(
                group_id=self.group_id
            )

        # Fall back to environment variable
        if not configured_judge_model:
            configured_judge_model = os.getenv("MLFLOW_EVAL_JUDGE_MODEL")

        # Default to databricks-claude-sonnet-4-5 if nothing configured
        if not configured_judge_model:
            configured_judge_model = "databricks-claude-sonnet-4-5"
            logger.info(
                f"[MLflowService] Using default judge model: {configured_judge_model}"
            )
        else:
            logger.info(
                f"[MLflowService] Using configured judge model: {configured_judge_model}"
            )

        # Clean up the model key - remove any provider prefixes or URI schemes
        model_key = configured_judge_model
        if "://" in model_key:
            # Remove URI schemes like "endpoints://" or "databricks://"
            model_key = model_key.split("://", 1)[1]
        if "/" in model_key and not model_key.startswith("databricks/"):
            # Remove provider prefixes except for the final databricks/ prefix we'll add
            parts = model_key.split("/")
            model_key = parts[-1]

        try:
            # Get model configuration to determine provider
            model_config = await self.model_config_service.get_model_config(model_key)
            provider = model_config.get("provider", "").lower()

            # Format model name according to provider requirements
            if provider == "databricks":
                # For Databricks models, LiteLLM requires the databricks/ prefix
                if not model_key.startswith("databricks/"):
                    formatted_model = f"databricks/{model_key}"
                else:
                    formatted_model = model_key
                logger.info(
                    f"[MLflowService] Resolved Databricks judge model: {formatted_model}"
                )
                return formatted_model
            else:
                # For other providers, use the model key as-is or with appropriate prefix
                logger.info(
                    f"[MLflowService] Resolved {provider} judge model: {model_key}"
                )
                return model_key

        except Exception as e:
            logger.warning(
                f"[MLflowService] Could not resolve model config for {model_key}: {e}"
            )
            # Fallback: assume it's a Databricks model and add prefix if needed
            if not model_key.startswith("databricks/"):
                fallback_model = f"databricks/{model_key}"
            else:
                fallback_model = model_key
            logger.info(
                f"[MLflowService] Using fallback judge model format: {fallback_model}"
            )
            return fallback_model

    async def trigger_evaluation(self, job_id: str) -> Dict[str, Any]:
        """
        MLflow 3.x-style evaluation for agent runs leveraging existing traces where possible.
        - Builds a minimal evaluation dataset from the recorded execution (and traces if available)
        - Runs mlflow.genai.evaluate (or mlflow.evaluate fallback) with LLM-judge scorers when configured
        - Returns the evaluation run metadata for deep-linking in the UI
        """
        logger.info(
            f"Triggering MLflow evaluation for job_id={job_id}, group_id={self.group_id}"
        )

        # Check toggle
        if not await self.is_evaluation_enabled():
            raise RuntimeError("MLflow evaluation is disabled for this workspace")

        # Load execution by job_id (respect group isolation if provided)
        exec_obj = await self.exec_repo.get_execution_by_job_id(
            job_id=job_id,
            group_ids=[self.group_id] if self.group_id else None,
        )
        if not exec_obj:
            raise RuntimeError(f"No execution found for job_id={job_id}")

        # Build inputs/predictions from execution record (fallback if traces are unavailable)
        from json import dumps

        inputs_obj: Dict[str, Any] = exec_obj.inputs or {}
        # Prefer a single text field for inputs to enable relevance-style scorers
        candidate_input_keys = [
            "question",
            "query",
            "prompt",
            "input",
            "task",
        ]
        inputs_text = None
        for k in candidate_input_keys:
            val = inputs_obj.get(k) if isinstance(inputs_obj, dict) else None
            if isinstance(val, str) and val.strip():
                inputs_text = val.strip()
                break
        if inputs_text is None:
            # Last resort: compact JSON dump
            try:
                inputs_text = dumps(inputs_obj, ensure_ascii=False)[:4000]
            except Exception:
                inputs_text = str(inputs_obj)[:4000]

        prediction_text = None
        try:
            res = exec_obj.result
            if isinstance(res, dict):
                for key in ("content", "output", "result", "final_answer"):
                    if key in res and isinstance(res[key], str) and res[key].strip():
                        prediction_text = res[key]
                        break
                if prediction_text is None:
                    prediction_text = dumps(res, ensure_ascii=False)[:4000]
            elif isinstance(res, str):
                prediction_text = res
        except Exception:
            prediction_text = None

        import asyncio
        import os

        # Run blocking MLflow 3.x evaluation code in a thread to keep API async/non-blocking
        # Resolve judge model using the model configuration system
        judge_model_route = await self._resolve_judge_model()
        judge_model_defaulted = judge_model_route.endswith(
            "databricks-claude-sonnet-4-5"
        )

        # Get auth context for evaluation (will be passed to thread)
        # IMPORTANT: Use PAT/SPN auth for MLflow evaluation (skip OBO) to avoid scope issues
        # This matches the authentication strategy used in crew execution subprocess
        auth_context = None
        if judge_model_route.startswith("databricks/"):
            from src.utils.databricks_auth import get_auth_context
            from src.utils.user_context import GroupContext, UserContext

            # CRITICAL: Set UserContext with group_id before calling get_auth_context()
            # UserContext is thread-local (contextvars), so we must set it explicitly
            # This ensures get_auth_context() can find the PAT token for this group
            if self.group_id:
                group_ctx = GroupContext(
                    group_ids=[self.group_id],
                    group_email=None,  # Not available in this context
                    access_token=None,  # Not needed for PAT lookup
                )
                UserContext.set_group_context(group_ctx)
                logger.info(
                    f"[MLflowService] Set UserContext with group_id={self.group_id} for PAT lookup"
                )

            # Pass user_token=None to skip OBO and use PAT/SPN directly
            auth_context = await get_auth_context(user_token=None)
            if not auth_context:
                raise RuntimeError(
                    "Failed to configure authentication for MLflow evaluation"
                )

        # Create evaluation runner with extracted parameters
        from src.services.mlflow.evaluation_runner import MLflowEvaluationRunner

        runner = MLflowEvaluationRunner(
            exec_obj=exec_obj,
            job_id=job_id,
            inputs_text=inputs_text,
            prediction_text=prediction_text,
            judge_model_route=judge_model_route,
            judge_model_defaulted=judge_model_defaulted,
            # Evaluate against the SAME experiment tracing/GEPA use — the
            # configured name (Configuration.tsx), not a hardcoded default.
            experiment_name=await self.configured_crew_traces_experiment(),
        )

        # Create evaluation run in background thread
        info = await asyncio.to_thread(runner.create_run, auth_context)
        try:
            if isinstance(info, dict):
                logger.info(
                    f"[MLflowService] MLflow evaluation run created for job_id={job_id}: "
                    f"experiment_id={info.get('experiment_id')}, run_id={info.get('run_id')}"
                )
                run_id_bg = info.get("run_id")
                if run_id_bg:
                    # Fire-and-forget background evaluation metrics logging
                    logger.info(
                        f"[MLflowService] Scheduling background evaluation completion for run_id={run_id_bg}"
                    )
                    asyncio.create_task(
                        asyncio.to_thread(
                            runner.complete_evaluation, run_id_bg, auth_context
                        )
                    )
        except Exception:
            pass

        # Persist evaluation run ID in dedicated database field
        try:
            from src.services.execution.status import ExecutionStatusService

            evaluation_run_id = info.get("run_id")
            if evaluation_run_id:
                success = await ExecutionStatusService.update_mlflow_evaluation_run_id(
                    session=self.session,
                    job_id=job_id,
                    evaluation_run_id=evaluation_run_id,
                )
                if success:
                    logger.info(
                        f"[MLflowService] Successfully stored evaluation run ID {evaluation_run_id} for job_id={job_id}"
                    )
                else:
                    logger.warning(
                        f"[MLflowService] Failed to store evaluation run ID for job_id={job_id}"
                    )
        except Exception as e:
            logger.warning(
                f"[MLflowService] Failed to persist evaluation_run_id for job_id={job_id}: {e}"
            )

        return info
