import logging
from typing import Optional, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.mlflow_repository import MLflowRepository
from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.services.model_config_service import ModelConfigService
from src.core.logger import LoggerManager

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
        # Optional per-request user token for OBO; set via router when available
        self._user_token: Optional[str] = None

    async def is_enabled(self) -> bool:
        return await self.repo.is_enabled(group_id=self.group_id)

    async def set_enabled(self, enabled: bool) -> bool:
        ok = await self.repo.set_enabled(enabled=enabled, group_id=self.group_id)
        return ok

    # Evaluation toggle
    async def is_evaluation_enabled(self) -> bool:
        return await self.repo.is_evaluation_enabled(group_id=self.group_id)

    async def set_evaluation_enabled(self, enabled: bool) -> bool:
        ok = await self.repo.set_evaluation_enabled(enabled=enabled, group_id=self.group_id)
        return ok

    # Optional OBO token setter (router can inject per-request user token)
    def set_user_token(self, token: Optional[str]) -> None:
        try:
            self._user_token = token if (isinstance(token, str) and token.strip()) else None
        except Exception:
            self._user_token = None

    async def _setup_mlflow_auth(self) -> Optional[Any]:
        """
        Setup MLflow authentication using unified auth chain (OBO → PAT → SPN).

        Returns:
            AuthContext if authentication was successful, None otherwise
        """
        from src.utils.databricks_auth import get_auth_context

        try:
            # Use unified auth chain: OBO (if user_token set) → PAT → SPN
            auth = await get_auth_context(user_token=self._user_token)

            if not auth or not auth.workspace_url:
                logger.error("[MLflowService] No authentication available for MLflow")
                return None

            logger.info(f"[MLflowService] MLflow authentication configured using {auth.auth_method}")
            return auth

        except Exception as e:
            logger.error(f"[MLflowService] Failed to setup MLflow authentication: {e}")
            return None

    async def get_experiment_info(self) -> Dict[str, Any]:
        """
        Get MLflow experiment info for crew execution traces.

        Returns:
            Dict with experiment_id and experiment_name

        Raises:
            RuntimeError: If authentication fails or experiment cannot be resolved
        """
        import asyncio

        # Setup authentication first
        auth = await self._setup_mlflow_auth()
        if not auth:
            raise RuntimeError("Failed to configure MLflow authentication. Please configure Databricks credentials.")

        # Run blocking MLflow operations in thread to keep async
        # Pass auth context to thread to avoid race conditions
        def _resolve_experiment(auth_context) -> Dict[str, Any]:
            import mlflow
            from databricks.sdk.core import Config

            # Create Databricks config for MLflow
            # MLflow will use this config internally without environment variables
            cfg = Config(
                host=auth_context.workspace_url,
                token=auth_context.token
            )

            # Set tracking URI with databricks:// scheme
            # MLflow will use the SDK's default credential provider
            mlflow.set_tracking_uri("databricks")

            # Temporarily set credentials for this thread only
            # This is unavoidable with MLflow's current design
            import os
            old_host = os.environ.get("DATABRICKS_HOST")
            old_token = os.environ.get("DATABRICKS_TOKEN")

            try:
                os.environ["DATABRICKS_HOST"] = auth_context.workspace_url
                os.environ["DATABRICKS_TOKEN"] = auth_context.token

                # Our standard experiment for crew execution traces
                exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")

                # set_experiment returns an Experiment object (creates if missing)
                exp = mlflow.set_experiment(exp_name)

                return {
                    "experiment_id": str(getattr(exp, "experiment_id", "")),
                    "experiment_name": exp_name,
                }
            finally:
                # Restore original environment
                if old_host is not None:
                    os.environ["DATABRICKS_HOST"] = old_host
                elif "DATABRICKS_HOST" in os.environ:
                    del os.environ["DATABRICKS_HOST"]

                if old_token is not None:
                    os.environ["DATABRICKS_TOKEN"] = old_token
                elif "DATABRICKS_TOKEN" in os.environ:
                    del os.environ["DATABRICKS_TOKEN"]

        try:
            result = await asyncio.to_thread(_resolve_experiment, auth)

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

        # Get workspace URL and ID from unified auth
        workspace_url = ""
        workspace_id = None
        auth = None

        try:
            auth = await get_auth_context(user_token=self._user_token)
            if auth and auth.workspace_url:
                workspace_url = auth.workspace_url.rstrip("/")
                logger.info(f"[MLflowService] Using workspace URL from {auth.auth_method} auth: {workspace_url}")

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
                from src.services.databricks_service import DatabricksService
                svc = DatabricksService(self.session)
                cfg = await svc.get_databricks_config()
                if cfg and getattr(cfg, "workspace_url", None):
                    w = cfg.workspace_url.strip()
                    if w and not w.startswith("http"):
                        w = f"https://{w}"
                    workspace_url = w.rstrip("/")
            except Exception as e:
                logger.warning(f"[MLflowService] Failed to get workspace URL from config: {e}")

        # Resolve experiment id (crew execution traces) - run in thread to avoid blocking
        experiment_id = ""
        if auth:
            def _get_experiment_id(auth_context) -> str:
                import mlflow
                import os
                try:
                    # Temporarily set credentials for this thread only
                    old_host = os.environ.get("DATABRICKS_HOST")
                    old_token = os.environ.get("DATABRICKS_TOKEN")

                    try:
                        os.environ["DATABRICKS_HOST"] = auth_context.workspace_url
                        os.environ["DATABRICKS_TOKEN"] = auth_context.token

                        mlflow.set_tracking_uri("databricks")
                        exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")
                        exp = mlflow.get_experiment_by_name(exp_name)
                        return str(getattr(exp, "experiment_id", "")) if exp else ""
                    finally:
                        # Restore original environment
                        if old_host is not None:
                            os.environ["DATABRICKS_HOST"] = old_host
                        elif "DATABRICKS_HOST" in os.environ:
                            del os.environ["DATABRICKS_HOST"]

                        if old_token is not None:
                            os.environ["DATABRICKS_TOKEN"] = old_token
                        elif "DATABRICKS_TOKEN" in os.environ:
                            del os.environ["DATABRICKS_TOKEN"]
                except Exception as e:
                    logger.warning(f"[MLflowService] Failed to get experiment ID: {e}")
                    return ""

            experiment_id = await asyncio.to_thread(_get_experiment_id, auth)
        else:
            logger.warning("[MLflowService] No auth available, cannot resolve experiment ID")

        # Try to extract trace id from the execution record when job_id is provided
        trace_id: Optional[str] = None
        if job_id:
            try:
                exec_obj = await self.exec_repo.get_execution_by_job_id(
                    job_id,
                    group_ids=[self.group_id]
                )
                if exec_obj and getattr(exec_obj, "mlflow_trace_id", None):
                    trace_id = str(exec_obj.mlflow_trace_id)
            except Exception as e:
                logger.warning(f"[MLflowService] Failed to get trace ID for job {job_id}: {e}")

        # Build URL
        if not workspace_url:
            return {
                "url": None,
                "experiment_id": experiment_id,
                "trace_id": trace_id,
                "workspace_url": None,
                "workspace_id": workspace_id,
                "message": "Workspace URL not configured; please configure Databricks credentials"
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

    async def _resolve_judge_model(self, configured_judge_model: Optional[str] = None) -> str:
        """
        Resolve the judge model using the model configuration system.
        This ensures proper provider prefixing and authentication setup.

        Args:
            configured_judge_model: Optional configured judge model key

        Returns:
            Properly formatted model name for LiteLLM (e.g., "databricks/databricks-claude-sonnet-4")
        """
        import os

        # Get configured judge model from database if not provided
        if not configured_judge_model:
            configured_judge_model = await self.repo.get_evaluation_judge_model(group_id=self.group_id)

        # Fall back to environment variable
        if not configured_judge_model:
            configured_judge_model = os.getenv("MLFLOW_EVAL_JUDGE_MODEL")

        # Default to databricks-claude-sonnet-4 if nothing configured
        if not configured_judge_model:
            configured_judge_model = "databricks-claude-sonnet-4"
            logger.info(f"[MLflowService] Using default judge model: {configured_judge_model}")
        else:
            logger.info(f"[MLflowService] Using configured judge model: {configured_judge_model}")

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
                logger.info(f"[MLflowService] Resolved Databricks judge model: {formatted_model}")
                return formatted_model
            else:
                # For other providers, use the model key as-is or with appropriate prefix
                logger.info(f"[MLflowService] Resolved {provider} judge model: {model_key}")
                return model_key

        except Exception as e:
            logger.warning(f"[MLflowService] Could not resolve model config for {model_key}: {e}")
            # Fallback: assume it's a Databricks model and add prefix if needed
            if not model_key.startswith("databricks/"):
                fallback_model = f"databricks/{model_key}"
            else:
                fallback_model = model_key
            logger.info(f"[MLflowService] Using fallback judge model format: {fallback_model}")
            return fallback_model

    async def trigger_evaluation(self, job_id: str) -> Dict[str, Any]:
        """
        MLflow 3.x-style evaluation for agent runs leveraging existing traces where possible.
        - Builds a minimal evaluation dataset from the recorded execution (and traces if available)
        - Runs mlflow.genai.evaluate (or mlflow.evaluate fallback) with LLM-judge scorers when configured
        - Returns the evaluation run metadata for deep-linking in the UI
        """
        logger.info(f"Triggering MLflow evaluation for job_id={job_id}, group_id={self.group_id}")

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

        import os
        import asyncio

        # Run blocking MLflow 3.x evaluation code in a thread to keep API async/non-blocking
        # Resolve judge model using the model configuration system
        judge_model_route = await self._resolve_judge_model()
        judge_model_defaulted = judge_model_route.endswith("databricks-claude-sonnet-4")

        # Get auth context for evaluation (will be passed to thread)
        # IMPORTANT: Use PAT/SPN auth for MLflow evaluation (skip OBO) to avoid scope issues
        # This matches the authentication strategy used in crew execution subprocess
        auth_context = None
        if judge_model_route.startswith("databricks/"):
            from src.utils.databricks_auth import get_auth_context
            from src.utils.user_context import UserContext, GroupContext

            # CRITICAL: Set UserContext with group_id before calling get_auth_context()
            # UserContext is thread-local (contextvars), so we must set it explicitly
            # This ensures get_auth_context() can find the PAT token for this group
            if self.group_id:
                group_ctx = GroupContext(
                    group_ids=[self.group_id],
                    group_email=None,  # Not available in this context
                    access_token=None  # Not needed for PAT lookup
                )
                UserContext.set_group_context(group_ctx)
                logger.info(f"[MLflowService] Set UserContext with group_id={self.group_id} for PAT lookup")

            # Pass user_token=None to skip OBO and use PAT/SPN directly
            auth_context = await get_auth_context(user_token=None)
            if not auth_context:
                raise RuntimeError("Failed to configure authentication for MLflow evaluation")

        # Create evaluation runner with extracted parameters
        from src.services.mlflow_evaluation_runner import MLflowEvaluationRunner
        runner = MLflowEvaluationRunner(
            exec_obj=exec_obj,
            job_id=job_id,
            inputs_text=inputs_text,
            prediction_text=prediction_text,
            judge_model_route=judge_model_route,
            judge_model_defaulted=judge_model_defaulted,
        )

        # Create evaluation run in background thread
        info = await asyncio.to_thread(runner.create_run, auth_context)
        try:
            if isinstance(info, dict):
                logger.info(
                    f"[MLflowService] MLflow evaluation run created for job_id={job_id}: "
                    f"experiment_id={info.get('experiment_id')}, run_id={info.get('run_id')}"
                )
                run_id_bg = info.get('run_id')
                if run_id_bg:
                    # Fire-and-forget background evaluation metrics logging
                    logger.info(f"[MLflowService] Scheduling background evaluation completion for run_id={run_id_bg}")
                    asyncio.create_task(asyncio.to_thread(runner.complete_evaluation, run_id_bg, auth_context))
        except Exception:
            pass

        # Persist evaluation run ID in dedicated database field
        try:
            from src.services.execution_status_service import ExecutionStatusService
            evaluation_run_id = info.get("run_id")
            if evaluation_run_id:
                success = await ExecutionStatusService.update_mlflow_evaluation_run_id(
                    session=self.session,
                    job_id=job_id,
                    evaluation_run_id=evaluation_run_id
                )
                if success:
                    logger.info(f"[MLflowService] Successfully stored evaluation run ID {evaluation_run_id} for job_id={job_id}")
                else:
                    logger.warning(f"[MLflowService] Failed to store evaluation run ID for job_id={job_id}")
        except Exception as e:
            logger.warning(f"[MLflowService] Failed to persist evaluation_run_id for job_id={job_id}: {e}")

        return info
