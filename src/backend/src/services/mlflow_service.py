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
        auth_context = None
        if judge_model_route.startswith("databricks/"):
            auth_context = await self._setup_mlflow_auth()
            if not auth_context:
                raise RuntimeError("Failed to configure authentication for MLflow evaluation")

        def _create_run_sync(auth_ctx) -> Dict[str, Any]:
            import mlflow
            import pandas as pd

            # Set up environment variables within thread context only
            old_host = None
            old_token = None
            old_base_url = None
            old_api_base = None
            old_endpoint = None

            try:
                if auth_ctx:
                    # Save old values
                    old_host = os.environ.get("DATABRICKS_HOST")
                    old_token = os.environ.get("DATABRICKS_TOKEN")
                    old_base_url = os.environ.get("DATABRICKS_BASE_URL")
                    old_api_base = os.environ.get("DATABRICKS_API_BASE")
                    old_endpoint = os.environ.get("DATABRICKS_ENDPOINT")

                    # Set for this thread
                    os.environ["DATABRICKS_HOST"] = auth_ctx.workspace_url
                    os.environ["DATABRICKS_TOKEN"] = auth_ctx.token

                    # Set additional API base URLs for consistency
                    from src.utils.databricks_url_utils import DatabricksURLUtils
                    api_base = DatabricksURLUtils.construct_serving_endpoints_url(auth_ctx.workspace_url) or ""
                    if api_base:
                        os.environ["DATABRICKS_BASE_URL"] = api_base
                        os.environ["DATABRICKS_API_BASE"] = api_base
                        os.environ["DATABRICKS_ENDPOINT"] = api_base

                # Ensure Databricks tracking and target experiments (consolidated: use traces experiment)
                mlflow.set_tracking_uri("databricks")
                eval_exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")
                eval_exp = mlflow.set_experiment(eval_exp_name)

                # Discover related traces and build a trace-derived evaluation dataset (best-effort)
                related_trace_ids = []
                records = []

                # First, try to get the trace ID directly from the execution history
                stored_trace_id = None
                try:
                    stored_trace_id = getattr(exec_obj, 'mlflow_trace_id', None)
                    if stored_trace_id:
                        logger.info(f"[MLflowService] Found stored trace ID: {stored_trace_id}")
                        related_trace_ids = [stored_trace_id]
                except Exception as e:
                    logger.warning(f"[MLflowService] Could not retrieve stored trace ID: {e}")

                # If no stored trace ID, fall back to searching for traces
                if not stored_trace_id:
                    logger.info("[MLflowService] No stored trace ID found, searching for traces by execution_id")
                    try:
                        search_traces = getattr(mlflow, "search_traces", None)
                        if callable(search_traces):
                            # Determine crew traces experiment
                            traces_exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")
                            traces_exp = mlflow.get_experiment_by_name(traces_exp_name)
                            traces_exp_id = str(getattr(traces_exp, "experiment_id", "")) if traces_exp else ""
                            df = search_traces(experiment_ids=[traces_exp_id]) if traces_exp_id else search_traces()
                            # Heuristic filter: match spans that carry our execution_id/job_id
                            # Different MLflow versions expose different column shapes; guard accordingly
                            if df is not None and len(df) > 0:
                                df_sel = df
                                if "attributes" in df.columns:
                                    try:
                                        mask = df["attributes"].apply(
                                            lambda attrs: isinstance(attrs, dict)
                                            and (attrs.get("execution_id") == getattr(exec_obj, "id", None)
                                                 or attrs.get("execution_id") == job_id)
                                        )
                                        df_sel = df.loc[mask]
                                        related_trace_ids = [str(x) for x in df_sel.get("trace_id", []).tolist()]
                                    except Exception:
                                        pass
                                # Build records from selected trace rows
                                try:
                                    import json as _json
                                    max_rows = int(os.getenv("MLFLOW_EVAL_MAX_ROWS", "200"))
                                    for _, r in df_sel.head(max_rows).iterrows():
                                        attrs = r.get("attributes", {}) if isinstance(r.get("attributes", {}), dict) else {}
                                        def _pick(keys):
                                            # Search attrs first, then row columns
                                            for kk in keys:
                                                v = attrs.get(kk) if isinstance(attrs, dict) else None
                                                if (v is None) and (kk in r):
                                                    v = r[kk]
                                                if isinstance(v, (dict, list)):
                                                    try:
                                                        v = _json.dumps(v, ensure_ascii=False)
                                                    except Exception:
                                                        v = str(v)
                                                if isinstance(v, str) and v.strip():
                                                    return v.strip()
                                            return None
                                        msg = _pick(["prompt", "question", "query", "input", "messages", "user_input", "task", "request"])
                                        pred = _pick(["output", "response", "content", "answer", "final_answer", "text"])
                                        ctx = _pick(["contexts", "context", "retrieved_contexts", "docs", "documents"])
                                        ref = _pick(["reference", "ground_truth", "expected_answer", "label"])
                                        if msg is None and pred is None:
                                            continue
                                        rec = {"messages": msg or "", "predictions": pred or ""}
                                        if isinstance(ctx, str) and ctx.strip():
                                            rec["contexts"] = ctx
                                        if isinstance(ref, str) and ref.strip():
                                            rec["references"] = ref
                                        records.append(rec)
                                except Exception:
                                    pass
                    except Exception:
                        pass

                # Fallback to single-record dataset from execution if trace-derived dataset is empty
                if not records:
                    records = [{"messages": inputs_text or "", "predictions": prediction_text or ""}]
                eval_df = pd.DataFrame.from_records(records)
                try:
                    logger.info(f"[MLflowService] Built evaluation dataset for job_id={job_id}: rows={len(eval_df)}, columns={list(eval_df.columns)}")
                except Exception:
                    pass

                # Ensure we are back on the evaluation experiment before starting the run
                mlflow.set_experiment(eval_exp_name)
                with mlflow.start_run(run_name=f"evaluation-{job_id}") as run:
                    run_id = run.info.run_id
                    try:
                        logger.info(f"[MLflowService] Started evaluation run: experiment='{eval_exp_name}', run_id={run_id}, job_id={job_id}")
                    except Exception:
                        pass
                    # Log linkage and context
                    try:
                        mlflow.log_params({
                            "job_id": job_id,
                            "group_id": getattr(exec_obj, "group_id", ""),
                            "status": getattr(exec_obj, "status", ""),
                            "related_trace_ids": ",".join(related_trace_ids) if related_trace_ids else "",
                            "judge_model_configured": bool(judge_model_route),
                            "judge_model_route": str(judge_model_route or ""),
                            "judge_model_defaulted": bool(judge_model_defaulted),
                        })
                    except Exception:
                        pass

                    # Always log simple baseline metrics so runs display metrics
                    try:
                        preds = eval_df["predictions"].tolist() if "predictions" in eval_df.columns else []
                        msgs = eval_df["messages"].tolist() if "messages" in eval_df.columns else []
                        pred_lengths = [len(str(x)) for x in preds]
                        if pred_lengths:
                            mlflow.log_metric("prediction_length_mean", float(sum(pred_lengths) / max(len(pred_lengths), 1)))
                            mlflow.log_metric("prediction_length_max", float(max(pred_lengths)))
                        # Word-count metrics
                        def _wc(s):
                            try:
                                return float(len(str(s).split()))
                            except Exception:
                                return 0.0
                        pred_wc = [_wc(p) for p in preds]
                        msg_wc = [_wc(m) for m in msgs]
                        if pred_wc:
                            mlflow.log_metric("prediction_word_count_mean", float(sum(pred_wc) / max(len(pred_wc), 1)))
                        if msg_wc:
                            mlflow.log_metric("input_word_count_mean", float(sum(msg_wc) / max(len(msg_wc), 1)))
                        # Simple Jaccard overlap between input and prediction words
                        try:
                            overlaps = []
                            for m, p in zip(msgs, preds):
                                ms = set(str(m).lower().split())
                                ps = set(str(p).lower().split())
                                inter = len(ms & ps)
                                union = max(len(ms | ps), 1)
                                overlaps.append(float(inter) / float(union))
                            if overlaps:
                                mlflow.log_metric("overlap_jaccard_mean", float(sum(overlaps) / max(len(overlaps), 1)))
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # Log raw inputs/prediction for reference
                    try:
                        mlflow.log_text(inputs_text or "", artifact_file="inputs.txt")
                    except Exception:
                        pass
                    try:
                        mlflow.log_text(prediction_text or "", artifact_file="prediction.txt")
                    except Exception:
                        pass

                    # Register dataset as MLflow input (MLflow 3.x)
                    try:
                        ds = mlflow.data.from_pandas(
                            df=eval_df,
                            name="agent_eval_dataset",
                            predictions="predictions",
                        )
                        mlflow.log_input(dataset=ds)
                    except Exception:
                        ds = None

                return {
                    "experiment_id": str(getattr(run, "info", object()).experiment_id if hasattr(getattr(run, "info", object()), "experiment_id") else ""),
                    "experiment_name": eval_exp_name,
                    "run_id": run_id,
                }
            finally:
                # Restore original environment variables
                if auth_ctx:
                    if old_host is not None:
                        os.environ["DATABRICKS_HOST"] = old_host
                    elif "DATABRICKS_HOST" in os.environ:
                        del os.environ["DATABRICKS_HOST"]

                    if old_token is not None:
                        os.environ["DATABRICKS_TOKEN"] = old_token
                    elif "DATABRICKS_TOKEN" in os.environ:
                        del os.environ["DATABRICKS_TOKEN"]

                    if old_base_url is not None:
                        os.environ["DATABRICKS_BASE_URL"] = old_base_url
                    elif "DATABRICKS_BASE_URL" in os.environ:
                        del os.environ["DATABRICKS_BASE_URL"]

                    if old_api_base is not None:
                        os.environ["DATABRICKS_API_BASE"] = old_api_base
                    elif "DATABRICKS_API_BASE" in os.environ:
                        del os.environ["DATABRICKS_API_BASE"]

                    if old_endpoint is not None:
                        os.environ["DATABRICKS_ENDPOINT"] = old_endpoint
                    elif "DATABRICKS_ENDPOINT" in os.environ:
                        del os.environ["DATABRICKS_ENDPOINT"]

        def _complete_eval_sync(run_id: str, auth_ctx_for_eval) -> None:
            import mlflow
            import pandas as pd

            # Set up environment variables for this background thread
            old_host_eval = None
            old_token_eval = None
            old_base_url_eval = None
            old_api_base_eval = None
            old_endpoint_eval = None

            try:
                if auth_ctx_for_eval:
                    # Save old values
                    old_host_eval = os.environ.get("DATABRICKS_HOST")
                    old_token_eval = os.environ.get("DATABRICKS_TOKEN")
                    old_base_url_eval = os.environ.get("DATABRICKS_BASE_URL")
                    old_api_base_eval = os.environ.get("DATABRICKS_API_BASE")
                    old_endpoint_eval = os.environ.get("DATABRICKS_ENDPOINT")

                    # Set for this thread
                    os.environ["DATABRICKS_HOST"] = auth_ctx_for_eval.workspace_url
                    os.environ["DATABRICKS_TOKEN"] = auth_ctx_for_eval.token

                    # Set additional API base URLs for consistency
                    from src.utils.databricks_url_utils import DatabricksURLUtils
                    api_base = DatabricksURLUtils.construct_serving_endpoints_url(auth_ctx_for_eval.workspace_url) or ""
                    if api_base:
                        os.environ["DATABRICKS_BASE_URL"] = api_base
                        os.environ["DATABRICKS_API_BASE"] = api_base
                        os.environ["DATABRICKS_ENDPOINT"] = api_base

                # Rebuild dataset similarly to creation step
                mlflow.set_tracking_uri("databricks")

                # Attempt to rebuild eval dataset (same logic)
                records = []
                related_trace_ids = []

                # First, try to get the trace ID directly from the execution history
                stored_trace_id = None
                try:
                    stored_trace_id = getattr(exec_obj, 'mlflow_trace_id', None)
                    if stored_trace_id:
                        logger.info(f"[MLflowService] Found stored trace ID for evaluation: {stored_trace_id}")
                        related_trace_ids = [stored_trace_id]
                except Exception as e:
                    logger.warning(f"[MLflowService] Could not retrieve stored trace ID for evaluation: {e}")

                # If no stored trace ID, fall back to searching for traces
                if not stored_trace_id:
                    logger.info("[MLflowService] No stored trace ID found for evaluation, searching by execution_id")
                    try:
                        search_traces = getattr(mlflow, "search_traces", None)
                        if callable(search_traces):
                            traces_exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")
                            traces_exp = mlflow.get_experiment_by_name(traces_exp_name)
                            traces_exp_id = str(getattr(traces_exp, "experiment_id", "")) if traces_exp else ""
                            df = search_traces(experiment_ids=[traces_exp_id]) if traces_exp_id else search_traces()
                            if df is not None and len(df) > 0:
                                df_sel = df
                                if "attributes" in df.columns:
                                    try:
                                        mask = df["attributes"].apply(
                                            lambda attrs: isinstance(attrs, dict)
                                            and (attrs.get("execution_id") == getattr(exec_obj, "id", None)
                                                 or attrs.get("execution_id") == job_id)
                                        )
                                        df_sel = df.loc[mask]
                                        # Extract trace IDs from the filtered results
                                        if len(df_sel) > 0 and "trace_id" in df_sel.columns:
                                            related_trace_ids = [str(x) for x in df_sel["trace_id"].tolist() if x]
                                            logger.info(f"[MLflowService] Found {len(related_trace_ids)} trace IDs from search")
                                    except Exception:
                                        pass
                                try:
                                    import json as _json
                                    max_rows = int(os.getenv("MLFLOW_EVAL_MAX_ROWS", "200"))
                                    for _, r in df_sel.head(max_rows).iterrows():
                                        attrs = r.get("attributes", {}) if isinstance(r.get("attributes", {}), dict) else {}
                                        def _pick(keys):
                                            for kk in keys:
                                                v = attrs.get(kk) if isinstance(attrs, dict) else None
                                                if (v is None) and (kk in r):
                                                    v = r[kk]
                                                if isinstance(v, (dict, list)):
                                                    try:
                                                        v = _json.dumps(v, ensure_ascii=False)
                                                    except Exception:
                                                        v = str(v)
                                                if isinstance(v, str) and v.strip():
                                                    return v.strip()
                                            return None
                                        msg = _pick(["prompt", "question", "query", "input", "messages", "user_input", "task", "request"])
                                        pred = _pick(["output", "response", "content", "answer", "final_answer", "text"])
                                        ctx = _pick(["contexts", "context", "retrieved_contexts", "docs", "documents"])
                                        ref = _pick(["reference", "ground_truth", "expected_answer", "label"])
                                        if msg is None and pred is None:
                                            continue
                                        rec = {"messages": msg or "", "predictions": pred or ""}
                                        if isinstance(ctx, str) and ctx.strip():
                                            rec["contexts"] = ctx
                                        if isinstance(ref, str) and ref.strip():
                                            rec["references"] = ref
                                        records.append(rec)
                                except Exception:
                                    pass
                    except Exception:
                        pass

                if not records:
                    records = [{"messages": inputs_text or "", "predictions": prediction_text or ""}]
                eval_df = pd.DataFrame.from_records(records)

                # Detect whether the constructed records include contexts/references
                # We will prefer record-based evaluation when enriched data is present to unlock more scorers
                has_ctx_col = False
                has_ref_col = False
                try:
                    if "contexts" in eval_df.columns:
                        try:
                            has_ctx_col = eval_df["contexts"].astype(str).str.strip().astype(bool).any()
                        except Exception:
                            has_ctx_col = any(isinstance(v, str) and v.strip() for v in eval_df["contexts"].tolist())
                    if "references" in eval_df.columns:
                        try:
                            has_ref_col = eval_df["references"].astype(str).str.strip().astype(bool).any()
                        except Exception:
                            has_ref_col = any(isinstance(v, str) and v.strip() for v in eval_df["references"].tolist())
                except Exception:
                    pass


                # Build scorers/metrics and run evaluate within the existing run
                try:
                    import mlflow as _ml
                    metrics_ns = getattr(_ml, "metrics", None)
                    m_genai_metrics = getattr(metrics_ns, "genai", None) if metrics_ns else None

                    # Prepare evaluation data for mlflow.genai.evaluate
                    # If we have trace IDs, fetch the actual trace objects for better scorer performance
                    eval_data = []
                    try:
                        # Try to fetch trace objects if we have trace IDs
                        trace_objects = []
                        if related_trace_ids:
                            logger.info(f"[MLflowService] Fetching trace objects for evaluation: {related_trace_ids}")
                            for trace_id in related_trace_ids:
                                try:
                                    trace_obj = mlflow.get_trace(trace_id)
                                    if trace_obj:
                                        trace_objects.append(trace_obj)
                                        logger.debug(f"[MLflowService] Successfully fetched trace: {trace_id}")
                                except Exception as e:
                                    logger.warning(f"[MLflowService] Failed to fetch trace {trace_id}: {e}")

                            # Attempt to extract contexts/query directly from trace request/response JSON
                            # to enable retrieval metrics even when search_traces attributes lack them
                            try:
                                import json as _json
                                enriched_records = []
                                for t in trace_objects:
                                    data = getattr(t, "data", None)
                                    req = getattr(data, "request", None) if data is not None else None
                                    resp = getattr(data, "response", None) if data is not None else None
                                    if isinstance(req, str) and req.strip():
                                        try:
                                            req_obj = _json.loads(req)
                                        except Exception:
                                            req_obj = {}
                                    else:
                                        req_obj = {}
                                    if isinstance(resp, str) and resp.strip():
                                        try:
                                            resp_obj = _json.loads(resp)
                                        except Exception:
                                            resp_obj = {}
                                    else:
                                        resp_obj = {}

                                    # Heuristics to extract query and contexts from request JSON
                                    def _find_text(d: dict, keys):
                                        for k in keys:
                                            v = d.get(k)
                                            if isinstance(v, str) and v.strip():
                                                return v
                                        return None

                                    def _find_list_or_str(d: dict, keys):
                                        for k in keys:
                                            v = d.get(k)
                                            if isinstance(v, list) and v:
                                                return [str(x) for x in v]
                                            if isinstance(v, str) and v.strip():
                                                return [v]
                                        return None

                                    inputs_obj = req_obj.get("inputs", {}) if isinstance(req_obj.get("inputs", {}), dict) else {}
                                    params_obj = req_obj.get("parameters", {}) if isinstance(req_obj.get("parameters", {}), dict) else {}
                                    top_ctx = _find_list_or_str(req_obj, ["contexts", "context", "retrieved_contexts", "docs", "documents"]) or []
                                    in_ctx = _find_list_or_str(inputs_obj, ["contexts", "context", "retrieved_contexts", "docs", "documents"]) or []
                                    p_ctx = _find_list_or_str(params_obj, ["contexts", "context", "retrieved_contexts", "docs", "documents"]) or []
                                    all_ctx = [c for c in (top_ctx + in_ctx + p_ctx) if isinstance(c, str) and c.strip()]

                                    # query
                                    query = (
                                        _find_text(inputs_obj, ["query", "question", "prompt", "messages", "input", "user_input", "task"]) or
                                        _find_text(req_obj, ["query", "question", "prompt", "messages", "input", "user_input", "task"]) or
                                        _find_text(params_obj, ["query", "question", "prompt"]) or
                                        ""
                                    )
                                    # response
                                    response = (
                                        _find_text(resp_obj, ["response", "output", "answer", "final_answer", "content", "text"]) or ""
                                    )

                                    if query or response or all_ctx:
                                        rec = {"inputs": {"query": query}, "outputs": {"response": response}}
                                        if all_ctx:
                                            rec["contexts"] = all_ctx
                                        enriched_records.append(rec)

                                if enriched_records:
                                    eval_data = enriched_records
                                    has_ctx_col = True
                                    logger.info(f"[MLflowService] Built {len(enriched_records)} enriched records from trace JSON (contexts present)")
                            except Exception:
                                pass
                        # If we have trace objects, validate they contain request/response JSON strings; otherwise fall back
                        # Skip if we already constructed enriched records from trace JSON
                        if trace_objects and not eval_data:
                            valid_traces = []
                            try:
                                for t in trace_objects:
                                    # Some traces (schema v3) may have missing request/response; MLflow expects JSON strings
                                    data = getattr(t, "data", None)
                                    req = getattr(data, "request", None) if data is not None else None
                                    if isinstance(req, str) and req.strip():
                                        valid_traces.append(t)
                                    else:
                                        logger.debug("[MLflowService] Skipping trace without request JSON; falling back to record for this row")
                            except Exception:
                                pass
                            # Prefer record-based evaluation when enriched with contexts/references; otherwise use traces
                            prefer_records = bool(has_ctx_col or has_ref_col)
                            if valid_traces and not prefer_records:
                                logger.info(f"[MLflowService] Using trace-based evaluation with {len(valid_traces)} traces (filtered from {len(trace_objects)})")
                                for trace_obj in valid_traces:
                                    eval_data.append({"trace": trace_obj})
                            else:
                                logger.info("[MLflowService] Using inputs/outputs/expectations format for evaluation (enriched records preferred)")
                                for _, r in eval_df.iterrows():
                                    inp = {}
                                    # Map our 'messages' string to 'query' for MLflow scorers (per docs)
                                    msg_val = r.get("messages", "")
                                    if msg_val:
                                        inp["query"] = msg_val
                                    # Include contexts if available (as a list at top-level)
                                    ctx_val = r.get("contexts") if "contexts" in eval_df.columns else None
                                    out = {"response": r.get("predictions", "")}
                                    rec = {"inputs": inp, "outputs": out}
                                    if isinstance(ctx_val, str) and ctx_val.strip():
                                        rec["contexts"] = [ctx_val]
                                    # Map references to expectations (support both correctness and sufficiency)
                                    ref_val = r.get("references") if "references" in eval_df.columns else None
                                    if isinstance(ref_val, str) and ref_val.strip():
                                        rec["expectations"] = {
                                            "expected_response": ref_val,
                                            # Provide a facts list as a best-effort mapping for sufficiency scorers
                                            "expected_facts": [ref_val],
                                        }
                                    eval_data.append(rec)
                        if not eval_data:
                            # Absolute fallback: minimal shaping
                            eval_data = [{"inputs": {"query": inputs_text or ""}, "outputs": {"response": prediction_text or ""}}]
                    except Exception as e:
                        logger.warning(f"[MLflowService] Error preparing evaluation data: {e}")
                        # Fall back to minimal shaping
                        eval_data = [{"inputs": {"query": inputs_text or ""}, "outputs": {"response": prediction_text or ""}}]

                    # Build GenAI scorers (preferred)
                    scorers = []
                    try:
                        genai_ns = getattr(mlflow, "genai", None)
                        m_scorers = getattr(genai_ns, "scorers", None) if genai_ns else None

                        # Log available scorers for debugging
                        if m_scorers:
                            try:
                                available_scorers = [name for name in dir(m_scorers) if not name.startswith('_') and name[0].isupper()]
                                logger.info(f"[MLflowService] Available MLflow scorers: {available_scorers}")
                            except Exception as e:
                                logger.debug(f"[MLflowService] Could not list available scorers: {e}")

                        def _to_scorer_model_uri(route: Optional[str]) -> Optional[str]:
                            if not route:
                                return None
                            try:
                                # Accept already formatted URIs
                                if ":/" in route:
                                    return route
                                # Convert provider/model -> provider:/model
                                if "/" in route:
                                    provider, model = route.split("/", 1)
                                    return f"{provider}:/" + model
                                # Allow bare 'databricks' to use managed judges
                                if route == "databricks":
                                    return route
                            except Exception:
                                pass
                            return route

                        def _add_scorer(name: str):
                            if m_scorers is None:
                                return
                            cls = getattr(m_scorers, name, None)
                            if cls is None:
                                logger.warning(f"[MLflowService] Scorer '{name}' not found in mlflow.genai.scorers - skipping")
                                return
                            try:
                                # Use configured judge model route when available; map to scorer URI format
                                model_uri = _to_scorer_model_uri(judge_model_route)
                                kw = {"model": model_uri} if model_uri else {}
                                scorers.append(cls(**kw))
                                logger.info(f"[MLflowService] Successfully added scorer: {name}")
                            except Exception as _e:
                                logger.warning(f"[MLflowService] Failed to init scorer {name} with model={judge_model_route}: {_e}")
                                pass
                        # Core evaluation scorers - these are the main ones that exist in MLflow
                        _add_scorer("RelevanceToQuery")
                        _add_scorer("Safety")

                        # Correctness scorer (requires reference answers)
                        if has_ref_col:
                            _add_scorer("Correctness")

                        # Retrieval-specific scorers (require contexts)
                        if has_ctx_col:
                            _add_scorer("Groundedness")  # Note: It's "Groundedness" not "RetrievalGroundedness"
                            _add_scorer("Relevance")     # Note: It's "Relevance" not "RetrievalRelevance"
                            # Add context sufficiency if references are also available
                            if has_ref_col:
                                _add_scorer("ContextSufficiency")  # Note: It's "ContextSufficiency" not "RetrievalSufficiency"
                        try:
                            scorer_names = [type(s).__name__ for s in scorers]
                            logger.info(f"[MLflowService] Using genai scorers: {scorer_names}, judge_model='{judge_model_route}'")
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # Legacy metrics for mlflow.evaluate fallback (optional)
                    extra_metrics = []
                    if judge_model_route and m_genai_metrics:
                        def _maybe_add_metric(name: str, **kwargs):
                            fn = getattr(m_genai_metrics, name, None)
                            if callable(fn):
                                try:
                                    extra_metrics.append(fn(model=judge_model_route, **kwargs))
                                except Exception:
                                    pass
                        _maybe_add_metric("answer_relevance")
                        if "references" in eval_df.columns:
                            _maybe_add_metric("answer_similarity", reference_column="references")
                        if "contexts" in eval_df.columns:
                            _maybe_add_metric("faithfulness", context_column="contexts")
                        _maybe_add_metric("toxicity")
                    if metrics_ns and "references" in eval_df.columns:
                        # Add classical text metrics when references are available
                        def _maybe_add_text(name: str, **kwargs):
                            fn2 = getattr(metrics_ns, name, None)
                            if callable(fn2):
                                try:
                                    extra_metrics.append(fn2(reference_column="references", **kwargs))
                                except Exception:
                                    pass
                        for n in ("rouge1", "rouge2", "rougeL", "rougeLsum"):
                            _maybe_add_text(n)

                    # Ensure we attach to the same run/environment safely
                    try:
                        from mlflow.tracking import MlflowClient
                        client = MlflowClient()
                        _run = client.get_run(run_id)
                        _exp = client.get_experiment(_run.info.experiment_id)
                        if _exp and getattr(_exp, "name", None):
                            try:
                                mlflow.set_experiment(_exp.name)
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        ar = mlflow.active_run()
                        if ar and getattr(ar, "info", object()).run_id != run_id:
                            mlflow.end_run()
                    except Exception:
                        pass
                    try:
                        import os as _os
                        _os.environ["MLFLOW_RUN_ID"] = run_id
                    except Exception:
                        pass

                    with mlflow.start_run(run_id=run_id):
                        # Use MLflow 3.x GenAI evaluation API (recommended for LLM/GenAI applications)
                        try:
                            import mlflow.genai
                            logger.info(f"[MLflowService] Running mlflow.genai.evaluate for job_id={job_id}, run_id={run_id}")

                            # For MLflow 3.0, trace information is included in the data parameter
                            # The eval_data now contains either trace objects or inputs/outputs/expectations
                            eval_kwargs = {
                                "data": eval_data,
                                "scorers": scorers if scorers else [],
                            }

                            # Pre-evaluation debug: shape and scorers
                            try:
                                first_keys = list(eval_data[0].keys()) if eval_data else []
                                scorer_names = [type(s).__name__ for s in (scorers or [])]
                                from_types = ("trace" if (eval_data and "trace" in eval_data[0]) else "records")
                                logger.info(f"[MLflowService] Eval mode={from_types}, rows={len(eval_data)}, first_keys={first_keys}")
                                logger.info(f"[MLflowService] Scorers={scorer_names}, judge_model_route='{judge_model_route}'")
                                print(f"EVAL_DEBUG mode={from_types} rows={len(eval_data)} first_keys={first_keys}")
                                print(f"EVAL_DEBUG scorers={scorer_names} judge_model_route={judge_model_route}")
                            except Exception:
                                pass

                            # Log the evaluation approach being used
                            if eval_data and "trace" in eval_data[0]:
                                logger.info(f"[MLflowService] Using trace-based evaluation with {len(eval_data)} trace objects")
                            else:
                                logger.info(f"[MLflowService] Using inputs/outputs/expectations evaluation with {len(eval_data)} records")

                            import traceback
                            try:
                                eval_result = mlflow.genai.evaluate(**eval_kwargs)
                            except Exception as e:
                                logger.exception(f"[MLflowService] mlflow.genai.evaluate raised: {e}")
                                print("EVAL_ERROR exception during mlflow.genai.evaluate:", str(e))
                                print("EVAL_ERROR traceback:\n" + traceback.format_exc())
                                raise
                            logger.info("[MLflowService] mlflow.genai.evaluate completed successfully")
                            print("EVAL_DEBUG evaluate completed successfully")

                        except Exception as e:
                            logger.error(f"mlflow.genai.evaluate failed for job_id={job_id}: {e}")
                            raise

                        # Persist aggregated judge metrics as run-level metrics for quick visibility
                        try:
                            # Log which scorers were used and judge model
                            try:
                                # Also record the scorer model URI mapping for transparency
                                def _to_scorer_model_uri(route):
                                    try:
                                        if not route:
                                            return None
                                        if ":/" in route:
                                            return route
                                        if "/" in route:
                                            p, m = route.split("/", 1)
                                            return f"{p}:/" + m
                                        return route
                                    except Exception:
                                        return route
                                mlflow.log_params({
                                    "genai_scorers": ",".join([type(s).__name__ for s in (scorers or [])]) or "",
                                    "genai_judge_model": str(judge_model_route or ""),
                                    "genai_judge_model_uri": str(_to_scorer_model_uri(judge_model_route) or ""),
                                })
                            except Exception:
                                pass

                            # Log result table if available and compute means for numeric columns
                            if eval_result is not None:
                                tbl = None
                                if hasattr(eval_result, "tables"):
                                    tbl = eval_result.tables.get("eval_results_table")
                                if tbl is not None:
                                    import io
                                    import pandas as _pd
                                    csv_buf = io.StringIO()
                                    tbl.to_csv(csv_buf, index=False)
                                    mlflow.log_text(csv_buf.getvalue(), artifact_file="eval_results.csv")

                                    # Compute means for judge metric columns and log as metrics
                                    try:
                                        df = tbl if isinstance(tbl, _pd.DataFrame) else None
                                        if df is None:
                                            # Some mlflow versions return a dict-like; best-effort conversion
                                            df = _pd.DataFrame(tbl)
                                        numeric_cols = [c for c in df.columns if _pd.api.types.is_numeric_dtype(df[c])]
                                        agg_logged = {}
                                        for col in numeric_cols:
                                            try:
                                                mean_val = float(df[col].mean())
                                                # Normalize metric name (replace slashes/spaces)
                                                metric_name = str(col).replace("/", "_").replace(" ", "_").lower()
                                                mlflow.log_metric(f"{metric_name}_mean", mean_val)
                                                agg_logged[f"{metric_name}_mean"] = mean_val
                                            except Exception:
                                                continue
                                        try:
                                            if agg_logged:
                                                logger.info(f"[MLflowService] Judge metrics (means): {agg_logged}")
                                        except Exception:
                                            pass
                                    except Exception as _agg_e:
                                        logger.warning(f"[MLflowService] Failed to aggregate judge metrics: {_agg_e}")
                        except Exception:
                            pass
                except Exception as e:
                    logger.warning(f"Failed to complete evaluation for job_id={job_id}: {e}")
            finally:
                # Restore original environment variables
                if auth_ctx_for_eval:
                    if old_host_eval is not None:
                        os.environ["DATABRICKS_HOST"] = old_host_eval
                    elif "DATABRICKS_HOST" in os.environ:
                        del os.environ["DATABRICKS_HOST"]

                    if old_token_eval is not None:
                        os.environ["DATABRICKS_TOKEN"] = old_token_eval
                    elif "DATABRICKS_TOKEN" in os.environ:
                        del os.environ["DATABRICKS_TOKEN"]

                    if old_base_url_eval is not None:
                        os.environ["DATABRICKS_BASE_URL"] = old_base_url_eval
                    elif "DATABRICKS_BASE_URL" in os.environ:
                        del os.environ["DATABRICKS_BASE_URL"]

                    if old_api_base_eval is not None:
                        os.environ["DATABRICKS_API_BASE"] = old_api_base_eval
                    elif "DATABRICKS_API_BASE" in os.environ:
                        del os.environ["DATABRICKS_API_BASE"]

                    if old_endpoint_eval is not None:
                        os.environ["DATABRICKS_ENDPOINT"] = old_endpoint_eval
                    elif "DATABRICKS_ENDPOINT" in os.environ:
                        del os.environ["DATABRICKS_ENDPOINT"]


        info = await asyncio.to_thread(_create_run_sync, auth_context)
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
                    asyncio.create_task(asyncio.to_thread(_complete_eval_sync, run_id_bg, auth_context))
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

