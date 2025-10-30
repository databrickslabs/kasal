"""
MLflow Evaluation Runner

Handles the creation and execution of MLflow evaluation runs for agent workflows.
Extracted from MLflowService.trigger_evaluation() to reduce file complexity.

This module contains:
- MLflowEvaluationRunner: Main class for managing evaluation runs
- Helper functions for dataset building, trace discovery, and scorer configuration
"""

import logging
import os
import asyncio
from typing import Dict, Any, List, Optional
from json import dumps

from src.core.logger import LoggerManager
from src.services.mlflow_scope_error_handler import MLflowScopeErrorHandler

logger = LoggerManager.get_instance().system


class MLflowEvaluationRunner:
    """
    Manages MLflow evaluation run creation and execution.

    Responsibilities:
    - Create MLflow evaluation runs
    - Build evaluation datasets from execution history and traces
    - Configure and run GenAI scorers
    - Handle scope errors with PAT/SPN fallback
    """

    def __init__(
        self,
        exec_obj: Any,
        job_id: str,
        inputs_text: str,
        prediction_text: Optional[str],
        judge_model_route: str,
        judge_model_defaulted: bool,
    ):
        """
        Initialize evaluation runner.

        Args:
            exec_obj: Execution history object
            job_id: Job ID for this evaluation
            inputs_text: Input text extracted from execution
            prediction_text: Prediction text extracted from execution
            judge_model_route: Judge model route (e.g., "databricks/databricks-claude-sonnet-4")
            judge_model_defaulted: Whether judge model is using default value
        """
        self.exec_obj = exec_obj
        self.job_id = job_id
        self.inputs_text = inputs_text
        self.prediction_text = prediction_text
        self.judge_model_route = judge_model_route
        self.judge_model_defaulted = judge_model_defaulted

    def create_run(self, auth_ctx: Optional[Any]) -> Dict[str, Any]:
        """
        Create MLflow evaluation run (sync, runs in thread).

        Args:
            auth_ctx: Authentication context for MLflow operations

        Returns:
            Dict with experiment_id, experiment_name, run_id
        """
        import mlflow
        import pandas as pd

        # Set up environment variables within thread context only
        old_env = self._save_environment_vars()
        scope_handler = MLflowScopeErrorHandler(auth_ctx)

        try:
            if auth_ctx:
                self._set_environment_vars(auth_ctx)

            # Ensure Databricks tracking and target experiments
            mlflow.set_tracking_uri("databricks")
            eval_exp_name = os.getenv(
                "MLFLOW_CREW_TRACES_EXPERIMENT",
                "/Shared/kasal-crew-execution-traces"
            )

            # Set experiment with scope error handling
            try:
                eval_exp = mlflow.set_experiment(eval_exp_name)
            except Exception as mlflow_e:
                eval_exp = scope_handler.handle_and_retry(
                    mlflow_e,
                    lambda: mlflow.set_experiment(eval_exp_name),
                    "set_experiment"
                )

            # Discover related traces and build evaluation dataset
            related_trace_ids, records = self._discover_traces_and_build_dataset(
                auth_ctx,
                scope_handler
            )

            # Fallback to single-record dataset if trace-derived dataset is empty
            if not records:
                records = [{
                    "messages": self.inputs_text or "",
                    "predictions": self.prediction_text or ""
                }]

            eval_df = pd.DataFrame.from_records(records)
            logger.info(
                f"[MLflowEvaluationRunner] Built evaluation dataset for job_id={self.job_id}: "
                f"rows={len(eval_df)}, columns={list(eval_df.columns)}"
            )

            # Create evaluation run
            mlflow.set_experiment(eval_exp_name)
            with mlflow.start_run(run_name=f"evaluation-{self.job_id}") as run:
                run_id = run.info.run_id
                logger.info(
                    f"[MLflowEvaluationRunner] Started evaluation run: "
                    f"experiment='{eval_exp_name}', run_id={run_id}, job_id={self.job_id}"
                )

                # Log parameters and linkage
                self._log_run_parameters(related_trace_ids)

                # Log baseline metrics
                self._log_baseline_metrics(eval_df)

                # Log raw inputs/predictions as artifacts
                self._log_artifacts()

                # Register dataset as MLflow input
                try:
                    ds = mlflow.data.from_pandas(
                        df=eval_df,
                        name="agent_eval_dataset",
                        predictions="predictions",
                    )
                    mlflow.log_input(dataset=ds)
                except Exception:
                    pass

                return {
                    "experiment_id": str(
                        getattr(run, "info", object()).experiment_id
                        if hasattr(getattr(run, "info", object()), "experiment_id")
                        else ""
                    ),
                    "experiment_name": eval_exp_name,
                    "run_id": run_id,
                }

        finally:
            self._restore_environment_vars(old_env, auth_ctx)

    def _discover_traces_and_build_dataset(
        self,
        auth_ctx: Optional[Any],
        scope_handler: MLflowScopeErrorHandler
    ) -> tuple[List[str], List[Dict[str, Any]]]:
        """
        Discover related MLflow traces and build evaluation dataset from them.

        Args:
            auth_ctx: Authentication context
            scope_handler: Scope error handler for fallback

        Returns:
            Tuple of (related_trace_ids, records)
        """
        import mlflow

        related_trace_ids = []
        records = []

        # First, try to get trace ID from execution history
        stored_trace_id = None
        try:
            stored_trace_id = getattr(self.exec_obj, 'mlflow_trace_id', None)
            if stored_trace_id:
                logger.info(f"[MLflowEvaluationRunner] Found stored trace ID: {stored_trace_id}")
                related_trace_ids = [stored_trace_id]
        except Exception as e:
            logger.warning(f"[MLflowEvaluationRunner] Could not retrieve stored trace ID: {e}")

        # If no stored trace ID, search for traces
        if not stored_trace_id:
            logger.info("[MLflowEvaluationRunner] No stored trace ID, searching by execution_id")
            try:
                search_traces = getattr(mlflow, "search_traces", None)
                if callable(search_traces):
                    traces_exp_name = os.getenv(
                        "MLFLOW_CREW_TRACES_EXPERIMENT",
                        "/Shared/kasal-crew-execution-traces"
                    )

                    # Get experiment with scope error handling
                    try:
                        traces_exp = mlflow.get_experiment_by_name(traces_exp_name)
                    except Exception as exp_error:
                        traces_exp = scope_handler.handle_and_retry(
                            exp_error,
                            lambda: mlflow.get_experiment_by_name(traces_exp_name),
                            "get_experiment_by_name"
                        )

                    traces_exp_id = str(getattr(traces_exp, "experiment_id", "")) if traces_exp else ""
                    df = search_traces(experiment_ids=[traces_exp_id]) if traces_exp_id else search_traces()

                    # Filter and build records from traces
                    if df is not None and len(df) > 0:
                        related_trace_ids, records = self._extract_records_from_traces(df)

            except Exception as e:
                logger.warning(f"[MLflowEvaluationRunner] Failed to search traces: {e}")

        return related_trace_ids, records

    def _extract_records_from_traces(self, df) -> tuple[List[str], List[Dict[str, Any]]]:
        """Extract evaluation records from trace dataframe."""
        import json as _json

        related_trace_ids = []
        records = []

        df_sel = df
        if "attributes" in df.columns:
            try:
                mask = df["attributes"].apply(
                    lambda attrs: isinstance(attrs, dict)
                    and (attrs.get("execution_id") == getattr(self.exec_obj, "id", None)
                         or attrs.get("execution_id") == self.job_id)
                )
                df_sel = df.loc[mask]
                related_trace_ids = [str(x) for x in df_sel.get("trace_id", []).tolist()]
            except Exception:
                pass

        # Build records from trace rows
        try:
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
        except Exception as e:
            logger.warning(f"[MLflowEvaluationRunner] Failed to extract records: {e}")

        return related_trace_ids, records

    def _log_run_parameters(self, related_trace_ids: List[str]) -> None:
        """Log parameters for the evaluation run."""
        import mlflow

        try:
            mlflow.log_params({
                "job_id": self.job_id,
                "group_id": getattr(self.exec_obj, "group_id", ""),
                "status": getattr(self.exec_obj, "status", ""),
                "related_trace_ids": ",".join(related_trace_ids) if related_trace_ids else "",
                "judge_model_configured": bool(self.judge_model_route),
                "judge_model_route": str(self.judge_model_route or ""),
                "judge_model_defaulted": bool(self.judge_model_defaulted),
            })
        except Exception as e:
            logger.warning(f"[MLflowEvaluationRunner] Failed to log parameters: {e}")

    def _log_baseline_metrics(self, eval_df) -> None:
        """Log simple baseline metrics."""
        import mlflow

        try:
            preds = eval_df["predictions"].tolist() if "predictions" in eval_df.columns else []
            msgs = eval_df["messages"].tolist() if "messages" in eval_df.columns else []

            # Prediction length metrics
            pred_lengths = [len(str(x)) for x in preds]
            if pred_lengths:
                mlflow.log_metric("prediction_length_mean", float(sum(pred_lengths) / len(pred_lengths)))
                mlflow.log_metric("prediction_length_max", float(max(pred_lengths)))

            # Word count metrics
            def _wc(s):
                try:
                    return float(len(str(s).split()))
                except Exception:
                    return 0.0

            pred_wc = [_wc(p) for p in preds]
            msg_wc = [_wc(m) for m in msgs]

            if pred_wc:
                mlflow.log_metric("prediction_word_count_mean", float(sum(pred_wc) / len(pred_wc)))
            if msg_wc:
                mlflow.log_metric("input_word_count_mean", float(sum(msg_wc) / len(msg_wc)))

            # Jaccard overlap between input and prediction words
            try:
                overlaps = []
                for m, p in zip(msgs, preds):
                    ms = set(str(m).lower().split())
                    ps = set(str(p).lower().split())
                    inter = len(ms & ps)
                    union = max(len(ms | ps), 1)
                    overlaps.append(float(inter) / float(union))
                if overlaps:
                    mlflow.log_metric("overlap_jaccard_mean", float(sum(overlaps) / len(overlaps)))
            except Exception:
                pass

        except Exception as e:
            logger.warning(f"[MLflowEvaluationRunner] Failed to log baseline metrics: {e}")

    def _log_artifacts(self) -> None:
        """Log raw inputs and predictions as artifacts."""
        import mlflow

        try:
            mlflow.log_text(self.inputs_text or "", artifact_file="inputs.txt")
        except Exception:
            pass

        try:
            mlflow.log_text(self.prediction_text or "", artifact_file="prediction.txt")
        except Exception:
            pass

    def _save_environment_vars(self) -> Dict[str, Optional[str]]:
        """Save current environment variables."""
        return {
            "DATABRICKS_HOST": os.environ.get("DATABRICKS_HOST"),
            "DATABRICKS_TOKEN": os.environ.get("DATABRICKS_TOKEN"),
            "DATABRICKS_BASE_URL": os.environ.get("DATABRICKS_BASE_URL"),
            "DATABRICKS_API_BASE": os.environ.get("DATABRICKS_API_BASE"),
            "DATABRICKS_ENDPOINT": os.environ.get("DATABRICKS_ENDPOINT"),
        }

    def _set_environment_vars(self, auth_ctx: Any) -> None:
        """Set environment variables from auth context."""
        from src.utils.databricks_url_utils import DatabricksURLUtils

        os.environ["DATABRICKS_HOST"] = auth_ctx.workspace_url
        os.environ["DATABRICKS_TOKEN"] = auth_ctx.token

        api_base = DatabricksURLUtils.construct_serving_endpoints_url(auth_ctx.workspace_url) or ""
        if api_base:
            os.environ["DATABRICKS_BASE_URL"] = api_base
            os.environ["DATABRICKS_API_BASE"] = api_base
            os.environ["DATABRICKS_ENDPOINT"] = api_base

    def _restore_environment_vars(self, old_env: Dict[str, Optional[str]], auth_ctx: Optional[Any]) -> None:
        """Restore original environment variables."""
        if not auth_ctx:
            return

        for key, value in old_env.items():
            if value is not None:
                os.environ[key] = value
            elif key in os.environ:
                del os.environ[key]

    def complete_evaluation(self, run_id: str, auth_ctx: Optional[Any]) -> None:
        """
        Complete MLflow evaluation with GenAI scorers (sync, runs in thread).

        Args:
            run_id: MLflow run ID from create_run()
            auth_ctx: Authentication context for MLflow operations

        This method:
        - Rebuilds evaluation dataset from traces
        - Fetches trace objects for enriched evaluation
        - Configures GenAI scorers based on available data
        - Runs mlflow.genai.evaluate
        - Logs aggregated metrics
        """
        import mlflow
        import pandas as pd

        # Set up environment variables within thread context only
        old_env = self._save_environment_vars()
        scope_handler = MLflowScopeErrorHandler(auth_ctx)

        try:
            if auth_ctx:
                self._set_environment_vars(auth_ctx)

            # Rebuild dataset similarly to creation step
            mlflow.set_tracking_uri("databricks")

            # Discover traces and build dataset (reuse existing logic)
            related_trace_ids, records = self._discover_traces_and_build_dataset(
                auth_ctx,
                scope_handler
            )

            # Fallback to single-record dataset if trace-derived dataset is empty
            if not records:
                records = [{
                    "messages": self.inputs_text or "",
                    "predictions": self.prediction_text or ""
                }]

            eval_df = pd.DataFrame.from_records(records)

            # Detect whether the constructed records include contexts/references
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
                eval_data = []
                try:
                    # Try to fetch trace objects if we have trace IDs
                    trace_objects = []
                    if related_trace_ids:
                        logger.info(f"[MLflowEvaluationRunner] Fetching trace objects for evaluation: {related_trace_ids}")
                        for trace_id in related_trace_ids:
                            try:
                                trace_obj = mlflow.get_trace(trace_id)
                                if trace_obj:
                                    trace_objects.append(trace_obj)
                                    logger.debug(f"[MLflowEvaluationRunner] Successfully fetched trace: {trace_id}")
                            except Exception as e:
                                logger.warning(f"[MLflowEvaluationRunner] Failed to fetch trace {trace_id}: {e}")

                        # Attempt to extract contexts/query directly from trace request/response JSON
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
                                logger.info(f"[MLflowEvaluationRunner] Built {len(enriched_records)} enriched records from trace JSON")
                        except Exception:
                            pass

                    # If we have trace objects, validate they contain request/response JSON strings
                    if trace_objects and not eval_data:
                        valid_traces = []
                        try:
                            for t in trace_objects:
                                data = getattr(t, "data", None)
                                req = getattr(data, "request", None) if data is not None else None
                                if isinstance(req, str) and req.strip():
                                    valid_traces.append(t)
                                else:
                                    logger.debug("[MLflowEvaluationRunner] Skipping trace without request JSON")
                        except Exception:
                            pass

                        # Prefer record-based evaluation when enriched with contexts/references
                        prefer_records = bool(has_ctx_col or has_ref_col)
                        if valid_traces and not prefer_records:
                            logger.info(f"[MLflowEvaluationRunner] Using trace-based evaluation with {len(valid_traces)} traces")
                            for trace_obj in valid_traces:
                                eval_data.append({"trace": trace_obj})
                        else:
                            logger.info("[MLflowEvaluationRunner] Using inputs/outputs/expectations format")
                            for _, r in eval_df.iterrows():
                                inp = {}
                                msg_val = r.get("messages", "")
                                if msg_val:
                                    inp["query"] = msg_val
                                ctx_val = r.get("contexts") if "contexts" in eval_df.columns else None
                                out = {"response": r.get("predictions", "")}
                                rec = {"inputs": inp, "outputs": out}
                                if isinstance(ctx_val, str) and ctx_val.strip():
                                    rec["contexts"] = [ctx_val]
                                ref_val = r.get("references") if "references" in eval_df.columns else None
                                if isinstance(ref_val, str) and ref_val.strip():
                                    rec["expectations"] = {
                                        "expected_response": ref_val,
                                        "expected_facts": [ref_val],
                                    }
                                eval_data.append(rec)

                    if not eval_data:
                        # Absolute fallback
                        eval_data = [{
                            "inputs": {"query": self.inputs_text or ""},
                            "outputs": {"response": self.prediction_text or ""}
                        }]
                except Exception as e:
                    logger.warning(f"[MLflowEvaluationRunner] Error preparing evaluation data: {e}")
                    eval_data = [{
                        "inputs": {"query": self.inputs_text or ""},
                        "outputs": {"response": self.prediction_text or ""}
                    }]

                # Build GenAI scorers
                scorers = []
                try:
                    genai_ns = getattr(mlflow, "genai", None)
                    m_scorers = getattr(genai_ns, "scorers", None) if genai_ns else None

                    def _to_scorer_model_uri(route: Optional[str]) -> Optional[str]:
                        if not route:
                            return None
                        try:
                            if ":/" in route:
                                return route
                            if "/" in route:
                                provider, model = route.split("/", 1)
                                return f"{provider}:/" + model
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
                            logger.warning(f"[MLflowEvaluationRunner] Scorer '{name}' not found - skipping")
                            return
                        try:
                            model_uri = _to_scorer_model_uri(self.judge_model_route)
                            kw = {"model": model_uri} if model_uri else {}
                            scorers.append(cls(**kw))
                            logger.info(f"[MLflowEvaluationRunner] Added scorer: {name}")
                        except Exception as _e:
                            logger.warning(f"[MLflowEvaluationRunner] Failed to init scorer {name}: {_e}")

                    # Core evaluation scorers
                    _add_scorer("RelevanceToQuery")
                    _add_scorer("Safety")

                    # Correctness scorer (requires reference answers)
                    if has_ref_col:
                        _add_scorer("Correctness")

                    # Retrieval-specific scorers (require contexts)
                    if has_ctx_col:
                        _add_scorer("Groundedness")
                        _add_scorer("Relevance")
                        if has_ref_col:
                            _add_scorer("ContextSufficiency")

                    try:
                        scorer_names = [type(s).__name__ for s in scorers]
                        logger.info(f"[MLflowEvaluationRunner] Using scorers: {scorer_names}")
                    except Exception:
                        pass
                except Exception:
                    pass

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
                    os.environ["MLFLOW_RUN_ID"] = run_id
                except Exception:
                    pass

                with mlflow.start_run(run_id=run_id):
                    # Use MLflow 3.x GenAI evaluation API
                    try:
                        import mlflow.genai
                        logger.info(f"[MLflowEvaluationRunner] Running mlflow.genai.evaluate for job_id={self.job_id}")

                        eval_kwargs = {
                            "data": eval_data,
                            "scorers": scorers if scorers else [],
                        }

                        # Pre-evaluation debug
                        try:
                            first_keys = list(eval_data[0].keys()) if eval_data else []
                            scorer_names = [type(s).__name__ for s in (scorers or [])]
                            from_types = ("trace" if (eval_data and "trace" in eval_data[0]) else "records")
                            logger.info(f"[MLflowEvaluationRunner] Eval mode={from_types}, rows={len(eval_data)}")
                        except Exception:
                            pass

                        eval_result = mlflow.genai.evaluate(**eval_kwargs)
                        logger.info("[MLflowEvaluationRunner] mlflow.genai.evaluate completed successfully")

                    except Exception as e:
                        logger.error(f"[MLflowEvaluationRunner] mlflow.genai.evaluate failed: {e}")
                        raise

                    # Persist aggregated judge metrics
                    try:
                        # Log scorer info
                        try:
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
                                "genai_judge_model": str(self.judge_model_route or ""),
                                "genai_judge_model_uri": str(_to_scorer_model_uri(self.judge_model_route) or ""),
                            })
                        except Exception:
                            pass

                        # Log result table and compute means
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

                                # Compute means for judge metric columns
                                try:
                                    df = tbl if isinstance(tbl, _pd.DataFrame) else _pd.DataFrame(tbl)
                                    numeric_cols = [c for c in df.columns if _pd.api.types.is_numeric_dtype(df[c])]
                                    agg_logged = {}
                                    for col in numeric_cols:
                                        try:
                                            mean_val = float(df[col].mean())
                                            metric_name = str(col).replace("/", "_").replace(" ", "_").lower()
                                            mlflow.log_metric(f"{metric_name}_mean", mean_val)
                                            agg_logged[f"{metric_name}_mean"] = mean_val
                                        except Exception:
                                            continue
                                    try:
                                        if agg_logged:
                                            logger.info(f"[MLflowEvaluationRunner] Judge metrics (means): {agg_logged}")
                                    except Exception:
                                        pass
                                except Exception as _agg_e:
                                    logger.warning(f"[MLflowEvaluationRunner] Failed to aggregate metrics: {_agg_e}")
                    except Exception:
                        pass

            except Exception as e:
                logger.warning(f"[MLflowEvaluationRunner] Failed to complete evaluation: {e}")

        finally:
            self._restore_environment_vars(old_env, auth_ctx)
