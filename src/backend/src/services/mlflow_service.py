import logging
from typing import Optional, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.mlflow_repository import MLflowRepository
from src.repositories.execution_history_repository import ExecutionHistoryRepository

logger = logging.getLogger(__name__)


class MLflowService:
    """
    Service layer for MLflow enable/disable and status queries, plus evaluation triggers.
    """

    def __init__(self, session: AsyncSession, group_id: Optional[str] = None):
        self.session = session
        self.group_id = group_id
        self.repo = MLflowRepository(session)
        self.exec_repo = ExecutionHistoryRepository(session)

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
        # Resolve judge model preference and normalize URI prefixes
        # Accept: endpoints:/, gateway:/, openai:/, anthropic:/, bedrock:/, mistral:/
        # Normalize legacy databricks:/<endpoint> to endpoints:/<endpoint> (required by mlflow.metrics.genai)
        judge_model_defaulted = False
        judge_model_route = await self.repo.get_evaluation_judge_model(group_id=self.group_id)

        import os as _os
        def _normalize_judge_uri(uri: Optional[str]) -> Optional[str]:
            if not uri:
                return None
            s = str(uri).strip()
            low = s.lower()
            if low.startswith("databricks:/"):
                # map to MLflow Deployments endpoints URI expected by metrics
                return "endpoints:/" + s.split(":/", 1)[1].lstrip("/")
            if low.startswith("endpoints:/") or low.startswith("gateway:/") or low.startswith("openai:/") or low.startswith("anthropic:/") or low.startswith("bedrock:/") or low.startswith("mistral:/"):
                return s
            return s

        if not isinstance(judge_model_route, str) or not judge_model_route.strip():
            env_judge = _os.getenv("MLFLOW_EVAL_JUDGE_MODEL")
            judge_model_route = env_judge if isinstance(env_judge, str) and env_judge.strip() else None

        judge_model_route = _normalize_judge_uri(judge_model_route)
        if not judge_model_route:
            # Default to Databricks Claude Sonnet 4 endpoint name (must exist in workspace)
            judge_model_route = "endpoints:/databricks-claude-sonnet-4"
            judge_model_defaulted = True

        def _create_run_sync() -> Dict[str, Any]:
            import mlflow
            import pandas as pd

            # Ensure Databricks tracking and target experiments
            mlflow.set_tracking_uri("databricks")
            eval_exp_name = os.getenv("MLFLOW_CREW_EVALUATIONS_EXPERIMENT", "/Shared/kasal-crew-evaluations")
            eval_exp = mlflow.set_experiment(eval_exp_name)

            # Discover related traces and build a trace-derived evaluation dataset (best-effort)
            related_trace_ids = []
            records = []
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

            # Ensure we are back on the evaluation experiment before starting the run
            mlflow.set_experiment(eval_exp_name)
            with mlflow.start_run(run_name=f"evaluation-{job_id}") as run:
                run_id = run.info.run_id
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

        def _complete_eval_sync(run_id: str) -> None:
            import mlflow
            import pandas as pd

            # Rebuild dataset similarly to creation step
            mlflow.set_tracking_uri("databricks")

            # Attempt to rebuild eval dataset (same logic)
            records = []
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

            # Build scorers/metrics and run evaluate within the existing run
            try:
                import mlflow as _ml
                metrics_ns = getattr(_ml, "metrics", None)
                m_genai_metrics = getattr(metrics_ns, "genai", None) if metrics_ns else None

                # Prepare answer-sheet style data for mlflow.genai.evaluate (inputs/outputs/expectations)
                eval_data = []
                try:
                    for _, r in eval_df.iterrows():
                        inp = {}
                        # Map our 'messages' string to a 'question' input for best scorer compatibility
                        msg_val = r.get("messages", "")
                        if msg_val:
                            inp["question"] = msg_val
                        # Include context if available
                        ctx_val = r.get("contexts") if "contexts" in eval_df.columns else None
                        if isinstance(ctx_val, str) and ctx_val.strip():
                            inp["context"] = ctx_val
                        out = {"response": r.get("predictions", "")}
                        rec = {"inputs": inp, "outputs": out}
                        # Map references to expectations (support both correctness and sufficiency)
                        ref_val = r.get("references") if "references" in eval_df.columns else None
                        if isinstance(ref_val, str) and ref_val.strip():
                            rec["expectations"] = {
                                "expected_response": ref_val,
                                # Provide a facts list as a best-effort mapping for sufficiency scorers
                                "expected_facts": [ref_val],
                            }
                        eval_data.append(rec)
                except Exception:
                    # Fall back to minimal shaping
                    eval_data = [{"inputs": {"question": inputs_text or ""}, "outputs": {"response": prediction_text or ""}}]

                # Build GenAI scorers (preferred)
                scorers = []
                try:
                    genai_ns = getattr(mlflow, "genai", None)
                    m_scorers = getattr(genai_ns, "scorers", None) if genai_ns else None
                    def _add_scorer(name: str):
                        if m_scorers is None:
                            return
                        cls = getattr(m_scorers, name, None)
                        if cls is not None:
                            try:
                                # Use configured judge model route when available
                                kw = {"model": judge_model_route} if judge_model_route else {}
                                scorers.append(cls(**kw))
                            except Exception:
                                pass
                    _add_scorer("RelevanceToQuery")
                    _add_scorer("Safety")
                    if "references" in eval_df.columns:
                        _add_scorer("Correctness")
                    if "contexts" in eval_df.columns:
                        _add_scorer("RetrievalGroundedness")
                        _add_scorer("RetrievalRelevance")
                        # RetrievalSufficiency typically benefits from ground-truth; add when references exist
                        if "references" in eval_df.columns:
                            _add_scorer("RetrievalSufficiency")
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
                    # Prefer MLflow 3.x GenAI evaluation API (no deprecated warnings)
                    eval_result = None
                    try:
                        genai_ns = getattr(mlflow, "genai", None)
                        eval_genai = getattr(genai_ns, "evaluate", None) if genai_ns else None
                        if callable(eval_genai):
                            try:
                                eval_result = eval_genai(
                                    data=eval_data,
                                    scorers=scorers if scorers else None,
                                )
                            except Exception as e:
                                logger.warning(f"mlflow.genai.evaluate failed for job_id={job_id}: {e}")
                    except Exception:
                        pass

                    # Fallback to mlflow.evaluate for compatibility
                    if eval_result is None:
                        evaluate_fn = getattr(mlflow, "evaluate", None)
                        if callable(evaluate_fn):
                            try:
                                eval_result = evaluate_fn(
                                    data=eval_df,
                                    predictions="predictions",
                                    extra_metrics=extra_metrics if extra_metrics else None,
                                    evaluator_config={"col_mapping": {"inputs": "messages"}},
                                )
                            except Exception as e:
                                logger.warning(f"Background evaluation (deprecated API) failed for job_id={job_id}: {e}")

                    # Log result table if available
                    try:
                        if eval_result is not None:
                            tbl = None
                            if hasattr(eval_result, "tables"):
                                tbl = eval_result.tables.get("eval_results_table")
                            if tbl is not None:
                                import io
                                csv_buf = io.StringIO()
                                tbl.to_csv(csv_buf, index=False)
                                mlflow.log_text(csv_buf.getvalue(), artifact_file="eval_results.csv")
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Failed to complete evaluation for job_id={job_id}: {e}")


        info = await asyncio.to_thread(_create_run_sync)
        try:
            if isinstance(info, dict):
                logger.info(
                    f"MLflow evaluation run created for job_id={job_id}: "
                    f"experiment_id={info.get('experiment_id')}, run_id={info.get('run_id')}"
                )
                run_id_bg = info.get('run_id')
                if run_id_bg:
                    # Fire-and-forget background evaluation metrics logging
                    asyncio.create_task(asyncio.to_thread(_complete_eval_sync, run_id_bg))
        except Exception:
            pass

        # Persist run_id reference on the execution (nest under result.mlflow_evaluation_run_id)
        try:
            current = exec_obj.result or {}
            if isinstance(current, dict):
                current.setdefault("mlflow", {})
                current["mlflow"]["evaluation_run_id"] = info.get("run_id")
                exec_obj.result = current
            else:
                exec_obj.result = {"value": str(current), "mlflow": {"evaluation_run_id": info.get("run_id")}}
            await self.session.flush()
            await self.session.commit()
        except Exception as e:
            logger.warning(f"Failed to persist evaluation_run_id for job_id={job_id}: {e}")

        return info

