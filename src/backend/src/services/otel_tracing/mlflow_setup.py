"""Centralized MLflow subprocess setup for Kasal executors.

This module consolidates all MLflow initialization logic that was previously
inline in process_crew_executor.py (~450 lines) into a single reusable function.
Both crew and flow executors call ``configure_mlflow_in_subprocess()`` to get
consistent MLflow tracing setup including:

- Databricks authentication (PAT / SPN / default)
- Tracking URI and experiment configuration
- Tracing destination and autolog enablement
- Async logging configuration
- LiteLLM ``tracked_completion`` monkey-patch for span instrumentation
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


logger = logging.getLogger(__name__)


@dataclass
class MlflowSetupResult:
    """Result of MLflow subprocess configuration."""

    enabled: bool  # MLflow was enabled in workspace config
    tracing_ready: bool  # Tracing fully initialized
    experiment_name: Optional[str] = None
    experiment_id: Optional[str] = None
    auth_method: Optional[str] = None  # "pat", "spn", "default"
    error: Optional[str] = None
    otel_exporter_active: bool = False  # OTel MLflow exporter handles trace creation


async def configure_mlflow_in_subprocess(
    db_config: Any,
    job_id: str,
    execution_id: str,
    group_id: Optional[str],
    group_context: Any = None,
    async_logger: Optional[logging.Logger] = None,
) -> MlflowSetupResult:
    """Configure MLflow tracing inside a subprocess (crew or flow).

    This function encapsulates all MLflow setup previously done inline in
    ``process_crew_executor.py`` (lines 601-1055).  It is safe to call from
    any subprocess — crew or flow — and returns a :class:`MlflowSetupResult`
    describing what was configured.

    The function is **fault-tolerant**: if any step fails, execution continues
    and the caller can inspect ``result.error`` / ``result.tracing_ready``.

    Args:
        db_config: Databricks configuration object (from DatabricksService).
            Must have ``mlflow_enabled`` attribute.
        job_id: The job / execution ID for log correlation.
        execution_id: The execution ID (often same as job_id).
        group_id: Optional group ID for multi-tenant isolation.
        group_context: Optional group context object for UserContext setup.
        async_logger: Logger routed to the subprocess log file.

    Returns:
        MlflowSetupResult with setup outcome.
    """
    alog = async_logger or logging.getLogger("crew")

    # -----------------------------------------------------------
    # 1. Check if MLflow is enabled for this workspace
    # -----------------------------------------------------------
    enabled_for_workspace = False
    try:
        enabled_for_workspace = bool(getattr(db_config, "mlflow_enabled", False))
    except Exception:
        enabled_for_workspace = False

    alog.info(f"[SUBPROCESS] MLflow enabled_for_workspace={enabled_for_workspace}")

    if not enabled_for_workspace:
        alog.info("[SUBPROCESS] MLflow is disabled for this workspace; skipping configuration")
        return MlflowSetupResult(enabled=False, tracing_ready=False)

    alog.info(f"[SUBPROCESS] Configuring MLflow for execution {execution_id}")

    try:
        # -------------------------------------------------------
        # 2. Import MLflow (graceful fallback if not installed)
        # -------------------------------------------------------
        try:
            import mlflow
        except ImportError:
            alog.warning("[SUBPROCESS] MLflow package not installed; skipping configuration")
            return MlflowSetupResult(
                enabled=True, tracing_ready=False, error="mlflow not installed"
            )

        # -------------------------------------------------------
        # 3. Auth setup: PAT / SPN / default
        # -------------------------------------------------------
        from src.utils.databricks_auth import get_auth_context
        from src.utils.user_context import UserContext

        # Set UserContext with group_context in subprocess (contextvars are process-local)
        if group_context:
            try:
                UserContext.set_group_context(group_context)
                alog.info(
                    f"[SUBPROCESS] Set UserContext with group_id: "
                    f"{getattr(group_context, 'primary_group_id', None)}"
                )
            except Exception as ctx_err:
                alog.warning(f"[SUBPROCESS] Could not set UserContext: {ctx_err}")

        # MLflow uses service-level auth (NO OBO) to avoid race conditions
        auth = await get_auth_context(user_token=None)
        if not auth:
            alog.warning("[SUBPROCESS] No Databricks authentication available for MLflow")
            return MlflowSetupResult(
                enabled=True,
                tracing_ready=False,
                error="no Databricks auth available",
            )

        os.environ["DATABRICKS_HOST"] = auth.workspace_url
        os.environ["DATABRICKS_TOKEN"] = auth.token
        auth_method = auth.auth_method
        alog.info(f"[SUBPROCESS] MLflow configured with {auth_method} authentication")

        # -------------------------------------------------------
        # 4. Tracking URI
        # -------------------------------------------------------
        mlflow.set_tracking_uri("databricks")

        # -------------------------------------------------------
        # 5. Experiment name resolution + set_experiment
        # -------------------------------------------------------
        experiment_name = "/Shared/kasal-crew-execution-traces"  # default
        try:
            from src.db.session import async_session_factory
            from src.services.databricks_service import DatabricksService

            async with async_session_factory() as session:
                databricks_service = DatabricksService(
                    session=session,
                    group_id=group_id,
                )
                fresh_config = await databricks_service.get_databricks_config()
                if fresh_config and fresh_config.mlflow_experiment_name:
                    exp_name = fresh_config.mlflow_experiment_name
                    if not exp_name.startswith("/"):
                        exp_name = f"/Shared/{exp_name}"
                    experiment_name = exp_name
                    alog.info(
                        f"[SUBPROCESS] Using MLflow experiment name from config: {experiment_name}"
                    )
        except Exception as config_err:
            alog.info(
                f"[SUBPROCESS] Could not fetch MLflow experiment name from config: "
                f"{config_err}, using default: {experiment_name}"
            )

        experiment = None
        experiment_id = None
        try:
            experiment = mlflow.set_experiment(experiment_name)
            experiment_id = experiment.experiment_id
            alog.info(
                f"[SUBPROCESS] MLflow experiment set: {experiment_name} (ID: {experiment_id})"
            )
        except Exception as exp_error:
            alog.warning(
                f"[SUBPROCESS] Could not set experiment {experiment_name}: {exp_error}"
            )
            try:
                experiment = mlflow.set_experiment("/Shared/crew-traces")
                experiment_id = experiment.experiment_id
                alog.info(
                    f"[SUBPROCESS] MLflow fallback experiment set: /Shared/crew-traces "
                    f"(ID: {experiment_id})"
                )
            except Exception as fallback_error:
                experiment = None
                alog.warning(
                    f"[SUBPROCESS] Could not set MLflow experiment: {fallback_error}"
                )

        # -------------------------------------------------------
        # 6. Tracing destination
        # -------------------------------------------------------
        try:
            from mlflow.tracing.destination import Databricks as _MlflowDbxDest

            if experiment is not None:
                mlflow.tracing.set_destination(
                    _MlflowDbxDest(experiment_id=str(experiment.experiment_id))
                )
                alog.info(
                    f"[SUBPROCESS] MLflow tracing destination set to experiment {experiment.experiment_id}"
                )
            else:
                alog.info("[SUBPROCESS] MLflow tracing destination not set (no experiment)")
        except Exception as e:
            alog.info(f"[SUBPROCESS] Could not set MLflow tracing destination: {e}")

        # -------------------------------------------------------
        # 7. Enable tracing
        # -------------------------------------------------------
        mlflow_tracing_ready = False
        try:
            mlflow.tracing.enable()
            mlflow_tracing_ready = True
            is_enabled_fn = getattr(
                getattr(mlflow, "tracing", None), "is_enabled", None
            )
            if callable(is_enabled_fn):
                alog.info(
                    f"[SUBPROCESS] MLflow tracing enabled (is_enabled={is_enabled_fn()})"
                )
            else:
                alog.info("[SUBPROCESS] MLflow tracing enabled")
        except Exception:
            pass

        # -------------------------------------------------------
        # 8. Autologs via mlflow_integration.py
        # -------------------------------------------------------
        try:
            from src.engines.crewai.mlflow_integration import (
                enable_autologs as _enable_autologs,
            )

            _enable_autologs(
                global_autolog=True,
                global_log_traces=True,
                crewai_autolog=True,
                litellm_spans_only=True,
            )
            alog.info("[SUBPROCESS] Autologs configured via tracing service")
        except Exception:
            pass

        # -------------------------------------------------------
        # 9. Async logging
        # -------------------------------------------------------
        try:
            mlflow.config.enable_async_logging()
            alog.info("[SUBPROCESS] MLflow async logging enabled")
        except Exception as async_log_err:
            alog.info(f"[SUBPROCESS] MLflow async logging not available: {async_log_err}")

        # Log MLflow state summary
        alog.info(f"[SUBPROCESS] MLflow tracking URI: {mlflow.get_tracking_uri()}")
        alog.info(
            f"[SUBPROCESS] DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST', '<unset>')}"
        )
        if experiment is not None:
            alog.info(f"[SUBPROCESS] MLflow experiment ID: {experiment.experiment_id}")

        # -------------------------------------------------------
        # 10. LiteLLM tracked_completion monkey-patch
        # -------------------------------------------------------
        try:
            from functools import wraps

            import litellm

            original_completion = litellm.completion

            @wraps(original_completion)
            def tracked_completion(*args: Any, **kwargs: Any) -> Any:
                import time as _llm_time

                model = kwargs.get("model", "unknown")
                llm_start_time = _llm_time.time()
                alog.info(f"[SUBPROCESS] LiteLLM call START - Model: {model}")

                # Log last active trace id before call
                try:
                    import mlflow as _ml_pre

                    get_last = getattr(
                        getattr(_ml_pre, "tracing", None),
                        "get_last_active_trace_id",
                        None,
                    )
                    if callable(get_last):
                        alog.info(
                            f"[SUBPROCESS] - Last active trace id (pre-call): {get_last()}"
                        )
                except Exception:
                    pass

                # Execute with manual nested span as fallback
                result = None
                try:
                    try:
                        import mlflow as _ml

                        span_name = f"litellm.completion:{model}"
                        with _ml.start_span(name=span_name) as _span:
                            try:
                                _span.set_attribute("model", model)
                                _span.set_attribute("mlflow_span_fallback", True)
                            except Exception:
                                pass
                            result = original_completion(*args, **kwargs)
                    except Exception as _span_err:
                        if (
                            "mlflow" in str(_span_err).lower()
                            or "span" in str(_span_err).lower()
                        ):
                            alog.warning(
                                f"[SUBPROCESS] Fallback MLflow span failed: {_span_err}"
                            )
                            result = original_completion(*args, **kwargs)
                        else:
                            raise
                except Exception as _llm_err:
                    llm_duration = _llm_time.time() - llm_start_time
                    alog.error(
                        f"[SUBPROCESS] LiteLLM call FAILED - Model: {model}, "
                        f"Duration: {llm_duration:.2f}s, Error: {str(_llm_err)[:500]}"
                    )
                    raise

                llm_duration = _llm_time.time() - llm_start_time

                # Log last active trace id after call
                try:
                    import mlflow as _ml_post

                    get_last = getattr(
                        getattr(_ml_post, "tracing", None),
                        "get_last_active_trace_id",
                        None,
                    )
                    if callable(get_last):
                        alog.info(
                            f"[SUBPROCESS] - Last active trace id (post-call): {get_last()}"
                        )
                except Exception:
                    pass

                # Log response details
                try:
                    if result is None:
                        alog.error(
                            f"[SUBPROCESS] LLM Response is None - Model: {model}, "
                            f"Duration: {llm_duration:.2f}s"
                        )
                    else:
                        choices = getattr(result, "choices", None)
                        if choices is None:
                            alog.error(
                                f"[SUBPROCESS] LLM Response has no 'choices' - Model: {model}, "
                                f"Duration: {llm_duration:.2f}s, Type: {type(result)}"
                            )
                        elif len(choices) == 0:
                            alog.error(
                                f"[SUBPROCESS] LLM Response 'choices' is empty - Model: {model}, "
                                f"Duration: {llm_duration:.2f}s"
                            )
                        else:
                            first_choice = choices[0]
                            message = getattr(first_choice, "message", None)
                            if message is None:
                                alog.error(
                                    f"[SUBPROCESS] LLM Response choice has no 'message' - "
                                    f"Model: {model}, Duration: {llm_duration:.2f}s"
                                )
                            else:
                                content = getattr(message, "content", None)
                                if content is None or content == "":
                                    alog.error(
                                        f"[SUBPROCESS] LLM Response content is None/empty - "
                                        f"Model: {model}, Duration: {llm_duration:.2f}s"
                                    )
                                    reasoning = getattr(message, "reasoning_content", None)
                                    if reasoning:
                                        alog.info(
                                            f"[SUBPROCESS] LLM has reasoning_content: "
                                            f"{str(reasoning)[:200]}..."
                                        )
                                else:
                                    alog.info(
                                        f"[SUBPROCESS] LLM Response OK - Model: {model}, "
                                        f"Content length: {len(content)}, "
                                        f"Duration: {llm_duration:.2f}s"
                                    )
                except Exception as log_err:
                    alog.warning(
                        f"[SUBPROCESS] Could not log response details: {log_err}"
                    )

                alog.info(
                    f"[SUBPROCESS] LiteLLM call COMPLETED - Model: {model}, "
                    f"Duration: {llm_duration:.2f}s"
                )
                return result

            litellm.completion = tracked_completion
            alog.info(
                "[SUBPROCESS] LiteLLM call tracking enabled (with fallback MLflow span)"
            )
        except Exception as tracking_error:
            alog.warning(
                f"[SUBPROCESS] Could not set up LiteLLM tracking: {tracking_error}"
            )

        alog.info("[SUBPROCESS] MLflow configuration completed successfully")

        return MlflowSetupResult(
            enabled=True,
            tracing_ready=mlflow_tracing_ready,
            experiment_name=experiment_name,
            experiment_id=experiment_id,
            auth_method=auth_method,
        )

    except Exception as mlflow_error:
        alog.error(f"[SUBPROCESS] MLflow configuration failed: {mlflow_error}")
        return MlflowSetupResult(
            enabled=True,
            tracing_ready=False,
            error=str(mlflow_error),
        )


# ---------------------------------------------------------------------------
# Runtime helpers – called by executors *after* configure_mlflow_in_subprocess
# ---------------------------------------------------------------------------


def _try_import_mlflow():
    """Import mlflow if available; return None otherwise."""
    try:
        import mlflow  # type: ignore
        return mlflow
    except ImportError:
        return None


def log_mlflow_state(
    phase: str,
    async_logger: Optional[logging.Logger] = None,
) -> None:
    """Log MLflow runtime state for diagnostics.

    Safe to call even when mlflow is not installed – becomes a no-op.

    Args:
        phase: Human-readable label such as ``"pre-exec"`` or ``"post-exec"``.
        async_logger: Logger routed to the subprocess log file.
    """
    alog = async_logger or logger
    mlflow = _try_import_mlflow()
    if mlflow is None:
        alog.info(f"[SUBPROCESS] MLflow not available; skipping {phase} state logs")
        return

    alog.info(f"[SUBPROCESS] MLflow state ({phase}):")
    try:
        try:
            import crewai as _c
            import litellm as _lt

            alog.info(
                f"[SUBPROCESS] - Versions: mlflow={getattr(mlflow, '__version__', '?')}, "
                f"crewai={getattr(_c, '__version__', '?')}, "
                f"litellm={getattr(_lt, '__version__', '?')}"
            )
        except Exception:
            alog.info(
                f"[SUBPROCESS] - Versions: mlflow={getattr(mlflow, '__version__', '?')}"
            )

        alog.info(f"[SUBPROCESS] - Tracking URI: {mlflow.get_tracking_uri()}")

        # Active run info
        try:
            active_run = mlflow.active_run()
            if active_run:
                alog.info(f"[SUBPROCESS] - Active run: {active_run.info.run_id}")
            else:
                alog.info(f"[SUBPROCESS] - No active run (traces may be run-less)")
        except Exception as e:
            alog.info(f"[SUBPROCESS] - Error checking active run: {e}")

        # Last active trace id
        try:
            get_last = getattr(
                getattr(mlflow, "tracing", None),
                "get_last_active_trace_id",
                None,
            )
            if callable(get_last):
                alog.info(
                    f"[SUBPROCESS] - Last active trace id ({phase}): {get_last()}"
                )
        except Exception as e:
            alog.info(
                f"[SUBPROCESS] - Could not get last trace id ({phase}): {e}"
            )
    except Exception as e:
        alog.info(f"[SUBPROCESS] - Error logging MLflow state: {e}")


async def capture_trace_and_update_execution(
    execution_id: str,
    experiment_name: Optional[str],
    group_id: Optional[str],
    async_logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    """Capture the last MLflow trace ID and persist it on the execution record.

    Args:
        execution_id: Execution to update.
        experiment_name: MLflow experiment name (for the execution record).
        group_id: Optional tenant group ID.
        async_logger: Logger routed to the subprocess log file.

    Returns:
        The captured trace ID, or ``None`` if unavailable.
    """
    alog = async_logger or logger
    try:
        from src.engines.crewai.mlflow_integration import (
            update_execution_trace_id as _update_trace,
        )
        from src.services.mlflow_tracing_service import (
            get_last_active_trace_id as _get_last_id,
        )

        last_trace_id = _get_last_id()
        if last_trace_id:
            alog.info(
                f"[SUBPROCESS] - Last active trace id: {last_trace_id}"
            )
            exp = experiment_name or "/Shared/kasal-crew-execution-traces"
            await _update_trace(
                execution_id=execution_id,
                trace_id=last_trace_id,
                experiment_name=exp,
                group_id=group_id,
            )
            alog.info(
                f"[SUBPROCESS] - Stored MLflow trace ID {last_trace_id} in execution history"
            )
        else:
            alog.info(f"[SUBPROCESS] - No last active trace id available")
        return last_trace_id
    except Exception as e:
        alog.warning(
            f"[SUBPROCESS] - Failed to capture/store trace ID: {e}"
        )
        return None


def disable_autologs_for_safety(
    async_logger: Optional[logging.Logger] = None,
) -> None:
    """Disable MLflow LiteLLM autolog hooks to prevent subprocess deadlocks.

    Args:
        async_logger: Logger routed to the subprocess log file.
    """
    alog = async_logger or logger
    mlflow = _try_import_mlflow()
    if mlflow is None:
        return
    try:
        mlflow.litellm.autolog(disable=True)
        alog.info(
            "[SUBPROCESS] Disabled MLflow LiteLLM autolog (prevents subprocess deadlock)"
        )
    except Exception as e:
        alog.warning(f"[SUBPROCESS] Could not disable LiteLLM autolog: {e}")


def set_trace_attributes(
    span: Any,
    config: dict,
    async_logger: Optional[logging.Logger] = None,
) -> None:
    """Set execution metadata attributes on a root trace span.

    Works for both crew and flow configurations.

    Args:
        span: The MLflow LiveSpan / trace object.
        config: Execution configuration dict with keys like ``run_name``,
            ``version``, ``process``, ``agents``, ``tasks``, ``model_name``.
        async_logger: Logger routed to the subprocess log file.
    """
    alog = async_logger or logger
    if span is None or not hasattr(span, "set_attribute"):
        return
    try:
        span.set_attribute("execution.name", config.get("run_name", "Unnamed"))
        span.set_attribute("execution.version", config.get("version", "1.0"))
        span.set_attribute(
            "execution.process_type", config.get("process", "sequential")
        )
        span.set_attribute(
            "execution.agent_count", len(config.get("agents", []))
        )
        span.set_attribute(
            "execution.task_count", len(config.get("tasks", []))
        )
        if config.get("model_name"):
            span.set_attribute("execution.model", config["model_name"])
    except Exception as e:
        alog.debug(f"[SUBPROCESS] Could not set trace attributes: {e}")


def extract_trace_outputs(
    result: Any,
    async_logger: Optional[logging.Logger] = None,
) -> dict:
    """Extract trace-friendly outputs from a CrewAI result object.

    Args:
        result: The object returned by ``crew.kickoff()`` or similar.
        async_logger: Logger routed to the subprocess log file.

    Returns:
        Dictionary suitable for ``span.set_outputs()``.
    """
    alog = async_logger or logger
    outputs: dict = {}
    try:
        if hasattr(result, "raw"):
            outputs["raw"] = result.raw
        if hasattr(result, "pydantic"):
            outputs["pydantic"] = result.pydantic
        if hasattr(result, "json_dict"):
            outputs["json_dict"] = result.json_dict
        if hasattr(result, "tasks_output"):
            outputs["tasks_output"] = [
                {
                    "description": (
                        task.description
                        if hasattr(task, "description")
                        else str(task)
                    ),
                    "name": (
                        task.name if hasattr(task, "name") else "Unknown"
                    ),
                    "output": (
                        task.raw if hasattr(task, "raw") else str(task)
                    ),
                }
                for task in (
                    result.tasks_output
                    if hasattr(result, "tasks_output")
                    else []
                )
            ]
        if hasattr(result, "token_usage"):
            outputs["token_usage"] = result.token_usage
    except Exception as e:
        alog.debug(f"[SUBPROCESS] Could not extract trace outputs: {e}")
    return outputs


# ---------------------------------------------------------------------------
# High-level execution wrapper — wraps crew/flow kickoff in an MLflow root trace
# ---------------------------------------------------------------------------


def execute_with_mlflow_trace(
    kickoff_fn: Callable[[], Any],
    mlflow_result: Optional[MlflowSetupResult],
    crew_config: dict,
    inputs: Optional[dict] = None,
    async_logger: Optional[logging.Logger] = None,
) -> Any:
    """Execute a crew/flow kickoff wrapped in an MLflow root trace.

    If MLflow tracing is not ready, executes the kickoff directly without
    any MLflow involvement.  The executor never sees ``import mlflow``.

    Args:
        kickoff_fn: Zero-arg callable that runs the crew/flow (e.g.
            ``lambda: crew.kickoff(inputs=inputs)``).
        mlflow_result: Result from ``configure_mlflow_in_subprocess``.
        crew_config: Execution configuration dict (used for trace attributes).
        inputs: Optional inputs dict for trace metadata.
        async_logger: Logger routed to the subprocess log file.

    Returns:
        The result from ``kickoff_fn()``.
    """
    alog = async_logger or logger

    if not mlflow_result or not mlflow_result.tracing_ready:
        return kickoff_fn()

    # When OTel MLflow exporter is active, it handles trace creation —
    # skip the inline wrapper to avoid duplicate traces.
    if mlflow_result.otel_exporter_active:
        alog.info("[SUBPROCESS] OTel MLflow exporter active, executing without inline trace wrapper")
        return kickoff_fn()

    # Import the root trace context manager from tracing service
    try:
        from src.services.mlflow_tracing_service import start_root_trace
    except ImportError:
        alog.warning("[SUBPROCESS] start_root_trace not available, executing without trace")
        return kickoff_fn()

    run_name = crew_config.get("run_name") or "Unnamed"
    trace_name = f"crew_kickoff:{run_name}"
    trace_inputs = {**(inputs or {}), "run_name": run_name}

    with start_root_trace(trace_name, trace_inputs) as root_span:
        # Set execution attributes on the root span
        set_trace_attributes(root_span, crew_config, alog)

        # Execute the crew/flow
        result = kickoff_fn()

        # Extract outputs and set on trace
        outputs = extract_trace_outputs(result, alog)
        if outputs and root_span is not None and hasattr(root_span, "set_outputs"):
            try:
                root_span.set_outputs(outputs)
            except Exception:
                pass

        return result


async def execute_with_mlflow_trace_async(
    kickoff_coro_fn: Callable[..., Any],
    mlflow_result: Optional[MlflowSetupResult],
    flow_config: dict,
    inputs: Optional[dict] = None,
    async_logger: Optional[logging.Logger] = None,
    **kickoff_kwargs: Any,
) -> Any:
    """Async variant: wraps an awaitable kickoff in an MLflow root trace.

    Args:
        kickoff_coro_fn: Async callable whose result we ``await``.
        mlflow_result: Result from ``configure_mlflow_in_subprocess``.
        flow_config: Execution configuration dict (used for trace attributes).
        inputs: Optional inputs dict for trace metadata.
        async_logger: Logger routed to the subprocess log file.
        **kickoff_kwargs: Forwarded to ``kickoff_coro_fn``.

    Returns:
        The awaited result.
    """
    alog = async_logger or logger

    if not mlflow_result or not mlflow_result.tracing_ready:
        return await kickoff_coro_fn(**kickoff_kwargs)

    # When OTel MLflow exporter is active, it handles trace creation —
    # skip the inline wrapper to avoid duplicate traces.
    if mlflow_result.otel_exporter_active:
        alog.info("[SUBPROCESS] OTel MLflow exporter active, executing without inline trace wrapper")
        return await kickoff_coro_fn(**kickoff_kwargs)

    try:
        from src.services.mlflow_tracing_service import start_root_trace
    except ImportError:
        alog.warning("[SUBPROCESS] start_root_trace not available, executing without trace")
        return await kickoff_coro_fn(**kickoff_kwargs)

    run_name = flow_config.get("run_name") or "Unnamed"
    trace_name = f"flow_kickoff:{run_name}"
    trace_inputs = {**(inputs or {}), "run_name": run_name}

    with start_root_trace(trace_name, trace_inputs) as root_span:
        set_trace_attributes(root_span, flow_config, alog)
        result = await kickoff_coro_fn(**kickoff_kwargs)
        outputs = extract_trace_outputs(result, alog)
        if outputs and root_span is not None and hasattr(root_span, "set_outputs"):
            try:
                root_span.set_outputs(outputs)
            except Exception:
                pass
        return result


# ---------------------------------------------------------------------------
# High-level post-execution cleanup — the ONLY function executors need to call
# ---------------------------------------------------------------------------


async def post_execution_mlflow_cleanup(
    mlflow_result: Optional[MlflowSetupResult],
    execution_id: str,
    group_id: Optional[str] = None,
    async_logger: Optional[logging.Logger] = None,
) -> None:
    """Post-execution MLflow cleanup: flush, capture trace ID, and stop writers.

    This is the **single entry-point** executors call after crew/flow execution.
    It is a no-op when ``mlflow_result`` is ``None`` or tracing was not ready.

    Order matters: we flush async logging FIRST so that autolog traces created
    during ``crew.kickoff()`` are committed before we attempt to read the last
    active trace ID.

    Args:
        mlflow_result: Result from ``configure_mlflow_in_subprocess`` (may be None).
        execution_id: Execution ID for the trace record update.
        group_id: Optional tenant group ID.
        async_logger: Logger routed to the subprocess log file.
    """
    import asyncio

    alog = async_logger or logger

    if not mlflow_result or not mlflow_result.tracing_ready:
        return

    # 1. Flush MLflow async logging FIRST — autolog traces are queued
    #    asynchronously and must be committed before we can read trace IDs.
    try:
        from src.services.mlflow_tracing_service import (
            flush_async_logging as _flush_mlflow,
        )
        await _flush_mlflow(async_logger=alog)
    except Exception as e:
        alog.warning(f"[SUBPROCESS] MLflow flush error: {e}")

    # Brief wait for HTTP transport to complete after flush
    await asyncio.sleep(1.0)

    # 2. Post-execution state log (now shows correct trace ID after flush)
    log_mlflow_state("post-exec", alog)

    # 3. Capture last MLflow trace ID and persist it on the execution record
    await capture_trace_and_update_execution(
        execution_id=execution_id,
        experiment_name=mlflow_result.experiment_name,
        group_id=group_id,
        async_logger=alog,
    )

    # 4. Flush and stop TraceManager writer (CrewAI-specific)
    try:
        from src.engines.crewai.mlflow_integration import (
            flush_and_stop_writers as _flush_stop,
        )
        await _flush_stop(async_logger=alog)
    except Exception as e:
        alog.warning(f"[SUBPROCESS] MLflow flush/stop error: {e}")
