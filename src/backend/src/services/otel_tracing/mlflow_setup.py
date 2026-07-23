"""Centralized MLflow subprocess setup for Kasal executors.

This module consolidates all MLflow initialization logic that was previously
inline in process_crew_executor.py (~450 lines) into a single reusable function.
Both crew and flow executors call ``configure_mlflow_in_subprocess()`` to get
consistent MLflow tracing setup including:

- Databricks authentication (SPN-only)
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


# Default prefix for the four UC OTel trace tables (<prefix>_otel_spans, etc.).
KASAL_TRACE_TABLE_PREFIX = "kasal"

# Suffix for the dedicated UC-only experiment. UC trace storage requires an
# experiment that has NEVER contained a managed (non-UC) trace, so UC traces use
# a separate experiment name that managed writers never touch. Both the crew
# subprocess and the parent-process services (dispatcher + generation) derive the
# UC experiment via this suffix so all their traces land together.
KASAL_UC_EXPERIMENT_SUFFIX = "-uc"


def uc_experiment_name(base: str) -> str:
    """Return the dedicated UC experiment name for a configured base name.

    Idempotent: an already-suffixed name is returned unchanged so repeated
    derivation (parent + subprocess) cannot produce ``...-uc-uc``.
    """
    if base.endswith(KASAL_UC_EXPERIMENT_SUFFIX):
        return base
    return f"{base}{KASAL_UC_EXPERIMENT_SUFFIX}"


def _build_uc_trace_location(
    catalog: Optional[str], schema: Optional[str], warehouse_id: Optional[str], log
) -> Any:
    """Build a UnityCatalog trace location so MLflow stores trace spans in UC
    Delta tables (in-network, via the SQL warehouse) instead of uploading a
    traces.json blob to workspace object storage — which a serverless Databricks
    App often cannot reach (``Connection refused`` to ``*.storage.cloud.databricks.com``).

    Requires MLflow >= 3.11 and a configured catalog + schema + SQL warehouse id.
    Returns ``None`` when any of those is missing (in which case the caller falls
    back to the legacy experiment artifact storage). The warehouse id is REQUIRED:
    MLflow runs the trace-table DDL through it, and passing a missing id into the
    backend raises an opaque "bad argument type for built-in operation" instead of
    a clear error.
    """
    if not (catalog and schema):
        return None
    # Guard against non-string values (e.g. a Pydantic BaseModel.schema *method*
    # leaking through when the field is read by its alias instead of `db_schema`).
    # Passing a non-str into MLflow surfaces only as "bad argument type for built-in
    # operation"; fail clearly here instead.
    if not (isinstance(catalog, str) and isinstance(schema, str)):
        log.warning(
            f"[MLflow] UC trace storage skipped: catalog/schema must be strings, got "
            f"catalog={type(catalog).__name__}, schema={type(schema).__name__}"
        )
        return None
    if not warehouse_id:
        log.warning(
            "[MLflow] UC trace storage skipped: no SQL warehouse id configured "
            "(set warehouse_id in the Databricks config) — falling back to artifact storage"
        )
        return None
    try:
        from mlflow.entities.trace_location import UnityCatalog
    except Exception:
        log.info(
            "[MLflow] UnityCatalog trace location unavailable (needs MLflow >= 3.11) "
            "— falling back to experiment artifact storage"
        )
        return None
    return UnityCatalog(
        catalog_name=catalog,
        schema_name=schema,
        table_prefix=KASAL_TRACE_TABLE_PREFIX,
    )


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
    uc_trace_storage: bool = False  # UC Delta-table trace storage active (native autolog path)


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
        # 2. SPN credential check FIRST — before importing mlflow.
        #
        # ``import mlflow`` costs ~1.1s inside the subprocess critical path
        # (spawn → kickoff). Environments without SPN credentials (all local
        # dev, any app without injected client credentials) skip MLflow
        # anyway, so check the cheap env vars before paying the import.
        #
        # MLflow auth: use SPN credentials (DATABRICKS_CLIENT_ID /
        # DATABRICKS_CLIENT_SECRET / DATABRICKS_HOST) injected by the
        # Databricks Apps platform.
        #
        # NOTE: Databricks Apps proxy redacts log lines containing words
        # like SECRET/TOKEN/BEARER.  Use neutral wording in all logs.
        # -------------------------------------------------------
        host = os.environ.get("DATABRICKS_HOST", "")
        client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

        alog.info(
            "[SUBPROCESS] MLflow auth env — host=%s, spn_id=%s, spn_cred=%s",
            "yes" if host else "no",
            "yes" if client_id else "no",
            "yes" if client_secret else "no",
        )

        # SPN credentials are required — skip MLflow if absent
        if not (host and client_id and client_secret):
            alog.warning("[SUBPROCESS] SPN credentials required for MLflow — skipping")
            return MlflowSetupResult(
                enabled=True,
                tracing_ready=False,
                error="SPN credentials required",
            )

        # -------------------------------------------------------
        # 3. Import MLflow (graceful fallback if not installed)
        # -------------------------------------------------------
        try:
            import mlflow
        except ImportError:
            alog.warning("[SUBPROCESS] MLflow package not installed; skipping configuration")
            return MlflowSetupResult(
                enabled=True, tracing_ready=False, error="mlflow not installed"
            )

        # -------------------------------------------------------
        # 4. Auth setup: SPN → PAT (NO OBO for MLflow)
        # -------------------------------------------------------
        from src.utils.user_context import UserContext

        # Set UserContext with group_context in subprocess (contextvars are
        # process-local). The crew subprocess also sets this independently
        # (process_crew_executor), so skipping it via the early returns above
        # is safe.
        if group_context:
            try:
                UserContext.set_group_context(group_context)
                alog.info(
                    f"[SUBPROCESS] Set UserContext with group_id: "
                    f"{getattr(group_context, 'primary_group_id', None)}"
                )
            except Exception as ctx_err:
                alog.warning(f"[SUBPROCESS] Could not set UserContext: {ctx_err}")

        # The platform also injects DATABRICKS_TOKEN (a PAT) which
        # conflicts with SPN in the SDK ("more than one authorization
        # method").  We strip PAT vars before the SDK call and restore
        # them after; a bearer token is extracted up-front so the MLflow
        # exporter uses simple HOST + TOKEN auth.

        auth_method: Optional[str] = None

        # Extract credential via SDK
        alog.info("[SUBPROCESS] Attempting SPN credential extraction")
        try:
            from databricks.sdk import WorkspaceClient as _WC

            alog.info("[SUBPROCESS] SDK imported OK")

            # Temporarily strip PAT/API-KEY from env to prevent
            # SDK dual-auth conflict (oauth + pat)
            _pat_backup = {}
            for _k in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
                if _k in os.environ:
                    _pat_backup[_k] = os.environ.pop(_k)
            try:
                from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct
                from databricks.sdk.useragent import with_product
                with_product(f"{KASAL_BASE}_{KasalProduct.MLFLOW}", VERSION)
                w = _WC(host=host, client_id=client_id, client_secret=client_secret)
                headers = w.config.authenticate()
            finally:
                os.environ.update(_pat_backup)

            alog.info("[SUBPROCESS] authenticate() returned OK")
            auth_header = headers.get("Authorization", "")
            alog.info(
                "[SUBPROCESS] auth header length=%d, prefix=%s",
                len(auth_header),
                auth_header[:7] if auth_header else "(empty)",
            )
            if auth_header.startswith("Bearer "):
                # SPN credentials verified. Configure UNIFIED OAuth M2M auth — the
                # recommended pattern for Databricks Apps — by KEEPING host +
                # client_id + client_secret so the SDK mints and refreshes tokens
                # itself. This is required for MLflow UC trace storage, which builds
                # its own WorkspaceClient() for the SQL-warehouse DDL. We do NOT
                # downgrade to a static PAT (not recommended for Apps in production).
                workspace_url = host.rstrip("/")
                if not workspace_url.startswith("http"):
                    workspace_url = f"https://{workspace_url}"
                os.environ["DATABRICKS_HOST"] = workspace_url
                os.environ["DATABRICKS_CLIENT_ID"] = client_id
                os.environ["DATABRICKS_CLIENT_SECRET"] = client_secret
                os.environ["DATABRICKS_AUTH_TYPE"] = "oauth-m2m"
                # Remove static token vars so oauth-m2m is the SINGLE auth method
                # (the SDK errors with "more than one authorization method
                # configured" if a token is also present).
                for _k in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
                    os.environ.pop(_k, None)
                auth_method = "service_principal"
                alog.info(
                    "[SUBPROCESS] MLflow configured with SPN OAuth M2M (unified auth)"
                )
            else:
                alog.warning("[SUBPROCESS] Unexpected auth header format")
        except Exception as spn_err:
            alog.warning(
                "[SUBPROCESS] SPN extraction failed: %s: %s",
                type(spn_err).__name__, spn_err,
            )

        if not auth_method:
            alog.warning("[SUBPROCESS] SPN credential extraction failed — MLflow cannot be configured")
            return MlflowSetupResult(
                enabled=True,
                tracing_ready=False,
                error="SPN credential extraction failed",
            )

        alog.info("[SUBPROCESS] MLflow configured with %s authentication", auth_method)

        # -------------------------------------------------------
        # 4. Tracking URI
        # -------------------------------------------------------
        mlflow.set_tracking_uri("databricks")

        # -------------------------------------------------------
        # 5. Experiment name + UC trace storage resolution
        # -------------------------------------------------------
        experiment_name = "/Shared/kasal-crew-execution-traces"  # default
        uc_catalog = None
        uc_schema = None
        warehouse_id = None
        try:
            from src.db.session import async_session_factory
            from src.services.databricks_service import DatabricksService

            async with async_session_factory() as session:
                databricks_service = DatabricksService(
                    session=session,
                    group_id=group_id,
                )
                fresh_config = await databricks_service.get_databricks_config()
                if fresh_config:
                    if fresh_config.mlflow_experiment_name:
                        exp_name = fresh_config.mlflow_experiment_name
                        if not exp_name.startswith("/"):
                            exp_name = f"/Shared/{exp_name}"
                        experiment_name = exp_name
                        alog.info(
                            f"[SUBPROCESS] Using MLflow experiment name from config: {experiment_name}"
                        )
                    # Reuse the workspace's configured catalog/schema/warehouse for
                    # in-network UC trace storage (MLflow >= 3.11).
                    # NOTE: the schema field is `db_schema` (Pydantic aliases it to
                    # "schema"); `getattr(config, "schema")` returns BaseModel.schema
                    # (a method), which MLflow then rejects with the opaque
                    # "bad argument type for built-in operation".
                    uc_catalog = getattr(fresh_config, "catalog", None)
                    uc_schema = getattr(fresh_config, "db_schema", None)
                    warehouse_id = getattr(fresh_config, "warehouse_id", None)
        except Exception as config_err:
            alog.info(
                f"[SUBPROCESS] Could not fetch MLflow config: "
                f"{config_err}, using default experiment: {experiment_name}"
            )

        # UC trace storage needs the SQL warehouse id in the environment BEFORE
        # set_experiment creates/binds the trace tables.
        if warehouse_id:
            os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = str(warehouse_id)

        alog.info(
            f"[SUBPROCESS] MLflow trace storage config — catalog={uc_catalog!r}, "
            f"schema={uc_schema!r}, warehouse_id={warehouse_id!r}"
        )
        trace_location = _build_uc_trace_location(uc_catalog, uc_schema, warehouse_id, alog)

        # When UC trace storage is active, spans are written to the
        # `<prefix>_otel_*` Delta tables by MLflow's DatabricksUCTableSpanExporter
        # via `MlflowClient.log_spans()` — a direct trace-server/SQL-warehouse
        # write, NOT the Databricks Apps localhost OTLP sidecar. So we still
        # neutralize the OTel-SDK default OTLP traces export (we never want the
        # localhost sidecar path -> the platform `otel_*` tables); MLflow's UC
        # processor is installed independently of OTEL_TRACES_EXPORTER, so this
        # does not suppress the UC write. The UC exporter is fed by native
        # autolog (enabled below) flowing through MLflow's own tracer; the
        # imperative KasalMLflowSpanExporter writes to the managed artifact store
        # (unreachable blob) and CANNOT populate UC, so it is NOT added to the
        # OTel pipeline in this mode (see process_*_executor.py).
        uc_trace_active = trace_location is not None
        if uc_trace_active:
            os.environ["OTEL_TRACES_EXPORTER"] = "none"
            for _otlp_k in (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "OTEL_EXPORTER_OTLP_ENDPOINT",
            ):
                os.environ.pop(_otlp_k, None)
            alog.info(
                "[SUBPROCESS] UC trace storage active — KasalMLflowSpanExporter owns "
                "the trace; OTLP traces export disabled (no localhost sidecar)"
            )
            # UC trace storage requires an experiment that has NEVER contained a
            # managed (non-UC) trace: MLflow permanently rejects binding a UC
            # Trace Destination to an experiment that "already contains traces"
            # (BAD_REQUEST). The dispatcher runs in the PARENT process where the
            # DB config is unavailable, so its trace_location is None and it
            # writes a *managed* trace to the base experiment first — poisoning
            # it for UC forever. Route UC crew traces to a dedicated, UC-only
            # experiment that no managed writer (dispatcher / legacy fallback)
            # ever touches, so the destination always binds and the
            # `<prefix>_otel_*` Delta tables actually receive spans.
            experiment_name = uc_experiment_name(experiment_name)
            alog.info(
                f"[SUBPROCESS] UC trace storage uses dedicated experiment: {experiment_name}"
            )

        def _set_experiment(name: str):
            """set_experiment with UC trace_location when available, else legacy."""
            if trace_location is not None:
                exp = mlflow.set_experiment(name, trace_location=trace_location)
                alog.info(
                    f"[SUBPROCESS] MLflow trace storage: Unity Catalog "
                    f"{uc_catalog}.{uc_schema}.{KASAL_TRACE_TABLE_PREFIX}_otel_* "
                    f"(warehouse={warehouse_id})"
                )
                return exp
            return mlflow.set_experiment(name)

        experiment = None
        experiment_id = None
        # Candidate experiment names. For UC trace storage we self-heal: if the
        # dedicated UC experiment somehow got poisoned by a managed trace, fall
        # forward to suffixed siblings rather than silently dropping back to a
        # managed experiment (the old `/Shared/crew-traces` fallback produced
        # searchable-but-not-UC traces and hid this bug). Legacy mode keeps the
        # original managed fallback.
        if uc_trace_active:
            candidate_names = [
                experiment_name,
                f"{experiment_name}-2",
                f"{experiment_name}-3",
            ]
        else:
            candidate_names = [experiment_name, "/Shared/crew-traces"]

        for _name in candidate_names:
            try:
                experiment = _set_experiment(_name)
                experiment_id = experiment.experiment_id
                alog.info(
                    f"[SUBPROCESS] MLflow experiment set: {_name} (ID: {experiment_id})"
                )
                break
            except Exception as exp_error:
                import traceback as _tb
                experiment = None
                alog.warning(
                    f"[SUBPROCESS] Could not set experiment {_name}: {exp_error}\n"
                    f"{_tb.format_exc()}"
                )
        if experiment is None:
            alog.warning("[SUBPROCESS] Could not set any MLflow experiment")

        # -------------------------------------------------------
        # 6. Tracing destination
        # -------------------------------------------------------
        try:
            from mlflow.tracing.destination import Databricks as _MlflowDbxDest

            if experiment is None:
                alog.info("[SUBPROCESS] MLflow tracing destination not set (no experiment)")
            elif uc_trace_active:
                # Do NOT pin the destination to Databricks(experiment_id). That
                # resolves to an MlflowExperimentLocation, which makes MLflow's
                # provider._get_span_processors SKIP UC auto-resolution and route
                # spans to managed (artifact) storage instead of the UC Delta
                # tables. Leaving the destination unset lets MLflow resolve the
                # active experiment's UnityCatalog trace_location and install
                # DatabricksUCTableSpanProcessor -> `<prefix>_otel_*` tables.
                alog.info(
                    "[SUBPROCESS] UC trace storage: destination auto-resolves to the "
                    f"experiment's UnityCatalog location (experiment {experiment.experiment_id}); "
                    "Databricks experiment-id destination intentionally NOT set"
                )
            else:
                mlflow.tracing.set_destination(
                    _MlflowDbxDest(experiment_id=str(experiment.experiment_id))
                )
                alog.info(
                    f"[SUBPROCESS] MLflow tracing destination set to experiment {experiment.experiment_id}"
                )
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
        # Native MLflow autolog (crewai + litellm) is what produces the spans
        # that flow through MLflow's own tracer. With UC trace storage active
        # those spans are written to the `<prefix>_otel_*` Delta tables by
        # DatabricksUCTableSpanExporter (log_spans -> warehouse, no localhost
        # sidecar). The inline start_root_trace wrapper (execute_with_mlflow_trace,
        # gated on otel_exporter_active=False) adds the named root span; autolog
        # nests the agent/LLM child spans under it. In both UC and legacy modes
        # we enable the same autologs — the difference is only the destination
        # (UC tables vs managed experiment).
        try:
            from src.engines.crewai.infra.mlflow_integration import (
                enable_autologs as _enable_autologs,
            )

            _enable_autologs(
                global_autolog=True,
                global_log_traces=True,
                crewai_autolog=True,
                litellm_spans_only=True,
            )
            if uc_trace_active:
                alog.info(
                    "[SUBPROCESS] Native MLflow autolog enabled — spans route to UC "
                    "Delta tables via DatabricksUCTableSpanExporter"
                )
            else:
                alog.info("[SUBPROCESS] Autologs configured via tracing service")
        except Exception as autolog_err:
            alog.warning(f"[SUBPROCESS] Could not enable MLflow autologs: {autolog_err}")

        # -------------------------------------------------------
        # 9. Async logging
        # -------------------------------------------------------
        # Async logging is DISABLED on purpose. Crew/flow tracing runs in a
        # subprocess that tears down immediately after the crew completes.
        # With async logging on, end_trace writes the trace *info*
        # synchronously but uploads the span-data artifact (traces.json) on a
        # background worker — which the subprocess kills before it finishes,
        # leaving a trace whose info is searchable but whose spans 404
        # (MlflowTraceDataNotFound). flush_trace_async_logging() does not
        # reliably cover the low-level client trace API here, so the robust fix
        # is a synchronous upload inside end_trace.
        try:
            mlflow.config.enable_async_logging(False)
            alog.info("[SUBPROCESS] MLflow async logging DISABLED (synchronous trace upload — required for subprocess lifecycle)")
        except Exception as async_log_err:
            alog.info(f"[SUBPROCESS] MLflow async logging config not available: {async_log_err}")

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
            uc_trace_storage=uc_trace_active,
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
        from src.engines.crewai.infra.mlflow_integration import (
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
    run_name: Optional[str] = None,
) -> None:
    """Set execution metadata attributes on a root trace span.

    Works for both crew and flow configurations.

    Args:
        span: The MLflow LiveSpan / trace object.
        config: Execution configuration dict with keys like ``run_name``,
            ``version``, ``process``, ``agents``, ``tasks``, ``model_name``.
        async_logger: Logger routed to the subprocess log file.
        run_name: Pre-resolved run name (from ``_derive_trace_run_name``) so
            ``execution.name`` matches the root span name; falls back to the
            same derivation when not supplied.
    """
    alog = async_logger or logger
    if span is None or not hasattr(span, "set_attribute"):
        return
    try:
        span.set_attribute(
            "execution.name", run_name or _derive_trace_run_name(config)
        )
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


def _derive_trace_run_name(
    config: dict,
    inputs: Optional[dict] = None,
    execution_id: Optional[str] = None,
) -> str:
    """Resolve a human-meaningful run name for the MLflow root span.

    Mirrors the ``execution_name`` precedence in crew_preparation.py so the
    trace root span matches the run name shown elsewhere, and falls back to the
    execution id so the span is never an opaque ``"Unnamed"`` when any
    identifier is available. ``run_name`` is frequently nested under
    ``inputs`` rather than at the top level, which is why the previous
    ``config.get("run_name")`` alone produced ``crew_kickoff:Unnamed``.
    """
    config_inputs = config.get("inputs") if isinstance(config.get("inputs"), dict) else {}
    arg_inputs = inputs if isinstance(inputs, dict) else {}
    for candidate in (
        config.get("run_name"),
        arg_inputs.get("run_name"),
        config_inputs.get("run_name"),
        config.get("crew_name"),
        config.get("name"),
        config.get("execution_id"),
        execution_id,
    ):
        if candidate and str(candidate).strip():
            return str(candidate).strip()
    return "Unnamed"


def execute_with_mlflow_trace(
    kickoff_fn: Callable[[], Any],
    mlflow_result: Optional[MlflowSetupResult],
    crew_config: dict,
    inputs: Optional[dict] = None,
    async_logger: Optional[logging.Logger] = None,
    execution_id: Optional[str] = None,
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

    run_name = _derive_trace_run_name(crew_config, inputs, execution_id)
    trace_name = f"crew_kickoff:{run_name}"
    trace_inputs = {**(inputs or {}), "run_name": run_name}

    with start_root_trace(trace_name, trace_inputs) as root_span:
        # Set execution attributes on the root span
        set_trace_attributes(root_span, crew_config, alog, run_name=run_name)

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
    trace_label: str = "flow_kickoff",
    execution_id: Optional[str] = None,
    **kickoff_kwargs: Any,
) -> Any:
    """Async variant: wraps an awaitable kickoff in an MLflow root trace.

    Args:
        kickoff_coro_fn: Async callable whose result we ``await``.
        mlflow_result: Result from ``configure_mlflow_in_subprocess``.
        flow_config: Execution configuration dict (used for trace attributes).
        inputs: Optional inputs dict for trace metadata.
        async_logger: Logger routed to the subprocess log file.
        trace_label: Prefix for the MLflow root-trace name. Defaults to
            ``"flow_kickoff"``; crew executors pass ``"crew_kickoff"`` so crew
            runs are not mislabeled as flows.
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

    run_name = _derive_trace_run_name(flow_config, inputs, execution_id)
    trace_name = f"{trace_label}:{run_name}"
    trace_inputs = {**(inputs or {}), "run_name": run_name}

    with start_root_trace(trace_name, trace_inputs) as root_span:
        set_trace_attributes(root_span, flow_config, alog, run_name=run_name)
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
        from src.engines.crewai.infra.mlflow_integration import (
            flush_and_stop_writers as _flush_stop,
        )
        await _flush_stop(async_logger=alog)
    except Exception as e:
        alog.warning(f"[SUBPROCESS] MLflow flush/stop error: {e}")
