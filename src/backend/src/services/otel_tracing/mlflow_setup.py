"""Back-compat shim: MLflow subprocess setup moved to ``services/mlflow/``.

``mlflow_setup`` and ``mlflow_parent_setup`` were MLflow-specific and now call
into ``services/mlflow`` (the experiment-name authority), so they live under
``services/mlflow/`` with the rest of the MLflow layer. This module re-exports
the moved names so every existing import path
(``otel_tracing.mlflow_setup``) keeps working — including the private helpers
(`_build_uc_trace_location`, `_derive_trace_run_name`, `_try_import_mlflow`)
some call sites and tests import.

Prefer importing from ``src.services.mlflow.mlflow_setup`` in new code.
"""

from src.services.mlflow.mlflow_setup import *  # noqa: F401,F403
from src.services.mlflow.mlflow_setup import (  # noqa: F401
    KASAL_TRACE_TABLE_PREFIX,
    KASAL_UC_EXPERIMENT_SUFFIX,
    MlflowSetupResult,
    _build_uc_trace_location,
    _derive_trace_run_name,
    _local_slug,
    _teamspace_name,
    _try_import_mlflow,
    capture_trace_and_update_execution,
    configure_mlflow_in_subprocess,
    disable_autologs_for_safety,
    execute_with_mlflow_trace,
    execute_with_mlflow_trace_async,
    extract_trace_outputs,
    log_mlflow_state,
    post_execution_mlflow_cleanup,
    set_trace_attributes,
    uc_experiment_name,
)
