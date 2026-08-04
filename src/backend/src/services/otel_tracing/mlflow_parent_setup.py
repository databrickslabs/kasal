"""Back-compat shim: parent-process MLflow tracing setup moved to ``services/mlflow/``.

See ``services/mlflow/mlflow_parent_setup.py``. This re-exports the moved names
so existing import paths (``otel_tracing.mlflow_parent_setup``) keep working,
including the private helpers (`_setup_sync`) tests import.

Prefer importing from ``src.services.mlflow.mlflow_parent_setup`` in new code.
"""

from src.services.mlflow.mlflow_parent_setup import *  # noqa: F401,F403
from src.services.mlflow.mlflow_parent_setup import (  # noqa: F401
    _group_cache_key,
    _setup_sync,
    configure_parent_mlflow_tracing,
    invalidate_parent_mlflow_cache,
    set_mlflow_tracing,
    set_root_span_outputs,
)
