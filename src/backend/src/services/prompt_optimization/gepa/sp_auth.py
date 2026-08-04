"""Back-compat shim: SP single-method auth now lives in the MLflow service layer.

MLflow is used across the app (tracing, evaluation, prompt registry, judges),
so the SP-token auth helper was consolidated into
``src/services/mlflow/sp_auth.py``. This module re-exports it so the existing
import path (``prompt_optimization.gepa.sp_auth``) keeps working.

Prefer importing from ``src.services.mlflow.sp_auth`` in new code.
"""

from src.services.mlflow.sp_auth import (  # noqa: F401
    SWAP_KEYS as _SWAP_KEYS,
    derive_sp_bearer as _derive_sp_bearer,
    single_auth_env,
    sp_single_auth,
)

__all__ = ["sp_single_auth", "single_auth_env", "_derive_sp_bearer", "_SWAP_KEYS"]
