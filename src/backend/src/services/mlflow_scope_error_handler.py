"""
MLflow Scope Error Handler Utility

Handles OAuth scope errors when OBO tokens lack MLflow permissions.
Automatically falls back to PAT/SPN authentication when scope errors are detected.

Usage:
    handler = MLflowScopeErrorHandler(auth_ctx)

    try:
        result = mlflow.some_operation()
    except Exception as e:
        result = handler.handle_and_retry(e, lambda: mlflow.some_operation())
"""

import logging
import os
import asyncio
from typing import Optional, Callable, Any

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().system


def is_mlflow_scope_error(error: Exception) -> bool:
    """
    Check if an error is due to missing OAuth scopes for MLflow operations.

    This is specific to MLflow API errors and includes patterns seen in MLflow 403 responses.

    Args:
        error: Exception to check

    Returns:
        True if error is due to missing OAuth scopes for MLflow
    """
    error_str = str(error).lower()
    return any(phrase in error_str for phrase in [
        "does not have required scopes",
        "required scopes",
        "insufficient scopes",
        "missing scopes",
        "invalid scope",  # MLflow API returns this in 403 responses
        "'invalid scope'",  # Quoted version from MLflow response body
    ])


class MLflowScopeErrorHandler:
    """
    Utility for handling MLflow OAuth scope errors with automatic PAT/SPN fallback.

    When an OBO token lacks required scopes for MLflow operations, this handler:
    1. Detects scope errors using is_scope_error()
    2. Falls back to PAT/SPN authentication
    3. Updates environment variables
    4. Retries the operation
    """

    def __init__(self, auth_ctx: Optional[Any] = None):
        """
        Initialize scope error handler.

        Args:
            auth_ctx: Current authentication context (from get_auth_context)
        """
        self.auth_ctx = auth_ctx
        self._fallback_applied = False

    def handle_and_retry(
        self,
        error: Exception,
        retry_func: Callable[[], Any],
        operation_name: str = "MLflow operation"
    ) -> Any:
        """
        Handle scope error and retry operation with PAT/SPN fallback.

        Args:
            error: The exception that was raised
            retry_func: Function to retry after fallback (no arguments)
            operation_name: Name of operation for logging

        Returns:
            Result of retry_func if fallback succeeds

        Raises:
            Original exception if not a scope error or fallback fails
        """
        from src.utils.databricks_auth import get_auth_context

        # Check if this is a scope error and we're using OBO
        if not is_mlflow_scope_error(error):
            logger.debug(f"[MLflowScopeErrorHandler] Not a MLflow scope error, re-raising: {error}")
            raise error

        if not self.auth_ctx:
            logger.debug(f"[MLflowScopeErrorHandler] No auth context, re-raising: {error}")
            raise error

        if self.auth_ctx.auth_method != "obo":
            # Already using PAT/SPN - can't fallback further
            logger.error(
                f"[MLflowScopeErrorHandler] Already using {self.auth_ctx.auth_method} auth but still getting scope error. "
                f"The {self.auth_ctx.auth_method.upper()} token/credential lacks required MLflow permissions: {error}"
            )
            raise error

        # Check if fallback was already applied in this handler instance
        if self._fallback_applied:
            logger.error(
                f"[MLflowScopeErrorHandler] Fallback already applied for this handler but {operation_name} still failing: {error}"
            )
            raise error

        # Log the scope error and initiate fallback
        logger.warning(
            f"[MLflowScopeErrorHandler] OBO token lacks MLflow scopes for {operation_name}, "
            f"falling back to PAT/SPN: {error}"
        )

        # Get fallback authentication (PAT or SPN)
        auth_fallback = asyncio.run(get_auth_context(user_token=None))

        if not auth_fallback:
            logger.error(f"[MLflowScopeErrorHandler] PAT/SPN fallback failed for {operation_name}")
            raise error

        # Check if fallback gives us the same auth method (would cause infinite loop)
        if auth_fallback.auth_method == self.auth_ctx.auth_method:
            logger.error(
                f"[MLflowScopeErrorHandler] Fallback returned same auth method '{auth_fallback.auth_method}', "
                f"cannot fallback further"
            )
            raise error

        # Update environment variables with fallback credentials
        self._apply_fallback_credentials(auth_fallback)
        self._fallback_applied = True

        # Update our auth_ctx to reflect the fallback (for future calls)
        self.auth_ctx = auth_fallback

        # Retry the operation
        logger.info(
            f"[MLflowScopeErrorHandler] Successfully applied {auth_fallback.auth_method} "
            f"fallback, retrying {operation_name}"
        )

        try:
            return retry_func()
        except Exception as retry_error:
            logger.error(
                f"[MLflowScopeErrorHandler] Operation failed even after fallback: {retry_error}"
            )
            raise retry_error

    def _apply_fallback_credentials(self, auth_fallback: Any) -> None:
        """
        Update environment variables with fallback credentials.

        Args:
            auth_fallback: Fallback authentication context
        """
        import mlflow
        from src.utils.databricks_url_utils import DatabricksURLUtils

        # Update Databricks credentials
        os.environ["DATABRICKS_HOST"] = auth_fallback.workspace_url
        os.environ["DATABRICKS_TOKEN"] = auth_fallback.token

        # Update API base URLs for consistency
        api_base = DatabricksURLUtils.construct_serving_endpoints_url(
            auth_fallback.workspace_url
        ) or ""

        if api_base:
            os.environ["DATABRICKS_BASE_URL"] = api_base
            os.environ["DATABRICKS_API_BASE"] = api_base
            os.environ["DATABRICKS_ENDPOINT"] = api_base

        # Ensure MLflow uses the new credentials
        mlflow.set_tracking_uri("databricks")

        logger.info(
            f"[MLflowScopeErrorHandler] Environment variables updated with "
            f"{auth_fallback.auth_method} credentials"
        )


def with_scope_error_fallback(
    auth_ctx: Optional[Any],
    operation_func: Callable[[], Any],
    operation_name: str = "MLflow operation"
) -> Any:
    """
    Convenience function to wrap MLflow operations with scope error handling.

    Example:
        result = with_scope_error_fallback(
            auth_ctx,
            lambda: mlflow.set_experiment(exp_name),
            "set_experiment"
        )

    Args:
        auth_ctx: Current authentication context
        operation_func: Function to execute (no arguments)
        operation_name: Name for logging

    Returns:
        Result of operation_func

    Raises:
        Exception if operation fails after fallback attempt
    """
    handler = MLflowScopeErrorHandler(auth_ctx)

    try:
        return operation_func()
    except Exception as e:
        return handler.handle_and_retry(e, operation_func, operation_name)
