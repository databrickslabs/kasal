"""
Unit tests for MLflowScopeErrorHandler.

Tests the scope error detection, fallback authentication handling,
and convenience wrapper for MLflow operations with OAuth scope errors.
"""
import os
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


from src.services.mlflow_scope_error_handler import (
    is_mlflow_scope_error,
    MLflowScopeErrorHandler,
    with_scope_error_fallback,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_auth_ctx(
    auth_method: str = "obo",
    workspace_url: str = "https://example.com",
    token: str = "tok-obo-123",
) -> SimpleNamespace:
    """Create a lightweight auth context mock."""
    return SimpleNamespace(
        auth_method=auth_method,
        workspace_url=workspace_url,
        token=token,
    )


def _scope_error(msg: str = "INVALID SCOPE") -> Exception:
    """Return an exception that passes the scope-error check."""
    return Exception(msg)


# ===========================================================================
# is_mlflow_scope_error
# ===========================================================================


class TestIsMlflowScopeError:
    """Tests for the is_mlflow_scope_error detection function."""

    @pytest.mark.parametrize(
        "message",
        [
            "does not have required scopes",
            "Token does not have required scopes for this operation",
            "required scopes are missing for MLflow",
            "insufficient scopes to perform this action",
            "missing scopes in the provided token",
            "invalid scope",
            "'invalid scope'",
            "Error: INVALID SCOPE returned by MLflow API",
        ],
    )
    def test_returns_true_for_scope_error_messages(self, message: str) -> None:
        """Scope-related error strings must be detected regardless of case."""
        assert is_mlflow_scope_error(Exception(message)) is True

    @pytest.mark.parametrize(
        "message",
        [
            "Connection refused",
            "404 Not Found",
            "Internal server error",
            "RESOURCE_DOES_NOT_EXIST",
            "Permission denied",
            "Unauthorized",
            "",
        ],
    )
    def test_returns_false_for_non_scope_errors(self, message: str) -> None:
        """Non-scope exceptions must not be misidentified."""
        assert is_mlflow_scope_error(Exception(message)) is False


# ===========================================================================
# MLflowScopeErrorHandler.handle_and_retry
# ===========================================================================


class TestHandleAndRetry:
    """Tests for the handle_and_retry method."""

    def test_reraises_non_scope_error(self) -> None:
        """Non-scope errors must propagate immediately without fallback."""
        auth_ctx = _make_auth_ctx()
        handler = MLflowScopeErrorHandler(auth_ctx)
        error = Exception("Connection refused")

        with pytest.raises(Exception, match="Connection refused"):
            handler.handle_and_retry(error, lambda: None, "test-op")

    def test_reraises_when_no_auth_context(self) -> None:
        """Scope errors with no auth context cannot fall back."""
        handler = MLflowScopeErrorHandler(auth_ctx=None)
        error = _scope_error()

        with pytest.raises(Exception, match="INVALID SCOPE"):
            handler.handle_and_retry(error, lambda: None, "test-op")

    def test_reraises_when_auth_method_is_not_obo(self) -> None:
        """When already using PAT/SPN there is no further fallback."""
        auth_ctx = _make_auth_ctx(auth_method="pat")
        handler = MLflowScopeErrorHandler(auth_ctx)
        error = _scope_error()

        with pytest.raises(Exception, match="INVALID SCOPE"):
            handler.handle_and_retry(error, lambda: None, "test-op")

    def test_reraises_when_fallback_already_applied(self) -> None:
        """A second scope error in the same handler must not retry again."""
        auth_ctx = _make_auth_ctx()
        handler = MLflowScopeErrorHandler(auth_ctx)
        handler._fallback_applied = True
        error = _scope_error()

        with pytest.raises(Exception, match="INVALID SCOPE"):
            handler.handle_and_retry(error, lambda: None, "test-op")

    @patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock)
    def test_reraises_when_fallback_returns_none(self, mock_get_auth: AsyncMock) -> None:
        """If get_auth_context returns None the original error propagates."""
        mock_get_auth.return_value = None

        auth_ctx = _make_auth_ctx()
        handler = MLflowScopeErrorHandler(auth_ctx)
        error = _scope_error()

        with pytest.raises(Exception, match="INVALID SCOPE"):
            handler.handle_and_retry(error, lambda: None, "test-op")

    @patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock)
    def test_reraises_when_fallback_gives_same_auth_method(
        self, mock_get_auth: AsyncMock
    ) -> None:
        """Fallback returning the same auth method would loop; must re-raise."""
        fallback_ctx = _make_auth_ctx(auth_method="obo", token="tok-other")
        mock_get_auth.return_value = fallback_ctx

        auth_ctx = _make_auth_ctx()
        handler = MLflowScopeErrorHandler(auth_ctx)
        error = _scope_error()

        with pytest.raises(Exception, match="INVALID SCOPE"):
            handler.handle_and_retry(error, lambda: None, "test-op")

    @patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock)
    def test_succeeds_with_valid_fallback(self, mock_get_auth: AsyncMock) -> None:
        """Successful fallback must apply credentials and return retry result."""
        fallback_ctx = _make_auth_ctx(
            auth_method="pat",
            workspace_url="https://example.com/ws",
            token="tok-pat-456",
        )
        mock_get_auth.return_value = fallback_ctx

        auth_ctx = _make_auth_ctx()
        handler = MLflowScopeErrorHandler(auth_ctx)
        error = _scope_error()

        retry_func = MagicMock(return_value="success-value")

        with patch.object(handler, "_apply_fallback_credentials") as mock_apply:
            result = handler.handle_and_retry(error, retry_func, "test-op")

        assert result == "success-value"
        retry_func.assert_called_once()
        mock_apply.assert_called_once_with(fallback_ctx)
        assert handler._fallback_applied is True
        assert handler.auth_ctx is fallback_ctx

    @patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock)
    def test_reraises_when_retry_fails_after_fallback(
        self, mock_get_auth: AsyncMock
    ) -> None:
        """If the retry itself fails, its error must propagate."""
        fallback_ctx = _make_auth_ctx(auth_method="pat")
        mock_get_auth.return_value = fallback_ctx

        auth_ctx = _make_auth_ctx()
        handler = MLflowScopeErrorHandler(auth_ctx)
        error = _scope_error()

        retry_error = RuntimeError("MLflow still broken")
        retry_func = MagicMock(side_effect=retry_error)

        with patch.object(handler, "_apply_fallback_credentials"):
            with pytest.raises(RuntimeError, match="MLflow still broken"):
                handler.handle_and_retry(error, retry_func, "test-op")


# ===========================================================================
# MLflowScopeErrorHandler._apply_fallback_credentials
# ===========================================================================


class TestApplyFallbackCredentials:
    """Tests for the _apply_fallback_credentials method."""

    def test_sets_env_vars_with_api_base(self) -> None:
        """Environment variables must be populated when api_base is available."""
        fallback_ctx = _make_auth_ctx(
            auth_method="pat",
            workspace_url="https://example.com/ws",
            token="tok-pat-789",
        )
        mock_url_utils = MagicMock()
        mock_url_utils.construct_serving_endpoints_url.return_value = (
            "https://example.com/serving-endpoints"
        )
        mock_mlflow = MagicMock()

        handler = MLflowScopeErrorHandler()

        env_snapshot = {
            "DATABRICKS_HOST": "",
            "DATABRICKS_TOKEN": "",
            "DATABRICKS_BASE_URL": "",
            "DATABRICKS_API_BASE": "",
            "DATABRICKS_ENDPOINT": "",
        }

        with patch.dict(os.environ, env_snapshot, clear=False), \
             patch.dict("sys.modules", {"mlflow": mock_mlflow}), \
             patch(
                 "src.utils.databricks_url_utils.DatabricksURLUtils",
                 mock_url_utils,
             ):
            handler._apply_fallback_credentials(fallback_ctx)

            assert os.environ["DATABRICKS_HOST"] == "https://example.com/ws"
            assert os.environ["DATABRICKS_TOKEN"] == "tok-pat-789"
            assert os.environ["DATABRICKS_BASE_URL"] == "https://example.com/serving-endpoints"
            assert os.environ["DATABRICKS_API_BASE"] == "https://example.com/serving-endpoints"
            assert os.environ["DATABRICKS_ENDPOINT"] == "https://example.com/serving-endpoints"

        mock_mlflow.set_tracking_uri.assert_called_once_with("databricks")

    def test_skips_api_base_env_vars_when_empty(self) -> None:
        """When construct_serving_endpoints_url returns None, base URLs stay unset."""
        fallback_ctx = _make_auth_ctx(
            auth_method="spn",
            workspace_url="https://example.com/ws2",
            token="tok-spn-000",
        )
        mock_url_utils = MagicMock()
        mock_url_utils.construct_serving_endpoints_url.return_value = None
        mock_mlflow = MagicMock()

        handler = MLflowScopeErrorHandler()

        clean_env = {
            k: ""
            for k in [
                "DATABRICKS_HOST",
                "DATABRICKS_TOKEN",
                "DATABRICKS_BASE_URL",
                "DATABRICKS_API_BASE",
                "DATABRICKS_ENDPOINT",
            ]
        }

        with patch.dict(os.environ, clean_env, clear=False), \
             patch.dict("sys.modules", {"mlflow": mock_mlflow}), \
             patch(
                 "src.utils.databricks_url_utils.DatabricksURLUtils",
                 mock_url_utils,
             ):
            handler._apply_fallback_credentials(fallback_ctx)

            assert os.environ["DATABRICKS_HOST"] == "https://example.com/ws2"
            assert os.environ["DATABRICKS_TOKEN"] == "tok-spn-000"
            # These must remain at their clean-env value (empty string) because
            # api_base resolved to None -> "" which is falsy.
            assert os.environ["DATABRICKS_BASE_URL"] == ""
            assert os.environ["DATABRICKS_API_BASE"] == ""
            assert os.environ["DATABRICKS_ENDPOINT"] == ""

        mock_mlflow.set_tracking_uri.assert_called_once_with("databricks")


# ===========================================================================
# with_scope_error_fallback (convenience wrapper)
# ===========================================================================


class TestWithScopeErrorFallback:
    """Tests for the top-level convenience wrapper."""

    def test_returns_result_on_first_try(self) -> None:
        """When the operation succeeds, its result is returned immediately."""
        auth_ctx = _make_auth_ctx()
        operation = MagicMock(return_value="first-try-result")

        result = with_scope_error_fallback(auth_ctx, operation, "test-op")

        assert result == "first-try-result"
        operation.assert_called_once()

    @patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock)
    def test_handles_scope_error_and_retries(self, mock_get_auth: AsyncMock) -> None:
        """Scope error on first call triggers fallback then successful retry."""
        fallback_ctx = _make_auth_ctx(auth_method="pat")
        mock_get_auth.return_value = fallback_ctx

        auth_ctx = _make_auth_ctx()

        call_count = 0

        def flaky_operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("invalid scope")
            return "retry-result"

        with patch(
            "src.services.mlflow_scope_error_handler.MLflowScopeErrorHandler._apply_fallback_credentials"
        ):
            result = with_scope_error_fallback(auth_ctx, flaky_operation, "test-op")

        assert result == "retry-result"
        assert call_count == 2

    def test_propagates_non_scope_error(self) -> None:
        """Non-scope exceptions must not be swallowed."""
        auth_ctx = _make_auth_ctx()
        operation = MagicMock(side_effect=RuntimeError("unrelated failure"))

        with pytest.raises(RuntimeError, match="unrelated failure"):
            with_scope_error_fallback(auth_ctx, operation, "test-op")

    def test_works_with_none_auth_context(self) -> None:
        """When auth_ctx is None and operation succeeds, it should return normally."""
        operation = MagicMock(return_value="ok")

        result = with_scope_error_fallback(None, operation, "test-op")

        assert result == "ok"
        operation.assert_called_once()

    def test_scope_error_with_none_auth_context_reraises(self) -> None:
        """Scope error with no auth context falls through to re-raise."""
        operation = MagicMock(side_effect=Exception("invalid scope"))

        with pytest.raises(Exception, match="invalid scope"):
            with_scope_error_fallback(None, operation, "test-op")
