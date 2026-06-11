"""Tests for deploy.py - OAuth scopes configuration."""
import sys
import os
import json
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))

from deploy import configure_oauth_scopes


def test_scopes_do_not_include_catalog_volumes():
    """catalog.volumes should NOT be in the configured scopes."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"user_api_scopes": []}),
            stderr=""
        )
        configure_oauth_scopes("test-app")

        # Extract the JSON payload from the command
        call_args = mock_run.call_args[0][0]
        json_idx = call_args.index("--json") + 1
        payload = json.loads(call_args[json_idx])
        scopes = payload["user_api_scopes"]

        assert "catalog.volumes" not in scopes


def test_scopes_include_postgres():
    """postgres (Lakebase database instances) IS in the configured scopes."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"user_api_scopes": []}),
            stderr=""
        )
        configure_oauth_scopes("test-app")

        call_args = mock_run.call_args[0][0]
        json_idx = call_args.index("--json") + 1
        payload = json.loads(call_args[json_idx])
        scopes = payload["user_api_scopes"]

        assert "postgres" in scopes


def test_scopes_match_the_required_app_consent_set():
    """The deploy configures EXACTLY the current Apps user-authorization
    scope ids (the consent-screen names) — the older granular ids
    (sql.warehouses, vectorsearch.*, serving.serving-endpoints,
    dashboards.genie, files.files) were replaced by these."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"user_api_scopes": []}),
            stderr=""
        )
        configure_oauth_scopes("test-app")

        call_args = mock_run.call_args[0][0]
        json_idx = call_args.index("--json") + 1
        payload = json.loads(call_args[json_idx])
        scopes = payload["user_api_scopes"]

        assert sorted(scopes) == sorted([
            "sql",
            "genie",
            "postgres",
            "model-serving",
            "catalog.catalogs:read",
            "catalog.schemas:read",
            "catalog.tables:read",
            "catalog.connections",
            "files",
            "ai-gateway",
            "vector-search",
            "workspace.workspace",
            "mcp.external",
            "mcp.functions",
        ])


def test_scopes_use_current_ids_not_legacy_ones():
    """None of the replaced legacy scope ids sneak back in."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"user_api_scopes": []}),
            stderr=""
        )
        configure_oauth_scopes("test-app")

        call_args = mock_run.call_args[0][0]
        json_idx = call_args.index("--json") + 1
        payload = json.loads(call_args[json_idx])
        scopes = payload["user_api_scopes"]

        for legacy in (
            "dashboards.genie",
            "files.files",
            "serving.serving-endpoints",
            "sql.warehouses",
            "vectorsearch.vector-search-endpoints",
            "vectorsearch.vector-search-indexes",
        ):
            assert legacy not in scopes, f"Legacy scope id present: {legacy}"


def test_scopes_exclude_dataplane_by_default():
    """serving-endpoints-data-plane excluded by default."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"user_api_scopes": []}),
            stderr=""
        )
        configure_oauth_scopes("test-app", exclude_dataplane=True)

        call_args = mock_run.call_args[0][0]
        json_idx = call_args.index("--json") + 1
        payload = json.loads(call_args[json_idx])
        scopes = payload["user_api_scopes"]

        assert "serving.serving-endpoints-data-plane" not in scopes


def test_scopes_include_dataplane_when_requested():
    """serving-endpoints-data-plane included when exclude_dataplane=False."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"user_api_scopes": []}),
            stderr=""
        )
        configure_oauth_scopes("test-app", exclude_dataplane=False)

        call_args = mock_run.call_args[0][0]
        json_idx = call_args.index("--json") + 1
        payload = json.loads(call_args[json_idx])
        scopes = payload["user_api_scopes"]

        assert "serving.serving-endpoints-data-plane" in scopes


def test_returns_true_on_success():
    """configure_oauth_scopes returns True when command succeeds."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"user_api_scopes": ["sql"]}),
            stderr=""
        )
        result = configure_oauth_scopes("test-app")
        assert result is True


def test_returns_false_on_failure():
    """configure_oauth_scopes returns False when command fails."""
    with patch("deploy.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="scope error"
        )
        result = configure_oauth_scopes("test-app")
        assert result is False


def test_returns_false_on_exception():
    """configure_oauth_scopes returns False on unexpected exception."""
    with patch("deploy.subprocess.run", side_effect=Exception("boom")):
        result = configure_oauth_scopes("test-app")
        assert result is False


def test_error_logging_on_failure():
    """Errors are logged when command fails."""
    with patch("deploy.subprocess.run") as mock_run, \
         patch("deploy.logger") as mock_logger:
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="output text",
            stderr="error text"
        )
        configure_oauth_scopes("test-app")
        # Verify error logging was called
        mock_logger.error.assert_called()
