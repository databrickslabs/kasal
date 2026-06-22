"""Tests for deploy.py — slow-deployment timeout handling.

Regression coverage for the case where the Databricks SDK waiter
(``waiter.result()``) gives up after its 20-minute default while the deployment
is still downloading source / building server-side. The deploy should poll the
deployment status instead of reporting a false failure.
"""
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))

from deploy import wait_for_deployment
from databricks.sdk.service.apps import AppDeploymentState


def _deployment(state, message=""):
    """Build a fake AppDeployment-like object with the given status state."""
    status = MagicMock()
    status.state = state
    status.message = message
    dep = MagicMock()
    dep.status = status
    return dep


def test_wait_for_deployment_succeeds():
    """A deployment that reaches SUCCEEDED returns True."""
    client = MagicMock()
    client.apps.get_deployment.return_value = _deployment(
        AppDeploymentState.SUCCEEDED, "done"
    )
    with patch("deploy.time.sleep"):
        assert wait_for_deployment(client, "kasal", "dep-1") is True


def test_wait_for_deployment_fails_on_failed_state():
    """A FAILED deployment returns False."""
    client = MagicMock()
    client.apps.get_deployment.return_value = _deployment(
        AppDeploymentState.FAILED, "boom"
    )
    with patch("deploy.time.sleep"):
        assert wait_for_deployment(client, "kasal", "dep-1") is False


def test_wait_for_deployment_fails_on_cancelled_state():
    """A CANCELLED deployment returns False."""
    client = MagicMock()
    client.apps.get_deployment.return_value = _deployment(
        AppDeploymentState.CANCELLED
    )
    with patch("deploy.time.sleep"):
        assert wait_for_deployment(client, "kasal", "dep-1") is False


def test_wait_for_deployment_polls_until_terminal():
    """IN_PROGRESS states are tolerated until a terminal state is reached."""
    client = MagicMock()
    client.apps.get_deployment.side_effect = [
        _deployment(AppDeploymentState.IN_PROGRESS),
        _deployment(AppDeploymentState.IN_PROGRESS),
        _deployment(AppDeploymentState.SUCCEEDED),
    ]
    with patch("deploy.time.sleep"):
        assert wait_for_deployment(client, "kasal", "dep-1") is True
    assert client.apps.get_deployment.call_count == 3


def test_wait_for_deployment_survives_transient_poll_errors():
    """A transient error while polling is retried, not treated as failure."""
    client = MagicMock()
    client.apps.get_deployment.side_effect = [
        RuntimeError("transient network blip"),
        _deployment(AppDeploymentState.SUCCEEDED),
    ]
    with patch("deploy.time.sleep"):
        assert wait_for_deployment(client, "kasal", "dep-1") is True


def test_wait_for_deployment_times_out():
    """If no terminal state is reached before the deadline, returns False."""
    client = MagicMock()
    client.apps.get_deployment.return_value = _deployment(
        AppDeploymentState.IN_PROGRESS
    )
    # timeout_seconds=0 → the deadline has already passed, so the loop never
    # runs and the function reports the non-terminal deployment as a failure.
    with patch("deploy.time.sleep"):
        assert wait_for_deployment(
            client, "kasal", "dep-1", timeout_seconds=0
        ) is False
