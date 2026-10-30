"""get_mcp_access_token runs the CLI off the event loop, with a timeout."""

import json
import subprocess
import threading
from unittest.mock import MagicMock, patch

import pytest

from src.utils import databricks_auth
from src.utils.databricks_mcp_cli import (
    _MCP_CLI_TIMEOUT_SECONDS,
    get_mcp_access_token,
)


def test_still_reachable_from_databricks_auth():
    assert databricks_auth.get_mcp_access_token is get_mcp_access_token


@pytest.mark.asyncio
async def test_cli_runs_off_the_event_loop_thread_with_a_timeout():
    loop_thread = threading.get_ident()
    seen = {}

    def fake_run(*args, **kwargs):
        seen["thread"] = threading.get_ident()
        seen["timeout"] = kwargs.get("timeout")
        return MagicMock(stdout=json.dumps({"access_token": "eyJabc"}))

    with patch("subprocess.run", side_effect=fake_run):
        token, error = await get_mcp_access_token()

    assert (token, error) == ("eyJabc", None)
    assert seen["thread"] != loop_thread
    assert seen["timeout"] == _MCP_CLI_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_hung_cli_times_out_instead_of_hanging():
    with patch(
        "subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="databricks", timeout=15),
    ):
        token, error = await get_mcp_access_token()

    assert token is None
    assert "timed out" in error
