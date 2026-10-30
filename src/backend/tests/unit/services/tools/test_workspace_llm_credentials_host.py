"""``workspace_llm_credentials`` fills in the run's Databricks credential only
for the run's own workspace.

The Power BI and converter task forms let a task set ``llm_workspace_url``.
With a token of its own that pair is used as given. Without one, the run's
credential (the user's OBO token or the teamspace PAT) may only go to the
workspace it was issued for.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.services.tools import async_bridge

RUN_AUTH = SimpleNamespace(workspace_url="https://ws.example.com/", token="run-token")


@pytest.fixture(autouse=True)
def _run_auth():
    with patch.object(async_bridge, "run_async_with_context", return_value=RUN_AUTH):
        with patch("src.utils.databricks_auth.get_auth_context", lambda: None):
            yield


def test_nothing_given_uses_the_run_workspace_and_credential():
    assert async_bridge.workspace_llm_credentials() == (
        "https://ws.example.com",
        "run-token",
    )


@pytest.mark.parametrize(
    "url", ["https://ws.example.com", "ws.example.com", "https://WS.example.com/"]
)
def test_the_run_workspace_gets_the_run_credential(url):
    assert async_bridge.workspace_llm_credentials(url) == (url, "run-token")


def test_another_host_does_not_get_the_run_credential():
    assert async_bridge.workspace_llm_credentials("https://other.example.net") == (
        "https://other.example.net",
        "",
    )


def test_a_url_with_its_own_token_is_used_as_given():
    """The pair is the one configured together: a task entry that moves the URL
    without its own token reaches here with no token, because the merge
    withholds the teamspace's (test_tool_config_override_reserved_keys)."""
    assert async_bridge.workspace_llm_credentials(
        "https://other.example.net", "own-token"
    ) == ("https://other.example.net", "own-token")
