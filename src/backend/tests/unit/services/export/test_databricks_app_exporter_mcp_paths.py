"""Which MCP URLs the Databricks App exporter treats as Databricks-managed.

A Databricks-managed server is emitted host-relative and pinned to
streamable-http; the exported app then gives it the app's Databricks identity
(``agent_server/agent.py`` ``_mcp_param``). Anything else is third-party: it
keeps its absolute URL and needs its own ``<NAME>_MCP_TOKEN``.

Unity Gateway MCP services (``/ai-gateway/mcp-services/...``, Genie One's GA
home among them) are Databricks-managed exactly like ``/api/2.0/mcp/...``.
"""

import pytest

from src.services.export.databricks_app_exporter import DatabricksAppExporter

HOST = "https://workspace.example.com"
GENIE_ONE = f"{HOST}/ai-gateway/mcp-services/system.ai.genie_one_mcp"


@pytest.fixture
def exporter():
    return DatabricksAppExporter()


@pytest.mark.parametrize(
    "url, relative",
    [
        (GENIE_ONE, "/ai-gateway/mcp-services/system.ai.genie_one_mcp"),
        (
            f"{HOST}/ai-gateway/mcp-services/main.gold.jira",
            "/ai-gateway/mcp-services/main.gold.jira",
        ),
        (f"{HOST}/api/2.0/mcp/genie/01ef", "/api/2.0/mcp/genie/01ef"),
        (f"{HOST}/api/2.0/mcp/sql", "/api/2.0/mcp/sql"),
    ],
)
def test_databricks_managed_urls_are_host_relative_and_streamable(
    exporter, url, relative
):
    # server_type "sse" proves the transport is pinned, not copied.
    block = exporter._mcp_block(
        [{"name": "managed", "server_url": url, "server_type": "sse"}]
    )
    assert block == f'    ("managed", "{relative}", "streamable-http"),\n'


def test_databricks_managed_urls_need_no_mcp_token(exporter):
    servers = [
        {"name": "Genie One", "server_url": GENIE_ONE},
        {"name": "Genie Space", "server_url": f"{HOST}/api/2.0/mcp/genie/01ef"},
    ]
    assert exporter._env_keys([], servers) == "# (none required for this crew)\n"


def test_third_party_urls_stay_absolute_and_need_a_token(exporter):
    server = {
        "name": "Acme Tools",
        "server_url": "https://acme.example.com/mcp",
        "server_type": "streamable",
    }
    assert exporter._mcp_block([server]) == (
        '    ("Acme Tools", "https://acme.example.com/mcp", "streamable-http"),\n'
    )
    assert exporter._env_keys([], [server]) == "# ACME_TOOLS_MCP_TOKEN=\n"


@pytest.mark.asyncio
async def test_exported_genie_one_app_uses_the_databricks_identity(exporter):
    crew = {
        "id": "crew-1",
        "name": "Genie Crew",
        "agents": [
            {
                "id": "a1",
                "name": "Analyst",
                "role": "Analyst",
                "goal": "Answer {question}",
                "backstory": "Knows the data.",
            }
        ],
        "tasks": [
            {
                "id": "t1",
                "name": "answer",
                "description": "Answer {question}.",
                "expected_output": "An answer.",
                "agent_id": "a1",
            }
        ],
        "mcp_servers": [
            {
                "name": "databricks genie: genie one",
                "server_url": GENIE_ONE,
                "server_type": "streamable",
            }
        ],
    }
    files = {
        f["path"]: f["content"] for f in (await exporter.export(crew, {}))["files"]
    }
    assert (
        '("databricks genie: genie one", '
        '"/ai-gateway/mcp-services/system.ai.genie_one_mcp", "streamable-http")'
    ) in files["agent_server/agent.py"]
    assert "GENIE_ONE_MCP_TOKEN" not in files[".env.example"]
