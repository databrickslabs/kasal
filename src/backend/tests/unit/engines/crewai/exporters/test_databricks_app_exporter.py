"""
Unit tests for the template-driven Databricks App exporter.

The exporter renders a CrewAI-adapted copy of Databricks' official agent-app
template (bundled under exporters/templates/databricks_app) and injects the
crew's config. These tests assert the rendered project is structurally faithful,
fully substituted, valid, and CrewAI-based (no OpenAI Agents SDK leakage).
"""

import ast
import re

import pytest
import yaml

from src.engines.crewai.exporters.databricks_app_exporter import DatabricksAppExporter

# Our placeholder tokens are uppercase {{TOKEN}}; GitHub Actions ${{ vars.X }}
# expressions in deploy.yml are intentionally left untouched.
_TOKEN_RE = re.compile(r"\{\{[A-Z_]+\}\}")


@pytest.fixture
def exporter():
    return DatabricksAppExporter()


@pytest.fixture
def crew_data():
    return {
        "id": "crew-123",
        "name": "Research Crew",
        "agents": [
            {
                "id": "a1",
                "name": "Researcher",
                "role": "Senior Researcher",
                "goal": "Research {topic} comprehensively",
                "backstory": "Expert researcher.",
                "llm": "databricks-claude-sonnet-4-5",
                "tools": ["SerperDevTool"],
            }
        ],
        "tasks": [
            {
                "id": "t1",
                "name": "Research Task",
                "description": "Research {topic}.",
                "expected_output": "A report on {topic}.",
                "agent_id": "a1",
                "tools": [],
            }
        ],
        "mcp_servers": [
            {
                "name": "genie space",
                "server_url": "https://x.databricks.com/api/2.0/mcp/genie/01ef",
                "server_type": "streamable",
            }
        ],
    }


def _files(result):
    return {f["path"]: f["content"] for f in result["files"]}


class TestDatabricksAppExporter:
    @pytest.mark.asyncio
    async def test_returns_expected_structure(self, exporter, crew_data):
        result = await exporter.export(crew_data, {})
        assert result["crew_id"] == "crew-123"
        assert result["crew_name"] == "Research Crew"
        assert result["export_format"] == "databricks_app"
        assert result["files"] and result["metadata"]
        assert "generated_at" in result and "size_bytes" in result

    @pytest.mark.asyncio
    async def test_reference_tree_present(self, exporter, crew_data):
        """The export ships the deploy-critical skeleton + generated config."""
        files = _files(await exporter.export(crew_data, {}))
        for required in [
            "app.yaml",
            "databricks.yml",
            "pyproject.toml",
            "README.md",
            ".gitignore",
            ".env.example",
            "agent_server/agent.py",
            "agent_server/conversation.py",
            "agent_server/start_server.py",
            "agent_server/utils.py",
            "agent_server/otel.py",
            "scripts/start_app.py",
            "config/agents.yaml",
            "config/tasks.yaml",
        ]:
            assert required in files, f"missing {required}"

    @pytest.mark.asyncio
    async def test_cruft_not_shipped(self, exporter, crew_data):
        """AI-assistant guides, gallery manifest, skills, and CI are dropped."""
        files = _files(await exporter.export(crew_data, {}))
        for absent in [
            "AGENTS.md",
            "CLAUDE.md",
            "manifest.yaml",
            ".github/workflows/deploy.yml",
            ".claude/skills/deploy/SKILL.md",
        ]:
            assert absent not in files, f"unexpected file shipped: {absent}"
        assert not any(p.startswith(".claude/") for p in files)

    @pytest.mark.asyncio
    async def test_no_leftover_tokens(self, exporter, crew_data):
        for f in (await exporter.export(crew_data, {}))["files"]:
            leftover = _TOKEN_RE.search(f["content"])
            assert not leftover, f"unrendered token {leftover.group()} in {f['path']}"

    @pytest.mark.asyncio
    async def test_rendered_python_is_valid(self, exporter, crew_data):
        for f in (await exporter.export(crew_data, {}))["files"]:
            if f["path"].endswith(".py"):
                ast.parse(f["content"], filename=f["path"])

    @pytest.mark.asyncio
    async def test_rendered_yaml_is_valid(self, exporter, crew_data):
        for f in (await exporter.export(crew_data, {}))["files"]:
            if f["path"].endswith((".yaml", ".yml")):
                yaml.safe_load(f["content"])

    @pytest.mark.asyncio
    async def test_agent_is_crewai_not_openai(self, exporter, crew_data):
        """agent.py wraps CrewAI behind ResponsesAgent; no OpenAI Agents SDK leaks."""
        files = _files(await exporter.export(crew_data, {}))
        agent = files["agent_server/agent.py"]
        assert "crew.kickoff" in agent
        assert "from crewai import" in agent
        for banned in (
            "databricks_openai",
            "openai-agents",
            "from agents",
            "McpServer",
        ):
            assert banned not in agent
        pyproject = files["pyproject.toml"]
        assert "crewai" in pyproject
        assert "openai-agents" not in pyproject and "databricks-openai" not in pyproject

    @pytest.mark.asyncio
    async def test_start_server_skips_version_tracking(self, exporter, crew_data):
        """Git-based version tracking is NOT enabled — with no git it makes a junk
        '<name>-no-git' LoggedModel and links traces to it. Traces flow to the
        experiment via autolog + @mlflow.trace instead."""
        start_server = _files(await exporter.export(crew_data, {}))[
            "agent_server/start_server.py"
        ]
        # Not imported (only AgentServer) and explicitly documented as skipped.
        assert "from mlflow.genai.agent_server import AgentServer\n" in start_server
        assert "intentionally do NOT call" in start_server
        # OTel export + AgentServer are still wired.
        assert "setup_otel_logging()" in start_server
        assert "AgentServer(" in start_server

    @pytest.mark.asyncio
    async def test_execution_settings_default_sequential(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert 'PROCESS = "sequential"' in agent
        assert "PLANNING = False" in agent
        assert "REASONING = False" in agent
        # Memory stays off until a Databricks-backed backend is wired (avoids
        # CrewAI's default OpenAI embedder/extraction).
        assert "MEMORY = False" in agent

    @pytest.mark.asyncio
    async def test_hierarchical_planning_reasoning_honored(self, exporter, crew_data):
        crew = dict(
            crew_data,
            process="hierarchical",
            planning=True,
            planning_llm="databricks-claude-sonnet-4-5",
            reasoning=True,
            manager_llm="databricks-llama-4-maverick",
            memory=False,
        )
        agent = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        assert 'PROCESS = "hierarchical"' in agent
        assert "PLANNING = True" in agent
        assert "PLANNING_LLM = 'databricks-claude-sonnet-4-5'" in agent
        assert "REASONING = True" in agent
        assert "MANAGER_LLM = 'databricks-llama-4-maverick'" in agent
        assert "MEMORY = False" in agent
        # build_crew wires hierarchical manager + planning; agents get reasoning.
        assert "Process.hierarchical" in agent
        assert "manager_llm" in agent
        assert 'reasoning=cfg.get("reasoning", REASONING)' in agent

    @pytest.mark.asyncio
    async def test_kickoff_offloaded_to_thread(self, exporter, crew_data):
        """Sync work must run off the event loop (CrewAI refuses on a live loop)."""
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "import asyncio" in agent
        assert "asyncio.to_thread(" in agent
        assert "conversation.respond" in agent

    @pytest.mark.asyncio
    async def test_supervisor_delegation_over_existing_agents(
        self, exporter, crew_data
    ):
        """The 'act' step delegates to existing agents via a hierarchical supervisor."""
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "_run_supervised" in agent
        assert "_build_supervised_crew" in agent
        # Supervisor agent delegates to the existing agents (its coworkers) via
        # allow_delegation; avoids the hierarchical autolog/coworker bugs.
        assert "allow_delegation=True" in agent
        assert "[supervisor, *workers.values()]" in agent
        # The conversation layer is wired to the supervisor runner, not the
        # predefined-task runner.
        assert "crew_runner=_run_supervised" in agent

    @pytest.mark.asyncio
    async def test_otel_uc_log_export_present(self, exporter, crew_data):
        """App ships OTel->Unity Catalog log export, wired at startup."""
        files = _files(await exporter.export(crew_data, {}))
        assert "agent_server/otel.py" in files
        otel = files["agent_server/otel.py"]
        assert "OTEL_EXPORTER_OTLP_ENDPOINT" in otel and "otel_logs" in otel
        assert "OTLPLogExporter" in otel and "LoggingHandler" in otel
        # Started from start_server.py.
        start_server = files["agent_server/start_server.py"]
        assert "setup_otel_logging()" in start_server
        # The OTLP exporter dependency is shipped.
        assert "opentelemetry-exporter-otlp" in files["pyproject.toml"]

    @pytest.mark.asyncio
    async def test_every_turn_is_traced(self, exporter, crew_data):
        """Each conversation turn is traced (so gather-only turns write to MLflow)."""
        files = _files(await exporter.export(crew_data, {}))
        assert "@mlflow.trace" in files["agent_server/conversation.py"]
        # Tracing setup is logged (not silent) so misconfig is visible in app logs.
        assert "MLFLOW_EXPERIMENT_ID is not set" in files["agent_server/agent.py"]

    @pytest.mark.asyncio
    async def test_clarify_and_resume_loop(self, exporter, crew_data):
        """Supervisor can ask for clarification; the loop resumes with context."""
        files = _files(await exporter.export(crew_data, {}))
        agent = files["agent_server/agent.py"]
        conv = files["agent_server/conversation.py"]
        # Supervisor is told to ask instead of guess.
        assert "CLARIFY:" in agent
        # Conversation detects CLARIFY and passes prior context so a follow-up resumes.
        assert "CLARIFY:" in conv
        assert "history[-6:]" in conv

    @pytest.mark.asyncio
    async def test_conversation_layer_present(self, exporter, crew_data):
        """Every app ships the generic conversation layer and wires it to the crew."""
        files = _files(await exporter.export(crew_data, {}))
        assert "agent_server/conversation.py" in files
        conv = files["agent_server/conversation.py"]
        # Single-pass router (no CrewAI Flow, which looped/re-entered in-server).
        assert "def respond" in conv and "def configure" in conv
        assert "def _classify" in conv and "def _gather" in conv
        assert "class ChatFlow" not in conv and "crewai.flow" not in conv
        agent = files["agent_server/agent.py"]
        assert "conversation.configure(" in agent
        assert "CREW_PURPOSE" in agent
        # Purpose is synthesized from the crew (here, the research task text).
        assert "Research" in agent

    @pytest.mark.asyncio
    async def test_deploy_env_threaded_into_app_yaml(self, exporter, crew_data):
        """Experiment id + catalog/schema selections land in app.yaml env."""
        opts = {
            "experiment_id": "1234567890",
            "databricks_catalog": "main",
            "databricks_schema": "agents",
        }
        app_yaml = _files(await exporter.export(crew_data, opts))["app.yaml"]
        assert "MLFLOW_EXPERIMENT_ID" in app_yaml and "1234567890" in app_yaml
        assert "DATABRICKS_CATALOG" in app_yaml and "main" in app_yaml
        assert "DATABRICKS_SCHEMA" in app_yaml and "agents" in app_yaml

    @pytest.mark.asyncio
    async def test_catalog_schema_default_from_crew_data(self, exporter, crew_data):
        """Plain export falls back to the workspace's configured catalog/schema."""
        crew = dict(crew_data, databricks_catalog="uc_cat", databricks_schema="uc_sch")
        app_yaml = _files(await exporter.export(crew, {}))["app.yaml"]
        assert "uc_cat" in app_yaml and "uc_sch" in app_yaml

    @pytest.mark.asyncio
    async def test_litellm_dependency_present(self, exporter, crew_data):
        """CrewAI needs LiteLLM to talk to databricks/ models — it must be shipped."""
        pyproject = _files(await exporter.export(crew_data, {}))["pyproject.toml"]
        assert "litellm" in pyproject

    @pytest.mark.asyncio
    async def test_codex_model_uses_responses_api(self, exporter, crew_data):
        """gpt-5-3-codex is routed via the Databricks Responses API, not chat."""
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "_is_codex_model" in agent
        assert "gpt-5-3-codex" in agent
        assert 'api="responses"' in agent
        assert "OpenAICompletion" in agent

    @pytest.mark.asyncio
    async def test_app_yaml_uses_uv_start_app(self, exporter, crew_data):
        """The faithful template runs via `uv run start-app`, not raw uvicorn."""
        app_yaml = _files(await exporter.export(crew_data, {}))["app.yaml"]
        assert "start-app" in app_yaml and "uv" in app_yaml

    @pytest.mark.asyncio
    async def test_metadata_counts_and_names(self, exporter, crew_data):
        meta = (await exporter.export(crew_data, {}))["metadata"]
        assert meta["agents_count"] == 1
        assert meta["tasks_count"] == 1
        assert meta["sanitized_name"] == "research_crew"
        assert meta["app_name"] == "research-crew"
        assert meta["bundle_name"] == "research_crew"
        assert meta["input_key"] == "topic"

    @pytest.mark.asyncio
    async def test_input_key_detected_from_placeholder(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "INPUT_KEY = 'topic'" in agent

    @pytest.mark.asyncio
    async def test_mcp_servers_rendered_as_relative_path(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert '("genie space", "/api/2.0/mcp/genie/01ef")' in agent

    @pytest.mark.asyncio
    async def test_mcp_setup_is_best_effort(self, exporter, crew_data):
        """MCP wiring is wrapped so a missing package / bad server doesn't 500."""
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "MCPServerAdapter" in agent
        assert "running crew without MCP" in agent  # the fallback log
        # The adapter use sits inside a try/except in _run_crew.
        start = agent.index("def _run_crew")
        run_crew = agent[start : agent.index("def _conversation_model")]
        assert "try:" in run_crew and "except Exception" in run_crew

    @pytest.mark.asyncio
    async def test_mcp_package_dep_added_when_mcp_servers(self, exporter, crew_data):
        """When MCP servers are configured, `mcp` is a dependency so the adapter
        never tries to interactively prompt-install it (which aborts in a server)."""
        with_mcp = _files(await exporter.export(crew_data, {}))["pyproject.toml"]
        assert "mcp>=" in with_mcp

        no_mcp_crew = dict(crew_data, mcp_servers=[])
        without = _files(await exporter.export(no_mcp_crew, {}))["pyproject.toml"]
        assert "mcp>=" not in without

    @pytest.mark.asyncio
    async def test_obo_toggle(self, exporter, crew_data):
        on = _files(await exporter.export(crew_data, {"include_obo_auth": True}))
        off = _files(await exporter.export(crew_data, {"include_obo_auth": False}))
        assert "ENABLE_OBO = True" in on["agent_server/agent.py"]
        assert "ENABLE_OBO = False" in off["agent_server/agent.py"]

    @pytest.mark.asyncio
    async def test_model_override_applied(self, exporter, crew_data):
        files = _files(
            await exporter.export(crew_data, {"model_override": "databricks-gpt-5"})
        )
        assert "MODEL_OVERRIDE = 'databricks-gpt-5'" in files["agent_server/agent.py"]

    @pytest.mark.asyncio
    async def test_no_model_override_keeps_none(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        assert "MODEL_OVERRIDE = None" in files["agent_server/agent.py"]

    @pytest.mark.asyncio
    async def test_tools_appear_in_config_and_tool_map(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        agents_cfg = yaml.safe_load(files["config/agents.yaml"])
        assert agents_cfg["researcher"]["tools"] == ["SerperDevTool"]
        assert (
            '"SerperDevTool": lambda: SerperDevTool()' in files["agent_server/agent.py"]
        )

    @pytest.mark.asyncio
    async def test_tasks_yaml_maps_agent(self, exporter, crew_data):
        tasks_cfg = yaml.safe_load(
            _files(await exporter.export(crew_data, {}))["config/tasks.yaml"]
        )
        assert tasks_cfg["research_task"]["agent"] == "researcher"

    @pytest.mark.asyncio
    async def test_extra_dependencies_added_for_tools(self, exporter):
        crew = {
            "id": "1",
            "name": "Scraper",
            "agents": [
                {
                    "id": "a1",
                    "name": "Scraper",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "llm": "databricks-llama-4-maverick",
                    "tools": ["ScrapeWebsiteTool"],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "T",
                    "description": "d",
                    "expected_output": "o",
                    "agent_id": "a1",
                }
            ],
        }
        pyproject = _files(await exporter.export(crew, {}))["pyproject.toml"]
        assert "beautifulsoup4" in pyproject

    @pytest.mark.asyncio
    async def test_custom_tools_emitted_when_requested(self, exporter):
        crew = {
            "id": "x",
            "name": "Genie Bot",
            "agents": [
                {
                    "id": "a1",
                    "name": "Analyst",
                    "role": "Analyst",
                    "goal": "Answer {question}",
                    "backstory": "bg",
                    "llm": "databricks-llama-4-maverick",
                    "tools": ["GenieTool"],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Answer",
                    "description": "Answer {question}",
                    "expected_output": "answer",
                    "agent_id": "a1",
                }
            ],
            "mcp_servers": [],
        }
        with_tools = _files(await exporter.export(crew, {"include_custom_tools": True}))
        assert "tools/custom_tools.py" in with_tools
        assert "tools/__init__.py" in with_tools
        assert "from tools.custom_tools import *" in with_tools["agent_server/agent.py"]

        without = _files(await exporter.export(crew, {"include_custom_tools": False}))
        assert "tools/custom_tools.py" not in without

    @pytest.mark.asyncio
    async def test_app_name_is_valid_databricks_name(self, exporter):
        crew = {"id": "1", "name": "My Crew!! 2025", "agents": [], "tasks": []}
        result = await exporter.export(crew, {})
        assert result["metadata"]["app_name"] == "my-crew-2025"
        db = yaml.safe_load(_files(result)["databricks.yml"])
        bundle = result["metadata"]["bundle_name"]
        assert db["resources"]["apps"][bundle]["name"] == "my-crew-2025"
