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

from src.engines.crewai.exporters.databricks_app_exporter import (
    TEMPLATE_DIR,
    DatabricksAppExporter,
)

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
    async def test_conversation_runs_configured_crew(self, exporter, crew_data):
        """The 'act' step runs the crew's CONFIGURED pipeline (like Kasal), not a
        fragile supervisor that delegates to a coworker ('coworker not found')."""
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "def _run_conversational" in agent
        assert "crew_runner=_run_conversational" in agent
        # Runs the predefined crew with the user's message as the input key.
        assert "_run_crew({INPUT_KEY: request_text})" in agent
        # The fragile supervisor+delegation wrapper is gone (per-agent
        # allow_delegation from the crew config may still legitimately appear).
        assert "_run_supervised" not in agent
        assert "_build_supervised_crew" not in agent
        assert "[supervisor, *workers" not in agent

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
        assert "traces will not be written" in files["agent_server/agent.py"]

    @pytest.mark.asyncio
    async def test_clarify_and_resume_loop(self, exporter, crew_data):
        """The conversation layer can ask a clarifying question and resume with
        prior context (multi-turn info-gathering)."""
        conv = _files(await exporter.export(crew_data, {}))["agent_server/conversation.py"]
        # Classify -> gather asks ONE clarifying question instead of guessing.
        assert "def _classify" in conv and "def _gather" in conv
        assert "clarifying question" in conv
        # Recent history is passed so a follow-up resumes the goal.
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
    async def test_mcp_servers_rendered_as_relative_path_with_transport(
        self, exporter, crew_data
    ):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        # Databricks-managed MCP keeps the host-relative path AND is pinned to
        # streamable-http (else the adapter defaults to SSE and times out).
        assert (
            '("genie space", "/api/2.0/mcp/genie/01ef", "streamable-http")' in agent
        )

    @pytest.mark.asyncio
    async def test_third_party_mcp_transport_and_token_env(self, exporter, crew_data):
        crew = dict(crew_data)
        crew["mcp_servers"] = [
            {
                "name": "Acme Tools",
                "server_url": "https://acme.example.com/sse",
                "server_type": "sse",
            }
        ]
        files = _files(await exporter.export(crew, {}))
        agent = files["agent_server/agent.py"]
        # Third-party server keeps its configured transport and full URL.
        assert '("Acme Tools", "https://acme.example.com/sse", "sse")' in agent
        # Its bearer comes from a per-server env var (never the Databricks token).
        assert "ACME_TOOLS_MCP_TOKEN" in files[".env.example"]

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
        # MCPServerAdapter imports mcpadapt.core — both are required, else it
        # fails with a misleading "missing the 'mcp' package" prompt.
        assert "mcpadapt" in with_mcp

        no_mcp_crew = dict(crew_data, mcp_servers=[])
        without = _files(await exporter.export(no_mcp_crew, {}))["pyproject.toml"]
        assert "mcp>=" not in without
        assert "mcpadapt" not in without

    @pytest.mark.asyncio
    async def test_pins_compatible_crewai_and_litellm(self, exporter, crew_data):
        """The app ships no lockfile, so crewai + litellm must be pinned to the
        tested pair — loose floors resolve a mismatch where crewai's LiteLLM
        fallback fails ("LiteLLM fallback package is not installed")."""
        pyproject = _files(await exporter.export(crew_data, {}))["pyproject.toml"]
        assert "crewai[tools]==1.14.5" in pyproject
        assert "litellm==1.74.9" in pyproject

    @pytest.mark.asyncio
    async def test_llm_passes_explicit_databricks_auth(self, exporter, crew_data):
        """_make_llm must pass an explicit api_base + api_key for the databricks/
        LiteLLM route (the Apps runtime won't auto-provide Databricks env to
        LiteLLM)."""
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "def _databricks_host_token" in agent
        assert '"api_base"' in agent and '"api_key"' in agent
        # AI Gateway-aware base for the chat (LiteLLM) route.
        assert "ai-gateway/mlflow/v1" in agent and "serving-endpoints" in agent

    @pytest.mark.asyncio
    async def test_app_creates_uc_bound_experiment_for_tracing(self, exporter, crew_data):
        """The app OWNS its experiment and creates it bound to Unity Catalog at
        creation (the only way to get UC trace storage — it can't be added to an
        existing experiment). Needs the SQL warehouse to provision the tables."""
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        # Experiment created by NAME with a UnityCatalog trace location.
        assert "MLFLOW_EXPERIMENT_NAME" in agent
        assert "trace_location=UnityCatalog(" in agent
        assert "table_prefix=_TRACE_TABLE_PREFIX" in agent
        # Warehouse set before set_experiment provisions the tables.
        assert 'os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"]' in agent
        # The deprecated destination API must not be used.
        assert "set_destination" not in agent
        assert "UCSchemaLocation" not in agent

    @pytest.mark.asyncio
    async def test_skips_binary_junk_in_template_tree(self, exporter, crew_data):
        """A stray binary file (e.g. macOS .DS_Store) must not crash the export
        or leak into the output."""
        junk = TEMPLATE_DIR / ".DS_Store"
        junk.write_bytes(b"\x00\x80\x81 not utf-8 \xff")
        try:
            files = _files(await exporter.export(crew_data, {}))
        finally:
            junk.unlink(missing_ok=True)
        assert ".DS_Store" not in files
        # Export still produced a valid project.
        ast.parse(files["agent_server/agent.py"])

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

    @staticmethod
    def _genie_crew(tool_configs=None):
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
        if tool_configs is not None:
            crew["tool_configs"] = tool_configs
        return crew

    @pytest.mark.asyncio
    async def test_bundled_tool_emitted_when_requested(self, exporter):
        crew = self._genie_crew()
        with_tools = _files(await exporter.export(crew, {"include_custom_tools": True}))
        # GenieTool ships as a self-contained module under tools/ (no Kasal deps).
        assert "tools/genie_tool.py" in with_tools
        assert "tools/__init__.py" in with_tools
        genie_src = with_tools["tools/genie_tool.py"]
        assert "class GenieTool" in genie_src
        assert "from src." not in genie_src  # must be standalone
        agent_py = with_tools["agent_server/agent.py"]
        assert "from tools.genie_tool import GenieTool" in agent_py
        # TOOL_MAP keyed by the title the crew's agents.yaml carries.
        assert '"GenieTool": lambda: GenieTool(' in agent_py

        without = _files(await exporter.export(crew, {"include_custom_tools": False}))
        assert "tools/genie_tool.py" not in without

    @pytest.mark.asyncio
    async def test_genie_space_id_baked_from_config(self, exporter):
        crew = self._genie_crew(tool_configs={"GenieTool": {"space_id": "abc123"}})
        agent_py = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        # space_id from config is baked as the default, with env override.
        assert "GENIE_SPACE_ID" in agent_py
        assert "'abc123'" in agent_py or '"abc123"' in agent_py

    @pytest.mark.asyncio
    async def test_tool_config_baked_into_factory(self, exporter):
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = ["SerperDevTool"]
        crew["tool_configs"] = {
            "SerperDevTool": {
                "n_results": 7,
                "country": "fr",
                "serper_api_key": "SECRET",  # must NOT be baked
            }
        }
        agent_py = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        assert '"SerperDevTool": lambda: SerperDevTool(' in agent_py
        assert "'n_results': 7" in agent_py
        assert "'country': 'fr'" in agent_py
        assert "SECRET" not in agent_py  # secrets are stripped

    @pytest.mark.asyncio
    async def test_unsupported_tool_flagged_not_called(self, exporter):
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = ["Power BI Comprehensive Analysis Tool"]
        result = await exporter.export(crew, {})
        agent_py = _files(result)["agent_server/agent.py"]
        # Flagged as a comment, never emitted as a runtime call that NameErrors.
        assert "unsupported standalone" in agent_py
        assert (
            "Power BI Comprehensive Analysis Tool"
            in result["metadata"]["unsupported_tools"]
        )

    @pytest.mark.asyncio
    async def test_llm_guardrail_reproduced(self, exporter):
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = []
        crew["tasks"][0]["llm_guardrail"] = {
            "description": "Output must cite at least one source",
            "llm_model": "databricks-llama-4-maverick",
        }
        agent_py = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        assert "TASK_GUARDRAILS = {" in agent_py
        assert "'type': 'llm'" in agent_py
        assert "Output must cite at least one source" in agent_py
        # The runtime builds a CrewAI LLMGuardrail from the spec.
        assert "def _make_task_guardrail" in agent_py
        assert "LLMGuardrail(" in agent_py

    @pytest.mark.asyncio
    async def test_code_guardrail_flagged_not_executed(self, exporter):
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = []
        crew["tasks"][0]["guardrail"] = {"type": "word_count", "max": 500}
        agent_py = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        # Code/factory guardrails are carried but flagged (can't be bundled).
        assert "'type': 'code'" in agent_py
        assert "'name': 'word_count'" in agent_py
        assert "not reproduced in this app" in agent_py

    @pytest.mark.asyncio
    async def test_no_guardrail_yields_empty_map(self, exporter):
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = []
        agent_py = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        assert "TASK_GUARDRAILS = {}" in agent_py

    @pytest.mark.asyncio
    async def test_app_name_is_valid_databricks_name(self, exporter):
        crew = {"id": "1", "name": "My Crew!! 2025", "agents": [], "tasks": []}
        result = await exporter.export(crew, {})
        assert result["metadata"]["app_name"] == "my-crew-2025"
        db = yaml.safe_load(_files(result)["databricks.yml"])
        bundle = result["metadata"]["bundle_name"]
        assert db["resources"]["apps"][bundle]["name"] == "my-crew-2025"
