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
        # build_crew wires hierarchical manager + planning.
        assert "Process.hierarchical" in agent
        assert "manager_llm" in agent
        # Reasoning is driven by the answer mode (research/deep), not a static flag.
        assert 'reasoning=REASONING_ENABLED and mode in ("research", "deep")' in agent
        # Planning runs when configured OR in deep mode, and always gets an explicit
        # planner LLM (never CrewAI's OpenAI default).
        assert 'planning_on = PLANNING or _current_mode() == "deep"' in agent
        assert "planning=planning_on" in agent
        assert (
            'kwargs["planning_llm"] = _make_llm(PLANNING_LLM or _conversation_model())'
            in agent
        )

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
        conv = _files(await exporter.export(crew_data, {}))[
            "agent_server/conversation.py"
        ]
        # Classify -> gather asks ONE clarifying question instead of guessing.
        assert "def _classify" in conv and "def _gather" in conv
        assert "clarifying question" in conv
        # Recent history is passed so a follow-up resumes the goal.
        assert "history[-6:]" in conv

    @pytest.mark.asyncio
    async def test_gather_does_not_produce_deliverable(self, exporter, crew_data):
        """While clarifying, the assistant must ASK and stop — never generate the
        deliverable unprompted (the crew only runs after the user confirms)."""
        files = _files(await exporter.export(crew_data, {}))
        conv = files["agent_server/conversation.py"]
        # Gather is explicitly forbidden from drafting/producing the deliverable.
        assert "NEVER produce, generate, draft, or preview the deliverable" in conv
        assert "Do NOT generate, draft, or output any part of the" in conv
        # Chat mode likewise asks one question for vague deliverable requests.
        agent = files["agent_server/agent.py"]
        assert "short clarifying question and STOP" in agent

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
        assert '("genie space", "/api/2.0/mcp/genie/01ef", "streamable-http")' in agent

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
    async def test_app_creates_uc_bound_experiment_for_tracing(
        self, exporter, crew_data
    ):
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
    async def test_llm_guardrail_from_plan(self, exporter):
        """An LLM guardrail is written into the editable plan (tasks.yaml), not
        hardcoded in app code; the runtime reproduces it from the task's config."""
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = []
        crew["tasks"][0]["llm_guardrail"] = {
            "description": "Output must cite at least one source",
            "llm_model": "databricks-llama-4-maverick",
        }
        files = _files(await exporter.export(crew, {}))
        tasks_cfg = yaml.safe_load(files["config/tasks.yaml"])
        assert (
            tasks_cfg["answer"]["guardrail"] == "Output must cite at least one source"
        )
        agent_py = files["agent_server/agent.py"]
        # Runtime reads the guardrail FROM the plan config and builds an LLMGuardrail.
        assert "def _make_task_guardrail(cfg" in agent_py
        assert 'cfg.get("guardrail")' in agent_py
        assert "LLMGuardrail(" in agent_py
        # Nothing is baked into app code anymore.
        assert "TASK_GUARDRAILS" not in agent_py

    @pytest.mark.asyncio
    async def test_code_guardrail_omitted_from_plan(self, exporter):
        """Code/factory guardrails can't run standalone, so they are omitted from
        the plan (no guardrail) rather than hardcoded or flagged in the app."""
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = []
        crew["tasks"][0]["guardrail"] = {"type": "word_count", "max": 500}
        tasks_cfg = yaml.safe_load(
            _files(await exporter.export(crew, {}))["config/tasks.yaml"]
        )
        assert "guardrail" not in tasks_cfg["answer"]

    @pytest.mark.asyncio
    async def test_no_guardrail_in_plan(self, exporter):
        """A task with no configured guardrail gets none — no guardrail key in the
        plan and no baked map in the app."""
        crew = self._genie_crew()
        crew["agents"][0]["tools"] = []
        files = _files(await exporter.export(crew, {}))
        tasks_cfg = yaml.safe_load(files["config/tasks.yaml"])
        assert "guardrail" not in tasks_cfg["answer"]
        assert "TASK_GUARDRAILS" not in files["agent_server/agent.py"]

    @pytest.mark.asyncio
    async def test_app_name_is_valid_databricks_name(self, exporter):
        crew = {"id": "1", "name": "My Crew!! 2025", "agents": [], "tasks": []}
        result = await exporter.export(crew, {})
        assert result["metadata"]["app_name"] == "my-crew-2025"
        db = yaml.safe_load(_files(result)["databricks.yml"])
        bundle = result["metadata"]["bundle_name"]
        assert db["resources"]["apps"][bundle]["name"] == "my-crew-2025"


class TestDatabricksAppA2UI:
    """A2UI generative-UI capability: catalog shipped, composer wired, bundled
    frontend (no runtime clone), and export-time surfaceKind hinting."""

    @pytest.mark.asyncio
    async def test_a2ui_catalog_shipped(self, exporter, crew_data):
        import json

        files = _files(await exporter.export(crew_data, {}))
        # The catalog now lives inside the vendored composer package.
        assert "agent_server/a2ui/catalog.json" in files
        # The old standalone catalog file is gone (replaced by the package).
        assert "agent_server/a2ui_catalog.json" not in files
        catalog = json.loads(files["agent_server/a2ui/catalog.json"])
        assert isinstance(catalog.get("components"), dict) and catalog["components"]
        assert "surfaceKinds" in catalog
        # Core components the composer/renderer rely on.
        for comp in (
            "Markdown",
            "Table",
            "Chart",
            "SlideDeck",
            "Slide",
            "Mindmap",
            "Quiz",
        ):
            assert comp in catalog["components"]
        # Quiz is a first-class surface kind (interactive assessment).
        assert "quiz" in catalog["surfaceKinds"]

    @pytest.mark.asyncio
    async def test_a2ui_composer_vendored_not_inlined(self, exporter, crew_data):
        """The composer is the ONE shared module vendored under agent_server/a2ui/,
        imported by agent.py — not a second inline copy that could drift."""
        files = _files(await exporter.export(crew_data, {}))
        assert "agent_server/a2ui/__init__.py" in files
        assert "agent_server/a2ui/compose.py" in files
        compose = files["agent_server/a2ui/compose.py"]
        # The real composer + the shared resolvers live in the vendored module.
        assert "def compose_a2ui(" in compose
        assert "def a2ui_system_prompt(" in compose
        assert "def guidance_for(" in compose
        agent = files["agent_server/agent.py"]
        # agent.py imports the shared composer instead of defining its own prompt.
        assert "from agent_server.a2ui.compose import" in agent
        assert "def _a2ui_system_prompt(" not in agent  # no inline duplicate

    @pytest.mark.asyncio
    async def test_a2ui_config_baked(self, exporter, crew_data):
        """The workspace's resolved UIConfig (enabled + per-deliverable directives)
        is baked next to the composer so the deployed app matches live chat."""
        import json

        files = _files(await exporter.export(crew_data, {}))
        assert "agent_server/a2ui/config.json" in files
        config = json.loads(files["agent_server/a2ui/config.json"])
        assert set(config) == {"enabled", "directives"}
        assert isinstance(config["enabled"], bool)
        assert isinstance(config["directives"], dict)

    @pytest.mark.asyncio
    async def test_a2ui_config_reflects_workspace(self, exporter, crew_data):
        """A minimal catalog choice + directives flow from crew_data into the bake."""
        import json

        crew = dict(
            crew_data,
            a2ui_enabled=False,
            a2ui_catalog={"surfaceKinds": ["document"], "components": {"Markdown": {}}},
            a2ui_directives={"presentation": "aim for about 8 slides"},
        )
        files = _files(await exporter.export(crew, {}))
        catalog = json.loads(files["agent_server/a2ui/catalog.json"])
        assert set(catalog["components"]) == {"Markdown"}  # restricted as configured
        config = json.loads(files["agent_server/a2ui/config.json"])
        assert config["enabled"] is False
        assert config["directives"]["presentation"] == "aim for about 8 slides"

    @pytest.mark.asyncio
    async def test_a2ui_themes_baked_into_frontend(self, exporter, crew_data):
        """The workspace's deck/quiz palettes are baked into App.tsx (frontend-only,
        NOT config.json) so the deployed app's themes match this workspace's live
        chat. Unconfigured → "{}" (built-in themes), token always substituted."""
        import json

        # Default: no workspace themes → empty object, no leftover token.
        app = _files(await exporter.export(crew_data, {}))["frontend/src/App.tsx"]
        assert "{{WORKSPACE_THEMES_JSON}}" not in app
        assert "const WORKSPACE_THEMES = {} as Record<string, ThemePalette>" in app
        assert "themesFor('presentation')" in app and "themesFor('quiz')" in app
        # Configured: the palette flows into the baked literal.
        crew = dict(
            crew_data,
            a2ui_themes={
                "presentation": {"accent": "#2272B4", "background": "#FFFFFF"}
            },
        )
        app2 = _files(await exporter.export(crew, {}))["frontend/src/App.tsx"]
        assert '"accent": "#2272B4"' in app2 and '"presentation"' in app2
        # Themes are frontend-only — never written into the composer config.
        config = json.loads(
            _files(await exporter.export(crew, {}))["agent_server/a2ui/config.json"]
        )
        assert "themes" not in config

    @pytest.mark.asyncio
    async def test_a2ui_composer_present(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "def _compose_a2ui(" in agent
        assert "A2UI_ENABLED" in agent

    @pytest.mark.asyncio
    async def test_a2ui_composed_off_critical_path(self, exporter, crew_data):
        """The surface is composed out-of-band (so the answer request returns fast
        and can't be dropped by the Databricks Apps proxy timeout): both endpoints
        schedule a background compose and NO LONGER return it inline in
        custom_outputs. The UI polls GET /a2ui/{id} instead."""
        files = _files(await exporter.export(crew_data, {}))
        agent = files["agent_server/agent.py"]
        # Background scheduling on both turns; no inline custom_outputs surface.
        assert "def _schedule_a2ui(" in agent
        assert agent.count("_schedule_a2ui(message, output_text, session_id)") == 2
        assert 'custom_outputs={"a2ui"' not in agent
        # The poll store is shipped and the route is exposed.
        assert "agent_server/a2ui_store.py" in files
        assert (
            '@app.get("/a2ui/{conversation_id}")'
            in files["agent_server/start_server.py"]
        )
        # The frontend polls for the surface and patches the message when ready.
        assert "export async function fetchA2ui(" in files["frontend/src/api.ts"]
        assert "pollA2ui(" in files["frontend/src/App.tsx"]

    @pytest.mark.asyncio
    async def test_a2ui_enabled_in_app_yaml(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        assert "A2UI_ENABLED" in files["app.yaml"]
        assert "A2UI_ENABLED" in files[".env.example"]

    @pytest.mark.asyncio
    async def test_a2ui_hint_presentation_keyword(self, exporter):
        crew = {
            "id": "p1",
            "name": "Pitch Builder",
            "agents": [
                {
                    "id": "a1",
                    "name": "Maker",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "llm": "databricks-claude-sonnet-4-5",
                    "tools": [],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Build",
                    "description": "Build a slide deck.",
                    "expected_output": "A 5-slide presentation.",
                    "agent_id": "a1",
                    "tools": [],
                }
            ],
        }
        agent = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        assert 'A2UI_HINT = """Prefer surfaceKind \'presentation\'."""' in agent

    @pytest.mark.asyncio
    async def test_a2ui_hint_empty_when_no_keywords(self, exporter):
        crew = {
            "id": "h1",
            "name": "Helper",
            "agents": [
                {
                    "id": "a1",
                    "name": "A",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "llm": "databricks-claude-sonnet-4-5",
                    "tools": [],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Answer",
                    "description": "Answer the user.",
                    "expected_output": "A helpful answer.",
                    "agent_id": "a1",
                    "tools": [],
                }
            ],
        }
        agent = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        assert 'A2UI_HINT = """"""' in agent
        ast.parse(agent, filename="agent_server/agent.py")

    @pytest.mark.asyncio
    async def test_a2ui_hint_quiz_keyword(self, exporter):
        """A quiz/assessment crew biases the composer toward the 'quiz' surface."""
        crew = {
            "id": "q1",
            "name": "Quiz Maker",
            "agents": [
                {
                    "id": "a1",
                    "name": "Maker",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "llm": "databricks-claude-sonnet-4-5",
                    "tools": [],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Build",
                    "description": "Create a multiple-choice quiz.",
                    "expected_output": "A 10-question assessment.",
                    "agent_id": "a1",
                    "tools": [],
                }
            ],
        }
        agent = _files(await exporter.export(crew, {}))["agent_server/agent.py"]
        assert 'A2UI_HINT = """Prefer surfaceKind \'quiz\'."""' in agent

    @pytest.mark.asyncio
    async def test_a2ui_quiz_surface_wired_in_composer(self, exporter, crew_data):
        """The composer prompt teaches the 'quiz' surface and the rich-intent
        heuristic treats quiz keywords as a rich (A2UI) request. The prompt now
        lives in the vendored shared composer, not inline in agent.py."""
        compose = _files(await exporter.export(crew_data, {}))[
            "agent_server/a2ui/compose.py"
        ]
        # System prompt instructs the model to build ONE Quiz component for quizzes.
        assert "surfaceKind from the USER'S REQUEST" in compose
        assert "for a quiz/assessment/test use 'quiz'" in compose
        # Rich-intent vocabulary includes quiz terms so the surface is composed.
        assert '"quiz",' in compose and '"assessment",' in compose

    @pytest.mark.asyncio
    async def test_frontend_bundled_not_cloned(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        assert "frontend/package.json" in files
        assert "frontend/src/a2ui/A2UIRenderer.tsx" in files
        assert "frontend/src/App.tsx" in files
        start_app = files["scripts/start_app.py"]
        assert "databricks/app-templates" not in start_app
        assert "clone_frontend_if_needed" not in start_app
        assert 'Path("frontend")' in start_app

    @pytest.mark.asyncio
    async def test_frontend_renderer_vendored_from_shared(self, exporter, crew_data):
        """The export ships the ONE shared frontend renderer (self-contained, with
        its own lib/ + ui/), byte-identical to live Kasal — not a drifted copy."""
        from src.engines.crewai.exporters.databricks_app_exporter import (
            SHARED_A2UI_FRONTEND_DIR,
        )

        files = _files(await exporter.export(crew_data, {}))
        for rel in (
            "frontend/src/a2ui/components.tsx",
            "frontend/src/a2ui/A2UIRenderer.tsx",
            "frontend/src/a2ui/registry.tsx",
            "frontend/src/a2ui/resolve.ts",
            "frontend/src/a2ui/lib/deckThemes.ts",
            "frontend/src/a2ui/lib/download.ts",
            "frontend/src/a2ui/lib/markdown.tsx",
            "frontend/src/a2ui/lib/surfaceContext.ts",
            "frontend/src/a2ui/ui/button.tsx",
        ):
            assert rel in files, f"missing vendored {rel}"
        # The drifted top-level lib copies are gone (only the shell's cn remains).
        assert "frontend/src/lib/download.ts" not in files
        assert "frontend/src/lib/deckThemes.ts" not in files
        assert "frontend/src/lib/markdown.tsx" not in files
        # Byte-identical to the live source → genuinely one implementation.
        live = (SHARED_A2UI_FRONTEND_DIR / "components.tsx").read_text(encoding="utf-8")
        assert files["frontend/src/a2ui/components.tsx"] == live
        # Renderer unit-test files are not shipped (the export only builds).
        assert not any(
            p.startswith("frontend/src/a2ui/") and ".test." in p for p in files
        )

    @pytest.mark.asyncio
    async def test_frontend_has_no_node_modules_or_dist(self, exporter, crew_data):
        """Build artifacts/deps must never be shipped in the export."""
        files = _files(await exporter.export(crew_data, {}))
        for p in files:
            assert "node_modules" not in p.split("/")
            assert "dist" not in p.split("/")

    @pytest.mark.asyncio
    async def test_starter_prompts_token_is_valid_json(self, exporter, crew_data):
        """The starter-prompts token must render to a valid JS/JSON array literal."""
        import json

        app = _files(await exporter.export(crew_data, {}))["frontend/src/App.tsx"]
        assert "{{STARTER_PROMPTS_JSON}}" not in app  # token substituted
        m = re.search(r"const STARTER_PROMPTS: string\[\] = (.+)", app)
        assert m, "STARTER_PROMPTS declaration missing"
        prompts = json.loads(m.group(1).strip())
        assert isinstance(prompts, list)
        # crew_data's tasks contain {placeholders}, so they are filtered out.
        assert prompts == []

    @pytest.mark.asyncio
    async def test_starter_prompts_derived_from_tasks(self, exporter):
        import json

        crew = {
            "id": "s1",
            "name": "Sales Analyst",
            "agents": [
                {
                    "id": "a1",
                    "name": "A",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "llm": "databricks-claude-sonnet-4-5",
                    "tools": [],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Analyze",
                    "description": "Analyze quarterly sales performance by region. Then rank them.",
                    "expected_output": "A ranked summary.",
                    "agent_id": "a1",
                    "tools": [],
                }
            ],
        }
        app = _files(await exporter.export(crew, {}))["frontend/src/App.tsx"]
        m = re.search(r"const STARTER_PROMPTS: string\[\] = (.+)", app)
        prompts = json.loads(m.group(1).strip())
        # First sentence of the task description becomes a starter prompt.
        assert prompts == ["Analyze quarterly sales performance by region."]

    @pytest.mark.asyncio
    async def test_server_serves_static_without_chat_proxy(self, exporter, crew_data):
        """The agent server serves the built SPA itself (no MLflow chat proxy,
        which double-encodes gzip and breaks asset loading)."""
        files = _files(await exporter.export(crew_data, {}))
        server = files["agent_server/start_server.py"]
        assert "enable_chat_proxy=True" not in server
        assert "StaticFiles" in server
        assert 'frontend" / "dist"' in server

    @pytest.mark.asyncio
    async def test_start_app_is_single_process(self, exporter, crew_data):
        """start_app builds the SPA but runs only the backend (which serves it);
        there is no separate frontend server."""
        start_app = _files(await exporter.export(crew_data, {}))["scripts/start_app.py"]
        assert "npm run build" in start_app
        assert '"npm", "run", "start"' not in start_app
        assert "vite preview" not in start_app

    @pytest.mark.asyncio
    async def test_frontend_uses_tailwind_and_shadcn(self, exporter, crew_data):
        """UI is built on Tailwind v4 + shadcn/ui components, TypeScript-only —
        no hand-written stylesheet."""
        files = _files(await exporter.export(crew_data, {}))
        # The old ad-hoc stylesheet is gone; Tailwind entry + tokens replace it.
        assert "frontend/src/styles.css" not in files
        assert "frontend/src/index.css" in files
        assert '@import "tailwindcss"' in files["frontend/src/index.css"]
        # shadcn primitives + the cn() helper are bundled.
        assert "frontend/src/lib/utils.ts" in files
        for comp in ("button", "card", "avatar", "separator"):
            assert f"frontend/src/components/ui/{comp}.tsx" in files
        pkg = files["frontend/package.json"]
        assert "tailwindcss" in pkg and "@tailwindcss/vite" in pkg
        # No JavaScript anywhere in the frontend source (TypeScript-only).
        assert not any(
            p.startswith("frontend/src/") and p.endswith((".js", ".jsx")) for p in files
        )


class TestDatabricksAppModes:
    """Answer modes (chat | research | deep): per-request depth selection, the
    fast/deep tool split, mode-driven reasoning/planning, and the chat runner."""

    @pytest.mark.asyncio
    async def test_mode_constants_and_extraction(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert 'VALID_MODES = ("chat", "research", "deep")' in agent
        assert 'DEFAULT_MODE = os.environ.get("CREW_MODE", "research")' in agent
        assert 'FAST_MAX_ITER = int(os.environ.get("FAST_MAX_ITER", "5"))' in agent
        # The request's custom_inputs.mode selects the depth.
        assert "def _extract_mode(request)" in agent
        assert 'ci.get("mode") in VALID_MODES' in agent

    @pytest.mark.asyncio
    async def test_mode_drives_reasoning_and_planning(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        # Reasoning on in research + deep; planning only in deep.
        assert 'reasoning=REASONING_ENABLED and mode in ("research", "deep")' in agent
        assert 'planning_on = PLANNING or _current_mode() == "deep"' in agent
        # Research mode caps tool-call loops for speed.
        assert 'min(cfg.get("max_iter", 25), FAST_MAX_ITER)' in agent

    @pytest.mark.asyncio
    async def test_reasoning_env_gated_and_off_for_local(self, exporter, crew_data):
        """CrewAI agent reasoning is env-toggleable and defaults OFF for local
        OpenAI-compatible models, which fail its structured StepObservation schema."""
        files = _files(await exporter.export(crew_data, {}))
        agent = files["agent_server/agent.py"]
        assert 'REASONING_ENABLED = os.environ.get("AGENT_REASONING"' in agent
        assert (
            '_REASONING_DEFAULT = "false" if os.environ.get("LOCAL_LLM_BASE_URL")'
            in agent
        )
        # Reasoning is gated by the toggle AND the mode.
        assert 'reasoning=REASONING_ENABLED and mode in ("research", "deep")' in agent
        assert "AGENT_REASONING" in files[".env.example"]

    @pytest.mark.asyncio
    async def test_fast_deep_tool_split(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        # Deep-only MCP tools are dropped in chat/research, kept in deep.
        assert "FAST_MODE_DISABLED_TOOLS" in agent
        assert 'os.environ.get("FAST_MODE_DISABLED_TOOLS", "")' in agent
        # Always-off tools are independent of mode.
        assert 'os.environ.get("MCP_DISABLED_TOOLS", "")' in agent
        files = _files(await exporter.export(crew_data, {}))
        assert "FAST_MODE_DISABLED_TOOLS" in files[".env.example"]

    @pytest.mark.asyncio
    async def test_chat_mode_uses_single_agent_runner(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        # Chat mode answers with one agent (agent.kickoff), no crew, wired in.
        assert "def _run_chat(" in agent
        assert "chat_runner=_run_chat" in agent
        assert "agent.kickoff(request_text)" in agent
        conv = _files(await exporter.export(crew_data, {}))[
            "agent_server/conversation.py"
        ]
        # The conversation layer routes chat mode straight to the chat runner.
        assert 'mode == "chat"' in conv and "chat_runner" in conv


class TestDatabricksAppControls:
    """Runtime safety controls: cooperative Stop, per-turn timeout, and the
    LLM/agent execution caps that bound token spend."""

    @pytest.mark.asyncio
    async def test_cancel_module_and_endpoint(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        # Thread-safe cancel registry shipped.
        assert "agent_server/cancel.py" in files
        cancel = files["agent_server/cancel.py"]
        assert "class CrewCancelled" in cancel
        assert "def request(" in cancel and "def is_cancelled(" in cancel
        # The server exposes a cancel endpoint that flags the conversation.
        server = files["agent_server/start_server.py"]
        assert "/cancel/{conversation_id}" in server
        assert "cancel.request(" in server

    @pytest.mark.asyncio
    async def test_cancel_wired_into_crew_callbacks(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        # Crew step/task callbacks abort cooperatively when cancellation is flagged.
        assert "step_callback" in agent and "task_callback" in agent
        assert "cancel.is_cancelled(conversation_id)" in agent
        assert "raise cancel.CrewCancelled(conversation_id)" in agent

    @pytest.mark.asyncio
    async def test_turn_timeout_and_llm_caps(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        agent = files["agent_server/agent.py"]
        assert (
            'LLM_REQUEST_TIMEOUT = int(os.environ.get("LLM_REQUEST_TIMEOUT", "300"))'
            in agent
        )
        assert (
            'CREW_TIMEOUT_SECONDS = int(os.environ.get("CREW_TIMEOUT_SECONDS", "600"))'
            in agent
        )
        assert (
            'AGENT_MAX_EXECUTION_TIME = int(os.environ.get("AGENT_MAX_EXECUTION_TIME", "0"))'
            in agent
        )
        # Whole-turn watchdog cancels the crew on expiry.
        assert "CREW_TIMEOUT_SECONDS" in agent and "asyncio.wait_for(" in agent
        # Documented for operators.
        for key in ("LLM_REQUEST_TIMEOUT", "CREW_TIMEOUT_SECONDS"):
            assert key in files["app.yaml"]
            assert key in files[".env.example"]

    @pytest.mark.asyncio
    async def test_inject_date_on_agents(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        # Both the crew agents and the chat agent get today's date injected.
        assert 'inject_date=cfg.get("inject_date", True)' in agent
        assert agent.count("inject_date=") >= 2

    @pytest.mark.asyncio
    async def test_live_progress_reporting(self, exporter, crew_data):
        """Ephemeral 'doing X' status is published per turn and exposed for the UI
        to poll, so the user sees activity while a turn runs."""
        files = _files(await exporter.export(crew_data, {}))
        assert "agent_server/progress.py" in files
        assert "agent_server/crew_progress.py" in files
        # A CrewAI event-bus listener feeds the progress store.
        crew_progress = files["agent_server/crew_progress.py"]
        assert (
            "BaseEventListener" in crew_progress and "crewai_event_bus" in crew_progress
        )
        # The server exposes the poll endpoint the frontend reads.
        assert "/progress/{conversation_id}" in files["agent_server/start_server.py"]
        assert "export async function fetchProgress" in files["frontend/src/api.ts"]


class TestDatabricksAppCitations:
    """Source citations: agents emit inline [n](url) markers + a Sources list,
    and the UI renders them as clickable references (open in a new tab)."""

    @pytest.mark.asyncio
    async def test_citation_directive_applied_to_agents(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert 'CITATIONS_ENABLED = os.environ.get("CITATIONS"' in agent
        assert "CITATION_DIRECTIVE = (" in agent
        assert "def _with_citations(backstory: str)" in agent
        # Applied to BOTH the crew agents and the single chat agent.
        assert 'backstory=_with_citations(cfg["backstory"])' in agent
        assert agent.count("_with_citations(") >= 2

    @pytest.mark.asyncio
    async def test_citations_documented_in_env(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        assert "CITATIONS" in files["app.yaml"]
        assert "CITATIONS=true" in files[".env.example"]

    @pytest.mark.asyncio
    async def test_citation_renderer_shipped(self, exporter, crew_data):
        files = _files(await exporter.export(crew_data, {}))
        # markdown.tsx lives inside the vendored shared renderer module now.
        assert "frontend/src/a2ui/lib/markdown.tsx" in files
        md = files["frontend/src/a2ui/lib/markdown.tsx"]
        # New-tab links + numeric-text citation chips + the linkifier.
        assert "export const mdComponents" in md
        assert 'rel="noopener noreferrer"' in md
        assert "align-super" in md
        assert "export function linkifyCitations" in md
        # Both markdown surfaces use the shared renderer + linkifier.
        for path in ("frontend/src/App.tsx", "frontend/src/a2ui/components.tsx"):
            src = files[path]
            assert "linkifyCitations" in src and "components={mdComponents}" in src


class TestDatabricksAppScoping:
    """Domain scoping: out-of-scope requests are declined (chat) or routed to a
    decline (research/deep) instead of being answered off-topic."""

    @pytest.mark.asyncio
    async def test_chat_agent_scoped_to_domain(self, exporter, crew_data):
        agent = _files(await exporter.export(crew_data, {}))["agent_server/agent.py"]
        assert "Stay strictly within this domain" in agent
        assert "politely decline anything outside it" in agent

    @pytest.mark.asyncio
    async def test_intake_declines_out_of_scope(self, exporter, crew_data):
        conv = _files(await exporter.export(crew_data, {}))[
            "agent_server/conversation.py"
        ]
        # Classifier gains a DECLINE outcome; respond routes it to _decline.
        assert "def _decline(" in conv
        assert '"DECLINE"' in conv
        assert "OUTSIDE the crew's purpose" in conv
        assert 'decision == "DECLINE"' in conv


class TestDatabricksAppPerAgentMCP:
    """Per-agent MCP scoping: only agents that reference a server get its tools."""

    @staticmethod
    def _scoped_crew():
        return {
            "id": "m1",
            "name": "Scoped Crew",
            "agents": [
                {
                    "id": "a1",
                    "name": "Searcher",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "llm": "databricks-llama-4-maverick",
                    "tools": [],
                    "mcp_servers": ["genie space"],
                },
                {
                    "id": "a2",
                    "name": "Writer",
                    "role": "r",
                    "goal": "g",
                    "backstory": "b",
                    "llm": "databricks-llama-4-maverick",
                    "tools": [],
                    "mcp_servers": [],
                },
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Find",
                    "description": "Find things.",
                    "expected_output": "o",
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

    @pytest.mark.asyncio
    async def test_per_agent_mcp_in_agents_yaml(self, exporter):
        files = _files(await exporter.export(self._scoped_crew(), {}))
        agents_cfg = yaml.safe_load(files["config/agents.yaml"])
        # Only the searcher is scoped to the server; the writer gets none.
        assert agents_cfg["searcher"]["mcp_servers"] == ["genie space"]
        assert agents_cfg["writer"]["mcp_servers"] == []

    @pytest.mark.asyncio
    async def test_runtime_filters_mcp_per_agent(self, exporter):
        agent = _files(await exporter.export(self._scoped_crew(), {}))[
            "agent_server/agent.py"
        ]
        # Each agent gets ONLY its configured servers; absent key = legacy all.
        assert 'allowed = cfg.get("mcp_servers")' in agent
        assert "if allowed is None:" in agent
        assert (
            "agent_mcp = [t for s in allowed for t in mcp_by_server.get(s, [])]"
            in agent
        )


class TestDatabricksAppKasalUI:
    """The chat UI mirrors Kasal's chat-mode look: sparkle composer with an
    up-arrow send, a 'Recent' session list with a per-session spinner + kebab."""

    @pytest.mark.asyncio
    async def test_composer_matches_kasal(self, exporter, crew_data):
        app = _files(await exporter.export(crew_data, {}))["frontend/src/App.tsx"]
        # Sparkle icon + up-arrow send (Kasal's input), placeholder wording.
        assert "Sparkles" in app and "ArrowUp" in app
        assert "Ask a question" in app
        # The answer-mode pill replaces Kasal's model selector.
        assert "Answer mode" in app

    @pytest.mark.asyncio
    async def test_session_list_matches_kasal(self, exporter, crew_data):
        app = _files(await exporter.export(crew_data, {}))["frontend/src/App.tsx"]
        assert "MoreVertical" in app  # vertical kebab like Kasal
        assert "Recent" in app  # section label
        # Per-session running spinner ("icon") keyed on the in-flight set.
        assert "pending.has(s.id)" in app and "animate-spin" in app
