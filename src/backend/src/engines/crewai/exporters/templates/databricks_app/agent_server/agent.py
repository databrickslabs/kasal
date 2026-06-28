"""CrewAI agent exposed through MLflow's AgentServer (ResponsesAgent interface).

The crew structure lives in ``config/agents.yaml`` and ``config/tasks.yaml`` —
edit those to change agents/tasks. This module loads that config, builds the
``Crew`` and exposes it behind the MLflow ``@invoke`` / ``@stream`` Responses
API so the app gets a chat UI and a queryable agent endpoint for free.

Each chat turn is handled by the generic conversation layer
(``agent_server.conversation``): it chats with the user, answers what the crew
can do, gathers the input the crew needs, and only runs ``crew.kickoff(...)``
once it has enough — keeping multi-turn history per conversation. See the MLflow
ResponsesAgent docs for the I/O shape:
https://mlflow.org/docs/latest/genai/flavors/responses-agent-intro/
"""

import asyncio
import json
import os
import threading
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import yaml
import mlflow
from crewai import Agent, Crew, Task, Process, LLM
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

import agent_server.conversation as conversation
from agent_server import a2ui_store, cancel, crew_progress, progress
from agent_server.utils import get_session_id, get_user_id, get_user_workspace_client

# The ONE shared, stdlib-only A2UI composer (vendored verbatim from Kasal's
# src.shared.a2ui into agent_server/a2ui/) — the SAME code live Kasal chat runs,
# so generative UI never forks into a second implementation.
from agent_server.a2ui.compose import (
    compose_a2ui as _compose_surface,
    guidance_for as _a2ui_guidance_for,
    load_catalog as _load_a2ui_catalog,
)

# --- Runaway / hang guards (so a stuck turn can't burn tokens forever) ---------
# All env-tunable; generous defaults that only act as safety nets.
#   LLM_REQUEST_TIMEOUT   — per LLM HTTP call; a hung call fails instead of hanging.
#   CREW_TIMEOUT_SECONDS  — whole-turn wall clock; on expiry the turn is cancelled
#                           cooperatively (see agent_server.cancel). 0 disables.
#   AGENT_MAX_EXECUTION_TIME — optional native per-agent cap (0 = unset).
LLM_REQUEST_TIMEOUT = int(os.environ.get("LLM_REQUEST_TIMEOUT", "300"))
CREW_TIMEOUT_SECONDS = int(os.environ.get("CREW_TIMEOUT_SECONDS", "600"))
AGENT_MAX_EXECUTION_TIME = int(os.environ.get("AGENT_MAX_EXECUTION_TIME", "0")) or None

# --- Answer modes -------------------------------------------------------------
# Three runtime depths the UI can pick per request (custom_inputs.mode):
#   chat     -> a single LiteAgent (agent.kickoff), fast tools, no crew. Quick Q&A.
#   research -> the full crew, fast tools, capped iterations, reasoning off.
#   deep     -> the full crew, deep tools, configured iterations + reasoning.
# DEFAULT_MODE applies when the client sends none. FAST_MODE_DISABLED_TOOLS lists
# MCP tools to drop in chat/research (e.g. slow deep-research ones); FAST_MAX_ITER
# caps per-agent loops in research mode.
from contextvars import ContextVar

VALID_MODES = ("chat", "research", "deep")
DEFAULT_MODE = os.environ.get("CREW_MODE", "research")
FAST_MAX_ITER = int(os.environ.get("FAST_MAX_ITER", "5"))
FAST_MODE_DISABLED_TOOLS = {
    t.strip() for t in os.environ.get("FAST_MODE_DISABLED_TOOLS", "").split(",") if t.strip()
}
# CrewAI's agent *reasoning* makes the model emit a structured ``StepObservation``
# (whose ``suggested_refinements`` must be objects). Weaker / local OpenAI-compatible
# models often return those as plain strings, so the reasoning step fails Pydantic
# validation and CrewAI falls back to a "conservative replan" — noisy, slower, and
# degraded. Default reasoning ON, but OFF when a local model is configured
# (LOCAL_LLM_BASE_URL set); override explicitly with AGENT_REASONING=true/false.
_REASONING_DEFAULT = "false" if os.environ.get("LOCAL_LLM_BASE_URL") else "true"
REASONING_ENABLED = os.environ.get("AGENT_REASONING", _REASONING_DEFAULT).strip().lower() not in (
    "0",
    "false",
    "no",
)
_CURRENT_MODE: ContextVar[str] = ContextVar("crew_mode", default="")


def _current_mode() -> str:
    return _CURRENT_MODE.get() or DEFAULT_MODE


def _extract_mode(request) -> str:
    ci = getattr(request, "custom_inputs", None)
    if isinstance(ci, dict) and ci.get("mode") in VALID_MODES:
        return ci["mode"]
    return DEFAULT_MODE


# --- Source citations ---------------------------------------------------------
# When on (default), agents are told to cite tool-sourced claims with inline
# numbered markdown links (e.g. ``... in October 2022.[1]`` written as
# ``[1](https://source)``) and a closing ``## Sources`` list. The chat UI renders
# a numeric-text link as a small clickable superscript reference. Set CITATIONS=0
# to disable. No-op for crews whose tools return no URLs.
CITATIONS_ENABLED = os.environ.get("CITATIONS", "true").strip().lower() not in (
    "0",
    "false",
    "no",
)
CITATION_DIRECTIVE = (
    "\n\nCITING SOURCES: When a statement is based on information returned by a "
    "search or web tool, cite it. Put an inline numbered marker right after the "
    "sentence it supports, written as a markdown link whose text is just the "
    "number and whose target is the source URL — for example "
    "`... began in October 2022.[1]` where `[1]` is `[1](https://the-source-url)`. "
    "Reuse the same number for the same source. End your answer with a `## Sources` "
    "section listing each source as `1. [Title](https://the-source-url)`. "
    "Only ever cite real URLs returned by your tools — never invent or guess links. "
    "If no tool URLs are available, omit citations and the Sources section."
)


def _with_citations(backstory: str) -> str:
    """Append the citation directive to a backstory when citations are enabled."""
    if not CITATIONS_ENABLED:
        return backstory
    return f"{backstory}{CITATION_DIRECTIVE}"

# Configure where CrewAI + LLM traces are stored. This app OWNS its MLflow
# experiment and creates it here, because a Unity Catalog trace location can ONLY
# be bound at experiment-creation time — it cannot be added to an existing
# experiment (Databricks: "an experiment can only be bound to a UC trace location
# at creation time"). So we create the experiment by name with a UnityCatalog
# trace location; MLflow then PROVISIONS the UC Delta tables
# (<catalog>.<schema>.<prefix>_otel_spans, ...) through the SQL warehouse, giving
# unlimited storage, fine-grained access, and queryability from SQL/notebooks.
# Requirements (per the docs above): a SQL warehouse (MLFLOW_TRACING_SQL_WAREHOUSE_ID,
# runs the table DDL), MLflow >= 3.11, and the workspace UC-tracing previews.
# See https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog
#
# Fallback order: a platform-injected experiment id, else no tracing.
_experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME")
_experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID")
_trace_catalog = os.environ.get("DATABRICKS_CATALOG")
_trace_schema = os.environ.get("DATABRICKS_SCHEMA")
_trace_warehouse = os.environ.get("DATABRICKS_WAREHOUSE_ID")
# Delta-table prefix for this app's UC traces (-> <prefix>_otel_spans, etc.).
_TRACE_TABLE_PREFIX = "agent"


def _experiment_path(name: str) -> str:
    """A Databricks experiment name must be an absolute workspace path. The app's
    service principal can write under /Shared, so place it there."""
    return name if name.startswith("/") else f"/Shared/{name}"


_traces_configured = False
if _experiment_name and _trace_catalog and _trace_schema and _trace_warehouse:
    try:
        from mlflow.entities.trace_location import UnityCatalog

        # The warehouse must be in the env BEFORE set_experiment provisions the
        # trace tables.
        os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = str(_trace_warehouse)
        _exp_path = _experiment_path(_experiment_name)
        mlflow.set_experiment(
            experiment_name=_exp_path,
            trace_location=UnityCatalog(
                catalog_name=_trace_catalog,
                schema_name=_trace_schema,
                table_prefix=_TRACE_TABLE_PREFIX,
            ),
        )
        _traces_configured = True
        print(
            f"MLflow traces -> Unity Catalog {_trace_catalog}.{_trace_schema} "
            f"(experiment '{_exp_path}', tables {_TRACE_TABLE_PREFIX}_otel_*)"
        )
    except Exception as e:  # noqa: BLE001
        # Most common cause: the app's service principal lacks UC permissions to
        # provision/write the trace tables. Grant it (run as a schema owner):
        #   GRANT USE CATALOG ON CATALOG <catalog> TO `<sp-client-id>`;
        #   GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA <catalog>.<schema> TO `<sp-client-id>`;
        # A UC trace location can only be bound at experiment creation, so after
        # granting, redeploy with a FRESH experiment name.
        print(
            f"Could not create UC-backed experiment ({e}). The app's service "
            f"principal likely needs USE CATALOG/SCHEMA + CREATE TABLE + MODIFY + "
            f"SELECT on {_trace_catalog}.{_trace_schema}."
        )
if not _traces_configured and _experiment_name:
    try:
        mlflow.set_experiment(experiment_name=_experiment_path(_experiment_name))
        print(
            f"MLflow tracing -> experiment {_experiment_name} (managed storage; "
            "set catalog + schema + warehouse on deploy for Unity Catalog traces)"
        )
        _traces_configured = True
    except Exception as e:  # noqa: BLE001
        print(f"Could not set MLflow experiment {_experiment_name}: {e}")
if not _traces_configured and _experiment_id:
    try:
        mlflow.set_experiment(experiment_id=_experiment_id)
        print(f"MLflow tracing -> experiment {_experiment_id} (managed storage)")
        _traces_configured = True
    except Exception as e:  # noqa: BLE001
        print(f"Could not set MLflow experiment {_experiment_id}: {e}")
if not _traces_configured:
    print(
        "No MLflow experiment configured — traces will not be written. "
        "Choose an MLflow experiment + SQL warehouse in the Kasal deploy screen."
    )
try:
    mlflow.crewai.autolog()
except Exception as e:  # noqa: BLE001
    print(f"mlflow.crewai.autolog() failed: {e}")

# Subtle live progress (which task / which tool) via CrewAI's event bus — written
# to the ephemeral progress channel the UI polls; nothing is persisted.
crew_progress.install()

{{TOOL_IMPORTS}}
CONFIG_DIR = Path(__file__).parent.parent / "config"

# GENERATED — overwritten by Kasal on export. Edit config/*.yaml to change the crew.
NAME = '{{NAME}}'
# Optional model override for every agent (None keeps each agent's own llm).
MODEL_OVERRIDE = {{MODEL_OVERRIDE}}
# The crew input key the user's chat message is mapped to (e.g. "topic").
INPUT_KEY = '{{INPUT_KEY}}'
# Short description of what this crew does — used by the conversation layer to
# explain capabilities and gather the right input.
CREW_PURPOSE = """{{CREW_PURPOSE}}"""
# Authenticate Databricks-managed tools/MCP as the requesting user (OBO).
ENABLE_OBO = {{ENABLE_OBO}}
# MCP servers auto-attached to the crew, as (name, url, transport) tuples.
# transport is "streamable-http" for Databricks-managed MCP, else "sse".
MCP_SERVERS = [
{{MCP_SERVERS}}]
# Crew execution settings — mirror how Kasal runs this crew.
PROCESS = "{{PROCESS}}"  # 'sequential' or 'hierarchical'
PLANNING = {{PLANNING}}  # plan all tasks up front before execution
PLANNING_LLM = {{PLANNING_LLM}}  # None, or a model name for the planner
REASONING = {{REASONING}}  # agents reason/reflect before acting
MANAGER_LLM = {{MANAGER_LLM}}  # None, or a model name for the hierarchical manager
MEMORY = {{MEMORY}}  # enable CrewAI memory
# A2UI generative UI: when enabled, the agent's text answer is also composed into
# a declarative A2UI surface (custom_outputs.a2ui) the frontend renders as rich UI
# (presentation/dashboard/mindmap/document/...). A2UI_HINT biases the surface kind
# based on the crew's purpose, detected at export time (may be empty).
A2UI_ENABLED = os.environ.get("A2UI_ENABLED", "true").lower() in ("1", "true", "yes")
A2UI_HINT = """{{A2UI_HINT}}"""
# END GENERATED

# Mapping of tool name -> factory; populated from the crew's configured tools.
TOOL_MAP = {
{{TOOL_MAP}}}


def _load_yaml(filename: str) -> Dict[str, Any]:
    with open(CONFIG_DIR / filename, "r") as f:
        return yaml.safe_load(f) or {}


AGENTS_CONFIG = _load_yaml("agents.yaml")
TASKS_CONFIG = _load_yaml("tasks.yaml")


def _build_tools(tool_names: List[str], mcp_tools: List[Any] | None = None) -> list:
    """Instantiate configured tools, plus any runtime-resolved MCP tools."""
    tools: list = list(mcp_tools or [])
    for name in tool_names:
        factory = TOOL_MAP.get(name)
        if factory:
            try:
                tools.append(factory())
            except Exception as exc:  # noqa: BLE001
                print(f"Could not instantiate tool {name}: {exc}")
    return tools


def _is_codex_model(model_name: str) -> bool:
    """gpt-5-3-codex on Databricks only works via the OpenAI Responses API."""
    return bool(model_name) and "gpt-5-3-codex" in str(model_name).lower()


def _gateway_on() -> bool:
    """Whether the workspace routes model traffic through the AI Gateway."""
    return os.environ.get("DATABRICKS_AI_GATEWAY_ENABLED", "false").lower() in (
        "1",
        "true",
        "yes",
    )


def _databricks_host_token() -> tuple:
    """Resolve the workspace host + a bearer token (OBO user token, else app SP).

    Uses ``config.authenticate()`` so the token is valid for any auth type (OBO,
    app service-principal OAuth, or PAT).
    """
    from databricks.sdk import WorkspaceClient

    w = get_user_workspace_client() if ENABLE_OBO else WorkspaceClient()
    host = (
        getattr(w.config, "host", None) or os.environ.get("DATABRICKS_HOST", "")
    ).rstrip("/")
    try:
        token = (
            (w.config.authenticate() or {}).get("Authorization", "").split(" ", 1)[-1]
        )
    except Exception:  # noqa: BLE001
        token = os.environ.get("DATABRICKS_TOKEN", "")
    return host, token


def _make_llm(model_name: str, temperature: float = 0.7):
    """Build the LLM for an agent.

    Databricks models route through CrewAI's LiteLLM fallback as
    ``databricks/<endpoint>`` with an EXPLICIT ``api_base`` + ``api_key`` — so the
    app authenticates with its own identity (OBO/SP) instead of relying on
    LiteLLM picking up Databricks env vars (it won't in the Apps runtime). This
    mirrors how Kasal configures the LLM at runtime. gpt-5-3-codex is the
    exception — the Chat Completions route returns 404 "Supervisor API is not
    enabled", so it must use the Databricks Responses API (OpenAICompletion).

    Local/self-hosted serving: when LOCAL_LLM_BASE_URL is set (an OpenAI-compatible
    endpoint, e.g. a vLLM server), EVERY model routes there instead of Databricks,
    so the whole app — crew + conversation + A2UI composer — runs with no Databricks
    auth. The crew's configured model names (e.g. ``databricks-gpt-5-3-codex``) won't
    exist on a local server, so set LOCAL_LLM_MODEL to the one model that server
    actually serves and every call uses it. No-op when LOCAL_LLM_BASE_URL is unset.
    """
    local_base = os.environ.get("LOCAL_LLM_BASE_URL")
    if local_base:
        from crewai.llms.providers.openai.completion import OpenAICompletion

        # Prefer an explicit local model name; otherwise fall back to the
        # configured name (stripping any "provider/" prefix).
        endpoint = os.environ.get("LOCAL_LLM_MODEL") or (
            model_name.split("/", 1)[1] if "/" in str(model_name) else model_name
        )
        # Some hosted models pin the sampling temperature (e.g. Kimi K2 only
        # accepts 1); LOCAL_LLM_TEMPERATURE overrides the caller's value when set.
        temp_override = os.environ.get("LOCAL_LLM_TEMPERATURE")
        return OpenAICompletion(
            model=endpoint,
            base_url=local_base,
            api_key=os.environ.get("LOCAL_LLM_API_KEY", "dummy"),
            temperature=float(temp_override) if temp_override else temperature,
            timeout=LLM_REQUEST_TIMEOUT,
        )
    host, token = _databricks_host_token()
    if _is_codex_model(model_name):
        from crewai.llms.providers.openai.completion import OpenAICompletion

        # Responses API: AI Gateway on -> /ai-gateway/openai/v1 ; off -> /serving-endpoints.
        base_path = "ai-gateway/openai/v1" if _gateway_on() else "serving-endpoints"
        return OpenAICompletion(
            model=model_name,
            api="responses",
            base_url=f"{host}/{base_path}",
            api_key=token,
            timeout=max(LLM_REQUEST_TIMEOUT, 300),
        )
    endpoint = (
        model_name.split("/", 1)[1]
        if str(model_name).startswith("databricks/")
        else model_name
    )
    # LiteLLM's Databricks provider appends /chat/completions to api_base:
    # AI Gateway on -> /ai-gateway/mlflow/v1 ; off -> /serving-endpoints.
    kwargs = {
        "model": f"databricks/{endpoint}",
        "temperature": temperature,
        "timeout": LLM_REQUEST_TIMEOUT,
    }
    if host:
        kwargs["api_base"] = (
            f"{host}/ai-gateway/mlflow/v1" if _gateway_on() else f"{host}/serving-endpoints"
        )
    if token:
        kwargs["api_key"] = token
    return LLM(**kwargs)


def _build_agents(mcp_by_server: Dict[str, List[Any]] | None = None) -> Dict[str, Agent]:
    mcp_by_server = mcp_by_server or {}
    mode = _current_mode()
    # Flat union, used only as the legacy fallback for exports that predate
    # per-agent MCP assignment (no ``mcp_servers`` key in agents.yaml).
    all_mcp_tools = [t for tools in mcp_by_server.values() for t in tools]
    agents: Dict[str, Agent] = {}
    for name, cfg in AGENTS_CONFIG.items():
        llm_model = MODEL_OVERRIDE or cfg.get("llm", "databricks-llama-4-maverick")
        llm = _make_llm(llm_model, cfg.get("temperature", 0.7))
        # Give this agent ONLY the MCP servers it is configured to use. Key absent
        # => legacy export: fall back to all servers (old all-agents behavior).
        # Key present (incl. []) => exactly those servers, nothing more.
        allowed = cfg.get("mcp_servers")
        if allowed is None:
            agent_mcp = all_mcp_tools
        else:
            agent_mcp = [t for s in allowed for t in mcp_by_server.get(s, [])]
        agents[name] = Agent(
            role=cfg["role"],
            goal=cfg["goal"],
            backstory=_with_citations(cfg["backstory"]),
            llm=llm,
            tools=_build_tools(cfg.get("tools", []), agent_mcp),
            verbose=cfg.get("verbose", True),
            allow_delegation=cfg.get("allow_delegation", False),
            # research mode caps tool-call loops for speed; deep keeps the config.
            max_iter=min(cfg.get("max_iter", 25), FAST_MAX_ITER)
            if mode == "research"
            else cfg.get("max_iter", 25),
            # Optional hard wall-clock cap per agent (CrewAI raises when exceeded);
            # off unless AGENT_MAX_EXECUTION_TIME (or per-agent config) is set.
            max_execution_time=cfg.get("max_execution_time", AGENT_MAX_EXECUTION_TIME),
            # Reasoning in research + deep; planning is added in deep (build_crew).
            reasoning=REASONING_ENABLED and mode in ("research", "deep"),
            # Inject today's date so agents answer "latest/current" correctly.
            inject_date=cfg.get("inject_date", True),
            date_format=cfg.get("date_format", "%Y-%m-%d"),
        )
    return agents


def _make_task_guardrail(cfg: Dict[str, Any]):
    """Build the output guardrail for a task FROM ITS PLAN (config/tasks.yaml), or None.

    The crew plan is the single source of truth: a task with a ``guardrail``
    description (string) gets a CrewAI ``LLMGuardrail`` reproducing it; a task with
    no ``guardrail`` in the plan gets none. Edit tasks.yaml to change or remove it —
    nothing is hardcoded here. (Kasal built-in code/factory guardrails can't run
    standalone, so the exporter omits them from the plan rather than baking one in.)
    """
    text = cfg.get("guardrail")
    if isinstance(text, dict):  # tolerate {description: ...} shape
        text = text.get("description")
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        from crewai.tasks.llm_guardrail import LLMGuardrail

        return LLMGuardrail(
            description=text.strip(),
            llm=_make_llm(MODEL_OVERRIDE or _conversation_model()),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Could not build LLM guardrail: {exc}")
        return None


def _build_tasks(agents: Dict[str, Agent]) -> list:
    tasks = []
    agent_names = list(agents.keys())
    for task_name, cfg in TASKS_CONFIG.items():
        agent_key = cfg.get("agent", agent_names[0] if agent_names else None)
        agent = agents.get(agent_key) or (next(iter(agents.values())) if agents else None)
        kwargs: Dict[str, Any] = dict(
            description=cfg["description"],
            expected_output=cfg["expected_output"],
            agent=agent,
            tools=_build_tools(cfg.get("tools", [])),
        )
        guardrail = _make_task_guardrail(cfg)
        if guardrail is not None:
            kwargs["guardrail"] = guardrail
        tasks.append(Task(**kwargs))
    return tasks


def build_crew(
    mcp_by_server: Dict[str, List[Any]] | None = None,
    conversation_id: Optional[str] = None,
) -> Crew:
    """Assemble the crew; inject runtime MCP tools into the agents when provided.

    Honors the crew's configured process (sequential/hierarchical), planning,
    reasoning and memory settings — mirroring how Kasal runs it.

    When ``conversation_id`` is given, the crew's step/task callbacks abort the
    kickoff (raising :class:`cancel.CrewCancelled`) as soon as that conversation
    is flagged for cancellation — a cooperative Stop that halts before the next
    LLM call rather than running to completion.
    """
    agents = _build_agents(mcp_by_server)
    # Planning runs when the crew is configured for it OR deep mode is active
    # (deep = planning + reasoning).
    planning_on = PLANNING or _current_mode() == "deep"
    kwargs: Dict[str, Any] = dict(
        agents=list(agents.values()),
        tasks=_build_tasks(agents),
        process=Process.hierarchical if PROCESS == "hierarchical" else Process.sequential,
        memory=MEMORY,
        planning=planning_on,
        verbose=True,
    )
    if conversation_id:
        def _abort_if_cancelled(_output: Any = None) -> None:
            if cancel.is_cancelled(conversation_id):
                raise cancel.CrewCancelled(conversation_id)

        kwargs["step_callback"] = _abort_if_cancelled
        kwargs["task_callback"] = _abort_if_cancelled
    if planning_on:
        # Always give the planner an explicit LLM. Without one, CrewAI's planning
        # step falls back to a default OpenAI model and 401s when no OPENAI_API_KEY
        # is set (deep mode enables planning even when the crew configured no
        # PLANNING_LLM). Route through _make_llm so the local/Databricks rules apply.
        kwargs["planning_llm"] = _make_llm(PLANNING_LLM or _conversation_model())
    if PROCESS == "hierarchical":
        # Hierarchical needs a manager; use the configured manager LLM, else the
        # override/default model.
        kwargs["manager_llm"] = _make_llm(MANAGER_LLM or MODEL_OVERRIDE or "databricks-llama-4-maverick")
    return Crew(**kwargs)


def _mcp_auth() -> tuple:
    """Resolve (host, auth_headers) for Databricks-managed MCP servers.

    Uses ``get_user_workspace_client`` — the requesting user's OBO token when
    present, else the app's service principal (NOT a broken token=None client).
    ``config.authenticate()`` yields a valid Authorization header for any auth
    type (unlike ``config.token`` which is None for OAuth and would 401).
    """
    host = (
        getattr(get_user_workspace_client().config, "host", None)
        or os.environ.get("DATABRICKS_HOST", "")
    ).rstrip("/")
    try:
        db_headers = dict(get_user_workspace_client().config.authenticate() or {})
    except Exception:  # noqa: BLE001
        token = os.environ.get("DATABRICKS_TOKEN", "")
        db_headers = {"Authorization": f"Bearer {token}"} if token else {}
    return host, db_headers


def _mcp_param(name: str, url: str, transport: str, host: str, db_headers: dict) -> dict:
    """Build the connection params for one MCP server.

    Transport MATTERS: Databricks-managed MCP is streamable-HTTP; without it the
    adapter falls back to SSE and times out. Databricks-managed servers (relative
    ``/api/2.0/mcp/...`` URLs) use the app's Databricks identity; third-party
    servers (absolute URLs) use their own ``<NAME>_MCP_TOKEN`` so the Databricks
    token is never sent off-platform.
    """
    if url.startswith("/"):
        return {"url": f"{host}{url}", "transport": transport, "headers": dict(db_headers)}
    env_key = (
        "".join(c if c.isalnum() else "_" for c in name.upper()).strip("_") + "_MCP_TOKEN"
    )
    token = os.environ.get(env_key, "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return {"url": url, "transport": transport, "headers": headers}


def _open_mcp_tools(stack) -> Dict[str, List[Any]]:
    """Connect to each configured MCP server INDEPENDENTLY and return its tools
    keyed by server name, keeping the adapters open via the caller's ExitStack for
    the duration of the crew kickoff.

    Returning a ``{server_name: [tools]}`` map (not a flat union) lets each agent
    receive ONLY the servers it is configured to use (see _build_agents), so a
    server assigned to one agent isn't handed to every agent.

    Per-server isolation is also the point: a server the app can't reach or isn't
    authorized for (e.g. a Genie space the service principal lacks access to) is
    skipped with a log, instead of taking down ALL MCP tools (the old single
    ``MCPServerAdapter([all])`` was all-or-nothing).
    """
    from crewai_tools import MCPServerAdapter

    # Databricks-managed MCP uses RELATIVE urls and needs Databricks auth; that auth
    # path hangs against a dummy host, so in local mode (LOCAL_LLM_BASE_URL set) we
    # skip those servers. Third-party servers (absolute urls, e.g. a local
    # perplexity) need no Databricks identity and work locally.
    local_mode = bool(os.environ.get("LOCAL_LLM_BASE_URL"))
    servers = []
    for name, url, transport in MCP_SERVERS:
        if local_mode and str(url).startswith("/"):
            print(f"MCP '{name}' is Databricks-managed; skipping in local mode.")
            continue
        servers.append((name, url, transport))
    if not servers:
        return {}
    # Only authenticate to Databricks when a Databricks-managed server remains.
    if any(str(u).startswith("/") for _, u, _ in servers):
        host, db_headers = _mcp_auth()
    else:
        host, db_headers = "", {}
    # Drop tools by name. MCP_DISABLED_TOOLS is always off; in chat/research mode
    # the deep tools (FAST_MODE_DISABLED_TOOLS, e.g. perplexity_research) are also
    # dropped so only the fast search/ask tools remain. Deep mode keeps them all.
    disabled = {t.strip() for t in os.environ.get("MCP_DISABLED_TOOLS", "").split(",") if t.strip()}
    if _current_mode() != "deep":
        disabled |= FAST_MODE_DISABLED_TOOLS
    by_server: Dict[str, List[Any]] = {}
    for name, url, transport in servers:
        try:
            adapter = stack.enter_context(
                MCPServerAdapter([_mcp_param(name, url, transport, host, db_headers)])
            )
            all_tools = list(adapter)
            server_tools = [t for t in all_tools if getattr(t, "name", "") not in disabled]
            by_server[name] = server_tools
            dropped = len(all_tools) - len(server_tools)
            note = f" ({dropped} disabled)" if dropped else ""
            print(f"MCP '{name}': {len(server_tools)} tool(s) available{note}")
        except Exception as exc:  # noqa: BLE001
            print(f"MCP '{name}' unavailable ({exc}); skipping that server.")
    return by_server


def _run_crew(inputs: Dict[str, Any]) -> str:
    """Run the predefined crew (its configured tasks) with the given inputs.

    MCP setup is best-effort: if the ``mcp`` package is missing or a server can't
    be reached/authenticated, the crew still runs *without* those tools rather
    than failing the whole request. In local mode, Databricks-managed MCP servers
    are skipped (see _open_mcp_tools) while third-party ones still connect.
    """
    # Bound to the current turn so the crew can be stopped/timed-out cooperatively.
    cid = progress.current()
    if MCP_SERVERS:
        from contextlib import ExitStack

        try:
            with ExitStack() as stack:
                mcp_by_server = _open_mcp_tools(stack)
                return str(
                    build_crew(mcp_by_server=mcp_by_server or None, conversation_id=cid).kickoff(
                        inputs=inputs
                    )
                )
        except cancel.CrewCancelled:
            raise  # a Stop/timeout must abort — never retry (it would re-spend tokens)
        except Exception as exc:  # noqa: BLE001
            print(f"MCP setup failed ({exc}); running crew without MCP.")
    return str(build_crew(conversation_id=cid).kickoff(inputs=inputs))


def _run_conversational(request_text: str) -> str:
    """Run the crew's CONFIGURED pipeline (its agents + tasks) with the user's
    request as the crew input — exactly how Kasal runs the crew.

    This replaces an earlier supervisor+delegation wrapper. Having a supervisor
    agent delegate to the crew's agents is fragile — a weaker model fails the
    ``delegate_work_to_coworker`` call with "coworker mentioned not found" — and
    it doesn't match how Kasal runs the crew. Running the predefined tasks
    directly is robust and produces the same result as in Kasal. Conversational
    clarification still happens in the conversation layer's gather step (not the
    crew), so multi-turn info-gathering is unchanged.
    """
    return _run_crew({INPUT_KEY: request_text})


def _run_chat(request_text: str) -> str:
    """Chat mode: answer with a SINGLE LiteAgent (agent.kickoff) — no crew, no
    task pipeline. Fast tools only (deep MCP tools are dropped in chat mode by
    _open_mcp_tools). For quick, conversational answers grounded in the crew's
    domain, without spinning up the full multi-task crew.
    """
    from contextlib import ExitStack

    with ExitStack() as stack:
        mcp_by_server = _open_mcp_tools(stack) if MCP_SERVERS else {}
        tools = [t for tools in mcp_by_server.values() for t in tools]
        agent = Agent(
            role=f"{NAME} assistant",
            goal=(
                "Answer the user's question helpfully, accurately, and concisely — but "
                "ONLY within this assistant's domain; politely decline anything outside it."
            ),
            backstory=_with_citations(
                f"You are a knowledgeable assistant for this domain: {CREW_PURPOSE}\n"
                "Stay strictly within this domain. If the user asks for something outside "
                "it (a different subject, or a task unrelated to this domain), do NOT "
                "attempt it and do NOT produce its content — briefly and politely say it's "
                "outside what you cover, then give one example of what you can help with. "
                "If the user asks you to create a deliverable but the request is too vague "
                "to do it well (e.g. missing the topic, scope, count, or format), ask ONE "
                "short clarifying question and STOP — do not guess and do not generate the "
                "deliverable until they answer. Within the domain and once the request is "
                "clear, answer directly; use your tools to look up current information when "
                "it helps, but keep it quick and to the point."
            ),
            llm=_make_llm(_conversation_model()),
            tools=tools,
            max_iter=int(os.environ.get("CHAT_MAX_ITER", "6")),
            inject_date=True,  # so "latest/current" questions use today's date
        )
        return str(getattr(agent.kickoff(request_text), "raw", "") or "")


def _conversation_model() -> str:
    """Model the conversation layer's intake/assistant agents use."""
    if MODEL_OVERRIDE:
        return MODEL_OVERRIDE
    for cfg in AGENTS_CONFIG.values():
        if cfg.get("llm"):
            return cfg["llm"]
    return "databricks-llama-4-maverick"


# --- A2UI generative-UI composer ---------------------------------------------
# Generative UI is produced by the ONE shared, stdlib-only composer vendored under
# agent_server/a2ui/ (copied verbatim from Kasal's src.shared.a2ui — the SAME code
# live Kasal chat runs, so there is no second implementation to drift). It turns
# the answer into a single declarative A2UI surface (custom_outputs.a2ui) the
# frontend renders as rich UI. The catalog + per-deliverable directives are baked
# from this workspace's UIConfig at export time (a2ui/catalog.json + config.json).

_A2UI_DIR = Path(__file__).parent / "a2ui"


def _load_a2ui_config() -> Dict[str, Any]:
    """The baked {enabled, directives} for this workspace (export-time UIConfig)."""
    try:
        return json.loads((_A2UI_DIR / "config.json").read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"A2UI config unavailable ({exc}); defaults (enabled, no directives).")
        return {"enabled": True, "directives": {}}


_A2UI_CONFIG = _load_a2ui_config()
_A2UI_CATALOG = _load_a2ui_catalog(str(_A2UI_DIR / "catalog.json"))
# Effective switch: the env flag AND the workspace's baked enabled flag.
_A2UI_ON = A2UI_ENABLED and bool(_A2UI_CONFIG.get("enabled", True))


@mlflow.trace(name="compose_a2ui")
def _compose_a2ui(
    output_text: str, purpose: str = "", hints: str = "", query: str = ""
) -> Dict[str, Any]:
    """Compose an A2UI surface from the agent's text answer (generic, never raises).

    Thin adapter over the shared composer: injects this app's LLM (via _make_llm,
    temperature=0 for well-formed JSON) plus the baked catalog and the directive
    for THIS turn's inferred deliverable.
    """

    def _llm_call(messages: List[Dict[str, str]]) -> str:
        out = _make_llm(_conversation_model(), temperature=0).call(messages)
        return out if isinstance(out, str) else str(out)

    try:
        retries = max(1, int(os.environ.get("A2UI_COMPOSE_RETRIES", "2")))
    except (TypeError, ValueError):
        retries = 2

    return _compose_surface(
        output_text,
        purpose,
        hints,
        query,
        llm_call=_llm_call,
        catalog=_A2UI_CATALOG,
        enabled=_A2UI_ON,
        retries=retries,
        guidance=_a2ui_guidance_for(_A2UI_CONFIG.get("directives", {}), query),
    )


def _schedule_a2ui(message: str, output_text: str, session_id: Optional[str]) -> None:
    """Compose the A2UI surface OFF the answer's critical path.

    Databricks Apps time out a long-held request; composing inline (an extra LLM
    call after the crew) would hold the connection open and risk dropping the
    answer. So the turn returns its text immediately and we compose in a daemon
    thread, stashing the surface for ``GET /a2ui/{id}`` to serve (a2ui_store).
    A markdown 'conversation' surface counts as "no rich surface" → stored as none
    so the UI stops polling and just shows the text.
    """
    a2ui_store.begin(session_id)

    def _work() -> None:
        try:
            surface = _compose_a2ui(output_text, CREW_PURPOSE, A2UI_HINT, message)
        except Exception as exc:  # noqa: BLE001 — UI compose must never break a turn
            print(f"A2UI background compose failed ({exc}); no surface.")
            surface = None
        if surface and surface.get("surfaceKind") in (None, "conversation"):
            surface = None
        a2ui_store.put(session_id, surface)

    threading.Thread(target=_work, name="a2ui-compose", daemon=True).start()


# Wire the generic conversation layer to this crew. The "act" step runs the
# crew's configured agents + tasks (the same pipeline Kasal runs).
conversation.configure(
    name=NAME,
    input_key=INPUT_KEY,
    purpose=CREW_PURPOSE,
    llm_factory=lambda: _make_llm(_conversation_model()),
    crew_runner=_run_conversational,
    chat_runner=_run_chat,
)


def _latest_user_message(request: ResponsesAgentRequest) -> str:
    """Extract the text of the latest user message."""
    for item in reversed(request.input):
        data = item if isinstance(item, dict) else item.model_dump()
        if data.get("role") == "user":
            content = data.get("content")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                return " ".join(
                    part.get("text", "")
                    for part in content
                    if isinstance(part, dict)
                    and part.get("type") in ("input_text", "text", "output_text")
                )
    return ""


def _message_item(text: str) -> Dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    }


def _respond_and_trace(
    message: str, conversation_id: Optional[str], user_id: Optional[str] = None
) -> tuple:
    """Run the (traced) conversation turn and capture its MLflow trace id.

    Runs in a worker thread; the trace id is read in the SAME thread right after
    the turn, before the A2UI composer creates its own (separate) trace.
    """
    text = conversation.respond(message, conversation_id, user_id)
    trace_id = None
    try:
        trace_id = mlflow.get_last_active_trace_id()
    except Exception:  # noqa: BLE001 — older MLflow / no trace configured
        try:
            tr = mlflow.get_last_active_trace()
            info = getattr(tr, "info", None)
            trace_id = getattr(info, "trace_id", None) or getattr(info, "request_id", None)
        except Exception:  # noqa: BLE001
            trace_id = None
    return text, trace_id


def get_trace_view(trace_id: str) -> Dict[str, Any]:
    """Serialize an MLflow trace's spans for the IN-APP trace viewer.

    The app's own credentials own the experiment, so authenticated end users —
    who typically have no MLflow UI access — can still inspect the agent's steps
    (tool calls, LLM calls, timings, I/O). Never raises; returns {error,...} on
    failure so the UI can show a friendly message.
    """
    try:
        trace = mlflow.get_trace(trace_id)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Trace unavailable: {exc}", "spans": []}
    if trace is None:
        return {"error": "Trace not found.", "spans": []}

    def _short(v: Any) -> Optional[str]:
        if v is None:
            return None
        try:
            txt = v if isinstance(v, str) else json.dumps(v, default=str, ensure_ascii=False)
        except Exception:  # noqa: BLE001
            txt = str(v)
        return txt[:4000]

    spans: list = []
    for s in getattr(trace.data, "spans", []) or []:
        start = getattr(s, "start_time_ns", None)
        end = getattr(s, "end_time_ns", None)
        status = getattr(s, "status", None)
        status_code = getattr(status, "status_code", status)
        span_type = getattr(s, "span_type", None)
        if span_type is None:
            try:
                span_type = s.attributes.get("mlflow.spanType")
            except Exception:  # noqa: BLE001
                span_type = None
        spans.append(
            {
                "id": getattr(s, "span_id", None),
                "parentId": getattr(s, "parent_id", None),
                "name": getattr(s, "name", "span"),
                "type": str(span_type).strip('"') if span_type else None,
                "status": str(status_code) if status_code is not None else None,
                "_startNs": start,
                "durationMs": ((end - start) / 1e6) if (start and end) else None,
                "inputs": _short(getattr(s, "inputs", None)),
                "outputs": _short(getattr(s, "outputs", None)),
            }
        )
    starts = [s["_startNs"] for s in spans if s["_startNs"]]
    t0 = min(starts) if starts else 0
    for s in spans:
        s["startMs"] = ((s["_startNs"] - t0) / 1e6) if s["_startNs"] else 0
        del s["_startNs"]
    total = None
    if spans:
        total = max((s["startMs"] + (s["durationMs"] or 0)) for s in spans)
    return {"trace_id": trace_id, "spans": spans, "totalMs": total}


async def _run_turn(
    message: str, session_id: Optional[str], user_id: Optional[str], mode: str
) -> str:
    """Run one turn in a worker thread with a hard wall-clock cap.

    The crew runs synchronously in a thread (CrewAI refuses a live event loop and
    a thread can't be force-killed), so on timeout we flip the cooperative cancel
    flag: the crew's callbacks abort it before the next LLM call. We clear any
    stale flag first so a previous turn's Stop/timeout never kills this one.

    ``mode`` (chat|research|deep) selects the execution depth; it's also set on the
    _CURRENT_MODE contextvar by the caller so it propagates into the worker thread.
    """
    cancel.clear(session_id)
    task = asyncio.ensure_future(
        asyncio.to_thread(conversation.respond, message, session_id, user_id, mode)
    )
    if not CREW_TIMEOUT_SECONDS or CREW_TIMEOUT_SECONDS <= 0:
        return await task
    try:
        return await asyncio.wait_for(asyncio.shield(task), CREW_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        cancel.request(session_id)  # stop spending; crew unwinds at the next step
        try:
            return await asyncio.wait_for(asyncio.shield(task), 60)
        except asyncio.TimeoutError:
            return "Stopped: this request took too long. Please try again or narrow it down."


@invoke()
async def invoke_agent(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    # The conversation layer (and crew kickoff) is synchronous and CrewAI refuses
    # to run from a live event loop — offload it to a worker thread.
    message = _latest_user_message(request)
    # Capture session + user identity ON the request thread (the forwarded
    # headers are gone inside asyncio.to_thread); respond() tags the trace.
    session_id = get_session_id(request)
    user_id = get_user_id(request)
    # Answer depth (chat|research|deep); set on the contextvar so it propagates
    # into the worker thread where the crew is built.
    mode = _extract_mode(request)
    _CURRENT_MODE.set(mode)
    try:
        output_text = await _run_turn(message, session_id, user_id, mode)
        # Compose the A2UI surface OFF this request's critical path (Databricks
        # Apps time out long-held connections); the UI polls GET /a2ui/{id} for it.
        _schedule_a2ui(message, output_text, session_id)
    finally:
        progress.clear(session_id)  # the turn is done — drop the live status
    return ResponsesAgentResponse(output=[_message_item(output_text)])


@stream()
async def stream_agent(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    # The conversation runs to completion before producing the final answer, so the
    # stream emits a single terminal output item rather than incremental tokens.
    message = _latest_user_message(request)
    session_id = get_session_id(request)
    user_id = get_user_id(request)
    mode = _extract_mode(request)
    _CURRENT_MODE.set(mode)
    try:
        output_text = await _run_turn(message, session_id, user_id, mode)
        # Surface composed out-of-band (see invoke_agent); the UI polls /a2ui/{id}.
        _schedule_a2ui(message, output_text, session_id)
    finally:
        progress.clear(session_id)  # the turn is done — drop the live status
    yield ResponsesAgentStreamEvent(
        type="response.output_item.done",
        item=_message_item(output_text),
    )
