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
import os
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List

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
from agent_server.utils import get_session_id, get_user_workspace_client

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
# Per-task output guardrails (task_name -> spec). LLM guardrails are reproduced
# as CrewAI LLMGuardrail; "code" guardrails are Kasal built-ins not bundled here.
TASK_GUARDRAILS = {{TASK_GUARDRAILS}}
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
    """
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
            timeout=300,
        )
    endpoint = (
        model_name.split("/", 1)[1]
        if str(model_name).startswith("databricks/")
        else model_name
    )
    # LiteLLM's Databricks provider appends /chat/completions to api_base:
    # AI Gateway on -> /ai-gateway/mlflow/v1 ; off -> /serving-endpoints.
    kwargs = {"model": f"databricks/{endpoint}", "temperature": temperature}
    if host:
        kwargs["api_base"] = (
            f"{host}/ai-gateway/mlflow/v1" if _gateway_on() else f"{host}/serving-endpoints"
        )
    if token:
        kwargs["api_key"] = token
    return LLM(**kwargs)


def _build_agents(mcp_tools: List[Any] | None = None) -> Dict[str, Agent]:
    agents: Dict[str, Agent] = {}
    for name, cfg in AGENTS_CONFIG.items():
        llm_model = MODEL_OVERRIDE or cfg.get("llm", "databricks-llama-4-maverick")
        llm = _make_llm(llm_model, cfg.get("temperature", 0.7))
        agents[name] = Agent(
            role=cfg["role"],
            goal=cfg["goal"],
            backstory=cfg["backstory"],
            llm=llm,
            tools=_build_tools(cfg.get("tools", []), mcp_tools),
            verbose=cfg.get("verbose", True),
            allow_delegation=cfg.get("allow_delegation", False),
            max_iter=cfg.get("max_iter", 25),
            # Reasoning is an agent-level setting in CrewAI; Kasal applies the
            # crew-level toggle to every agent.
            reasoning=cfg.get("reasoning", REASONING),
        )
    return agents


def _make_task_guardrail(task_name: str):
    """Build the output guardrail for a task, or None.

    LLM guardrails (configured on the task in Kasal) are reproduced with CrewAI's
    native ``LLMGuardrail``. Kasal built-in code/factory guardrails can't be
    bundled standalone, so they're flagged and skipped.
    """
    spec = TASK_GUARDRAILS.get(task_name)
    if not spec:
        return None
    if spec.get("type") == "llm":
        try:
            from crewai.tasks.llm_guardrail import LLMGuardrail

            llm = _make_llm(spec.get("llm_model") or MODEL_OVERRIDE or _conversation_model())
            return LLMGuardrail(
                description=spec.get("description") or "Validate the task output",
                llm=llm,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"Could not build LLM guardrail for task '{task_name}': {exc}")
            return None
    print(
        f"Task '{task_name}' has a Kasal built-in guardrail "
        f"'{spec.get('name')}' that is not reproduced in this app."
    )
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
        guardrail = _make_task_guardrail(task_name)
        if guardrail is not None:
            kwargs["guardrail"] = guardrail
        tasks.append(Task(**kwargs))
    return tasks


def build_crew(mcp_tools: List[Any] | None = None) -> Crew:
    """Assemble the crew; inject runtime MCP tools into the agents when provided.

    Honors the crew's configured process (sequential/hierarchical), planning,
    reasoning and memory settings — mirroring how Kasal runs it.
    """
    agents = _build_agents(mcp_tools)
    kwargs: Dict[str, Any] = dict(
        agents=list(agents.values()),
        tasks=_build_tasks(agents),
        process=Process.hierarchical if PROCESS == "hierarchical" else Process.sequential,
        memory=MEMORY,
        planning=PLANNING,
        verbose=True,
    )
    if PLANNING and PLANNING_LLM:
        kwargs["planning_llm"] = _make_llm(PLANNING_LLM)
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


def _open_mcp_tools(stack) -> List[Any]:
    """Connect to each configured MCP server INDEPENDENTLY and return the union of
    their tools, keeping the adapters open via the caller's ExitStack for the
    duration of the crew kickoff.

    Per-server isolation is the point: a server the app can't reach or isn't
    authorized for (e.g. a Genie space the service principal lacks access to) is
    skipped with a log, instead of taking down ALL MCP tools (the old single
    ``MCPServerAdapter([all])`` was all-or-nothing).
    """
    from crewai_tools import MCPServerAdapter

    host, db_headers = _mcp_auth()
    tools: List[Any] = []
    for name, url, transport in MCP_SERVERS:
        try:
            adapter = stack.enter_context(
                MCPServerAdapter([_mcp_param(name, url, transport, host, db_headers)])
            )
            server_tools = list(adapter)
            tools.extend(server_tools)
            print(f"MCP '{name}': {len(server_tools)} tool(s) available")
        except Exception as exc:  # noqa: BLE001
            print(f"MCP '{name}' unavailable ({exc}); skipping that server.")
    return tools


def _run_crew(inputs: Dict[str, Any]) -> str:
    """Run the predefined crew (its configured tasks) with the given inputs.

    MCP setup is best-effort: if the ``mcp`` package is missing or a server can't
    be reached/authenticated, the crew still runs *without* those tools rather
    than failing the whole request.
    """
    if MCP_SERVERS:
        from contextlib import ExitStack

        try:
            with ExitStack() as stack:
                mcp_tools = _open_mcp_tools(stack)
                return str(
                    build_crew(mcp_tools=mcp_tools or None).kickoff(inputs=inputs)
                )
        except Exception as exc:  # noqa: BLE001
            print(f"MCP setup failed ({exc}); running crew without MCP.")
    return str(build_crew().kickoff(inputs=inputs))


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


def _conversation_model() -> str:
    """Model the conversation layer's intake/assistant agents use."""
    if MODEL_OVERRIDE:
        return MODEL_OVERRIDE
    for cfg in AGENTS_CONFIG.values():
        if cfg.get("llm"):
            return cfg["llm"]
    return "databricks-llama-4-maverick"


# Wire the generic conversation layer to this crew. The "act" step runs the
# crew's configured agents + tasks (the same pipeline Kasal runs).
conversation.configure(
    name=NAME,
    input_key=INPUT_KEY,
    purpose=CREW_PURPOSE,
    llm_factory=lambda: _make_llm(_conversation_model()),
    crew_runner=_run_conversational,
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


@invoke()
async def invoke_agent(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    # The conversation layer (and crew kickoff) is synchronous and CrewAI refuses
    # to run from a live event loop — offload it to a worker thread.
    message = _latest_user_message(request)
    output_text = await asyncio.to_thread(
        conversation.respond, message, get_session_id(request)
    )
    return ResponsesAgentResponse(output=[_message_item(output_text)])


@stream()
async def stream_agent(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    # The conversation runs to completion before producing the final answer, so the
    # stream emits a single terminal output item rather than incremental tokens.
    message = _latest_user_message(request)
    output_text = await asyncio.to_thread(
        conversation.respond, message, get_session_id(request)
    )
    yield ResponsesAgentStreamEvent(type="response.output_item.done", item=_message_item(output_text))
