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

# Point tracing at the experiment configured on the app (MLFLOW_EXPERIMENT_ID is
# set in app.yaml by the Kasal deploy), then capture CrewAI + LLM traces there.
# Logged (not silent) so it's clear in the app logs whether tracing is wired.
_experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID")
if _experiment_id:
    try:
        mlflow.set_experiment(experiment_id=_experiment_id)
        print(f"MLflow tracing -> experiment {_experiment_id}")
    except Exception as e:  # noqa: BLE001
        print(f"Could not set MLflow experiment {_experiment_id}: {e}")
else:
    print(
        "MLFLOW_EXPERIMENT_ID is not set — traces will not be written. "
        "Choose an MLflow experiment in the Kasal deploy screen and redeploy."
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
# MCP servers auto-attached to the crew, as (name, url) pairs.
MCP_SERVERS = [
{{MCP_SERVERS}}]
# Crew execution settings — mirror how Kasal runs this crew.
PROCESS = "{{PROCESS}}"  # 'sequential' or 'hierarchical'
PLANNING = {{PLANNING}}  # plan all tasks up front before execution
PLANNING_LLM = {{PLANNING_LLM}}  # None, or a model name for the planner
REASONING = {{REASONING}}  # agents reason/reflect before acting
MANAGER_LLM = {{MANAGER_LLM}}  # None, or a model name for the hierarchical manager
MEMORY = {{MEMORY}}  # enable CrewAI memory
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


def _make_llm(model_name: str, temperature: float = 0.7):
    """Build the LLM for an agent.

    Databricks models normally route through LiteLLM with a ``databricks/`` prefix
    (``LLM(model="databricks/<endpoint>")``). gpt-5-3-codex is the exception — the
    Chat Completions route returns 404 "Supervisor API is not enabled", so it must
    use the Databricks Responses API instead.
    """
    if _is_codex_model(model_name):
        from databricks.sdk import WorkspaceClient
        from crewai.llms.providers.openai.completion import OpenAICompletion

        w = get_user_workspace_client() if ENABLE_OBO else WorkspaceClient()
        host = (
            getattr(w.config, "host", None) or os.environ.get("DATABRICKS_HOST", "")
        ).rstrip("/")
        try:
            token = (w.config.authenticate() or {}).get("Authorization", "").split(" ", 1)[-1]
        except Exception:  # noqa: BLE001
            token = os.environ.get("DATABRICKS_TOKEN", "")
        # AI Gateway on -> /ai-gateway/openai/v1 ; off -> /serving-endpoints (default).
        gateway_on = os.environ.get("DATABRICKS_AI_GATEWAY_ENABLED", "false").lower() in (
            "1",
            "true",
            "yes",
        )
        base_path = "ai-gateway/openai/v1" if gateway_on else "serving-endpoints"
        return OpenAICompletion(
            model=model_name,
            api="responses",
            base_url=f"{host}/{base_path}",
            api_key=token,
            timeout=300,
        )
    model = model_name if str(model_name).startswith("databricks/") else f"databricks/{model_name}"
    return LLM(model=model, temperature=temperature)


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


def _build_tasks(agents: Dict[str, Agent]) -> list:
    tasks = []
    agent_names = list(agents.keys())
    for cfg in TASKS_CONFIG.values():
        agent_key = cfg.get("agent", agent_names[0] if agent_names else None)
        agent = agents.get(agent_key) or (next(iter(agents.values())) if agents else None)
        tasks.append(
            Task(
                description=cfg["description"],
                expected_output=cfg["expected_output"],
                agent=agent,
                tools=_build_tools(cfg.get("tools", [])),
            )
        )
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


def _mcp_server_params() -> list:
    """Resolve MCP connection params; Databricks-managed URLs get host + bearer.

    Uses ``config.authenticate()`` so the Authorization header is valid for any
    auth type — OBO user token, app service-principal OAuth, or PAT. (``config.token``
    is only populated for PAT auth and is None for OAuth, which would 401 the MCP call.)
    """
    from databricks.sdk import WorkspaceClient

    w = get_user_workspace_client() if ENABLE_OBO else WorkspaceClient()
    host = (
        getattr(w.config, "host", None) or os.environ.get("DATABRICKS_HOST", "")
    ).rstrip("/")
    try:
        auth_headers = dict(w.config.authenticate() or {})
    except Exception:  # noqa: BLE001
        token = os.environ.get("DATABRICKS_TOKEN", "")
        auth_headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = []
    for _name, url in MCP_SERVERS:
        full_url = f"{host}{url}" if url.startswith("/") else url
        params.append({"url": full_url, "headers": dict(auth_headers)})
    return params


def _run_crew(inputs: Dict[str, Any]) -> str:
    """Run the predefined crew (its configured tasks) with the given inputs.

    MCP setup is best-effort: if the ``mcp`` package is missing or a server can't
    be reached/authenticated, the crew still runs *without* those tools rather
    than failing the whole request.
    """
    if MCP_SERVERS:
        try:
            from crewai_tools import MCPServerAdapter

            with MCPServerAdapter(_mcp_server_params()) as mcp_tools:
                crew = build_crew(mcp_tools=list(mcp_tools))
                return str(crew.kickoff(inputs=inputs))
        except Exception as exc:  # noqa: BLE001
            print(f"MCP tools unavailable ({exc}); running crew without MCP.")
    return str(build_crew().kickoff(inputs=inputs))


def _build_supervised_crew(request_text: str, mcp_tools: List[Any] | None = None) -> Crew:
    """Build a crew where a supervisor agent delegates the user's request to the
    crew's existing agents (its coworkers).

    Uses a sequential process with the task assigned to the supervisor (which has
    ``allow_delegation=True``) rather than the hierarchical process: hierarchical
    leaves the task's agent unset, which makes ``mlflow.crewai.autolog`` crash
    reading ``task.agent.role`` and is prone to the "coworker not found" bug.
    """
    workers = _build_agents(mcp_tools)
    supervisor = Agent(
        role=f"{NAME} supervisor",
        goal=(
            "Fulfill the user's request by delegating to the most appropriate "
            "specialist coworker(s) and synthesizing their work into a final answer."
        ),
        backstory=(
            "You coordinate a team of specialists. You delegate work to the right "
            "coworker(s) for the request and combine their results."
        ),
        llm=_make_llm(MANAGER_LLM or MODEL_OVERRIDE or _conversation_model()),
        allow_delegation=True,  # gives the Delegate/Ask-coworker tools over the team
        verbose=True,
    )
    task = Task(
        description=(
            f"{CREW_PURPOSE}\n\n"
            "Handle the following conversation/request by delegating to the most "
            f"appropriate specialist coworker(s) on the team:\n{request_text}\n\n"
            "If the request is ambiguous or missing essential details needed to do a "
            "good job, do NOT guess — reply with exactly 'CLARIFY:' followed by one "
            "concise question for the user."
        ),
        expected_output=(
            "A complete, helpful response to the user's request, OR a single line "
            "'CLARIFY: <question>' when essential information is missing."
        ),
        agent=supervisor,  # assigned, so autolog can read the agent's role
    )
    return Crew(
        agents=[supervisor, *workers.values()],
        tasks=[task],
        process=Process.sequential,
        verbose=True,
    )


def _run_supervised(request_text: str) -> str:
    """Run the request through the supervisor that delegates to the team (best-effort MCP)."""
    if MCP_SERVERS:
        try:
            from crewai_tools import MCPServerAdapter

            with MCPServerAdapter(_mcp_server_params()) as mcp_tools:
                return str(_build_supervised_crew(request_text, list(mcp_tools)).kickoff())
        except Exception as exc:  # noqa: BLE001
            print(f"MCP tools unavailable ({exc}); running crew without MCP.")
    return str(_build_supervised_crew(request_text).kickoff())


def _conversation_model() -> str:
    """Model the conversation layer's intake/assistant agents use."""
    if MODEL_OVERRIDE:
        return MODEL_OVERRIDE
    for cfg in AGENTS_CONFIG.values():
        if cfg.get("llm"):
            return cfg["llm"]
    return "databricks-llama-4-maverick"


# Wire the generic conversation layer to this crew. The "act" step delegates the
# user's request to the existing agents via a hierarchical supervisor.
conversation.configure(
    name=NAME,
    input_key=INPUT_KEY,
    purpose=CREW_PURPOSE,
    llm_factory=lambda: _make_llm(_conversation_model()),
    crew_runner=_run_supervised,
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
