"""Service for crew generation operations.

Turns a natural-language description into a persisted crew. There are four ways
to do that and they share almost nothing but the LLM client, so each lives in its
own module under ``crew_generation/`` and is mixed in below:

* ``CompleteGenerationMixin``     — one-shot ``/create-crew``
* ``ProgressiveGenerationMixin``  — streaming ``/create-crew-streaming`` (canvas chat + ChatMode)
* ``ConversationGenerationMixin`` — ``/from-conversation``
* ``ChatFastPathMixin``           — ChatMode's single-agent answer mode
* ``RecipeHooksMixin``            — workflow-recipe reuse, used by the strategies above

What stays HERE is the shared infrastructure every strategy needs: the session and
repositories, LLM-interaction logging, prompt-template assembly, tool resolution,
and the crew-config builder.

Mixins rather than composition, deliberately: the split is then pure movement —
every method still reads ``self`` exactly as it did in the single 2,332-line file,
and no call site changes.
"""

import logging
import traceback
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from src.repositories.crew_generator_repository import CrewGeneratorRepository
from src.repositories.log_repository import LLMLogRepository
from src.services.catalog.templates import TemplateService
from src.services.execution.logs.llm_log_service import LLMLogService
from src.services.generation.crew import (
    ChatFastPathMixin,
    CompleteGenerationMixin,
    ConversationGenerationMixin,
    ProgressiveGenerationMixin,
    RecipeHooksMixin,
)
from src.services.tools.tool_service import ToolService
from src.utils.user_context import GroupContext

if TYPE_CHECKING:  # imported for the annotation only, no runtime cost
    from src.schemas.crew import CrewStreamingRequest

logger = logging.getLogger(__name__)


class CrewGenerationService(
    CompleteGenerationMixin,
    ProgressiveGenerationMixin,
    ConversationGenerationMixin,
    ChatFastPathMixin,
    RecipeHooksMixin,
):
    """Service for crew generation operations."""

    def __init__(self, session: Any):
        """
        Initialize the service with database session.

        Args:
            session: Database session from dependency injection
        """
        self.session = session
        # Initialize log service with repository using the same session
        self.log_service = LLMLogService(LLMLogRepository(session))
        self.tool_service = None  # Will be initialized when needed
        # Initialize the crew generator repository with session
        self.crew_generator_repository = CrewGeneratorRepository(session)
        logger.info("Initialized CrewGeneratorRepository during service creation")

    async def _log_llm_interaction(
        self,
        endpoint: str,
        prompt: str,
        response: str,
        model: str,
        status: str = "success",
        error_message: str = None,
        group_context: Optional[GroupContext] = None,
    ) -> None:
        """
        Log LLM interaction using the log service.

        Args:
            endpoint: API endpoint that was called
            prompt: Input prompt text
            response: Response from the LLM
            model: Model used for generation
            status: Status of the interaction ('success' or 'error')
            error_message: Error message if status is 'error'
        """
        try:
            await self.log_service.create_log(
                endpoint=endpoint,
                prompt=prompt,
                response=response,
                model=model,
                status=status,
                error_message=error_message,
                group_context=group_context,
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    async def _prepare_prompt_template(
        self,
        tools: List[Dict[str, Any]],
        group_context: Optional[GroupContext],
        prompt: Optional[str] = None,
    ) -> str:
        """
        Prepare the prompt template (with group/user appended overrides) and tool descriptions.

        Args:
            tools: List of tool dictionaries, each containing name, description, parameters, etc.
            group_context: Current request's group context

        Returns:
            str: Complete system message with tools context

        Raises:
            ValueError: If prompt template is not found
        """
        # Get composed prompt template from database using the TemplateService
        system_message = await TemplateService.get_effective_template_content(
            "generate_crew", group_context
        )

        if not system_message:
            raise ValueError(
                "Required prompt template 'generate_crew' not found in database"
            )

        # NOTE: the generation templates are format-neutral (content/structure only,
        # never HTML/CSS/JS). Output formatting is owned entirely by the shared A2UI
        # composer (a2ui_runner), which composes a surface post-execution, so no
        # per-call directive is prepended here.

        # Build tools context for the prompt with detailed descriptions
        tools_context = ""
        if tools:
            tools_context = "\n\nAvailable tools:\n"
            for tool in tools:
                # Generation only needs the tool NAME + short description to decide
                # assignments; parameter schemas matter at execution, not here.
                # Omitting them keeps the prompt small (it scales with tool count)
                # without changing which tools the model picks.
                name = tool.get("name", "Unknown Tool")
                description = tool.get("description", "No description available")
                tools_context += f"- {name}: {description}\n"

            tools_context += "\n\nEnsure that agents and tasks only use tools from this list. Assign tools to agents based on their capabilities and the tools' functionalities."

            # Add specific usage example for the NL2SQLTool if it's in the tools list
            if any(tool.get("name") == "NL2SQLTool" for tool in tools):
                tools_context += "\n\nFor NL2SQLTool, use the following format for input: {'sql_query': <your_query>}"

        # Bias the generator toward GenieTool for internal-data questions when it's
        # available — otherwise Auto-format prompts ("most effective campaign") get a
        # web-research crew (Perplexity) and never surface the Genie-space picker.
        if any(tool.get("name") == "GenieTool" for tool in (tools or [])):
            from src.seeds.prompt_templates import GENIE_ROUTING_DIRECTIVE

            tools_context += f"\n{GENIE_ROUTING_DIRECTIVE}"
            logger.info(
                "[GenieRouting] generate_crew: applied Genie routing directive (GenieTool available)"
            )

        # Add tools context to the system message. Exemplars are appended by the
        # caller: it owns the recipe decision (including the holdout arm) because
        # it is also what records the trial once generation has produced ids.
        return system_message + tools_context

    def _safe_get_attr(self, obj, attr, default=None):
        """
        Safely get an attribute from an object, whether it's a dictionary or an object.

        Args:
            obj: The object or dictionary to get the attribute from
            attr: The attribute name to get
            default: The default value to return if the attribute is not found

        Returns:
            The attribute value or default
        """
        if hasattr(obj, "get") and callable(obj.get):
            # Dictionary-like access
            return obj.get(attr, default)
        elif hasattr(obj, attr):
            # Object attribute access
            return getattr(obj, attr, default)
        else:
            return default

    def _create_tool_name_to_id_map(
        self, tools: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        Create a mapping from tool names to tool IDs.

        Args:
            tools: List of tool dictionaries

        Returns:
            Dict mapping tool names to their IDs
        """
        name_to_id = {}
        for tool in tools:
            # Use title as name if available
            name = tool.get("title") or tool.get("name")
            tool_id = tool.get("id")

            if name and tool_id:
                # Ensure ID is a string
                name_to_id[name] = str(tool_id)

                # Also add the original name as a key if different from title
                if "name" in tool and tool["name"] != name:
                    name_to_id[tool["name"]] = str(tool_id)

        return name_to_id

    async def _get_tool_details(
        self, tool_identifiers: List[Any], tool_service: ToolService
    ) -> List[Dict[str, Any]]:
        """
        Get detailed information about tools from the tool service.

        This handles different possible input formats:
        - List of strings (tool names or IDs)
        - List of dictionaries with at least 'name' or 'id' fields

        Args:
            tool_identifiers: List of tool identifiers in any supported format
            tool_service: ToolService instance to use for retrieving tool details

        Returns:
            List of dictionaries with complete tool details
        """
        detailed_tools = []

        try:
            # Get all available tools using the provided service
            tools_response = await tool_service.get_all_tools()
            all_tools = tools_response.tools
            logger.info(f"Retrieved {len(all_tools)} tools from tool service")

            # Create lookup maps for faster tool retrieval
            tools_by_name = {
                tool.title: tool for tool in all_tools if hasattr(tool, "title")
            }
            tools_by_id = {
                str(tool.id): tool for tool in all_tools if hasattr(tool, "id")
            }

            # Process each tool identifier
            for identifier in tool_identifiers:
                tool_detail = None

                if isinstance(identifier, str):
                    # Check if it's a name or ID
                    if identifier in tools_by_name:
                        tool_detail = tools_by_name[identifier]
                    elif identifier in tools_by_id:
                        tool_detail = tools_by_id[identifier]
                    else:
                        logger.warning(f"Tool not found: {identifier}")
                        # Add a placeholder with just the name
                        detailed_tools.append(
                            {
                                "name": identifier,
                                "description": f"A tool named {identifier}",
                                "id": identifier,
                            }
                        )
                        continue

                elif isinstance(identifier, dict):
                    # Extract name or ID from dictionary
                    name = identifier.get("name")
                    tool_id = identifier.get("id")

                    if name and name in tools_by_name:
                        tool_detail = tools_by_name[name]
                    elif tool_id and str(tool_id) in tools_by_id:
                        tool_detail = tools_by_id[str(tool_id)]
                    elif name:
                        # If we have a name but no match, add it as is
                        logger.warning(f"Tool not found by name: {name}")
                        detailed_tools.append(
                            {
                                "name": name,
                                "description": identifier.get(
                                    "description", f"A tool named {name}"
                                ),
                                "id": tool_id
                                or name,  # Use ID if available, otherwise use name
                            }
                        )
                        continue
                    else:
                        logger.warning(
                            f"Invalid tool identifier, missing name or id: {identifier}"
                        )
                        continue
                else:
                    logger.warning(f"Unknown tool identifier format: {identifier}")
                    continue

                # Convert tool to dictionary with all details
                if tool_detail:
                    if hasattr(tool_detail, "model_dump"):
                        tool_dict = tool_detail.model_dump()
                    else:
                        # If it's already a dictionary or has __dict__
                        tool_dict = (
                            tool_detail.__dict__
                            if hasattr(tool_detail, "__dict__")
                            else dict(tool_detail)
                        )

                    # Ensure we have name and description
                    if "name" not in tool_dict and hasattr(tool_detail, "title"):
                        tool_dict["name"] = tool_detail.title
                    if "description" not in tool_dict and hasattr(
                        tool_detail, "description"
                    ):
                        tool_dict["description"] = tool_detail.description

                    detailed_tools.append(tool_dict)

            logger.info(
                f"Processed {len(detailed_tools)} tools with detailed information"
            )
            return detailed_tools

        except Exception as e:
            logger.error(f"Error retrieving tool details: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

            # Fall back to basic processing if tool service fails
            return [
                {
                    "name": t if isinstance(t, str) else t.get("name", "Unknown"),
                    "description": f"A tool named {t if isinstance(t, str) else t.get('name', 'Unknown')}",
                    "id": (
                        t
                        if isinstance(t, str)
                        else t.get("id", t.get("name", "Unknown"))
                    ),
                }
                for t in tool_identifiers
            ]

    @staticmethod
    async def _has_persistent_memory_backend(session, group_context) -> bool:
        """Whether the group has a real (persistent) memory backend configured.

        Only Databricks Vector Search and Lakebase count — the default
        (ephemeral ChromaDB/LanceDB) backend does not persist in a deployed
        Databricks App, so when neither is configured we disable memory on
        generated agents rather than silently writing to throwaway local storage.
        """
        try:
            primary_group_id = group_context.primary_group_id if group_context else None
            if not primary_group_id:
                return False
            from src.models.memory_backend import MemoryBackendTypeEnum
            from src.repositories.memory_backend_repository import (
                MemoryBackendRepository,
            )

            mem_repo = MemoryBackendRepository(session)
            for backend_type in (
                MemoryBackendTypeEnum.DATABRICKS,
                MemoryBackendTypeEnum.LAKEBASE,
            ):
                if await mem_repo.get_by_type(primary_group_id, backend_type):
                    return True
            return False
        except Exception as e:
            logger.warning(
                f"Memory backend check failed, assuming no persistent memory: {e}"
            )
            return False

    @staticmethod
    def build_crew_config_from_generated(
        request: "CrewStreamingRequest",
        agent_results: List[Dict[str, Any]],
        clean_tasks: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build an executable crew config (CrewConfig dict) from the agents/tasks
        just produced by progressive generation.

        Backend mirror of the frontend ``buildCrewConfigFromGenerated`` used by
        ChatMode: keys agents as ``agent_<id>`` and tasks as ``task_<id>``, injects
        the selected MCP servers into each agent's and task's ``tool_configs``,
        grounds task descriptions with the user's request, resolves task→agent and
        task→task (context) links, and applies the chat memory settings. Used only
        for ChatMode auto-execute (``request.auto_execute``); the AgentBuilder /
        crew canvas never calls this — it renders the plan as nodes instead.
        """
        mcp_servers = list(request.mcp_servers or [])
        # Files attached in this chat turn. When present, the knowledge search tool
        # is scoped to ONLY these files (by basename), so the run grounds on the
        # just-uploaded document instead of any other file in the group.
        knowledge_file_paths = list(
            getattr(request, "knowledge_file_paths", None) or []
        )
        user_request = request.original_prompt or request.prompt

        # Agent Bricks endpoints picked in the chat "+" menu. This backend builder is
        # the ONLY config path for ChatMode auto-execute (the Crew canvas / AgentBuilder
        # build their run config from saved-node tool_configs instead, which already
        # carry the endpoint — that's why those channels work and chat didn't).
        # Mirror the MCP injection below: when an endpoint is picked, equip + configure
        # the AgentBricksTool (catalog seed id 71) on each agent/task; when NONE is
        # picked, STRIP any AgentBricksTool the generator/LLM equipped on its own, since
        # an unconfigured AgentBricksTool aborts the task ("endpoint name is not configured").
        agentbricks_endpoints = list(
            getattr(request, "agentbricks_endpoints", None) or []
        )
        has_agentbricks = bool(agentbricks_endpoints)
        ABT_ID = "71"  # AgentBricksTool seed id

        def _is_agentbricks_tool(tool: Any) -> bool:
            return str(tool) == ABT_ID or str(tool) == "AgentBricksTool"

        def _apply_agentbricks_tools(tools: List[Any]) -> List[Any]:
            # Only operate on real lists — leave any other value (None, or a loose
            # test mock) untouched, so this never iterates a non-list tools value.
            if not isinstance(tools, list):
                return tools
            if has_agentbricks:
                if not any(_is_agentbricks_tool(t) for t in tools):
                    return [*tools, ABT_ID]
                return tools
            return [t for t in tools if not _is_agentbricks_tool(t)]

        OPTIONAL_AGENT_FIELDS = (
            "llm",
            "function_calling_llm",
            "max_iter",
            "max_rpm",
            "max_execution_time",
            "memory",
            "verbose",
            "allow_delegation",
            "cache",
            "system_template",
            "prompt_template",
            "response_template",
            "allow_code_execution",
            "code_execution_mode",
            "max_retry_limit",
            "use_system_prompt",
            "respect_context_window",
        )

        agents_yaml: Dict[str, Dict[str, Any]] = {}
        agent_id_to_key: Dict[str, str] = {}
        for agent in agent_results:
            aid = str(agent.get("id") or "")
            key = f"agent_{aid}"
            agent_id_to_key[aid] = key
            cfg: Dict[str, Any] = {
                "role": agent.get("role") or "",
                "goal": agent.get("goal") or "",
                "backstory": agent.get("backstory") or "",
                "tools": _apply_agentbricks_tools(agent.get("tools") or []),
            }
            for field in OPTIONAL_AGENT_FIELDS:
                if agent.get(field) is not None:
                    cfg[field] = agent[field]
            # Honor the model picker: when an agent carries no explicit model, use
            # the request's selected model. Without this the chat (light-agent) fast
            # path — whose default agent has no llm — silently falls back to the
            # engine default (databricks-llama-4-maverick), which serializes MCP/Genie
            # tool calls as Pythonic text instead of executing them (no tool runs, no
            # tool trace). The picker model (e.g. claude-sonnet) tool-calls reliably.
            if request.model and not cfg.get("llm"):
                cfg["llm"] = request.model
            if mcp_servers:
                cfg.setdefault("tool_configs", {})["MCP_SERVERS"] = {
                    "servers": mcp_servers
                }
            if knowledge_file_paths:
                cfg.setdefault("tool_configs", {})["DatabricksKnowledgeSearchTool"] = {
                    "file_paths": knowledge_file_paths
                }
            if has_agentbricks:
                cfg.setdefault("tool_configs", {})["AgentBricksTool"] = {
                    "endpointName": agentbricks_endpoints
                }
            # "No memory" mode: force every agent to be created without memory so
            # the backend disables crew memory entirely (nothing recalled/persisted).
            if request.disable_memory:
                cfg["memory"] = False
            agents_yaml[key] = cfg

        tasks_yaml: Dict[str, Dict[str, Any]] = {}
        for task in clean_tasks:
            tid = str(task.get("id") or "")
            key = f"task_{tid}"
            agent_id = str(task.get("agent_id") or task.get("agent") or "")
            agent_key = agent_id_to_key.get(agent_id)
            context: List[str] = []
            for dep in task.get("context") or []:
                if isinstance(dep, str):
                    context.append(f"task_{dep}")
                elif isinstance(dep, dict) and dep.get("id"):
                    context.append(f"task_{dep['id']}")
            # Generated task descriptions are often generic mission statements;
            # ground the run with the chat prompt + attached MCP sources so the
            # crew has a concrete question and actually queries its tools.
            base_desc = str(task.get("description") or "")
            grounding: List[str] = []
            if user_request:
                grounding.append(
                    f"USER REQUEST — this run exists to answer it:\n{user_request}"
                )
            if mcp_servers:
                grounding.append(
                    f"MCP data sources attached — query them for data questions: {', '.join(mcp_servers)}"
                )
            if has_agentbricks:
                grounding.append(
                    "An Agent Bricks agent is assigned to this task — use the AgentBricksTool to "
                    "delegate the request to it and base your answer on its response: "
                    f"{', '.join(agentbricks_endpoints)}"
                )
            description = (
                f"{base_desc}\n\n" + "\n\n".join(grounding) if grounding else base_desc
            )
            entry: Dict[str, Any] = {
                "id": tid,
                "description": description,
                "expected_output": task.get("expected_output") or "",
                "tools": _apply_agentbricks_tools(task.get("tools") or []),
                "context": context,
                "agent": agent_key,
                "async_execution": bool(task.get("async_execution")),
                "output_file": task.get("output_file") or f"output/{tid}.md",
            }
            if mcp_servers:
                entry.setdefault("tool_configs", {})["MCP_SERVERS"] = {
                    "servers": mcp_servers
                }
            if knowledge_file_paths:
                entry.setdefault("tool_configs", {})[
                    "DatabricksKnowledgeSearchTool"
                ] = {"file_paths": knowledge_file_paths}
            if has_agentbricks:
                entry.setdefault("tool_configs", {})["AgentBricksTool"] = {
                    "endpointName": agentbricks_endpoints
                }
            tasks_yaml[key] = entry

        # ── ChatMode answer mode → reasoning budget / execution_type ──────────
        # The mode now selects the MODEL's native reasoning budget (thinking effort)
        # rather than a prompted planner — the engine has no planner/replan loop, so
        # the old `planning` / `planning_llm` flags were a silent no-op and are gone.
        # chat     = single light agent (Agent.kickoff_async), no crew, no extra thinking
        # research = crew with a medium reasoning budget
        # deep     = crew with a high reasoning budget
        # The effort rides in inputs.reasoning_config and is applied per agent to the
        # agent's own LLM by the shared agent builder, capability-gated per model
        # (unsupported models drop it silently — see utils/model_config).
        _mode = getattr(request, "chat_mode_type", "chat") or "chat"
        _reasoning_effort = {"research": "medium", "deep": "high"}.get(_mode)
        _reasoning = _reasoning_effort is not None
        _execution_type = "agent" if _mode == "chat" else "crew"

        return {
            "agents_yaml": agents_yaml,
            "tasks_yaml": tasks_yaml,
            # reasoning_config rides in inputs — prepare_and_run_crew reads it from
            # inputs_with_run_name.get("reasoning_config"), not a top-level key.
            "inputs": (
                {"reasoning_config": {"reasoning_effort": _reasoning_effort}}
                if _reasoning_effort
                else {}
            ),
            "reasoning": _reasoning,
            "model": request.model or None,
            "execution_type": _execution_type,
            "schema_detection_enabled": True,
            "session_id": request.session_id,
            "memory_workspace_scope": request.memory_workspace_scope,
        }
