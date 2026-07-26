"""
Service for crew generation operations.

This module provides business logic for generating crew setups
using LLM models to convert natural language descriptions into
structured CrewAI configurations.
"""

import json
import logging
import os
import re
import traceback
import uuid
from collections import defaultdict
from typing import Dict, Any, List, Tuple, Optional


from src.utils.prompt_utils import robust_json_parser
from src.services.template_service import TemplateService
from src.services.tool_service import ToolService

from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse, CrewStreamingRequest
from src.schemas.task_generation import TaskGenerationRequest
from src.schemas.task_generation import Agent as TaskGenAgent
from src.repositories.log_repository import LLMLogRepository
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager
from src.core.sse_manager import sse_manager, SSEEvent
from src.core.exceptions import KasalError, BadRequestError
from src.models.agent import Agent
from src.models.task import Task
from src.repositories.crew_generator_repository import CrewGeneratorRepository
from src.services.agent_generation_service import AgentGenerationService
from src.services.task_generation_service import TaskGenerationService
from src.utils.user_context import GroupContext

# Configure logging
logger = logging.getLogger(__name__)

class CrewGenerationService:
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

    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str,
                                  status: str = 'success', error_message: str = None,
                                  group_context: Optional[GroupContext] = None) -> None:
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
                group_context=group_context
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    async def _prepare_prompt_template(self, tools: List[Dict[str, Any]], group_context: Optional[GroupContext], prompt: Optional[str] = None) -> str:
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
        system_message = await TemplateService.get_effective_template_content("generate_crew", group_context)

        if not system_message:
            raise ValueError("Required prompt template 'generate_crew' not found in database")

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
                name = tool.get('name', 'Unknown Tool')
                description = tool.get('description', 'No description available')
                tools_context += f"- {name}: {description}\n"

            tools_context += "\n\nEnsure that agents and tasks only use tools from this list. Assign tools to agents based on their capabilities and the tools' functionalities."

            # Add specific usage example for the NL2SQLTool if it's in the tools list
            if any(tool.get('name') == 'NL2SQLTool' for tool in tools):
                tools_context += "\n\nFor NL2SQLTool, use the following format for input: {'sql_query': <your_query>}"

        # Bias the generator toward GenieTool for internal-data questions when it's
        # available — otherwise Auto-format prompts ("most effective campaign") get a
        # web-research crew (Perplexity) and never surface the Genie-space picker.
        if any(tool.get('name') == 'GenieTool' for tool in (tools or [])):
            from src.seeds.prompt_templates import GENIE_ROUTING_DIRECTIVE
            tools_context += f"\n{GENIE_ROUTING_DIRECTIVE}"
            logger.info("[GenieRouting] generate_crew: applied Genie routing directive (GenieTool available)")

        # Add tools context to the system message. Exemplars are appended by the
        # caller: it owns the recipe decision (including the holdout arm) because
        # it is also what records the trial once generation has produced ids.
        return system_message + tools_context

    async def _prepare_exemplars(self, request: Any, group_context: Optional[GroupContext],
                                 session: Any = None) -> Optional[Any]:
        """Ask the recipe library what it can contribute to this generation.

        Returns the decision (text + candidates + arm) or None when reuse is not
        applicable or errored. Failure is swallowed on purpose: crew generation
        predates this feature and must keep working without it.

        ``session`` is explicit because the progressive path runs as a background
        task AFTER the request-scoped session is closed — it must pass one of its
        own rather than let this reach for ``self.session``.
        """
        prompt = getattr(request, "prompt", None)
        if not prompt or not group_context:
            return None
        try:
            from src.services.workflow_recipe_service import WorkflowRecipeService

            return await WorkflowRecipeService(session or self.session).prepare_exemplars(
                prompt, group_context.group_ids or []
            )
        except Exception as exemplar_err:  # noqa: BLE001
            logger.warning(f"CREATE CREW: exemplar injection skipped: {exemplar_err}")
            return None

    async def _record_recipe_trial(self, decision: Optional[Any], result: Dict[str, Any],
                                   group_context: Optional[GroupContext],
                                   session: Any = None) -> None:
        """Record what the recipe library did for this generation, and what came
        out of it. Best-effort — the service's own recorder never raises."""
        if decision is None:
            return
        try:
            from src.services.workflow_recipe_service import WorkflowRecipeService

            await WorkflowRecipeService(session or self.session).record_trial(
                decision,
                generated=result,
                group_id=group_context.primary_group_id if group_context else None,
                group_email=group_context.group_email if group_context else None,
            )
        except Exception as trial_err:  # noqa: BLE001
            logger.warning(f"CREATE CREW: recipe trial not recorded: {trial_err}")

    async def _isolated_session_ctx(self):
        """A PRIVATE-connection session context, matching the generation flow.

        Never the shared StaticPool ``async_session_factory``: on SQLite that is
        one connection, and a concurrent commit/rollback on it can discard an
        agent this generation already committed, breaking the next task's
        agent_id foreign key. That is a fixed regression with a test guarding it
        — recipe work must not reintroduce it just because its own writes look
        harmless.
        """
        import os as _os

        from src.db.database_router import (
            get_lakebase_config_from_db,
            is_lakebase_enabled,
        )
        from src.db.lakebase_session import get_lakebase_session
        from src.db.session import get_isolated_db_session

        if await is_lakebase_enabled():
            lb_config = await get_lakebase_config_from_db()
            lb_instance = (
                (lb_config or {}).get("instance_name")
                or _os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")
            )
            return get_lakebase_session(lb_instance)
        return get_isolated_db_session()

    async def _recipe_decision_isolated(self, request: Any,
                                        group_context: Optional[GroupContext]) -> Optional[Any]:
        """Exemplar decision on a session of its own.

        The progressive path plans BEFORE it opens its working session (planning
        does no DB writes), and its inherited request session is already closed,
        so retrieval needs a short-lived session that it owns and disposes of.
        """
        try:
            ctx = await self._isolated_session_ctx()
            async with ctx as recipe_session:
                return await self._prepare_exemplars(
                    request, group_context, session=recipe_session
                )
        except Exception as exc:  # noqa: BLE001 — never block generation
            logger.warning(f"PROGRESSIVE: exemplar lookup skipped: {exc}")
            return None

    async def _record_recipe_trial_isolated(self, decision: Optional[Any],
                                            agents: List[Dict[str, Any]],
                                            tasks: List[Dict[str, Any]],
                                            group_context: Optional[GroupContext]) -> None:
        """Trial write on a session of its own, for the same reason."""
        if decision is None:
            return
        try:
            ctx = await self._isolated_session_ctx()
            async with ctx as trial_session:
                await self._record_recipe_trial(
                    decision,
                    {"agents": agents, "tasks": tasks},
                    group_context,
                    session=trial_session,
                )
        except Exception as exc:  # noqa: BLE001 — measurement never breaks a run
            logger.warning(f"PROGRESSIVE: recipe trial not recorded: {exc}")

    def _process_crew_setup(self, setup: Dict[str, Any], allowed_tools: List[Dict[str, Any]], tool_name_to_id_map: Dict[str, str], model: str = None, disable_memory: bool = False) -> Dict[str, Any]:
        """
        Process and validate crew setup.

        Args:
            setup: Raw crew setup from LLM
            allowed_tools: List of allowed tools with descriptions
            tool_name_to_id_map: Mapping from tool names to their IDs
            model: Model used for generation, will be assigned to each agent's llm field
            disable_memory: When True (no persistent memory backend), set memory=False
                on each agent so the crew doesn't use ephemeral local memory storage

        Returns:
            Processed crew setup

        Raises:
            ValueError: If setup is invalid
        """
        # Extract just the tool names for filtering
        allowed_tool_names = [t.get('name') for t in allowed_tools if t.get('name')]

        # Log the raw setup from LLM
        agent_names = [a.get('name', 'Unknown') for a in setup.get('agents', [])]
        task_names = [t.get('name', 'Unknown') for t in setup.get('tasks', [])]
        logger.info(f"PROCESSING: LLM crew setup with {len(setup.get('agents', []))} agents and {len(setup.get('tasks', []))} tasks")
        logger.info(f"Agent names: {agent_names}")
        logger.info(f"Task names: {task_names}")

        # Log agent assignments from LLM
        for task in setup.get('tasks', []):
            task_name = task.get('name', 'Unknown')
            agent_name = task.get('agent')
            if not agent_name:
                agent_name = task.get('assigned_agent')

            if agent_name:
                logger.info(f"RAW LLM OUTPUT: Task '{task_name}' assigned to agent '{agent_name}'")
                # IMPORTANT: Make sure assignments are preserved by explicitly setting both fields
                task['agent'] = agent_name  # Ensure 'agent' field exists
                if 'assigned_agent' not in task:
                    task['assigned_agent'] = agent_name  # Also set assigned_agent as fallback
            else:
                logger.warning(f"RAW LLM OUTPUT: Task '{task_name}' has no agent assignment in LLM output")

        # Validate required fields
        if "agents" not in setup or not isinstance(setup["agents"], list) or len(setup["agents"]) == 0:
            logger.error("Missing or empty 'agents' array in LLM response")
            raise ValueError("Missing or empty 'agents' array in response")

        if "tasks" not in setup or not isinstance(setup["tasks"], list) or len(setup["tasks"]) == 0:
            logger.error("Missing or empty 'tasks' array in LLM response")
            raise ValueError("Missing or empty 'tasks' array in response")

        # Remove orphan agents that have no tasks assigned to them
        assigned_agent_names = set()
        for task in setup["tasks"]:
            agent_name = task.get("agent") or task.get("assigned_agent")
            if agent_name:
                assigned_agent_names.add(agent_name)
        orphan_agents = [
            a.get("name", "Unknown") for a in setup["agents"]
            if a.get("name") not in assigned_agent_names
        ]
        if orphan_agents:
            logger.warning(
                f"PROCESSING: Removing {len(orphan_agents)} orphan agent(s) "
                f"with no tasks: {orphan_agents}"
            )
            setup["agents"] = [
                a for a in setup["agents"]
                if a.get("name") in assigned_agent_names
            ]

        # Validate agent fields
        for i, agent in enumerate(setup["agents"]):
            agent_name = agent.get('name', f'Agent_{i}')
            logger.info(f"VALIDATING: Agent '{agent_name}'")

            required_agent_fields = ["name", "role", "goal", "backstory"]
            for field in required_agent_fields:
                if field not in agent:
                    logger.error(f"Agent '{agent_name}' is missing required field: {field}")
                    raise ValueError(f"Missing required field '{field}' in agent {i}")

        # Assign the generation model to each agent so they use the dispatcher's model
        if model:
            for agent in setup['agents']:
                agent['llm'] = model
                logger.info(f"MODEL: Assigned model '{model}' to agent '{agent.get('name', 'Unknown')}'")

        # No persistent memory backend → disable memory on each agent so the crew
        # doesn't write to ephemeral local storage that won't survive in a deployed app.
        if disable_memory:
            for agent in setup['agents']:
                agent['memory'] = False
            logger.info("MEMORY: No persistent backend — set memory=False on all generated agents")

        # Filter agent tools to only include allowed tools and convert tool names to IDs
        for agent in setup['agents']:
            agent_name = agent.get('name', 'Unknown')

            if 'tools' in agent and isinstance(agent['tools'], list):
                original_tools = agent['tools'].copy()

                # First filter tools to include only allowed ones
                filtered_tools = [tool for tool in agent['tools'] if tool in allowed_tool_names]

                if len(filtered_tools) != len(original_tools):
                    removed_tools = [tool for tool in original_tools if tool not in allowed_tool_names]
                    logger.info(f"TOOLS: Removed tools from agent '{agent_name}': {removed_tools}")
                    logger.info(f"TOOLS: Remaining tools for agent '{agent_name}': {filtered_tools}")

                # Convert tool names to IDs
                tool_ids = []
                for tool_name in filtered_tools:
                    if tool_name in tool_name_to_id_map:
                        tool_ids.append(tool_name_to_id_map[tool_name])
                    else:
                        logger.warning(f"Could not find ID for tool: {tool_name}")
                        # Keep the name as is if ID not found
                        tool_ids.append(tool_name)

                agent['tools'] = tool_ids
                logger.info(f"TOOLS: Converted tool names to IDs for agent '{agent_name}': {agent['tools']}")

            # Remove any existing ID to let the database generate it
            if 'id' in agent:
                logger.info(f"PROCESSING: Removing existing ID from agent '{agent_name}': {agent['id']}")
                del agent['id']

            # Ensure tools is a list
            if not isinstance(agent.get('tools'), list):
                logger.info(f"PROCESSING: Initializing empty tools list for agent '{agent_name}'")
                agent['tools'] = []

        # Filter task tools to only include allowed tools and convert to IDs
        for task in setup['tasks']:
            task_name = task.get('name', 'Unknown')

            # Debug log task fields
            logger.info(f"TASK FIELDS: Task '{task_name}' has fields: {list(task.keys())}")

            # Process Tools (existing logic)
            if 'tools' in task and isinstance(task['tools'], list):
                original_tools = task['tools'].copy()
                filtered_tools = [tool for tool in task['tools'] if tool in allowed_tool_names]

                # Convert tool names to IDs
                tool_ids = []
                for tool_name in filtered_tools:
                    if tool_name in tool_name_to_id_map:
                        tool_ids.append(tool_name_to_id_map[tool_name])
                    else:
                        logger.warning(f"Could not find ID for tool: {tool_name}")
                        # Keep the name as is if ID not found
                        tool_ids.append(tool_name)

                task['tools'] = tool_ids

                if len(filtered_tools) != len(original_tools):
                    removed_tools = [tool for tool in original_tools if tool not in allowed_tool_names]
                    logger.info(f"TOOLS: Removed tools from task '{task_name}': {removed_tools}")
                logger.info(f"TOOLS: Converted tool names to IDs for task '{task_name}': {task['tools']}")

            if not isinstance(task.get('tools'), list):
                 task['tools'] = [] # Ensure tools is a list

            # Remove any existing ID to let the database generate it
            if 'id' in task:
                logger.info(f"PROCESSING: Removing existing ID from task '{task_name}': {task['id']}")
                del task['id']

            # --- Start: Process Context/Dependencies ---
            raw_context = task.get('context')
            if isinstance(raw_context, list) and len(raw_context) > 0:
                # Assume context from LLM contains dependency names/refs
                # Store these raw refs temporarily for the repository to resolve later
                task['_context_refs'] = raw_context
                logger.info(f"PROCESSING: Stored {len(raw_context)} context refs for task '{task_name}': {raw_context}")
            else:
                # Ensure _context_refs doesn't exist if context is empty/invalid
                if '_context_refs' in task:
                    del task['_context_refs']

            # Explicitly set the main context field to an empty list for initial creation
            # The repository will populate this later using _context_refs
            task['context'] = []
            logger.info(f"PROCESSING: Initialized empty context list for task '{task_name}' (refs stored separately)")
            # --- End: Process Context/Dependencies ---

            # Log agent assignment for this task AGAIN to ensure it's preserved
            agent_name = task.get('agent')
            if not agent_name:
                agent_name = task.get('assigned_agent')

            if agent_name:
                logger.info(f"FINAL LLM STRUCTURE: Task '{task_name}' will be assigned to agent '{agent_name}'")
                # Double-check both fields are set
                task['agent'] = agent_name
                task['assigned_agent'] = agent_name
            else:
                logger.warning(f"FINAL LLM STRUCTURE: Task '{task_name}' has no agent assignment")

        logger.info("PROCESSING: Finished processing crew setup")
        return setup



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
        if hasattr(obj, 'get') and callable(obj.get):
            # Dictionary-like access
            return obj.get(attr, default)
        elif hasattr(obj, attr):
            # Object attribute access
            return getattr(obj, attr, default)
        else:
            return default

    # NOTE: the crewai-docs retrieval helper (_get_relevant_documentation)
    # was removed with the crewai->kasal migration: it was never invoked and
    # the docs.crewai.com seeder that fed it is gone.

    async def create_crew_complete(self, request: CrewGenerationRequest, group_context: Optional[GroupContext] = None, fast_planning: bool = True) -> Dict[str, Any]:
        """Public entrypoint — wraps crew generation in an MLflow root trace so
        it lands in the shared UC experiment (alongside dispatcher intent, agent
        generation, task generation and crew execution).

        The dispatcher (chat) path uses ``create_crew_progressive`` and sets up
        MLflow itself; this covers the direct ``generate-crew`` API call.
        """
        from contextlib import nullcontext
        from src.services.otel_tracing.mlflow_parent_setup import (
            configure_parent_mlflow_tracing,
            set_root_span_outputs,
        )

        mlflow_on = await configure_parent_mlflow_tracing(
            self.session, group_context, label="CrewGeneration"
        )
        if mlflow_on:
            from src.services.mlflow_tracing_service import start_root_trace
            trace_ctx = start_root_trace(
                "crew_generation",
                inputs={
                    "prompt": getattr(request, "prompt", None),
                    "model": getattr(request, "model", None) or "default",
                },
            )
        else:
            trace_ctx = nullcontext()

        with trace_ctx as root_span:
            result = await self._create_crew_complete_impl(
                request, group_context=group_context, fast_planning=fast_planning
            )
            set_root_span_outputs(root_span, result)
            return result

    async def _create_crew_complete_impl(self, request: CrewGenerationRequest, group_context: Optional[GroupContext] = None, fast_planning: bool = True) -> Dict[str, Any]:
        """
        Create a crew with agents and tasks.

        Args:
            request: The crew generation request with prompt, model, and tool information
            group_context: Group context for multi-tenant isolation

        Returns:
            Dictionary containing the created agents and tasks
        """
        try:
            logger.info("CREATE CREW: Starting crew generation process")

            # Get tool details using the tool service with session
            # Create tool service with session
            tool_service = ToolService(self.session)
            # Process tools to ensure we have complete tool information
            tools_with_details = await self._get_tool_details(request.tools or [], tool_service)

            # Filter out Databricks knowledge tool if no Databricks memory is configured for this group
            try:
                from src.repositories.memory_backend_repository import MemoryBackendRepository
                from src.models.memory_backend import MemoryBackendTypeEnum
                primary_group_id = group_context.primary_group_id if group_context else None
                if primary_group_id:
                    mem_repo = MemoryBackendRepository(self.session)
                    databricks_backends = await mem_repo.get_by_type(primary_group_id, MemoryBackendTypeEnum.DATABRICKS)
                    if not databricks_backends:
                        before_count = len(tools_with_details)
                        tools_with_details = [
                            t for t in tools_with_details
                            if (t.get('name') or t.get('title')) not in ('DatabricksKnowledgeSearchTool',)
                            and t.get('title') not in ('DatabricksKnowledgeSearchTool',)
                        ]
                        after_count = len(tools_with_details)
                        if before_count != after_count:
                            logger.info(
                                f"CREATE CREW: Filtered DatabricksKnowledgeSearchTool out (no Databricks memory for group {primary_group_id})"
                            )
            except Exception as e:
                logger.warning(f"CREATE CREW: Tool filtering skipped due to error: {e}")


            # Create a mapping from tool names to tool IDs for later use
            tool_name_to_id_map = self._create_tool_name_to_id_map(tools_with_details)
            logger.info(f"Tool name to ID mapping: {tool_name_to_id_map}")

            # Generate the crew using the LLM
            model = request.model or os.getenv("CREW_MODEL", "databricks-gpt-5-3-codex")

            # Get and prepare the prompt template with tool descriptions (incl. group/user overrides)
            system_message = await self._prepare_prompt_template(
                tools_with_details, group_context, prompt=getattr(request, "prompt", None)
            )
            logger.info("CREATE CREW: Prepared prompt template with detailed tool information")

            # Few-shot examples from crews this workspace already built and a
            # human marked good. The decision is kept (not just the text) so the
            # trial can be recorded below with what came out — including the
            # control case, where blessed matches existed and were withheld.
            # Empty and inert until someone curates; never fails generation.
            recipe_decision = await self._prepare_exemplars(request, group_context)
            if recipe_decision is not None and recipe_decision.text:
                system_message += recipe_decision.text
                logger.info("CREATE CREW: injected curated workflow-recipe exemplars")

            # Documentation context disabled: skip vector search/embedding for crew generation
            # Prepare messages for the LLM
            messages = [
                {"role": "system", "content": system_message}
            ]

            # (No documentation context injected)

            # Add the user's prompt
            messages.append({"role": "user", "content": request.prompt})

            logger.info(f"CREATE CREW: Configured LLM with model: {model}")

            # Generate completion via unified LLMManager.completion()
            try:
                logger.info("CREATE CREW: Calling LLM API...")
                _max_tokens = 4000
                logger.info(f"CREATE CREW: Using max_tokens={_max_tokens} for model={model}")

                from src.utils.telemetry import get_user_agent_header, KasalProduct
                content = await LLMManager.completion(
                    messages=messages,
                    model=model,
                    temperature=0.7,
                    max_tokens=_max_tokens,
                    extra_headers=get_user_agent_header(KasalProduct.CREW_GENERATION)
                )

                logger.info(f"CREATE CREW: Extracted content from LLM response (length: {len(content)})")

                # Log the LLM interaction
                await self._log_llm_interaction(
                    endpoint='generate-crew',
                    prompt=f"System: {system_message}\nUser: {request.prompt}",
                    response=content,
                    model=model,
                    group_context=group_context
                )

                # Parse JSON setup
                logger.info("CREATE CREW: Parsing JSON response from LLM")
                crew_setup = robust_json_parser(content)
                logger.info(f"CREATE CREW: Successfully parsed JSON")

                # No persistent memory backend → disable memory on generated agents
                # (avoids writing to ephemeral local storage that doesn't persist).
                has_memory = await self._has_persistent_memory_backend(self.session, group_context)
                # Process and validate LLM response with the tool name to ID mapping
                processed_setup = self._process_crew_setup(
                    crew_setup, tools_with_details, tool_name_to_id_map,
                    model=model, disable_memory=not has_memory,
                )

            except Exception as e:
                error_msg = f"Error generating crew: {str(e)}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Log agent assignments before converting to dictionaries
            logger.info("CREATE CREW: Current agent assignments:")
            for task in processed_setup.get('tasks', []):
                task_name = task.get('name', 'Unknown')
                agent_name = task.get('agent')
                if not agent_name:
                    agent_name = task.get('assigned_agent')

                if agent_name:
                    logger.info(f"ASSIGNMENTS: Task '{task_name}' assigned to agent '{agent_name}'")
                else:
                    logger.warning(f"ASSIGNMENTS: Task '{task_name}' HAS NO AGENT ASSIGNMENT")

            # Convert Pydantic models to dictionaries while preserving agent assignments
            agents_dict = []
            for agent in processed_setup.get('agents', []):
                # If it's a Pydantic model, convert to dict
                if hasattr(agent, 'model_dump'):
                    agent_dict = agent.model_dump()
                else:
                    agent_dict = agent.copy() if isinstance(agent, dict) else agent

                agents_dict.append(agent_dict)

            tasks_dict = []
            for task in processed_setup.get('tasks', []):
                # If it's a Pydantic model, convert to dict
                if hasattr(task, 'model_dump'):
                    task_dict = task.model_dump()
                else:
                    task_dict = task.copy() if isinstance(task, dict) else task

                # IMPORTANT: Ensure agent assignments are preserved
                task_name = task_dict.get('name', 'Unknown')
                agent_name = task.get('agent')
                if not agent_name:
                    agent_name = task.get('assigned_agent')

                if agent_name:
                    # Make sure both fields are set in the dictionary
                    task_dict['agent'] = agent_name
                    task_dict['assigned_agent'] = agent_name
                    logger.info(f"PRESERVE: Task '{task_name}' assignment to agent '{agent_name}' preserved in dictionary conversion")
                else:
                    logger.warning(f"PRESERVE: Task '{task_name}' HAS NO AGENT ASSIGNMENT to preserve")

                tasks_dict.append(task_dict)

            # Create a new dictionary to send to repository
            crew_dict = {
                'agents': agents_dict,
                'tasks': tasks_dict
            }

            # Log the data being sent to repository
            logger.info(f"CREATE CREW: Sending {len(agents_dict)} agents and {len(tasks_dict)} tasks to repository")
            for idx, agent in enumerate(agents_dict):
                logger.info(f"AGENT {idx+1}: '{agent.get('name')}' - Role: '{agent.get('role')}', Tools: {agent.get('tools', [])}")

            for idx, task in enumerate(tasks_dict):
                logger.info(f"TASK {idx+1}: '{task.get('name')}' - Agent: '{task.get('agent')}', Dependencies: {task.get('context', [])}")

            # Create entities in repository with group context
            result = await self.crew_generator_repository.create_crew_entities(crew_dict, group_context)

            logger.info("CREATE CREW: Successfully created crew entities")

            # Measurement ledger. Recorded AFTER creation so the row carries the
            # agent ids that later link this generation to the run it becomes —
            # the only exact join between "we suggested a shape" and "here is how
            # that crew actually did".
            await self._record_recipe_trial(recipe_decision, result, group_context)
            return result
        except Exception as e:
            logger.error(f"CREATE CREW: Error creating crew: {str(e)}")
            logger.error(f"CREATE CREW: Exception traceback: {traceback.format_exc()}")
            raise

    async def synthesize_crew_from_conversation(
        self,
        session_id: str,
        group_context: Optional[GroupContext] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Distill a reusable crew from a WHOLE chat conversation.

        ChatMode answer/"chat" turns run a GENERIC single assistant (see
        ``_run_chat_fast_path``), so bookmarking that to the catalog saves nothing
        specific. This reads the ENTIRE conversation for ``session_id`` and asks
        the LLM to design a crew that reproduces the full workflow the user went
        through — one task per distinct step (e.g. gather info → build dashboard),
        chained in order — then persists it via the normal crew-creation path.
        The created entities come back with DB ids so the chat can show exactly
        what was saved.

        It is incremental by construction: it re-distills from the full, current
        conversation each time, so as the session grows (the user adds more
        steps) a re-save captures the additional steps too.

        Args:
            session_id: Chat session whose conversation is distilled.
            group_context: Multi-tenant context (group scoping + LLM auth).
            model: Optional LLM override for the synthesis.

        Returns:
            ``{"agents": [...], "tasks": [...]}`` — created DB entities.
        """
        transcript = await self._build_conversation_transcript(session_id, group_context)
        if not transcript:
            raise BadRequestError(
                "No conversation found for this session to build a crew from"
            )

        prompt = (
            "Below is the FULL conversation between a USER and an AI ASSISTANT across "
            "a chat session. It may contain SEVERAL distinct requests in sequence "
            "(for example: first gathering information, then building a dashboard "
            "from it).\n\n"
            "Design a reusable crew that reproduces this ENTIRE workflow on its own, "
            "WITHOUT the back-and-forth. Cover EVERY distinct step the user went "
            "through, in order: create a separate task for each step (and an agent "
            "suited to it), and chain them so each later task builds on the output of "
            "the earlier ones (use task context/dependencies). Do NOT collapse a "
            "multi-step conversation into a single generic task — if the user did N "
            "things, the crew should have tasks covering all N.\n\n"
            "Base each agent's role/goal/backstory and each task's description/"
            "expected_output on what the USER actually asked for and the answers that "
            "satisfied them — be specific to the domain and deliverables in this "
            "conversation, NOT a generic 'helpful assistant'. Each task description "
            "must state its objective clearly enough to run standalone.\n\n"
            f"Conversation:\n{transcript}"
        )
        request = CrewGenerationRequest(prompt=prompt, model=model)
        logger.info(
            f"SYNTHESIZE CREW: distilling reusable crew from session {session_id} "
            f"({len(transcript)} transcript chars)"
        )
        return await self.create_crew_complete(request, group_context)

    async def _build_conversation_transcript(
        self,
        session_id: str,
        group_context: Optional[GroupContext],
        max_chars: int = 12000,
    ) -> str:
        """The session's USER/ASSISTANT turns as a transcript, weighted so EVERY
        user step survives.

        Group-scoped (tenant isolation) and best-effort: returns ``""`` when there
        is no session, no group, or no usable content. Placeholder rows
        ("Thinking...", "[ui-card]") are skipped and each turn is capped.

        Why the weighting (vs. a flat "most recent ``max_chars``" window): a chat
        that goes "gather info → build a dashboard" is dominated, by character
        count, by the large ASSISTANT outputs (the dashboard/report). A flat tail
        clamp would drop the early, short USER request ("gather info …") — exactly
        the step the distilled crew must still cover. So ALL user turns are kept
        (each capped), assistant turns are capped harder, and when over budget the
        OLDEST assistant turns are dropped first; user turns are never dropped.
        """
        group_ids = list(getattr(group_context, "group_ids", None) or [])
        primary = getattr(group_context, "primary_group_id", None)
        if not group_ids and primary:
            group_ids = [primary]
        if not session_id or not group_ids:
            return ""

        from src.repositories.chat_history_repository import ChatHistoryRepository
        messages = await ChatHistoryRepository(self.session).get_recent_by_session_and_group(
            session_id, group_ids, limit=200
        )

        placeholders = {"thinking...", "[ui-card]", ""}
        user_cap = 800
        assistant_cap = 500
        # (role, "User: ..."/"Assistant: ..." line) in chronological order.
        entries: List[Tuple[str, str]] = []
        for m in messages:
            mtype = getattr(m, "message_type", "")
            if mtype not in ("user", "assistant"):
                continue
            content = (getattr(m, "content", "") or "").strip()
            if content.lower() in placeholders or content.startswith("[ui-card]"):
                continue
            cap = user_cap if mtype == "user" else assistant_cap
            if len(content) > cap:
                content = content[:cap] + "…"
            label = "User" if mtype == "user" else "Assistant"
            entries.append((mtype, f"{label}: {content}"))

        if not entries:
            return ""

        # Enforce the budget by dropping the OLDEST assistant turns first; never
        # drop a user turn (each one is a step the crew must still cover).
        def _total(items: List[Tuple[str, str]]) -> int:
            return sum(len(line) + 1 for _, line in items)

        selected = list(entries)
        while selected and _total(selected) > max_chars:
            drop_at = next((i for i, (role, _) in enumerate(selected) if role == "assistant"), None)
            if drop_at is None:
                break  # only user turns remain — keep them even if slightly over
            selected.pop(drop_at)

        return "\n".join(line for _, line in selected)

    def _create_tool_name_to_id_map(self, tools: List[Dict[str, Any]]) -> Dict[str, str]:
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
            name = tool.get('title') or tool.get('name')
            tool_id = tool.get('id')

            if name and tool_id:
                # Ensure ID is a string
                name_to_id[name] = str(tool_id)

                # Also add the original name as a key if different from title
                if 'name' in tool and tool['name'] != name:
                    name_to_id[tool['name']] = str(tool_id)

        return name_to_id

    async def _get_tool_details(self, tool_identifiers: List[Any], tool_service: ToolService) -> List[Dict[str, Any]]:
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
            tools_by_name = {tool.title: tool for tool in all_tools if hasattr(tool, 'title')}
            tools_by_id = {str(tool.id): tool for tool in all_tools if hasattr(tool, 'id')}

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
                        detailed_tools.append({"name": identifier, "description": f"A tool named {identifier}", "id": identifier})
                        continue

                elif isinstance(identifier, dict):
                    # Extract name or ID from dictionary
                    name = identifier.get('name')
                    tool_id = identifier.get('id')

                    if name and name in tools_by_name:
                        tool_detail = tools_by_name[name]
                    elif tool_id and str(tool_id) in tools_by_id:
                        tool_detail = tools_by_id[str(tool_id)]
                    elif name:
                        # If we have a name but no match, add it as is
                        logger.warning(f"Tool not found by name: {name}")
                        detailed_tools.append({
                            "name": name,
                            "description": identifier.get('description', f"A tool named {name}"),
                            "id": tool_id or name  # Use ID if available, otherwise use name
                        })
                        continue
                    else:
                        logger.warning(f"Invalid tool identifier, missing name or id: {identifier}")
                        continue
                else:
                    logger.warning(f"Unknown tool identifier format: {identifier}")
                    continue

                # Convert tool to dictionary with all details
                if tool_detail:
                    if hasattr(tool_detail, 'model_dump'):
                        tool_dict = tool_detail.model_dump()
                    else:
                        # If it's already a dictionary or has __dict__
                        tool_dict = tool_detail.__dict__ if hasattr(tool_detail, '__dict__') else dict(tool_detail)

                    # Ensure we have name and description
                    if 'name' not in tool_dict and hasattr(tool_detail, 'title'):
                        tool_dict['name'] = tool_detail.title
                    if 'description' not in tool_dict and hasattr(tool_detail, 'description'):
                        tool_dict['description'] = tool_detail.description

                    detailed_tools.append(tool_dict)

            logger.info(f"Processed {len(detailed_tools)} tools with detailed information")
            return detailed_tools

        except Exception as e:
            logger.error(f"Error retrieving tool details: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

            # Fall back to basic processing if tool service fails
            return [{"name": t if isinstance(t, str) else t.get('name', 'Unknown'),
                    "description": f"A tool named {t if isinstance(t, str) else t.get('name', 'Unknown')}",
                    "id": t if isinstance(t, str) else t.get('id', t.get('name', 'Unknown'))}
                   for t in tool_identifiers]

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
            from src.repositories.memory_backend_repository import MemoryBackendRepository
            from src.models.memory_backend import MemoryBackendTypeEnum
            mem_repo = MemoryBackendRepository(session)
            for backend_type in (MemoryBackendTypeEnum.DATABRICKS, MemoryBackendTypeEnum.LAKEBASE):
                if await mem_repo.get_by_type(primary_group_id, backend_type):
                    return True
            return False
        except Exception as e:
            logger.warning(f"Memory backend check failed, assuming no persistent memory: {e}")
            return False

    # ------------------------------------------------------------------ #
    #  Progressive / Streaming crew generation
    # ------------------------------------------------------------------ #

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
        knowledge_file_paths = list(getattr(request, "knowledge_file_paths", None) or [])
        user_request = request.original_prompt or request.prompt

        # Agent Bricks endpoints picked in the chat "+" menu. This backend builder is
        # the ONLY config path for ChatMode auto-execute (the Crew canvas / AgentBuilder
        # build their run config from saved-node tool_configs instead, which already
        # carry the endpoint — that's why those channels work and chat didn't).
        # Mirror the MCP injection below: when an endpoint is picked, equip + configure
        # the AgentBricksTool (catalog seed id 71) on each agent/task; when NONE is
        # picked, STRIP any AgentBricksTool the generator/LLM equipped on its own, since
        # an unconfigured AgentBricksTool aborts the task ("endpoint name is not configured").
        agentbricks_endpoints = list(getattr(request, "agentbricks_endpoints", None) or [])
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
            "llm", "function_calling_llm", "max_iter", "max_rpm",
            "max_execution_time", "memory", "verbose", "allow_delegation",
            "cache", "system_template", "prompt_template", "response_template",
            "allow_code_execution", "code_execution_mode", "max_retry_limit",
            "use_system_prompt", "respect_context_window",
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
                cfg.setdefault("tool_configs", {})["MCP_SERVERS"] = {"servers": mcp_servers}
            if knowledge_file_paths:
                cfg.setdefault("tool_configs", {})["DatabricksKnowledgeSearchTool"] = {"file_paths": knowledge_file_paths}
            if has_agentbricks:
                cfg.setdefault("tool_configs", {})["AgentBricksTool"] = {"endpointName": agentbricks_endpoints}
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
            for dep in (task.get("context") or []):
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
                grounding.append(f"USER REQUEST — this run exists to answer it:\n{user_request}")
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
            description = f"{base_desc}\n\n" + "\n\n".join(grounding) if grounding else base_desc
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
                entry.setdefault("tool_configs", {})["MCP_SERVERS"] = {"servers": mcp_servers}
            if knowledge_file_paths:
                entry.setdefault("tool_configs", {})["DatabricksKnowledgeSearchTool"] = {"file_paths": knowledge_file_paths}
            if has_agentbricks:
                entry.setdefault("tool_configs", {})["AgentBricksTool"] = {"endpointName": agentbricks_endpoints}
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
        _mode = (getattr(request, "chat_mode_type", "chat") or "chat")
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

    async def create_crew_progressive(
        self,
        request: CrewStreamingRequest,
        group_context: Optional[GroupContext],
        generation_id: str,
        mlflow_enabled: bool = False,
    ) -> None:
        """
        Progressively generate a crew, broadcasting SSE events as each entity
        is created.

        Phase 1 — Plan: Fast LLM call returning agent names/roles + task names.
        Phase 2 — Agent details: Reuse AgentGenerationService per agent.
        Phase 3 — Task details: Reuse TaskGenerationService per task.

        IMPORTANT: This method runs as a background task after the HTTP response
        has already been sent. The request-scoped DB session is closed by then,
        so all database work uses an independent session created here.
        """
        from contextlib import nullcontext, asynccontextmanager
        from src.db.session import async_session_factory, detach_request_session, get_isolated_db_session
        from src.db.database_router import is_lakebase_enabled, get_lakebase_config_from_db
        from src.db.lakebase_session import get_lakebase_session

        # This runs via asyncio.create_task, so it inherited a COPY of the
        # dispatch request's context — including the request-scoped DB session,
        # which FastAPI has already closed. Detach it so every
        # request_scoped_session() below (notably the model-config read inside
        # LLMManager.configure_kasal_llm during planning) opens a fresh session
        # instead of failing with "Cannot operate on a closed database".
        detach_request_session()

        if mlflow_enabled:
            try:
                from src.services.mlflow_tracing_service import start_root_trace
                trace_ctx = start_root_trace(
                    "crew_generation",
                    inputs={"prompt": request.prompt, "model": request.model or "default"},
                )
            except Exception:
                trace_ctx = nullcontext()
        else:
            trace_ctx = nullcontext()

        with trace_ctx as root_span:
            try:
                # ── ChatMode 'chat' fast path ─────────────────────────────
                # 'chat' answer mode runs a SINGLE Agent.kickoff_async. The
                # bespoke plan + agent + task LLM generations below add ~3 LLM
                # round-trips with no benefit for what is just a default
                # assistant answering the user's message. Skip them entirely:
                # synthesize a default agent + a task from the raw prompt and go
                # straight to auto-execute. 'research'/'deep' still generate a
                # full crew (they need the plan/agents/tasks).
                if (getattr(request, "chat_mode_type", "chat") or "chat") == "chat" \
                        and getattr(request, "auto_execute", False):
                    await self._run_chat_fast_path(
                        request, group_context, generation_id, root_span
                    )
                    return

                model = request.model or os.getenv("CREW_MODEL", "databricks-gpt-5-3-codex")

                # ── Compute caps BEFORE planning so the LLM knows the limits ──
                # Caps are UPPER BOUNDS, not predictions: the PLAN LLM decides the
                # actual counts by mapping the user's distinct actions to tasks
                # (the generate_crew_plan template + few-shots own that logic, and
                # "use the minimum agents needed" keeps simple prompts small).
                # NEVER derive ceilings from keyword heuristics here — a hardcoded
                # verb lexicon capped "list data products, understand the
                # contracts, …" to ONE task because none of its verbs were in the
                # list (the "always 1 agent / 1 task" regression vs v1.3.0). Only
                # an EXPLICIT numeric request tightens/raises the caps.
                ABSOLUTE_MAX_AGENTS = 10
                ABSOLUTE_MAX_TASKS = 10
                # Defaults mirror the generate_crew template LIMITS ("at most 3
                # agents and 6 tasks unless the user explicitly asks for more").
                DEFAULT_MAX_TASKS = 6
                DEFAULT_MAX_AGENTS = 3

                # Check BOTH the (possibly LLM-rewritten) prompt AND the original
                # user message for explicit count requests.
                user_prompt = (request.prompt or "").lower()
                original_prompt = (
                    getattr(request, "original_prompt", None) or ""
                ).lower()
                combined = user_prompt + " " + original_prompt
                # Bounded gap (≤3 words, e.g. "4 specialized research agents") and a
                # lookahead so a count can't be claimed ACROSS the other noun —
                # "4 agents and 8 tasks" must read tasks=8, not greedily tasks=4.
                _count_re = r'(\d+)\s+(?:(?!agents?\b|tasks?\b)\w+\s+){0,3}%s\b'
                agent_count_match = re.search(_count_re % 'agents?', combined)
                task_count_match = re.search(_count_re % 'tasks?', combined)

                if task_count_match:
                    max_tasks = min(int(task_count_match.group(1)), ABSOLUTE_MAX_TASKS)
                    logger.info(f"PROGRESSIVE [{generation_id}]: User requested {max_tasks} tasks")
                else:
                    max_tasks = DEFAULT_MAX_TASKS
                if agent_count_match:
                    max_agents = min(int(agent_count_match.group(1)), ABSOLUTE_MAX_AGENTS)
                    logger.info(f"PROGRESSIVE [{generation_id}]: User requested {max_agents} agents")
                else:
                    max_agents = min(DEFAULT_MAX_AGENTS, max_tasks)

                # Chat (light agent) ANSWER mode runs a SINGLE Agent.kickoff_async —
                # force exactly one agent + one task so there is one agent to kick
                # off and one grounded task description to use as its prompt. This
                # applies ONLY when this generation IS the chat answer run
                # (auto_execute) — that path normally short-circuits into
                # _run_chat_fast_path above, so this is a defensive guard. A
                # GENERATE-ONLY request (the AgentBuilder canvas chat, which leaves
                # auto_execute False and renders the plan as nodes) must plan the
                # full crew like research/deep: chat_mode_type defaults to "chat"
                # in the schema, and clamping on it alone collapsed every canvas
                # generation to 1 agent / 1 task (regression vs v1.3.0).
                if (
                    (getattr(request, "chat_mode_type", "chat") or "chat") == "chat"
                    and getattr(request, "auto_execute", False)
                ):
                    max_agents = 1
                    max_tasks = 1
                    logger.info(f"PROGRESSIVE [{generation_id}]: chat (light agent) answer mode — capping to 1 agent / 1 task")

                # ── Phase 1: Planning (LLM only, no DB writes) ───────────
                # Inject the computed cap into the request so the LLM generates
                # the correct number from the start (instead of generating many
                # and truncating, which loses the user's actual goal).
                logger.info(f"PROGRESSIVE [{generation_id}]: Phase 1 — Planning (max {max_agents} agents, {max_tasks} tasks)")

                # Crews this workspace already built and a human marked good.
                # The PLAN call is where they belong: it decides the shape —
                # how many agents, how the work splits — which is exactly what a
                # past crew is evidence about. Injecting them into the later
                # per-agent/per-task calls would arrive after those decisions
                # were already made. Empty and inert until someone curates.
                recipe_decision = await self._recipe_decision_isolated(
                    request, group_context
                )
                if recipe_decision is not None and recipe_decision.injected_labels:
                    logger.info(
                        f"PROGRESSIVE [{generation_id}]: reusing "
                        f"{len(recipe_decision.injected_labels)} curated recipe(s)"
                    )

                try:
                    plan = await self._generate_crew_plan(
                        request, group_context, model,
                        max_agents=max_agents, max_tasks=max_tasks,
                        exemplars=(recipe_decision.text if recipe_decision else ""),
                    )
                except Exception as e:
                    logger.error(f"PROGRESSIVE [{generation_id}]: Planning failed: {e}")
                    await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                        data={"type": "generation_failed", "error": str(e)},
                        event="generation_failed",
                    ))
                    return

                plan_agents = plan.get("agents", [])
                plan_tasks = plan.get("tasks", [])
                process_type = plan.get("process_type", "sequential")
                complexity = plan.get("complexity", "standard")

                # Safety net: if LLM still exceeded caps, truncate as last resort.
                # For single-agent (max=1), keep the LAST agent/task since in a
                # sequential pipeline the final step produces the user's deliverable
                # (e.g., dashboard builder > scraper). For multi-agent, keep the first N.
                if len(plan_agents) > max_agents:
                    logger.warning(
                        f"PROGRESSIVE [{generation_id}]: Truncating agents from "
                        f"{len(plan_agents)} to {max_agents}"
                    )
                    if max_agents == 1 and process_type == "sequential":
                        plan_agents = plan_agents[-1:]
                    else:
                        plan_agents = plan_agents[:max_agents]
                if len(plan_tasks) > max_tasks:
                    logger.warning(
                        f"PROGRESSIVE [{generation_id}]: Truncating tasks from "
                        f"{len(plan_tasks)} to {max_tasks}"
                    )
                    if max_tasks == 1 and process_type == "sequential":
                        plan_tasks = plan_tasks[-1:]
                    else:
                        plan_tasks = plan_tasks[:max_tasks]
                if not plan_agents:
                    await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                        data={"type": "generation_failed", "error": "Plan returned no agents"},
                        event="generation_failed",
                    ))
                    return

                # Re-assign orphaned tasks to valid agents and clean stale context refs
                valid_agent_names = {a.get("name") for a in plan_agents}
                valid_task_names = {t.get("name") for t in plan_tasks}
                for task in plan_tasks:
                    if task.get("assigned_agent") not in valid_agent_names:
                        task["assigned_agent"] = plan_agents[0].get("name", "")
                    # Remove context references to tasks that were truncated
                    if task.get("context"):
                        task["context"] = [
                            c for c in task["context"] if c in valid_task_names
                        ]

                # Remove orphan agents that have no tasks assigned
                assigned_agents = {t.get("assigned_agent") for t in plan_tasks}
                orphan_agents = [
                    a for a in plan_agents
                    if a.get("name") not in assigned_agents
                ]
                if orphan_agents:
                    orphan_names = [a.get("name") for a in orphan_agents]
                    logger.warning(
                        f"PROGRESSIVE [{generation_id}]: Removing "
                        f"{len(orphan_agents)} orphan agent(s) with no tasks: "
                        f"{orphan_names}"
                    )
                    plan_agents = [
                        a for a in plan_agents
                        if a.get("name") in assigned_agents
                    ]

                # ── Enforce sequential dependency chain ────────────────
                if process_type == "sequential":
                    for i, task in enumerate(plan_tasks):
                        if i > 0 and not task.get("context"):
                            prev_name = plan_tasks[i - 1].get("name", "")
                            if prev_name:
                                task["context"] = [prev_name]
                                logger.info(
                                    f"PROGRESSIVE [{generation_id}]: Auto-chained "
                                    f"task '{task.get('name')}' → depends on '{prev_name}'"
                                )

                logger.info(
                    f"PROGRESSIVE [{generation_id}]: Plan — complexity={complexity}, "
                    f"process={process_type}, {len(plan_agents)} agents, {len(plan_tasks)} tasks"
                )

                # Broadcast plan_ready
                await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                    data={
                        "type": "plan_ready",
                        "agents": plan_agents,
                        "tasks": plan_tasks,
                        "process_type": process_type,
                        "complexity": complexity,
                    },
                    event="plan_ready",
                ))

                # ── Phases 2-4: DB writes use an independent session ──────
                # The request-scoped session is already closed by FastAPI DI,
                # so we create a standalone session for all database operations.
                # IMPORTANT: Route to Lakebase when enabled, matching
                # get_smart_db_session() so reads/writes hit the same DB.
                #
                # On SQLite, use a PRIVATE connection (get_isolated_db_session)
                # rather than the shared StaticPool one. This flow commits an agent,
                # then makes a seconds-long LLM call, then inserts a task referencing
                # it — and on the shared connection a concurrent request's
                # commit/rollback in that window can silently discard the committed
                # agent, making the task's agent_id FK fail. A private connection is
                # immune to that interference. (Lakebase/Postgres pooled checkouts
                # are already per-connection, so the helper falls through to them.)
                if await is_lakebase_enabled():
                    lb_config = await get_lakebase_config_from_db()
                    lb_instance = (
                        (lb_config or {}).get("instance_name")
                        or os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")
                    )
                    _session_ctx = get_lakebase_session(lb_instance)
                else:
                    _session_ctx = get_isolated_db_session()

                async with _session_ctx as session:
                    try:
                        repo = CrewGeneratorRepository(session)
                        agent_gen_service = AgentGenerationService(session)
                        task_gen_service = TaskGenerationService(session)

                        # ── Resolve workspace tools ───────────────────────
                        tool_name_to_id_map: Dict[str, str] = {}
                        available_tools_for_llm: List[Dict[str, str]] = []
                        if request.tools:
                            try:
                                tool_service = ToolService(session)
                                tools_with_details = await self._get_tool_details(
                                    request.tools, tool_service
                                )
                                tool_name_to_id_map = self._create_tool_name_to_id_map(
                                    tools_with_details
                                )
                                available_tools_for_llm = [
                                    {
                                        "name": t.get('title') or t.get('name', ''),
                                        "description": t.get('description', ''),
                                    }
                                    for t in tools_with_details
                                    if t.get('title') or t.get('name')
                                ]
                                logger.info(
                                    f"PROGRESSIVE [{generation_id}]: Resolved "
                                    f"{len(available_tools_for_llm)} workspace tools"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"PROGRESSIVE [{generation_id}]: "
                                    f"Tool resolution failed, continuing without tools: {e}"
                                )

                        # ── Build reverse map: tool_id → tool_title ──────
                        tool_id_to_title: Dict[str, str] = {
                            v: k for k, v in tool_name_to_id_map.items()
                        }

                        # ── Group tasks by assigned agent for interleaved generation ──
                        tasks_by_agent: Dict[str, List[Dict]] = defaultdict(list)
                        unassigned_tasks: List[Dict] = []
                        for task_plan in plan_tasks:
                            assigned = task_plan.get("assigned_agent", "")
                            if assigned:
                                tasks_by_agent[assigned.lower()].append(task_plan)
                            else:
                                unassigned_tasks.append(task_plan)

                        # ── Interleaved Phase: Agent → its Tasks → next Agent → its Tasks ──
                        logger.info(f"PROGRESSIVE [{generation_id}]: Interleaved agent→task generation")
                        agent_results: List[Dict[str, Any]] = []
                        task_results: List[Dict[str, Any]] = []
                        global_task_index = 0

                        # If no persistent memory backend is configured, disable memory on
                        # generated agents (otherwise memory silently writes to ephemeral
                        # local storage that doesn't survive in a deployed app).
                        has_memory = await self._has_persistent_memory_backend(session, group_context)
                        logger.info(f"PROGRESSIVE [{generation_id}]: persistent memory backend present = {has_memory}")

                        for i, agent_plan in enumerate(plan_agents):
                            agent_name = agent_plan.get("name", f"Agent {i+1}")
                            agent_role = agent_plan.get("role", "Specialist")
                            try:
                                prompt = (
                                    f"Create an agent named '{agent_name}' with role "
                                    f"'{agent_role}' for a crew that: {request.prompt}"
                                )
                                agent_config = await agent_gen_service.generate_agent(
                                    prompt_text=prompt,
                                    model=model,
                                    tools=[],
                                    group_context=group_context,
                                )

                                # Tools are assigned at the task level, not agent level
                                agent_tool_ids: List[str] = []

                                agent_data = {
                                    "name": agent_config.get("name", agent_name),
                                    "role": agent_config.get("role", agent_role),
                                    "goal": agent_config.get("goal", ""),
                                    "backstory": agent_config.get("backstory", ""),
                                    "llm": model,
                                    "tools": agent_tool_ids,
                                }
                                # No persistent memory backend → disable memory so the
                                # crew doesn't write to ephemeral local storage.
                                if not has_memory:
                                    agent_data["memory"] = False
                                adv = agent_config.get("advanced_config", {})
                                for key in (
                                    "function_calling_llm", "max_iter", "max_rpm",
                                    "verbose", "allow_delegation", "cache",
                                    "code_execution_mode", "max_retry_limit",
                                    "use_system_prompt", "respect_context_window",
                                ):
                                    if key in adv:
                                        agent_data[key] = adv[key]

                                saved = await repo.create_single_agent(
                                    agent_data, group_context
                                )
                                # Commit each agent so it exists for FK constraints
                                await session.commit()
                                agent_results.append(saved)

                                await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                    data={"type": "agent_detail", "index": i, "agent": saved},
                                    event="agent_detail",
                                ))
                                logger.info(f"PROGRESSIVE [{generation_id}]: Agent {i+1}/{len(plan_agents)} done — {saved.get('name')}")

                            except Exception as e:
                                logger.error(f"PROGRESSIVE [{generation_id}]: Agent '{agent_name}' failed: {e}")
                                await session.rollback()
                                await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                    data={
                                        "type": "entity_error", "index": i,
                                        "entity_type": "agent", "name": agent_name, "error": str(e),
                                    },
                                    event="entity_error",
                                ))
                                continue

                            # ── Generate tasks assigned to this agent ──────
                            agent_tasks = tasks_by_agent.get(agent_name.lower(), [])
                            for task_plan in agent_tasks:
                                task_name = task_plan.get("name", f"Task {global_task_index+1}")
                                try:
                                    agent_context = self._find_agent_context(task_plan, agent_results)

                                    task_request = TaskGenerationRequest(
                                        text=(
                                            f"Create a task named '{task_name}' "
                                            f"for a crew that: {request.prompt}. "
                                            f"THIS SPECIFIC TASK is '{task_name}'."
                                        ),
                                        model=model,
                                        agent=agent_context,
                                        available_tools=available_tools_for_llm or None,
                                    )
                                    task_response = await task_gen_service.generate_task(
                                        task_request, group_context
                                    )

                                    agent_id = self._resolve_agent_id(task_plan, agent_results)

                                    # Convert tool names to DB IDs
                                    task_tool_ids = [
                                        tool_name_to_id_map[
                                            t.get("name") if isinstance(t, dict) else str(t)
                                        ]
                                        for t in (task_response.tools or [])
                                        if (t.get("name") if isinstance(t, dict) else str(t)) in tool_name_to_id_map
                                    ]

                                    task_data = {
                                        "name": task_response.name,
                                        "description": task_response.description,
                                        "expected_output": task_response.expected_output,
                                        "tools": task_tool_ids,
                                        "tool_configs": {},
                                        "async_execution": False,
                                        "human_input": False,
                                        "llm_guardrail": task_response.llm_guardrail.model_dump() if task_response.llm_guardrail else None,
                                    }

                                    task_saved = await repo.create_single_task(
                                        task_data, agent_id, group_context
                                    )
                                    await session.commit()
                                    task_results.append({**task_saved, "_plan": task_plan})

                                    await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                        data={"type": "task_detail", "index": global_task_index, "task": task_saved},
                                        event="task_detail",
                                    ))
                                    logger.info(f"PROGRESSIVE [{generation_id}]: Task {global_task_index+1}/{len(plan_tasks)} done — {task_saved.get('name')}")

                                    # ── Detect GenieTool and suggest space ──
                                    needs_genie_config = any(
                                        tool_id_to_title.get(tid) == 'GenieTool' for tid in task_tool_ids
                                    )
                                    if needs_genie_config:
                                        suggested = await self._suggest_genie_space(
                                            task_name=task_saved["name"],
                                            task_description=task_saved.get("description", ""),
                                        )
                                        await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                            data={
                                                "type": "tool_config_needed",
                                                "task_id": task_saved["id"],
                                                "task_name": task_saved["name"],
                                                "tool_name": "GenieTool",
                                                "config_fields": ["spaceId"],
                                                "suggested_space": suggested,
                                            },
                                            event="tool_config_needed",
                                        ))

                                except Exception as e:
                                    logger.error(f"PROGRESSIVE [{generation_id}]: Task '{task_name}' failed: {e}")
                                    await session.rollback()
                                    await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                        data={
                                            "type": "entity_error", "index": global_task_index,
                                            "entity_type": "task", "name": task_name, "error": str(e),
                                        },
                                        event="entity_error",
                                    ))
                                global_task_index += 1

                        # ── Handle unassigned tasks at the end ──────────
                        for task_plan in unassigned_tasks:
                            task_name = task_plan.get("name", f"Task {global_task_index+1}")
                            try:
                                agent_context = self._find_agent_context(task_plan, agent_results)

                                task_request = TaskGenerationRequest(
                                    text=(
                                        f"Create a task named '{task_name}' "
                                        f"for a crew that: {request.prompt}"
                                    ),
                                    model=model,
                                    agent=agent_context,
                                    available_tools=available_tools_for_llm or None,
                                )
                                task_response = await task_gen_service.generate_task(
                                    task_request, group_context
                                )

                                agent_id = self._resolve_agent_id(task_plan, agent_results)

                                task_tool_ids = [
                                    tool_name_to_id_map[
                                        t.get("name") if isinstance(t, dict) else str(t)
                                    ]
                                    for t in (task_response.tools or [])
                                    if (t.get("name") if isinstance(t, dict) else str(t)) in tool_name_to_id_map
                                ]

                                task_data = {
                                    "name": task_response.name,
                                    "description": task_response.description,
                                    "expected_output": task_response.expected_output,
                                    "tools": task_tool_ids,
                                    "tool_configs": {},
                                    "async_execution": False,
                                    "human_input": False,
                                    "llm_guardrail": task_response.llm_guardrail.model_dump() if task_response.llm_guardrail else None,
                                }

                                task_saved = await repo.create_single_task(
                                    task_data, agent_id, group_context
                                )
                                await session.commit()
                                task_results.append({**task_saved, "_plan": task_plan})

                                await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                    data={"type": "task_detail", "index": global_task_index, "task": task_saved},
                                    event="task_detail",
                                ))
                                logger.info(f"PROGRESSIVE [{generation_id}]: Task {global_task_index+1}/{len(plan_tasks)} done — {task_saved.get('name')}")

                                # ── Detect GenieTool and suggest space ──
                                needs_genie_config = any(
                                    tool_id_to_title.get(tid) == 'GenieTool' for tid in task_tool_ids
                                )
                                if needs_genie_config:
                                    suggested = await self._suggest_genie_space(
                                        task_name=task_saved["name"],
                                        task_description=task_saved.get("description", ""),
                                    )
                                    await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                        data={
                                            "type": "tool_config_needed",
                                            "task_id": task_saved["id"],
                                            "task_name": task_saved["name"],
                                            "tool_name": "GenieTool",
                                            "config_fields": ["spaceId"],
                                            "suggested_space": suggested,
                                        },
                                        event="tool_config_needed",
                                    ))

                            except Exception as e:
                                logger.error(f"PROGRESSIVE [{generation_id}]: Task '{task_name}' failed: {e}")
                                await session.rollback()
                                await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                    data={
                                        "type": "entity_error", "index": global_task_index,
                                        "entity_type": "task", "name": task_name, "error": str(e),
                                    },
                                    event="entity_error",
                                ))
                            global_task_index += 1

                        # ── Fallback: synthesize tasks when generation produced none ──
                        # Per-task LLM generation occasionally fails for EVERY task
                        # (small models returning malformed JSON). Reaching save /
                        # auto-execute with agents but ZERO tasks dies in crew
                        # preparation ("Failed to prepare crew"). Synthesize a minimal
                        # task per planned task — from the plan name + the user's
                        # request — so the crew stays runnable.
                        if not task_results and plan_tasks and agent_results:
                            logger.warning(
                                f"PROGRESSIVE [{generation_id}]: all task generation failed — "
                                f"synthesizing {len(plan_tasks)} task(s) from the plan"
                            )
                            for task_plan in plan_tasks:
                                task_name = task_plan.get("name", f"Task {global_task_index + 1}")
                                try:
                                    agent_id = self._resolve_agent_id(task_plan, agent_results)
                                    task_data = {
                                        "name": task_name,
                                        "description": (
                                            f"{task_name} — complete this task for a crew that: "
                                            f"{request.prompt}"
                                        ),
                                        "expected_output": "A complete, well-structured result for this task.",
                                        "tools": [],
                                        "tool_configs": {},
                                        "async_execution": False,
                                        "human_input": False,
                                        "llm_guardrail": None,
                                    }
                                    task_saved = await repo.create_single_task(
                                        task_data, agent_id, group_context
                                    )
                                    await session.commit()
                                    task_results.append({**task_saved, "_plan": task_plan})
                                    await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                        data={"type": "task_detail", "index": global_task_index, "task": task_saved},
                                        event="task_detail",
                                    ))
                                    logger.info(
                                        f"PROGRESSIVE [{generation_id}]: Synthesized fallback task — "
                                        f"{task_saved.get('name')}"
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"PROGRESSIVE [{generation_id}]: Fallback task '{task_name}' failed: {e}"
                                    )
                                    await session.rollback()
                                    await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                                        data={
                                            "type": "entity_error", "index": global_task_index,
                                            "entity_type": "task", "name": task_name, "error": str(e),
                                        },
                                        event="entity_error",
                                    ))
                                global_task_index += 1

                        # ── Phase 4: Resolve task dependencies ────────────
                        await self._resolve_progressive_dependencies(
                            task_results, generation_id, repo
                        )
                        await session.commit()

                    except Exception as e:
                        await session.rollback()
                        raise

                # Broadcast resolved dependencies so frontend can create
                # task-to-task edges with real DB IDs.
                for t in task_results:
                    resolved = t.get("context", [])
                    if resolved:
                        await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                            data={
                                "type": "dependencies_resolved",
                                "task_id": t["id"],
                                "task_name": t.get("name", ""),
                                "context": resolved,
                            },
                            event="dependencies_resolved",
                        ))

                # ── Done ──────────────────────────────────────────────────
                clean_tasks = [{k: v for k, v in t.items() if k != "_plan"} for t in task_results]
                gen_complete_data = {
                    "type": "generation_complete",
                    "status": "completed",
                    "agents": agent_results,
                    "tasks": clean_tasks,
                }

                # Measurement ledger — written with the ids that were actually
                # persisted, which later link this generation to the run it
                # becomes.
                await self._record_recipe_trial_isolated(
                    recipe_decision, agent_results, clean_tasks, group_context
                )

                # Tell the user what this drew on, AFTER the fact. Reuse is
                # surfaced, never gated: an approval step on every generation
                # would slow the common case down to re-confirm a judgement
                # already made when the recipe was marked good.
                if recipe_decision is not None and recipe_decision.injected_labels:
                    gen_complete_data["reused_recipes"] = recipe_decision.injected_labels

                # ── ChatMode auto-execute ─────────────────────────────────
                # ChatMode generates AND runs in one backend flow so the run
                # survives the user switching sessions before the plan finishes
                # — the frontend never has to round-trip a createExecution call
                # (which is what used to drop the run on session switch). The
                # execution id is FOLDED INTO generation_complete (a single
                # terminal event) so the frontend can stop the generation stream
                # immediately — no open-window for SSE reconnect/replay to
                # re-deliver and cross-route trace events. AgentBuilder leaves
                # auto_execute False and only renders the plan as nodes.
                if getattr(request, "auto_execute", False) and not clean_tasks:
                    # A crew with zero tasks cannot run — crew preparation requires
                    # at least one task. Don't launch it (it would crash in
                    # preparation); surface an actionable error on the
                    # generation_complete event instead. This is the terminal guard
                    # after the synthesize-tasks fallback also came up empty.
                    msg = (
                        "Auto-execute skipped: the crew has no runnable tasks "
                        "(task generation and the fallback both produced none)."
                    )
                    logger.error(f"PROGRESSIVE [{generation_id}]: {msg}")
                    gen_complete_data["execution_error"] = msg
                elif getattr(request, "auto_execute", False):
                    try:
                        from src.schemas.execution import CrewConfig
                        from src.services.execution_service import ExecutionService

                        crew_config = CrewConfig(
                            **self.build_crew_config_from_generated(
                                request, agent_results, clean_tasks
                            )
                        )
                        # session=None: the whole execution stack opens its own
                        # request_scoped_session() (already detached above), so a
                        # request-scoped session would only be a closed handle.
                        # background_tasks=None launches via asyncio.create_task.
                        exec_result = await ExecutionService(session=None).create_execution(
                            config=crew_config,
                            background_tasks=None,
                            group_context=group_context,
                        )
                        gen_complete_data["execution_id"] = exec_result.get("execution_id")
                        gen_complete_data["run_name"] = exec_result.get("run_name")
                        logger.info(
                            f"PROGRESSIVE [{generation_id}]: Auto-execute launched "
                            f"execution {exec_result.get('execution_id')}"
                        )
                    except Exception as exec_err:
                        logger.error(
                            f"PROGRESSIVE [{generation_id}]: Auto-execute failed: "
                            f"{exec_err}"
                        )
                        logger.error(traceback.format_exc())
                        gen_complete_data["execution_error"] = str(exec_err)

                await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                    data=gen_complete_data,
                    event="generation_complete",
                ))
                logger.info(f"PROGRESSIVE [{generation_id}]: Generation complete")

                # Populate the trace Response (otherwise it shows null).
                try:
                    from src.services.otel_tracing.mlflow_parent_setup import (
                        set_root_span_outputs,
                    )
                    set_root_span_outputs(root_span, gen_complete_data)
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"PROGRESSIVE [{generation_id}]: Unexpected error: {e}")
                logger.error(traceback.format_exc())
                await sse_manager.broadcast_to_job(generation_id, SSEEvent(
                    data={"type": "generation_failed", "status": "failed", "error": str(e)},
                    event="generation_failed",
                ))

    async def _run_chat_fast_path(
        self,
        request: "CrewStreamingRequest",
        group_context: Optional[GroupContext],
        generation_id: str,
        root_span: Any,
    ) -> None:
        """ChatMode 'chat' fast path — no crew generation.

        Builds a default single assistant + one task carrying the user's request
        (plus any explicitly-attached tools / MCP servers / Agent Bricks
        endpoints from the chat "+" menu — no LLM needed to pick those) and
        auto-executes the light agent. Emits ONLY the terminal
        ``generation_complete`` event the chat UI requires (with ``agents``,
        ``tasks`` and the ``execution_id``); no plan/agent/task cards, since
        nothing was generated. Cuts chat latency from ~3 generation LLM calls +
        the answer down to just the answer.
        """
        from src.schemas.execution import CrewConfig
        from src.services.execution_service import ExecutionService

        user_request = request.original_prompt or request.prompt or ""
        attached_tools = list(getattr(request, "tools", None) or [])

        # A default lightweight assistant + a single task. The config builder
        # injects the attached MCP servers / Agent Bricks endpoints and grounds the
        # task with the user's request, sets execution_type='agent' (light) and carries
        # session_id / memory_workspace_scope — identical to a generated chat agent,
        # only without the LLM generation.
        agent_results = [{
            "id": "chat",
            "name": "Assistant",
            "role": "Assistant",
            "goal": "Answer the user's request helpfully, accurately and concisely.",
            "backstory": "You are a helpful AI assistant.",
            "tools": attached_tools,
        }]
        clean_tasks = [{
            "id": "chat",
            "name": "Chat response",
            "description": "Respond directly and helpfully to the user's request.",
            "expected_output": "A helpful, complete answer to the user's request.",
            "agent_id": "chat",
            "tools": attached_tools,
            "context": [],
        }]

        gen_complete_data: Dict[str, Any] = {
            "type": "generation_complete",
            "status": "completed",
            "agents": agent_results,
            "tasks": clean_tasks,
            "user_request": user_request,
        }

        try:
            crew_config = CrewConfig(
                **self.build_crew_config_from_generated(
                    request, agent_results, clean_tasks
                )
            )
            exec_result = await ExecutionService(session=None).create_execution(
                config=crew_config,
                background_tasks=None,
                group_context=group_context,
            )
            gen_complete_data["execution_id"] = exec_result.get("execution_id")
            gen_complete_data["run_name"] = exec_result.get("run_name")
            logger.info(
                f"PROGRESSIVE [{generation_id}]: chat fast-path launched "
                f"execution {exec_result.get('execution_id')} (no generation)"
            )
        except Exception as exec_err:
            logger.error(
                f"PROGRESSIVE [{generation_id}]: chat fast-path execute "
                f"failed: {exec_err}"
            )
            logger.error(traceback.format_exc())
            gen_complete_data["execution_error"] = str(exec_err)

        await sse_manager.broadcast_to_job(generation_id, SSEEvent(
            data=gen_complete_data,
            event="generation_complete",
        ))
        logger.info(f"PROGRESSIVE [{generation_id}]: chat fast-path complete")

        try:
            from src.services.otel_tracing.mlflow_parent_setup import (
                set_root_span_outputs,
            )
            set_root_span_outputs(root_span, gen_complete_data)
        except Exception:
            pass

    # ── Progressive helpers ───────────────────────────────────────────

    async def _suggest_genie_space(self, task_name: str, task_description: str) -> Optional[Dict]:
        """Query Genie spaces and suggest the best match based on task context."""
        try:
            from src.repositories.genie_repository import GenieRepository
            genie_repo = GenieRepository()

            # Search using task name as query
            response = await genie_repo.get_spaces(
                search_query=task_name,
                page_size=5,
                enabled_only=True,
            )

            if response.spaces:
                best = response.spaces[0]
                return {"id": best.id, "name": best.name, "description": best.description or ""}

            # Fallback: get first available space if search returned nothing
            response = await genie_repo.get_spaces(page_size=1, enabled_only=True)
            if response.spaces:
                best = response.spaces[0]
                return {"id": best.id, "name": best.name, "description": best.description or ""}

            return None
        except Exception as e:
            logger.warning(f"Failed to suggest Genie space: {e}")
            return None

    async def _generate_crew_plan(
        self,
        request: CrewStreamingRequest,
        group_context: Optional[GroupContext],
        model: str,
        max_agents: int = 1,
        max_tasks: int = 1,
        exemplars: str = "",
    ) -> Dict[str, Any]:
        """Fast LLM call to get crew outline (names/roles only).

        NOTE: This method is called from create_crew_progressive which runs as
        a background task after the request-scoped session is closed. It uses
        an independent session to log the LLM interaction.

        Args:
            max_agents: Maximum number of agents to generate. Injected into
                the user message so the LLM plans within the limit rather
                than generating excess agents that get truncated (which would
                lose the user's actual goal).
            max_tasks: Maximum number of tasks to generate.
        """
        # Dedicated lightweight plan template (~1.4k chars). The old approach
        # sent the full 9.4k-char generate_crew template and then told the
        # model to IGNORE most of it (descriptions/backstories/tools) — ~2k
        # wasted prompt tokens with contradictory instructions on every
        # "create a crew" chat message.
        system_message = await TemplateService.get_effective_template_content(
            "generate_crew_plan", group_context
        )
        if not system_message:
            # Fallback for DBs seeded before the plan template existed.
            system_message = await TemplateService.get_effective_template_content(
                "generate_crew", group_context
            )
            if not system_message:
                raise KasalError("Required prompt template 'generate_crew_plan' not found")
            planning_prefix = (
                "You are generating a PLAN OUTLINE only. Return a lightweight JSON with:\n"
                '{"complexity": "light|standard|complex", "process_type": "sequential|parallel", '
                '"agents": [{"name": "...", "role": "..."}], '
                '"tasks": [{"name": "...", "assigned_agent": "...", "context": []}]}\n'
                "Do NOT include descriptions, goals, backstories, or tools — those will be generated separately.\n\n"
            )
            system_message = planning_prefix + system_message

        # Inject cap constraints based on verb-counted max_tasks
        system_cap = (
            f"OUTPUT CONSTRAINT: Generate up to {max_agents} agent(s) and "
            f"up to {max_tasks} task(s). Each distinct action verb in the user's "
            f"message should map to a separate task. Use the minimum number of "
            f"agents needed to cover the tasks.\n\n"
        )
        cap_instruction = (
            f"\n\nCONSTRAINT: Generate up to {max_agents} agent(s) and "
            f"up to {max_tasks} task(s). Match task count to the number of "
            f"distinct action verbs in the message."
        )

        user_message = request.prompt + cap_instruction

        messages = [
            # Exemplars go on the SYSTEM message so they cannot displace the
            # hardcoded verb-to-task few-shots below, which own the output format.
            {"role": "system", "content": system_cap + system_message + exemplars},
        ]

        # Few-shot examples showing verb-to-task mapping AND both agent-count
        # outcomes: consolidation (one specialist covers related tasks) and
        # escalation (genuinely different specialisms get their own agent).
        # Without the second example the model over-consolidated to one agent
        # even when the cap allowed more.
        messages.extend([
            {
                "role": "user",
                "content": (
                    "gather swiss news, create a presentation, and send an email to the team\n\n"
                    "CONSTRAINT: Generate up to 2 agent(s) and up to 3 task(s). "
                    "Match task count to the number of distinct action verbs in the message."
                ),
            },
            {
                "role": "assistant",
                "content": '{"complexity":"complex","process_type":"sequential","agents":[{"name":"Swiss News Specialist","role":"News Research and Content Creation Expert"}],"tasks":[{"name":"Gather Swiss News","assigned_agent":"Swiss News Specialist","context":[]},{"name":"Create News Presentation","assigned_agent":"Swiss News Specialist","context":["Gather Swiss News"]},{"name":"Send Email to Team","assigned_agent":"Swiss News Specialist","context":["Create News Presentation"]}]}',
            },
            {
                "role": "user",
                "content": (
                    "research our top competitors, analyze their pricing, and write a summary report\n\n"
                    "CONSTRAINT: Generate up to 2 agent(s) and up to 3 task(s). "
                    "Match task count to the number of distinct action verbs in the message."
                ),
            },
            {
                "role": "assistant",
                "content": '{"complexity":"standard","process_type":"sequential","agents":[{"name":"Market Research Analyst","role":"Competitive research and pricing analysis specialist"},{"name":"Report Writer","role":"Business report composition specialist"}],"tasks":[{"name":"Research Competitors","assigned_agent":"Market Research Analyst","context":[]},{"name":"Analyze Pricing","assigned_agent":"Market Research Analyst","context":["Research Competitors"]},{"name":"Write Summary Report","assigned_agent":"Report Writer","context":["Analyze Pricing"]}]}',
            },
        ])

        messages.append(
            {"role": "user", "content": user_message},
        )

        # 4000 (was 2000): reasoning models (Qwen3-thinking, gpt-oss, R1-style)
        # spend part of the budget on hidden reasoning tokens, so 2000 could
        # exhaust before the plan JSON closed → truncated, unparseable output.
        content = await LLMManager.completion(
            messages=messages,
            model=model,
            temperature=0.3,
            max_tokens=4000,
        )

        # Log via an independent session (the request-scoped session is closed
        # by the time this background task runs).
        from src.db.session import async_session_factory as _plan_session_factory
        try:
            async with _plan_session_factory() as log_session:
                log_service = LLMLogService(LLMLogRepository(log_session))
                await log_service.create_log(
                    endpoint="generate-crew-plan",
                    prompt=f"System: {system_message}\nUser: {user_message}",
                    response=content,
                    model=model,
                    status="success",
                    group_context=group_context,
                )
                await log_session.commit()
                logger.info("Logged generate-crew-plan interaction to database")
        except Exception as e:
            logger.error(f"Failed to log crew plan LLM interaction: {e}")

        plan = robust_json_parser(content)

        if not isinstance(plan.get("agents"), list) or len(plan["agents"]) == 0:
            raise BadRequestError("Plan returned no agents")

        if not isinstance(plan.get("tasks"), list) or len(plan["tasks"]) == 0:
            raise BadRequestError("Plan returned no tasks")

        return plan

    @staticmethod
    def _find_agent_context(
        task_plan: Dict[str, Any],
        agent_results: List[Dict[str, Any]],
    ) -> Optional[TaskGenAgent]:
        """Build a TaskGenAgent for the task's assigned agent, if found."""
        assigned = task_plan.get("assigned_agent", "")
        if not assigned:
            return None

        for agent in agent_results:
            if agent.get("name", "").lower() == assigned.lower():
                return TaskGenAgent(
                    name=agent["name"],
                    role=agent.get("role", ""),
                    goal=agent.get("goal", ""),
                    backstory=agent.get("backstory", ""),
                )
        return None

    @staticmethod
    def _resolve_agent_id(
        task_plan: Dict[str, Any],
        agent_results: List[Dict[str, Any]],
    ) -> Optional[str]:
        """Resolve the assigned_agent name to a database agent ID."""
        assigned = task_plan.get("assigned_agent", "")
        if not assigned:
            return agent_results[0]["id"] if agent_results else None

        for agent in agent_results:
            if agent.get("name", "").lower() == assigned.lower():
                return agent["id"]

        # Fallback: first agent
        return agent_results[0]["id"] if agent_results else None

    async def _resolve_progressive_dependencies(
        self,
        task_results: List[Dict[str, Any]],
        generation_id: str,
        repo: Optional["CrewGeneratorRepository"] = None,
    ) -> None:
        """Resolve task context references (names) to database IDs."""
        effective_repo = repo or self.crew_generator_repository

        task_name_to_id: Dict[str, str] = {}
        for t in task_results:
            name = t.get("name", "")
            tid = t.get("id", "")
            if name and tid:
                task_name_to_id[name] = tid

        for t in task_results:
            plan = t.get("_plan", {})
            context_refs = plan.get("context", [])
            if not context_refs:
                continue

            resolved_ids = []
            for ref in context_refs:
                dep_id = task_name_to_id.get(ref)
                if dep_id and dep_id != t.get("id"):
                    resolved_ids.append(dep_id)

            if resolved_ids:
                try:
                    await effective_repo.update_task_dependencies(
                        t["id"], resolved_ids
                    )
                    t["context"] = resolved_ids
                    logger.info(
                        f"PROGRESSIVE [{generation_id}]: "
                        f"Task '{t.get('name')}' dependencies: {resolved_ids}"
                    )
                except Exception as e:
                    logger.error(
                        f"PROGRESSIVE [{generation_id}]: "
                        f"Failed to set deps for '{t.get('name')}': {e}"
                    )