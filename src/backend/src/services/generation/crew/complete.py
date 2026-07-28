"""One-shot crew generation (``POST /crew/create-crew``).

A single LLM call returns the whole crew, which is then validated and
persisted. Used by the crew-planning dialog; ChatMode and the canvas chat
input take the progressive path instead."""

import logging
import os
import traceback
from typing import Dict, Any, List, Tuple, Optional
from src.utils.prompt_utils import robust_json_parser
from src.services.tools.tool_service import ToolService
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse, CrewStreamingRequest
from src.services.llm.manager import LLMManager
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class CompleteGenerationMixin:
    """One-shot crew generation (``POST /crew/create-crew``).

    A single LLM call returns the whole crew, which is then validated and
    persisted. Used by the crew-planning dialog; ChatMode and the canvas chat
    input take the progressive path instead."""

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
            from src.services.mlflow.tracing import start_root_trace
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
