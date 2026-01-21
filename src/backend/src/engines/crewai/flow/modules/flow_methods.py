"""
Flow methods module for CrewAI flow execution.

This module handles dynamic creation of flow methods (starting points, listeners, routers).
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Callable
from crewai.flow.flow import listen, router, start, and_, or_
from crewai import Crew, Process, Task

from src.core.logger import LoggerManager
from .flow_state import FlowStateManager

# Initialize logger - use flow logger for flow execution
logger = LoggerManager.get_instance().flow


def extract_final_answer(results) -> str:
    """
    Extract only the final answer from flow results, excluding the thinking process.

    CrewAI agent outputs often include the full thinking process followed by "Final Answer:".
    This function extracts only the final answer portion for cleaner context passing between
    crews in a flow.

    Args:
        results: Flow results which can be:
            - List of dicts with 'content' key
            - CrewOutput/TaskOutput object with 'raw' attribute
            - String
            - Other iterable

    Returns:
        str: The extracted final answer, or the full content if no "Final Answer:" marker found
    """
    if not results:
        return ""

    # Get the first result
    first_result = results[0] if hasattr(results, '__getitem__') else results

    # Handle list of dicts with 'content' key
    if isinstance(first_result, list):
        # Multiple content items - extract final answer from each and join
        contents = []
        for item in first_result:
            if isinstance(item, dict) and 'content' in item:
                content = item['content']
                # Extract only the Final Answer portion if present
                if 'Final Answer:' in content:
                    # Get everything after "Final Answer:"
                    final_answer_part = content.split('Final Answer:')[-1].strip()
                    contents.append(final_answer_part)
                elif 'Final Answer' in content:
                    # Handle case without colon
                    final_answer_part = content.split('Final Answer')[-1].strip()
                    # Remove leading colon or newline if present
                    final_answer_part = final_answer_part.lstrip(':').strip()
                    contents.append(final_answer_part)
                else:
                    contents.append(content)
            elif isinstance(item, str):
                contents.append(item)
        return '\n\n'.join(contents)

    # Handle dict with 'content' key
    if isinstance(first_result, dict) and 'content' in first_result:
        content = first_result['content']
    # Handle objects with 'raw' attribute (TaskOutput, CrewOutput)
    elif hasattr(first_result, 'raw') and first_result.raw:
        content = str(first_result.raw)
    # Handle string
    elif isinstance(first_result, str):
        content = first_result
    else:
        # Fallback to string conversion
        content = str(first_result)

    # Extract only the Final Answer portion if present
    if 'Final Answer:' in content:
        return content.split('Final Answer:')[-1].strip()
    elif 'Final Answer' in content:
        final_answer_part = content.split('Final Answer')[-1].strip()
        return final_answer_part.lstrip(':').strip()

    return content


async def get_model_context_limits(agent, group_context) -> tuple[int, int]:
    """
    Get the context window and max output tokens for the agent's model using ModelConfigService.

    Args:
        agent: CrewAI Agent instance with llm attribute
        group_context: Group context for multi-tenant isolation

    Returns:
        tuple[int, int]: (context_window_tokens, max_output_tokens), defaults to (128000, 16000) if not found
    """
    default_context_window = 128000
    default_max_output = 16000

    try:
        # Get the model name from agent's llm attribute
        model_name = None
        if hasattr(agent, 'llm') and agent.llm:
            # The agent.llm could be a LiteLLM instance or string
            if isinstance(agent.llm, str):
                model_name = agent.llm
            elif hasattr(agent.llm, 'model'):
                model_name = agent.llm.model
            else:
                logger.warning(f"Agent LLM has unknown type: {type(agent.llm)}")
                return default_context_window, default_max_output

        if not model_name:
            logger.info(f"No model name found for agent, using defaults")
            return default_context_window, default_max_output

        # Extract group_id from group_context
        group_id = None
        if group_context:
            if hasattr(group_context, 'primary_group_id'):
                group_id = group_context.primary_group_id
            elif hasattr(group_context, 'group_ids') and group_context.group_ids:
                group_id = group_context.group_ids[0]

        if not group_id:
            logger.info(f"No group_id found, using defaults")
            return default_context_window, default_max_output

        # Use ModelConfigService to get model configuration
        from src.db.session import async_session_factory
        from src.services.model_config_service import ModelConfigService

        async with async_session_factory() as session:
            model_config_service = ModelConfigService(session, group_id)
            model_config = await model_config_service.find_by_key(model_name)

            if model_config:
                context_window = model_config.context_window if hasattr(model_config, 'context_window') and model_config.context_window else default_context_window
                max_output = model_config.max_output_tokens if hasattr(model_config, 'max_output_tokens') and model_config.max_output_tokens else default_max_output

                logger.info(f"Model {model_name}: context_window={context_window}, max_output_tokens={max_output}")
                return context_window, max_output

            logger.info(f"No model config found for {model_name}, using defaults")
            return default_context_window, default_max_output

    except Exception as e:
        logger.warning(f"Error getting model config: {e}, using defaults")
        return default_context_window, default_max_output


class FlowMethodFactory:
    """
    Factory for creating dynamic flow methods (starting points, listeners, routers).
    """

    @staticmethod
    def create_starting_point_crew_method(
        method_name: str,
        task_list: List[Any],
        crew_name: str,
        callbacks: Optional[Dict[str, Any]],
        group_context: Optional[Any],
        create_execution_callbacks: Callable,
        crew_data: Optional[Any] = None
    ) -> Callable:
        """
        Create a starting point method that executes multiple tasks as a crew.

        Args:
            method_name: Name of the method
            task_list: List of Task objects to execute sequentially (with task.context dependencies set)
            crew_name: Name of the crew
            callbacks: Callbacks dict with job_id
            group_context: Group context for multi-tenant isolation
            create_execution_callbacks: Function to create execution callbacks
            crew_data: Crew data from database for configuration inheritance

        Returns:
            Async function decorated with @start()
        """
        @start()
        async def starting_point_crew_method(self):
            """Starting point method - executes crew with multiple sequential tasks."""
            logger.info("="*80)
            logger.info(f"START CREW METHOD CALLED - Crew: {crew_name}")
            logger.info(f"Number of tasks: {len(task_list)}")
            logger.info("="*80)

            # Collect all unique agents from tasks
            agents = []
            agent_roles_seen = set()

            for task in task_list:
                if hasattr(task, 'agent') and task.agent:
                    agent_role = task.agent.role if hasattr(task.agent, 'role') else 'Unknown'
                    if agent_role not in agent_roles_seen:
                        agents.append(task.agent)
                        agent_roles_seen.add(agent_role)
                        logger.info(f"  Agent: {agent_role}")

                        # Log if agent has no tools
                        if not hasattr(task.agent, 'tools') or not task.agent.tools:
                            logger.info(f"  Agent {agent_role} has no tools assigned but will continue with execution")

            logger.info(f"Total unique agents: {len(agents)}")
            logger.info(f"Total tasks: {len(task_list)}")

            # Log task dependencies
            for idx, task in enumerate(task_list):
                task_desc = task.description[:50] + '...' if len(task.description) > 50 else task.description
                if hasattr(task, 'context') and task.context and isinstance(task.context, list):
                    logger.info(f"  Task {idx}: {task_desc} (depends on {len(task.context)} previous task(s))")
                else:
                    logger.info(f"  Task {idx}: {task_desc} (no dependencies)")

            # Create crew with all tasks
            # Determine memory setting based on agent configuration
            # This follows the same pattern as CrewPreparation in regular crew execution
            logger.info(f"Creating Crew instance: {crew_name}")

            # Determine crew memory setting - crew_data takes priority, then agent settings
            crew_memory = True  # Default
            if crew_data and hasattr(crew_data, 'memory') and crew_data.memory is not None:
                crew_memory = crew_data.memory
                logger.info(f"Using crew memory setting from configuration: {crew_memory}")
            else:
                # Check agent memory settings to determine crew memory
                agents_with_memory_enabled = []
                agents_with_memory_disabled = []
                for agent in agents:
                    agent_role = agent.role if hasattr(agent, 'role') else 'Unknown'
                    # Check if agent has memory attribute set
                    if hasattr(agent, 'memory'):
                        if agent.memory is False:
                            agents_with_memory_disabled.append(agent_role)
                        else:
                            agents_with_memory_enabled.append(agent_role)
                    else:
                        # Default to memory enabled if not specified
                        agents_with_memory_enabled.append(agent_role)

                # Determine crew memory setting
                if agents_with_memory_disabled and not agents_with_memory_enabled:
                    crew_memory = False
                    logger.info(f"All agents have memory disabled - setting crew memory to False")
                else:
                    logger.info(f"At least one agent has memory enabled - setting crew memory to True")

            # Determine process type from crew_data
            process_type = Process.sequential  # Default
            if crew_data and hasattr(crew_data, 'process') and crew_data.process:
                if crew_data.process.lower() == 'hierarchical':
                    process_type = Process.hierarchical
                    logger.info(f"Using hierarchical process from crew configuration")
                else:
                    logger.info(f"Using sequential process from crew configuration")
            else:
                logger.info(f"Using default sequential process")

            # Determine verbose setting from crew_data
            crew_verbose = True  # Default
            if crew_data and hasattr(crew_data, 'verbose') and crew_data.verbose is not None:
                crew_verbose = crew_data.verbose

            crew_kwargs = {
                'name': crew_name,
                'agents': agents,
                'tasks': task_list,  # Pass ALL tasks - CrewAI will respect task.context for sequential execution
                'verbose': crew_verbose,
                'process': process_type,
                'memory': crew_memory,
            }

            # Add planning configuration if enabled
            if crew_data and hasattr(crew_data, 'planning') and crew_data.planning:
                crew_kwargs['planning'] = True
                logger.info(f"Planning enabled for crew from configuration")

            # Add reasoning configuration if enabled
            if crew_data and hasattr(crew_data, 'reasoning') and crew_data.reasoning:
                crew_kwargs['reasoning'] = True
                logger.info(f"Reasoning enabled for crew from configuration")

            # Log crew configuration for debugging
            logger.info(f"📋 Crew configuration: memory={crew_memory}, process={process_type}, planning={crew_kwargs.get('planning', False)}, reasoning={crew_kwargs.get('reasoning', False)}")

            crew = Crew(**crew_kwargs)
            logger.info(f"Crew instance '{crew_name}' created successfully with {len(task_list)} tasks, kwargs: {list(crew_kwargs.keys())}")

            # Set up execution callbacks
            job_id = None
            if callbacks:
                job_id = callbacks.get('job_id')
                if job_id:
                    logger.info(f"Extracted job_id from callbacks: {job_id}")

            # Create and set synchronous step and task callbacks
            if job_id:
                try:
                    step_callback, task_callback = create_execution_callbacks(
                        job_id=job_id,
                        config={},
                        group_context=group_context,
                        crew=crew
                    )
                    crew.step_callback = step_callback
                    crew.task_callback = task_callback
                    logger.info(f"✅ Set synchronous execution callbacks on crew for job {job_id}")
                except Exception as callback_error:
                    logger.warning(f"Failed to set execution callbacks: {callback_error}")
            else:
                logger.warning("No job_id available, skipping execution callbacks setup")

            try:
                # Enhanced logging for truncation diagnosis
                import time
                start_time = time.time()

                # Log LLM configuration details for first agent
                first_agent = agents[0] if agents else None
                if first_agent and hasattr(first_agent, 'llm') and first_agent.llm:
                    llm = first_agent.llm
                    llm_info = {
                        'model': getattr(llm, 'model', 'unknown'),
                        'max_tokens': getattr(llm, 'max_tokens', 'not set'),
                        'timeout': getattr(llm, 'timeout', 'not set'),
                    }
                    logger.info(f"📊 LLM Configuration: {llm_info}")

                logger.info(f"📝 Total tasks: {len(task_list)}")
                logger.info("⏱️ Calling crew.kickoff_async() with 10 minute timeout...")

                result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)

                elapsed_time = time.time() - start_time
                logger.info(f"⏱️ Crew '{crew_name}' execution took {elapsed_time:.2f} seconds")

                # Log result details for truncation diagnosis
                if result:
                    if hasattr(result, 'raw') and result.raw:
                        result_length = len(str(result.raw))
                        logger.info(f"✅ kickoff_async completed - result.raw length: {result_length} chars")
                        raw_str = str(result.raw)
                        if result_length > 400:
                            logger.info(f"📄 Result preview - First 200 chars: {raw_str[:200]}")
                            logger.info(f"📄 Result preview - Last 200 chars: {raw_str[-200:]}")
                        else:
                            logger.info(f"📄 Full result: {raw_str}")
                    else:
                        logger.info(f"✅ kickoff_async completed - result type: {type(result)}, str length: {len(str(result))}")
                else:
                    logger.warning("⚠️ kickoff_async returned None or empty result")

                # Return serializable value for @persist compatibility
                # CrewOutput objects are not JSON-serializable, so extract raw content
                serializable_result = None
                if hasattr(result, 'raw') and result.raw:
                    serializable_result = result.raw
                elif result is not None:
                    serializable_result = str(result)
                else:
                    serializable_result = result

                # Store result in state for checkpoint resume support
                # This allows skipped crews to retrieve the output when resuming
                if serializable_result is not None:
                    if hasattr(self, 'state'):
                        self.state[method_name] = serializable_result
                        self.state[crew_name] = serializable_result
                        logger.info(f"📦 Stored crew output in state['{method_name}'] and state['{crew_name}'] for checkpoint support")

                return serializable_result
            except asyncio.TimeoutError:
                elapsed_time = time.time() - start_time if 'start_time' in dir() else 0
                logger.error(f"❌ Crew '{crew_name}' execution timed out after {elapsed_time:.2f} seconds (limit: 600s)")
                raise TimeoutError(f"Crew '{crew_name}' execution timed out after 10 minutes")
            except Exception as e:
                elapsed_time = time.time() - start_time if 'start_time' in dir() else 0
                logger.error(f"❌ Error during crew '{crew_name}' kickoff after {elapsed_time:.2f} seconds: {e}", exc_info=True)
                raise

        # Set metadata
        starting_point_crew_method.__name__ = method_name
        starting_point_crew_method.__qualname__ = method_name

        return starting_point_crew_method

    @staticmethod
    def create_listener_method(
        method_name: str,
        listener_tasks: List[Any],
        method_condition: Any,
        condition_type: str,
        callbacks: Optional[Dict[str, Any]],
        group_context: Optional[Any],
        create_execution_callbacks: Callable,
        crew_name: Optional[str] = None,
        crew_data: Optional[Any] = None
    ) -> Callable:
        """
        Create a listener method for the flow.

        Args:
            method_name: Name of the method
            listener_tasks: List of task objects to execute
            method_condition: Condition for @listen() decorator (method name, and_(), or or_())
            condition_type: Type of condition (NONE, AND, OR)
            callbacks: Callbacks dict with job_id
            group_context: Group context for multi-tenant isolation
            create_execution_callbacks: Function to create execution callbacks
            crew_name: Name of the crew from flow configuration (for trace tracking)
            crew_data: Crew data from database containing memory and configuration settings

        Returns:
            Async function decorated with @listen()
        """
        decorator = listen(method_condition)

        @decorator
        async def listener_method(self, *results):
            """Listener method - executes when listening to a specific event."""
            logger.info("="*80)
            condition_desc = f"{condition_type} conditional " if condition_type in ["AND", "OR"] else ""
            logger.info(f"LISTENER METHOD CALLED - Executing {condition_desc}listener with {len(listener_tasks)} tasks")

            # Log and store previous outputs from preceding methods
            if results:
                logger.info(f"📥 RECEIVED {len(results)} PREVIOUS OUTPUT(S):")
                for i, result in enumerate(results):
                    result_str = str(result)
                    logger.info(f"  Output {i}: {result_str[:200]}...")
                    # Store each result in state
                    self.state[f'previous_output_{i}'] = result
                    if i == 0:
                        # Also store first output as 'previous_output' for easy access
                        self.state['previous_output'] = result
            else:
                logger.info("📭 No previous outputs received")

            logger.info("="*80)

            # Create runtime tasks with previous output injected into descriptions
            # This follows the official CrewAI Flow pattern of creating tasks at runtime
            runtime_tasks = []
            previous_output_context = ""

            if results:
                # Get the first agent to determine context limits
                first_agent = listener_tasks[0].agent if listener_tasks else None

                # Get model's context window and max output tokens using ModelConfigService
                context_window_tokens, max_output_tokens = await get_model_context_limits(first_agent, group_context) if first_agent else (128000, 16000)

                # Calculate available input budget (subtract output reservation)
                available_input_tokens = context_window_tokens - max_output_tokens

                # Allocate 60% of available input for previous output
                # This leaves 40% for system prompts, tools, conversation history, and safety buffer
                max_context_tokens = int(available_input_tokens * 0.6)

                # Convert tokens to characters (using 3.5 chars/token for safety)
                # This conservative ratio accounts for code and structured data
                max_context_length = int(max_context_tokens * 3.5)

                logger.info(f"Model limits: context={context_window_tokens} tokens, max_output={max_output_tokens} tokens")
                logger.info(f"Available input: {available_input_tokens} tokens, allocating {max_context_tokens} tokens ({max_context_length} chars) for previous output")

                # Create a concise context string to inject into task descriptions
                # Use extract_final_answer to get only the final answer, not the full thinking process
                previous_output_str = extract_final_answer(results)
                if len(previous_output_str) > max_context_length:
                    previous_output_context = f"\n\nContext from previous step:\n{previous_output_str[:max_context_length]}...\n(Output truncated for brevity)"
                else:
                    previous_output_context = f"\n\nContext from previous step:\n{previous_output_str}"
                logger.info(f"📤 Injecting previous output context into task descriptions ({len(previous_output_context)} chars)")

            # Create new Task objects with modified descriptions
            for task in listener_tasks:
                # Create new task with injected context
                runtime_task = Task(
                    description=f"{task.description}{previous_output_context}",
                    agent=task.agent,
                    expected_output=task.expected_output if hasattr(task, 'expected_output') else "Task completed successfully"
                )
                runtime_tasks.append(runtime_task)
                logger.info(f"Created runtime task with injected context for agent: {task.agent.role}")

            # Create a crew with runtime tasks
            agents = list(set(task.agent for task in runtime_tasks))
            logger.info(f"Number of agents in listener: {len(agents)}")

            # Log if agents have no tools
            for agent in agents:
                if not hasattr(agent, 'tools') or not agent.tools:
                    logger.info(f"Agent {agent.role} has no tools assigned but will continue with execution")

            logger.info("Creating Crew instance for listener method")
            logger.info(f"Listener has {len(agents)} agents and {len(runtime_tasks)} tasks")

            # Use provided crew name from flow config, fallback to first agent role
            listener_crew_name = crew_name if crew_name else (agents[0].role if agents and hasattr(agents[0], 'role') and agents[0].role else "Listener Crew")
            logger.info(f"Creating listener crew with name: {listener_crew_name}")

            # Determine crew memory setting - crew_data takes priority, then agent settings
            crew_memory = True  # Default
            if crew_data and hasattr(crew_data, 'memory') and crew_data.memory is not None:
                crew_memory = crew_data.memory
                logger.info(f"Using listener crew memory setting from configuration: {crew_memory}")
            else:
                # Determine memory setting based on agent configuration
                # This follows the same pattern as CrewPreparation in regular crew execution
                agents_with_memory_enabled = []
                agents_with_memory_disabled = []
                for agent in agents:
                    agent_role = agent.role if hasattr(agent, 'role') else 'Unknown'
                    # Check if agent has memory attribute set
                    if hasattr(agent, 'memory'):
                        if agent.memory is False:
                            agents_with_memory_disabled.append(agent_role)
                        else:
                            agents_with_memory_enabled.append(agent_role)
                    else:
                        # Default to memory enabled if not specified
                        agents_with_memory_enabled.append(agent_role)

                # Determine crew memory setting
                if agents_with_memory_disabled and not agents_with_memory_enabled:
                    crew_memory = False
                    logger.info(f"All agents have memory disabled - setting listener crew memory to False")
                else:
                    logger.info(f"At least one agent has memory enabled - setting listener crew memory to True")

            # Determine process type from crew_data
            process_type = Process.sequential  # Default
            if crew_data and hasattr(crew_data, 'process') and crew_data.process:
                if crew_data.process.lower() == 'hierarchical':
                    process_type = Process.hierarchical
                    logger.info(f"Using hierarchical process for listener crew from configuration")
                else:
                    logger.info(f"Using sequential process for listener crew from configuration")
            else:
                logger.info(f"Using default sequential process for listener crew")

            # Determine verbose setting from crew_data
            crew_verbose = True  # Default
            if crew_data and hasattr(crew_data, 'verbose') and crew_data.verbose is not None:
                crew_verbose = crew_data.verbose

            # Create crew with configuration from crew_data
            crew_kwargs = {
                'name': listener_crew_name,
                'agents': agents,
                'tasks': runtime_tasks,
                'verbose': crew_verbose,
                'process': process_type,
                'memory': crew_memory,
            }

            # Add planning configuration if enabled
            if crew_data and hasattr(crew_data, 'planning') and crew_data.planning:
                crew_kwargs['planning'] = True
                logger.info(f"Planning enabled for listener crew from configuration")

            # Add reasoning configuration if enabled
            if crew_data and hasattr(crew_data, 'reasoning') and crew_data.reasoning:
                crew_kwargs['reasoning'] = True
                logger.info(f"Reasoning enabled for listener crew from configuration")

            # Log crew configuration for debugging
            logger.info(f"Listener crew configuration: memory={crew_memory}, process={process_type}, planning={crew_kwargs.get('planning', False)}, reasoning={crew_kwargs.get('reasoning', False)}")

            crew = Crew(**crew_kwargs)
            logger.info(f"Crew instance '{listener_crew_name}' created for listener, kwargs: {list(crew_kwargs.keys())}")

            # Set up execution callbacks
            job_id = None
            if callbacks:
                job_id = callbacks.get('job_id')
                if job_id:
                    logger.info(f"Extracted job_id from callbacks for listener: {job_id}")

            # Create and set synchronous step and task callbacks
            if job_id:
                try:
                    step_callback, task_callback = create_execution_callbacks(
                        job_id=job_id,
                        config={},
                        group_context=group_context,
                        crew=crew
                    )
                    crew.step_callback = step_callback
                    crew.task_callback = task_callback
                    logger.info(f"✅ Set synchronous execution callbacks on listener crew for job {job_id}")
                except Exception as callback_error:
                    logger.warning(f"Failed to set execution callbacks on listener: {callback_error}")
            else:
                logger.warning("No job_id available for listener, skipping execution callbacks setup")

            try:
                # Enhanced logging for truncation diagnosis
                import time
                start_time = time.time()

                # Log LLM configuration details for first agent
                first_agent = agents[0] if agents else None
                if first_agent and hasattr(first_agent, 'llm') and first_agent.llm:
                    llm = first_agent.llm
                    llm_info = {
                        'model': getattr(llm, 'model', 'unknown'),
                        'max_tokens': getattr(llm, 'max_tokens', 'not set'),
                        'timeout': getattr(llm, 'timeout', 'not set'),
                    }
                    logger.info(f"📊 Listener LLM Configuration: {llm_info}")

                logger.info(f"📝 Listener tasks: {len(runtime_tasks)}")
                logger.info("⏱️ Calling listener crew.kickoff_async() with 10 minute timeout...")

                result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)

                elapsed_time = time.time() - start_time
                logger.info(f"⏱️ Listener crew execution took {elapsed_time:.2f} seconds")

                # Log result details for truncation diagnosis
                if result:
                    if hasattr(result, 'raw') and result.raw:
                        result_length = len(str(result.raw))
                        logger.info(f"✅ Listener kickoff completed - result.raw length: {result_length} chars")
                        raw_str = str(result.raw)
                        if result_length > 400:
                            logger.info(f"📄 Listener result preview - First 200 chars: {raw_str[:200]}")
                            logger.info(f"📄 Listener result preview - Last 200 chars: {raw_str[-200:]}")
                        else:
                            logger.info(f"📄 Listener full result: {raw_str}")
                    else:
                        logger.info(f"✅ Listener kickoff completed - result type: {type(result)}, str length: {len(str(result))}")
                else:
                    logger.warning("⚠️ Listener kickoff returned None or empty result")

                # Return serializable value for @persist compatibility
                # CrewOutput objects are not JSON-serializable, so extract raw content
                serializable_result = None
                if hasattr(result, 'raw') and result.raw:
                    serializable_result = result.raw
                elif result is not None:
                    serializable_result = str(result)
                else:
                    serializable_result = result

                # Store result in state for checkpoint resume support
                # This allows skipped crews to retrieve the output when resuming
                if serializable_result is not None:
                    if hasattr(self, 'state'):
                        self.state[method_name] = serializable_result
                        self.state[crew_name] = serializable_result
                        logger.info(f"📦 Stored listener output in state['{method_name}'] and state['{crew_name}'] for checkpoint support")

                return serializable_result
            except asyncio.TimeoutError:
                elapsed_time = time.time() - start_time if 'start_time' in dir() else 0
                logger.error(f"❌ Listener crew execution timed out after {elapsed_time:.2f} seconds (limit: 600s)")
                raise TimeoutError("Listener crew execution timed out")
            except Exception as e:
                elapsed_time = time.time() - start_time if 'start_time' in dir() else 0
                logger.error(f"❌ Error during listener crew kickoff after {elapsed_time:.2f} seconds: {e}", exc_info=True)
                raise

        # Set metadata
        listener_method.__name__ = method_name
        listener_method.__qualname__ = method_name

        return listener_method

    @staticmethod
    def create_skipped_crew_method(
        method_name: str,
        crew_name: str,
        crew_sequence: int,
        is_starting_point: bool = True,
        method_condition: Any = None,
        condition_type: str = "NONE",
        checkpoint_output: Any = None
    ) -> Callable:
        """
        Create a stub method for a crew that should be skipped during checkpoint resume.

        When resuming from a checkpoint, crews that have already completed (sequence <= resume_from)
        are replaced with stub methods that return the checkpoint output from the database,
        allowing the flow to continue with proper context for downstream crews.

        Args:
            method_name: Name of the flow method
            crew_name: Name of the crew being skipped
            crew_sequence: The sequence number of this crew
            is_starting_point: True if this is a starting point method, False for listener
            method_condition: For listeners, the method(s) to listen to
            condition_type: For listeners, the condition type (AND, OR, NONE)
            checkpoint_output: The actual output from the previous execution (from database traces)

        Returns:
            A decorated async method that returns the checkpoint output
        """
        logger.info(f"Creating SKIP method '{method_name}' for crew '{crew_name}' (sequence: {crew_sequence})")
        if checkpoint_output is not None:
            logger.info(f"  📦 Checkpoint output provided: {str(checkpoint_output)[:200]}...")
        
        def get_cached_output(flow_instance, method_nm, crew_nm, prev_output=None):
            """
            Retrieve cached output from persistence layer.

            Checks multiple sources for cached crew output:
            1. _method_outputs (set by @persist decorator)
            2. state dictionary with method_name key
            3. state dictionary with crew_name key
            4. state dictionary with 'crew_{sequence}_output' key
            5. Falls back to previous_output if provided
            """
            cached_output = None

            # DIAGNOSTIC: Log what's available in the flow instance
            logger.info(f"  🔍 DIAGNOSTIC - Looking for cached output for '{method_nm}' / '{crew_nm}'")
            if hasattr(flow_instance, '_method_outputs'):
                logger.info(f"  🔍 DIAGNOSTIC - _method_outputs exists: {bool(flow_instance._method_outputs)}")
                if flow_instance._method_outputs:
                    logger.info(f"  🔍 DIAGNOSTIC - _method_outputs keys: {list(flow_instance._method_outputs.keys()) if isinstance(flow_instance._method_outputs, dict) else 'not a dict'}")
            else:
                logger.info(f"  🔍 DIAGNOSTIC - _method_outputs does not exist")

            if hasattr(flow_instance, 'state'):
                state = flow_instance.state
                logger.info(f"  🔍 DIAGNOSTIC - state exists, type: {type(state)}")
                if hasattr(state, 'keys'):
                    logger.info(f"  🔍 DIAGNOSTIC - state keys: {list(state.keys())}")
                elif hasattr(state, '__dict__'):
                    logger.info(f"  🔍 DIAGNOSTIC - state attrs: {list(vars(state).keys())}")
            else:
                logger.info(f"  🔍 DIAGNOSTIC - state does not exist")

            # Try to get from _method_outputs (CrewAI @persist stores outputs here)
            if hasattr(flow_instance, '_method_outputs') and flow_instance._method_outputs:
                if isinstance(flow_instance._method_outputs, dict) and method_nm in flow_instance._method_outputs:
                    cached_output = flow_instance._method_outputs[method_nm]
                    logger.info(f"  📦 Found cached output in _method_outputs['{method_nm}']")
                    return cached_output

            # Try to get from state with various key patterns
            if hasattr(flow_instance, 'state'):
                state = flow_instance.state

                # Check for method_name as key
                if hasattr(state, 'get'):
                    # Dict-like state
                    if method_nm in state:
                        cached_output = state.get(method_nm)
                        logger.info(f"  📦 Found cached output in state['{method_nm}']")
                        return cached_output

                    # Check for crew_name as key
                    if crew_nm in state:
                        cached_output = state.get(crew_nm)
                        logger.info(f"  📦 Found cached output in state['{crew_nm}']")
                        return cached_output

                    # Check for crew_{sequence}_output pattern
                    seq_key = f"crew_{crew_sequence}_output"
                    if seq_key in state:
                        cached_output = state.get(seq_key)
                        logger.info(f"  📦 Found cached output in state['{seq_key}']")
                        return cached_output

                    # Check for {method_name}_output pattern
                    output_key = f"{method_nm}_output"
                    if output_key in state:
                        cached_output = state.get(output_key)
                        logger.info(f"  📦 Found cached output in state['{output_key}']")
                        return cached_output

                    # Check previous_output key
                    if 'previous_output' in state:
                        cached_output = state.get('previous_output')
                        logger.info(f"  📦 Found cached output in state['previous_output']")
                        return cached_output
                else:
                    # Object-like state
                    if hasattr(state, method_nm):
                        cached_output = getattr(state, method_nm)
                        logger.info(f"  📦 Found cached output in state.{method_nm}")
                        return cached_output
                    if hasattr(state, crew_nm):
                        cached_output = getattr(state, crew_nm)
                        logger.info(f"  📦 Found cached output in state.{crew_nm}")
                        return cached_output

            # Fall back to previous_output if provided (pass-through for listeners)
            if prev_output is not None:
                logger.info(f"  📦 Using previous_output as fallback (pass-through mode)")
                return prev_output

            logger.warning(f"  ⚠️ No cached output found for '{method_nm}' / '{crew_nm}'")
            return None
        
        if is_starting_point:
            # Create a starting point stub method that returns checkpoint output
            @start()
            async def skipped_starting_method(self):
                logger.info("="*80)
                logger.info(f"⏭️  CHECKPOINT RESUME: Skipping crew '{crew_name}' (sequence: {crew_sequence})")
                logger.info(f"Method: {method_name}")
                logger.info(f"This crew was already completed in a previous execution")

                # Primary source: checkpoint_output from database traces (passed from flow builder)
                result_output = checkpoint_output

                if result_output is not None:
                    logger.info(f"  ✅ Using checkpoint output from database: {str(result_output)[:200]}...")
                else:
                    # Fallback: try to get from persistence layer (in case @persist loaded state)
                    result_output = get_cached_output(self, method_name, crew_name)

                    if result_output is not None:
                        logger.info(f"  ✅ Using cached output from persistence: {str(result_output)[:200]}...")
                    else:
                        logger.warning(f"  ⚠️ No checkpoint output found, returning placeholder")
                        # Create a placeholder output to allow flow to continue
                        result_output = {
                            "status": "skipped",
                            "crew_name": crew_name,
                            "message": f"Crew '{crew_name}' was skipped during checkpoint resume"
                        }

                # Store in state for downstream propagation
                if hasattr(self, 'state'):
                    self.state[method_name] = result_output
                    self.state[crew_name] = result_output
                    logger.info(f"  📦 Stored output in state['{method_name}'] and state['{crew_name}']")

                logger.info("="*80)
                return result_output

            skipped_starting_method.__name__ = method_name
            skipped_starting_method.__qualname__ = method_name
            return skipped_starting_method
        else:
            # Create a listener stub method that returns checkpoint output
            @listen(method_condition)
            async def skipped_listener_method(self, previous_output=None):
                logger.info("="*80)
                logger.info(f"⏭️  CHECKPOINT RESUME: Skipping listener crew '{crew_name}' (sequence: {crew_sequence})")
                logger.info(f"Method: {method_name}")
                logger.info(f"Listening to: {method_condition}")
                logger.info(f"Previous output received: {str(previous_output)[:200] if previous_output else 'None'}...")
                logger.info(f"This crew was already completed in a previous execution")

                # Primary source: checkpoint_output from database traces (passed from flow builder)
                result_output = checkpoint_output

                if result_output is not None:
                    logger.info(f"  ✅ Using checkpoint output from database: {str(result_output)[:200]}...")
                else:
                    # Fallback: try to get from persistence layer, with previous_output as last resort
                    result_output = get_cached_output(self, method_name, crew_name, previous_output)

                    if result_output is not None:
                        logger.info(f"  ✅ Using cached/fallback output: {str(result_output)[:200]}...")
                    else:
                        logger.warning(f"  ⚠️ No checkpoint output found and no previous_output, returning placeholder")
                        # Create a placeholder output to allow flow to continue
                        result_output = {
                            "status": "skipped",
                            "crew_name": crew_name,
                            "message": f"Crew '{crew_name}' was skipped during checkpoint resume"
                        }

                # Store in state to propagate to downstream crews
                if hasattr(self, 'state'):
                    self.state[method_name] = result_output
                    self.state[crew_name] = result_output
                    self.state['previous_output'] = result_output
                    logger.info(f"  📦 Stored output in state['{method_name}'] and state['{crew_name}']")

                logger.info("="*80)
                return result_output

            skipped_listener_method.__name__ = method_name
            skipped_listener_method.__qualname__ = method_name
            return skipped_listener_method
