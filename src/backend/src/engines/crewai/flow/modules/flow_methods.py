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
    def create_starting_point_method(
        method_name: str,
        task_obj: Any,
        callbacks: Optional[Dict[str, Any]],
        group_context: Optional[Any],
        create_execution_callbacks: Callable
    ) -> Callable:
        """
        Create a starting point method for the flow.

        Args:
            method_name: Name of the method
            task_obj: Task object to execute
            callbacks: Callbacks dict with job_id
            group_context: Group context for multi-tenant isolation
            create_execution_callbacks: Function to create execution callbacks

        Returns:
            Async function decorated with @start()
        """
        @start()
        async def starting_point_method(self):
            """Starting point method - executes first in the flow."""
            logger.info("="*80)
            logger.info(f"START METHOD CALLED - Task: {task_obj.description}")
            logger.info("="*80)

            # Get the agent for this task
            agent = task_obj.agent
            logger.info(f"Agent role: {agent.role}")

            # Log if agent has no tools
            if not hasattr(agent, 'tools') or not agent.tools:
                logger.info(f"Agent {agent.role} has no tools assigned but will continue with execution")

            # Create a single-task crew
            logger.info("Creating Crew instance for start method")
            logger.info(f"Agent details - role: {agent.role}, has_llm: {hasattr(agent, 'llm') and agent.llm is not None}")
            logger.info(f"Task details - description length: {len(task_obj.description)}, has_agent: {task_obj.agent is not None}")

            # Set crew name from agent role for better tracing
            start_crew_name = agent.role if hasattr(agent, 'role') and agent.role else "Start Crew"
            logger.info(f"Creating crew with name: {start_crew_name}")

            crew = Crew(
                name=start_crew_name,
                agents=[agent],
                tasks=[task_obj],
                verbose=True,
                process=Process.sequential
            )
            logger.info(f"Crew instance '{start_crew_name}' created successfully")

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
                logger.info("Calling crew.kickoff_async() with 10 minute timeout...")
                result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)
                logger.info(f"✅ kickoff_async completed successfully, result type: {type(result)}")
                return result
            except asyncio.TimeoutError:
                logger.error("❌ Crew execution timed out after 10 minutes")
                raise TimeoutError("Crew execution timed out after 10 minutes")
            except Exception as e:
                logger.error(f"❌ Error during crew kickoff: {e}", exc_info=True)
                raise

        # Set metadata
        starting_point_method.__name__ = method_name
        starting_point_method.__qualname__ = method_name

        return starting_point_method

    @staticmethod
    def create_starting_point_crew_method(
        method_name: str,
        task_list: List[Any],
        crew_name: str,
        callbacks: Optional[Dict[str, Any]],
        group_context: Optional[Any],
        create_execution_callbacks: Callable
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
            logger.info(f"Creating Crew instance: {crew_name}")

            crew = Crew(
                name=crew_name,
                agents=agents,
                tasks=task_list,  # Pass ALL tasks - CrewAI will respect task.context for sequential execution
                verbose=True,
                process=Process.sequential
            )
            logger.info(f"Crew instance '{crew_name}' created successfully with {len(task_list)} tasks")

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
                logger.info("Calling crew.kickoff_async() with 10 minute timeout...")
                result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)
                logger.info(f"✅ kickoff_async completed successfully for crew '{crew_name}', result type: {type(result)}")
                return result
            except asyncio.TimeoutError:
                logger.error(f"❌ Crew '{crew_name}' execution timed out after 10 minutes")
                raise TimeoutError(f"Crew '{crew_name}' execution timed out after 10 minutes")
            except Exception as e:
                logger.error(f"❌ Error during crew '{crew_name}' kickoff: {e}", exc_info=True)
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
        create_execution_callbacks: Callable
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
                previous_output_str = str(results[0])
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

            # Set crew name based on first agent role
            listener_crew_name = agents[0].role if agents and hasattr(agents[0], 'role') and agents[0].role else "Listener Crew"
            logger.info(f"Creating listener crew with name: {listener_crew_name}")

            crew = Crew(
                name=listener_crew_name,
                agents=agents,
                tasks=runtime_tasks,
                verbose=True,
                process=Process.sequential
            )
            logger.info(f"Crew instance '{listener_crew_name}' created for listener")

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
                logger.info("Calling listener crew.kickoff_async() with 10 minute timeout...")
                result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)
                logger.info(f"✅ Listener kickoff_async completed, result type: {type(result)}")
                return result
            except asyncio.TimeoutError:
                logger.error("❌ Listener crew execution timed out after 10 minutes")
                raise TimeoutError("Listener crew execution timed out")
            except Exception as e:
                logger.error(f"❌ Error during listener crew kickoff: {e}", exc_info=True)
                raise

        # Set metadata
        listener_method.__name__ = method_name
        listener_method.__qualname__ = method_name

        return listener_method

    @staticmethod
    def create_router_method(
        router_method_name: str,
        listen_to_method: str,
        routes: List[Dict[str, Any]]
    ) -> Callable:
        """
        Create a router method for the flow.

        Args:
            router_method_name: Name of the router method
            listen_to_method: Method name to listen to
            routes: List of route configs with name and condition

        Returns:
            Async function decorated with @router() and @listen()
        """
        @router(listen_to_method)
        async def router_method(self):
            """Router method - evaluates conditions and returns route name."""
            logger.info("="*80)
            logger.info(f"ROUTER METHOD CALLED - {router_method_name}")
            logger.info(f"Listening to: {listen_to_method}")
            logger.info(f"Evaluating {len(routes)} route(s)")
            logger.info("="*80)

            try:
                # Evaluate each route condition in order
                for route in routes:
                    route_name = route['name']
                    condition = route.get('condition')

                    logger.info(f"  Checking route '{route_name}'")
                    logger.info(f"    Condition: {condition if condition else 'None (default)'}")

                    # Evaluate condition
                    if FlowStateManager.evaluate_condition(self.state, condition):
                        logger.info(f"  ✓ Route '{route_name}' condition matched")
                        logger.info(f"Router returned route: {route_name}")
                        return route_name
                    else:
                        logger.info(f"  ✗ Route '{route_name}' condition not matched")

                # No route matched
                logger.warning(f"Router {router_method_name}: No route conditions matched")
                logger.info("Router returned route: None")
                return None

            except Exception as e:
                logger.error(f"Error in router {router_method_name}: {e}", exc_info=True)
                return None

        # Set metadata
        router_method.__name__ = router_method_name
        router_method.__qualname__ = router_method_name

        return router_method

    @staticmethod
    def create_route_listener_method(
        route_listener_method_name: str,
        expected_route: str,
        route_tasks: List[Any],
        callbacks: Optional[Dict[str, Any]],
        group_context: Optional[Any],
        create_execution_callbacks: Callable
    ) -> Callable:
        """
        Create a route listener method for a specific route.

        Args:
            route_listener_method_name: Name of the route listener method
            expected_route: The route name this listener expects
            route_tasks: List of task objects for this route
            callbacks: Callbacks dict with job_id
            group_context: Group context for multi-tenant isolation
            create_execution_callbacks: Function to create execution callbacks

        Returns:
            Async function decorated with @listen(route_name)
        """
        @listen(expected_route)
        async def route_listener_method(self, previous_output):
            """Route listener method - executes when router returns this route name."""
            logger.info("="*80)
            logger.info(f"ROUTE LISTENER METHOD CALLED - {route_listener_method_name}")
            logger.info(f"Executing route listener for route: {expected_route}")

            # Log and store previous output from router
            if previous_output:
                logger.info(f"📥 RECEIVED PREVIOUS OUTPUT FROM ROUTER:")
                logger.info(f"  Output: {str(previous_output)[:200]}...")
                self.state['previous_output'] = previous_output
            else:
                logger.info("📭 No previous output received from router")

            logger.info("="*80)

            # Create runtime tasks with previous output injected into descriptions
            # This follows the official CrewAI Flow pattern of creating tasks at runtime
            runtime_tasks = []
            previous_output_context = ""

            if previous_output:
                # Get the first agent to determine context limits
                first_agent = route_tasks[0].agent if route_tasks else None

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
                previous_output_str = str(previous_output)
                if len(previous_output_str) > max_context_length:
                    previous_output_context = f"\n\nContext from previous step:\n{previous_output_str[:max_context_length]}...\n(Output truncated for brevity)"
                else:
                    previous_output_context = f"\n\nContext from previous step:\n{previous_output_str}"
                logger.info(f"📤 Injecting previous output context into task descriptions ({len(previous_output_context)} chars)")

            # Create new Task objects with modified descriptions
            for task in route_tasks:
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
            logger.info(f"Number of agents in route listener: {len(agents)}")

            # Log if agents have no tools
            for agent in agents:
                if not hasattr(agent, 'tools') or not agent.tools:
                    logger.info(f"Agent {agent.role} has no tools assigned but will continue with execution")

            logger.info("Creating Crew instance for route listener method")
            logger.info(f"Route listener has {len(agents)} agents and {len(runtime_tasks)} tasks")

            # Set crew name based on first agent role
            route_crew_name = agents[0].role if agents and hasattr(agents[0], 'role') and agents[0].role else "Route Listener Crew"
            logger.info(f"Creating route listener crew with name: {route_crew_name}")

            crew = Crew(
                name=route_crew_name,
                agents=agents,
                tasks=runtime_tasks,
                verbose=True,
                process=Process.sequential
            )
            logger.info(f"Crew instance '{route_crew_name}' created for route listener")

            # Set up execution callbacks
            job_id = None
            if callbacks:
                job_id = callbacks.get('job_id')
                if job_id:
                    logger.info(f"Extracted job_id from callbacks for route listener: {job_id}")

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
                    logger.info(f"✅ Set synchronous execution callbacks on route listener crew for job {job_id}")
                except Exception as callback_error:
                    logger.warning(f"Failed to set execution callbacks on route listener: {callback_error}")
            else:
                logger.warning("No job_id available for route listener, skipping execution callbacks setup")

            try:
                logger.info("Calling route listener crew.kickoff_async() with 10 minute timeout...")
                result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)
                logger.info(f"✅ Route listener kickoff_async completed, result type: {type(result)}")
                return result
            except asyncio.TimeoutError:
                logger.error("❌ Route listener crew execution timed out after 10 minutes")
                raise TimeoutError("Route listener crew execution timed out")
            except Exception as e:
                logger.error(f"❌ Error during route listener crew kickoff: {e}", exc_info=True)
                raise

        # Set metadata
        route_listener_method.__name__ = route_listener_method_name
        route_listener_method.__qualname__ = route_listener_method_name

        return route_listener_method
