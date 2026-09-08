"""Execute a selected router branch with the same task policies as other crews."""

from typing import Any, Sequence

from src.core.logger import LoggerManager
from src.services.execution.harnesses import active_harness
from src.services.execution.kernel.execution_callback import create_execution_callbacks
from src.services.flow_builder.modules.flow_conditions import state_snapshot
from src.services.flow_builder.modules.flow_methods import (
    _emit_checkpoint_restored,
    collect_task_agents,
    crew_inputs_from_state,
    extract_final_answer,
    get_model_context_limits,
)
from src.services.flow_builder.modules.task_context import (
    task_with_context,
    tasks_for_review,
)
from src.services.flow_builder.runtime import listen

logger = LoggerManager.get_instance().flow


def route_listener_factory(
    route_task_list: list[Any],
    route_listener_method_name: str,
    callbacks_param: dict[str, Any] | None,
    group_ctx: Any,
    expected_route: str,
    route_crew_name_param: str | None,
    upstream_method: str,
    approval_gates: Sequence[Any] = (),
    replay_output: Any = None,
) -> Any:
    route_task_list = tasks_for_review(
        route_task_list, route_listener_method_name, callbacks_param
    )

    @listen(expected_route)
    async def route_listener_method(self: Any, previous_output: Any) -> Any:
        logger.info("=" * 80)
        logger.info(f"ROUTE LISTENER METHOD CALLED - {route_listener_method_name}")
        logger.info(f"Executing route listener for route: {expected_route}")

        # A @listen(route_name) method is handed the ROUTER'S
        # RETURN VALUE, which is the route name — not the
        # upstream crew's output. Injecting that verbatim gave
        # the routed crew "Context from previous step:
        # route_to_politics_presentation" and none of the
        # classification it was supposed to work from: the
        # branch ran, on nothing.
        #
        # The real output is in state under the method the
        # router listens to, stored as state[<method>] and
        # state[<crew name>] when that crew finished.
        routed_from = state_snapshot(self.state).get(upstream_method)
        if routed_from:
            logger.info(
                "Route %s: taking upstream output from "
                "state[%r] (%d chars) instead of the router's "
                "return value %r",
                expected_route,
                upstream_method,
                len(str(routed_from)),
                str(previous_output)[:60],
            )
            previous_output = routed_from
        else:
            logger.warning(
                "Route %s: state has no %r, so the routed crew "
                "gets only the router's return value. It will "
                "run without the upstream crew's output.",
                expected_route,
                upstream_method,
            )

        # On approval resume, already completed branches must not execute again.
        if replay_output is not None:
            if route_crew_name_param:
                _emit_checkpoint_restored(route_crew_name_param, replay_output)
            self.state[route_listener_method_name] = replay_output
            if route_crew_name_param:
                self.state[route_crew_name_param] = replay_output
            return replay_output

        # Gate only the selected branch, after resolving the actual source output.
        # Calling the gate body preserves its database-backed pause/resume behavior.
        for gate in approval_gates:
            previous_output = await gate._meth(self, previous_output)

        # Log and store previous output from router
        if previous_output:
            logger.info("📥 RECEIVED PREVIOUS OUTPUT FROM ROUTER:")
            logger.info(f"  Output: {str(previous_output)[:200]}...")
            # Serialize before storing — @persist JSON-serializes the
            # whole state and a raw CrewOutput is not serializable.
            self.state["previous_output"] = (
                previous_output.raw
                if hasattr(previous_output, "raw") and previous_output.raw
                else (
                    str(previous_output)
                    if previous_output is not None
                    else previous_output
                )
            )
        else:
            logger.info("📭 No previous output received from router")

        logger.info("=" * 80)

        # Get agents for these tasks
        agents = collect_task_agents(route_task_list)
        logger.info(f"Number of agents in route listener: {len(agents)}")

        # CRITICAL FIX: Inject previous output context into task descriptions
        # This ensures the agent has access to the data from the previous crew
        # Same pattern as create_listener_method in flow_methods.py
        runtime_tasks = []
        previous_output_context = ""

        if previous_output:
            # Get the first agent to determine context limits
            first_agent = route_task_list[0].agent if route_task_list else None

            # Get model's context window and max output tokens using ModelConfigService
            context_window_tokens, max_output_tokens = (
                await get_model_context_limits(first_agent, group_ctx)
                if first_agent
                else (128000, 16000)
            )

            # Calculate available input budget (subtract output reservation)
            available_input_tokens = context_window_tokens - max_output_tokens

            # Allocate 60% of available input for previous output
            # This leaves 40% for system prompts, tools, conversation history, and safety buffer
            max_context_tokens = int(available_input_tokens * 0.6)

            # Convert tokens to characters (using 3.5 chars/token for safety)
            max_context_length = int(max_context_tokens * 3.5)

            logger.info(
                f"Model limits: context={context_window_tokens} tokens, max_output={max_output_tokens} tokens"
            )
            logger.info(
                f"Available input: {available_input_tokens} tokens, allocating {max_context_tokens} tokens ({max_context_length} chars) for previous output"
            )

            # Create a concise context string to inject into task descriptions
            # Use extract_final_answer to get only the final answer, not the full thinking process
            previous_output_str = extract_final_answer([previous_output])
            if len(previous_output_str) > max_context_length:
                previous_output_context = f"\n\nContext from previous step:\n{previous_output_str[:max_context_length]}...\n(Output truncated for brevity)"
            else:
                previous_output_context = (
                    f"\n\nContext from previous step:\n{previous_output_str}"
                )
            logger.info(
                f"📤 Injecting previous output context into task descriptions ({len(previous_output_context)} chars)"
            )

        # Create new Task objects with modified descriptions
        for task in route_task_list:
            # Create new task with injected context
            runtime_task = task_with_context(task, previous_output_context)
            runtime_tasks.append(runtime_task)
            logger.info(
                f"Created runtime task with injected context for agent: {task.agent.role}"
            )

        # Use runtime_tasks (with context) instead of original route_task_list
        tasks_to_use = runtime_tasks

        # CrewAI validation: A crew cannot end with more than one async task
        # If we have multiple async tasks, auto-create a completion task
        async_tasks = [t for t in tasks_to_use if getattr(t, "async_execution", False)]

        if len(async_tasks) > 1:
            # Auto-create a lightweight completion task that waits for all async tasks
            completion_agent = async_tasks[-1].agent
            completion_task = active_harness().build_task(
                description="Aggregate and return results from parallel task executions",
                expected_output="Combined results from all parallel tasks",
                agent=completion_agent,
                context=async_tasks,
                async_execution=False,
            )
            tasks_to_use.append(completion_task)
            logger.info(
                f"Auto-created completion task for route listener to handle {len(async_tasks)} async tasks"
            )

        # Create crew with runtime tasks (with injected context)
        logger.info("Creating Crew instance for route listener")

        # Use provided crew name, fallback to first agent role
        route_crew_name = (
            route_crew_name_param
            if route_crew_name_param
            else (
                agents[0].role
                if agents and hasattr(agents[0], "role") and agents[0].role
                else "Route Crew"
            )
        )
        logger.info(f"Creating route crew with name: {route_crew_name}")

        engine = active_harness()
        crew = engine.build_crew(
            name=route_crew_name,  # Set crew name for proper event tracing
            agents=agents,
            tasks=tasks_to_use,  # Use validated task list
            verbose=True,
            process=engine.process("sequential"),
        )
        logger.info(f"Crew instance '{route_crew_name}' created for route")

        # SECURITY: Same assembly-time checks as all other crew creation paths.
        try:
            from src.services.security.tool_capability_manifest import (
                run_crew_security_checks as _run_security_checks,
            )

            _run_security_checks(
                crew,
                context=f"flow router crew '{route_crew_name}'",
            )
        except Exception as _sec_err:
            logger.debug(
                "[SECURITY] Flow router crew security checks skipped: %s",
                _sec_err,
            )

        # CRITICAL: Set up execution callbacks like regular crew execution
        # Extract job_id directly from callbacks dict
        job_id = None
        if callbacks_param:
            # Get job_id directly from callbacks dict (no longer using JobOutputCallback)
            job_id = callbacks_param.get("job_id")
            if job_id:
                logger.info(
                    f"Extracted job_id from callbacks for route listener: {job_id}"
                )

        # Create and set synchronous step and task callbacks
        if job_id:
            try:
                step_callback, task_callback = create_execution_callbacks(
                    job_id=job_id,
                    config={},
                    group_context=group_ctx,
                    crew=crew,
                )
                crew.step_callback = step_callback
                crew.task_callback = task_callback
                logger.info(
                    f"✅ Set synchronous execution callbacks on route listener crew for job {job_id}"
                )
            except Exception as callback_error:
                logger.warning(
                    f"Failed to set execution callbacks on route listener: {callback_error}"
                )
        else:
            logger.warning(
                "No job_id available for route listener, skipping execution callbacks setup"
            )

        # The third crew call site, and the one that was
        # still passing nothing. A routed crew whose task
        # reads "{topic}" got the literal braces while the
        # start and listener crews got the value, so the same
        # placeholder resolved or not depending on which
        # branch reached it.
        crew_inputs = crew_inputs_from_state(self)
        if crew_inputs:
            logger.info(
                f"Passing {sorted(crew_inputs)} from flow state "
                f"into route listener crew '{route_crew_name_param}'"
            )
        result = await crew.kickoff_async(inputs=crew_inputs)
        logger.info(
            f"Route listener kickoff_async completed, result type: {type(result)}"
        )
        # The next router needs this crew's actual output, and
        # its chosen branch reads it back by upstream method.
        output = (
            result.raw
            if getattr(result, "raw", None)
            else (str(result) if result is not None else None)
        )
        self.state[route_listener_method_name] = output
        if route_crew_name_param:
            self.state[route_crew_name_param] = output
        return output

    route_listener_method.__name__ = route_listener_method_name
    route_listener_method.__qualname__ = route_listener_method_name
    if hasattr(route_listener_method, "_meth"):
        route_listener_method._meth.__name__ = route_listener_method_name
        route_listener_method._meth.__qualname__ = route_listener_method_name
    return route_listener_method
