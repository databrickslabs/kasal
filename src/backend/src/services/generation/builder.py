"""A traceable builder conversation turn, independent of executing its output."""

from src.services.execution import generation_run
from src.services.mlflow.builder_trace import builder_mlflow_trace
from src.services.mlflow.mlflow_parent_setup import set_root_span_outputs
from src.services.otel_tracing.generation_scope import generation_trace


class BuilderGenerationService:
    def __init__(self, session):
        self.session = session

    async def open(self, mode, request, group_context):
        prompt = request.prompt if mode == "flow" else request.message
        job_id = await generation_run.open_run(
            self.session,
            run_name=f"{'Flow' if mode == 'flow' else 'Crew'} design — {prompt[:100]}",
            inputs={
                "builder_mode": mode,
                "prompt": prompt,
                "session_id": request.session_id,
            },
            trigger_type="builder_generation",
            group_context=group_context,
        )
        if not job_id:
            raise RuntimeError("Could not start the generation run")
        return job_id

    async def generate(self, mode, request, group_context, job_id):
        async with (
            builder_mlflow_trace(
                self.session, mode, request, group_context, job_id
            ) as span,
            generation_trace(
                job_id,
                group_context,
                "Flow Builder" if mode == "flow" else "Agent Builder",
                (
                    "Choose crews and design connections"
                    if mode == "flow"
                    else "Create the crew plan"
                ),
            ),
        ):
            if mode == "flow":
                from src.services.flow_builder.generation import FlowGenerationService

                result = await FlowGenerationService(self.session).generate(
                    request, group_context
                )
                output = result.model_dump(mode="json")
                set_root_span_outputs(span, output)
                return output
            from src.services.chat.dispatcher import DispatcherService
            from src.services.tools.tool_service import ToolService

            enabled = await ToolService(self.session).get_enabled_tools_for_group(
                group_context
            )
            available = [
                {"title": tool.title, "description": tool.description}
                for tool in enabled.tools
            ]
            output = await DispatcherService.create(self.session).dispatch(
                request, group_context, available_tools=available
            )
            set_root_span_outputs(span, output)
            return output
