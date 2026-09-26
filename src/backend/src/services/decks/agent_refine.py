"""Slide edits use Chat's light-agent executor and capability configuration."""

from typing import Any

from src.schemas.crew import CrewStreamingRequest
from src.schemas.deck import SlideRefineRequest
from src.schemas.execution import CrewConfig
from src.services.decks.slide_refine import _system_prompt, _user_message
from src.services.execution.service import ExecutionService
from src.services.generation.crews import CrewGenerationService
from src.services.tools.tool_service import ToolService
from src.utils.user_context import GroupContext


async def start_slide_refinement(
    body: SlideRefineRequest, session: Any, group_context: GroupContext
) -> dict[str, Any]:
    if body.mode == "refine" and not (body.slide or "").strip():
        raise ValueError("a refine needs the slide to revise")

    # Match Chat's workspace tool boundary. Never trust client tool IDs/names
    # to enable tools that the current workspace has disabled.
    enabled = await ToolService(session).get_enabled_tools_for_group(group_context)
    selected = set(body.tools) if body.tools is not None else None
    tools = [
        tool.title
        for tool in enabled.tools
        if (selected is None and tool.title != "DatabricksKnowledgeSearchTool")
        or (
            selected is not None
            and (tool.title in selected or str(tool.id) in selected)
        )
    ]
    prompt = _user_message(
        body.mode,
        body.instruction,
        body.slide,
        body.reference,
        body.before,
        body.after,
        body.position,
    )
    request = CrewStreamingRequest(
        prompt=prompt,
        original_prompt=prompt,
        model=body.model,
        tools=tools,
        mcp_servers=body.mcp_servers,
        agentbricks_endpoints=body.agentbricks_endpoints,
        skills=body.skills,
        chat_mode_type="chat",
        disable_memory=True,
    )
    instructions = await _system_prompt(group_context)
    instructions += (
        "\nUse the available tools, MCP servers or delegated agents when needed "
        "to fulfill the instruction. When online research or verification is "
        "requested, obtain evidence using those capabilities before revising the "
        "slide; never claim to have searched without doing so. Include concise "
        "source links in the slide for researched claims. If the required access "
        "is unavailable or research fails, explain the limitation instead of "
        "fabricating research or returning a supposedly verified slide. "
        "Edit only the supplied slide; never create a whole deck."
    )
    config = CrewConfig(
        **CrewGenerationService.build_crew_config_from_generated(
            request,
            [
                {
                    "id": "slide-editor",
                    "role": "Slide editor",
                    "goal": "Fulfill the requested edit using relevant evidence and return one slide.",
                    "backstory": instructions,
                    "tools": tools,
                }
            ],
            [
                {
                    "id": "slide-edit",
                    "agent_id": "slide-editor",
                    "tools": tools,
                    "description": "Revise the selected slide according to the user's instruction.",
                    "expected_output": 'Exactly one complete <section class="slide">...</section> in an HTML fence.',
                }
            ],
        )
    )
    config.output_contract = "slide"
    # Only the selected slide and its neighbours ground this focused edit.
    # Prior chat history must not ask this run to recreate the whole deck.
    launched = await ExecutionService(session=None).create_execution(
        config=config,
        background_tasks=None,
        group_context=group_context,
    )
    return {"job_id": launched["execution_id"]}
