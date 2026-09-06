"""The Chat dispatcher forwards run settings without deriving them from intent."""

from src.schemas.crew import CrewStreamingRequest
from src.schemas.dispatcher import DispatcherRequest


def streaming_request_for(
    request: DispatcherRequest, suggested_prompt, effective_tools
):
    return CrewStreamingRequest(
        prompt=suggested_prompt or request.message,
        # Ground the run with the user's CLEAN message when the
        # frontend sent it (message may carry a steering prefix).
        original_prompt=request.original_prompt or request.message,
        model=request.model,
        tools=effective_tools or [],
        # ChatMode generates AND runs on the backend so the run
        # survives a session switch before the plan completes. The
        # crew canvas leaves this False (default) — it renders the
        # plan and the user runs it via Play; auto-executing here
        # too would double-run the crew.
        execution_effort=request.execution_effort,
        auto_execute=request.auto_execute,
        session_id=request.session_id,
        memory_workspace_scope=request.memory_workspace_scope,
        disable_memory=request.disable_memory,
        answer_from_conversation=request.answer_from_conversation,
        mcp_servers=request.mcp_servers or [],
        agentbricks_endpoints=request.agentbricks_endpoints or [],
        # Files attached in this chat turn — scopes the knowledge
        # search tool so the run grounds on the just-uploaded doc.
        knowledge_file_paths=request.knowledge_file_paths or [],
        # Images attached in this turn — the agents learn how to
        # place them in the HTML they write.
        image_assets=request.image_assets or [],
        # Skills picked in the chat "+" menu — attached to every
        # agent of the run by the shared kernel builder.
        skills=request.skills or [],
        # ChatMode answer mode (chat|research|deep) → drives
        # reasoning/execution_type at config-build time.
        chat_mode_type=request.chat_mode_type or "chat",
    )
