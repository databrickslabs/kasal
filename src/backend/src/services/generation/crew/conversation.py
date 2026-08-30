"""Crew synthesis from a chat transcript (``POST /crew/from-conversation``)."""

import logging
from typing import Any, Dict, List, Optional

from src.core.exceptions import BadRequestError, KasalError
from src.schemas.crew import (
    CrewGenerationRequest,
    CrewGenerationResponse,
    CrewStreamingRequest,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class ConversationGenerationMixin:
    """Crew synthesis from a chat transcript (``POST /crew/from-conversation``)."""

    async def synthesize_crew_from_conversation(
        self,
        session_id: str,
        group_context: Optional[GroupContext] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Distill a reusable crew from a WHOLE chat conversation.

        ChatMode answer/"chat" turns run a GENERIC single assistant (see
        ``_run_chat_fast_path``), so bookmarking that to the catalog saves nothing
        specific. This reads the USER's own requests for ``session_id`` — and
        ONLY those: the assistant's replies never leave the workspace — and asks
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
        transcript = await self._build_conversation_transcript(
            session_id, group_context
        )
        if not transcript:
            raise BadRequestError(
                "No conversation found for this session to build a crew from"
            )

        prompt = (
            "Below are the USER's requests from a chat session, in order. The "
            "assistant's replies are deliberately NOT included — only what the "
            "user asked for. There may be SEVERAL distinct requests in sequence "
            "(for example: first gathering information, then building a dashboard "
            "from it).\n\n"
            "Design a reusable crew that fulfils this entire workflow on its own, "
            "WITHOUT the back-and-forth. Cover EVERY distinct step the user asked "
            "for, in order: create a separate task for each step (and an agent "
            "suited to it), and chain them so each later task builds on the output of "
            "the earlier ones (use task context/dependencies). Do NOT collapse "
            "multiple requests into a single generic task — if the user asked for N "
            "things, the crew should have tasks covering all N.\n\n"
            "Base each agent's role/goal/backstory and each task's description/"
            "expected_output on what the USER actually asked for — be specific to "
            "the domain and deliverables in these requests, NOT a generic 'helpful "
            "assistant'. Each task description must state its objective clearly "
            "enough to run standalone.\n\n"
            f"User requests:\n{transcript}"
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
    ) -> str:
        """The session's USER turns — the user's own prompts, nothing else.

        The assistant's replies are deliberately NOT included. They are mostly
        deliverable bytes (a 20KB report or slide deck) that balloon the LLM
        payload without describing the workflow, they can carry third-party
        content the user never wrote, and the user's request alone is what a
        distilled task must reproduce. Every user turn is kept (each capped at
        {cap} chars) so a multi-step session distills into a multi-task crew;
        the fetch itself is bounded (last 200 messages).

        Group-scoped (tenant isolation) and best-effort: returns ``""`` when
        there is no session, no group, or no usable content. Placeholder rows
        ("Thinking...", "[ui-card]") are skipped.
        """
        group_ids = list(getattr(group_context, "group_ids", None) or [])
        primary = getattr(group_context, "primary_group_id", None)
        if not group_ids and primary:
            group_ids = [primary]
        if not session_id or not group_ids:
            return ""

        # Chat history is ChatHistoryService's domain.
        from src.services.chat.history import ChatHistoryService

        messages = await ChatHistoryService(self.session).get_recent_messages(
            session_id, group_ids, limit=200
        )

        placeholders = {"thinking...", "[ui-card]", ""}
        user_cap = 800
        lines: List[str] = []
        for m in messages:
            if getattr(m, "message_type", "") != "user":
                continue
            content = (getattr(m, "content", "") or "").strip()
            if content.lower() in placeholders or content.startswith("[ui-card]"):
                continue
            if len(content) > user_cap:
                content = content[:user_cap] + "…"
            lines.append(f"User: {content}")
        return "\n".join(lines)
