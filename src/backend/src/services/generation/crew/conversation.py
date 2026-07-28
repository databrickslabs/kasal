"""Crew synthesis from a chat transcript (``POST /crew/from-conversation``)."""

import logging
from typing import Any, Dict, List, Optional, Tuple

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
        transcript = await self._build_conversation_transcript(
            session_id, group_context
        )
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

        messages = await ChatHistoryRepository(
            self.session
        ).get_recent_by_session_and_group(session_id, group_ids, limit=200)

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
            drop_at = next(
                (i for i, (role, _) in enumerate(selected) if role == "assistant"), None
            )
            if drop_at is None:
                break  # only user turns remain — keep them even if slightly over
            selected.pop(drop_at)

        return "\n".join(line for _, line in selected)
