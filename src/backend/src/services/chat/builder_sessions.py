"""Builder persistence uses the same session identity and DB route as Chat."""

import asyncio
import json

from sqlalchemy.exc import IntegrityError

from src.core.exceptions import ConflictError, ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context
from src.repositories.chat_history_repository import ChatHistoryRepository
from src.repositories.chat_session_repository import ChatSessionRepository
from src.schemas.builder_session import BuilderCanvasResponse
from src.utils.encryption_utils import EncryptionUtils


class BuilderSessionService:
    def __init__(self, session):
        self.repo = ChatSessionRepository(session)
        self.history = ChatHistoryRepository(session)

    @staticmethod
    def _identity(context):
        if not context.primary_group_id or not context.group_email:
            raise ForbiddenError("A teamspace and user are required")
        if not check_role_in_context(context, ["admin", "editor"]):
            raise ForbiddenError("Only editors and admins can access builder canvases")
        return context.primary_group_id, context.group_email

    async def get(self, session_id, context):
        group_id, user_id = self._identity(context)
        row = await self.repo.get_by_id_and_group(session_id, [group_id])
        if not row or row.user_id != user_id:
            raise NotFoundError("Session not found")
        state = None
        if row.canvas_state:
            # Encrypt the complete snapshot, including any tool credentials.
            raw = await asyncio.to_thread(
                EncryptionUtils.decrypt_value, row.canvas_state
            )
            state = json.loads(raw)
        return BuilderCanvasResponse(state=state, revision=row.canvas_revision)

    async def save(self, session_id, request, context):
        group_id, user_id = self._identity(context)
        row = await self.repo.get_by_id_and_group(session_id, [group_id])
        if row and row.user_id != user_id:
            raise NotFoundError("Session not found")
        if not row:
            if request.revision or await self.history.session_has_other_owner(
                session_id, user_id, group_id
            ):
                raise ConflictError("Session cannot be imported")
            try:
                row = await self.repo.create(
                    {
                        "id": session_id,
                        "title": request.title,
                        "user_id": user_id,
                        "group_id": group_id,
                        "group_email": user_id,
                        "mode": request.mode,
                    }
                )
            except IntegrityError as exc:
                raise ConflictError(
                    "Session already exists; reload before saving"
                ) from exc
        state = {
            **request.state,
            "group_id": group_id,
            "chatSessionId": session_id,
            "name": request.title,
            "viewMode": request.mode,
        }
        encrypted = await asyncio.to_thread(
            EncryptionUtils.encrypt_value, json.dumps(state)
        )
        saved = await self.repo.save_canvas(
            session_id,
            group_id,
            user_id,
            request.revision,
            title=request.title,
            mode=request.mode,
            canvas_state=encrypted,
        )
        if not saved:
            raise ConflictError(
                "This session changed in another browser. Reload it before saving."
            )
        return BuilderCanvasResponse(revision=request.revision + 1)
