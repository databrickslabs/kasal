"""Transactional creation of a teamspace from another team's configuration."""

from src.core.exceptions import BadRequestError, NotFoundError
from src.models.user import User
from src.repositories.group_duplication_repository import GroupDuplicationRepository
from src.schemas.group import GroupDuplicateRequest, GroupResponse
from src.services.groups.groups import GroupService
from src.utils.user_context import clear_membership_cache


class GroupDuplicationService:
    def __init__(self, session):
        self.session = session
        self.groups = GroupService(session)
        self.repository = GroupDuplicationRepository(session)

    async def duplicate(
        self, source_id: str, request: GroupDuplicateRequest, actor: User
    ) -> GroupResponse:
        source = await self.groups.get_group_by_id(source_id)
        if source is None:
            raise NotFoundError("Source teamspace not found")
        if await self.groups.user_repo.get_by_personal_group_id(source_id):
            raise BadRequestError(
                "Choose a teamspace to duplicate, rather than a Personal Space"
            )

        try:
            target = await self.groups.create_group(
                name=request.name,
                description=request.description,
                created_by_email=actor.email,
            )
            await self.repository.copy_configuration(source_id, target.id, actor.email)
            await self.repository.copy_members(
                source_id, target.id, actor.id, actor.email, request.include_members
            )
            response = GroupResponse(
                id=target.id,
                name=target.name,
                description=target.description,
                status=target.status,
                auto_created=False,
                created_by_email=actor.email,
                created_at=target.created_at,
                updated_at=target.updated_at,
                user_count=await self.groups.get_group_user_count(target.id),
            )
            # Complete the whole copy before reporting success. A failed copy
            # leaves no empty or partially configured teamspace behind.
            await self.session.commit()
        except Exception:
            await self.session.rollback()
            raise
        clear_membership_cache()
        return response
