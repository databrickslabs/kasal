"""Who may read the credentials stored in tool configurations.

Agent, task and crew tool configs and the tool catalogue's configs carry Power
BI service-principal secrets, passwords, PATs and API keys. Members who build
workflows see them as stored: they edit them, and the canvas sends them back
with every run. Members without builder rights (operators, the default role)
only run and monitor, so their read responses carry a placeholder instead.

"Builder rights" follows the Access screen, as the frontend does: the editor
and admin roles, or an Agent Builder / Flow Builder override on the membership.
Masking happens on a response copy, never on the ORM row, which the request
session would otherwise commit.
"""

from typing import Optional, Tuple, Type, TypeVar

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.permissions import get_effective_role
from src.utils.sensitive_data_utils import mask_sensitive_strings
from src.utils.user_context import GroupContext

#: Roles that may use the builders unless the Access screen says otherwise.
BUILDER_ROLES = frozenset({"admin", "editor"})

_Response = TypeVar("_Response", bound=BaseModel)


class ToolSecretView:
    """Decides whether the caller sees stored tool credentials."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def visible_to(self, group_context: Optional[GroupContext]) -> bool:
        """True for editors, admins and members granted a builder.

        Anyone else, an unresolved role included, gets masked values. The
        membership is only read for a non-builder role, so editors and admins
        cost no query.
        """
        if group_context is None:
            return False
        role = (get_effective_role(group_context) or "").lower()
        if role in BUILDER_ROLES:
            return True
        user_id = group_context.user_id
        group_id = group_context.primary_group_id
        if not user_id or not group_id:
            return False
        from src.services.groups.groups import GroupService

        membership = await GroupService(self.session).get_user_group_membership(
            user_id, group_id
        )
        return bool(
            membership
            and (membership.allow_agent_builder or membership.allow_flow_builder)
        )


def _masked(value: object) -> object:
    """``value`` with its credential strings masked; a nested model stays one."""
    if isinstance(value, BaseModel):
        return type(value).model_validate(mask_sensitive_strings(value.model_dump()))
    return mask_sensitive_strings(value)


def masked_response(
    schema: Type[_Response], row: object, fields: Tuple[str, ...]
) -> _Response:
    """``row`` as ``schema``, with the credentials in ``fields`` masked."""
    response = schema.model_validate(row)
    return response.model_copy(
        update={field: _masked(getattr(response, field, None)) for field in fields}
    )
