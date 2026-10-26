"""Chat ``/delete crew <name>`` and ``/delete flow <name>``.

Deleting is an editor or admin action, the same rule as the REST routes
(``DELETE /crews/{id}``, ``DELETE /flows/{id}``). The target is resolved by its
exact name (case-insensitive) or its id, never by a fragment of the name, and
nothing is deleted until the user repeats the command with ``--confirm``. The
first reply names that exact command with the item's id, so two items that share
a name can still be told apart.
"""

from typing import Dict, List, Literal, Optional, Protocol, Sequence, Tuple
from uuid import UUID

from src.core.permissions import check_role_in_context
from src.services.catalog.crews import CrewService
from src.services.flow_builder.flow_service import FlowService
from src.utils.user_context import GroupContext

CONFIRM_FLAG = "--confirm"
DELETE_ROLES = ["admin", "editor"]
# The intent ``source`` slash_commands.detect_slash_command sets.
SLASH_COMMAND_SOURCE = "slash_command"
_MAX_SUGGESTIONS = 5

Kind = Literal["crew", "flow"]

# What a delete takes with it, said before the user confirms.
_CONSEQUENCE: Dict[Kind, str] = {
    "crew": " and withdraws its publications",
    "flow": " and its run history",
}


class _Deletable(Protocol):
    """A saved crew or flow, as far as deleting it is concerned."""

    @property
    def id(self) -> UUID: ...

    @property
    def name(self) -> Optional[str]: ...


def can_delete(group_context: Optional[GroupContext]) -> bool:
    """Whether the caller may delete crews and flows in this workspace."""
    return group_context is not None and check_role_in_context(
        group_context, DELETE_ROLES
    )


def split_confirm(args: str) -> Tuple[str, bool]:
    """``("<target>", confirmed)`` from the text after ``/delete crew|flow``."""
    text = args.strip()
    if text.lower() == CONFIRM_FLAG:
        return "", True
    head, sep, tail = text.rpartition(" ")
    if sep and tail.lower() == CONFIRM_FLAG:
        return head.strip(), True
    return text, False


def exact_matches(items: Sequence[_Deletable], target: str) -> List[_Deletable]:
    """Items whose id is ``target``, else whose whole name is ``target``."""
    by_id = [item for item in items if str(item.id) == target]
    if by_id:
        return by_id
    wanted = target.casefold()
    return [item for item in items if (item.name or "").strip().casefold() == wanted]


def _command(kind: Kind, item: _Deletable) -> str:
    return f"/delete {kind} {item.id} {CONFIRM_FLAG}"


def _no_match(kind: Kind, target: str, items: Sequence[_Deletable]) -> str:
    fragment = target.casefold()
    similar = [
        item.name for item in items if item.name and fragment in item.name.casefold()
    ][:_MAX_SUGGESTIONS]
    message = f"No {kind} is named '{target}'."
    if similar:
        names = ", ".join(f"'{name}'" for name in similar)
        message += f" Did you mean {names}? Type the full name to delete it."
    return message


def _several(kind: Kind, target: str, matches: Sequence[_Deletable]) -> str:
    lines = "\n".join(f"- `{_command(kind, item)}`" for item in matches)
    return (
        f"Several {kind}s are named '{target}', so nothing was deleted. "
        f"Delete one by its id:\n{lines}"
    )


async def _load(
    kind: Kind,
    group_context: Optional[GroupContext],
    crew_service: CrewService,
    flow_service: FlowService,
) -> Sequence[_Deletable]:
    if kind == "crew":
        return await crew_service.find_by_group(group_context)
    return await flow_service.get_all_flows_for_group(group_context)


async def delete_from_chat(
    kind: Kind,
    args: str,
    group_context: Optional[GroupContext],
    crew_service: CrewService,
    flow_service: FlowService,
    source: Optional[str] = None,
) -> Dict[str, str]:
    """Handle ``/delete <kind> <args>`` and return the chat reply.

    ``source`` is where the intent came from. Only a slash command the user
    typed deletes anything: a delete intent from the language-model classifier
    is refused, whatever arguments it carries.
    """
    result_type = "catalog_delete" if kind == "crew" else "flow_delete"
    if source != SLASH_COMMAND_SOURCE:
        return {
            "type": result_type,
            "message": f"To delete a {kind}, type `/delete {kind} <name>`.",
        }
    if not can_delete(group_context):
        return {
            "type": result_type,
            "message": f"Only editors and admins can delete {kind}s.",
        }

    target, confirmed = split_confirm(args)
    if not target:
        return {
            "type": result_type,
            "message": f"Please specify a {kind} name to delete. "
            f"Usage: `/delete {kind} <name>`",
        }

    items = await _load(kind, group_context, crew_service, flow_service)
    matches = exact_matches(items, target)
    if not matches:
        return {"type": result_type, "message": _no_match(kind, target, items)}
    if len(matches) > 1:
        return {"type": result_type, "message": _several(kind, target, matches)}

    item = matches[0]
    if not confirmed:
        command = _command(kind, item)
        return {
            "type": result_type,
            "message": f"This permanently deletes {kind} '{item.name}'"
            f"{_CONSEQUENCE[kind]}. To confirm, send `{command}`.",
            "confirm_command": command,
        }

    if kind == "crew":
        await crew_service.delete_by_group(item.id, group_context)
    else:
        await flow_service.force_delete_flow_with_executions_with_group_check(
            item.id, group_context
        )
    return {
        "type": result_type,
        "message": f"{kind.capitalize()} '{item.name}' has been deleted.",
    }
