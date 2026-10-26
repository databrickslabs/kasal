"""Taking a deleted crew or flow off every external surface.

A publication outlives the thing it names unless something removes it, and
nothing did. One workspace ended up advertising nine MCP tools for crews that
had been deleted and eight capabilities for flows that were gone; each could
only ever answer "no longer exists". Worse, a dangling row still HOLDS its
external name — ``(external_name, group_id)`` is unique — so a deleted flow
could permanently block a new crew from publishing under the same name, which
surfaced as an ``IntegrityError`` and a 404 that said the crew was not
published.

The catalogue drops dangling rows on read, and resolution refuses them; this is
the other half, so they stop being created at all.

Why functions and not another service class: the callers are the crew and flow
services, mid-deletion, and what they need is one statement — not a second
service object with its own repository and lifecycle. Every one of these is
best-effort at the call site: the entity is already gone by then, and failing a
deletion that happened over a registry row would be the worse trade.
"""

import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


async def _delete(
    session: Any,
    entity_type: str,
    entity_ids: Optional[List[str]] = None,
    group_ids: Optional[List[str]] = None,
) -> int:
    from src.repositories.crew_publication_repository import PublicationRepository

    removed = await PublicationRepository(session).delete_publications(
        entity_type=entity_type, entity_ids=entity_ids, group_ids=group_ids
    )
    if removed:
        logger.info(
            "[publications] withdrew %d %s publication(s)", removed, entity_type
        )
        # Same announcement a publish makes. A client holding a tool for a crew
        # that has just been deleted is as wrong as one missing a new capability,
        # and it is the case that produces a confident call to something that
        # cannot run.
        from src.services.publications import signals

        await signals.catalogue_changed(group_ids)
    return removed


async def withdraw_entity(
    session: Any,
    entity_type: str,
    entity_id: Any,
    group_ids: Optional[List[str]] = None,
) -> int:
    """Unpublish one crew or flow that is being deleted."""
    return await _delete(session, entity_type, [str(entity_id)], group_ids)


async def withdraw_entities(
    session: Any,
    entity_type: str,
    entity_ids: List[Any],
    group_ids: Optional[List[str]] = None,
) -> int:
    """Unpublish several — the bulk deletes, which know their ids."""
    return await _delete(session, entity_type, [str(i) for i in entity_ids], group_ids)


async def withdraw_all(
    session: Any, entity_type: str, group_ids: Optional[List[str]] = None
) -> int:
    """Unpublish every crew or flow of this kind.

    For the ``delete_all`` paths. ``group_ids`` scopes it to a workspace; None
    is the unscoped variant, and belongs only to the unscoped delete it
    accompanies.
    """
    return await _delete(session, entity_type, None, group_ids)
