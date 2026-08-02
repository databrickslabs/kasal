"""The publication registry: which crews are reachable, by whom, over what.

One record per crew, listing its protocols — see ``models/crew_publication.py``
for why that is one record and not a flag per protocol.

The service exists so every surface reads capabilities through the SAME call.
``list_capabilities_for_group`` is what an MCP ``list_crews`` returns, what an
A2A Agent Card's ``skills[]`` is built from, and what the ChatMode "Use existing"
router picks over; because it is one function they cannot advertise different
capabilities, which is the invariant the whole design rests on.

The registry is protocol-NEUTRAL, which is why it no longer lives under
``external/``: ``chat`` is an internal protocol reaching the same rows through
the same group filter. See this package's ``__init__`` for why the move mattered.

Two shapes of read, and the difference matters:

* ``*_for_group`` take primitives and are the core. Internal callers — who
  already hold a trusted ``GroupContext`` — use these.
* ``list_capabilities`` / ``resolve_capability`` take an ``ExternalCaller`` and
  delegate. The adapters keep the API they have.

**Do not build an ``ExternalCaller`` for internal traffic.** ``identity.py`` opens
with "An MCP client or an A2A agent is, by definition, outside the workspace" —
its job is turning untrusted headers into a tenant. Wrapping a ``GroupContext``
in one drags in the external role double-gating and stamps external-origin
attribution on internal traffic, polluting the external audit trail.
"""

import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import ConflictError
from src.models.crew_publication import Publication
from src.repositories.crew_publication_repository import PublicationRepository
from src.schemas.crew_publication import (
    CrewPublicationCreate,
    CrewPublicationUpdate,
    PublishedCapability,
)
from src.utils.user_context import GroupContext

if TYPE_CHECKING:  # pragma: no cover
    # Type-only: the two adapter-facing delegations below are typed for it, but
    # the registry must not depend on the external trust boundary at runtime.
    from src.services.external.identity import ExternalCaller

logger = logging.getLogger(__name__)


def _teamspace_suffix(group_id: Any) -> str:
    """A group id, as it can appear inside a tool name.

    MCP tool names and A2A skill ids are addressed by external clients, so they
    are restricted to characters those clients and their agents handle without
    quoting: ``bi-specialist`` becomes ``bi_specialist``.
    """
    cleaned = "".join(
        char if char.isalnum() else "_" for char in str(group_id or "")
    ).strip("_")
    return cleaned.lower() or "other"


def _publication_order(row: Any) -> tuple:
    """Stable ordering for deciding which publication keeps an unqualified name.

    Oldest first, id as the tiebreak. Deterministic on purpose: the name a
    client pins must not depend on row order coming back from the database, or a
    tool would change identity between two reads that saw the same data.
    """
    created = getattr(row, "created_at", None)
    return (created is None, created or datetime.min, getattr(row, "id", 0) or 0)


def display_names(rows: List[Any]) -> Dict[int, str]:
    """``row id -> the name this publication is addressed by``.

    A caller identified only by email sees every teamspace they belong to, and
    the name uniqueness constraint is per TEAMSPACE — so two teamspaces can each
    publish ``quiz`` and the merged list has two tools with one name. A client
    cannot tell them apart, and resolution would pick whichever row the database
    returned first.

    So the first publication (oldest) keeps the plain name and the rest carry
    their teamspace: ``quiz`` and ``quiz__bi_specialist``. Both stay callable,
    which is the point — dropping the loser would make one teamspace's
    capability silently unreachable.

    ONE function, used by the listing and by resolution, because the two
    disagreeing is precisely how a caller ends up running the other teamspace's
    crew.
    """
    by_name: Dict[str, List[Any]] = {}
    for row in rows:
        by_name.setdefault(str(row.external_name), []).append(row)

    names: Dict[int, str] = {}
    for name, group in by_name.items():
        if len(group) == 1:
            names[group[0].id] = name
            continue
        for index, row in enumerate(sorted(group, key=_publication_order)):
            names[row.id] = (
                name if index == 0 else f"{name}__{_teamspace_suffix(row.group_id)}"
            )
    return names


def _checkable(value: Any) -> bool:
    """Whether this entity id can be looked up at all.

    Both repositories resolve ids as UUIDs, so an id that is not one can never
    be FOUND — and treating "not found" as "does not exist" would silently drop
    a capability on a question that was never answerable. Anything non-UUID is
    kept and left to fail loudly at invocation, which is what it did before.
    """
    try:
        uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return False
    return True


def _normalize_id(value: Any) -> str:
    """A UUID in the one form both tables can be compared in.

    ``flows.id`` is a UUID column, which SQLite stores as 32 hex characters with
    no dashes, while ``publications.entity_id`` is a string column holding the
    dashed form. Comparing them raw silently never matches — which is how a
    conversational flow would have looked one-shot forever, with nothing
    logged and nothing failing.
    """
    return str(value).replace("-", "").lower().strip()


class PublicationService:
    """CRUD plus the one group-scoped read both adapters depend on."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = PublicationRepository(session)

    async def list_capabilities_for_group(
        self, group_ids: List[str], protocol: str
    ) -> List[PublishedCapability]:
        """Capabilities visible to ``group_ids`` on one protocol.

        The core read. Protocol-neutral on purpose: the MCP tool list, the A2A
        ``skills[]`` and the ChatMode route catalog are three renderings of this
        one list.

        An empty ``group_ids`` returns ``[]``, not everything — see the
        repository, where that guarantee lives.
        """
        rows = await self._live_rows(group_ids, protocol)
        _, conversational = await self._entity_facts(rows)
        names = display_names(rows)

        capabilities: List[PublishedCapability] = []
        for row in rows:
            key = _normalize_id(row.entity_id)
            capabilities.append(
                PublishedCapability(
                    entity_type=row.entity_type,
                    entity_id=row.entity_id,
                    name=names.get(row.id, row.external_name),
                    teamspace=row.group_id,
                    description=row.description,
                    input_schema=row.input_schema,
                    conversational=key in conversational,
                )
            )
        return capabilities

    async def _entity_exists(self, row: Any) -> bool:
        """Whether the crew or flow behind one publication is still there.

        Reuses the catalogue's own lookup so "does it exist" is answered one
        way. True whenever the question cannot be answered — a failed read or a
        non-UUID id — because refusing on a lookup that did not work would take
        a live capability offline.
        """
        existing, _ = await self._entity_facts([row])
        live = existing.get(row.entity_type)
        if live is None or not _checkable(row.entity_id):
            return True
        return _normalize_id(row.entity_id) in live

    async def _live_rows(self, group_ids: List[str], protocol: str) -> List[Any]:
        """Publications for these groups, minus the ones whose entity is gone.

        The single row set both the catalogue and resolution work from. They
        used to filter separately, and a name dropped from the list stayed
        resolvable — an MCP client's cached tool then ran a crew that no longer
        existed and failed deep in the engine instead of answering "unknown".
        """
        rows = await self.repository.list_published_for_group(
            group_ids=group_ids, protocol=protocol
        )
        existing, _ = await self._entity_facts(rows)

        live: List[Any] = []
        for row in rows:
            known = existing.get(row.entity_type)
            if (
                known is not None
                and _checkable(row.entity_id)
                and _normalize_id(row.entity_id) not in known
            ):
                # The crew or flow behind this publication is gone — deleted, or
                # lost in a restore. Advertising it hands every surface a name
                # that cannot run: the chat router SPENDS a turn picking it and
                # the user is told nothing matches, and an MCP client is offered
                # a tool whose only possible answer is "no longer exists".
                # Dropped from the catalogue, not unpublished: the publication
                # row stays visible in the publish dialog so it can be fixed or
                # removed deliberately.
                logger.warning(
                    "[publications] %r points at a %s that no longer exists "
                    "(%s); excluded from the catalogue",
                    row.external_name,
                    row.entity_type,
                    row.entity_id,
                )
                continue
            live.append(row)
        return live

    async def _entity_facts(
        self, rows: List[Any]
    ) -> Tuple[Dict[str, Optional[Set[str]]], Set[str]]:
        """``({entity_type: the ones that exist}, flows that hold a conversation)``.

        Both kinds are checked, and that symmetry is the point. Only flows were
        looked up here, so deleting a CREW left its publication advertising a
        tool whose only possible answer is "no longer exists" — one workspace
        was offering nine of them over MCP. Nothing removes a publication when
        its entity is deleted, so the catalogue is the place that has to know.

        One query per kind, not one per row: this read renders the MCP tool
        list and the A2A card as well as the chat catalogue, and a
        per-capability lookup would make every one of those N+1.

        A value is None when that lookup FAILED, which is different from "none
        of them exist" — a failed read must not empty the catalogue, so the
        caller skips the filter for that kind and keeps every capability. An
        entity type with no entry (a future kind) is likewise not filtered.
        """
        existing: Dict[str, Optional[Set[str]]] = {}

        crew_ids = [str(row.entity_id) for row in rows if row.entity_type == "crew"]
        if crew_ids:
            try:
                from src.repositories.crew_repository import CrewRepository

                crews = await CrewRepository(self.session).find_by_ids(crew_ids)
                existing["crew"] = {_normalize_id(crew.id) for crew in crews}
            except Exception as exc:  # noqa: BLE001 — a catalogue must render
                logger.debug("[publications] could not read crews: %s", exc)
                existing["crew"] = None
        else:
            existing["crew"] = set()

        conversational: Set[str] = set()
        flow_ids = [str(row.entity_id) for row in rows if row.entity_type == "flow"]
        if not flow_ids:
            existing["flow"] = set()
            return existing, conversational
        try:
            from src.repositories.flow_repository import FlowRepository

            flows = await FlowRepository(self.session).find_by_ids(flow_ids)
        except Exception as exc:  # noqa: BLE001 — a catalogue must still render
            logger.debug("[publications] could not read flows: %s", exc)
            existing["flow"] = None
            return existing, conversational

        live: Set[str] = set()
        for flow in flows:
            key = _normalize_id(flow.id)
            live.add(key)
            config = getattr(flow, "flow_config", None) or {}
            state = config.get("state") if isinstance(config, dict) else None
            if isinstance(state, dict) and state.get("conversational"):
                conversational.add(key)
        existing["flow"] = live
        return existing, conversational

    async def resolve_capability_for_group(
        self, group_ids: List[str], protocol: str, external_name: str
    ) -> Optional[Publication]:
        """The publication behind a name, or None. The single authorisation choke point.

        Returns None both when the name does not exist and when it exists in
        another tenant — a caller must not be able to tell those apart, or the
        surface becomes an oracle for other workspaces' capability names.

        Also returns None when the capability is not published to ``protocol``:
        being on the A2A card must not make it invocable over MCP, and being
        chat-routable must not make it either.

        And None when the crew or flow behind it no longer exists. That has to
        match what the catalogue shows or the two disagree, and the disagreement
        is reachable: an MCP client caches the tool list from when it connected,
        so a name dropped from the catalogue is still callable from that cache.
        Resolving it produced a run that failed deep in the engine with
        "Published crew <uuid> no longer exists"; refusing here makes it the same
        plain "unknown" every other unavailable name gets.

        The name resolved is the name the caller was SHOWN, which for a caller in
        several teamspaces may carry the teamspace (``quiz__bi_specialist``).
        Resolution walks the same ``display_names`` mapping the listing does, so
        a qualified name lands on exactly the publication that was advertised
        under it — never on the other teamspace's crew of the same name.

        Every surface resolves through here. Reaching past it to
        ``find_by_external_name``, or resolving a name through the catalogue
        instead, creates a second visibility semantic where an unpublished crew
        quietly becomes invocable.
        """
        rows = await self._live_rows(group_ids, protocol)
        names = display_names(rows)
        by_display = {names.get(row.id, row.external_name): row for row in rows}

        row = by_display.get(external_name)
        if row is None:
            # Either genuinely unknown, or published to another protocol. The
            # distinction is worth a log line and nothing more: telling the
            # caller which it was makes the surface an oracle for names it may
            # not see.
            logger.info(
                "[publication] %r is not a capability this caller may run over %s",
                external_name,
                protocol,
            )
            return None
        return row

    async def list_capabilities(
        self, caller: "ExternalCaller", protocol: Optional[str] = None
    ) -> List[PublishedCapability]:
        """``list_capabilities_for_group`` for an external caller.

        Defaults ``protocol`` to the caller's own, so an adapter cannot
        accidentally list capabilities that are not published to its surface.
        """
        return await self.list_capabilities_for_group(
            caller.group_ids,
            protocol if protocol is not None else caller.protocol,
        )

    async def resolve_capability(
        self, caller: "ExternalCaller", external_name: str
    ) -> Optional[Publication]:
        """``resolve_capability_for_group`` for an external caller."""
        return await self.resolve_capability_for_group(
            caller.group_ids, caller.protocol, external_name
        )

    async def _claim_name(
        self,
        external_name: str,
        entity_type: str,
        entity_id: str,
        group_context: GroupContext,
    ) -> None:
        """Make ``external_name`` available to this entity, or refuse.

        ``(external_name, group_id)`` is unique, so publishing under a name
        another entity holds hits the constraint. That surfaced as a raw
        ``IntegrityError`` in the log and a 404 "Crew is not published" in the
        UI — an error about a collision reported as if the publish had simply
        not happened, with nothing saying which name was taken.

        A DANGLING holder is taken over rather than refused. A publication whose
        crew or flow was deleted is not a live claim on anything: it is invisible
        in the catalogue and refuses to resolve, so leaving it squatting the name
        would make a deleted flow permanently block a crew from using its name,
        with no way to find out why.

        A live holder is a real conflict and raises, because the alternative is
        silently retargeting a name external callers already use.
        """
        holder = await self.repository.find_by_external_name(
            external_name=external_name, group_ids=group_context.group_ids or []
        )
        if holder is None:
            return
        if holder.entity_type == entity_type and str(holder.entity_id) == str(
            entity_id
        ):
            return

        if await self._entity_exists(holder):
            raise ConflictError(
                f"The name {external_name!r} is already published to another "
                f"{holder.entity_type} in this workspace. Choose a different name, "
                "or unpublish that one first."
            )

        logger.warning(
            "[publications] %r was held by a %s that no longer exists (%s); "
            "reclaiming the name",
            external_name,
            holder.entity_type,
            holder.entity_id,
        )
        await self.repository.delete_by_entity(
            entity_type=holder.entity_type,
            entity_id=holder.entity_id,
            group_ids=group_context.group_ids or [],
        )
        await self.session.flush()

    async def publish(
        self,
        entity_id: str,
        data: CrewPublicationCreate,
        group_context: GroupContext,
        entity_type: str = "crew",
    ) -> Publication:
        """Publish a crew or flow, or update its publication if it already has one.

        Idempotent by entity: publishing twice adjusts the existing record
        rather than creating a second one, which the unique constraint would
        reject anyway.
        """
        group_id = group_context.primary_group_id
        if not group_id:
            raise ValueError("Cannot publish without a group context.")

        existing = await self.repository.find_by_entity(
            entity_type=entity_type,
            entity_id=entity_id,
            group_ids=group_context.group_ids or [],
        )
        if existing is not None:
            await self._claim_name(
                data.external_name, entity_type, entity_id, group_context
            )
            existing.external_name = data.external_name
            existing.description = data.description
            existing.protocols = list(data.protocols)
            existing.input_schema = data.input_schema
            await self.session.flush()
            await self._announce(group_context)
            return existing

        await self._claim_name(
            data.external_name, entity_type, entity_id, group_context
        )

        row = Publication(
            entity_type=entity_type,
            entity_id=entity_id,
            external_name=data.external_name,
            description=data.description,
            protocols=list(data.protocols),
            input_schema=data.input_schema,
            group_id=group_id,
            created_by_email=group_context.group_email,
        )
        self.session.add(row)
        await self.session.flush()
        logger.info(
            "[external] published %s %s as %s over %s (group %s)",
            entity_type,
            entity_id,
            data.external_name,
            data.protocols,
            group_id,
        )
        await self._announce(group_context)
        return row

    async def _announce(self, group_context: GroupContext) -> None:
        """Tell the surfaces the published set moved.

        An MCP client's tool list is a snapshot from when it connected, so
        without this a capability published now is invisible to it until it
        reconnects — which is the whole reason a generic "list them at runtime"
        tool used to be necessary.
        """
        from src.services.publications import signals

        await signals.catalogue_changed(list(group_context.group_ids or []) or None)

    async def update(
        self,
        entity_id: str,
        data: CrewPublicationUpdate,
        group_context: GroupContext,
        entity_type: str = "crew",
    ) -> Optional[Publication]:
        """Adjust an existing publication. Omitted fields are left alone."""
        row = await self.repository.find_by_entity(
            entity_type=entity_type,
            entity_id=entity_id,
            group_ids=group_context.group_ids or [],
        )
        if row is None:
            return None

        if data.external_name is not None:
            # A rename can collide exactly as a publish can, and hits the same
            # constraint.
            await self._claim_name(
                data.external_name, entity_type, entity_id, group_context
            )
            row.external_name = data.external_name
        if data.description is not None:
            row.description = data.description
        if data.protocols is not None:
            row.protocols = list(data.protocols)
        if data.input_schema is not None:
            row.input_schema = data.input_schema

        await self.session.flush()
        await self._announce(group_context)
        return row

    async def unpublish(
        self,
        entity_id: str,
        group_context: GroupContext,
        entity_type: str = "crew",
    ) -> bool:
        """Withdraw a crew or flow from every external surface."""
        removed = await self.repository.delete_by_entity(
            entity_type=entity_type,
            entity_id=entity_id,
            group_ids=group_context.group_ids or [],
        )
        if removed:
            logger.info("[external] unpublished %s %s", entity_type, entity_id)
            await self._announce(group_context)
        return removed > 0
