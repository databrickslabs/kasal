"""Managing the remote A2A agents Kasal can call.

Two audiences, and the split between them is the point:

- A **Kasal admin** registers a remote agent globally. That row carries a URL
  Kasal will POST to and a credential to POST with, so registering one is a
  system-administration act — the same reason MCP servers are registered
  globally rather than per workspace.
- A **workspace admin** turns a globally-available agent on or off for their
  own workspace. They cannot add one, cannot change its URL, and cannot read
  its key.

Availability is the Kasal admin's decision; use is the workspace's. Neither
substitutes for the other, so a globally-available agent still does nothing
until a workspace opts in, and a Kasal admin turning one off cascades
immediately regardless of what workspaces had enabled.

The protocol lives beside this, in ``client.py``. This service owns policy —
who may configure, group scoping, credential encryption — and never builds a
request. It sits in the ``a2a`` package rather than as a loose
``a2a_agent_service.py`` for the same reason ``mcp/service.py`` does: everything
one protocol needs is findable in one place.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.a2a_agent_repository import A2AAgentRepository
from src.schemas.a2a_agent import (
    AUTH_TYPES,
    A2AAgentCreate,
    A2AAgentUpdate,
    A2AConnectionTest,
)
from src.services.a2a.a2a_client import client as a2a_client
from src.utils.encryption_utils import EncryptionUtils
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

#: Fields a workspace override copies from its base. Everything an outbound call
#: needs, and nothing that identifies the base row — an override is a private
#: copy so a workspace's on/off state cannot touch anyone else's.
_CLONED_FIELDS = (
    "name",
    "card_url",
    "description",
    "auth_type",
    "encrypted_api_key",
    "timeout_seconds",
    "cached_card",
    "card_fetched_at",
)


class A2AAgentService:
    def __init__(self, session: AsyncSession):
        # No `self.session`: reads and writes both go through the repository.
        self.repository = A2AAgentRepository(session)

    # ---------------------------------------------------------------- reading

    async def list_base_agents(self) -> List[Any]:
        """The Kasal admin catalogue. System-admin only, enforced at the router."""
        return await self.repository.find_all_base()

    async def list_agents(self, group_context: GroupContext) -> List[Any]:
        """What this workspace sees: globally-available agents plus its own rows."""
        return await self.repository.list_for_group_scope(
            group_context.primary_group_id
            or (group_context.group_ids[0] if group_context.group_ids else None)
        )

    async def get_agent(self, agent_id: int) -> Any:
        return await self.repository.get(agent_id)

    # -------------------------------------------------------- global registry

    async def create_agent(
        self, data: A2AAgentCreate, group_context: GroupContext
    ) -> Any:
        """Register a remote agent globally. Kasal admins only.

        ``group_id`` is NULL: this is a base row, visible to every workspace once
        ``enabled``. There is no workspace-scoped create — a remote agent's URL
        and credential are a system-administration concern.
        """
        self._validate_auth_type(data.auth_type)

        if await self.repository.find_base_by_name(data.name):
            raise ValueError(f"An agent named '{data.name}' already exists.")

        from src.models.a2a_agent import A2AAgent

        agent = A2AAgent(
            name=data.name,
            card_url=data.card_url,
            description=data.description,
            auth_type=data.auth_type,
            encrypted_api_key=(
                EncryptionUtils.encrypt_value(data.api_key) if data.api_key else None
            ),
            enabled=data.enabled,
            global_enabled=data.global_enabled,
            timeout_seconds=data.timeout_seconds,
            group_id=None,
            created_by_email=getattr(group_context, "group_email", None),
        )
        await self.repository.insert(agent)

        # Fetch the card immediately so a typo in the URL is a message on the
        # form rather than a tool that silently does nothing at run time.
        await self._refresh_card(agent, group_context)
        return agent

    async def update_agent(
        self, agent_id: int, data: A2AAgentUpdate, group_context: GroupContext
    ) -> Optional[Any]:
        """Edit a base row. Kasal admins only.

        Refuses workspace override rows: an override is a copy of a base, and
        editing one would leave a workspace silently calling a different remote
        than the catalogue says it is.
        """
        agent = await self.repository.get(agent_id)
        if not agent or agent.group_id is not None:
            return None

        if data.auth_type is not None:
            self._validate_auth_type(data.auth_type)

        for field in (
            "name",
            "card_url",
            "description",
            "auth_type",
            "enabled",
            "global_enabled",
            "timeout_seconds",
        ):
            value = getattr(data, field)
            if value is not None:
                setattr(agent, field, value)

        if data.api_key is not None:
            # An empty string CLEARS the key; None means "leave it alone". Without
            # the distinction there is no way to remove a credential once set.
            agent.encrypted_api_key = (
                EncryptionUtils.encrypt_value(data.api_key) if data.api_key else None
            )

        await self.repository.save()
        if data.card_url is not None or data.api_key is not None:
            await self._refresh_card(agent, group_context)
        return agent

    async def delete_agent(self, agent_id: int) -> bool:
        """Remove a base row and every workspace override of it.

        Leaving the overrides behind would keep the agent callable in workspaces
        that had enabled it, long after the row defining it was gone.
        """
        agent = await self.repository.get(agent_id)
        if not agent or agent.group_id is not None:
            return False
        name = agent.name
        await self.repository.remove(agent)
        await self.repository.delete_overrides_by_name(name)
        await self.repository.save()
        return True

    async def set_global_availability(self, agent_id: int, enabled: bool) -> Any:
        """Kasal admin: offer a base agent to workspaces, or withdraw it.

        Withdrawing cascades — the repository hides workspace overrides of a
        disabled base — so this is the one switch that stops an agent being
        called everywhere at once.
        """
        agent = await self.repository.get(agent_id)
        if not agent or agent.group_id is not None:
            return None
        agent.enabled = enabled
        await self.repository.save()
        return agent

    # ------------------------------------------------------ workspace opt-in

    async def set_enabled_for_group(
        self, agent_id: int, group_id: str, enabled: bool
    ) -> Optional[Any]:
        """Workspace admin: turn an agent on or off for THIS workspace.

        - Its own row → flip in place.
        - A base row → create or update this workspace's override. The base is
          NEVER mutated, so one workspace's choice cannot reach another's.
        - Another workspace's row → not found, so an id cannot be probed.
        """
        if not group_id:
            raise ValueError("No workspace selected.")

        target = await self.repository.get(agent_id)
        if not target:
            return None

        if target.group_id == group_id:
            target.enabled = enabled
            await self.repository.save()
            return target

        if target.group_id is not None:
            return None

        existing = await self.repository.find_by_name_and_group(target.name, group_id)
        if existing:
            existing.enabled = enabled
            await self.repository.save()
            return existing

        from src.models.a2a_agent import A2AAgent

        override = A2AAgent(
            **{field: getattr(target, field) for field in _CLONED_FIELDS},
            enabled=enabled,
            global_enabled=bool(target.global_enabled),
            group_id=group_id,
        )
        await self.repository.insert(override)
        return override

    # ------------------------------------------------------------- behaviour

    async def test_connection(
        self, agent_id: int, group_context: GroupContext
    ) -> Optional[A2AConnectionTest]:
        """Fetch the card now and report what happened."""
        agent = await self.repository.get(agent_id)
        if not agent:
            return None
        return await self._refresh_card(agent, group_context)

    async def resolve_for_call(
        self, name: str, group_ids: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Everything one outbound call needs, resolved in one place.

        Reads the workspace's OWN enabled row — a globally-available agent the
        workspace never opted into does not resolve. Returns a call plan rather
        than the row, so a caller cannot pass a model object, and its credential,
        somewhere it does not belong.
        """
        rows = await self.repository.list_enabled_for_group(group_ids)
        agent = next((row for row in rows if row.name == name), None)
        if not agent:
            return None

        card = agent.cached_card or {}
        return {
            "name": agent.name,
            "interface_url": a2a_client.interface_url_of(
                card, a2a_client.card_url_for(agent.card_url)
            ),
            "api_key": self._decrypt(agent),
            "auth_type": agent.auth_type,
            "timeout_seconds": agent.timeout_seconds or 300,
            "skills": a2a_client.skills_of(card),
        }

    async def _refresh_card(
        self, agent: Any, group_context: GroupContext
    ) -> A2AConnectionTest:
        """Fetch and cache the remote's card. Never raises.

        A remote being unreachable is a state to display, not an error that
        rolls back the configuration the admin just wrote — they usually need to
        save the row precisely so they can fix the URL on it.
        """
        token = getattr(group_context, "access_token", None)
        try:
            card = await a2a_client.fetch_card(
                agent.card_url,
                api_key=self._decrypt(agent),
                token=token if agent.auth_type == "obo" else None,
            )
        except Exception as exc:  # noqa: BLE001
            agent.last_error = str(exc)[:500]
            agent.card_fetched_at = datetime.utcnow()
            await self.repository.save()
            return A2AConnectionTest(connected=False, message=str(exc)[:500])

        agent.cached_card = card
        agent.card_fetched_at = datetime.utcnow()
        agent.last_error = None
        if not agent.description:
            agent.description = str(card.get("description") or "")[:1000]
        await self.repository.save()

        skills = a2a_client.skills_of(card)
        return A2AConnectionTest(
            connected=True,
            message=f"Connected. {len(skills)} skill(s) advertised.",
            agent_name=str(card.get("name") or agent.name),
            skills=skills,
        )

    @staticmethod
    def _decrypt(agent: Any) -> Optional[str]:
        if not agent.encrypted_api_key:
            return None
        try:
            return EncryptionUtils.decrypt_value(agent.encrypted_api_key) or None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not decrypt key for A2A agent %s: %s", agent.id, exc)
            return None

    @staticmethod
    def _validate_auth_type(auth_type: str) -> None:
        if auth_type not in AUTH_TYPES:
            raise ValueError(
                f"auth_type must be one of {', '.join(AUTH_TYPES)}, not '{auth_type}'."
            )
