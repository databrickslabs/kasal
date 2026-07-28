"""Managing the remote A2A agents a workspace has attached.

CRUD plus the one behaviour that makes the registry more than a table: fetching
the remote's Agent Card. A row whose card has never been fetched is a URL
somebody typed; a row with a cached card is a capability with known skills, and
the difference is what the UI and the tool description both need.

The protocol lives in ``services/a2a/client.py``. This service owns policy —
group scoping, credential encryption, who may configure — and never builds a
request.
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
from src.services.a2a import client as a2a_client
from src.utils.encryption_utils import EncryptionUtils
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class A2AAgentService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = A2AAgentRepository(session)

    async def list_agents(self, group_context: GroupContext) -> List[Any]:
        return await self.repository.list_for_group(group_context.group_ids or [])

    async def get_agent(self, agent_id: int, group_context: GroupContext) -> Any:
        return await self.repository.find_for_group(
            agent_id, group_context.group_ids or []
        )

    async def create_agent(
        self, data: A2AAgentCreate, group_context: GroupContext
    ) -> Any:
        self._validate_auth_type(data.auth_type)
        group_id = group_context.primary_group_id or (
            group_context.group_ids[0] if group_context.group_ids else None
        )
        if not group_id:
            raise ValueError("A workspace is required to configure a remote agent.")

        existing = await self.repository.find_by_name(data.name, [group_id])
        if existing:
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
            group_id=group_id,
            created_by_email=group_context.group_email,
        )
        self.session.add(agent)
        await self.session.flush()

        # Fetch the card immediately so a typo in the URL is a message on the
        # form rather than a tool that silently does nothing at run time.
        await self._refresh_card(agent, group_context)
        return agent

    async def update_agent(
        self, agent_id: int, data: A2AAgentUpdate, group_context: GroupContext
    ) -> Optional[Any]:
        agent = await self.get_agent(agent_id, group_context)
        if not agent:
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

        await self.session.flush()
        if data.card_url is not None or data.api_key is not None:
            await self._refresh_card(agent, group_context)
        return agent

    async def delete_agent(self, agent_id: int, group_context: GroupContext) -> bool:
        agent = await self.get_agent(agent_id, group_context)
        if not agent:
            return False
        await self.session.delete(agent)
        await self.session.flush()
        return True

    async def test_connection(
        self, agent_id: int, group_context: GroupContext
    ) -> Optional[A2AConnectionTest]:
        agent = await self.get_agent(agent_id, group_context)
        if not agent:
            return None
        return await self._refresh_card(agent, group_context)

    async def resolve_for_call(
        self, name: str, group_ids: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Everything one outbound call needs, resolved in one place.

        Returns the interface URL and decrypted key rather than the row, so a
        caller cannot accidentally pass a model object — and its credential —
        somewhere it does not belong.
        """
        agent = await self.repository.find_by_name(name, group_ids)
        if not agent or not agent.enabled:
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
        rolls back the configuration the operator just wrote — they usually need
        to save the row precisely so they can fix the URL on it.
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
            await self.session.flush()
            return A2AConnectionTest(connected=False, message=str(exc)[:500])

        agent.cached_card = card
        agent.card_fetched_at = datetime.utcnow()
        agent.last_error = None
        if not agent.description:
            agent.description = str(card.get("description") or "")[:1000]
        await self.session.flush()

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
