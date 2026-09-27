"""Workspace opt-in to the decision model (Jev, directly or through OpenRouter).

Credentials belong to the existing API key service. Which key a workspace
needs follows the deployment's connection (``connection.current()``):
``JEV_API_KEY`` for the Jev API, ``OPENROUTER_API_KEY`` for OpenRouter.
"""

import logging
from typing import cast

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import BadRequestError, ConflictError, KasalError
from src.repositories.decision_config_repository import DecisionConfigRepository
from src.schemas.decision_config import DecisionConfigResponse, DecisionConfigUpdate
from src.services.decisions.connection import Connection
from src.services.decisions.connection import current as current_connection
from src.services.settings.api_keys import ApiKeysService
from src.utils.encryption_utils import EncryptionUtils

logger = logging.getLogger(__name__)

JEV_KEY_NAME = "JEV_API_KEY"


class DecisionCredentialUnreadable(KasalError):
    """The workspace opted in and has a JEV_API_KEY, but it cannot be decrypted.

    Distinct from "no key": that is a configuration the admin chose, this is a
    fault (a rotated encryption key, a corrupted row) someone has to fix, so
    it must not look the same. Callers that fall back (``decisions.runtime``)
    still fall back; they now know why.
    """

    status_code = 500
    detail = (
        f"The stored {JEV_KEY_NAME} could not be decrypted; "
        "re-enter it in Configuration > API Keys"
    )


class DecisionSettingsService:
    def __init__(self, session: AsyncSession, group_id: str) -> None:
        if not group_id:
            raise BadRequestError("A workspace is required")
        self.session = session
        self.group_id = group_id
        self.repository = DecisionConfigRepository(session)
        self.api_keys = ApiKeysService(session, group_id=group_id)

    async def get(self) -> DecisionConfigResponse:
        connection = current_connection()
        row = await self.repository.get(self.group_id)
        return self._response(
            connection, bool(row and row.enabled), await self._keyed(connection)
        )

    async def _keyed(self, connection: Connection) -> bool:
        """Whether this workspace has the key the deployment's connection spends."""
        key = await self.api_keys.find_by_name(connection.key_name)
        return bool(key and key.encrypted_value)

    @staticmethod
    def _response(
        connection: Connection, enabled: bool, keyed: bool
    ) -> DecisionConfigResponse:
        return DecisionConfigResponse(
            enabled=enabled,
            api_key_configured=keyed,
            available=enabled and keyed and connection.configured,
            connection=connection.kind,
            api_key_name=connection.key_name,
        )

    async def save(self, update: DecisionConfigUpdate) -> DecisionConfigResponse:
        connection = current_connection()
        keyed = await self._keyed(connection)
        if update.enabled and not connection.configured:
            raise BadRequestError(
                "No decision model is available on this deployment: a system admin "
                "must set the Jev API URL in System administration → Models"
            )
        if update.enabled and not keyed:
            raise BadRequestError(
                f"Configure {connection.key_name} in Configuration > API Keys before "
                "enabling the decision model"
            )
        try:
            await self.repository.save(self.group_id, update.enabled)
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            # Only a row that appeared since our read is a concurrent insert;
            # anything else is a fault and must not be reported as one.
            if await self.repository.get(self.group_id) is not None:
                raise ConflictError(
                    "Decision model settings changed concurrently. Reload and try again."
                ) from exc
            # decision_config holds only a workspace id and a flag: nothing
            # secret in the statement, so the traceback is safe to log.
            logger.exception(
                "Saving decision model settings for workspace %s failed",
                self.group_id,
            )
            raise KasalError(
                "Could not save decision model settings: the database rejected the "
                "change. Check the server log."
            ) from exc
        return self._response(connection, update.enabled, keyed)

    async def credential(self) -> str | None:
        """The connection's decrypted key when this workspace opted in, on OUR session.

        ``JEV_API_KEY`` for the Jev API, ``OPENROUTER_API_KEY`` for OpenRouter
        (where only Auto asks; see ``provider.OPENROUTER_POLICIES``).

        None when the workspace has not opted in or has no key. A key that
        exists but will not decrypt raises ``DecisionCredentialUnreadable``
        after an error-level log, rather than reading as "no key".

        Not ``ApiKeysService.get_provider_api_key``: that classmethod opens a
        second session of its own, i.e. two sessions per decision on a path
        that runs from memory and kernel hot spots. ``find_by_name`` on the
        injected service keeps the group scoping and uses this session.
        """
        row = await self.repository.get(self.group_id)
        if not row or row.enabled is not True:
            return None
        key_name = current_connection().key_name
        key = await self.api_keys.find_by_name(key_name)
        if not key or not key.encrypted_value:
            return None
        try:
            return EncryptionUtils.decrypt_value(cast(str, key.encrypted_value))
        except Exception as exc:
            # Never log the value or the ciphertext, so no traceback either.
            logger.error(  # noqa: TRY400 — a traceback could carry the ciphertext
                "Could not decrypt %s for workspace %s (%s); decisions are off "
                "for it until the key is re-entered",
                key_name,
                self.group_id,
                type(exc).__name__,
            )
            raise DecisionCredentialUnreadable() from None
