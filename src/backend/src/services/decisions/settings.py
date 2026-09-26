"""Workspace opt-in; credentials belong to the existing API key service."""

import logging
from typing import cast

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import BadRequestError, ConflictError, KasalError
from src.repositories.decision_config_repository import DecisionConfigRepository
from src.schemas.decision_config import DecisionConfigResponse, DecisionConfigUpdate
from src.services.decisions import provider
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
        row = await self.repository.get(self.group_id)
        key = await self.api_keys.find_by_name(JEV_KEY_NAME)
        return DecisionConfigResponse(
            enabled=bool(row and row.enabled),
            api_key_configured=bool(key and key.encrypted_value),
        )

    async def save(self, update: DecisionConfigUpdate) -> DecisionConfigResponse:
        key = await self.api_keys.find_by_name(JEV_KEY_NAME)
        if update.enabled and not provider.is_configured():
            raise BadRequestError(
                "Jev is not available on this deployment: a system admin must set the "
                "Jev API URL in Configuration → Engines"
            )
        if update.enabled and not (key and key.encrypted_value):
            raise BadRequestError(
                "Configure JEV_API_KEY in Configuration > API Keys before enabling Jev"
            )
        try:
            await self.repository.save(self.group_id, update.enabled)
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            raise ConflictError(
                "Jev settings changed concurrently. Reload and try again."
            ) from exc
        return DecisionConfigResponse(
            enabled=update.enabled,
            api_key_configured=bool(key and key.encrypted_value),
        )

    async def credential(self) -> str | None:
        """The decrypted key when this workspace opted in, read on OUR session.

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
        key = await self.api_keys.find_by_name(JEV_KEY_NAME)
        if not key or not key.encrypted_value:
            return None
        try:
            return EncryptionUtils.decrypt_value(cast(str, key.encrypted_value))
        except Exception as exc:
            # Never log the value or the ciphertext, so no traceback either.
            logger.error(  # noqa: TRY400 — a traceback could carry the ciphertext
                "Could not decrypt %s for workspace %s (%s); decisions are off "
                "for it until the key is re-entered",
                JEV_KEY_NAME,
                self.group_id,
                type(exc).__name__,
            )
            raise DecisionCredentialUnreadable() from None
