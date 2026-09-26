"""Workspace opt-in; credentials belong to the existing API key service."""

import logging
from typing import cast

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import BadRequestError, ConflictError
from src.repositories.decision_config_repository import DecisionConfigRepository
from src.schemas.decision_config import DecisionConfigResponse, DecisionConfigUpdate
from src.services.decisions import provider
from src.services.settings.api_keys import ApiKeysService
from src.utils.encryption_utils import EncryptionUtils

logger = logging.getLogger(__name__)

JEV_KEY_NAME = "JEV_API_KEY"


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
                "Jev is not available on this deployment: JEV_API_BASE is not set"
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
            # Never log the value or the ciphertext.
            logger.warning(
                "Could not decrypt %s (%s)", JEV_KEY_NAME, type(exc).__name__
            )
            return None
