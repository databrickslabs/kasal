"""Workspace opt-in; credentials belong to the existing API key service."""

from sqlalchemy.exc import IntegrityError

from src.core.exceptions import BadRequestError, ConflictError
from src.repositories.decision_config_repository import DecisionConfigRepository
from src.schemas.decision_config import DecisionConfigResponse, DecisionConfigUpdate
from src.services.settings.api_keys import ApiKeysService

JEV_KEY_NAME = "JEV_API_KEY"


class DecisionSettingsService:
    def __init__(self, session, group_id: str):
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
        row = await self.repository.get(self.group_id)
        if not row or row.enabled is not True:
            return None
        return await ApiKeysService.get_provider_api_key("jev", group_id=self.group_id)
