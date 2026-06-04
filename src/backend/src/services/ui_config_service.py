from typing import Optional
import logging

from src.models.ui_config import UIConfig
from src.repositories.ui_config_repository import UIConfigRepository
from src.schemas.ui_config import UIConfigResponse, UIConfigUpdate

logger = logging.getLogger(__name__)


class UIConfigService:
    """
    Group-aware service for the per-workspace Predefined UI configuration.

    Reads default to "disabled" when a workspace has never configured it, so
    crews keep their normal (HTML/markdown) behavior until a workspace admin
    explicitly opts in.
    """

    def __init__(self, session, group_id: Optional[str] = None):
        self.session = session
        self.repository = UIConfigRepository(session)
        self.group_id = group_id

    async def get_config(self) -> UIConfigResponse:
        """Return this workspace's UI config, or a disabled default."""
        config = await self.repository.get_for_group(self.group_id)
        if config is None:
            return UIConfigResponse(group_id=self.group_id)
        return UIConfigResponse.model_validate(config)

    async def update_config(
        self, config_in: UIConfigUpdate, created_by_email: Optional[str] = None
    ) -> UIConfigResponse:
        """Upsert this workspace's UI config."""
        existing = await self.repository.get_for_group(self.group_id)
        if existing is None:
            existing = UIConfig(group_id=self.group_id, created_by_email=created_by_email)
            self.session.add(existing)

        existing.enabled = config_in.enabled
        existing.catalog_type = config_in.catalog_type
        existing.catalog_json = config_in.catalog_json
        existing.style_json = config_in.style_json

        await self.session.commit()
        await self.session.refresh(existing)
        logger.info(
            "Updated UI config for group %s (enabled=%s, catalog=%s)",
            self.group_id, existing.enabled, existing.catalog_type,
        )
        return UIConfigResponse.model_validate(existing)
