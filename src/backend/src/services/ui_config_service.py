from typing import Optional
import logging

from src.models.ui_config import UIConfig
from src.repositories.ui_config_repository import UIConfigRepository
from src.schemas.ui_config import UIConfigResponse, UIConfigUpdate

logger = logging.getLogger(__name__)


# NOTE: the crew/task GENERATION templates are now format-neutral (they describe
# content and structure, never HTML/CSS/JS), and output formatting is owned by the
# shared A2UI composer (a2ui_runner) which composes a surface post-execution. The
# former UI_DOCUMENT_GENERATION_DIRECTIVE prepend was therefore redundant and has
# been removed.


class UIConfigService:
    """
    Group-aware service for the per-workspace Predefined UI configuration.

    A workspace that has never configured it gets the schema defaults — which are
    ``enabled=True`` + ``catalog_type="minimal"`` (see UIConfigBase). So Predefined
    UI is ON by default until an admin disables it; the A2UI composer treats an
    unconfigured workspace as enabled with the full bundled catalog (it only honors
    a restricted catalog once an admin saves a choice).
    """

    def __init__(self, session, group_id: Optional[str] = None):
        self.session = session
        self.repository = UIConfigRepository(session)
        self.group_id = group_id

    async def get_config(self) -> UIConfigResponse:
        """Return this workspace's UI config, or the schema defaults (enabled=True,
        catalog_type='minimal') when it has never been configured."""
        config = await self.repository.get_for_group(self.group_id)
        if config is None:
            return UIConfigResponse(group_id=self.group_id)
        return UIConfigResponse.model_validate(config)

    @classmethod
    async def is_predefined_ui_enabled(cls, group_id: Optional[str]) -> bool:
        """Whether this workspace has Predefined UI enabled.

        Opens its own short-lived session so generation services (which may not
        share a session) can check cheaply. Never raises — a read failure simply
        means "not enabled", preserving the default HTML/markdown behavior.
        """
        if not group_id:
            return False
        try:
            from src.db.session import request_scoped_session

            async with request_scoped_session() as session:
                config = await cls(session, group_id=group_id).get_config()
            return bool(config.enabled)
        except Exception as e:  # noqa: BLE001 — never let a UI check break generation
            logger.warning("[UIConfig] enabled-check failed for group %s: %s", group_id, e)
            return False

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
