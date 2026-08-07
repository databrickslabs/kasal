import logging
from typing import Optional

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

    Resolution is two-level, like MCP servers: a workspace's own row wins, else the
    global default row (``group_id IS NULL``), else the schema defaults.

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

    async def update_config(
        self, config_in: UIConfigUpdate, created_by_email: Optional[str] = None
    ) -> UIConfigResponse:
        """Upsert this workspace's UI config."""
        # EXACT, not the resolving lookup: get_for_group falls back to the global
        # default (group_id IS NULL), and editing that in place would rewrite the
        # default for every other workspace. A workspace saving for the first time
        # must create its OWN row.
        existing = await self.repository.get_for_group_exact(self.group_id)
        if existing is None:
            existing = UIConfig(
                group_id=self.group_id, created_by_email=created_by_email
            )
            await self.repository.add(existing)

        existing.enabled = config_in.enabled
        existing.catalog_type = config_in.catalog_type
        existing.catalog_json = config_in.catalog_json
        existing.style_json = config_in.style_json

        await self.session.commit()
        await self.repository.reload(existing)
        logger.info(
            "Updated UI config for group %s (enabled=%s, catalog=%s)",
            self.group_id,
            existing.enabled,
            existing.catalog_type,
        )
        return UIConfigResponse.model_validate(existing)
