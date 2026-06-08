from typing import Optional
import logging

from src.models.ui_config import UIConfig
from src.repositories.ui_config_repository import UIConfigRepository
from src.schemas.ui_config import UIConfigResponse, UIConfigUpdate

logger = logging.getLogger(__name__)


# Prepended to the crew/task GENERATION prompt when a workspace has Predefined UI
# enabled. The base generation templates mandate "raw HTML <!DOCTYPE html>" for
# presentations/dashboards; that conflicts with the UI renderer and, at execution
# time, makes weaker models emit HTML (or stall asking which format to use). The
# real fix is to never bake HTML into the generated task: this directive tells the
# generator to produce a format-neutral, content/structure-only task so the
# downstream UI-document instruction has nothing to conflict with.
UI_DOCUMENT_GENERATION_DIRECTIVE = "\n".join([
    "=== RENDERING MODE: DESIGN-SYSTEM UI (not HTML) ===",
    "This workspace renders every final deliverable through a structured design-system",
    "UI renderer, NOT a web browser. This mode OVERRIDES any 'output HTML' guidance below.",
    "When generating tasks you MUST follow these rules:",
    "- Do NOT instruct any task to produce raw HTML, \"<!DOCTYPE html>\", a web page, CSS,",
    "  JavaScript, or a downloadable .html file. Do NOT mention HTML anywhere in a task.",
    "- For a presentation / dashboard / report / quiz, the final task's `description` must",
    "  describe only the CONTENT and STRUCTURE (sections, slides, bullet points, KPIs/metrics,",
    "  chart data, quiz questions) — never the HTML/CSS/JS form or visual styling code.",
    "- The final task's `expected_output` must describe that structured deliverable in a",
    "  FORMAT-NEUTRAL way (e.g. \"a structured slide presentation: a title slide plus 6-9",
    "  slides, each with a heading and 3-5 concise points, using metrics and charts where",
    "  relevant\") and MUST NOT mention HTML, <!DOCTYPE html>, CSS or JavaScript.",
    "- Keep the CONTENT-quality intent of any design guidance below (substantive points,",
    "  clear sections, KPIs, charts) but IGNORE its HTML/CSS/JS form requirements — the",
    "  platform builds the visual layout automatically.",
    "- Do NOT set output_json or output_pydantic. Research/data-gathering tasks keep normal",
    "  text output as usual.",
    "",
])


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
