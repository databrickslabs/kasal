"""Fill the Configuration → Engines settings snapshot from the database.

Separate from ``engine_settings`` so that module stays stdlib-only: it ships
inside exported apps (the scrape tool reads its limits from it), and this is the
one part that needs Kasal's database.
"""

import logging

from src.services.settings import engine_settings

logger = logging.getLogger(__name__)


async def load() -> None:
    """(Re)load the snapshot from the database. Never raises: defaults apply."""
    try:
        from src.db.session import routed_scoped_session
        from src.services.settings.engine import EngineConfigService

        async with routed_scoped_session() as session:
            rows = await EngineConfigService(session).find_all()
        engine_settings.replace(rows)
    except Exception as exc:  # noqa: BLE001 — settings must never block a run
        logger.warning("Could not load engine settings; using defaults: %s", exc)
