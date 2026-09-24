"""Session ownership for decisions invoked outside an HTTP request/session."""

from src.db.session import get_isolated_db_session


async def decision_credential(group_id: str) -> str | None:
    from src.services.decisions.settings import DecisionSettingsService

    async with get_isolated_db_session() as session:
        return await DecisionSettingsService(session, group_id).credential()
