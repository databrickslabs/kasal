"""Auto under the OpenRouter connection: the Jev Router model answers.

OpenRouter has no native decision endpoint, so Kasal does not ask Jev to pick
one of the workspace's models there. Auto resolves to the Jev Router model
instead (``typesafe/jev-router`` through the ``openrouter`` LLM provider), which
picks the model and reasoning effort for each request on OpenRouter's side and
returns the answer itself. There is no separate decision call.

The connection implies the model: Jev Router does not have to be enabled in
Configuration → Models, because choosing the OpenRouter connection is the
administrator's choice of it. It has to exist in the catalogue (it is seeded)
and the workspace must have opted in with an ``OPENROUTER_API_KEY``, the same
bar as ``GET /decision-config`` ``available``. Otherwise this returns None and
Auto takes its usual fallback: the workspace default model.
"""

import logging
from time import monotonic
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.decisions.connection import JEV_ROUTER_KEY, OPENROUTER
from src.services.decisions.connection import current as current_connection
from src.services.decisions.model_selection import ModelSelection
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


async def select_router(
    session: AsyncSession, group_context: Optional[GroupContext]
) -> Optional[ModelSelection]:
    """Jev Router for this request, or None when it does not apply."""
    if current_connection().kind != OPENROUTER:
        return None
    started = monotonic()
    group_id = group_context.primary_group_id if group_context is not None else None
    if not group_id:
        return None
    from src.services.decisions.settings import DecisionSettingsService
    from src.services.settings.models import ModelConfigService

    if not (await DecisionSettingsService(session, group_id).get()).available:
        return None
    if await ModelConfigService(session, group_id).find_by_key(JEV_ROUTER_KEY) is None:
        logger.warning(
            "Auto: the OpenRouter connection is set but the %s model is missing "
            "from the catalogue; using the workspace default",
            JEV_ROUTER_KEY,
        )
        return None
    logger.info("Auto model selection for workspace %s: Jev Router", group_id)
    return ModelSelection(JEV_ROUTER_KEY, "selected", (monotonic() - started) * 1000)
