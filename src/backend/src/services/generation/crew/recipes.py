"""Workflow-recipe hooks shared by every generation strategy.

Kept apart from the strategies because this is the seam where reuse enters
generation: retrieval before the prompt is built, and the measurement trial
after the entities exist. Both are best-effort — generation predates this
feature and must keep working without it."""

import logging
from typing import Dict, Any, List, Tuple, Optional
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class RecipeHooksMixin:
    """Workflow-recipe hooks shared by every generation strategy.

    Kept apart from the strategies because this is the seam where reuse enters
    generation: retrieval before the prompt is built, and the measurement trial
    after the entities exist. Both are best-effort — generation predates this
    feature and must keep working without it."""

    async def _prepare_exemplars(self, request: Any, group_context: Optional[GroupContext],
                                 session: Any = None) -> Optional[Any]:
        """Ask the recipe library what it can contribute to this generation.

        Returns the decision (text + candidates + arm) or None when reuse is not
        applicable or errored. Failure is swallowed on purpose: crew generation
        predates this feature and must keep working without it.

        ``session`` is explicit because the progressive path runs as a background
        task AFTER the request-scoped session is closed — it must pass one of its
        own rather than let this reach for ``self.session``.
        """
        prompt = getattr(request, "prompt", None)
        if not prompt or not group_context:
            return None
        try:
            from src.services.recipes.recipes import WorkflowRecipeService

            return await WorkflowRecipeService(session or self.session).prepare_exemplars(
                prompt, group_context.group_ids or []
            )
        except Exception as exemplar_err:  # noqa: BLE001
            logger.warning(f"CREATE CREW: exemplar injection skipped: {exemplar_err}")
            return None

    async def _record_recipe_trial(self, decision: Optional[Any], result: Dict[str, Any],
                                   group_context: Optional[GroupContext],
                                   session: Any = None) -> None:
        """Record what the recipe library did for this generation, and what came
        out of it. Best-effort — the service's own recorder never raises."""
        if decision is None:
            return
        try:
            from src.services.recipes.recipes import WorkflowRecipeService

            await WorkflowRecipeService(session or self.session).record_trial(
                decision,
                generated=result,
                group_id=group_context.primary_group_id if group_context else None,
                group_email=group_context.group_email if group_context else None,
            )
        except Exception as trial_err:  # noqa: BLE001
            logger.warning(f"CREATE CREW: recipe trial not recorded: {trial_err}")

    async def _isolated_session_ctx(self):
        """A PRIVATE-connection session context, matching the generation flow.

        Never the shared StaticPool ``async_session_factory``: on SQLite that is
        one connection, and a concurrent commit/rollback on it can discard an
        agent this generation already committed, breaking the next task's
        agent_id foreign key. That is a fixed regression with a test guarding it
        — recipe work must not reintroduce it just because its own writes look
        harmless.
        """
        import os as _os

        from src.db.database_router import (
            get_lakebase_config_from_db,
            is_lakebase_enabled,
        )
        from src.db.lakebase_session import get_lakebase_session
        from src.db.session import get_isolated_db_session

        if await is_lakebase_enabled():
            lb_config = await get_lakebase_config_from_db()
            lb_instance = (
                (lb_config or {}).get("instance_name")
                or _os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")
            )
            return get_lakebase_session(lb_instance)
        return get_isolated_db_session()

    async def _recipe_decision_isolated(self, request: Any,
                                        group_context: Optional[GroupContext]) -> Optional[Any]:
        """Exemplar decision on a session of its own.

        The progressive path plans BEFORE it opens its working session (planning
        does no DB writes), and its inherited request session is already closed,
        so retrieval needs a short-lived session that it owns and disposes of.
        """
        try:
            ctx = await self._isolated_session_ctx()
            async with ctx as recipe_session:
                return await self._prepare_exemplars(
                    request, group_context, session=recipe_session
                )
        except Exception as exc:  # noqa: BLE001 — never block generation
            logger.warning(f"PROGRESSIVE: exemplar lookup skipped: {exc}")
            return None

    async def _record_recipe_trial_isolated(self, decision: Optional[Any],
                                            agents: List[Dict[str, Any]],
                                            tasks: List[Dict[str, Any]],
                                            group_context: Optional[GroupContext]) -> None:
        """Trial write on a session of its own, for the same reason."""
        if decision is None:
            return
        try:
            ctx = await self._isolated_session_ctx()
            async with ctx as trial_session:
                await self._record_recipe_trial(
                    decision,
                    {"agents": agents, "tasks": tasks},
                    group_context,
                    session=trial_session,
                )
        except Exception as exc:  # noqa: BLE001 — measurement never breaks a run
            logger.warning(f"PROGRESSIVE: recipe trial not recorded: {exc}")
