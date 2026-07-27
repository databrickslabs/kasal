"""Triggering recipe mining when a crew run actually finishes.

Mining used to be a 5-minute polling loop, on the reasoning that a crew writes
its terminal status from inside the spawned subprocess, where a hook would reach
nothing. That is true of the CHILD, but the parent awaits ``process.join()`` and
then reads the exit code — by which point the child has exited and its status
row and traces are committed. That join is a real "this crew just finished"
signal, and it is what this module hangs off.

The polling loop is gone: it cost every run up to five minutes of waiting before
its recipe could be curated, which reads as the feature being broken.

Kept in its own module so ``process_crew_executor`` gains a call, not a lump of
mining logic — and so this can be tested without spawning a subprocess.
"""

import asyncio
import logging

logger = logging.getLogger(__name__)

# One sweep at a time, with at most one more queued behind it. Several crews can
# finish within the same second (a flow's crews, a batch of scheduled runs), and
# a sweep per completion would have them contending over the same rows to mine
# the same handful of executions. Coalescing keeps that to two passes: the one
# running, and one after it that sees everything the others finished.
_sweep_lock = asyncio.Lock()
_sweep_pending = False


async def mine_now() -> int:
    """Run one mining sweep, coalesced against any sweep already running.

    Returns the number of recipes created or refreshed by THIS call (0 when it
    folded into a sweep that was already in flight).
    """
    global _sweep_pending

    if _sweep_lock.locked():
        # A sweep is mid-flight. It may already have read past our execution, so
        # ask for one more pass rather than assuming we are covered.
        _sweep_pending = True
        return 0

    async with _sweep_lock:
        from src.services.workflow_recipe_service import WorkflowRecipeService

        mined = await WorkflowRecipeService.sweep()

        if _sweep_pending:
            _sweep_pending = False
            mined += await WorkflowRecipeService.sweep()

    return mined


def schedule_mining_after_run(execution_id: str) -> None:
    """Mine in the background now that ``execution_id`` has finished.

    Fire-and-forget by construction: mining must never delay a run's result or
    fail it. Every failure mode — no event loop, a mining error — is swallowed
    here, since the worst case is a recipe that appears on the next completed
    run instead of this one.
    """

    async def _mine() -> None:
        try:
            mined = await mine_now()
            if mined:
                logger.info(
                    f"[WorkflowRecipes] Mined {mined} recipe(s) after {execution_id}"
                )
        except Exception as mining_err:  # noqa: BLE001 — never touches the run
            logger.warning(
                f"[WorkflowRecipes] Mining after {execution_id} failed: {mining_err}"
            )

    try:
        asyncio.get_running_loop().create_task(_mine())
    except RuntimeError:
        # Called from a thread with no loop. Nothing to schedule onto, and
        # blocking here would be worse than the recipe arriving later.
        logger.debug(
            f"[WorkflowRecipes] No running loop; skipped mining after {execution_id}"
        )
