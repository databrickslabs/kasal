"""Seeding as server startup runs it: in the background, never blocking boot.

A failed seed at startup is logged as an ERROR naming every failed seeder, and
the server keeps serving. That is deliberate: seeding is an upsert of shipped
defaults, so a database that already has them loses nothing, and a failure in
demo content (example crews, the BI workspace) must not take the app down. The
one place a failure does block startup is a fresh Lakebase installation, which
has nothing to fall back on; that path is ``installation.seed_installed_database``.
"""

from typing import Optional

from src.core.logger import get_logger
from src.seeds.errors import SeedingError

logger = get_logger(__name__)


async def run_seeders_in_background() -> None:
    """Run every seeder; log (never raise) a failure. For ``create_task``."""
    from src.seeds.seed_runner import run_all_seeders

    logger.info("Background seeders started...")
    failure: Optional[SeedingError] = None
    try:
        await run_all_seeders()
    except SeedingError as e:
        failure = e  # each seeder's traceback is already logged by the runner
    except Exception:
        logger.exception("Database seeding FAILED at startup with an unexpected error")
        return
    if failure is None:
        logger.info("Background database seeding completed successfully!")
        return
    essential = failure.essential_failures()
    logger.error(
        "Database seeding FAILED at startup (%s). The server keeps running%s. "
        "Fix the cause above, then restart or run `python run_seeders.py`.",
        failure,
        (
            f"; missing essential defaults ({', '.join(essential)}) leave the UI "
            "without models, tools or prompts until this is fixed"
            if essential
            else ""
        ),
    )
