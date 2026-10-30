"""Await and verify installation defaults before accepting the first request."""

from typing import Optional

from sqlalchemy import select

from src.core.logger import get_logger
from src.db.session import async_session_factory
from src.models.model_config import ModelConfig
from src.models.template import PromptTemplate
from src.models.tool import Tool
from src.seeds.errors import SeedingError
from src.seeds.seed_runner import run_all_seeders

logger = get_logger(__name__)


async def seed_installed_database() -> None:
    """Use the already-activated Lakebase factory, including in imported seeders.

    Blocks startup (raises) when an ESSENTIAL seeder failed or its table is
    still empty: a fresh install has nothing to fall back on. A failure in any
    other seeder (demo crews, skills, schemas...) is logged as an error and the
    app starts anyway, without that content.
    """
    logger.info("Seeding the installed Lakebase database before accepting requests")
    failure: Optional[SeedingError] = None
    try:
        await run_all_seeders()
    except SeedingError as e:
        failure = e
    if failure is not None:
        essential = failure.essential_failures()
        if essential:
            raise RuntimeError(
                "Lakebase initialization incomplete: essential seeder(s) "
                f"{', '.join(essential)} failed. {failure}"
            ) from failure
        # The runner already logged each failure with its traceback.
        logger.error(
            "Non-essential seeding failed on the installed database (%s); "
            "starting without that content",
            failure,
        )
    # Verify usable defaults from a fresh session so an uncommitted/failed seed
    # cannot report readiness.
    async with async_session_factory() as session:
        for model in (ModelConfig, PromptTemplate, Tool):
            result = await session.execute(select(model.id).limit(1))
            if result.scalar_one_or_none() is None:
                raise RuntimeError(
                    f"Lakebase initialization incomplete: {model.__tablename__} "
                    "has no seeded defaults. Check the preceding seeder errors."
                )
    logger.info("Lakebase installation defaults verified")
