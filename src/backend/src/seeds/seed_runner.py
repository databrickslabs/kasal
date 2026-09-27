"""
Main entry point for running database seeders.
"""

import argparse
import asyncio
import inspect
import logging
import os
import sys
import traceback
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

# Use centralized logger - no need for basicConfig
from src.core.logger import get_logger
from src.seeds.errors import SeedingError

# Create module logger using centralized configuration
logger = get_logger(__name__)

# Log when this module is imported
logger.info("⭐ seed_runner.py module imported")

# Set DEBUG to True to enable more detailed logging
DEBUG = os.getenv("SEED_DEBUG", "False").lower() in ("true", "1", "yes")
if DEBUG:
    logger.debug("Seed runner debug mode enabled")


def debug_log(message: str) -> None:
    """Helper function for debug logging"""
    if DEBUG:
        # Get the calling function's name
        frame = inspect.currentframe()
        caller_frame = frame.f_back if frame else None
        caller = caller_frame.f_code.co_name if caller_frame else "?"
        logger.debug(f"[{caller}] {message}")


# Import seeders
try:
    debug_log("Importing seeders...")
    # Import all needed modules
    from src.db.session import async_session_factory as async_session_factory
    from src.seeds import (
        api_keys,
        bi_specialist_crews,
        example_crews,
        groups,
        model_configs,
        prompt_templates,
        schemas,
        skills,
        tools,
    )

    debug_log("Successfully imported all seeder modules")
except ImportError as e:
    logger.error(f"Error importing seeder modules: {e}")
    logger.error(traceback.format_exc())
    # Continue as some modules might still be available

# Dictionary of available seeders with their names and corresponding functions
SEEDERS = {}

# Try to add each seeder individually to avoid total failure if one module is missing
try:
    SEEDERS["tools"] = tools.seed
    debug_log("Added tools.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding tools seeder: {e}")

try:
    SEEDERS["skills"] = skills.seed
    debug_log("Added skills.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding skills seeder: {e}")

try:
    SEEDERS["schemas"] = schemas.seed
    debug_log("Added schemas.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding schemas seeder: {e}")

try:
    SEEDERS["prompt_templates"] = prompt_templates.seed
    debug_log("Added prompt_templates.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding prompt_templates seeder: {e}")

try:
    SEEDERS["model_configs"] = model_configs.seed
    debug_log("Added model_configs.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding model_configs seeder: {e}")

# The documentation seeder (crewai-docs scraper/embedder) was REMOVED with the
# crewai→kasal engine migration: it had been disabled for a while (generation
# retrieval was off; its idempotency bug bloated the table ~96x) and its content
# documented the retired crewAI framework. The documentation_embeddings table,
# model, repository, and DocumentationEmbeddingService all stay — the knowledge
# file-upload feature (KnowledgeEmbedding) stores its vectors there.

# Roles seeder removed - using simplified 3-tier role system

try:
    SEEDERS["groups"] = groups.seed
    debug_log("Added groups.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding groups seeder: {e}")

try:
    SEEDERS["api_keys"] = api_keys.seed
    debug_log("Added api_keys.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding api_keys seeder: {e}")

try:
    SEEDERS["example_crews"] = example_crews.seed
    debug_log("Added example_crews.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding example_crews seeder: {e}")

try:
    SEEDERS["bi_specialist_crews"] = bi_specialist_crews.seed
    debug_log("Added bi_specialist_crews.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding bi_specialist_crews seeder: {e}")

# Log available seeders
logger.info(f"Available seeders: {list(SEEDERS.keys())}")


async def _run_one(
    seeder_name: str,
    seeder_func: Callable[[], Awaitable[None]],
    failures: Dict[str, str],
) -> None:
    """Run one seeder, recording (not raising) its failure.

    The run carries on to the next seeder so one failure cannot hide another;
    the caller raises SeedingError once every seeder has had its turn.
    """
    logger.info(f"Running {seeder_name} seeder...")
    try:
        debug_log(f"About to execute {seeder_name} seeder function")
        await seeder_func()
        logger.info(f"Completed {seeder_name} seeder.")
    except Exception as e:
        failures[seeder_name] = str(e) or type(e).__name__
        logger.error(f"❌ {seeder_name} seeder FAILED: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")


def _raise_if_failed(failures: Dict[str, str], ran: int) -> None:
    """Log the run's outcome in one line and raise if anything failed."""
    if failures:
        error = SeedingError(failures)
        logger.error(f"❌ Seeding FAILED: {error}")
        raise error
    logger.info(f"✅ All {ran} seeder(s) completed.")


async def run_seeders(seeders_to_run: List[str]) -> None:
    """Run the specified seeders; raise SeedingError if any of them failed."""
    failures: Dict[str, str] = {}
    ran = 0
    for seeder_name in seeders_to_run:
        if seeder_name in SEEDERS:
            await _run_one(seeder_name, SEEDERS[seeder_name], failures)
            ran += 1
        else:
            logger.warning(f"Unknown seeder: {seeder_name}")
    _raise_if_failed(failures, ran)


async def run_all_seeders() -> None:
    """Run all available seeders."""
    logger.info("🚀 run_all_seeders function called")
    logger.info(f"Attempting to run {len(SEEDERS)} seeders: {list(SEEDERS.keys())}")

    if not SEEDERS:
        # Only happens when the seeder modules failed to import (logged above);
        # seeding nothing is a failure, not a successful empty run.
        _raise_if_failed(
            {"<registry>": "no seeders are registered; the seeder imports failed"}, 0
        )

    # Separate fast seeders from slow ones. Anything not explicitly fast runs
    # in the background so a slow seeder can never block startup (the removed
    # documentation seeder used to be the only slow one).
    fast_seeders = [
        "groups",
        "api_keys",
        "tools",
        "schemas",
        "prompt_templates",
        "model_configs",
        "example_crews",
        "bi_specialist_crews",
    ]
    slow_seeders = [name for name in SEEDERS if name not in fast_seeders]

    # Every seeder gets its turn even after one fails, so a single run reports
    # every failure; SeedingError is raised at the end (after the sequence
    # resync, which the rows that DID land still need).
    failures: Dict[str, str] = {}

    # Run fast seeders first (sequentially as they're quick)
    for seeder_name, seeder_func in SEEDERS.items():
        if seeder_name in fast_seeders:
            await _run_one(seeder_name, seeder_func, failures)

    # Then the slow ones, AWAITED. run_all_seeders itself already runs as a
    # background task (main.py), so nothing here blocks startup — and the old
    # fire-and-forget create_task, whose reference was dropped when this
    # function returned, is the one shape asyncio documents as unsafe: the loop
    # only holds a weak reference to a task, so it can be garbage-collected
    # mid-execution. On Databricks Apps that is exactly what the logs showed
    # for the skills seeder: a start line, then never a completion or an error
    # (issue #9). Awaiting also lets the sequence resync below see these rows.
    for seeder_name, seeder_func in SEEDERS.items():
        if seeder_name in slow_seeders:
            await _run_one(seeder_name, seeder_func, failures)

    # Resync PostgreSQL sequences after seeding.
    # Seeds (and backup restores) insert rows with explicit IDs which leaves
    # PostgreSQL auto-increment sequences behind, causing duplicate-key errors.
    await resync_postgres_sequences()

    _raise_if_failed(failures, len(SEEDERS))


async def resync_postgres_sequences() -> None:
    """Reset all PostgreSQL sequences to max(id)+1 so inserts don't collide.

    Only runs when the backend is PostgreSQL — silently skips for SQLite.
    """
    try:
        from src.config.settings import settings

        db_uri = str(settings.DATABASE_URI)
        # The configured fallback URI can still be SQLite after native Lakebase
        # activation. Seeders write through that activated factory, as must DDL.
        from src.db.session import async_session_factory as async_session_factory

        if "sqlite" in db_uri and not getattr(
            async_session_factory, "is_lakebase", False
        ):
            return  # SQLite uses ROWID, no sequences

        import re

        from sqlalchemy import text as sa_text

        from src.db.session import async_session_factory as async_session_factory

        safe_id_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        async with async_session_factory() as session:
            # Get all tables that have a serial/identity 'id' column
            result = await session.execute(
                sa_text(
                    "SELECT table_name FROM information_schema.columns "
                    "WHERE column_name = 'id' AND table_schema = current_schema() "
                    "AND (is_identity = 'YES' OR column_default LIKE 'nextval%')"
                )
            )
            tables = [row[0] for row in result.fetchall()]

            for table_name in tables:
                try:
                    if not safe_id_re.match(table_name):
                        continue
                    seq_name = f"{table_name}_id_seq"
                    await session.execute(
                        sa_text(
                            f"SELECT setval('{seq_name}', COALESCE((SELECT MAX(id) FROM \"{table_name}\"), 0) + 1, false)"
                        )
                    )
                except Exception:
                    pass

            await session.commit()
            logger.info(f"Resynced PostgreSQL sequences for {len(tables)} table(s)")
    except Exception as e:
        logger.debug(f"Sequence resync skipped: {e}")


async def run_seeders_with_factory(
    factory: Any, exclude: Optional[Set[str]] = None
) -> None:
    """Run seeders using a custom session factory instead of the default.

    This temporarily patches async_session_factory in all seeder modules
    so they connect to a different database (e.g., Lakebase after schema creation).

    Args:
        factory: async_sessionmaker to use for database connections
        exclude: set of seeder names to skip (e.g., {'tools'})
    """
    exclude = exclude or set()

    if not SEEDERS:
        _raise_if_failed(
            {"<registry>": "no seeders are registered; the seeder imports failed"}, 0
        )

    # Collect seeder modules that reference async_session_factory.
    # Use sys.modules to avoid NameError if a module failed to import.
    # The explicit names are unioned with the modules of whatever is actually
    # registered in SEEDERS: a hand-maintained list once omitted
    # src.seeds.skills, so on the Lakebase path every seeder wrote to Lakebase
    # except skills, which kept writing to the local database (issue #9).
    seeder_modules: List[Any] = []
    seed_module_names = sorted(
        {
            "src.seeds.tools",
            "src.seeds.skills",
            "src.seeds.schemas",
            "src.seeds.prompt_templates",
            "src.seeds.model_configs",
            "src.seeds.groups",
            "src.seeds.api_keys",
            "src.seeds.example_crews",
            "src.seeds.bi_specialist_crews",
        }
        | {getattr(func, "__module__", "") for func in SEEDERS.values()}
    )
    for mod_name in seed_module_names:
        mod = sys.modules.get(mod_name)
        if mod and hasattr(mod, "async_session_factory"):
            seeder_modules.append(mod)

    # Save originals and patch each module's reference
    originals: Dict[Any, Any] = {}
    for mod in seeder_modules:
        originals[mod] = mod.async_session_factory
        mod.async_session_factory = factory

    failures: Dict[str, str] = {}
    ran = 0
    try:
        for seeder_name, seeder_func in SEEDERS.items():
            if seeder_name in exclude:
                logger.info(f"Skipping {seeder_name} seeder (excluded)")
                continue
            await _run_one(seeder_name, seeder_func, failures)
            ran += 1
    finally:
        # Restore original factories
        for mod, original in originals.items():
            mod.async_session_factory = original
        logger.debug("Restored original session factory in all seeder modules")
    _raise_if_failed(failures, ran)


# Command-line entry point
async def main() -> int:
    """Main entry point for the seed runner; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Database seeding tool")
    parser.add_argument("--all", action="store_true", help="Run all seeders")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Add argument for each available seeder
    for seeder_name in SEEDERS.keys():
        parser.add_argument(
            f"--{seeder_name}",
            action="store_true",
            help=f"Run the {seeder_name} seeder",
        )

    args = parser.parse_args()

    # Enable debug mode if --debug flag is used
    global DEBUG
    if args.debug:
        DEBUG = True
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled via command line")

    try:
        # If --all is specified or no specific seeders are selected, run all
        if args.all or all(
            not getattr(args, seeder_name) for seeder_name in SEEDERS.keys()
        ):
            await run_all_seeders()
        else:
            # Run only the specified seeders
            selected_seeders = [
                seeder_name
                for seeder_name in SEEDERS.keys()
                if getattr(args, seeder_name)
            ]
            await run_seeders(selected_seeders)
    except SeedingError:
        return 1  # already logged by _raise_if_failed
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
