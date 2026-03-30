"""
Main entry point for running database seeders.
"""
import asyncio
import argparse
import traceback
import os
import sys
import inspect
from typing import List, Callable, Awaitable, Optional, Set

# Use centralized logger - no need for basicConfig
from src.core.logger import get_logger

# Create module logger using centralized configuration
logger = get_logger(__name__)

# Log when this module is imported
logger.info("⭐ seed_runner.py module imported")

# Set DEBUG to True to enable more detailed logging
DEBUG = os.getenv("SEED_DEBUG", "False").lower() in ("true", "1", "yes")
if DEBUG:
    logger.debug("Seed runner debug mode enabled")

def debug_log(message):
    """Helper function for debug logging"""
    if DEBUG:
        # Get the calling function's name
        caller = inspect.currentframe().f_back.f_code.co_name
        logger.debug(f"[{caller}] {message}")

# Import seeders
try:
    debug_log("Importing seeders...")
    # Import all needed modules
    from src.seeds import tools, schemas, prompt_templates, model_configs, documentation, groups, api_keys, dspy_examples, example_crews
    from src.db.session import async_session_factory
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

try:
    SEEDERS["documentation"] = documentation.seed
    debug_log("Added documentation.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding documentation seeder: {e}")

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
    SEEDERS["dspy_examples"] = dspy_examples.seed
    debug_log("Added dspy_examples.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding dspy_examples seeder: {e}")

try:
    SEEDERS["example_crews"] = example_crews.seed
    debug_log("Added example_crews.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding example_crews seeder: {e}")

# Log available seeders
logger.info(f"Available seeders: {list(SEEDERS.keys())}")

async def run_seeders(seeders_to_run: List[str]) -> None:
    """Run the specified seeders."""
    for seeder_name in seeders_to_run:
        if seeder_name in SEEDERS:
            logger.info(f"Running {seeder_name} seeder...")
            try:
                debug_log(f"Calling {seeder_name}.seed() function")
                await SEEDERS[seeder_name]()
                logger.info(f"Completed {seeder_name} seeder.")
            except Exception as e:
                logger.error(f"Error running {seeder_name} seeder: {e}")
                logger.error(traceback.format_exc())
                # Continue to next seeder even if this one fails
        else:
            logger.warning(f"Unknown seeder: {seeder_name}")

async def run_all_seeders() -> None:
    """Run all available seeders."""
    logger.info("🚀 run_all_seeders function called")
    logger.info(f"Attempting to run {len(SEEDERS)} seeders: {list(SEEDERS.keys())}")
    
    if not SEEDERS:
        logger.warning("No seeders are registered! Check if seeder modules were imported correctly.")
        return
    
    # Separate fast seeders from slow ones
    fast_seeders = ['groups', 'api_keys', 'tools', 'schemas', 'prompt_templates', 'model_configs', 'dspy_examples', 'example_crews']
    slow_seeders = ['documentation']  # Documentation seeder is slow due to embeddings

    # Run fast seeders first (sequentially as they're quick)
    for seeder_name, seeder_func in SEEDERS.items():
        if seeder_name in fast_seeders:
            logger.info(f"Running {seeder_name} seeder...")
            try:
                debug_log(f"About to execute {seeder_name} seeder function")
                await seeder_func()
                logger.info(f"Completed {seeder_name} seeder successfully.")
            except Exception as e:
                logger.error(f"Error in {seeder_name} seeder: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                # Continue to next seeder even if current one fails
    
    # Run slow seeders in the background (non-blocking)
    background_tasks = []
    for seeder_name, seeder_func in SEEDERS.items():
        if seeder_name in slow_seeders:
            logger.info(f"Starting {seeder_name} seeder in background (non-blocking)...")
            
            async def run_seeder_background(name, func):
                """Run a seeder in the background with error handling."""
                try:
                    debug_log(f"Background execution of {name} seeder starting")
                    await func()
                    logger.info(f"✅ Background {name} seeder completed successfully.")
                except Exception as e:
                    logger.error(f"❌ Error in background {name} seeder: {str(e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Create task but don't await it (non-blocking)
            task = asyncio.create_task(run_seeder_background(seeder_name, seeder_func))
            background_tasks.append(task)
            logger.info(f"✓ {seeder_name} seeder started in background, continuing...")
    
    logger.info("✅ All fast seeders completed, slow seeders running in background.")

    # Optionally, you can store the tasks if you need to track them later
    # But we don't await them here to keep it non-blocking
    if background_tasks:
        logger.info(f"Running {len(background_tasks)} seeder(s) in background (non-blocking)")

    # Resync PostgreSQL sequences after seeding.
    # Seeds (and backup restores) insert rows with explicit IDs which leaves
    # PostgreSQL auto-increment sequences behind, causing duplicate-key errors.
    await resync_postgres_sequences()


async def resync_postgres_sequences() -> None:
    """Reset all PostgreSQL sequences to max(id)+1 so inserts don't collide.

    Only runs when the backend is PostgreSQL — silently skips for SQLite.
    """
    try:
        from src.config.settings import settings

        db_uri = str(settings.DATABASE_URI)
        if "sqlite" in db_uri:
            return  # SQLite uses ROWID, no sequences

        import re
        from sqlalchemy import text as sa_text
        from src.db.session import async_session_factory

        safe_id_re = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
        async with async_session_factory() as session:
            # Get all tables that have a serial/identity 'id' column
            result = await session.execute(sa_text(
                "SELECT table_name FROM information_schema.columns "
                "WHERE column_name = 'id' AND table_schema = 'public' "
                "AND (is_identity = 'YES' OR column_default LIKE 'nextval%')"
            ))
            tables = [row[0] for row in result.fetchall()]

            for table_name in tables:
                try:
                    if not safe_id_re.match(table_name):
                        continue
                    seq_name = f"{table_name}_id_seq"
                    await session.execute(sa_text(
                        f"SELECT setval('{seq_name}', COALESCE((SELECT MAX(id) FROM \"{table_name}\"), 0) + 1, false)"
                    ))
                except Exception:
                    pass

            await session.commit()
            logger.info(f"Resynced PostgreSQL sequences for {len(tables)} table(s)")
    except Exception as e:
        logger.debug(f"Sequence resync skipped: {e}")


async def run_seeders_with_factory(factory, exclude: Optional[Set[str]] = None) -> None:
    """Run seeders using a custom session factory instead of the default.

    This temporarily patches async_session_factory in all seeder modules
    so they connect to a different database (e.g., Lakebase after schema creation).

    Args:
        factory: async_sessionmaker to use for database connections
        exclude: set of seeder names to skip (e.g., {'documentation'})
    """
    exclude = exclude or set()

    if not SEEDERS:
        logger.warning("No seeders registered — skipping run_seeders_with_factory")
        return

    # Collect seeder modules that reference async_session_factory.
    # Use sys.modules to avoid NameError if a module failed to import.
    seeder_modules = []
    seed_module_names = [
        'src.seeds.tools', 'src.seeds.schemas', 'src.seeds.prompt_templates',
        'src.seeds.model_configs', 'src.seeds.documentation', 'src.seeds.groups',
        'src.seeds.api_keys', 'src.seeds.dspy_examples', 'src.seeds.example_crews',
    ]
    for mod_name in seed_module_names:
        mod = sys.modules.get(mod_name)
        if mod and hasattr(mod, 'async_session_factory'):
            seeder_modules.append(mod)

    # Save originals and patch each module's reference
    originals = {}
    for mod in seeder_modules:
        originals[mod] = mod.async_session_factory
        mod.async_session_factory = factory

    try:
        for seeder_name, seeder_func in SEEDERS.items():
            if seeder_name in exclude:
                logger.info(f"Skipping {seeder_name} seeder (excluded)")
                continue
            logger.info(f"Running {seeder_name} seeder with custom factory...")
            try:
                await seeder_func()
                logger.info(f"Completed {seeder_name} seeder.")
            except Exception as e:
                logger.error(f"Error running {seeder_name} seeder: {e}")
                logger.error(traceback.format_exc())
    finally:
        # Restore original factories
        for mod, original in originals.items():
            mod.async_session_factory = original
        logger.debug("Restored original session factory in all seeder modules")


# Command-line entry point
async def main() -> None:
    """Main entry point for the seed runner."""
    parser = argparse.ArgumentParser(description="Database seeding tool")
    parser.add_argument("--all", action="store_true", help="Run all seeders")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    # Add argument for each available seeder
    for seeder_name in SEEDERS.keys():
        parser.add_argument(
            f"--{seeder_name}", 
            action="store_true", 
            help=f"Run the {seeder_name} seeder"
        )
    
    args = parser.parse_args()
    
    # Enable debug mode if --debug flag is used
    global DEBUG
    if args.debug:
        DEBUG = True
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled via command line")
    
    # If --all is specified or no specific seeders are selected, run all
    if args.all or all(not getattr(args, seeder_name) for seeder_name in SEEDERS.keys()):
        await run_all_seeders()
    else:
        # Run only the specified seeders
        selected_seeders = [
            seeder_name for seeder_name in SEEDERS.keys() 
            if getattr(args, seeder_name)
        ]
        await run_seeders(selected_seeders)

if __name__ == "__main__":
    asyncio.run(main()) 