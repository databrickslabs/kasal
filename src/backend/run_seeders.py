#!/usr/bin/env python
"""Build the schema if needed, then run every seeder.

Exits 0 only when every seeder succeeded, and 1 otherwise, naming the seeders
that failed. (It used to exit 0 after logging hundreds of errors.)

It runs ``init_db()`` first, as server startup does: on an empty database every
seeder used to fail with ``no such table``, because this script never created
the tables. ``init_db()`` is idempotent (it creates missing tables and self-heals
missing columns), so running it on an existing database is safe.
"""

import asyncio
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SeedTest")

# Enable debug mode for seeders
os.environ["SEED_DEBUG"] = "True"

# Ensure the current directory is in the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def run_test() -> int:
    """Create the schema, run all seeders, and return the process exit code."""
    logger.info("Starting manual seeder run...")

    from src.db.session import init_db
    from src.seeds.errors import SeedingError
    from src.seeds.seed_runner import run_all_seeders

    try:
        logger.info("Ensuring the database schema exists (init_db)...")
        await init_db()

        logger.info("Calling run_all_seeders()...")
        await run_all_seeders()
    except SeedingError as e:
        logger.error(f"SEEDING FAILED: {e}")
        return 1
    except Exception as e:
        logger.exception(f"SEEDING FAILED before the seeders could finish: {e}")
        return 1

    logger.info("All seeders completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run_test()))
