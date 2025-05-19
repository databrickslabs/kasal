"""
Main entry point for running database seeders.
"""
import asyncio
import argparse
import logging
import traceback
import os
import sys
import inspect
from typing import List, Callable, Awaitable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Log when this module is imported
logger.info("⭐ seed_runner.py module imported")

# Set DEBUG to True to enable more detailed logging
DEBUG = os.getenv("SEED_DEBUG", "False").lower() in ("true", "1", "yes")
if DEBUG:
    logger.setLevel(logging.DEBUG)
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
    from src.seeds import tools, schemas, prompt_templates, model_configs, documentation
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
    
    for seeder_name, seeder_func in SEEDERS.items():
        logger.info(f"Running {seeder_name} seeder...")
        try:
            # Run each seeder independently
            debug_log(f"About to execute {seeder_name} seeder function")
            await seeder_func()
            logger.info(f"Completed {seeder_name} seeder successfully.")
        except Exception as e:
            logger.error(f"Error in {seeder_name} seeder: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Continue to next seeder even if current one fails
    
    logger.info("✅ All seeder operations completed.")

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