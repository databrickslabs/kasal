#!/usr/bin/env python
"""
Script to manually run seeders for testing purposes.
"""
import os
import sys
import asyncio
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SeedTest")

# Enable debug mode for seeders
os.environ["SEED_DEBUG"] = "True"

# Ensure the current directory is in the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def run_test():
    """Run all seeders for testing."""
    logger.info("Starting manual seeder test...")
    
    try:
        # Import the run_all_seeders function
        from src.seeds.seed_runner import run_all_seeders
        
        # Run all seeders
        logger.info("Calling run_all_seeders()...")
        await run_all_seeders()
        
        logger.info("All seeders completed successfully!")
    except Exception as e:
        logger.error(f"Error running seeders: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_test()) 