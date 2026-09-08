"""
Database seeder for initial groups.

Currently empty as groups are created dynamically based on user domains.
"""

import logging

logger = logging.getLogger(__name__)


async def seed() -> None:
    """
    Seed initial groups (currently none).

    Groups are now created dynamically based on user email domains.
    Personal workspaces are created automatically for each user.
    """
    logger.info("✅ Groups seeder completed (no default groups to seed)")
