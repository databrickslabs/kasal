"""Installation-managed Lakebase configuration with the existing local override path."""

import logging
from typing import Any, Dict

from src.core.databricks_app import LakebaseAppResource

logger = logging.getLogger(__name__)


async def get_config(repository) -> Dict[str, Any]:
    """
    Get current Lakebase configuration.

    Returns:
        Dictionary with Lakebase configuration
    """
    installed = LakebaseAppResource.from_env()
    if installed:
        return installed.configuration()
    try:
        config = await repository.get_by_key("lakebase")
        if config:
            return {
                "enabled": config.value.get("enabled", False),
                "instance_name": config.value.get("instance_name", "kasal-lakebase"),
                "capacity": config.value.get("capacity", "CU_1"),
                "retention_days": config.value.get("retention_days", 14),
                "node_count": config.value.get("node_count", 1),
                "instance_status": config.value.get("instance_status", "NOT_CREATED"),
                "endpoint": config.value.get("endpoint"),
                "created_at": config.value.get("created_at"),
                "database_type": config.value.get("database_type", "lakebase"),
            }
        else:
            # Return default configuration
            return {
                "enabled": False,
                "instance_name": "kasal-lakebase",
                "capacity": "CU_1",
                "retention_days": 14,
                "node_count": 1,
                "instance_status": "NOT_CREATED",
                "database_type": "lakebase",
            }
    except Exception as e:
        logger.error(f"Error getting Lakebase config: {e}")
        raise
