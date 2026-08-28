"""
Service for engine configuration operations.

This module provides business logic for engine configuration operations,
including retrieving and managing engine configurations.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from src.core.logger import LoggerManager
from src.models.engine_config import EngineConfig
from src.repositories.engine_config_repository import EngineConfigRepository

logger = LoggerManager.get_instance().crew


class EngineConfigService:
    """Service for engine configuration operations."""

    def __init__(self, session):
        """
        Initialize the service with session.

        Args:
            session: Database session from FastAPI DI (from core.dependencies)
        """
        self.repository = EngineConfigRepository(session)

    async def find_all(self) -> List[EngineConfig]:
        """
        Get all engine configurations from the repository.

        Returns:
            List of all engine configurations
        """
        return await self.repository.find_all()

    async def find_enabled_configs(self) -> List[EngineConfig]:
        """
        Get all enabled engine configurations from the repository.

        Returns:
            List of enabled engine configurations
        """
        return await self.repository.find_enabled_configs()

    async def find_by_engine_name(self, engine_name: str) -> Optional[EngineConfig]:
        """
        Get an engine configuration by its name from the repository.

        Args:
            engine_name: The engine name to find

        Returns:
            Engine configuration if found, None otherwise
        """
        return await self.repository.find_by_engine_name(engine_name)

    async def find_by_engine_and_key(
        self, engine_name: str, config_key: str
    ) -> Optional[EngineConfig]:
        """
        Get an engine configuration by engine name and config key.

        Args:
            engine_name: The engine name to find
            config_key: The configuration key to find

        Returns:
            Engine configuration if found, None otherwise
        """
        return await self.repository.find_by_engine_and_key(engine_name, config_key)

    async def find_by_engine_type(self, engine_type: str) -> List[EngineConfig]:
        """
        Get all engine configurations by engine type.

        Args:
            engine_type: The engine type to find

        Returns:
            List of engine configurations
        """
        return await self.repository.find_by_engine_type(engine_type)

    async def create_engine_config(self, config_data):
        """
        Create a new engine configuration.

        Args:
            config_data: Data for the new engine configuration

        Returns:
            Created engine configuration

        Raises:
            ValueError: If engine configuration with the same engine name and config key already exists
        """
        # Check if engine config already exists for this specific engine_name and config_key
        existing_config = await self.repository.find_by_engine_and_key(
            config_data.engine_name, config_data.config_key
        )
        if existing_config:
            raise ValueError(
                f"Engine configuration with name {config_data.engine_name} and key {config_data.config_key} already exists"
            )

        # Convert Pydantic model to dict if needed
        if hasattr(config_data, "model_dump"):
            config_dict = config_data.model_dump()
        elif hasattr(config_data, "dict"):
            config_dict = config_data.model_dump()
        else:
            config_dict = dict(config_data)

        # Create new engine config
        return await self.repository.create(config_dict)

    async def update_engine_config(self, engine_name: str, config_data):
        """
        Update an existing engine configuration.

        Args:
            engine_name: Name of the engine to update
            config_data: Updated configuration data

        Returns:
            Updated engine configuration, or None if not found
        """
        # Check if engine config exists
        existing_config = await self.repository.find_by_engine_name(engine_name)
        if not existing_config:
            return None

        # Convert Pydantic model to dict if needed
        if hasattr(config_data, "model_dump"):
            config_dict = config_data.model_dump(exclude_unset=True)
        elif hasattr(config_data, "dict"):
            config_dict = config_data.model_dump(exclude_unset=True)
        else:
            config_dict = dict(config_data)

        # Update engine config
        return await self.repository.update(existing_config.id, config_dict)

    async def toggle_engine_enabled(
        self, engine_name: str, enabled: bool
    ) -> Optional[EngineConfig]:
        """
        Toggle the enabled status of an engine configuration.

        Args:
            engine_name: Name of the engine to toggle
            enabled: New enabled status

        Returns:
            Updated engine configuration, or None if not found
        """
        try:
            # Use the direct DML method to avoid locking
            updated = await self.repository.toggle_enabled(engine_name, enabled)

            if not updated:
                return None

            # Get the updated engine config
            return await self.repository.find_by_engine_name(engine_name)
        except Exception as e:
            # Log the error at service level but don't expose internal details
            logger.error(
                f"Error in toggle_engine_enabled for engine={engine_name}: {str(e)}"
            )
            # Re-raise for controller layer to handle
            raise

    async def update_config_value(
        self, engine_name: str, config_key: str, config_value: str
    ) -> Optional[EngineConfig]:
        """
        Update the configuration value for a specific engine and key.

        Args:
            engine_name: Name of the engine
            config_key: Configuration key
            config_value: New configuration value

        Returns:
            Updated engine configuration, or None if not found
        """
        try:
            # Use the repository method to update config value
            updated = await self.repository.update_config_value(
                engine_name, config_key, config_value
            )

            if not updated:
                return None

            # Get the updated engine config
            return await self.repository.find_by_engine_and_key(engine_name, config_key)
        except Exception as e:
            # Log the error at service level but don't expose internal details
            logger.error(
                f"Error in update_config_value for {engine_name}.{config_key}: {str(e)}"
            )
            # Re-raise for controller layer to handle
            raise

    async def get_kasal_flow_enabled(self) -> bool:
        """
        Get the CrewAI flow enabled status.

        Returns:
            True if flow is enabled (defaults to True if not found or on error)
        """
        try:
            return await self.repository.get_kasal_flow_enabled()
        except Exception as e:
            logger.error(f"Error getting CrewAI flow enabled status: {str(e)}")
            return True  # Default to enabled on error

    async def set_kasal_flow_enabled(self, enabled: bool) -> bool:
        """
        Set the CrewAI flow enabled status.

        Args:
            enabled: Whether flow should be enabled

        Returns:
            True if successful
        """
        try:
            return await self.repository.set_kasal_flow_enabled(enabled)
        except Exception as e:
            logger.error(f"Error setting CrewAI flow enabled status: {str(e)}")
            raise

    async def get_otel_app_telemetry_enabled(self) -> bool:
        """Get the OTel App Telemetry enabled status (system-level).

        Returns:
            True if enabled (defaults to False if not found or on error)
        """
        try:
            return await self.repository.get_otel_app_telemetry_enabled()
        except Exception as e:
            logger.error(f"Error getting OTel App Telemetry status: {str(e)}")
            return False

    async def set_otel_app_telemetry_enabled(self, enabled: bool) -> bool:
        """Set the OTel App Telemetry enabled status (system-level).

        Args:
            enabled: Whether OTel App Telemetry should be enabled

        Returns:
            True if successful
        """
        try:
            return await self.repository.set_otel_app_telemetry_enabled(enabled)
        except Exception as e:
            logger.error(f"Error setting OTel App Telemetry status: {str(e)}")
            raise

    async def get_event_triggers_enabled(self) -> bool:
        """Get the event-trigger feature enabled status (system-level).

        Returns:
            True if enabled (defaults to False if not found or on error).
        """
        try:
            return await self.repository.get_event_triggers_enabled()
        except Exception as e:
            logger.error(f"Error getting event triggers status: {str(e)}")
            return False

    async def set_event_triggers_enabled(self, enabled: bool) -> bool:
        """Set the event-trigger feature enabled status (system-level).

        Args:
            enabled: Whether the event-trigger queue consumer should run.

        Returns:
            True if successful
        """
        try:
            return await self.repository.set_event_triggers_enabled(enabled)
        except Exception as e:
            logger.error(f"Error setting event triggers status: {str(e)}")
            raise

    async def get_otel_app_telemetry_log_level(self) -> str:
        """Get the OTel App Telemetry log level (system-level).

        Returns:
            Log level string (defaults to "INFO" on error)
        """
        try:
            return await self.repository.get_otel_app_telemetry_log_level()
        except Exception as e:
            logger.error(f"Error getting OTel App Telemetry log level: {str(e)}")
            return "INFO"

    async def set_otel_app_telemetry_log_level(self, log_level: str) -> bool:
        """Set the OTel App Telemetry log level (system-level).

        Args:
            log_level: One of DEBUG, INFO, WARNING, ERROR

        Returns:
            True if successful
        """
        try:
            return await self.repository.set_otel_app_telemetry_log_level(log_level)
        except Exception as e:
            logger.error(f"Error setting OTel App Telemetry log level: {str(e)}")
            raise

    async def delete_engine_config(self, engine_name: str) -> bool:
        """
        Delete an engine configuration.

        Args:
            engine_name: Name of the engine to delete

        Returns:
            True if deleted, False if not found
        """
        logger.info(
            f"Service: Attempting to delete engine config with name: {engine_name}"
        )

        # Find the engine config first
        config = await self.repository.find_by_engine_name(engine_name)
        if not config:
            logger.warning(
                f"Engine config with name {engine_name} not found for deletion"
            )
            return False

        # Delete the engine config
        try:
            await self.repository.delete(config.id)
            logger.info(f"Successfully deleted engine config with name {engine_name}")
            return True
        except Exception as e:
            logger.error(
                f"Error deleting engine config with name {engine_name}: {str(e)}"
            )
            raise

    # ------------------------------------------------------------------
    # Which harness runs a job, by default
    # ------------------------------------------------------------------

    async def get_harness(self) -> str:
        """The configured engine, guaranteed to be one this build can name.

        A stored value that is not a known engine is REPORTED and downgraded to
        the default rather than raised: this is read at the start of every
        execution, and a typo in one row must not stop the platform from
        running. It still has to be visible, or the run silently uses an engine
        nobody chose — which is the failure this whole layer exists to avoid.
        """
        from src.services.execution.harnesses import DEFAULT_HARNESS, coerce

        stored = await self.repository.get_harness()
        resolved = coerce(stored)
        if resolved is None:
            logger.warning(
                "Configured harness %r is not a known harness; " "falling back to %s",
                stored,
                DEFAULT_HARNESS.value,
            )
            return DEFAULT_HARNESS.value
        return resolved.value

    async def set_harness(self, harness: str) -> str:
        """Switch the engine every subsequent run starts on.

        Refuses an engine that cannot actually run here — CrewAI not installed,
        say. Storing it anyway would turn one bad configuration change into
        every later execution failing, each with an error about an import rather
        than about the setting that caused it.
        """
        from src.services.execution.harnesses import (
            HarnessUnavailableError,
            binding_for,
            coerce,
        )

        resolved = coerce(harness)
        if resolved is None:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown harness: {harness!r}",
            )
        try:
            binding_for(resolved)
        except HarnessUnavailableError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:  # noqa: BLE001 — surface the real reason
            raise HTTPException(
                status_code=400,
                detail=f"Harness {resolved.value!r} cannot run here: {e}",
            )

        await self.repository.set_harness(resolved.value)
        logger.info("Default harness set to %s", resolved.value)
        return resolved.value

    async def get_harnesses(self) -> Dict[str, Any]:
        """The selection plus every engine's availability and capabilities.

        One call rather than three, because the UI cannot render the choice
        without all of it: a radio button for an engine that is not installed
        needs the REASON next to it, not just a disabled state.
        """
        from src.services.execution.harnesses import describe_harnesses

        return {
            "harness": await self.get_harness(),
            "harnesses": describe_harnesses(),
        }
