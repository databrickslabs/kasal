"""
Manager configuration builder for hierarchical crew processes.

Handles manager LLM and manager agent configuration for hierarchical processes.
"""

from typing import Dict, Any, Optional
import logging
from src.services.execution.runtime import Process

from src.core.logger import LoggerManager
from src.services.llm.manager import LLMManager
from src.services.agent_builder.agent_adapter import create_agent

logger = LoggerManager.get_instance().crew


class ManagerConfigBuilder:
    """Handles manager LLM/agent configuration for hierarchical processes"""

    def __init__(
        self,
        config: Dict[str, Any],
        tool_service=None,
        tool_factory=None,
        user_token: Optional[str] = None
    ):
        """
        Initialize the ManagerConfigBuilder

        Args:
            config: Crew configuration dictionary
            tool_service: Tool service instance
            tool_factory: Tool factory instance
            user_token: Optional user access token for OBO authentication
        """
        self.config = config
        self.tool_service = tool_service
        self.tool_factory = tool_factory
        self.user_token = user_token

    async def configure_manager(
        self,
        crew_kwargs: Dict[str, Any],
        process_type: Process
    ) -> Dict[str, Any]:
        """
        Configure manager LLM or agent for the crew

        Args:
            crew_kwargs: Crew keyword arguments to update
            process_type: Process type (hierarchical or sequential)

        Returns:
            Updated crew_kwargs
        """
        crew_config = self.config.get('crew', {})
        requested_model = self.config.get('model') or crew_config.get('model')

        try:
            if process_type == Process.hierarchical:
                crew_kwargs = await self._configure_hierarchical_manager(crew_kwargs, crew_config, requested_model)
            # A sequential crew has no manager. The old sequential branch only set
            # manager_llm when crew planning was on (to keep the prose planner off
            # OpenAI); the planner is gone, so there is nothing left to configure.

        except ImportError:
            logger.warning("Enhanced Databricks auth not available for crew preparation")
        except Exception as e:
            logger.warning(f"Error configuring crew manager LLM: {e}")

        return crew_kwargs

    async def _configure_hierarchical_manager(
        self,
        crew_kwargs: Dict[str, Any],
        crew_config: Dict[str, Any],
        requested_model: Optional[str]
    ) -> Dict[str, Any]:
        """Configure manager for hierarchical process"""
        group_id = self.config.get('group_id')

        # Check for manager_agent first
        if 'manager_agent' in crew_config and crew_config['manager_agent']:
            manager_config = crew_config['manager_agent']
            if isinstance(manager_config, dict):
                logger.info("Creating custom manager agent from configuration")
                agent_llm = None
                if requested_model and group_id:
                    try:
                        agent_llm = await LLMManager.configure_kasal_llm(requested_model, group_id)
                    except Exception:
                        pass
                manager_agent = await create_agent(
                    agent_config=manager_config,
                    llm=agent_llm,
                    tool_service=self.tool_service,
                    tool_factory=self.tool_factory,
                    user_token=self.user_token
                )
                crew_kwargs['manager_agent'] = manager_agent
                logger.info("Set custom manager agent for hierarchical process")

        # Check for manager_llm
        elif 'manager_llm' not in crew_config:
            logger.info("Hierarchical process detected - setting manager_llm")
            if requested_model and group_id:
                try:
                    manager_llm = await LLMManager.configure_kasal_llm(requested_model, group_id)
                    crew_kwargs['manager_llm'] = manager_llm
                    logger.info(f"Set manager LLM for hierarchical process: {requested_model}")
                except Exception as llm_error:
                    logger.warning(f"Could not create manager LLM: {llm_error}")
                    # Use fallback
                    crew_kwargs = await self._set_fallback_manager_llm(crew_kwargs)
            else:
                logger.warning("Hierarchical process requires manager_llm or manager_agent - using fallback")
                crew_kwargs = await self._set_fallback_manager_llm(crew_kwargs)

        else:
            # manager_llm already in crew_config
            provided_manager_llm = crew_config['manager_llm']

            # Check if it's a string that needs conversion
            if isinstance(provided_manager_llm, str):
                logger.info(f"Converting manager_llm string '{provided_manager_llm}' to LLM object")
                if not group_id:
                    logger.error("Cannot convert manager_llm string: no group_id in config")
                    crew_kwargs = await self._set_fallback_manager_llm(crew_kwargs)
                else:
                    try:
                        crew_kwargs['manager_llm'] = await LLMManager.configure_kasal_llm(provided_manager_llm, group_id)
                        logger.info(f"Successfully converted manager_llm '{provided_manager_llm}' to LLM object")
                    except Exception as llm_error:
                        logger.error(f"Failed to convert manager_llm string to LLM object: {llm_error}")

                        # Try with databricks/ prefix
                        if 'databricks' in provided_manager_llm.lower() and not provided_manager_llm.startswith('databricks/'):
                            prefixed_model = f"databricks/{provided_manager_llm}"
                            logger.info(f"Retrying with databricks/ prefix: {prefixed_model}")
                            try:
                                crew_kwargs['manager_llm'] = await LLMManager.configure_kasal_llm(prefixed_model, group_id)
                                logger.info(f"Successfully created manager_llm with prefix: {prefixed_model}")
                            except Exception as retry_error:
                                logger.error(f"Failed to create manager_llm even with prefix: {retry_error}")
                                crew_kwargs = await self._set_fallback_manager_llm(crew_kwargs)
                        else:
                            crew_kwargs = await self._set_fallback_manager_llm(crew_kwargs)
            else:
                # Already an LLM object
                crew_kwargs['manager_llm'] = provided_manager_llm
                logger.info("Using provided manager_llm object for hierarchical process")

        return crew_kwargs

    async def _set_fallback_manager_llm(self, crew_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Set fallback manager LLM"""
        try:
            group_id = self.config.get('group_id')
            if not group_id:
                logger.warning("No group_id in config, cannot set fallback manager LLM")
                return crew_kwargs

            fallback_llm = await LLMManager.configure_kasal_llm("databricks-llama-4-maverick", group_id)
            crew_kwargs['manager_llm'] = fallback_llm
            logger.info("Using fallback Databricks model for manager_llm")
        except Exception as e:
            logger.warning(f"Could not set fallback manager LLM: {e}")

        return crew_kwargs
