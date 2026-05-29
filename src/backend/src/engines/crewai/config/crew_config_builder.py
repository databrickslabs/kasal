"""
Crew configuration builder for assembling crew kwargs.

Handles:
- Process type determination
- Crew memory configuration
- Base crew kwargs assembly
- Optional parameters
"""

from typing import Dict, Any, List, Optional
import logging
from crewai import Process

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew


class CrewConfigBuilder:
    """Handles crew configuration assembly"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the CrewConfigBuilder

        Args:
            config: Crew configuration dictionary
        """
        self.config = config

    def determine_crew_memory_setting(self) -> bool:
        """
        Determine if crew memory should be enabled based on agent settings

        Returns:
            Boolean indicating if crew memory should be enabled
        """
        agents_with_memory_enabled = []
        agents_with_memory_disabled = []

        for agent_config in self.config.get('agents', []):
            agent_name = agent_config.get('name', agent_config.get('role', 'Unknown'))

            if 'memory' in agent_config:
                if agent_config['memory'] is False:
                    agents_with_memory_disabled.append(agent_name)
                    logger.info(f"Agent '{agent_name}' has memory explicitly disabled")
                else:
                    agents_with_memory_enabled.append(agent_name)
                    logger.info(f"Agent '{agent_name}' has memory enabled")
            else:
                # Default to True if not specified
                agents_with_memory_enabled.append(agent_name)
                logger.info(f"Agent '{agent_name}' has memory enabled (default)")

        # If ALL agents have memory disabled, disable crew memory
        if agents_with_memory_disabled and not agents_with_memory_enabled:
            default_crew_memory = False
            logger.info("All agents have memory disabled - setting crew memory to False")
        else:
            # At least one agent has memory enabled
            crew_config = self.config.get('crew', {})
            default_crew_memory = crew_config.get('memory', True)
            if agents_with_memory_enabled:
                logger.info(f"At least one agent has memory enabled - using crew memory setting: {default_crew_memory}")
            else:
                logger.info(f"No agent memory settings found - using crew memory default: {default_crew_memory}")

        return default_crew_memory

    def determine_process_type(self) -> Process:
        """
        Determine process type from configuration

        Returns:
            Process enum value
        """
        crew_config = self.config.get('crew', {})
        process_type = crew_config.get('process', 'sequential')

        if isinstance(process_type, str):
            if process_type.lower() == 'hierarchical':
                process_type = Process.hierarchical
                logger.info("Using hierarchical process for crew")
            else:
                process_type = Process.sequential
                logger.info("Using sequential process for crew")

        return process_type

    def build_base_crew_kwargs(
        self,
        agents: List[Any],
        tasks: List[Any],
        process_type: Process,
        default_crew_memory: bool
    ) -> Dict[str, Any]:
        """
        Build base crew kwargs

        Args:
            agents: List of agent instances
            tasks: List of task instances
            process_type: Process type enum
            default_crew_memory: Memory enabled flag

        Returns:
            Base crew kwargs dictionary
        """
        crew_kwargs = {
            'agents': agents,
            'tasks': tasks,
            'process': process_type,
            'verbose': True,
            'memory': default_crew_memory,
            'prompt_to_print_output': False  # CRITICAL: Disable interactive trace prompt to prevent subprocess hang
            # Note: 'tracing' parameter removed - not supported in all CrewAI versions
            # CrewAI cloud tracing is disabled by default anyway
        }

        return crew_kwargs

    def add_optional_parameters(self, crew_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add optional parameters from configuration

        Args:
            crew_kwargs: Base crew kwargs

        Returns:
            Updated crew kwargs
        """
        crew_config = self.config.get('crew', {})

        # Max RPM
        if 'max_rpm' in self.config:
            crew_kwargs['max_rpm'] = self.config['max_rpm']

        # Planning
        if 'planning' in crew_config:
            crew_kwargs['planning'] = crew_config['planning']

        # NOTE: 'reasoning' is an Agent-level parameter in CrewAI, NOT a Crew-level parameter
        # The reasoning config is propagated to agents in CrewPreparation._create_agents()
        # Do NOT add 'reasoning' to crew_kwargs as Crew doesn't accept this parameter

        return crew_kwargs

    async def add_llm_parameters(self, crew_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add planning_llm if specified

        Args:
            crew_kwargs: Base crew kwargs

        Returns:
            Updated crew kwargs

        Note:
            'reasoning_llm' is NOT a valid CrewAI Crew parameter. In CrewAI, reasoning
            is an Agent-level feature that uses the agent's own LLM for reasoning.
            The agent's reasoning capability is enabled via Agent(reasoning=True).
        """
        from src.core.llm_manager import LLMManager

        crew_config = self.config.get('crew', {})

        # Planning LLM
        group_id = self.config.get('group_id')
        if 'planning_llm' in crew_config:
            try:
                if group_id:
                    planning_llm = await LLMManager.configure_crewai_llm(crew_config['planning_llm'], group_id)
                else:
                    planning_llm = await LLMManager.get_llm(crew_config['planning_llm'])
                crew_kwargs['planning_llm'] = planning_llm
                logger.info(f"Set crew planning LLM to: {crew_config['planning_llm']}")
            except Exception as llm_error:
                logger.warning(f"Could not create planning LLM for model {crew_config['planning_llm']}: {llm_error}")
        elif crew_config.get('planning', False):
            # If planning is enabled but no planning_llm specified, use the default model
            default_model = self.config.get('model')
            if default_model and group_id:
                try:
                    planning_llm = await LLMManager.configure_crewai_llm(default_model, group_id)
                    crew_kwargs['planning_llm'] = planning_llm
                    logger.info(f"Set crew planning LLM to default model: {default_model}")
                except Exception as llm_error:
                    logger.warning(f"Could not create default planning LLM for model {default_model}: {llm_error}")

        # NOTE: 'reasoning_llm' is NOT a valid CrewAI parameter
        # In CrewAI, reasoning is enabled per-agent via Agent(reasoning=True)
        # and uses the agent's own LLM for reasoning. There is no separate reasoning_llm.
        if 'reasoning_llm' in crew_config:
            logger.warning(
                f"'reasoning_llm' is not a valid CrewAI Crew parameter. "
                f"Reasoning uses each agent's own LLM. Enable reasoning via the agent's 'reasoning' flag."
            )

        return crew_kwargs

    def disable_memory_completely(self, crew_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Completely disable memory for the crew

        Args:
            crew_kwargs: Crew kwargs to update

        Returns:
            Updated crew kwargs with all memory disabled
        """
        logger.info("=" * 80)
        logger.info("MEMORY COMPLETELY DISABLED FOR THIS CREW")
        logger.info("Reason: All agents are stateless and don't require memory")
        logger.info("Performance benefit: No memory operations will be performed")
        logger.info("=" * 80)

        crew_kwargs['memory'] = False
        # Legacy per-type kwargs (pre-1.10) are popped defensively in case
        # anything upstream still sets them.
        crew_kwargs.pop('short_term_memory', None)
        crew_kwargs.pop('long_term_memory', None)
        crew_kwargs.pop('entity_memory', None)
        crew_kwargs.pop('embedder', None)

        return crew_kwargs

    def check_memory_disabled_by_backend_config(self, memory_backend_config: Optional[Dict[str, Any]]) -> bool:
        """Return True when the loaded config explicitly disables memory.

        With CrewAI 1.10+ unified memory there are no per-type enable flags.
        The crew_memory_service returns ``None`` (or a config with no active
        row) to signal disabled memory — this helper is kept for the API
        layer which may still pass a disabled sentinel. A config with
        ``is_active=False`` or ``backend_type='disabled'`` counts as off.
        """
        if not memory_backend_config:
            return False
        if memory_backend_config.get("is_active") is False:
            logger.info("Memory backend config is inactive — disabling crew memory")
            return True
        if memory_backend_config.get("backend_type") == "disabled":
            logger.info("Memory backend type is 'disabled' — disabling crew memory")
            return True
        return False

    def log_memory_configuration(
        self,
        crew_kwargs: Dict[str, Any],
        memory_backend_config: Optional[Dict[str, Any]]
    ) -> None:
        """
        Log memory configuration before crew creation

        Args:
            crew_kwargs: Crew kwargs
            memory_backend_config: Memory backend configuration
        """
        logger.info("=== MEMORY CONFIGURATION BEFORE CREW CREATION ===")
        memory_value = crew_kwargs.get('memory', False)
        logger.info(f"Memory enabled: {bool(memory_value)}")

        if memory_backend_config:
            logger.info(f"Memory backend: {memory_backend_config.get('backend_type', 'unknown')}")
        else:
            logger.info("Memory backend: Default (CrewAI unified Memory / LanceDB)")

        if memory_value in (True, False, None):
            logger.info(f"Unified Memory: {memory_value}")
        else:
            logger.info(
                f"Unified Memory: {type(memory_value).__name__} "
                f"(storage={type(getattr(memory_value, '_storage', None) or getattr(memory_value, 'storage', None)).__name__})"
            )
        logger.info(f"Embedder: {'configured' if 'embedder' in crew_kwargs else 'not configured'}")

        # Banner for explicit memory backend summary
        try:
            if memory_backend_config:
                backend_type = memory_backend_config.get('backend_type', 'unknown')
                bt = backend_type.lower() if isinstance(backend_type, str) else str(backend_type).lower()

                if bt == 'databricks':
                    dbc = memory_backend_config.get('databricks_config')
                    if hasattr(dbc, 'model_dump') and callable(getattr(dbc, 'model_dump')):
                        dbc = dbc.model_dump()
                    elif hasattr(dbc, 'dict') and callable(getattr(dbc, 'dict')):
                        dbc = dbc.dict()
                    dbc = dbc or {}

                    logger.info("=" * 80)
                    logger.info("MEMORY BACKEND: DATABRICKS VECTOR SEARCH (unified)")
                    logger.info(f"Endpoint: {dbc.get('endpoint_name')} | Workspace: {dbc.get('workspace_url')}")
                    logger.info(f"Unified memory index: {dbc.get('memory_index')}")
                    logger.info("=" * 80)
                elif bt == 'lakebase':
                    lbc = memory_backend_config.get('lakebase_config')
                    if hasattr(lbc, 'model_dump') and callable(getattr(lbc, 'model_dump')):
                        lbc = lbc.model_dump()
                    elif hasattr(lbc, 'dict') and callable(getattr(lbc, 'dict')):
                        lbc = lbc.dict()
                    lbc = lbc or {}
                    logger.info("=" * 80)
                    logger.info("MEMORY BACKEND: LAKEBASE PGVECTOR (unified)")
                    logger.info(f"Instance: {lbc.get('instance_name') or 'default'}")
                    logger.info(f"Unified memory table: {lbc.get('memory_table')}")
                    logger.info("=" * 80)
                else:
                    logger.info("=" * 80)
                    logger.info("MEMORY BACKEND: DEFAULT (CrewAI unified Memory / LanceDB)")
                    logger.info("=" * 80)
            else:
                logger.info("=" * 80)
                logger.info("MEMORY BACKEND: DEFAULT (ChromaDB + SQLite)")
                logger.info("CrewAI will manage local ChromaDB collections for short-term/entity and SQLite for long-term")
                logger.info("=" * 80)
        except Exception as banner_err:
            logger.debug(f"Could not print memory backend banner: {banner_err}")

        # Embedder info
        if 'embedder' in crew_kwargs:
            embedder_info = crew_kwargs['embedder']
            if isinstance(embedder_info, dict):
                logger.info(f"Embedder provider: {embedder_info.get('provider', 'unknown')}")
            else:
                logger.info("Embedder provider: custom Databricks embedder")
                if hasattr(embedder_info, 'model'):
                    logger.info(f"Custom embedder model: {embedder_info.model}")
                    logger.info(f"Expected embedding dimension: 1024")

        logger.info("================================================")
