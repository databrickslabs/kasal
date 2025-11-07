"""
Crew memory service for handling memory backend configuration and setup.

This service centralizes all memory-related logic including:
- Memory backend configuration fetching
- Crew ID generation
- Storage directory setup
- Memory component configuration
- Memory tracing and context setup
"""

from typing import Dict, Any, Optional
import logging
import hashlib
import json
import os
from pathlib import Path

from src.core.logger import LoggerManager
from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType
from src.engines.crewai.memory.memory_backend_factory import MemoryBackendFactory

logger = LoggerManager.get_instance().crew


class CrewMemoryService:
    """Handles all crew memory backend configuration and setup"""

    def __init__(self, config: Dict[str, Any], user_token: Optional[str] = None):
        """
        Initialize the CrewMemoryService

        Args:
            config: Crew configuration dictionary
            user_token: Optional user access token for OBO authentication
        """
        self.config = config
        self.user_token = user_token
        self._original_storage_dir = None

    async def fetch_memory_backend_config(self) -> Optional[Dict[str, Any]]:
        """
        Fetch memory backend configuration from database

        Returns:
            Memory backend configuration dict or None
        """
        try:
            from src.services.memory_backend_service import MemoryBackendService
            from src.db.session import async_session_factory

            async with async_session_factory() as session:
                service = MemoryBackendService(session)
                group_id = self.config.get('group_id')
                logger.info(f"Fetching memory backend config for group_id: {group_id}")

                active_config = await service.get_active_config(group_id)
                if not active_config:
                    logger.warning("No active memory backend configuration found in database")
                    return None

                logger.info(
                    f"Found active config: backend_type={active_config.backend_type}, "
                    f"enable_short_term={active_config.enable_short_term}, "
                    f"enable_long_term={active_config.enable_long_term}, "
                    f"enable_entity={active_config.enable_entity}"
                )

                # Check if this is a "Disabled Configuration"
                is_disabled_config = (
                    not active_config.enable_short_term and
                    not active_config.enable_long_term and
                    not active_config.enable_entity
                )

                if is_disabled_config:
                    logger.info("Found 'Disabled Configuration' - will use default memory")
                    return None

                # Convert to dict format
                memory_backend_config = {
                    'backend_type': active_config.backend_type.value,
                    'databricks_config': active_config.databricks_config,
                    'enable_short_term': active_config.enable_short_term,
                    'enable_long_term': active_config.enable_long_term,
                    'enable_entity': active_config.enable_entity,
                    'enable_relationship_retrieval': active_config.enable_relationship_retrieval,
                }
                logger.info(f"Loaded memory backend config from database: {memory_backend_config['backend_type']}")
                return memory_backend_config

        except Exception as e:
            logger.warning(f"Failed to load memory backend config from database: {e}")
            return None

    def generate_crew_id(self) -> str:
        """
        Generate a deterministic crew ID based on crew configuration

        Returns:
            Crew ID string
        """
        # Use provided crew_id if available
        if self.config.get('crew_id'):
            return self.config.get('crew_id')

        # Check for database crew_id
        db_crew_id = self.config.get('database_crew_id')
        if db_crew_id:
            crew_id = f"crew_db_{db_crew_id}"
            logger.info(f"Using database crew_id: {crew_id} for consistent memory across runs")
            return crew_id

        # Generate hash-based crew_id from configuration
        agents = self.config.get('agents', [])
        tasks = self.config.get('tasks', [])

        # Create sorted lists for stable hashing
        agent_roles = sorted([agent.get('role', '') for agent in agents if isinstance(agent, dict)])
        task_names = sorted([task.get('name', task.get('description', '')[:50]) for task in tasks if isinstance(task, dict)])

        # Get run_name and group_id
        run_name = self.config.get('run_name') or self.config.get('inputs', {}).get('run_name')
        group_id = self.config.get('group_id')

        if not group_id:
            logger.warning("No group_id found in config - crew memory may not be properly isolated between tenants")

        # Create stable identifier
        crew_identifier = {
            'agent_roles': agent_roles,
            'task_names': task_names,
            'crew_name': self.config.get('name', self.config.get('crew', {}).get('name', 'unnamed_crew')),
            'model': self.config.get('model', 'default'),
            'run_name': run_name,
            'group_id': group_id or 'default'
        }

        # Create hash
        crew_identifier_json = json.dumps(crew_identifier, sort_keys=True)
        crew_hash = hashlib.md5(crew_identifier_json.encode()).hexdigest()[:8]
        crew_id = f"{group_id or 'default'}_crew_{crew_hash}"

        # Detailed logging
        logger.info("=" * 80)
        logger.info("CREW_ID GENERATION - DETAILED DEBUG INFO")
        logger.info("=" * 80)
        logger.info(f"Crew Identifier Components:")
        logger.info(f"  - Agent Roles: {agent_roles}")
        logger.info(f"  - Task Names: {task_names}")
        logger.info(f"  - Crew Name: {crew_identifier['crew_name']}")
        logger.info(f"  - Model: {crew_identifier['model']}")
        logger.info(f"  - Run Name: {run_name}")
        logger.info(f"  - Group ID: {group_id or 'default'}")
        logger.info(f"JSON for hashing (sorted): {crew_identifier_json}")
        logger.info(f"MD5 Hash: {hashlib.md5(crew_identifier_json.encode()).hexdigest()}")
        logger.info(f"Hash (first 8 chars): {crew_hash}")
        logger.info(f"Generated crew_id: {crew_id}")
        logger.info(f"This crew_id will persist across runs with the SAME configuration")
        logger.info("=" * 80)

        return crew_id

    def setup_storage_directory(self, crew_id: str, memory_backend_config: Optional[Dict[str, Any]]) -> None:
        """
        Setup custom storage directory for memory backends

        Args:
            crew_id: Crew identifier
            memory_backend_config: Memory backend configuration
        """
        if not memory_backend_config:
            return

        backend_type = memory_backend_config.get('backend_type')
        if backend_type not in ['databricks', 'default']:
            return

        # Save original value
        self._original_storage_dir = os.environ.get("CREWAI_STORAGE_DIR")

        # Set unique directory name
        if backend_type == 'databricks':
            storage_dirname = f"kasal_databricks_{crew_id}"
        else:
            storage_dirname = f"kasal_default_{crew_id}"

        os.environ["CREWAI_STORAGE_DIR"] = storage_dirname

        # Detailed logging
        logger.info("=" * 80)
        logger.info("STORAGE PATH CONFIGURATION - DETAILED DEBUG INFO")
        logger.info("=" * 80)
        logger.info(f"Backend Type: {backend_type}")
        logger.info(f"CREWAI_STORAGE_DIR environment variable set to: {storage_dirname}")

        # Check storage path
        from crewai.utilities.paths import db_storage_path
        storage_path = Path(db_storage_path())
        logger.info(f"Full storage path resolved by CrewAI: {storage_path.absolute()}")
        logger.info(f"Storage path exists: {storage_path.exists()}")

        if storage_path.exists():
            try:
                contents = list(storage_path.iterdir())
                logger.info(f"Storage directory contains {len(contents)} items:")
                for item in contents[:10]:
                    logger.info(f"  - {item.name} ({'dir' if item.is_dir() else 'file'})")
                if len(contents) > 10:
                    logger.info(f"  ... and {len(contents) - 10} more items")
            except Exception as e:
                logger.warning(f"Could not list storage directory contents: {e}")
            logger.info("Memory will persist from previous runs")
        else:
            logger.info("Creating NEW storage directory - this is the FIRST run with this configuration")
        logger.info("=" * 80)

    async def create_memory_backends(
        self,
        memory_backend_config: Dict[str, Any],
        crew_id: str,
        embedder: Any
    ) -> Dict[str, Any]:
        """
        Create memory backends using the factory

        Args:
            memory_backend_config: Memory backend configuration
            crew_id: Crew identifier
            embedder: Embedder instance or config

        Returns:
            Dictionary of memory backends
        """
        # Convert databricks_config dict to object if needed
        if 'databricks_config' in memory_backend_config and isinstance(memory_backend_config['databricks_config'], dict):
            from src.schemas.memory_backend import DatabricksMemoryConfig
            memory_backend_config['databricks_config'] = DatabricksMemoryConfig(**memory_backend_config['databricks_config'])

        # Create MemoryBackendConfig object
        memory_config = MemoryBackendConfig(**memory_backend_config)

        logger.info(f"Creating memory backends for crew {crew_id} with backend type: {memory_config.backend_type}")

        # Create backends
        memory_backends = await MemoryBackendFactory.create_memory_backends(
            config=memory_config,
            crew_id=crew_id,
            embedder=embedder,
            user_token=self.user_token
        )
        logger.info(f"Created memory backends: {list(memory_backends.keys())}")

        return memory_backends

    def configure_crew_memory_components(
        self,
        crew_kwargs: Dict[str, Any],
        memory_config: MemoryBackendConfig,
        memory_backends: Dict[str, Any],
        crew_id: str,
        custom_embedder: Any = None
    ) -> Dict[str, Any]:
        """
        Configure CrewAI memory components

        Args:
            crew_kwargs: Crew keyword arguments to update
            memory_config: Memory backend configuration
            memory_backends: Created memory backends
            crew_id: Crew identifier
            custom_embedder: Custom embedder instance

        Returns:
            Updated crew_kwargs
        """
        try:
            from crewai.memory import ShortTermMemory, LongTermMemory, EntityMemory
            from crewai.memory.storage.rag_storage import RAGStorage

            # Handle DEFAULT backend with custom embedder
            if memory_config.backend_type == MemoryBackendType.DEFAULT and custom_embedder:
                logger.info("Configuring DEFAULT backend with Databricks custom embedder")

                from crewai.utilities.paths import db_storage_path
                from src.engines.crewai.memory.chromadb_databricks_storage import ChromaDBDatabricksStorage

                storage_path = Path(db_storage_path())

                # Configure short-term memory
                if memory_config.enable_short_term:
                    storage_st = ChromaDBDatabricksStorage(
                        storage_path=storage_path,
                        collection_name=f"{crew_id}_short_term",
                        embedding_function=custom_embedder,
                        memory_type="short_term"
                    )
                    crew_kwargs['short_term_memory'] = ShortTermMemory(storage=storage_st)
                    logger.info("Configured short-term memory with Databricks embedder")

                # Configure entity memory
                if memory_config.enable_entity:
                    storage_entity = ChromaDBDatabricksStorage(
                        storage_path=storage_path,
                        collection_name=f"{crew_id}_entities",
                        embedding_function=custom_embedder,
                        memory_type="entities"
                    )
                    crew_kwargs['entity_memory'] = EntityMemory(storage=storage_entity)
                    logger.info("Configured entity memory with Databricks embedder")

                # Configure long-term memory
                if memory_config.enable_long_term:
                    from crewai.memory.storage.ltm_sqlite_storage import LTMSQLiteStorage
                    ltm_storage = LTMSQLiteStorage()
                    crew_kwargs['long_term_memory'] = LongTermMemory(storage=ltm_storage)
                    logger.info("Configured long-term memory with SQLite")

                crew_kwargs['memory'] = False
                logger.info("Set memory=False for DEFAULT backend with custom Databricks embedder")

            # Configure non-default backends
            elif memory_config.backend_type != MemoryBackendType.DEFAULT:
                # Short-term memory
                if 'short_term' in memory_backends and memory_config.enable_short_term:
                    logger.info(f"Configuring custom short-term memory backend for type: {memory_config.backend_type}")
                    if memory_config.backend_type == MemoryBackendType.DATABRICKS:
                        crew_kwargs['short_term_memory'] = ShortTermMemory(storage=memory_backends['short_term'])
                        logger.info("Successfully configured Databricks short-term memory")
                    else:
                        if crew_kwargs.get('embedder'):
                            rag_storage = RAGStorage(type="short_term", embedder_config=crew_kwargs.get('embedder'))
                            crew_kwargs['short_term_memory'] = ShortTermMemory(storage=rag_storage)
                            logger.info("Successfully configured default short-term memory with RAGStorage")

                # Long-term memory
                if 'long_term' in memory_backends and memory_config.enable_long_term:
                    logger.info("Configuring custom long-term memory backend")
                    crew_kwargs['long_term_memory'] = LongTermMemory(storage=memory_backends['long_term'])
                    logger.info("Successfully configured Databricks long-term memory")

                # Entity memory
                if 'entity' in memory_backends and memory_config.enable_entity:
                    logger.info("Configuring custom entity memory backend")
                    if memory_config.backend_type == MemoryBackendType.DATABRICKS:
                        crew_kwargs['entity_memory'] = EntityMemory(
                            storage=memory_backends['entity'],
                            embedder_config=crew_kwargs.get('embedder')
                        )
                        logger.info("Successfully configured Databricks entity memory")
                    else:
                        if crew_kwargs.get('embedder'):
                            rag_storage = RAGStorage(type="entities", embedder_config=crew_kwargs.get('embedder'))
                            crew_kwargs['entity_memory'] = EntityMemory(storage=rag_storage)
                            logger.info("Successfully configured default entity memory with RAGStorage")

                logger.info(f"Memory backend configuration completed for crew {crew_id}")

                # Set memory=False for Databricks to prevent conflicts
                if memory_config.backend_type == MemoryBackendType.DATABRICKS:
                    crew_kwargs['memory'] = False
                    logger.info("Set memory=False for Databricks backend to prevent conflicts")

        except ImportError as e:
            logger.error(f"Failed to import CrewAI memory classes: {e}")
            logger.warning("Falling back to default memory implementation")
        except Exception as e:
            logger.error(f"Error configuring custom memory backends: {e}")
            logger.warning("Falling back to default memory implementation")

        return crew_kwargs

    def attach_memory_trace_context(self, crew: Any, memory_backend_config: Optional[Dict[str, Any]], crew_kwargs: Dict[str, Any]) -> None:
        """
        Attach execution trace context to memory storages

        Args:
            crew: Crew instance
            memory_backend_config: Memory backend configuration
            crew_kwargs: Crew keyword arguments
        """
        try:
            exec_id = self.config.get('execution_id') or self.config.get('run_name') or self.config.get('inputs', {}).get('run_name')
            grp_id = self.config.get('group_id') or 'default'

            trace_ctx = {
                'job_id': exec_id,
                'group_context': {'primary_group_id': grp_id},
                'execution_id': exec_id
            }

            def set_trace_ctx(mem_obj):
                try:
                    if not mem_obj:
                        return
                    storage = getattr(mem_obj, 'storage', None)
                    if storage is not None and hasattr(storage, 'trace_context'):
                        setattr(storage, 'trace_context', trace_ctx)
                    if hasattr(mem_obj, 'trace_context'):
                        setattr(mem_obj, 'trace_context', trace_ctx)
                except Exception:
                    pass

            # Apply to all memory types
            set_trace_ctx(getattr(crew, '_short_term_memory', None))
            set_trace_ctx(getattr(crew, '_long_term_memory', None))
            set_trace_ctx(getattr(crew, '_entity_memory', None))

            # For DEFAULT backend, monkeypatch save/search to emit traces
            backend_type = (memory_backend_config or {}).get('backend_type') if isinstance(memory_backend_config, dict) else (memory_backend_config.backend_type.value if memory_backend_config else 'default')
            is_default_backend = (not memory_backend_config) or (str(backend_type).lower() == 'default')

            if is_default_backend and crew_kwargs.get('memory', False):
                self._patch_default_memory_tracing(crew, exec_id, grp_id)

        except Exception as trace_ctx_err:
            logger.debug(f"Could not attach memory trace context: {trace_ctx_err}")

    def _patch_default_memory_tracing(self, crew: Any, exec_id: str, grp_id: str) -> None:
        """Patch default memory methods for tracing"""
        from src.services.trace_queue import get_trace_queue

        def patch_memory(mem_attr_name: str):
            mem_obj = getattr(crew, mem_attr_name, None)
            if not mem_obj:
                return

            storage = getattr(mem_obj, 'storage', None)
            target = storage or mem_obj
            mt = 'short_term' if 'short' in mem_attr_name else ('long_term' if 'long' in mem_attr_name else 'entity')

            # Patch save
            if hasattr(target, 'save') and callable(getattr(target, 'save')):
                original_save = target.save
                def traced_save(*args, **kwargs):
                    result = original_save(*args, **kwargs)
                    try:
                        q = get_trace_queue()
                        payload = {'backend': 'default', 'memory_type': mt}
                        if args and isinstance(args[0], str):
                            val = args[0]
                            payload['value_snippet'] = (val[:200] + '...') if len(val) > 200 else val
                        q.put_nowait({
                            'job_id': exec_id,
                            'event_type': 'memory_write',
                            'event_source': f"Memory[{mt}:default]",
                            'event_context': 'chroma/sqlite',
                            'output': payload,
                            'group_context': {'primary_group_id': grp_id}
                        })
                    except Exception:
                        pass
                    return result
                try:
                    setattr(target, 'save', traced_save)
                except Exception:
                    pass

            # Patch search
            if hasattr(target, 'search') and callable(getattr(target, 'search')):
                original_search = target.search
                def traced_search(query, *args, **kwargs):
                    results = original_search(query, *args, **kwargs)
                    try:
                        q = get_trace_queue()
                        def summarize(item):
                            text = None
                            if isinstance(item, dict):
                                text = item.get('content') or item.get('context')
                            if not text:
                                text = str(item)
                            return (text[:200] + '...') if isinstance(text, str) and len(text) > 200 else text
                        summary = [summarize(r) for r in (results or [])][:5] if results else []
                        q.put_nowait({
                            'job_id': exec_id,
                            'event_type': 'memory_retrieval',
                            'event_source': f"Memory[{mt}:default]",
                            'event_context': 'chroma/sqlite',
                            'output': {
                                'backend': 'default',
                                'memory_type': mt,
                                'query': query if isinstance(query, str) else '[embedding]',
                                'results': summary
                            },
                            'group_context': {'primary_group_id': grp_id}
                        })
                    except Exception:
                        pass
                    return results
                try:
                    setattr(target, 'search', traced_search)
                except Exception:
                    pass

        patch_memory('_short_term_memory')
        patch_memory('_long_term_memory')
        patch_memory('_entity_memory')

    def set_crew_reference_on_memory(self, crew: Any) -> None:
        """
        Set crew reference on memory wrappers for proper model attribution

        Args:
            crew: Crew instance
        """
        try:
            # Long-term memory
            if hasattr(crew, '_long_term_memory') and crew._long_term_memory:
                long_term_storage = crew._long_term_memory.storage
                if hasattr(long_term_storage, 'crew'):
                    long_term_storage.crew = crew
                    logger.info("Set crew reference for long-term memory to enable LLM model extraction")

            # Entity memory
            if hasattr(crew, '_entity_memory') and crew._entity_memory:
                entity_storage = crew._entity_memory.storage
                if hasattr(entity_storage, 'set_agent_context') and crew.agents:
                    first_agent = crew.agents[0]
                    entity_storage.set_agent_context(first_agent)
                    logger.info(f"Set agent context for entity memory: {getattr(first_agent, 'role', 'Unknown')}")
                if hasattr(entity_storage, 'crew'):
                    entity_storage.crew = crew
                    logger.info("Set crew reference for entity memory")

            # Short-term memory
            if hasattr(crew, '_short_term_memory') and crew._short_term_memory:
                short_term_storage = crew._short_term_memory.storage
                if hasattr(short_term_storage, 'crew'):
                    short_term_storage.crew = crew
                    logger.info("Set crew reference for short-term memory")

        except Exception as context_error:
            logger.warning(f"Failed to set context on memory backends: {context_error}")

    def restore_storage_directory(self) -> None:
        """Restore original storage directory environment variable"""
        if self._original_storage_dir is not None:
            os.environ["CREWAI_STORAGE_DIR"] = self._original_storage_dir
        elif "CREWAI_STORAGE_DIR" in os.environ:
            del os.environ["CREWAI_STORAGE_DIR"]
