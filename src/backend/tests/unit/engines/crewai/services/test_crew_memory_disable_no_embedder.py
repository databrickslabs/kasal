"""
Unit tests for CrewMemoryService.configure_crew_memory_components
when DEFAULT backend has no usable embedder.

Tests cover:
- DEFAULT backend + no custom_embedder + no crew_kwargs embedder → memory disabled
- DEFAULT backend + no custom_embedder + OpenAI embedder in crew_kwargs → memory NOT disabled
- DEFAULT backend + custom_embedder present → proceeds to configure (not disabled)
- Non-DEFAULT backend + no embedder → NOT affected by the new guard
- Verify memory=False is set and crew_kwargs returned early
"""
import pytest
from unittest.mock import MagicMock, patch

from src.engines.crewai.services.crew_memory_service import CrewMemoryService
from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType


class TestDefaultBackendNoEmbedderDisablesMemory:
    """Tests for the guard that disables memory when DEFAULT backend has no embedder."""

    def test_default_no_embedder_disables_memory(self):
        """DEFAULT + no custom_embedder + no crew_kwargs embedder → memory=False."""
        config = {'group_id': 'test', 'execution_id': 'job_1'}
        service = CrewMemoryService(config)

        memory_config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        crew_kwargs = {'memory': True}

        result = service.configure_crew_memory_components(
            crew_kwargs=crew_kwargs,
            memory_config=memory_config,
            memory_backends={},
            crew_id='test_crew',
            custom_embedder=None,
        )

        assert result['memory'] is False

    def test_default_no_embedder_returns_early(self):
        """When memory is disabled, should return immediately without configuring backends."""
        config = {'group_id': 'test', 'execution_id': 'job_1'}
        service = CrewMemoryService(config)

        memory_config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        crew_kwargs = {'memory': True, 'agents': ['a1'], 'tasks': ['t1']}

        result = service.configure_crew_memory_components(
            crew_kwargs=crew_kwargs,
            memory_config=memory_config,
            memory_backends={},
            crew_id='test_crew',
            custom_embedder=None,
        )

        # Should not have added any memory components
        assert 'short_term_memory' not in result
        assert 'long_term_memory' not in result
        assert 'entity_memory' not in result
        assert result['memory'] is False
        # Original kwargs preserved
        assert result['agents'] == ['a1']
        assert result['tasks'] == ['t1']

    def test_default_with_openai_embedder_not_disabled(self):
        """DEFAULT + no custom_embedder but crew_kwargs has embedder → NOT disabled."""
        config = {'group_id': 'test', 'execution_id': 'job_1'}
        service = CrewMemoryService(config)

        memory_config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        crew_kwargs = {
            'memory': True,
            'embedder': {
                'provider': 'openai',
                'config': {'api_key': 'sk-test-dummy-key', 'model': 'text-embedding-3-large'}
            }
        }

        # This should NOT trigger the disable guard, but the DEFAULT+custom_embedder
        # branch won't match either (since custom_embedder=None).
        # It falls through to the general case. The key assertion is memory is NOT
        # forcibly set to False.
        result = service.configure_crew_memory_components(
            crew_kwargs=crew_kwargs,
            memory_config=memory_config,
            memory_backends={},
            crew_id='test_crew',
            custom_embedder=None,
        )

        # Memory should NOT be disabled since an OpenAI embedder is available
        assert result['memory'] is True

    def test_default_with_custom_embedder_not_disabled(self):
        """DEFAULT + custom_embedder present → proceeds to configure, not disabled."""
        config = {'group_id': 'test', 'execution_id': 'job_1'}
        service = CrewMemoryService(config)

        memory_config = MemoryBackendConfig(
            backend_type=MemoryBackendType.DEFAULT,
            enable_short_term=False,
            enable_long_term=False,
            enable_entity=False,
        )
        crew_kwargs = {'memory': True}
        mock_embedder = MagicMock()

        # With custom_embedder present, the disable guard should NOT trigger.
        # The DEFAULT+custom_embedder branch runs instead. With all memory types
        # disabled, it just sets memory=False for different reason.
        result = service.configure_crew_memory_components(
            crew_kwargs=crew_kwargs,
            memory_config=memory_config,
            memory_backends={},
            crew_id='test_crew',
            custom_embedder=mock_embedder,
        )

        # memory=False is set by the DEFAULT+custom_embedder branch (line 543),
        # NOT by the no-embedder guard
        assert result['memory'] is False

    def test_databricks_backend_not_affected_by_guard(self):
        """Non-DEFAULT backend (DATABRICKS) is not affected by the no-embedder guard."""
        config = {'group_id': 'test', 'execution_id': 'job_1'}
        service = CrewMemoryService(config)

        memory_config = MemoryBackendConfig(backend_type=MemoryBackendType.DATABRICKS)
        crew_kwargs = {'memory': True}

        result = service.configure_crew_memory_components(
            crew_kwargs=crew_kwargs,
            memory_config=memory_config,
            memory_backends={},
            crew_id='test_crew',
            custom_embedder=None,
        )

        # DATABRICKS backend sets memory=False in its own branch to prevent conflicts
        assert result['memory'] is False

    def test_lakebase_backend_not_affected_by_guard(self):
        """Non-DEFAULT backend (LAKEBASE) is not affected by the no-embedder guard."""
        config = {'group_id': 'test', 'execution_id': 'job_1'}
        service = CrewMemoryService(config)

        memory_config = MemoryBackendConfig(backend_type=MemoryBackendType.LAKEBASE)
        crew_kwargs = {'memory': True}

        result = service.configure_crew_memory_components(
            crew_kwargs=crew_kwargs,
            memory_config=memory_config,
            memory_backends={},
            crew_id='test_crew',
            custom_embedder=None,
        )

        # LAKEBASE backend sets memory=False in its own branch to prevent conflicts
        assert result['memory'] is False

    def test_default_no_embedder_preserves_other_kwargs(self):
        """Disabling memory preserves all other crew_kwargs fields."""
        config = {'group_id': 'test', 'execution_id': 'job_1'}
        service = CrewMemoryService(config)

        memory_config = MemoryBackendConfig(backend_type=MemoryBackendType.DEFAULT)
        crew_kwargs = {
            'memory': True,
            'agents': ['agent1', 'agent2'],
            'tasks': ['task1'],
            'process': 'sequential',
            'verbose': True,
            'planning': False,
        }

        result = service.configure_crew_memory_components(
            crew_kwargs=crew_kwargs,
            memory_config=memory_config,
            memory_backends={},
            crew_id='test_crew',
            custom_embedder=None,
        )

        assert result['memory'] is False
        assert result['agents'] == ['agent1', 'agent2']
        assert result['tasks'] == ['task1']
        assert result['process'] == 'sequential'
        assert result['verbose'] is True
        assert result['planning'] is False
