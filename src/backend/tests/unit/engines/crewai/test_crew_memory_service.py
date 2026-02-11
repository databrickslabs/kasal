"""
Comprehensive tests for crew_memory_service.py

Tests cover:
- generate_crew_id with group_id isolation
- run_name exclusion from crew_id hash
- create_memory_backends with error handling
- _emit_index_validation_trace method
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import hashlib
import json

from src.engines.crewai.services.crew_memory_service import CrewMemoryService
from src.engines.crewai.memory.memory_backend_factory import DatabricksIndexValidationError


class TestGenerateCrewIdGroupIsolation:
    """Tests for crew_id generation with group_id isolation."""

    def test_crew_id_includes_group_id_prefix(self):
        """Test that crew_id is prefixed with group_id for isolation."""
        config = {
            'group_id': 'tenant_abc',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Research Task'}],
            'name': 'Test Crew',
            'model': 'gpt-4'
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id.startswith('tenant_abc_crew_')

    def test_crew_id_uses_default_when_no_group_id(self):
        """Test that crew_id uses 'default' prefix when no group_id provided."""
        config = {
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Research Task'}],
            'name': 'Test Crew',
            'model': 'gpt-4'
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id.startswith('default_crew_')

    def test_same_config_different_groups_get_different_crew_ids(self):
        """Test that identical configs with different group_ids get different crew_ids."""
        base_config = {
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Research Task'}],
            'name': 'Test Crew',
            'model': 'gpt-4'
        }

        config_group_a = {**base_config, 'group_id': 'group_a'}
        config_group_b = {**base_config, 'group_id': 'group_b'}

        service_a = CrewMemoryService(config_group_a)
        service_b = CrewMemoryService(config_group_b)

        crew_id_a = service_a.generate_crew_id()
        crew_id_b = service_b.generate_crew_id()

        assert crew_id_a != crew_id_b
        assert 'group_a' in crew_id_a
        assert 'group_b' in crew_id_b

    def test_run_name_not_included_in_hash(self):
        """Test that run_name is NOT included in crew_id hash (memory persists across runs)."""
        config_run1 = {
            'group_id': 'test_group',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Research Task'}],
            'name': 'Test Crew',
            'model': 'gpt-4',
            'run_name': 'execution_run_001'  # Different run_name
        }
        config_run2 = {
            'group_id': 'test_group',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Research Task'}],
            'name': 'Test Crew',
            'model': 'gpt-4',
            'run_name': 'execution_run_002'  # Different run_name
        }

        service_run1 = CrewMemoryService(config_run1)
        service_run2 = CrewMemoryService(config_run2)

        crew_id_run1 = service_run1.generate_crew_id()
        crew_id_run2 = service_run2.generate_crew_id()

        # Should be SAME because run_name is not part of the hash
        assert crew_id_run1 == crew_id_run2

    def test_provided_crew_id_gets_group_prefix(self):
        """Test that provided crew_id gets group_id prefix if not already present."""
        config = {
            'group_id': 'tenant_xyz',
            'crew_id': 'my_custom_crew_id'
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id == 'tenant_xyz_my_custom_crew_id'

    def test_provided_crew_id_with_existing_prefix_unchanged(self):
        """Test that provided crew_id with correct prefix is not double-prefixed."""
        config = {
            'group_id': 'tenant_xyz',
            'crew_id': 'tenant_xyz_my_custom_crew_id'  # Already has prefix
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id == 'tenant_xyz_my_custom_crew_id'

    def test_database_crew_id_includes_group_prefix(self):
        """Test that database_crew_id path includes group_id prefix."""
        config = {
            'group_id': 'tenant_123',
            'database_crew_id': 'db_crew_456'
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id == 'tenant_123_crew_db_db_crew_456'

    def test_deterministic_hash_with_same_config(self):
        """Test that same configuration always produces same crew_id."""
        config = {
            'group_id': 'test_group',
            'agents': [
                {'role': 'Researcher'},
                {'role': 'Writer'}
            ],
            'tasks': [
                {'name': 'Task A'},
                {'name': 'Task B'}
            ],
            'name': 'Deterministic Crew',
            'model': 'gpt-4'
        }

        # Create service multiple times
        results = []
        for _ in range(5):
            service = CrewMemoryService(config.copy())
            results.append(service.generate_crew_id())

        # All should be identical
        assert all(r == results[0] for r in results)


class TestCreateMemoryBackendsErrorHandling:
    """Tests for create_memory_backends error handling and trace emission."""

    @pytest.mark.asyncio
    async def test_create_memory_backends_catches_validation_error(self):
        """Test that DatabricksIndexValidationError is caught and trace is emitted."""
        config = {
            'execution_id': 'job_123',
            'group_id': 'test_group'
        }
        service = CrewMemoryService(config)

        validation_result = {
            "valid": False,
            "missing_indexes": ["short_term: catalog.schema.stm_index"],
            "provisioning_indexes": [],
            "error_type": "missing_indexes"
        }

        with patch.object(
            service, '_emit_index_validation_trace', new_callable=AsyncMock
        ) as mock_emit:
            with patch(
                'src.engines.crewai.services.crew_memory_service.MemoryBackendFactory.create_memory_backends',
                new_callable=AsyncMock
            ) as mock_factory:
                mock_factory.side_effect = DatabricksIndexValidationError(
                    "Test error", validation_result
                )

                with pytest.raises(DatabricksIndexValidationError):
                    await service.create_memory_backends(
                        memory_backend_config={'backend_type': 'databricks'},
                        crew_id='test_crew',
                        embedder=None
                    )

                # Verify trace was emitted
                mock_emit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_memory_backends_success_no_trace(self):
        """Test that successful creation doesn't emit error trace."""
        config = {
            'execution_id': 'job_123',
            'group_id': 'test_group'
        }
        service = CrewMemoryService(config)

        with patch.object(
            service, '_emit_index_validation_trace', new_callable=AsyncMock
        ) as mock_emit:
            with patch(
                'src.engines.crewai.services.crew_memory_service.MemoryBackendFactory.create_memory_backends',
                new_callable=AsyncMock,
                return_value={'short_term': MagicMock()}
            ):
                result = await service.create_memory_backends(
                    memory_backend_config={'backend_type': 'default'},
                    crew_id='test_crew',
                    embedder=None
                )

                # Verify no error trace was emitted
                mock_emit.assert_not_called()
                assert 'short_term' in result


class TestEmitIndexValidationTrace:
    """Tests for _emit_index_validation_trace method."""

    @pytest.mark.asyncio
    async def test_emit_trace_for_missing_indexes(self):
        """Test trace emission for missing indexes error."""
        config = {
            'execution_id': 'job_456',
            'group_id': 'tenant_abc'
        }
        service = CrewMemoryService(config)

        validation_result = {
            "valid": False,
            "missing_indexes": ["short_term: catalog.schema.stm_index"],
            "provisioning_indexes": [],
            "error_type": "missing_indexes"
        }
        error = DatabricksIndexValidationError("Missing indexes", validation_result)

        # Patch at the source location since imports happen inside the method
        with patch('src.db.session.request_scoped_session') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session.return_value.__aenter__.return_value = mock_session_instance

            with patch('src.services.execution_trace_service.ExecutionTraceService') as MockTraceService:
                mock_trace_service = MagicMock()
                mock_trace_service.create_trace = AsyncMock()
                MockTraceService.return_value = mock_trace_service

                await service._emit_index_validation_trace(error)

                # Verify create_trace was called
                mock_trace_service.create_trace.assert_called_once()

                # Verify trace data
                trace_data = mock_trace_service.create_trace.call_args[0][0]
                assert trace_data['job_id'] == 'job_456'
                assert trace_data['event_type'] == 'memory_backend_error'
                assert trace_data['event_source'] == 'Memory Backend'
                assert trace_data['group_id'] == 'tenant_abc'
                assert 'Indexes Not Found' in trace_data['trace_metadata']['title']

    @pytest.mark.asyncio
    async def test_emit_trace_for_provisioning_indexes(self):
        """Test trace emission for provisioning indexes error."""
        config = {
            'execution_id': 'job_789',
            'group_id': 'tenant_xyz'
        }
        service = CrewMemoryService(config)

        validation_result = {
            "valid": False,
            "missing_indexes": [],
            "provisioning_indexes": ["entity: catalog.schema.entity (state: PROVISIONING)"],
            "error_type": "provisioning_indexes"
        }
        error = DatabricksIndexValidationError("Provisioning", validation_result)

        # Patch at the source location since imports happen inside the method
        with patch('src.db.session.request_scoped_session') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session.return_value.__aenter__.return_value = mock_session_instance

            with patch('src.services.execution_trace_service.ExecutionTraceService') as MockTraceService:
                mock_trace_service = MagicMock()
                mock_trace_service.create_trace = AsyncMock()
                MockTraceService.return_value = mock_trace_service

                await service._emit_index_validation_trace(error)

                # Verify trace data
                trace_data = mock_trace_service.create_trace.call_args[0][0]
                assert 'Still Provisioning' in trace_data['trace_metadata']['title']
                assert 'PROVISIONING' in trace_data['output']['content']

    @pytest.mark.asyncio
    async def test_emit_trace_skipped_when_no_job_id(self):
        """Test trace emission is skipped when no job_id is available."""
        config = {
            # No execution_id or job_id
            'group_id': 'tenant_abc'
        }
        service = CrewMemoryService(config)

        validation_result = {
            "valid": False,
            "missing_indexes": ["test_index"],
            "error_type": "missing_indexes"
        }
        error = DatabricksIndexValidationError("Missing", validation_result)

        # Patch at the source location since imports happen inside the method
        with patch('src.db.session.request_scoped_session') as mock_session:
            with patch('src.services.execution_trace_service.ExecutionTraceService') as MockTraceService:
                await service._emit_index_validation_trace(error)

                # Session should not be called when no job_id
                mock_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_emit_trace_handles_exception_gracefully(self):
        """Test that trace emission failures don't crash the process."""
        config = {
            'execution_id': 'job_123',
            'group_id': 'tenant_abc'
        }
        service = CrewMemoryService(config)

        validation_result = {
            "valid": False,
            "missing_indexes": ["test_index"],
            "error_type": "missing_indexes"
        }
        error = DatabricksIndexValidationError("Missing", validation_result)

        # Patch at the source location since imports happen inside the method
        with patch('src.db.session.request_scoped_session') as mock_session:
            mock_session.return_value.__aenter__.side_effect = Exception("DB connection failed")

            # Should not raise - just log warning
            await service._emit_index_validation_trace(error)

    @pytest.mark.asyncio
    async def test_emit_trace_uses_job_id_fallback(self):
        """Test that job_id is used as fallback when execution_id is not present."""
        config = {
            'job_id': 'fallback_job_id',  # Using job_id instead of execution_id
            'group_id': 'tenant_abc'
        }
        service = CrewMemoryService(config)

        validation_result = {
            "valid": False,
            "missing_indexes": ["test_index"],
            "error_type": "missing_indexes"
        }
        error = DatabricksIndexValidationError("Missing", validation_result)

        # Patch at the source location since imports happen inside the method
        with patch('src.db.session.request_scoped_session') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session.return_value.__aenter__.return_value = mock_session_instance

            with patch('src.services.execution_trace_service.ExecutionTraceService') as MockTraceService:
                mock_trace_service = MagicMock()
                mock_trace_service.create_trace = AsyncMock()
                MockTraceService.return_value = mock_trace_service

                await service._emit_index_validation_trace(error)

                trace_data = mock_trace_service.create_trace.call_args[0][0]
                assert trace_data['job_id'] == 'fallback_job_id'


class TestCrewIdHashComponents:
    """Tests for verifying correct components in crew_id hash."""

    def test_agent_roles_in_hash(self):
        """Test that agent roles affect the crew_id hash."""
        config_agents1 = {
            'group_id': 'test',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Task'}],
            'name': 'Crew',
            'model': 'gpt-4'
        }
        config_agents2 = {
            'group_id': 'test',
            'agents': [{'role': 'Writer'}],  # Different role
            'tasks': [{'name': 'Task'}],
            'name': 'Crew',
            'model': 'gpt-4'
        }

        id1 = CrewMemoryService(config_agents1).generate_crew_id()
        id2 = CrewMemoryService(config_agents2).generate_crew_id()

        assert id1 != id2

    def test_task_names_in_hash(self):
        """Test that task names affect the crew_id hash."""
        config_tasks1 = {
            'group_id': 'test',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Task A'}],
            'name': 'Crew',
            'model': 'gpt-4'
        }
        config_tasks2 = {
            'group_id': 'test',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Task B'}],  # Different task
            'name': 'Crew',
            'model': 'gpt-4'
        }

        id1 = CrewMemoryService(config_tasks1).generate_crew_id()
        id2 = CrewMemoryService(config_tasks2).generate_crew_id()

        assert id1 != id2

    def test_crew_name_in_hash(self):
        """Test that crew name affects the crew_id hash."""
        config_name1 = {
            'group_id': 'test',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Task'}],
            'name': 'Crew Alpha',
            'model': 'gpt-4'
        }
        config_name2 = {
            'group_id': 'test',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Task'}],
            'name': 'Crew Beta',  # Different name
            'model': 'gpt-4'
        }

        id1 = CrewMemoryService(config_name1).generate_crew_id()
        id2 = CrewMemoryService(config_name2).generate_crew_id()

        assert id1 != id2

    def test_model_in_hash(self):
        """Test that model affects the crew_id hash."""
        config_model1 = {
            'group_id': 'test',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Task'}],
            'name': 'Crew',
            'model': 'gpt-4'
        }
        config_model2 = {
            'group_id': 'test',
            'agents': [{'role': 'Researcher'}],
            'tasks': [{'name': 'Task'}],
            'name': 'Crew',
            'model': 'gpt-3.5-turbo'  # Different model
        }

        id1 = CrewMemoryService(config_model1).generate_crew_id()
        id2 = CrewMemoryService(config_model2).generate_crew_id()

        assert id1 != id2

    def test_task_description_fallback(self):
        """Test that task description is used as fallback when name is missing."""
        config_with_name = {
            'group_id': 'test',
            'agents': [{'role': 'Agent'}],
            'tasks': [{'name': 'Named Task'}],
            'name': 'Crew',
            'model': 'gpt-4'
        }
        config_with_desc = {
            'group_id': 'test',
            'agents': [{'role': 'Agent'}],
            'tasks': [{'description': 'Named Task'}],  # Using description instead of name
            'name': 'Crew',
            'model': 'gpt-4'
        }

        # First 50 chars of description should match the name
        id1 = CrewMemoryService(config_with_name).generate_crew_id()
        id2 = CrewMemoryService(config_with_desc).generate_crew_id()

        # Should be same since "Named Task" is used in both cases
        assert id1 == id2
