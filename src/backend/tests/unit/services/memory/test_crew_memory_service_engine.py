"""
Comprehensive tests for crew_memory_service.py

Tests cover:
- generate_crew_id with group_id isolation
- run_name exclusion from crew_id hash
- create_memory_backends with error handling
"""

import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.memory.crew_memory import CrewMemoryService


class TestGenerateCrewIdGroupIsolation:
    """Tests for crew_id generation with group_id isolation."""

    def test_crew_id_includes_group_id_prefix(self):
        """Test that crew_id is prefixed with group_id for isolation."""
        config = {
            "group_id": "tenant_abc",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Research Task"}],
            "name": "Test Crew",
            "model": "gpt-4",
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id.startswith("tenant_abc_crew_")

    def test_crew_id_uses_default_when_no_group_id(self):
        """Test that crew_id uses 'default' prefix when no group_id provided."""
        config = {
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Research Task"}],
            "name": "Test Crew",
            "model": "gpt-4",
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id.startswith("default_crew_")

    def test_same_config_different_groups_get_different_crew_ids(self):
        """Test that identical configs with different group_ids get different crew_ids."""
        base_config = {
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Research Task"}],
            "name": "Test Crew",
            "model": "gpt-4",
        }

        config_group_a = {**base_config, "group_id": "group_a"}
        config_group_b = {**base_config, "group_id": "group_b"}

        service_a = CrewMemoryService(config_group_a)
        service_b = CrewMemoryService(config_group_b)

        crew_id_a = service_a.generate_crew_id()
        crew_id_b = service_b.generate_crew_id()

        assert crew_id_a != crew_id_b
        assert "group_a" in crew_id_a
        assert "group_b" in crew_id_b

    def test_run_name_not_included_in_hash(self):
        """Test that run_name is NOT included in crew_id hash (memory persists across runs)."""
        config_run1 = {
            "group_id": "test_group",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Research Task"}],
            "name": "Test Crew",
            "model": "gpt-4",
            "run_name": "execution_run_001",  # Different run_name
        }
        config_run2 = {
            "group_id": "test_group",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Research Task"}],
            "name": "Test Crew",
            "model": "gpt-4",
            "run_name": "execution_run_002",  # Different run_name
        }

        service_run1 = CrewMemoryService(config_run1)
        service_run2 = CrewMemoryService(config_run2)

        crew_id_run1 = service_run1.generate_crew_id()
        crew_id_run2 = service_run2.generate_crew_id()

        # Should be SAME because run_name is not part of the hash
        assert crew_id_run1 == crew_id_run2

    def test_provided_crew_id_gets_group_prefix(self):
        """Test that provided crew_id gets group_id prefix if not already present."""
        config = {"group_id": "tenant_xyz", "crew_id": "my_custom_crew_id"}
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id == "tenant_xyz_my_custom_crew_id"

    def test_provided_crew_id_with_existing_prefix_unchanged(self):
        """Test that provided crew_id with correct prefix is not double-prefixed."""
        config = {
            "group_id": "tenant_xyz",
            "crew_id": "tenant_xyz_my_custom_crew_id",  # Already has prefix
        }
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id == "tenant_xyz_my_custom_crew_id"

    def test_database_crew_id_includes_group_prefix(self):
        """Test that database_crew_id path includes group_id prefix."""
        config = {"group_id": "tenant_123", "database_crew_id": "db_crew_456"}
        service = CrewMemoryService(config)

        crew_id = service.generate_crew_id()

        assert crew_id == "tenant_123_crew_db_db_crew_456"

    def test_deterministic_hash_with_same_config(self):
        """Test that same configuration always produces same crew_id."""
        config = {
            "group_id": "test_group",
            "agents": [{"role": "Researcher"}, {"role": "Writer"}],
            "tasks": [{"name": "Task A"}, {"name": "Task B"}],
            "name": "Deterministic Crew",
            "model": "gpt-4",
        }

        # Create service multiple times
        results = []
        for _ in range(5):
            service = CrewMemoryService(config.copy())
            results.append(service.generate_crew_id())

        # All should be identical
        assert all(r == results[0] for r in results)


class TestCrewIdHashComponents:
    """Tests for verifying correct components in crew_id hash."""

    def test_agent_roles_in_hash(self):
        """Test that agent roles affect the crew_id hash."""
        config_agents1 = {
            "group_id": "test",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Task"}],
            "name": "Crew",
            "model": "gpt-4",
        }
        config_agents2 = {
            "group_id": "test",
            "agents": [{"role": "Writer"}],  # Different role
            "tasks": [{"name": "Task"}],
            "name": "Crew",
            "model": "gpt-4",
        }

        id1 = CrewMemoryService(config_agents1).generate_crew_id()
        id2 = CrewMemoryService(config_agents2).generate_crew_id()

        assert id1 != id2

    def test_task_names_in_hash(self):
        """Test that task names affect the crew_id hash."""
        config_tasks1 = {
            "group_id": "test",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Task A"}],
            "name": "Crew",
            "model": "gpt-4",
        }
        config_tasks2 = {
            "group_id": "test",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Task B"}],  # Different task
            "name": "Crew",
            "model": "gpt-4",
        }

        id1 = CrewMemoryService(config_tasks1).generate_crew_id()
        id2 = CrewMemoryService(config_tasks2).generate_crew_id()

        assert id1 != id2

    def test_crew_name_in_hash(self):
        """Test that crew name affects the crew_id hash."""
        config_name1 = {
            "group_id": "test",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Task"}],
            "name": "Crew Alpha",
            "model": "gpt-4",
        }
        config_name2 = {
            "group_id": "test",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Task"}],
            "name": "Crew Beta",  # Different name
            "model": "gpt-4",
        }

        id1 = CrewMemoryService(config_name1).generate_crew_id()
        id2 = CrewMemoryService(config_name2).generate_crew_id()

        assert id1 != id2

    def test_model_in_hash(self):
        """Test that model affects the crew_id hash."""
        config_model1 = {
            "group_id": "test",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Task"}],
            "name": "Crew",
            "model": "gpt-4",
        }
        config_model2 = {
            "group_id": "test",
            "agents": [{"role": "Researcher"}],
            "tasks": [{"name": "Task"}],
            "name": "Crew",
            "model": "gpt-3.5-turbo",  # Different model
        }

        id1 = CrewMemoryService(config_model1).generate_crew_id()
        id2 = CrewMemoryService(config_model2).generate_crew_id()

        assert id1 != id2

    def test_task_description_fallback(self):
        """Test that task description is used as fallback when name is missing."""
        config_with_name = {
            "group_id": "test",
            "agents": [{"role": "Agent"}],
            "tasks": [{"name": "Named Task"}],
            "name": "Crew",
            "model": "gpt-4",
        }
        config_with_desc = {
            "group_id": "test",
            "agents": [{"role": "Agent"}],
            "tasks": [
                {"description": "Named Task"}
            ],  # Using description instead of name
            "name": "Crew",
            "model": "gpt-4",
        }

        # First 50 chars of description should match the name
        id1 = CrewMemoryService(config_with_name).generate_crew_id()
        id2 = CrewMemoryService(config_with_desc).generate_crew_id()

        # Should be same since "Named Task" is used in both cases
        assert id1 == id2


class TestAttachCrewMemoryToAgents:
    """Per-task auto-save reads agent.memory, so the crew Memory must be
    attached to each agent (without recreating a per-agent OpenAI Memory)."""

    def test_attaches_memory_instance_to_all_agents(self):
        service = CrewMemoryService({"group_id": "g"})
        a1, a2 = MagicMock(), MagicMock()
        a1.memory = None
        a2.memory = None
        sentinel_memory = MagicMock(name="crew_memory")
        crew_kwargs = {"memory": sentinel_memory, "agents": [a1, a2]}

        service._attach_crew_memory_to_agents(crew_kwargs)

        assert a1.memory is sentinel_memory
        assert a2.memory is sentinel_memory

    def test_noop_when_memory_disabled_or_default(self):
        service = CrewMemoryService({"group_id": "g"})
        for sentinel in (False, True, None):
            agent = MagicMock()
            agent.memory = None
            service._attach_crew_memory_to_agents(
                {"memory": sentinel, "agents": [agent]}
            )
            # Must NOT attach True/False/None (would create a per-agent default).
            assert agent.memory is None
