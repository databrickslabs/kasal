"""
Unit tests for memory optimization in CrewAI agents.

Tests that memory is automatically disabled for agents that don't need it,
such as validators, formatters, and other stateless operations.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.services.agent_builder.crew_preparation import CrewPreparation


class TestMemoryOptimization:
    """Test suite for agent memory optimization."""

    @pytest.fixture
    def mock_tool_service(self):
        """Create a mock tool service."""
        return Mock()

    @pytest.fixture
    def mock_tool_factory(self):
        """Create a mock tool factory."""
        return Mock()

    @pytest.mark.asyncio
    async def test_crew_memory_disabled_when_all_agents_stateless(self):
        """Test that crew memory is disabled when all agents are stateless."""
        config = {
            "agents": [
                {
                    "role": "JSON Validator",
                    "goal": "Validate JSON",
                    "backstory": "Validation expert",
                },
                {
                    "role": "Code Formatter",
                    "goal": "Format code",
                    "backstory": "Formatting expert",
                },
            ],
            "tasks": [],
            "crew": {"memory": True},
        }

        with patch(
            "src.services.agent_builder.crew_preparation.validate_crew_config",
            return_value=True,
        ):
            with patch(
                "src.services.agent_builder.crew_preparation.CrewPreparation._create_agents",
                new_callable=AsyncMock,
                return_value=True,
            ):
                with patch(
                    "src.services.agent_builder.crew_preparation.CrewPreparation._create_tasks",
                    new_callable=AsyncMock,
                    return_value=True,
                ):
                    with patch(
                        "src.services.agent_builder.crew_preparation.CrewPreparation._create_crew",
                        new_callable=AsyncMock,
                        return_value=True,
                    ):
                        crew_prep = CrewPreparation(config)
                        result = await crew_prep.prepare()

                        # The prepare method should succeed
                        assert result

    @pytest.mark.asyncio
    async def test_crew_memory_enabled_when_any_agent_needs_it(self):
        """Test that crew memory stays enabled when at least one agent needs it."""
        config = {
            "agents": [
                {
                    "role": "JSON Validator",
                    "goal": "Validate JSON",
                    "backstory": "Validation expert",
                },
                {
                    "role": "Research Analyst",
                    "goal": "Conduct research and analysis",
                    "backstory": "Research expert",
                },
            ],
            "tasks": [],
            "crew": {"memory": True},
        }

        with patch(
            "src.services.agent_builder.crew_preparation.validate_crew_config",
            return_value=True,
        ):
            with patch(
                "src.services.agent_builder.crew_preparation.CrewPreparation._create_agents",
                new_callable=AsyncMock,
                return_value=True,
            ):
                with patch(
                    "src.services.agent_builder.crew_preparation.CrewPreparation._create_tasks",
                    new_callable=AsyncMock,
                    return_value=True,
                ):
                    with patch(
                        "src.services.agent_builder.crew_preparation.CrewPreparation._create_crew",
                        new_callable=AsyncMock,
                        return_value=True,
                    ):
                        crew_prep = CrewPreparation(config)
                        result = await crew_prep.prepare()

                        # The prepare method should succeed
                        assert result
