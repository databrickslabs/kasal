"""Unit tests for CrewGeneratorRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

from src.repositories.crew_generator_repository import CrewGeneratorRepository
from src.core.exceptions import KasalError


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()
    session.add = MagicMock()
    session.add_all = MagicMock()
    return session


@pytest.fixture
def repo(mock_session):
    return CrewGeneratorRepository(mock_session)


class TestInit:

    def test_initialization(self, mock_session):
        repo = CrewGeneratorRepository(mock_session)
        assert repo.session == mock_session

    def test_create_instance(self, mock_session):
        repo = CrewGeneratorRepository.create_instance(mock_session)
        assert isinstance(repo, CrewGeneratorRepository)
        assert repo.session == mock_session


class TestSafeGetAttr:

    def test_dict_access(self, repo):
        assert repo._safe_get_attr({"key": "value"}, "key") == "value"

    def test_dict_missing_key(self, repo):
        assert repo._safe_get_attr({"key": "value"}, "missing", "default") == "default"

    def test_object_access(self, repo):
        obj = MagicMock(name="test")
        obj.attr = "value"
        assert repo._safe_get_attr(obj, "attr") == "value"

    def test_none_default(self, repo):
        assert repo._safe_get_attr({}, "key") is None


class TestCreate:

    @pytest.mark.asyncio
    async def test_creates_entity(self, repo, mock_session):
        entity = MagicMock()

        result = await repo.create(entity)

        mock_session.add.assert_called_once_with(entity)
        mock_session.flush.assert_called_once()
        mock_session.refresh.assert_called_once_with(entity)
        assert result == entity

    @pytest.mark.asyncio
    async def test_raises_on_error(self, repo, mock_session):
        mock_session.flush.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            await repo.create(MagicMock())


class TestUpdate:

    @pytest.mark.asyncio
    async def test_updates_task_context(self, repo, mock_session):
        task = MagicMock(context=[])
        with patch("src.repositories.crew_generator_repository.TaskRepository") as MockTaskRepo:
            mock_task_repo = MockTaskRepo.return_value
            mock_task_repo.get = AsyncMock(return_value=task)

            result = await repo.update("task-1", {"context": ["dep-1"]})

            assert result == task
            assert task.context == ["dep-1"]

    @pytest.mark.asyncio
    async def test_returns_none_when_task_not_found(self, repo, mock_session):
        with patch("src.repositories.crew_generator_repository.TaskRepository") as MockTaskRepo:
            mock_task_repo = MockTaskRepo.return_value
            mock_task_repo.get = AsyncMock(return_value=None)

            result = await repo.update("missing", {"context": []})

            assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_non_task_update(self, repo, mock_session):
        result = await repo.update("entity-1", {"name": "new"})

        assert result is None


class TestCreateCrewEntities:

    @pytest.mark.asyncio
    async def test_creates_agents_and_tasks(self, repo, mock_session):
        agent_data = [{
            "name": "Researcher",
            "role": "Research",
            "goal": "Find info",
            "backstory": "Expert",
        }]
        task_data = [{
            "name": "Search Task",
            "description": "Search the web",
            "expected_output": "Report",
            "agent": "Researcher",
        }]

        # Mock flush/refresh to set attributes on entity
        async def mock_flush():
            pass

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None

        mock_session.flush = AsyncMock(side_effect=mock_flush)
        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_crew_entities({"agents": agent_data, "tasks": task_data})

        assert "agents" in result
        assert "tasks" in result
        assert len(result["agents"]) == 1
        assert len(result["tasks"]) == 1
        assert result["agents"][0]["name"] == "Researcher"

    @pytest.mark.asyncio
    async def test_creates_entities_without_group_context(self, repo, mock_session):
        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_crew_entities({
            "agents": [{"name": "A", "role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [{"name": "T", "description": "D", "expected_output": "E", "agent": "A"}],
        })

        assert len(result["agents"]) == 1


class TestCreateAgents:

    @pytest.mark.asyncio
    async def test_creates_agents_with_group_context(self, repo, mock_session):
        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g-1"
        group_ctx.group_email = "a@b.com"

        agents_data = [{"name": "Agent1", "role": "Role", "goal": "Goal", "backstory": "Back"}]

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo._create_agents(agents_data, group_ctx)

        assert len(result) == 1
        assert result[0].group_id == "g-1"

    @pytest.mark.asyncio
    async def test_forces_allow_code_execution_false(self, repo, mock_session):
        agents_data = [{"name": "Agent1", "role": "R", "goal": "G", "backstory": "B", "allow_code_execution": True}]

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo._create_agents(agents_data, None)

        assert result[0].allow_code_execution is False


class TestCreateTasks:

    @pytest.mark.asyncio
    async def test_assigns_agent_by_name(self, repo, mock_session):
        agent_map = {"Agent1": "agent-uuid-1"}
        tasks_data = [{"name": "Task1", "description": "D", "expected_output": "E", "agent": "Agent1"}]

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo._create_tasks(tasks_data, agent_map, None)

        assert len(result) == 1
        assert result[0].agent_id == "agent-uuid-1"

    @pytest.mark.asyncio
    async def test_round_robin_when_no_agent_match(self, repo, mock_session):
        agent_map = {"Agent1": "agent-uuid-1"}
        tasks_data = [{"name": "Task1", "description": "D", "expected_output": "E"}]

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo._create_tasks(tasks_data, agent_map, None)

        assert result[0].agent_id == "agent-uuid-1"

    @pytest.mark.asyncio
    async def test_case_insensitive_agent_match(self, repo, mock_session):
        agent_map = {"Researcher Agent": "agent-uuid-1"}
        tasks_data = [{"name": "T", "description": "D", "expected_output": "E", "agent": "researcher agent"}]

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo._create_tasks(tasks_data, agent_map, None)

        assert result[0].agent_id == "agent-uuid-1"


class TestCreateTaskDependencies:

    def _make_task_mock(self, id_val, name_val):
        """Create a MagicMock with .name set correctly (MagicMock's name= kwarg is special)."""
        task = MagicMock()
        task.id = id_val
        task.name = name_val
        task.context = None
        return task

    @pytest.mark.asyncio
    async def test_resolves_dependencies_by_name(self, repo, mock_session):
        task1 = self._make_task_mock("t-1", "Task1")
        task2 = self._make_task_mock("t-2", "Task2")

        tasks_data = [
            {"name": "Task1", "_context_refs": []},
            {"name": "Task2", "_context_refs": ["Task1"]},
        ]

        await repo._create_task_dependencies([task1, task2], tasks_data)

        assert task2.context == ["t-1"]

    @pytest.mark.asyncio
    async def test_handles_empty_context_refs(self, repo, mock_session):
        task1 = self._make_task_mock("t-1", "Task1")

        tasks_data = [{"name": "Task1"}]

        await repo._create_task_dependencies([task1], tasks_data)

        assert task1.context == []

    @pytest.mark.asyncio
    async def test_skips_self_dependency(self, repo, mock_session):
        task1 = self._make_task_mock("t-1", "Task1")

        tasks_data = [{"name": "Task1", "_context_refs": ["Task1"]}]

        await repo._create_task_dependencies([task1], tasks_data)

        # Self-reference is skipped; no valid deps resolved so context remains None
        assert task1.context is None


class TestCreateSingleAgent:

    @pytest.mark.asyncio
    async def test_create_single_agent_success(self, repo, mock_session):
        agent_data = {
            "name": "Researcher",
            "role": "Research Specialist",
            "goal": "Find information",
            "backstory": "Expert researcher",
            "llm": "gpt-4",
            "tools": ["search", "browse"],
        }

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_single_agent(agent_data)

        assert result["name"] == "Researcher"
        assert result["role"] == "Research Specialist"
        assert result["goal"] == "Find information"
        assert result["backstory"] == "Expert researcher"
        assert result["llm"] == "gpt-4"
        assert result["tools"] == ["search", "browse"]
        assert result["id"] is not None
        assert "allow_delegation" in result
        assert "verbose" in result
        assert "max_iter" in result
        assert "max_rpm" in result
        assert "cache" in result
        assert "allow_code_execution" in result
        assert "code_execution_mode" in result
        assert "max_retry_limit" in result
        assert "use_system_prompt" in result
        assert "respect_context_window" in result
        assert "function_calling_llm" in result
        assert "created_at" in result
        assert "updated_at" in result
        mock_session.add.assert_called()
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_create_single_agent_with_group_context(self, repo, mock_session):
        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g-1"
        group_ctx.group_email = "user@example.com"

        agent_data = {
            "name": "Agent1",
            "role": "Role",
            "goal": "Goal",
            "backstory": "Back",
        }

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_single_agent(agent_data, group_context=group_ctx)

        assert result["name"] == "Agent1"
        # Verify the Agent was constructed with group context by checking the add call
        added_entity = mock_session.add.call_args[0][0]
        assert added_entity.group_id == "g-1"
        assert added_entity.created_by_email == "user@example.com"

    @pytest.mark.asyncio
    async def test_create_single_agent_defaults(self, repo, mock_session):
        agent_data = {
            "name": "MinimalAgent",
            "role": "R",
            "goal": "G",
            "backstory": "B",
        }

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_single_agent(agent_data)

        assert result["max_iter"] == 25
        assert result["max_rpm"] == 10
        assert result["cache"] is True
        assert result["allow_code_execution"] is False

    @pytest.mark.asyncio
    async def test_create_single_agent_error_raises_kasal_error(self, repo, mock_session):
        mock_session.flush.side_effect = Exception("DB connection lost")

        agent_data = {
            "name": "Agent1",
            "role": "R",
            "goal": "G",
            "backstory": "B",
        }

        with pytest.raises(KasalError, match="Failed to persist agent:"):
            await repo.create_single_agent(agent_data)


class TestCreateSingleTask:

    @pytest.mark.asyncio
    async def test_create_single_task_success(self, repo, mock_session):
        task_data = {
            "name": "Research Task",
            "description": "Research the topic",
            "expected_output": "Detailed report",
            "tools": ["search"],
        }

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None
            if not hasattr(entity, 'context'):
                entity.context = None
            if not hasattr(entity, 'tool_configs'):
                entity.tool_configs = None

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_single_task(task_data, agent_id="agent-uuid-1")

        assert result["name"] == "Research Task"
        assert result["description"] == "Research the topic"
        assert result["expected_output"] == "Detailed report"
        assert result["tools"] == ["search"]
        assert result["id"] is not None
        assert "agent_id" in result
        assert "async_execution" in result
        assert "context" in result
        assert "output" in result
        assert "human_input" in result
        assert "llm_guardrail" in result
        assert "tool_configs" in result
        assert "created_at" in result
        assert "updated_at" in result
        mock_session.add.assert_called()
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_create_single_task_with_agent_id(self, repo, mock_session):
        task_data = {
            "name": "Task1",
            "description": "D",
            "expected_output": "E",
        }

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None
            if not hasattr(entity, 'context'):
                entity.context = None
            if not hasattr(entity, 'tool_configs'):
                entity.tool_configs = None

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_single_task(task_data, agent_id="specific-agent-id")

        assert result["agent_id"] == "specific-agent-id"
        added_entity = mock_session.add.call_args[0][0]
        assert added_entity.agent_id == "specific-agent-id"

    @pytest.mark.asyncio
    async def test_create_single_task_tool_configs(self, repo, mock_session):
        task_data = {
            "name": "Task1",
            "description": "D",
            "expected_output": "E",
            "tool_configs": {"search": {"max_results": 10}},
        }

        async def mock_refresh(entity):
            if not hasattr(entity, 'id') or entity.id is None:
                entity.id = str(uuid.uuid4())
            if not hasattr(entity, 'created_at'):
                entity.created_at = None
            if not hasattr(entity, 'updated_at'):
                entity.updated_at = None
            if not hasattr(entity, 'context'):
                entity.context = None
            if not hasattr(entity, 'tool_configs'):
                entity.tool_configs = None

        mock_session.refresh = AsyncMock(side_effect=mock_refresh)

        result = await repo.create_single_task(task_data, agent_id="agent-1")

        assert result["tool_configs"] == {"search": {"max_results": 10}}
        added_entity = mock_session.add.call_args[0][0]
        assert added_entity.tool_configs == {"search": {"max_results": 10}}

    @pytest.mark.asyncio
    async def test_create_single_task_error_raises_kasal_error(self, repo, mock_session):
        mock_session.flush.side_effect = Exception("DB write failed")

        task_data = {
            "name": "Task1",
            "description": "D",
            "expected_output": "E",
        }

        with pytest.raises(KasalError, match="Failed to persist task:"):
            await repo.create_single_task(task_data, agent_id="agent-1")


class TestUpdateTaskDependencies:

    @pytest.mark.asyncio
    async def test_update_task_dependencies_success(self, repo, mock_session):
        task = MagicMock()
        task.context = []

        with patch("src.repositories.crew_generator_repository.TaskRepository") as MockTaskRepo:
            mock_task_repo = MockTaskRepo.return_value
            mock_task_repo.get = AsyncMock(return_value=task)

            await repo.update_task_dependencies("task-1", ["dep-1", "dep-2"])

            mock_task_repo.get.assert_called_once_with("task-1")
            assert task.context == ["dep-1", "dep-2"]
            mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_update_task_dependencies_task_not_found(self, repo, mock_session):
        with patch("src.repositories.crew_generator_repository.TaskRepository") as MockTaskRepo:
            mock_task_repo = MockTaskRepo.return_value
            mock_task_repo.get = AsyncMock(return_value=None)

            # Should not raise, just log a warning
            await repo.update_task_dependencies("missing-id", ["dep-1"])

            mock_task_repo.get.assert_called_once_with("missing-id")
            # flush should not be called since no task was found
            mock_session.flush.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_task_dependencies_error_raises_kasal_error(self, repo, mock_session):
        with patch("src.repositories.crew_generator_repository.TaskRepository") as MockTaskRepo:
            mock_task_repo = MockTaskRepo.return_value
            mock_task_repo.get = AsyncMock(side_effect=Exception("DB error"))

            with pytest.raises(KasalError, match="Failed to update task dependencies:"):
                await repo.update_task_dependencies("task-1", ["dep-1"])
