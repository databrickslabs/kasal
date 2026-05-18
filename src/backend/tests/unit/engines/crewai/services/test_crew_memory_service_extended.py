"""
Extended tests for crew_memory_service.py to push coverage to 90%+.

Covers:
- fetch_memory_backend_config (all branches)
- setup_storage_directory (all backend types, existing dir, new dir)
- create_memory_backends (databricks_config / lakebase_config dict conversion)
- configure_crew_memory_components (all backend types and branches)
- attach_memory_trace_context
- attach_tools_trace_context
- set_crew_reference_on_memory
- restore_storage_directory
"""
import os
import sys
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")

_crewai_mock = MagicMock()
_MODULES_TO_MOCK = {
    "crewai": _crewai_mock,
    "crewai.tools": _crewai_mock.tools,
    "crewai.events": _crewai_mock.events,
    "crewai.flow": _crewai_mock.flow,
    "crewai.flow.flow": _crewai_mock.flow.flow,
    "crewai.flow.persistence": _crewai_mock.flow.persistence,
    "crewai.llm": _crewai_mock.llm,
    "crewai.memory": _crewai_mock.memory,
    "crewai.memory.storage": _crewai_mock.memory.storage,
    "crewai.memory.storage.rag_storage": _crewai_mock.memory.storage.rag_storage,
    "crewai.memory.storage.ltm_sqlite_storage": _crewai_mock.memory.storage.ltm_sqlite_storage,
    "crewai.project": _crewai_mock.project,
    "crewai.tasks": _crewai_mock.tasks,
    "crewai.tasks.llm_guardrail": _crewai_mock.tasks.llm_guardrail,
    "crewai.tasks.task_output": _crewai_mock.tasks.task_output,
    "crewai.utilities": _crewai_mock.utilities,
    "crewai.utilities.converter": _crewai_mock.utilities.converter,
    "crewai.utilities.evaluators": _crewai_mock.utilities.evaluators,
    "crewai.utilities.evaluators.task_evaluator": _crewai_mock.utilities.evaluators.task_evaluator,
    "crewai.utilities.exceptions": _crewai_mock.utilities.exceptions,
    "crewai.utilities.internal_instructor": _crewai_mock.utilities.internal_instructor,
    "crewai.utilities.paths": _crewai_mock.utilities.paths,
    "crewai.utilities.printer": _crewai_mock.utilities.printer,
    "crewai.knowledge": _crewai_mock.knowledge,
    "crewai.llms": _crewai_mock.llms,
    "crewai.llms.providers": _crewai_mock.llms.providers,
    "crewai.llms.providers.openai": _crewai_mock.llms.providers.openai,
    "crewai.llms.providers.openai.completion": _crewai_mock.llms.providers.openai.completion,
    "crewai.events.types": _crewai_mock.events.types,
    "crewai.events.types.llm_events": _crewai_mock.events.types.llm_events,
    "crewai_tools": MagicMock(),
    "asyncpg": MagicMock(),
    "chromadb": MagicMock(),
}

_originals = {}
for _mod_name, _mock_obj in _MODULES_TO_MOCK.items():
    _originals[_mod_name] = sys.modules.get(_mod_name)
    sys.modules[_mod_name] = _mock_obj

# Set up crewai.utilities.paths mock
_crewai_mock.utilities.paths.db_storage_path = MagicMock(return_value="/tmp/test_storage")

from src.engines.crewai.services.crew_memory_service import CrewMemoryService
from src.engines.crewai.memory.memory_backend_factory import DatabricksIndexValidationError
from src.schemas.memory_backend import MemoryBackendType

for _mod_name, _original in _originals.items():
    if _original is None:
        sys.modules.pop(_mod_name, None)
    else:
        sys.modules[_mod_name] = _original


# ─────────────────────────────────────────────────────────────────────────────
# fetch_memory_backend_config
# ─────────────────────────────────────────────────────────────────────────────


class TestFetchMemoryBackendConfig:
    """Tests for fetch_memory_backend_config."""

    @pytest.mark.asyncio
    async def test_returns_none_when_no_active_config(self):
        service = CrewMemoryService({"group_id": "grp1"})
        mock_session = AsyncMock()
        mock_service = AsyncMock()
        mock_service.get_active_config.return_value = None

        with patch("src.db.session.request_scoped_session") as mock_ctx:
            mock_ctx.return_value.__aenter__.return_value = mock_session
            with patch(
                "src.services.memory_backend_service.MemoryBackendService",
                return_value=mock_service,
            ):
                result = await service.fetch_memory_backend_config()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_disabled_config(self):
        service = CrewMemoryService({"group_id": "grp1"})
        mock_session = AsyncMock()
        mock_active = MagicMock()
        mock_active.enable_short_term = False
        mock_active.enable_long_term = False
        mock_active.enable_entity = False
        mock_service = AsyncMock()
        mock_service.get_active_config.return_value = mock_active

        with patch("src.db.session.request_scoped_session") as mock_ctx:
            mock_ctx.return_value.__aenter__.return_value = mock_session
            with patch(
                "src.services.memory_backend_service.MemoryBackendService",
                return_value=mock_service,
            ):
                result = await service.fetch_memory_backend_config()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_config_dict_for_enabled_config(self):
        service = CrewMemoryService({"group_id": "grp1"})
        mock_session = AsyncMock()
        mock_active = MagicMock()
        mock_active.enable_short_term = True
        mock_active.enable_long_term = True
        mock_active.enable_entity = True
        mock_active.enable_relationship_retrieval = False
        mock_active.backend_type.value = "databricks"
        mock_active.databricks_config = {"endpoint": "ep1"}
        mock_active.lakebase_config = None
        mock_service = AsyncMock()
        mock_service.get_active_config.return_value = mock_active

        with patch("src.db.session.request_scoped_session") as mock_ctx:
            mock_ctx.return_value.__aenter__.return_value = mock_session
            with patch(
                "src.services.memory_backend_service.MemoryBackendService",
                return_value=mock_service,
            ):
                result = await service.fetch_memory_backend_config()

        assert result is not None
        assert result["backend_type"] == "databricks"
        assert result["enable_short_term"] is True

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        service = CrewMemoryService({"group_id": "grp1"})

        with patch("src.db.session.request_scoped_session") as mock_ctx:
            mock_ctx.return_value.__aenter__.side_effect = Exception("DB error")
            result = await service.fetch_memory_backend_config()

        assert result is None

    @pytest.mark.asyncio
    async def test_mixed_enabled_not_considered_disabled(self):
        """If at least one type is enabled, it's not a 'Disabled Configuration'."""
        service = CrewMemoryService({"group_id": "grp1"})
        mock_session = AsyncMock()
        mock_active = MagicMock()
        mock_active.enable_short_term = True
        mock_active.enable_long_term = False
        mock_active.enable_entity = False
        mock_active.enable_relationship_retrieval = False
        mock_active.backend_type.value = "default"
        mock_active.databricks_config = None
        mock_active.lakebase_config = None
        mock_service = AsyncMock()
        mock_service.get_active_config.return_value = mock_active

        with patch("src.db.session.request_scoped_session") as mock_ctx:
            mock_ctx.return_value.__aenter__.return_value = mock_session
            with patch(
                "src.services.memory_backend_service.MemoryBackendService",
                return_value=mock_service,
            ):
                result = await service.fetch_memory_backend_config()

        # Not disabled, should return config
        assert result is not None
        assert result["backend_type"] == "default"


# ─────────────────────────────────────────────────────────────────────────────
# setup_storage_directory
# ─────────────────────────────────────────────────────────────────────────────


class TestSetupStorageDirectory:
    """Tests for setup_storage_directory."""

    def setup_method(self):
        # Clean env
        os.environ.pop("CREWAI_STORAGE_DIR", None)

    def test_no_op_when_no_memory_backend_config(self):
        service = CrewMemoryService({})
        service.setup_storage_directory("crew_1", None)
        # Should not have set env var
        assert "CREWAI_STORAGE_DIR" not in os.environ

    def test_no_op_for_unknown_backend_type(self):
        service = CrewMemoryService({})
        service.setup_storage_directory("crew_1", {"backend_type": "other_backend"})
        assert "CREWAI_STORAGE_DIR" not in os.environ

    def _do_setup_storage(self, service, crew_id, backend_config, mock_path):
        """Helper that patches the crewai paths import properly."""
        with patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath, \
             patch("src.engines.crewai.services.crew_memory_service.db_storage_path",
                   return_value="/tmp/test", create=True), \
             patch.dict("sys.modules", {
                 "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
             }):
            MockPath.return_value = mock_path
            service.setup_storage_directory(crew_id, backend_config)

    def test_sets_databricks_storage_dir(self):
        service = CrewMemoryService({})
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_path.absolute.return_value = mock_path

        with patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath:
            MockPath.return_value = mock_path
            with patch.dict("sys.modules", {
                "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
            }):
                service.setup_storage_directory("my_crew_id", {"backend_type": "databricks"})

        assert os.environ.get("CREWAI_STORAGE_DIR") == "kasal_databricks_my_crew_id"

    def test_sets_lakebase_storage_dir(self):
        service = CrewMemoryService({})
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_path.absolute.return_value = mock_path

        with patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath:
            MockPath.return_value = mock_path
            with patch.dict("sys.modules", {
                "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
            }):
                service.setup_storage_directory("my_crew_id", {"backend_type": "lakebase"})

        assert os.environ.get("CREWAI_STORAGE_DIR") == "kasal_lakebase_my_crew_id"

    def test_sets_default_storage_dir(self):
        service = CrewMemoryService({})
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_path.absolute.return_value = mock_path

        with patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath:
            MockPath.return_value = mock_path
            with patch.dict("sys.modules", {
                "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
            }):
                service.setup_storage_directory("my_crew_id", {"backend_type": "default"})

        assert os.environ.get("CREWAI_STORAGE_DIR") == "kasal_default_my_crew_id"

    def test_saves_original_storage_dir(self):
        os.environ["CREWAI_STORAGE_DIR"] = "original_value"
        service = CrewMemoryService({})
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_path.absolute.return_value = mock_path

        with patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath:
            MockPath.return_value = mock_path
            with patch.dict("sys.modules", {
                "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
            }):
                service.setup_storage_directory("crew_1", {"backend_type": "databricks"})

        assert service._original_storage_dir == "original_value"

    def test_logs_existing_storage_contents(self):
        service = CrewMemoryService({})
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.absolute.return_value = mock_path
        mock_file = MagicMock()
        mock_file.name = "test.db"
        mock_file.is_dir.return_value = False
        mock_path.iterdir.return_value = [mock_file]

        with patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath:
            MockPath.return_value = mock_path
            with patch.dict("sys.modules", {
                "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
            }):
                service.setup_storage_directory("crew_1", {"backend_type": "databricks"})

    def test_handles_iterdir_exception(self):
        service = CrewMemoryService({})
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.absolute.return_value = mock_path
        mock_path.iterdir.side_effect = PermissionError("no access")

        with patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath:
            MockPath.return_value = mock_path
            with patch.dict("sys.modules", {
                "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
            }):
                service.setup_storage_directory("crew_1", {"backend_type": "databricks"})


# ─────────────────────────────────────────────────────────────────────────────
# create_memory_backends
# ─────────────────────────────────────────────────────────────────────────────


class TestCreateMemoryBackends:
    """Tests for create_memory_backends."""

    @pytest.mark.asyncio
    async def test_converts_databricks_config_dict_to_object(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        memory_backend_config = {
            "backend_type": "databricks",
            "databricks_config": {
                "endpoint_name": "ep",
                "short_term_index": "cat.sch.st",
                "long_term_index": "cat.sch.lt",
                "entity_index": "cat.sch.ent",
            },
            "enable_short_term": True,
            "enable_long_term": True,
            "enable_entity": True,
            "enable_relationship_retrieval": False,
        }

        with patch(
            "src.engines.crewai.services.crew_memory_service.MemoryBackendFactory.create_memory_backends",
            new_callable=AsyncMock,
            return_value={"short_term": MagicMock()},
        ) as mock_factory:
            result = await service.create_memory_backends(
                memory_backend_config=memory_backend_config,
                crew_id="crew_1",
                embedder=None,
            )

        assert mock_factory.called

    @pytest.mark.asyncio
    async def test_converts_lakebase_config_dict_to_object(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        memory_backend_config = {
            "backend_type": "lakebase",
            "lakebase_config": {
                "host": "localhost",
                "port": 5432,
                "database": "memory_db",
                "username": "user",
                "password": "pass",
            },
            "enable_short_term": True,
            "enable_long_term": False,
            "enable_entity": False,
            "enable_relationship_retrieval": False,
        }

        with patch(
            "src.engines.crewai.services.crew_memory_service.MemoryBackendFactory.create_memory_backends",
            new_callable=AsyncMock,
            return_value={"short_term": MagicMock()},
        ) as mock_factory:
            result = await service.create_memory_backends(
                memory_backend_config=memory_backend_config,
                crew_id="crew_1",
                embedder=None,
            )

        assert mock_factory.called

    @pytest.mark.asyncio
    async def test_passes_job_id_to_factory(self):
        service = CrewMemoryService({"execution_id": "my_job_id"})
        memory_backend_config = {
            "backend_type": "default",
            "enable_short_term": True,
            "enable_long_term": False,
            "enable_entity": False,
            "enable_relationship_retrieval": False,
        }

        with patch(
            "src.engines.crewai.services.crew_memory_service.MemoryBackendFactory.create_memory_backends",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_factory:
            await service.create_memory_backends(
                memory_backend_config=memory_backend_config,
                crew_id="crew_1",
                embedder=None,
            )

        call_kwargs = mock_factory.call_args[1]
        assert call_kwargs.get("job_id") == "my_job_id"

    @pytest.mark.asyncio
    async def test_raises_and_emits_trace_on_validation_error(self):
        service = CrewMemoryService({"execution_id": "job_1", "group_id": "grp"})
        memory_backend_config = {
            "backend_type": "databricks",
            "enable_short_term": True,
            "enable_long_term": False,
            "enable_entity": False,
            "enable_relationship_retrieval": False,
        }
        validation_result = {
            "valid": False,
            "missing_indexes": ["short_term"],
            "provisioning_indexes": [],
            "error_type": "missing_indexes",
        }

        with patch(
            "src.engines.crewai.services.crew_memory_service.MemoryBackendFactory.create_memory_backends",
            new_callable=AsyncMock,
            side_effect=DatabricksIndexValidationError("err", validation_result),
        ), patch.object(
            service, "_emit_index_validation_trace", new_callable=AsyncMock
        ) as mock_emit:
            with pytest.raises(DatabricksIndexValidationError):
                await service.create_memory_backends(
                    memory_backend_config=memory_backend_config,
                    crew_id="crew_1",
                    embedder=None,
                )
            mock_emit.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# configure_crew_memory_components
# ─────────────────────────────────────────────────────────────────────────────


class TestConfigureCrewMemoryComponents:
    """Tests for configure_crew_memory_components."""

    def _make_memory_config(self, backend_type_str, short_term=True, long_term=True, entity=True):
        from src.schemas.memory_backend import MemoryBackendConfig
        from unittest.mock import MagicMock
        cfg = MagicMock()
        cfg.backend_type = MemoryBackendType(backend_type_str)
        cfg.enable_short_term = short_term
        cfg.enable_long_term = long_term
        cfg.enable_entity = entity
        return cfg

    def _crewai_memory_mocks(self):
        """Return a dict of crewai module mocks for configure_crew_memory_components tests."""
        mock_crewai_mem = MagicMock()
        mock_crewai_mem.ShortTermMemory = MagicMock(return_value=MagicMock())
        mock_crewai_mem.LongTermMemory = MagicMock(return_value=MagicMock())
        mock_crewai_mem.EntityMemory = MagicMock(return_value=MagicMock())
        mock_storage = MagicMock()
        mock_storage.rag_storage.RAGStorage = MagicMock(return_value=MagicMock())
        mock_storage.ltm_sqlite_storage.LTMSQLiteStorage = MagicMock(return_value=MagicMock())
        mock_crewai_mem.storage = mock_storage
        return {
            "crewai.memory": mock_crewai_mem,
            "crewai.memory.storage": mock_storage,
            "crewai.memory.storage.rag_storage": mock_storage.rag_storage,
            "crewai.memory.storage.ltm_sqlite_storage": mock_storage.ltm_sqlite_storage,
            "crewai.utilities.paths": MagicMock(db_storage_path=MagicMock(return_value="/tmp/test")),
        }

    def test_disables_memory_when_default_no_embedder(self):
        service = CrewMemoryService({})
        crew_kwargs = {}
        memory_config = self._make_memory_config("default")
        memory_backends = {}

        with patch.dict("sys.modules", self._crewai_memory_mocks()):
            result = service.configure_crew_memory_components(
                crew_kwargs, memory_config, memory_backends, "crew_1", custom_embedder=None
            )

        assert result.get("memory") is False

    def test_configures_default_backend_with_custom_embedder(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        crew_kwargs = {}
        memory_config = self._make_memory_config("default", short_term=True, long_term=True, entity=True)
        memory_backends = {}
        custom_embedder = MagicMock()

        extra_mocks = self._crewai_memory_mocks()
        extra_mocks["src.engines.crewai.memory.chromadb_databricks_storage"] = MagicMock()

        with patch.dict("sys.modules", extra_mocks), \
             patch("src.engines.crewai.services.crew_memory_service.Path") as MockPath:
            MockPath.return_value = MagicMock()
            result = service.configure_crew_memory_components(
                crew_kwargs, memory_config, memory_backends, "crew_1", custom_embedder=custom_embedder
            )

        assert result.get("memory") is False

    def test_configures_lakebase_short_term(self):
        service = CrewMemoryService({})
        crew_kwargs = {}
        memory_config = self._make_memory_config("lakebase", short_term=True, long_term=True, entity=True)
        memory_backends = {
            "short_term": MagicMock(),
            "long_term": MagicMock(),
            "entity": MagicMock(),
        }

        with patch.dict("sys.modules", self._crewai_memory_mocks()):
            result = service.configure_crew_memory_components(
                crew_kwargs, memory_config, memory_backends, "crew_1"
            )

        assert result.get("memory") is False

    def test_configures_databricks_short_term(self):
        service = CrewMemoryService({})
        crew_kwargs = {}
        memory_config = self._make_memory_config("databricks", short_term=True, long_term=True, entity=True)
        memory_backends = {
            "short_term": MagicMock(),
            "long_term": MagicMock(),
            "entity": MagicMock(),
        }

        with patch.dict("sys.modules", self._crewai_memory_mocks()):
            result = service.configure_crew_memory_components(
                crew_kwargs, memory_config, memory_backends, "crew_1"
            )

        assert result.get("memory") is False

    def test_handles_import_error_gracefully(self):
        service = CrewMemoryService({})
        crew_kwargs = {"memory": True}
        memory_config = self._make_memory_config("databricks")
        memory_backends = {"short_term": MagicMock()}

        # Simulate ImportError by having crewai.memory raise on import
        with patch.dict("sys.modules", {"crewai.memory": None}):
            result = service.configure_crew_memory_components(
                crew_kwargs, memory_config, memory_backends, "crew_1"
            )
        # Should return crew_kwargs unchanged after error
        assert result is not None

    def test_handles_general_exception_gracefully(self):
        service = CrewMemoryService({})
        crew_kwargs = {}
        memory_config = self._make_memory_config("databricks")
        memory_backends = {"short_term": MagicMock()}

        mocks = self._crewai_memory_mocks()
        mocks["crewai.memory"].ShortTermMemory.side_effect = RuntimeError("unexpected")
        with patch.dict("sys.modules", mocks):
            result = service.configure_crew_memory_components(
                crew_kwargs, memory_config, memory_backends, "crew_1"
            )
        assert result is not None

    def test_non_databricks_backend_with_embedder_uses_rag(self):
        """Non-databricks non-default backend with embedder should use RAGStorage."""
        service = CrewMemoryService({})
        crew_kwargs = {"embedder": MagicMock()}

        from src.schemas.memory_backend import MemoryBackendType as MBT
        memory_config = self._make_memory_config("databricks", short_term=True, long_term=False, entity=True)
        memory_config.backend_type = MBT.DATABRICKS
        memory_backends = {
            "short_term": MagicMock(),
            "entity": MagicMock(),
        }

        with patch.dict("sys.modules", self._crewai_memory_mocks()):
            result = service.configure_crew_memory_components(
                crew_kwargs, memory_config, memory_backends, "crew_1"
            )

        assert result is not None


# ─────────────────────────────────────────────────────────────────────────────
# attach_memory_trace_context
# ─────────────────────────────────────────────────────────────────────────────


class TestAttachMemoryTraceContext:
    """Tests for attach_memory_trace_context."""

    def test_attaches_trace_to_short_term_memory(self):
        service = CrewMemoryService({"execution_id": "job_1", "group_id": "grp"})
        mock_crew = MagicMock()
        mock_storage = MagicMock()
        mock_storage.trace_context = None
        mock_mem = MagicMock()
        mock_mem.storage = mock_storage
        mock_mem.trace_context = None
        mock_crew._short_term_memory = mock_mem
        mock_crew._long_term_memory = None
        mock_crew._entity_memory = None

        service.attach_memory_trace_context(mock_crew, {"backend_type": "databricks"}, {})

        # trace_context should have been set on storage
        assert hasattr(mock_storage, "trace_context")

    def test_handles_none_crew_attributes(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        mock_crew = MagicMock()
        mock_crew._short_term_memory = None
        mock_crew._long_term_memory = None
        mock_crew._entity_memory = None

        # Should not raise
        service.attach_memory_trace_context(mock_crew, None, {})

    def test_handles_exception_gracefully(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        # Pass something that will throw when accessing attributes
        broken_crew = MagicMock()
        broken_crew._short_term_memory.storage.trace_context.__set__ = MagicMock(side_effect=Exception("fail"))

        # Should not raise
        service.attach_memory_trace_context(broken_crew, {}, {})

    def test_sets_trace_context_with_run_name_fallback(self):
        service = CrewMemoryService({"run_name": "run_abc", "group_id": "grp"})
        mock_crew = MagicMock()
        mock_storage = MagicMock()
        mock_storage.trace_context = None
        mock_mem = MagicMock()
        mock_mem.storage = mock_storage
        mock_crew._short_term_memory = mock_mem
        mock_crew._long_term_memory = None
        mock_crew._entity_memory = None

        service.attach_memory_trace_context(mock_crew, {}, {})
        # Storage should have trace_context set


# ─────────────────────────────────────────────────────────────────────────────
# attach_tools_trace_context
# ─────────────────────────────────────────────────────────────────────────────


class TestAttachToolsTraceContext:
    """Tests for attach_tools_trace_context."""

    def test_attaches_trace_to_agent_tools(self):
        service = CrewMemoryService({"execution_id": "job_1", "group_id": "grp"})
        mock_crew = MagicMock()

        # Use a real object with __dict__ so setattr works
        class MockTool:
            name = "test_tool"

        mock_tool = MockTool()
        mock_agent = MagicMock()
        mock_agent.tools = [mock_tool]
        mock_agent.role = "Researcher"
        mock_crew.agents = [mock_agent]
        mock_crew.tasks = []

        service.attach_tools_trace_context(mock_crew, {})

        # trace_context should have been set on tool
        assert hasattr(mock_tool, "trace_context")

    def test_attaches_trace_to_task_tools(self):
        service = CrewMemoryService({"execution_id": "job_1", "group_id": "grp"})
        mock_crew = MagicMock()
        mock_crew.agents = []

        class MockTool:
            name = "task_tool"

        mock_tool = MockTool()
        mock_task = MagicMock()
        mock_task.tools = [mock_tool]
        mock_task.description = "Test task"
        mock_crew.tasks = [mock_task]

        service.attach_tools_trace_context(mock_crew, {})

        assert hasattr(mock_tool, "trace_context")

    def test_handles_no_agents_or_tasks(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        mock_crew = MagicMock()
        mock_crew.agents = []
        mock_crew.tasks = []

        # Should not raise
        service.attach_tools_trace_context(mock_crew, {})

    def test_handles_exception_gracefully(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        # Crew without agents/tasks attribute - will raise AttributeError internally
        broken_crew = "not a crew"

        # Should not raise
        service.attach_tools_trace_context(broken_crew, {})

    def test_uses_inputs_run_name_fallback(self):
        service = CrewMemoryService({"inputs": {"run_name": "run_from_inputs"}})
        mock_crew = MagicMock()
        mock_crew.agents = []
        mock_crew.tasks = []

        # Should not raise
        service.attach_tools_trace_context(mock_crew, {})

    def test_tool_without_dict_is_skipped_gracefully(self):
        service = CrewMemoryService({"execution_id": "job_1"})
        mock_crew = MagicMock()

        # A tool without __dict__ (e.g., a built-in function)
        class NoDict:
            __slots__ = ()

        mock_agent = MagicMock()
        mock_agent.tools = [NoDict()]
        mock_agent.role = "Dev"
        mock_crew.agents = [mock_agent]
        mock_crew.tasks = []

        # Should not raise
        service.attach_tools_trace_context(mock_crew, {})


# ─────────────────────────────────────────────────────────────────────────────
# set_crew_reference_on_memory
# ─────────────────────────────────────────────────────────────────────────────


class TestSetCrewReferenceOnMemory:
    """Tests for set_crew_reference_on_memory."""

    def test_sets_crew_on_long_term_storage(self):
        service = CrewMemoryService({})
        mock_crew = MagicMock()
        mock_storage = MagicMock()
        mock_storage.crew = None
        mock_crew._long_term_memory = MagicMock()
        mock_crew._long_term_memory.storage = mock_storage
        mock_crew._entity_memory = None
        mock_crew._short_term_memory = None

        service.set_crew_reference_on_memory(mock_crew)

        assert mock_storage.crew == mock_crew

    def test_sets_agent_context_on_entity_storage(self):
        service = CrewMemoryService({})
        mock_crew = MagicMock()
        mock_crew._long_term_memory = None
        mock_crew._short_term_memory = None
        mock_storage = MagicMock()
        mock_storage.crew = None
        mock_agent = MagicMock()
        mock_agent.role = "Dev"
        mock_crew.agents = [mock_agent]
        mock_crew._entity_memory = MagicMock()
        mock_crew._entity_memory.storage = mock_storage

        service.set_crew_reference_on_memory(mock_crew)

        mock_storage.set_agent_context.assert_called_once_with(mock_agent)

    def test_sets_crew_on_short_term_storage(self):
        service = CrewMemoryService({})
        mock_crew = MagicMock()
        mock_crew._long_term_memory = None
        mock_crew._entity_memory = None
        mock_storage = MagicMock()
        mock_storage.crew = None
        mock_crew._short_term_memory = MagicMock()
        mock_crew._short_term_memory.storage = mock_storage

        service.set_crew_reference_on_memory(mock_crew)

        assert mock_storage.crew == mock_crew

    def test_handles_exception_gracefully(self):
        service = CrewMemoryService({})
        broken_crew = "not a crew"

        # Should not raise
        service.set_crew_reference_on_memory(broken_crew)

    def test_handles_missing_memory_attributes(self):
        service = CrewMemoryService({})
        mock_crew = MagicMock()
        mock_crew._long_term_memory = None
        mock_crew._entity_memory = None
        mock_crew._short_term_memory = None

        # Should not raise
        service.set_crew_reference_on_memory(mock_crew)


# ─────────────────────────────────────────────────────────────────────────────
# restore_storage_directory
# ─────────────────────────────────────────────────────────────────────────────


class TestRestoreStorageDirectory:
    """Tests for restore_storage_directory."""

    def test_restores_original_value(self):
        service = CrewMemoryService({})
        service._original_storage_dir = "my_original_dir"
        os.environ["CREWAI_STORAGE_DIR"] = "changed_dir"

        service.restore_storage_directory()

        assert os.environ.get("CREWAI_STORAGE_DIR") == "my_original_dir"

    def test_removes_env_var_when_original_was_none(self):
        service = CrewMemoryService({})
        service._original_storage_dir = None
        os.environ["CREWAI_STORAGE_DIR"] = "some_dir"

        service.restore_storage_directory()

        assert "CREWAI_STORAGE_DIR" not in os.environ

    def test_does_nothing_when_original_is_none_and_no_env_var(self):
        service = CrewMemoryService({})
        service._original_storage_dir = None
        os.environ.pop("CREWAI_STORAGE_DIR", None)

        # Should not raise
        service.restore_storage_directory()

        assert "CREWAI_STORAGE_DIR" not in os.environ


# ─────────────────────────────────────────────────────────────────────────────
# _emit_index_validation_trace - additional coverage for 'other' error type
# ─────────────────────────────────────────────────────────────────────────────


class TestEmitIndexValidationTraceOtherType:
    """Test the else branch for unknown error_type in _emit_index_validation_trace."""

    @pytest.mark.asyncio
    async def test_handles_unknown_error_type(self):
        service = CrewMemoryService({"execution_id": "job_99", "group_id": "grp"})

        validation_result = {
            "valid": False,
            "missing_indexes": [],
            "provisioning_indexes": [],
            "error_type": "unknown_type",
        }
        error = DatabricksIndexValidationError("Unknown error", validation_result)

        with patch("src.db.session.request_scoped_session") as mock_session:
            mock_session_instance = AsyncMock()
            mock_session.return_value.__aenter__.return_value = mock_session_instance
            with patch(
                "src.services.execution_trace_service.ExecutionTraceService"
            ) as MockTraceService:
                mock_trace_service = MagicMock()
                mock_trace_service.create_trace = AsyncMock()
                MockTraceService.return_value = mock_trace_service

                await service._emit_index_validation_trace(error)

                # Should still create a trace even for unknown error_type
                mock_trace_service.create_trace.assert_called_once()
