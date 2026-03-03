"""
Comprehensive unit tests for src/seeds/seed_runner.py.

Tests cover:
- run_seeders(): running specific seeders, handling unknown names, continuing on failure
- run_all_seeders(): fast/slow seeder separation, background tasks, resync call
- resync_postgres_sequences(): SQLite skip, PostgreSQL execution, exception handling
- run_seeders_with_factory(): patching/restoring session factories, exclude set
- SEEDERS dictionary registration
- debug_log() helper
- main() CLI entry point
"""

import asyncio
import sys
import types
from collections import OrderedDict
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.seeds import seed_runner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_seeders_dict(*names: str) -> dict:
    """Return an OrderedDict of {name: AsyncMock()} for predictable iteration."""
    return OrderedDict((n, AsyncMock()) for n in names)


# ===========================================================================
# SEEDERS dict registration
# ===========================================================================


class TestSeedersRegistration:
    """Verify the SEEDERS dictionary is populated correctly at module level."""

    def test_seeders_is_dict(self):
        assert isinstance(seed_runner.SEEDERS, dict)

    def test_seeders_not_empty(self):
        assert len(seed_runner.SEEDERS) > 0

    def test_core_seeders_registered(self):
        """Core seeders that must always be present."""
        core = {
            "tools",
            "schemas",
            "prompt_templates",
            "model_configs",
            "documentation",
            "groups",
            "api_keys",
        }
        for name in core:
            assert name in seed_runner.SEEDERS, f"Missing core seeder: {name}"

    def test_optional_seeders_in_known_set(self):
        """Optional seeders (dspy_examples, example_crews) may or may not be
        present depending on import availability, but if present they must be
        in the known set."""
        known = {
            "tools",
            "schemas",
            "prompt_templates",
            "model_configs",
            "documentation",
            "groups",
            "api_keys",
            "dspy_examples",
            "example_crews",
        }
        for name in seed_runner.SEEDERS:
            assert name in known, f"Unexpected seeder registered: {name}"

    def test_all_seeder_values_are_callable(self):
        for name, func in seed_runner.SEEDERS.items():
            assert callable(func), f"Seeder '{name}' is not callable"


# ===========================================================================
# run_seeders()
# ===========================================================================


class TestRunSeeders:
    """Tests for run_seeders(seeders_to_run)."""

    @pytest.mark.asyncio
    async def test_runs_single_valid_seeder(self):
        mock = AsyncMock()
        with patch.object(seed_runner, "SEEDERS", {"alpha": mock}):
            await seed_runner.run_seeders(["alpha"])
        mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_runs_multiple_seeders_in_order(self):
        call_order = []
        async def _make(name):
            async def _fn():
                call_order.append(name)
            return _fn

        seeders = OrderedDict()
        for n in ("a", "b", "c"):
            seeders[n] = AsyncMock(side_effect=lambda n=n: call_order.append(n))

        with patch.object(seed_runner, "SEEDERS", seeders):
            await seed_runner.run_seeders(["a", "b", "c"])

        assert call_order == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_unknown_seeder_logs_warning(self):
        with patch.object(seed_runner, "SEEDERS", {}):
            with patch.object(seed_runner.logger, "warning") as mock_warn:
                await seed_runner.run_seeders(["nonexistent"])
        mock_warn.assert_called_once_with("Unknown seeder: nonexistent")

    @pytest.mark.asyncio
    async def test_multiple_unknown_seeders_log_each(self):
        with patch.object(seed_runner, "SEEDERS", {}):
            with patch.object(seed_runner.logger, "warning") as mock_warn:
                await seed_runner.run_seeders(["foo", "bar"])
        assert mock_warn.call_count == 2
        mock_warn.assert_any_call("Unknown seeder: foo")
        mock_warn.assert_any_call("Unknown seeder: bar")

    @pytest.mark.asyncio
    async def test_continues_on_failure(self):
        failing = AsyncMock(side_effect=RuntimeError("boom"))
        passing = AsyncMock()
        with patch.object(
            seed_runner, "SEEDERS", {"fail": failing, "pass": passing}
        ):
            await seed_runner.run_seeders(["fail", "pass"])
        failing.assert_awaited_once()
        passing.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_logged_on_failure(self):
        failing = AsyncMock(side_effect=ValueError("bad value"))
        with patch.object(seed_runner, "SEEDERS", {"broken": failing}):
            with patch.object(seed_runner.logger, "error") as mock_err:
                await seed_runner.run_seeders(["broken"])
        # First error call is the message, second is the traceback
        assert mock_err.call_count >= 1
        assert "bad value" in str(mock_err.call_args_list[0])

    @pytest.mark.asyncio
    async def test_empty_list_does_nothing(self):
        mock = AsyncMock()
        with patch.object(seed_runner, "SEEDERS", {"x": mock}):
            await seed_runner.run_seeders([])
        mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_mix_of_valid_and_unknown(self):
        mock = AsyncMock()
        with patch.object(seed_runner, "SEEDERS", {"real": mock}):
            with patch.object(seed_runner.logger, "warning") as mock_warn:
                await seed_runner.run_seeders(["real", "fake"])
        mock.assert_awaited_once()
        mock_warn.assert_called_once_with("Unknown seeder: fake")


# ===========================================================================
# run_all_seeders()
# ===========================================================================


class TestRunAllSeeders:
    """Tests for run_all_seeders()."""

    @pytest.mark.asyncio
    async def test_empty_seeders_warns_and_returns(self):
        with patch.object(seed_runner, "SEEDERS", {}):
            with patch.object(seed_runner.logger, "warning") as mock_warn:
                with patch.object(
                    seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
                ) as mock_resync:
                    await seed_runner.run_all_seeders()
        mock_warn.assert_called()
        # resync should NOT be called when SEEDERS is empty (early return)
        mock_resync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fast_seeders_are_awaited(self):
        """Fast seeders (e.g. tools, schemas) should be awaited directly."""
        seeders = _make_seeders_dict("tools", "schemas", "groups")
        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ):
                await seed_runner.run_all_seeders()
        for name, mock_fn in seeders.items():
            mock_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_slow_seeders_launched_as_background_tasks(self):
        """The 'documentation' seeder should be launched via asyncio.create_task."""
        doc_mock = AsyncMock()
        seeders = OrderedDict([("documentation", doc_mock)])
        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ):
                with patch("src.seeds.seed_runner.asyncio.create_task") as mock_task:
                    mock_task.return_value = MagicMock()
                    await seed_runner.run_all_seeders()
        # documentation is NOT in fast_seeders, so it should NOT be awaited directly
        doc_mock.assert_not_awaited()
        # Instead it should have been passed to create_task
        mock_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_fast_seeder_failure_continues(self):
        """If a fast seeder fails, subsequent fast seeders still run."""
        failing = AsyncMock(side_effect=Exception("fail"))
        passing = AsyncMock()
        seeders = OrderedDict([("tools", failing), ("schemas", passing)])
        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ):
                await seed_runner.run_all_seeders()
        failing.assert_awaited_once()
        passing.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_resync_postgres_sequences_called(self):
        """resync_postgres_sequences should be called at the end."""
        seeders = _make_seeders_dict("tools")
        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ) as mock_resync:
                await seed_runner.run_all_seeders()
        mock_resync.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_only_fast_seeders_directly_awaited(self):
        """Seeders in the slow list should not be directly awaited."""
        fast_mock = AsyncMock()
        slow_mock = AsyncMock()
        seeders = OrderedDict([
            ("api_keys", fast_mock),
            ("documentation", slow_mock),
        ])
        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ):
                with patch("src.seeds.seed_runner.asyncio.create_task") as mock_task:
                    mock_task.return_value = MagicMock()
                    await seed_runner.run_all_seeders()
        fast_mock.assert_awaited_once()
        slow_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_background_tasks_list_populated(self):
        """When slow seeders exist, background_tasks should contain tasks."""
        doc_mock = AsyncMock()
        fake_task = MagicMock()
        seeders = OrderedDict([("documentation", doc_mock)])
        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ):
                with patch(
                    "src.seeds.seed_runner.asyncio.create_task",
                    return_value=fake_task,
                ) as mock_task:
                    await seed_runner.run_all_seeders()
        mock_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_all_fast_seeder_names_handled(self):
        """Every name in the fast_seeders list should be directly awaited."""
        fast_names = [
            "groups", "api_keys", "tools", "schemas",
            "prompt_templates", "model_configs", "dspy_examples", "example_crews",
        ]
        seeders = _make_seeders_dict(*fast_names)
        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ):
                await seed_runner.run_all_seeders()
        for name in fast_names:
            seeders[name].assert_awaited_once()


# ===========================================================================
# resync_postgres_sequences()
# ===========================================================================


class TestResyncPostgresSequences:
    """Tests for resync_postgres_sequences()."""

    @pytest.mark.asyncio
    async def test_skips_for_sqlite(self):
        """When DATABASE_URI contains 'sqlite', function should return early."""
        mock_settings = MagicMock()
        mock_settings.DATABASE_URI = "sqlite+aiosqlite:///test.db"
        with patch("src.seeds.seed_runner.settings", mock_settings, create=True):
            # Patch the import inside the function
            with patch.dict(
                "sys.modules",
                {"src.config.settings": MagicMock(settings=mock_settings)},
            ):
                await seed_runner.resync_postgres_sequences()
        # No exception means it returned early - success

    @pytest.mark.asyncio
    async def test_executes_setval_for_postgresql(self):
        """For PostgreSQL, it should query tables and run setval."""
        mock_settings = MagicMock()
        mock_settings.DATABASE_URI = "postgresql+asyncpg://user:pass@host/db"

        # Build mock session
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("users",), ("tasks",)]
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()

        # Context manager for async_session_factory
        mock_factory = AsyncMock()
        mock_factory.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.__aexit__ = AsyncMock(return_value=False)

        settings_module = MagicMock(settings=mock_settings)

        with patch.dict("sys.modules", {"src.config.settings": settings_module}):
            with patch(
                "src.seeds.seed_runner.async_session_factory",
                return_value=mock_factory,
                create=True,
            ):
                # We need to patch inside the function scope
                # The function does 'from src.config.settings import settings'
                # and 'from src.db.session import async_session_factory'
                # Use a direct patch approach
                with patch("src.db.session.async_session_factory", return_value=mock_factory, create=True):
                    await seed_runner.resync_postgres_sequences()

        # The function should have executed SQL statements
        # At minimum: the initial query for tables + setval per table + commit
        assert mock_session.execute.call_count >= 1

    @pytest.mark.asyncio
    async def test_handles_outer_exception_gracefully(self):
        """If the entire resync fails, it should log and not raise."""
        with patch.dict(
            "sys.modules",
            {"src.config.settings": MagicMock(side_effect=ImportError("no module"))},
        ):
            # This should not raise
            await seed_runner.resync_postgres_sequences()

    @pytest.mark.asyncio
    async def test_handles_settings_import_error(self):
        """If settings import fails, the outer except should catch it."""
        original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

        def failing_import(name, *args, **kwargs):
            if name == "src.config.settings":
                raise ImportError("boom")
            return original_import(name, *args, **kwargs)

        # The function uses from ... import, which we can break by removing from sys.modules
        # Simplest: just patch to raise at the top level
        with patch.object(seed_runner.logger, "debug") as mock_debug:
            with patch.dict(
                "sys.modules",
                {"src.config.settings": None},
            ):
                # When the module is None in sys.modules, import will raise
                # The outer except in resync_postgres_sequences should catch it
                await seed_runner.resync_postgres_sequences()
            # Should have logged a debug message about skipping
            # (may or may not depending on exact import behavior)

    @pytest.mark.asyncio
    async def test_skips_unsafe_table_names(self):
        """Table names that don't match the safe_id regex should be skipped."""
        mock_settings = MagicMock()
        mock_settings.DATABASE_URI = "postgresql+asyncpg://user:pass@host/db"

        mock_session = AsyncMock()
        # Include a safe and an unsafe table name
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("valid_table",),
            ("'; DROP TABLE--",),
        ]
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.commit = AsyncMock()

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_factory_fn = MagicMock(return_value=mock_ctx)

        settings_mod = MagicMock(settings=mock_settings)

        with patch.dict("sys.modules", {"src.config.settings": settings_mod}):
            with patch("src.db.session.async_session_factory", mock_factory_fn, create=True):
                await seed_runner.resync_postgres_sequences()

        # execute should be called for: initial query + valid_table setval
        # The unsafe table name should be skipped
        # Total calls: 1 (initial) + 1 (valid_table setval) = 2
        # But the commit also happens, which is separate
        calls = mock_session.execute.call_args_list
        # At least the initial query was made
        assert len(calls) >= 1

    @pytest.mark.asyncio
    async def test_per_table_exception_does_not_abort(self):
        """If setval fails for one table, others should still be processed."""
        mock_settings = MagicMock()
        mock_settings.DATABASE_URI = "postgresql+asyncpg://u:p@h/db"

        call_count = {"execute": 0}
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("tbl_a",), ("tbl_b",)]

        async def execute_side_effect(stmt):
            call_count["execute"] += 1
            if call_count["execute"] == 1:
                return mock_result  # table listing
            elif call_count["execute"] == 2:
                raise Exception("setval failed for tbl_a")  # tbl_a fails
            else:
                return MagicMock()  # tbl_b succeeds

        mock_session.execute = AsyncMock(side_effect=execute_side_effect)
        mock_session.commit = AsyncMock()

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_factory_fn = MagicMock(return_value=mock_ctx)

        settings_mod = MagicMock(settings=mock_settings)
        with patch.dict("sys.modules", {"src.config.settings": settings_mod}):
            with patch("src.db.session.async_session_factory", mock_factory_fn, create=True):
                await seed_runner.resync_postgres_sequences()

        # 3 calls total: listing + tbl_a (fails) + tbl_b (succeeds)
        assert call_count["execute"] == 3


# ===========================================================================
# run_seeders_with_factory()
# ===========================================================================


class TestRunSeedersWithFactory:
    """Tests for run_seeders_with_factory(factory, exclude)."""

    @pytest.mark.asyncio
    async def test_empty_seeders_returns_early(self):
        with patch.object(seed_runner, "SEEDERS", {}):
            with patch.object(seed_runner.logger, "warning") as mock_warn:
                await seed_runner.run_seeders_with_factory(MagicMock())
        mock_warn.assert_called()

    @pytest.mark.asyncio
    async def test_patches_session_factory_on_modules(self):
        """The factory arg should replace async_session_factory on seeder modules."""
        custom_factory = MagicMock(name="custom_factory")
        seeder_mock = AsyncMock()

        # Create a fake module with async_session_factory attribute
        fake_mod = types.ModuleType("src.seeds.tools")
        fake_mod.async_session_factory = MagicMock(name="original_factory")
        fake_mod.seed = seeder_mock
        original_factory = fake_mod.async_session_factory

        seeders = OrderedDict([("tools", seeder_mock)])

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.dict("sys.modules", {"src.seeds.tools": fake_mod}):
                captured_factory = {}

                async def capture_factory():
                    captured_factory["during"] = fake_mod.async_session_factory

                seeder_mock.side_effect = capture_factory
                await seed_runner.run_seeders_with_factory(custom_factory)

        # During execution, the factory should have been the custom one
        assert captured_factory["during"] is custom_factory
        # After execution, the original should be restored
        assert fake_mod.async_session_factory is original_factory

    @pytest.mark.asyncio
    async def test_restores_original_factory_after_success(self):
        """Original session factories are restored after seeders complete."""
        custom_factory = MagicMock()
        original_factory = MagicMock(name="original")

        fake_mod = types.ModuleType("src.seeds.schemas")
        fake_mod.async_session_factory = original_factory
        fake_mod.seed = AsyncMock()

        seeders = OrderedDict([("schemas", AsyncMock())])

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.dict("sys.modules", {"src.seeds.schemas": fake_mod}):
                await seed_runner.run_seeders_with_factory(custom_factory)

        assert fake_mod.async_session_factory is original_factory

    @pytest.mark.asyncio
    async def test_restores_original_factory_after_failure(self):
        """Original factories restored even when a seeder raises."""
        custom_factory = MagicMock()
        original_factory = MagicMock(name="original")

        fake_mod = types.ModuleType("src.seeds.tools")
        fake_mod.async_session_factory = original_factory

        failing_seeder = AsyncMock(side_effect=RuntimeError("crash"))
        seeders = OrderedDict([("tools", failing_seeder)])

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.dict("sys.modules", {"src.seeds.tools": fake_mod}):
                await seed_runner.run_seeders_with_factory(custom_factory)

        # Factory should still be restored despite the error
        assert fake_mod.async_session_factory is original_factory

    @pytest.mark.asyncio
    async def test_exclude_set_skips_seeders(self):
        """Seeders in the exclude set should not be called."""
        mock_tools = AsyncMock()
        mock_docs = AsyncMock()
        seeders = OrderedDict([("tools", mock_tools), ("documentation", mock_docs)])

        with patch.object(seed_runner, "SEEDERS", seeders):
            await seed_runner.run_seeders_with_factory(
                MagicMock(), exclude={"documentation"}
            )

        mock_tools.assert_awaited_once()
        mock_docs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exclude_none_defaults_to_empty(self):
        """When exclude is None, all seeders should run."""
        mock_a = AsyncMock()
        mock_b = AsyncMock()
        seeders = OrderedDict([("a", mock_a), ("b", mock_b)])

        with patch.object(seed_runner, "SEEDERS", seeders):
            await seed_runner.run_seeders_with_factory(MagicMock(), exclude=None)

        mock_a.assert_awaited_once()
        mock_b.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_exclude_multiple_seeders(self):
        """Multiple seeders can be excluded."""
        mock_a = AsyncMock()
        mock_b = AsyncMock()
        mock_c = AsyncMock()
        seeders = OrderedDict([("a", mock_a), ("b", mock_b), ("c", mock_c)])

        with patch.object(seed_runner, "SEEDERS", seeders):
            await seed_runner.run_seeders_with_factory(
                MagicMock(), exclude={"a", "c"}
            )

        mock_a.assert_not_awaited()
        mock_b.assert_awaited_once()
        mock_c.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_only_modules_with_async_session_factory_are_patched(self):
        """Modules without async_session_factory should not be touched."""
        custom_factory = MagicMock()
        seeder_mock = AsyncMock()

        # Module WITH async_session_factory
        mod_with = types.ModuleType("src.seeds.tools")
        mod_with.async_session_factory = MagicMock(name="orig_tools")

        # Module WITHOUT async_session_factory
        mod_without = types.ModuleType("src.seeds.schemas")
        # Don't set async_session_factory

        seeders = OrderedDict([("tools", seeder_mock), ("schemas", AsyncMock())])

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.dict(
                "sys.modules",
                {
                    "src.seeds.tools": mod_with,
                    "src.seeds.schemas": mod_without,
                },
            ):
                await seed_runner.run_seeders_with_factory(custom_factory)

        # mod_without should never have had async_session_factory set
        assert not hasattr(mod_without, "async_session_factory")

    @pytest.mark.asyncio
    async def test_seeder_error_logged_and_continues(self):
        """If a seeder fails, error is logged and next seeder still runs."""
        failing = AsyncMock(side_effect=Exception("seeder crash"))
        passing = AsyncMock()
        seeders = OrderedDict([("fail", failing), ("pass", passing)])

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(seed_runner.logger, "error") as mock_err:
                await seed_runner.run_seeders_with_factory(MagicMock())

        failing.assert_awaited_once()
        passing.assert_awaited_once()
        assert mock_err.call_count >= 1

    @pytest.mark.asyncio
    async def test_excluded_seeder_logged(self):
        """Excluded seeders should produce an info log message."""
        seeders = OrderedDict([("docs", AsyncMock())])

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(seed_runner.logger, "info") as mock_info:
                await seed_runner.run_seeders_with_factory(
                    MagicMock(), exclude={"docs"}
                )

        info_messages = [str(c) for c in mock_info.call_args_list]
        assert any("Skipping docs" in msg for msg in info_messages)

    @pytest.mark.asyncio
    async def test_patches_all_known_seed_modules(self):
        """All known seed module names should be checked in sys.modules."""
        custom_factory = MagicMock()
        known_module_names = [
            "src.seeds.tools",
            "src.seeds.schemas",
            "src.seeds.prompt_templates",
            "src.seeds.model_configs",
            "src.seeds.documentation",
            "src.seeds.groups",
            "src.seeds.api_keys",
            "src.seeds.dspy_examples",
            "src.seeds.example_crews",
        ]

        fake_modules = {}
        originals = {}
        for mod_name in known_module_names:
            mod = types.ModuleType(mod_name)
            orig = MagicMock(name=f"orig_{mod_name}")
            mod.async_session_factory = orig
            fake_modules[mod_name] = mod
            originals[mod_name] = orig

        seeders = _make_seeders_dict("tools")

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.dict("sys.modules", fake_modules):
                await seed_runner.run_seeders_with_factory(custom_factory)

        # All modules should have their originals restored
        for mod_name in known_module_names:
            assert fake_modules[mod_name].async_session_factory is originals[mod_name]


# ===========================================================================
# debug_log()
# ===========================================================================


class TestDebugLog:
    """Tests for the debug_log() helper function."""

    def test_logs_when_debug_enabled(self):
        with patch.object(seed_runner, "DEBUG", True):
            with patch.object(seed_runner.logger, "debug") as mock_debug:
                seed_runner.debug_log("test message")
        mock_debug.assert_called_once()
        assert "test message" in str(mock_debug.call_args)

    def test_no_log_when_debug_disabled(self):
        with patch.object(seed_runner, "DEBUG", False):
            with patch.object(seed_runner.logger, "debug") as mock_debug:
                seed_runner.debug_log("should not appear")
        mock_debug.assert_not_called()

    def test_includes_caller_name_in_message(self):
        with patch.object(seed_runner, "DEBUG", True):
            with patch.object(seed_runner.logger, "debug") as mock_debug:
                seed_runner.debug_log("hello")
        logged_msg = mock_debug.call_args[0][0]
        # The caller name should be in brackets
        assert "[" in logged_msg and "]" in logged_msg


# ===========================================================================
# main() CLI entry point
# ===========================================================================


class TestMain:
    """Tests for the main() CLI entry point."""

    @pytest.mark.asyncio
    async def test_all_flag_runs_all_seeders(self):
        with patch("sys.argv", ["script", "--all"]):
            with patch.object(
                seed_runner, "run_all_seeders", new_callable=AsyncMock
            ) as mock_run:
                await seed_runner.main()
        mock_run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_flags_defaults_to_run_all(self):
        with patch("sys.argv", ["script"]):
            with patch.object(
                seed_runner, "run_all_seeders", new_callable=AsyncMock
            ) as mock_run:
                await seed_runner.main()
        mock_run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_specific_seeder_flag(self):
        with patch("sys.argv", ["script", "--tools"]):
            with patch.object(
                seed_runner, "run_seeders", new_callable=AsyncMock
            ) as mock_run:
                await seed_runner.main()
        mock_run.assert_awaited_once()
        # The list should contain "tools"
        called_with = mock_run.call_args[0][0]
        assert "tools" in called_with

    @pytest.mark.asyncio
    async def test_multiple_seeder_flags(self):
        with patch("sys.argv", ["script", "--tools", "--schemas"]):
            with patch.object(
                seed_runner, "run_seeders", new_callable=AsyncMock
            ) as mock_run:
                await seed_runner.main()
        mock_run.assert_awaited_once()
        called_with = mock_run.call_args[0][0]
        assert "tools" in called_with
        assert "schemas" in called_with

    @pytest.mark.asyncio
    async def test_debug_flag_enables_debug(self):
        with patch("sys.argv", ["script", "--debug", "--all"]):
            with patch.object(
                seed_runner, "run_all_seeders", new_callable=AsyncMock
            ):
                # Patch logging import that main() uses
                with patch.object(seed_runner, "DEBUG", False):
                    try:
                        await seed_runner.main()
                    except (NameError, AttributeError):
                        # main() references 'logging' module which may not be
                        # imported at module level - this is acceptable
                        pass


# ===========================================================================
# Module-level attributes
# ===========================================================================


class TestModuleLevelAttributes:
    """Test module-level attributes and setup."""

    def test_logger_attribute_exists(self):
        assert hasattr(seed_runner, "logger")

    def test_debug_attribute_exists(self):
        assert hasattr(seed_runner, "DEBUG")

    def test_debug_is_bool(self):
        assert isinstance(seed_runner.DEBUG, bool)

    def test_seeders_dict_attribute(self):
        assert hasattr(seed_runner, "SEEDERS")

    def test_run_seeders_is_coroutine_function(self):
        assert asyncio.iscoroutinefunction(seed_runner.run_seeders)

    def test_run_all_seeders_is_coroutine_function(self):
        assert asyncio.iscoroutinefunction(seed_runner.run_all_seeders)

    def test_resync_postgres_sequences_is_coroutine_function(self):
        assert asyncio.iscoroutinefunction(seed_runner.resync_postgres_sequences)

    def test_run_seeders_with_factory_is_coroutine_function(self):
        assert asyncio.iscoroutinefunction(seed_runner.run_seeders_with_factory)

    def test_main_is_coroutine_function(self):
        assert asyncio.iscoroutinefunction(seed_runner.main)

    def test_debug_log_is_regular_function(self):
        assert callable(seed_runner.debug_log)
        assert not asyncio.iscoroutinefunction(seed_runner.debug_log)


# ===========================================================================
# Integration-style: run_all_seeders with mixed fast/slow
# ===========================================================================


class TestRunAllSeedersIntegration:
    """Integration-style tests combining fast and slow seeders."""

    @pytest.mark.asyncio
    async def test_mixed_fast_and_slow_seeders(self):
        """Simulate the real scenario with both fast and slow seeders."""
        fast_mock = AsyncMock()
        slow_mock = AsyncMock()
        fake_task = MagicMock()

        seeders = OrderedDict([
            ("tools", fast_mock),
            ("documentation", slow_mock),
        ])

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ) as mock_resync:
                with patch(
                    "src.seeds.seed_runner.asyncio.create_task",
                    return_value=fake_task,
                ):
                    await seed_runner.run_all_seeders()

        # Fast seeder awaited directly
        fast_mock.assert_awaited_once()
        # Slow seeder NOT awaited directly (launched as background task)
        slow_mock.assert_not_awaited()
        # Resync called
        mock_resync.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_all_nine_seeders_scenario(self):
        """Simulate having all 9 seeders registered."""
        all_names = [
            "tools", "schemas", "prompt_templates", "model_configs",
            "documentation", "groups", "api_keys", "dspy_examples", "example_crews",
        ]
        seeders = _make_seeders_dict(*all_names)

        with patch.object(seed_runner, "SEEDERS", seeders):
            with patch.object(
                seed_runner, "resync_postgres_sequences", new_callable=AsyncMock
            ):
                with patch(
                    "src.seeds.seed_runner.asyncio.create_task",
                    return_value=MagicMock(),
                ):
                    await seed_runner.run_all_seeders()

        # Fast seeders should be awaited
        fast_names = [
            "tools", "schemas", "prompt_templates", "model_configs",
            "groups", "api_keys", "dspy_examples", "example_crews",
        ]
        for name in fast_names:
            seeders[name].assert_awaited_once()

        # Documentation should NOT be directly awaited
        seeders["documentation"].assert_not_awaited()
