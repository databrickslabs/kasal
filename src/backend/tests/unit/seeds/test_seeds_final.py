"""
Unit tests for seed runner module and seed data integrity.

Tests seed runner functions, seeder registration, and data validation
across all seed modules.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.seeds import seed_runner, model_configs, prompt_templates


class TestSeedRunnerModuleAttributes:
    """Test seed_runner module-level attributes."""

    def test_logger_exists(self):
        """Test that the logger attribute is present."""
        assert hasattr(seed_runner, "logger")

    def test_debug_flag_exists(self):
        """Test that the DEBUG flag is present."""
        assert hasattr(seed_runner, "DEBUG")

    def test_seeders_dict_exists(self):
        """Test that the SEEDERS dictionary is present."""
        assert hasattr(seed_runner, "SEEDERS")
        assert isinstance(seed_runner.SEEDERS, dict)

    def test_seeders_dict_not_empty(self):
        """Test that SEEDERS has registered seeders."""
        assert len(seed_runner.SEEDERS) > 0

    def test_expected_seeders_registered(self):
        """Test that core seeders are registered."""
        expected = [
            "tools", "schemas", "prompt_templates",
            "model_configs", "documentation", "groups", "api_keys",
        ]
        for name in expected:
            assert name in seed_runner.SEEDERS, f"Missing seeder: {name}"

    def test_all_seeders_are_callable(self):
        """Test that all registered seeders are callable."""
        for name, func in seed_runner.SEEDERS.items():
            assert callable(func), f"Seeder '{name}' is not callable"


class TestRunSeedersFunction:
    """Test cases for the run_seeders function."""

    @pytest.mark.asyncio
    async def test_run_seeders_with_valid_seeder(self):
        """Test running a specific seeder by name."""
        mock_seeder = AsyncMock()
        with patch.object(seed_runner, "SEEDERS", {"test_seeder": mock_seeder}):
            await seed_runner.run_seeders(["test_seeder"])
        mock_seeder.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_seeders_with_unknown_seeder(self):
        """Test that unknown seeders are logged as warnings."""
        with patch.object(seed_runner.logger, "warning") as mock_warn:
            await seed_runner.run_seeders(["nonexistent_seeder"])
        mock_warn.assert_called_with("Unknown seeder: nonexistent_seeder")

    @pytest.mark.asyncio
    async def test_run_seeders_continues_on_error(self):
        """Test that run_seeders continues after a seeder fails."""
        failing_seeder = AsyncMock(side_effect=Exception("fail"))
        passing_seeder = AsyncMock()
        with patch.object(
            seed_runner, "SEEDERS",
            {"fail_seeder": failing_seeder, "pass_seeder": passing_seeder},
        ):
            await seed_runner.run_seeders(["fail_seeder", "pass_seeder"])
        failing_seeder.assert_awaited_once()
        passing_seeder.assert_awaited_once()


class TestRunAllSeedersFunction:
    """Test cases for the run_all_seeders function."""

    @pytest.mark.asyncio
    async def test_run_all_seeders_empty(self):
        """Test run_all_seeders with no registered seeders."""
        with patch.object(seed_runner, "SEEDERS", {}):
            with patch.object(seed_runner.logger, "warning") as mock_warn:
                await seed_runner.run_all_seeders()
            mock_warn.assert_called()

    @pytest.mark.asyncio
    async def test_run_all_seeders_runs_fast_seeders(self):
        """Test that run_all_seeders executes fast seeders."""
        mock_seeder = AsyncMock()
        with patch.object(seed_runner, "SEEDERS", {"tools": mock_seeder}):
            await seed_runner.run_all_seeders()
        mock_seeder.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_all_seeders_handles_exception(self):
        """Test that run_all_seeders handles exceptions gracefully."""
        failing_seeder = AsyncMock(side_effect=Exception("boom"))
        with patch.object(seed_runner, "SEEDERS", {"tools": failing_seeder}):
            with patch.object(seed_runner.logger, "error"):
                await seed_runner.run_all_seeders()
        failing_seeder.assert_awaited_once()


class TestMainFunction:
    """Test cases for the main() CLI entry point."""

    @pytest.mark.asyncio
    async def test_main_with_all_flag(self):
        """Test main() with --all flag."""
        with patch("sys.argv", ["script", "--all"]):
            with patch.object(
                seed_runner, "run_all_seeders", new_callable=AsyncMock
            ) as mock_run:
                await seed_runner.main()
                mock_run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_main_no_flags_runs_all(self):
        """Test main() with no flags defaults to run_all_seeders."""
        with patch("sys.argv", ["script"]):
            with patch.object(
                seed_runner, "run_all_seeders", new_callable=AsyncMock
            ) as mock_run:
                await seed_runner.main()
                mock_run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_main_with_specific_seeder(self):
        """Test main() with a specific seeder flag."""
        with patch("sys.argv", ["script", "--tools"]):
            with patch.object(
                seed_runner, "run_seeders", new_callable=AsyncMock
            ) as mock_run:
                await seed_runner.main()
                mock_run.assert_awaited_once()


class TestDebugLogFunction:
    """Test cases for the debug_log helper."""

    def test_debug_log_when_debug_enabled(self):
        """Test debug_log outputs when DEBUG is True."""
        with patch.object(seed_runner, "DEBUG", True):
            with patch.object(seed_runner.logger, "debug") as mock_debug:
                with patch("inspect.currentframe") as mock_frame:
                    mock_frame.return_value.f_back.f_code.co_name = "test_caller"
                    seed_runner.debug_log("test message")
                mock_debug.assert_called_once()


class TestModelConfigsDataIntegrity:
    """Cross-module data integrity tests for model_configs."""

    def test_model_configs_has_default_models(self):
        """Test that model_configs exposes DEFAULT_MODELS."""
        assert hasattr(model_configs, "DEFAULT_MODELS")
        assert len(model_configs.DEFAULT_MODELS) > 0

    def test_model_configs_has_seed_functions(self):
        """Test that model_configs exposes seed functions."""
        assert callable(model_configs.seed)
        assert callable(model_configs.seed_async)

    def test_model_configs_logger(self):
        """Test that model_configs has a logger."""
        import logging
        assert hasattr(model_configs, "logger")
        assert isinstance(model_configs.logger, logging.Logger)


class TestPromptTemplatesDataIntegrity:
    """Cross-module data integrity tests for prompt_templates."""

    def test_prompt_templates_has_default_templates(self):
        """Test that prompt_templates exposes DEFAULT_TEMPLATES."""
        assert hasattr(prompt_templates, "DEFAULT_TEMPLATES")
        assert len(prompt_templates.DEFAULT_TEMPLATES) > 0

    def test_prompt_templates_has_seed_functions(self):
        """Test that prompt_templates exposes seed functions."""
        assert callable(prompt_templates.seed)
        assert callable(prompt_templates.seed_async)

    def test_prompt_templates_no_todo_or_fixme(self):
        """Test that template content has no TODO or FIXME markers."""
        for tpl in prompt_templates.DEFAULT_TEMPLATES:
            content = tpl["template"]
            assert "TODO" not in content, (
                f"Template '{tpl['name']}' contains TODO"
            )
            assert "FIXME" not in content, (
                f"Template '{tpl['name']}' contains FIXME"
            )


class TestAllSeedModuleFunctions:
    """Test that all seed modules expose expected functions."""

    def test_seed_runner_main_callable(self):
        """Test seed_runner.main is callable."""
        assert callable(seed_runner.main)

    def test_seed_runner_run_all_seeders_callable(self):
        """Test seed_runner.run_all_seeders is callable."""
        assert callable(seed_runner.run_all_seeders)

    def test_seed_runner_run_seeders_callable(self):
        """Test seed_runner.run_seeders is callable."""
        assert callable(seed_runner.run_seeders)
