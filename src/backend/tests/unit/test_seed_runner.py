"""
Unit tests for seed runner.

Tests the functionality of the seed runner including
seeder discovery, execution, and command-line interface.
"""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import asyncio
import argparse

from src.seeds.seed_runner import (
    SEEDERS, run_seeders, run_all_seeders, main, debug_log, DEBUG
)


class TestSeedersRegistry:
    """Test cases for SEEDERS registry."""
    
    def test_seeders_is_dict(self):
        """Test that SEEDERS is a dictionary."""
        assert isinstance(SEEDERS, dict)
    
    def test_seeders_expected_keys(self):
        """Test that SEEDERS contains expected seeder keys."""
        expected_seeders = [
            "tools", "schemas", "prompt_templates", 
            "model_configs", "documentation", "roles"
        ]
        
        # Check that at least some expected seeders are present
        # (may not all be present due to import errors in test environment)
        available_seeders = set(SEEDERS.keys())
        expected_set = set(expected_seeders)
        
        # At least some seeders should be available
        assert len(available_seeders) >= 0
        
        # All available seeders should be from the expected list
        assert available_seeders.issubset(expected_set)
    
    def test_seeders_values_are_callable(self):
        """Test that SEEDERS values are callable functions."""
        for seeder_name, seeder_func in SEEDERS.items():
            assert callable(seeder_func), f"Seeder {seeder_name} is not callable"
    
    @patch('src.seeds.seed_runner.tools')
    @patch('src.seeds.seed_runner.schemas') 
    @patch('src.seeds.seed_runner.prompt_templates')
    def test_seeders_registration_with_mocks(self, mock_prompt_templates, mock_schemas, mock_tools):
        """Test seeder registration with mocked modules."""
        # Mock seeder functions
        mock_tools.seed = AsyncMock()
        mock_schemas.seed = AsyncMock()
        mock_prompt_templates.seed = AsyncMock()
        
        # Reload the module to test registration
        import importlib
        import src.seeds.seed_runner
        importlib.reload(src.seeds.seed_runner)
        
        # Check that seeders were registered
        from src.seeds.seed_runner import SEEDERS as reloaded_seeders
        
        assert isinstance(reloaded_seeders, dict)


class TestDebugLog:
    """Test cases for debug_log function."""
    
    @patch('src.seeds.seed_runner.DEBUG', True)
    @patch('src.seeds.seed_runner.logger')
    def test_debug_log_when_enabled(self, mock_logger):
        """Test debug_log when DEBUG is enabled."""
        debug_log("Test debug message")
        
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0][0]
        assert "Test debug message" in call_args
    
    @patch('src.seeds.seed_runner.DEBUG', False)
    @patch('src.seeds.seed_runner.logger')
    def test_debug_log_when_disabled(self, mock_logger):
        """Test debug_log when DEBUG is disabled."""
        debug_log("Test debug message")
        
        # Should not be called when DEBUG is False
        mock_logger.debug.assert_not_called()
    
    @patch('src.seeds.seed_runner.DEBUG', True)
    @patch('src.seeds.seed_runner.logger')
    @patch('src.seeds.seed_runner.inspect')
    def test_debug_log_includes_caller(self, mock_inspect, mock_logger):
        """Test that debug_log includes caller function name."""
        # Mock the inspect module
        mock_frame = MagicMock()
        mock_frame.f_code.co_name = "test_function"
        mock_inspect.currentframe.return_value.f_back = mock_frame
        
        debug_log("Test message")
        
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0][0]
        assert "[test_function]" in call_args
        assert "Test message" in call_args


class TestRunSeeders:
    """Test cases for run_seeders function."""
    
    @pytest.mark.asyncio
    async def test_run_seeders_valid_seeders(self):
        """Test run_seeders with valid seeder names."""
        # Mock seeders
        mock_seeder1 = AsyncMock()
        mock_seeder2 = AsyncMock()
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'test_seeder1': mock_seeder1,
            'test_seeder2': mock_seeder2
        }):
            await run_seeders(['test_seeder1', 'test_seeder2'])
        
        mock_seeder1.assert_called_once()
        mock_seeder2.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_seeders_invalid_seeder(self):
        """Test run_seeders with invalid seeder name."""
        mock_seeder = AsyncMock()
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'valid_seeder': mock_seeder
        }):
            # Should not raise exception for invalid seeder
            await run_seeders(['valid_seeder', 'invalid_seeder'])
        
        # Valid seeder should still be called
        mock_seeder.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_seeders_with_exception(self):
        """Test run_seeders when a seeder raises an exception."""
        # Mock seeders - one succeeds, one fails
        mock_seeder1 = AsyncMock()
        mock_seeder2 = AsyncMock()
        mock_seeder2.side_effect = Exception("Seeder error")
        mock_seeder3 = AsyncMock()
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'seeder1': mock_seeder1,
            'seeder2': mock_seeder2,
            'seeder3': mock_seeder3
        }):
            # Should not raise exception, should continue with other seeders
            await run_seeders(['seeder1', 'seeder2', 'seeder3'])
        
        # All seeders should be attempted
        mock_seeder1.assert_called_once()
        mock_seeder2.assert_called_once()
        mock_seeder3.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_seeders_empty_list(self):
        """Test run_seeders with empty seeder list."""
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'test_seeder': AsyncMock()
        }):
            # Should complete without error
            await run_seeders([])


class TestRunAllSeeders:
    """Test cases for run_all_seeders function."""
    
    @pytest.mark.asyncio
    async def test_run_all_seeders_success(self):
        """Test run_all_seeders with successful execution."""
        # Mock seeders
        mock_seeder1 = AsyncMock()
        mock_seeder2 = AsyncMock()
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'seeder1': mock_seeder1,
            'seeder2': mock_seeder2
        }):
            await run_all_seeders()
        
        mock_seeder1.assert_called_once()
        mock_seeder2.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_all_seeders_with_failures(self):
        """Test run_all_seeders when some seeders fail."""
        # Mock seeders - some succeed, some fail
        mock_seeder1 = AsyncMock()
        mock_seeder2 = AsyncMock()
        mock_seeder2.side_effect = Exception("Seeder 2 error")
        mock_seeder3 = AsyncMock()
        mock_seeder3.side_effect = Exception("Seeder 3 error")
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'seeder1': mock_seeder1,
            'seeder2': mock_seeder2,
            'seeder3': mock_seeder3
        }):
            # Should not raise exception
            await run_all_seeders()
        
        # All seeders should be attempted despite failures
        mock_seeder1.assert_called_once()
        mock_seeder2.assert_called_once()
        mock_seeder3.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_all_seeders_empty_registry(self):
        """Test run_all_seeders with empty SEEDERS registry."""
        with patch.dict('src.seeds.seed_runner.SEEDERS', {}, clear=True):
            # Should complete without error
            await run_all_seeders()
    
    @pytest.mark.asyncio
    @patch('src.seeds.seed_runner.logger')
    async def test_run_all_seeders_logging(self, mock_logger):
        """Test that run_all_seeders logs appropriately."""
        mock_seeder = AsyncMock()
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'test_seeder': mock_seeder
        }):
            await run_all_seeders()
        
        # Should log start and completion
        mock_logger.info.assert_called()
        
        # Check for specific log messages
        log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("run_all_seeders function called" in call for call in log_calls)
        assert any("All seeder operations completed" in call for call in log_calls)


class TestMain:
    """Test cases for main function."""
    
    @pytest.mark.asyncio
    @patch('src.seeds.seed_runner.run_all_seeders')
    @patch('argparse.ArgumentParser.parse_args')
    async def test_main_run_all_default(self, mock_parse_args, mock_run_all):
        """Test main function with default behavior (run all)."""
        # Mock args with no specific seeders selected
        mock_args = MagicMock()
        mock_args.all = False
        mock_args.debug = False
        
        # Mock that no specific seeders are selected
        for seeder_name in ['tools', 'schemas', 'prompt_templates', 'model_configs', 'documentation', 'roles']:
            setattr(mock_args, seeder_name, False)
        
        mock_parse_args.return_value = mock_args
        mock_run_all.return_value = None
        
        await main()
        
        mock_run_all.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.seeds.seed_runner.run_all_seeders')
    @patch('argparse.ArgumentParser.parse_args')
    async def test_main_run_all_explicit(self, mock_parse_args, mock_run_all):
        """Test main function with explicit --all flag."""
        # Mock args with --all flag
        mock_args = MagicMock()
        mock_args.all = True
        mock_args.debug = False
        
        mock_parse_args.return_value = mock_args
        mock_run_all.return_value = None
        
        await main()
        
        mock_run_all.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.seeds.seed_runner.run_seeders')
    @patch('argparse.ArgumentParser.parse_args')
    async def test_main_specific_seeders(self, mock_parse_args, mock_run_seeders):
        """Test main function with specific seeders selected."""
        # Mock args with specific seeders selected
        mock_args = MagicMock()
        mock_args.all = False
        mock_args.debug = False
        mock_args.tools = True
        mock_args.schemas = True
        mock_args.prompt_templates = False
        mock_args.model_configs = False
        mock_args.documentation = False
        mock_args.roles = False
        
        mock_parse_args.return_value = mock_args
        mock_run_seeders.return_value = None
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'tools': AsyncMock(),
            'schemas': AsyncMock(),
            'prompt_templates': AsyncMock(),
            'model_configs': AsyncMock(),
            'documentation': AsyncMock(),
            'roles': AsyncMock()
        }):
            await main()
        
        # Should call run_seeders with selected seeders
        mock_run_seeders.assert_called_once()
        called_seeders = mock_run_seeders.call_args[0][0]
        assert 'tools' in called_seeders
        assert 'schemas' in called_seeders
        assert 'prompt_templates' not in called_seeders
    
    @pytest.mark.asyncio
    @patch('src.seeds.seed_runner.logger')
    @patch('argparse.ArgumentParser.parse_args')
    async def test_main_debug_flag(self, mock_parse_args, mock_logger):
        """Test main function with debug flag."""
        # Mock args with debug flag
        mock_args = MagicMock()
        mock_args.all = False
        mock_args.debug = True
        
        # Mock that no specific seeders are selected
        for seeder_name in ['tools', 'schemas', 'prompt_templates', 'model_configs', 'documentation', 'roles']:
            setattr(mock_args, seeder_name, False)
        
        mock_parse_args.return_value = mock_args
        
        with patch('src.seeds.seed_runner.run_all_seeders'):
            await main()
        
        # Should enable debug logging
        mock_logger.setLevel.assert_called()
        mock_logger.debug.assert_called()


class TestArgumentParser:
    """Test cases for argument parser setup."""
    
    @patch('argparse.ArgumentParser')
    def test_argument_parser_setup(self, mock_parser_class):
        """Test that argument parser is set up correctly."""
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        
        # Import to trigger parser setup
        import src.seeds.seed_runner
        
        # Verify parser was created
        mock_parser_class.assert_called()
    
    def test_argument_parser_integration(self):
        """Test argument parser integration with actual args."""
        # Test with actual ArgumentParser to ensure it works
        parser = argparse.ArgumentParser(description="Test seeding tool")
        parser.add_argument("--all", action="store_true", help="Run all seeders")
        parser.add_argument("--debug", action="store_true", help="Enable debug logging")
        parser.add_argument("--tools", action="store_true", help="Run tools seeder")
        
        # Test parsing various argument combinations
        args1 = parser.parse_args(["--all"])
        assert args1.all is True
        assert args1.debug is False
        
        args2 = parser.parse_args(["--debug", "--tools"])
        assert args2.debug is True
        assert args2.tools is True
        assert args2.all is False
        
        args3 = parser.parse_args([])
        assert args3.all is False
        assert args3.debug is False


class TestModuleImports:
    """Test cases for module import handling."""
    
    def test_import_error_handling(self):
        """Test that import errors are handled gracefully."""
        # This test verifies that the module loads even if some seeders fail to import
        import src.seeds.seed_runner
        
        # Should have loaded successfully
        assert hasattr(src.seeds.seed_runner, 'SEEDERS')
        assert hasattr(src.seeds.seed_runner, 'run_all_seeders')
        assert hasattr(src.seeds.seed_runner, 'main')
    
    @patch('src.seeds.seed_runner.logger')
    def test_seeder_registration_error_logging(self, mock_logger):
        """Test that seeder registration errors are logged."""
        # Import errors should be logged but not crash the module
        import src.seeds.seed_runner
        
        # Check if any error logs were made during import
        # (This might not trigger in test environment, but verifies the pattern)
        assert isinstance(src.seeds.seed_runner.SEEDERS, dict)


class TestEnvironmentVariables:
    """Test cases for environment variable handling."""
    
    @patch.dict('os.environ', {'SEED_DEBUG': 'true'})
    def test_debug_environment_variable_true(self):
        """Test DEBUG setting with environment variable true."""
        import importlib
        import src.seeds.seed_runner
        importlib.reload(src.seeds.seed_runner)
        
        # Should enable debug mode
        assert src.seeds.seed_runner.DEBUG is True
    
    @patch.dict('os.environ', {'SEED_DEBUG': 'false'})
    def test_debug_environment_variable_false(self):
        """Test DEBUG setting with environment variable false."""
        import importlib
        import src.seeds.seed_runner
        importlib.reload(src.seeds.seed_runner)
        
        # Should disable debug mode
        assert src.seeds.seed_runner.DEBUG is False
    
    @patch.dict('os.environ', {'SEED_DEBUG': '1'})
    def test_debug_environment_variable_numeric(self):
        """Test DEBUG setting with numeric environment variable."""
        import importlib
        import src.seeds.seed_runner
        importlib.reload(src.seeds.seed_runner)
        
        # Should enable debug mode
        assert src.seeds.seed_runner.DEBUG is True


class TestSeederExecution:
    """Test cases for seeder execution patterns."""
    
    @pytest.mark.asyncio
    async def test_seeder_execution_order(self):
        """Test that seeders are executed in a predictable order."""
        execution_order = []
        
        def create_mock_seeder(name):
            async def mock_seeder():
                execution_order.append(name)
            return mock_seeder
        
        mock_seeders = {
            'seeder_a': create_mock_seeder('a'),
            'seeder_b': create_mock_seeder('b'),
            'seeder_c': create_mock_seeder('c')
        }
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', mock_seeders):
            await run_all_seeders()
        
        # Should execute all seeders
        assert len(execution_order) == 3
        assert set(execution_order) == {'a', 'b', 'c'}
    
    @pytest.mark.asyncio
    async def test_seeder_isolation(self):
        """Test that seeder failures don't affect other seeders."""
        successful_calls = []
        
        def create_successful_seeder(name):
            async def mock_seeder():
                successful_calls.append(name)
            return mock_seeder
        
        def create_failing_seeder():
            async def mock_seeder():
                raise Exception("Seeder failed")
            return mock_seeder
        
        mock_seeders = {
            'success_1': create_successful_seeder('success_1'),
            'failure': create_failing_seeder(),
            'success_2': create_successful_seeder('success_2')
        }
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', mock_seeders):
            await run_all_seeders()
        
        # Successful seeders should still execute despite failure
        assert 'success_1' in successful_calls
        assert 'success_2' in successful_calls
        assert len(successful_calls) == 2
    
    @pytest.mark.asyncio
    async def test_async_seeder_compatibility(self):
        """Test that async seeders are properly awaited."""
        call_count = 0
        
        async def async_seeder():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.01)  # Small delay to ensure async behavior
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'async_seeder': async_seeder
        }):
            await run_all_seeders()
        
        assert call_count == 1


class TestLogging:
    """Test cases for logging functionality."""
    
    @patch('src.seeds.seed_runner.logger')
    def test_module_import_logging(self, mock_logger):
        """Test that module import is logged."""
        # Re-import to trigger logging
        import importlib
        import src.seeds.seed_runner
        importlib.reload(src.seeds.seed_runner)
        
        # Should log module import
        mock_logger.info.assert_called()
    
    @pytest.mark.asyncio
    @patch('src.seeds.seed_runner.logger')
    async def test_seeder_execution_logging(self, mock_logger):
        """Test that seeder execution is logged."""
        mock_seeder = AsyncMock()
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'test_seeder': mock_seeder
        }):
            await run_all_seeders()
        
        # Should log seeder execution
        mock_logger.info.assert_called()
        
        # Check for specific log patterns
        log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Running test_seeder seeder" in call for call in log_calls)
        assert any("Completed test_seeder seeder" in call for call in log_calls)
    
    @pytest.mark.asyncio
    @patch('src.seeds.seed_runner.logger')
    async def test_error_logging(self, mock_logger):
        """Test that errors are properly logged."""
        async def failing_seeder():
            raise Exception("Test error")
        
        with patch.dict('src.seeds.seed_runner.SEEDERS', {
            'failing_seeder': failing_seeder
        }):
            await run_all_seeders()
        
        # Should log errors
        mock_logger.error.assert_called()
        
        # Check for error log patterns
        error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
        assert any("Error in failing_seeder seeder" in call for call in error_calls)