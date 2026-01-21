"""
Unit tests for CrewAI callback manager module.
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any

from src.engines.crewai.flow.modules.callback_manager import CallbackManager


class TestCallbackManager:
    """Test cases for CallbackManager class."""

    @pytest.fixture
    def mock_job_id(self):
        """Mock job ID for testing."""
        return "test_job_123"

    @pytest.fixture
    def mock_config(self):
        """Mock configuration for testing."""
        return {"key": "value"}

    @pytest.fixture
    def mock_group_context(self):
        """Mock group context for testing."""
        return {"tenant_id": "test_tenant"}

    @pytest.fixture
    def mock_crewai_event_bus(self):
        """Mock CrewAI event bus."""
        with patch('src.engines.crewai.flow.modules.callback_manager.crewai_event_bus') as mock_bus:
            mock_bus.register = Mock()
            yield mock_bus

    def test_init_callbacks_success(self, mock_job_id, mock_config, mock_group_context, mock_crewai_event_bus):
        """Test successful callback initialization."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            # Mock callback instances
            job_cb = Mock()
            event_cb = Mock()
            trace_cb = Mock()
            
            mock_job_callback.return_value = job_cb
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(
                job_id=mock_job_id,
                config=mock_config,
                group_context=mock_group_context
            )
            
            assert 'handlers' in result
            assert 'streaming' in result
            assert 'event_streaming' in result
            assert 'agent_trace' in result
            assert 'start_trace_writer' in result
            
            assert len(result['handlers']) == 3
            assert job_cb in result['handlers']
            assert event_cb in result['handlers']
            assert trace_cb in result['handlers']
            
            # Check that callbacks were created with correct parameters
            mock_job_callback.assert_called_once_with(
                job_id=mock_job_id,
                max_retries=3,
                group_context=mock_group_context
            )
            mock_event_callback.assert_called_once_with(
                job_id=mock_job_id,
                config=mock_config,
                group_context=mock_group_context
            )
            mock_trace_callback.assert_called_once_with(
                job_id=mock_job_id,
                group_context=mock_group_context
            )

    def test_init_callbacks_no_job_id(self):
        """Test callback initialization without job ID."""
        result = CallbackManager.init_callbacks()
        
        assert result == {'handlers': []}

    def test_init_callbacks_none_job_id(self):
        """Test callback initialization with None job ID."""
        result = CallbackManager.init_callbacks(job_id=None)
        
        assert result == {'handlers': []}

    def test_init_callbacks_job_output_callback_error(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization when JobOutputCallback creation fails."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            # Make JobOutputCallback creation fail
            mock_job_callback.side_effect = Exception("JobOutputCallback error")
            
            event_cb = Mock()
            trace_cb = Mock()
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should still return result with other callbacks
            assert 'handlers' in result
            assert 'event_streaming' in result
            assert 'agent_trace' in result
            assert 'streaming' not in result  # Failed to create
            
            assert len(result['handlers']) == 2  # Only event and trace callbacks

    def test_init_callbacks_event_streaming_callback_error(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization when EventStreamingCallback creation fails."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            job_cb = Mock()
            trace_cb = Mock()
            mock_job_callback.return_value = job_cb
            mock_event_callback.side_effect = Exception("EventStreamingCallback error")
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should still return result with other callbacks
            assert 'handlers' in result
            assert 'streaming' in result
            assert 'agent_trace' in result
            assert 'event_streaming' not in result  # Failed to create
            
            assert len(result['handlers']) == 2  # Only job and trace callbacks

    def test_init_callbacks_agent_trace_callback_error(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization when AgentTraceEventListener creation fails."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            job_cb = Mock()
            event_cb = Mock()
            mock_job_callback.return_value = job_cb
            mock_event_callback.return_value = event_cb
            mock_trace_callback.side_effect = Exception("AgentTraceEventListener error")
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should still return result with other callbacks
            assert 'handlers' in result
            assert 'streaming' in result
            assert 'event_streaming' in result
            assert 'agent_trace' not in result  # Failed to create
            
            assert len(result['handlers']) == 2  # Only job and event callbacks

    def test_init_callbacks_all_creation_errors(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization when all callback creations fail."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            # Make all callback creations fail
            mock_job_callback.side_effect = Exception("JobOutputCallback error")
            mock_event_callback.side_effect = Exception("EventStreamingCallback error")
            mock_trace_callback.side_effect = Exception("AgentTraceEventListener error")
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should still return basic structure
            assert 'handlers' in result
            assert len(result['handlers']) == 0

    def test_init_callbacks_general_exception(self, mock_job_id):
        """Test callback initialization with general exception."""
        # Patch the handlers list initialization to raise an exception in the try block
        with patch('src.engines.crewai.flow.modules.callback_manager.CallbackManager.ensure_event_listeners_registered') as mock_ensure:
            mock_ensure.side_effect = Exception("General error during initialization")
            
            # Mock callbacks to be created successfully
            with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
                 patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
                 patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
                
                job_cb = Mock(spec=['event_bus'])
                event_cb = Mock(spec=['event_bus'])
                trace_cb = Mock(spec=['event_bus'])
                
                mock_job_callback.return_value = job_cb
                mock_event_callback.return_value = event_cb
                mock_trace_callback.return_value = trace_cb
                
                result = CallbackManager.init_callbacks(job_id=mock_job_id)
                
                # Should return empty handlers on general failure
                assert result == {'handlers': []}

    def test_ensure_event_listeners_registered_empty_list(self):
        """Test ensuring event listeners registration with empty list."""
        CallbackManager.ensure_event_listeners_registered([])
        # Should not raise any exceptions

    def test_ensure_event_listeners_registered_none(self):
        """Test ensuring event listeners registration with None."""
        CallbackManager.ensure_event_listeners_registered(None)
        # Should not raise any exceptions

    def test_ensure_event_listeners_registered_success(self, mock_crewai_event_bus):
        """Test successful event listeners registration."""
        # Mock listeners with setup_listeners method
        listener1 = Mock()
        listener1.event_bus = None
        listener1.setup_listeners = Mock()
        listener1.connect_events = Mock()
        
        listener2 = Mock()
        listener2.event_bus = mock_crewai_event_bus
        listener2.setup_listeners = Mock()
        listener2.connect_events = Mock()
        
        listeners = [listener1, listener2]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Check that event_bus was set for listener1
        assert listener1.event_bus == mock_crewai_event_bus
        
        # Check that setup_listeners was called for both
        listener1.setup_listeners.assert_called_once_with(mock_crewai_event_bus)
        listener2.setup_listeners.assert_called_once_with(mock_crewai_event_bus)
        
        # Check that connect_events was called for both
        listener1.connect_events.assert_called_once()
        listener2.connect_events.assert_called_once()

    def test_ensure_event_listeners_registered_direct_registration(self, mock_crewai_event_bus):
        """Test event listeners registration via direct event bus registration."""
        # Mock listener without setup_listeners but with event bus register method
        listener = Mock(spec=['event_bus', 'connect_events'])
        listener.event_bus = None
        listener.connect_events = Mock()
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should try direct registration
        mock_crewai_event_bus.register.assert_called_once_with(listener)
        # Should also call connect_events
        listener.connect_events.assert_called_once()

    def test_ensure_event_listeners_registered_no_methods(self, mock_crewai_event_bus):
        """Test event listeners registration when listener has no registration methods."""
        # Mock listener without setup_listeners and event bus without register
        listener = Mock()
        listener.event_bus = None
        del listener.setup_listeners  # Remove the method
        
        # Remove register method from event bus
        del mock_crewai_event_bus.register
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should handle gracefully without errors

    def test_ensure_event_listeners_registered_setup_error(self, mock_crewai_event_bus):
        """Test event listeners registration when setup_listeners fails."""
        listener = Mock()
        listener.event_bus = None
        listener.setup_listeners = Mock(side_effect=Exception("Setup error"))
        listener.connect_events = Mock()
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should handle error gracefully and still try connect_events
        listener.connect_events.assert_called_once()

    def test_ensure_event_listeners_registered_connect_error(self, mock_crewai_event_bus):
        """Test event listeners registration when connect_events fails."""
        listener = Mock()
        listener.event_bus = None
        listener.setup_listeners = Mock()
        listener.connect_events = Mock(side_effect=Exception("Connect error"))
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should handle error gracefully
        listener.setup_listeners.assert_called_once()

    def test_ensure_event_listeners_registered_general_error(self, mock_crewai_event_bus):
        """Test event listeners registration with general error."""
        with patch('src.engines.crewai.flow.modules.callback_manager.crewai_event_bus', side_effect=Exception("General error")):
            listener = Mock()
            listeners = [listener]
            
            CallbackManager.ensure_event_listeners_registered(listeners)
            
            # Should handle general error gracefully

    def test_cleanup_callbacks_empty(self):
        """Test cleaning up empty callbacks."""
        CallbackManager.cleanup_callbacks({})
        # Should not raise any exceptions

    def test_cleanup_callbacks_none(self):
        """Test cleaning up None callbacks."""
        CallbackManager.cleanup_callbacks(None)
        # Should not raise any exceptions

    def test_cleanup_callbacks_success(self):
        """Test successful callback cleanup."""
        event_streaming_cb = Mock()
        event_streaming_cb.cleanup = Mock()
        
        agent_trace_cb = Mock()
        
        callbacks = {
            'event_streaming': event_streaming_cb,
            'agent_trace': agent_trace_cb
        }
        
        CallbackManager.cleanup_callbacks(callbacks)
        
        # Should call cleanup on event streaming callback
        event_streaming_cb.cleanup.assert_called_once()

    def test_cleanup_callbacks_no_event_streaming(self):
        """Test callback cleanup without event streaming callback."""
        agent_trace_cb = Mock()
        
        callbacks = {
            'agent_trace': agent_trace_cb
        }
        
        CallbackManager.cleanup_callbacks(callbacks)
        
        # Should complete without errors

    def test_cleanup_callbacks_cleanup_error(self):
        """Test callback cleanup when cleanup method fails."""
        event_streaming_cb = Mock()
        event_streaming_cb.cleanup = Mock(side_effect=Exception("Cleanup error"))
        
        callbacks = {
            'event_streaming': event_streaming_cb
        }
        
        CallbackManager.cleanup_callbacks(callbacks)
        
        # Should handle cleanup error gracefully
        event_streaming_cb.cleanup.assert_called_once()

    def test_cleanup_callbacks_trace_cleanup_error(self):
        """Test callback cleanup when agent trace cleanup fails."""
        event_streaming_cb = Mock()
        event_streaming_cb.cleanup = Mock()
        
        agent_trace_cb = Mock()
        
        callbacks = {
            'event_streaming': event_streaming_cb,
            'agent_trace': agent_trace_cb
        }
        
        CallbackManager.cleanup_callbacks(callbacks)
        
        # Should complete successfully
        event_streaming_cb.cleanup.assert_called_once()

    def test_init_callbacks_with_base_event_listeners(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization with listeners that inherit from BaseEventListener."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback, \
             patch.object(CallbackManager, 'ensure_event_listeners_registered') as mock_ensure:
            
            # Mock callback instances with setup_listeners (inheriting from BaseEventListener)
            job_cb = Mock()
            job_cb.setup_listeners = Mock()  # Has setup_listeners, so inherits from BaseEventListener
            
            event_cb = Mock()
            event_cb.setup_listeners = Mock()  # Has setup_listeners, so inherits from BaseEventListener
            
            trace_cb = Mock(spec=['event_bus'])  # No setup_listeners method, so doesn't inherit from BaseEventListener
            
            mock_job_callback.return_value = job_cb
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should only register non-BaseEventListener handlers
            mock_ensure.assert_called_once()
            call_args = mock_ensure.call_args[0][0]  # First argument
            assert len(call_args) == 1  # Only trace_cb should be in the list
            assert trace_cb in call_args

    def test_init_callbacks_all_base_event_listeners(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization when all listeners inherit from BaseEventListener."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback, \
             patch.object(CallbackManager, 'ensure_event_listeners_registered') as mock_ensure:
            
            # Mock all callback instances with setup_listeners (all inherit from BaseEventListener)
            job_cb = Mock()
            job_cb.setup_listeners = Mock()
            
            event_cb = Mock()
            event_cb.setup_listeners = Mock()
            
            trace_cb = Mock()
            trace_cb.setup_listeners = Mock()
            
            mock_job_callback.return_value = job_cb
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should not call ensure_event_listeners_registered since all inherit from BaseEventListener
            mock_ensure.assert_not_called()

    def test_ensure_event_listeners_registered_direct_register_error(self, mock_crewai_event_bus):
        """Test event listeners registration when direct register fails."""
        listener = Mock(spec=['event_bus', 'connect_events'])
        listener.event_bus = None
        listener.connect_events = Mock()
        
        # Make register method fail
        mock_crewai_event_bus.register.side_effect = Exception("Register error")
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should handle register error gracefully
        mock_crewai_event_bus.register.assert_called_once_with(listener)
        # Should still call connect_events
        listener.connect_events.assert_called_once()

    def test_init_callbacks_import_level_exception(self, mock_job_id):
        """Test callback initialization when import-level exception occurs."""
        # Simulate an import exception that happens in the try block
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            # Make all imports fail to trigger the outer exception handler
            mock_job_callback.side_effect = ImportError("Cannot import callback modules")
            mock_event_callback.side_effect = ImportError("Cannot import callback modules")
            mock_trace_callback.side_effect = ImportError("Cannot import callback modules")
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should return structure with empty handlers when all imports fail
            assert result == {'handlers': [], 'start_trace_writer': True}
    
    def test_init_callbacks_with_config_none(self, mock_job_id):
        """Test callback initialization with None config."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            job_cb = Mock()
            event_cb = Mock()
            trace_cb = Mock()
            
            mock_job_callback.return_value = job_cb
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id, config=None)
            
            # Should pass None config to EventStreamingCallback
            mock_event_callback.assert_called_once_with(
                job_id=mock_job_id,
                config=None,
                group_context=None
            )
    
    def test_init_callbacks_with_empty_string_job_id(self):
        """Test callback initialization with empty string job ID."""
        result = CallbackManager.init_callbacks(job_id="")
        
        assert result == {'handlers': []}
    
    def test_init_callbacks_partial_failures(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization with partial failures and non-base listeners."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback, \
             patch.object(CallbackManager, 'ensure_event_listeners_registered') as mock_ensure:
            
            # Create callbacks where some inherit from BaseEventListener and some don't
            job_cb = Mock()
            job_cb.setup_listeners = Mock()  # Has setup_listeners (BaseEventListener)
            
            event_cb = Mock(spec=['event_bus'])  # No setup_listeners method (not BaseEventListener)
            
            trace_cb = Mock()
            trace_cb.setup_listeners = Mock()  # Has setup_listeners (BaseEventListener)
            
            mock_job_callback.return_value = job_cb
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should register only non-BaseEventListener callbacks
            mock_ensure.assert_called_once()
            call_args = mock_ensure.call_args[0][0]  # First argument
            assert len(call_args) == 1  # Only event_cb should be in the list
            assert event_cb in call_args
            assert job_cb not in call_args
            assert trace_cb not in call_args
    
    def test_init_callbacks_none_handlers(self, mock_job_id, mock_crewai_event_bus):
        """Test callback initialization when some handlers are None."""
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback, \
             patch.object(CallbackManager, 'ensure_event_listeners_registered') as mock_ensure:
            
            # Set some callbacks to return None by making them raise exceptions
            mock_job_callback.side_effect = Exception("Job callback error")
            event_cb = Mock(spec=['event_bus'])  # No setup_listeners (not BaseEventListener)
            trace_cb = Mock()
            trace_cb.setup_listeners = Mock()  # Has setup_listeners (BaseEventListener)
            
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            result = CallbackManager.init_callbacks(job_id=mock_job_id)
            
            # Should handle None callbacks gracefully
            assert 'handlers' in result
            assert len(result['handlers']) == 2  # Only non-None callbacks
            assert event_cb in result['handlers']
            assert trace_cb in result['handlers']
    
    def test_ensure_event_listeners_connect_events_not_callable(self, mock_crewai_event_bus):
        """Test event listeners registration when connect_events is not callable."""
        listener = Mock()
        listener.event_bus = None
        listener.setup_listeners = Mock()
        listener.connect_events = "not_callable"  # Not a callable
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should handle non-callable connect_events gracefully
        listener.setup_listeners.assert_called_once_with(mock_crewai_event_bus)
    
    def test_ensure_event_listeners_no_connect_events(self, mock_crewai_event_bus):
        """Test event listeners registration when listener has no connect_events."""
        listener = Mock()
        listener.event_bus = None
        listener.setup_listeners = Mock()
        del listener.connect_events  # Remove connect_events attribute
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should handle missing connect_events gracefully
        listener.setup_listeners.assert_called_once_with(mock_crewai_event_bus)
    
    def test_ensure_event_listeners_already_has_event_bus(self, mock_crewai_event_bus):
        """Test event listeners registration when listener already has event_bus."""
        existing_bus = Mock()
        listener = Mock()
        listener.event_bus = existing_bus  # Already has event_bus
        listener.setup_listeners = Mock()
        listener.connect_events = Mock()
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should not overwrite existing event_bus
        assert listener.event_bus == existing_bus
        listener.setup_listeners.assert_called_once_with(mock_crewai_event_bus)
        listener.connect_events.assert_called_once()
    
    def test_cleanup_callbacks_with_agent_trace_cleanup_exception(self):
        """Test callback cleanup when agent trace section has exception."""
        event_streaming_cb = Mock()
        event_streaming_cb.cleanup = Mock()
        
        agent_trace_cb = Mock()
        
        callbacks = {
            'event_streaming': event_streaming_cb,
            'agent_trace': agent_trace_cb
        }
        
        # Simulate the trace cleanup logic path
        with patch('src.engines.crewai.flow.modules.callback_manager.logger') as mock_logger:
            CallbackManager.cleanup_callbacks(callbacks)
            
            # Should call cleanup on event streaming callback
            event_streaming_cb.cleanup.assert_called_once()
            # Should log info about trace processing
            mock_logger.info.assert_any_call("Cleaning up EventStreamingCallback")
            mock_logger.info.assert_any_call("EventStreamingCallback cleanup completed successfully")
            mock_logger.info.assert_any_call("Ensuring traces are processed")
    
    def test_cleanup_callbacks_only_agent_trace(self):
        """Test callback cleanup with only agent trace callback."""
        agent_trace_cb = Mock()
        
        callbacks = {
            'agent_trace': agent_trace_cb
        }
        
        with patch('src.engines.crewai.flow.modules.callback_manager.logger') as mock_logger:
            CallbackManager.cleanup_callbacks(callbacks)
            
            # Should log info about trace processing
            mock_logger.info.assert_any_call("Ensuring traces are processed")
    
    def test_cleanup_callbacks_trace_cleanup_exception(self):
        """Test callback cleanup when trace cleanup section throws exception."""
        event_streaming_cb = Mock()
        event_streaming_cb.cleanup = Mock()
        
        agent_trace_cb = Mock()
        
        callbacks = {
            'event_streaming': event_streaming_cb,
            'agent_trace': agent_trace_cb
        }
        
        # Mock logger to raise exception in trace cleanup section
        with patch('src.engines.crewai.flow.modules.callback_manager.logger') as mock_logger:
            # Make the trace processing line raise an exception
            mock_logger.info.side_effect = [None, None, Exception("Trace error")]
            
            CallbackManager.cleanup_callbacks(callbacks)
            
            # Should handle exception gracefully
            event_streaming_cb.cleanup.assert_called_once()
    
    def test_init_callbacks_major_exception_in_try_block(self, mock_job_id):
        """Test callback initialization when major exception occurs in main try block."""
        # Test when a major exception occurs in the try block after callback creation
        with patch('src.engines.crewai.callbacks.streaming_callbacks.JobOutputCallback') as mock_job_callback, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_callback, \
             patch('src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener') as mock_trace_callback:
            
            # Mock successful callback creation
            job_cb = Mock(spec=['event_bus'])
            event_cb = Mock(spec=['event_bus'])
            trace_cb = Mock(spec=['event_bus'])
            
            mock_job_callback.return_value = job_cb
            mock_event_callback.return_value = event_cb
            mock_trace_callback.return_value = trace_cb
            
            # Make the ensure_event_listeners_registered call fail with a major error
            with patch.object(CallbackManager, 'ensure_event_listeners_registered') as mock_ensure:
                mock_ensure.side_effect = RuntimeError("Major runtime error in try block")
                
                result = CallbackManager.init_callbacks(job_id=mock_job_id)
                
                # Should return empty handlers on major failure
                assert result == {'handlers': []}
    
    def test_ensure_event_listeners_registered_with_direct_register(self, mock_crewai_event_bus):
        """Test event listeners registration via direct crewai_event_bus.register method."""
        # Create a mock listener WITHOUT setup_listeners method to trigger direct registration
        listener = Mock()
        listener.event_bus = None
        listener.connect_events = Mock()
        
        # Use Mock spec to explicitly exclude setup_listeners
        listener = Mock(spec=['event_bus', 'connect_events'])
        listener.event_bus = None
        listener.connect_events = Mock()
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should try direct registration since no setup_listeners method
        mock_crewai_event_bus.register.assert_called_once_with(listener)
        # Should also call connect_events
        listener.connect_events.assert_called_once()
    
    def test_ensure_event_listeners_registered_major_exception(self, mock_crewai_event_bus):
        """Test event listeners registration when major exception occurs in main try block."""
        # Create listener that will cause exception during enumeration
        listeners = ["invalid_listener"]  # String instead of proper object
        
        with patch('src.engines.crewai.flow.modules.callback_manager.enumerate', side_effect=Exception("Major enumeration error")):
            CallbackManager.ensure_event_listeners_registered(listeners)
            
            # Should handle major exception gracefully without crashing
    
    def test_ensure_event_listeners_registered_direct_register_exception(self, mock_crewai_event_bus):
        """Test event listeners registration when direct register throws exception."""
        # Create a mock listener WITHOUT setup_listeners method 
        listener = Mock(spec=['event_bus', 'connect_events'])
        listener.event_bus = None
        listener.connect_events = Mock()
        
        # Make the direct register call fail
        mock_crewai_event_bus.register.side_effect = Exception("Direct register failed")
        
        listeners = [listener]
        
        CallbackManager.ensure_event_listeners_registered(listeners)
        
        # Should attempt direct registration and handle the exception
        mock_crewai_event_bus.register.assert_called_once_with(listener)
        # Should still call connect_events despite register failure
        listener.connect_events.assert_called_once()