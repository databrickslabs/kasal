import pytest
import uuid
import asyncio
import os
import tempfile
import json
from unittest.mock import Mock, patch, AsyncMock, MagicMock, call, PropertyMock
from datetime import datetime, UTC

from src.engines.crewai.flow.backend_flow import BackendFlow
from src.repositories.flow_repository import SyncFlowRepository


class TestBackendFlow:
    """Test cases for BackendFlow - targeting 100% coverage."""
    
    def create_mock_crewai_flow(self, start_methods=None, method_return_values=None):
        """Helper to create a properly mocked CrewAI flow with start methods."""
        if start_methods is None:
            start_methods = ['start_flow_node1']
        if method_return_values is None:
            method_return_values = {'start_flow_node1': {"output": "test"}}
        
        mock_flow = Mock()
        
        # Set up start methods
        for method_name in start_methods:
            method = AsyncMock(return_value=method_return_values.get(method_name, {"output": "test"}))
            setattr(mock_flow, method_name, method)
        
        # Mock dir() to return the start methods and other attributes
        all_attrs = start_methods + ['some_other_method', '__class__', '__dict__']
        
        def mock_dir(obj):
            return all_attrs
        
        # Patch the built-in dir function for this mock
        with patch('builtins.dir', side_effect=mock_dir):
            pass
        
        # Alternative: directly set the dir result
        mock_flow.__dir__ = Mock(return_value=all_attrs)
        
        return mock_flow

    # Test __init__ method - lines 39-67
    def test_init_with_job_id_only(self):
        """Test BackendFlow initialization with job_id only."""
        job_id = "test-job-123"
        flow = BackendFlow(job_id=job_id)
        
        assert flow._job_id == job_id
        assert flow._flow_id is None
        assert flow._flow_data is None
        assert flow._output_dir is None
        assert flow._config == {}
        assert flow._repositories == {}

    def test_init_with_flow_id_uuid(self):
        """Test BackendFlow initialization with UUID flow_id."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(flow_id=flow_id)
        
        assert flow._job_id is None
        assert flow._flow_id == flow_id

    def test_init_with_flow_id_string(self):
        """Test BackendFlow initialization with string flow_id."""
        flow_id_str = "550e8400-e29b-41d4-a716-446655440000"
        flow_id_uuid = uuid.UUID(flow_id_str)
        flow = BackendFlow(flow_id=flow_id_str)
        
        assert flow._flow_id == flow_id_uuid

    def test_init_with_invalid_flow_id(self):
        """Test BackendFlow initialization with invalid flow_id."""
        with pytest.raises(ValueError, match="Invalid flow_id format"):
            BackendFlow(flow_id="invalid-uuid")

    def test_init_with_both_parameters(self):
        """Test BackendFlow initialization with both job_id and flow_id."""
        job_id = "test-job-123"
        flow_id = uuid.uuid4()
        flow = BackendFlow(job_id=job_id, flow_id=flow_id)
        
        assert flow._job_id == job_id
        assert flow._flow_id == flow_id

    def test_init_with_no_parameters(self):
        """Test BackendFlow initialization with no parameters."""
        flow = BackendFlow()
        
        assert flow._job_id is None
        assert flow._flow_id is None

    def test_init_with_flow_id_none(self):
        """Test BackendFlow initialization with explicit None flow_id."""
        flow = BackendFlow(flow_id=None)
        
        assert flow._flow_id is None

    def test_init_with_flow_id_attribute_error(self):
        """Test BackendFlow initialization with flow_id causing AttributeError."""
        with pytest.raises(ValueError, match="Invalid flow_id format"):
            BackendFlow(flow_id=123)  # This will cause AttributeError in str() conversion

    def test_init_with_flow_id_type_error(self):
        """Test BackendFlow initialization with flow_id causing TypeError."""
        with pytest.raises(ValueError, match="Invalid flow_id format"):
            BackendFlow(flow_id=[])  # This will cause TypeError in UUID() conversion

    # Test property getters and setters - lines 69-95
    def test_config_property(self):
        """Test config property getter and setter."""
        flow = BackendFlow()
        
        # Test getter
        assert flow.config == {}
        
        # Test setter
        new_config = {"key": "value"}
        flow.config = new_config
        assert flow.config == new_config

    def test_output_dir_property_getter(self):
        """Test output_dir property getter."""
        flow = BackendFlow()
        
        with patch('src.engines.crewai.flow.backend_flow.logger') as mock_logger:
            result = flow.output_dir
            assert result is None
            mock_logger.info.assert_called_once_with("Getting output_dir: None")

    def test_output_dir_property_setter_none(self):
        """Test output_dir property setter with None."""
        flow = BackendFlow()
        
        with patch('src.engines.crewai.flow.backend_flow.logger') as mock_logger:
            flow.output_dir = None
            assert flow._output_dir is None
            mock_logger.info.assert_called_once_with("Setting output_dir to: None")

    def test_output_dir_property_setter_with_path(self):
        """Test output_dir property setter with actual path."""
        flow = BackendFlow()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = os.path.join(temp_dir, "test_output")
            
            with patch('src.engines.crewai.flow.backend_flow.logger') as mock_logger:
                with patch('os.makedirs') as mock_makedirs:
                    flow.output_dir = test_path
                    
                    assert flow._output_dir == test_path
                    mock_logger.info.assert_called_once_with(f"Setting output_dir to: {test_path}")
                    mock_makedirs.assert_called_once_with(test_path, exist_ok=True)

    def test_repositories_property(self):
        """Test repositories property getter and setter."""
        flow = BackendFlow()
        
        # Test getter
        assert flow.repositories == {}
        
        # Test setter
        new_repos = {"flow": Mock()}
        flow.repositories = new_repos
        assert flow.repositories == new_repos

    # Test load_flow method - lines 97-141
    def test_load_flow_no_flow_id(self):
        """Test load_flow with no flow_id."""
        flow = BackendFlow()
        
        with pytest.raises(ValueError, match="No flow_id provided"):
            flow.load_flow()

    def test_load_flow_with_repository_success(self):
        """Test load_flow with provided repository."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(flow_id=flow_id)
        
        mock_flow = Mock()
        mock_flow.id = flow_id
        mock_flow.name = "Test Flow"
        mock_flow.crew_id = 1
        mock_flow.nodes = [{"id": "node1"}]
        mock_flow.edges = [{"source": "node1", "target": "node2"}]
        mock_flow.flow_config = {"key": "value"}
        
        mock_repository = Mock(spec=SyncFlowRepository)
        mock_repository.find_by_id.return_value = mock_flow
        
        result = flow.load_flow(repository=mock_repository)
        
        assert result["id"] == flow_id
        assert result["name"] == "Test Flow"
        assert result["crew_id"] == 1
        assert result["nodes"] == [{"id": "node1"}]
        assert result["edges"] == [{"source": "node1", "target": "node2"}]
        assert result["flow_config"] == {"key": "value"}
        
        mock_repository.find_by_id.assert_called_once_with(flow_id)

    def test_load_flow_without_repository_success(self):
        """Test load_flow without provided repository."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(flow_id=flow_id)
        
        mock_flow = Mock()
        mock_flow.id = flow_id
        mock_flow.name = "Test Flow"
        mock_flow.crew_id = 1
        mock_flow.nodes = [{"id": "node1"}]
        mock_flow.edges = []
        mock_flow.flow_config = {}
        
        mock_repository = Mock(spec=SyncFlowRepository)
        mock_repository.find_by_id.return_value = mock_flow
        
        with patch('src.repositories.flow_repository.get_sync_flow_repository') as mock_get_repo:
            mock_get_repo.return_value = mock_repository
            
            result = flow.load_flow()
            
            assert result["id"] == flow_id
            assert result["name"] == "Test Flow"
            mock_get_repo.assert_called_once()

    def test_load_flow_not_found(self):
        """Test load_flow when flow not found."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(flow_id=flow_id)
        
        mock_repository = Mock(spec=SyncFlowRepository)
        mock_repository.find_by_id.return_value = None
        
        with pytest.raises(ValueError, match=f"Flow with ID {flow_id} not found"):
            flow.load_flow(repository=mock_repository)

    def test_load_flow_exception(self):
        """Test load_flow with exception."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(flow_id=flow_id)
        
        mock_repository = Mock(spec=SyncFlowRepository)
        mock_repository.find_by_id.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            flow.load_flow(repository=mock_repository)

    # Test _get_llm method - lines 143-159
    @pytest.mark.asyncio
    async def test_get_llm_success(self):
        """Test _get_llm method success."""
        flow = BackendFlow()
        
        mock_llm = Mock()
        
        with patch('src.engines.crewai.flow.backend_flow.LLMManager') as mock_llm_manager:
            mock_llm_manager.get_llm = AsyncMock(return_value=mock_llm)
            
            with patch.dict(os.environ, {'DEFAULT_LLM_MODEL': 'test-model'}):
                result = await flow._get_llm()
                
                assert result == mock_llm
                mock_llm_manager.get_llm.assert_called_once_with('test-model')

    @pytest.mark.asyncio
    async def test_get_llm_default_model(self):
        """Test _get_llm method with default model."""
        flow = BackendFlow()
        
        mock_llm = Mock()
        
        with patch('src.engines.crewai.flow.backend_flow.LLMManager') as mock_llm_manager:
            mock_llm_manager.get_llm = AsyncMock(return_value=mock_llm)
            
            # Remove DEFAULT_LLM_MODEL from environment
            with patch.dict(os.environ, {}, clear=True):
                result = await flow._get_llm()
                
                assert result == mock_llm
                mock_llm_manager.get_llm.assert_called_once_with('gpt-4o')

    @pytest.mark.asyncio
    async def test_get_llm_exception(self):
        """Test _get_llm method with exception."""
        flow = BackendFlow()
        
        with patch('src.engines.crewai.flow.backend_flow.LLMManager') as mock_llm_manager:
            mock_llm_manager.get_llm = AsyncMock(side_effect=Exception("LLM error"))
            
            with pytest.raises(Exception, match="LLM error"):
                await flow._get_llm()

    # Test flow method - lines 161-190
    @pytest.mark.asyncio
    async def test_flow_with_existing_flow_data(self):
        """Test flow method with existing flow data."""
        flow = BackendFlow()
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_dynamic_flow = Mock()
        
        with patch('src.engines.crewai.flow.backend_flow.FlowBuilder') as mock_flow_builder:
            mock_flow_builder.build_flow = AsyncMock(return_value=mock_dynamic_flow)
            
            with patch.object(flow, '_init_callbacks') as mock_init_callbacks:
                result = await flow.flow()
                
                assert result == mock_dynamic_flow
                mock_init_callbacks.assert_called_once()
                mock_flow_builder.build_flow.assert_called_once_with(
                    flow_data=flow._flow_data,
                    repositories=flow._repositories,
                    callbacks=flow._config.get('callbacks', {})
                )

    @pytest.mark.asyncio
    async def test_flow_without_flow_data_with_repository(self):
        """Test flow method without flow data but with repository."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(flow_id=flow_id)
        
        mock_flow_repo = Mock()
        flow._repositories = {'flow': mock_flow_repo}
        
        mock_flow_db = Mock()
        mock_flow_db.id = flow_id
        mock_flow_db.name = "Test Flow"
        mock_flow_db.crew_id = 1
        mock_flow_db.nodes = [{"id": "node1"}]
        mock_flow_db.edges = []
        mock_flow_db.flow_config = {}
        
        mock_flow_repo.find_by_id.return_value = mock_flow_db
        mock_dynamic_flow = Mock()
        
        with patch('src.engines.crewai.flow.backend_flow.FlowBuilder') as mock_flow_builder:
            mock_flow_builder.build_flow = AsyncMock(return_value=mock_dynamic_flow)
            
            with patch.object(flow, '_init_callbacks') as mock_init_callbacks:
                result = await flow.flow()
                
                assert result == mock_dynamic_flow
                assert flow._flow_data is not None

    @pytest.mark.asyncio
    async def test_flow_without_flow_data_no_repository(self):
        """Test flow method without flow data and no repository."""
        flow = BackendFlow()
        
        with pytest.raises(ValueError, match="No flow_id provided"):
            await flow.flow()

    @pytest.mark.asyncio
    async def test_flow_build_exception(self):
        """Test flow method with FlowBuilder exception."""
        flow = BackendFlow()
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        with patch('src.engines.crewai.flow.backend_flow.FlowBuilder') as mock_flow_builder:
            mock_flow_builder.build_flow = AsyncMock(side_effect=Exception("Build error"))
            
            with patch.object(flow, '_init_callbacks'):
                with pytest.raises(ValueError, match="Failed to create flow: Build error"):
                    await flow.flow()

    # Test _init_callbacks method - lines 192-201
    def test_init_callbacks(self):
        """Test _init_callbacks method."""
        flow = BackendFlow(job_id="test-job")
        flow._config = {"group_context": {"key": "value"}}
        
        mock_callbacks = {"callback1": Mock()}
        
        with patch('src.engines.crewai.flow.backend_flow.CallbackManager') as mock_callback_manager:
            mock_callback_manager.init_callbacks.return_value = mock_callbacks
            
            flow._init_callbacks()
            
            assert flow._config['callbacks'] == mock_callbacks
            mock_callback_manager.init_callbacks.assert_called_once_with(
                job_id="test-job",
                config=flow._config,
                group_context={"key": "value"}
            )

    def test_init_callbacks_no_group_context(self):
        """Test _init_callbacks method without group_context."""
        flow = BackendFlow(job_id="test-job")
        
        mock_callbacks = {"callback1": Mock()}
        
        with patch('src.engines.crewai.flow.backend_flow.CallbackManager') as mock_callback_manager:
            mock_callback_manager.init_callbacks.return_value = mock_callbacks
            
            flow._init_callbacks()
            
            mock_callback_manager.init_callbacks.assert_called_once_with(
                job_id="test-job",
                config=flow._config,
                group_context=None
            )

    # Test kickoff method - lines 203-349
    @pytest.mark.asyncio
    async def test_kickoff_success_with_trace_writer(self):
        """Test kickoff method with successful execution and trace writer."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(job_id="test-job", flow_id=flow_id)
        flow._config = {"callbacks": {"start_trace_writer": True}}
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = self.create_mock_crewai_flow()
        
        with patch('src.engines.crewai.trace_management.TraceManager') as mock_trace_manager:
            mock_trace_manager.ensure_writer_started = AsyncMock()
            
            with patch.object(flow, 'flow', return_value=mock_crewai_flow) as mock_flow_method:
                with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                    result = await flow.kickoff()
                
                    assert result["success"] is True
                    # Dict result gets updated directly into combined_results, then each value is processed
                    assert result["result"]["output"]["content"] == "test"
                    assert result["flow_id"] == flow_id
                    
                    mock_trace_manager.ensure_writer_started.assert_called_once()
                    mock_flow_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_kickoff_trace_writer_error(self):
        """Test kickoff method with trace writer error."""
        flow = BackendFlow(job_id="test-job")
        flow._config = {"callbacks": {"start_trace_writer": True}}
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = self.create_mock_crewai_flow()
        
        with patch('src.engines.crewai.trace_management.TraceManager') as mock_trace_manager:
            mock_trace_manager.ensure_writer_started = AsyncMock(side_effect=Exception("Trace error"))
            
            with patch.object(flow, 'flow', return_value=mock_crewai_flow):
                with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                    result = await flow.kickoff()
                    
                    assert result["success"] is True  # Should continue despite trace error

    @pytest.mark.asyncio
    async def test_kickoff_load_flow_data_during_kickoff(self):
        """Test kickoff method loading flow data during execution."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(job_id="test-job", flow_id=flow_id)
        flow._repositories = {"flow": Mock()}
        
        mock_flow_db = Mock()
        mock_flow_db.id = flow_id
        mock_flow_db.name = "Test Flow"
        mock_flow_db.crew_id = 1
        mock_flow_db.nodes = [{"id": "node1"}]
        mock_flow_db.edges = []
        mock_flow_db.flow_config = {}
        
        flow._repositories["flow"].find_by_id.return_value = mock_flow_db
        
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value={"output": "test"})
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            result = await flow.kickoff()
            
            assert result["success"] is True
            assert flow._flow_data is not None

    @pytest.mark.asyncio
    async def test_kickoff_load_flow_data_error(self):
        """Test kickoff method with flow data loading error."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(job_id="test-job", flow_id=flow_id)
        flow._repositories = {"flow": Mock()}
        flow._repositories["flow"].find_by_id.side_effect = Exception("Load error")
        
        result = await flow.kickoff()
        
        assert result["success"] is False
        assert result["error"] == "Failed to load flow data: Load error"
        assert result["flow_id"] == flow_id

    @pytest.mark.asyncio
    async def test_kickoff_create_flow_error(self):
        """Test kickoff method with flow creation error."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        with patch.object(flow, 'flow', side_effect=Exception("Flow creation error")):
            result = await flow.kickoff()
            
            assert result["success"] is False
            assert result["error"] == "Failed to create CrewAI flow: Flow creation error"

    @pytest.mark.asyncio
    async def test_kickoff_no_start_methods(self):
        """Test kickoff method with no start methods."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = Mock()
        # No start_flow_ methods
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['other_method', 'not_a_start_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                assert result["result"] == {}

    @pytest.mark.asyncio
    async def test_kickoff_start_method_direct_call_success(self):
        """Test kickoff method with successful direct start method call."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_result = {"content": "success"}
        mock_crewai_flow = self.create_mock_crewai_flow(
            method_return_values={'start_flow_node1': mock_result}
        )
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                # Dict result gets updated directly into combined_results
                assert result["result"]["content"]["content"] == "success"

    @pytest.mark.asyncio
    async def test_kickoff_start_method_fallback_kickoff_async(self):
        """Test kickoff method with fallback to kickoff_async."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_result = {"content": "fallback success"}
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(side_effect=Exception("Direct call failed"))
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=mock_result)
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                # Dict result gets updated directly into combined_results
                assert result["result"]["content"]["content"] == "fallback success"

    @pytest.mark.asyncio
    async def test_kickoff_start_method_fallback_sync_kickoff(self):
        """Test kickoff method with fallback to sync kickoff."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_result = {"content": "sync fallback success"}
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(side_effect=Exception("Direct call failed"))
        # Remove kickoff_async attribute and add sync kickoff
        del mock_crewai_flow.kickoff_async
        mock_crewai_flow.kickoff = Mock(return_value=mock_result)
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                # Dict result gets updated directly into combined_results
                assert result["result"]["content"]["content"] == "sync fallback success"

    @pytest.mark.asyncio
    async def test_kickoff_start_method_all_fallbacks_fail(self):
        """Test kickoff method when all fallbacks fail."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(side_effect=Exception("Direct call failed"))
        mock_crewai_flow.kickoff_async = AsyncMock(side_effect=Exception("Async kickoff failed"))
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            result = await flow.kickoff()
            
            # Should continue with other methods even if one fails
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_none(self):
        """Test kickoff method with None result conversion."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = self.create_mock_crewai_flow(
            method_return_values={'start_flow_node1': None}
        )
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                # None result gets converted to empty dict, but since no valid method result, combined_results is empty
                assert result["result"] == {}

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_dict(self):
        """Test kickoff method with dict result."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_result = {"key": "value"}
        mock_crewai_flow = self.create_mock_crewai_flow(
            method_return_values={'start_flow_node1': mock_result}
        )
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                # Dict result gets updated into combined_results, then each value is processed
                assert result["result"] == {"key": {"content": "value"}}

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_to_dict_method(self):
        """Test kickoff method with result having to_dict method."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_result_obj = Mock()
        mock_result_obj.to_dict.return_value = {"converted": "data"}
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value=mock_result_obj)
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            result = await flow.kickoff()
            
            assert result["success"] is True
            assert result["result"]["start_flow_node1"] == {"converted": "data"}

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_dict_attribute(self):
        """Test kickoff method with result having __dict__ attribute."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        class MockResult:
            def __init__(self):
                self.attr = "value"
        
        mock_result_obj = MockResult()
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value=mock_result_obj)
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                assert result["result"]["start_flow_node1"] == {"attr": "value"}

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_raw_attribute(self):
        """Test kickoff method with result having raw attribute."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        # Create an object with slots to avoid __dict__
        class ResultWithSlots:
            __slots__ = ['raw', 'token_usage']
            def __init__(self):
                self.raw = "raw content"
                self.token_usage = "100 tokens"
        
        mock_result_obj = ResultWithSlots()
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value=mock_result_obj)
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                assert result["result"]["start_flow_node1"]["content"] == "raw content"
                assert result["result"]["start_flow_node1"]["token_usage"] == "100 tokens"

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_raw_no_token_usage(self):
        """Test kickoff method with result having raw but no token_usage."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        # Create an object with slots to avoid __dict__, with only raw
        class ResultWithSlots:
            __slots__ = ['raw']
            def __init__(self):
                self.raw = "raw content"
        
        mock_result_obj = ResultWithSlots()
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value=mock_result_obj)
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                assert result["result"]["start_flow_node1"]["content"] == "raw content"
                assert result["result"]["start_flow_node1"]["token_usage"] is None

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_string_fallback(self):
        """Test kickoff method with string result fallback."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value="simple string")
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            result = await flow.kickoff()
            
            assert result["success"] is True
            assert result["result"]["start_flow_node1"] == {"content": "simple string"}

    @pytest.mark.asyncio
    async def test_kickoff_result_conversion_error(self):
        """Test kickoff method with result conversion error."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_result_obj = Mock()
        mock_result_obj.to_dict.side_effect = Exception("Conversion error")
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value=mock_result_obj)
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            result = await flow.kickoff()
            
            assert result["success"] is True
            # Should fallback to string representation
            assert "content" in result["result"]["start_flow_node1"]

    @pytest.mark.asyncio
    async def test_kickoff_multiple_start_methods(self):
        """Test kickoff method with multiple start methods."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}, {"id": "node2"}]}
        
        mock_crewai_flow = self.create_mock_crewai_flow(
            start_methods=['start_flow_node1', 'start_flow_node2'],
            method_return_values={
                'start_flow_node1': {"output1": "test1"},
                'start_flow_node2': {"output2": "test2"}
            }
        )
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'start_flow_node2', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                assert result["result"]["output1"]["content"] == "test1"
                assert result["result"]["output2"]["content"] == "test2"

    @pytest.mark.asyncio
    async def test_kickoff_general_exception(self):
        """Test kickoff method with general exception."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        with patch.object(flow, 'flow', side_effect=Exception("General error")):
            result = await flow.kickoff()
            
            assert result["success"] is False
            assert result["error"] == "Failed to create CrewAI flow: General error"

    @pytest.mark.asyncio
    async def test_kickoff_cleanup_callbacks(self):
        """Test kickoff method cleanup callbacks in finally block."""
        flow = BackendFlow(job_id="test-job")
        flow._config = {"callbacks": {"callback1": Mock()}}
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = Mock()
        mock_crewai_flow.start_flow_node1 = AsyncMock(return_value={"output": "test"})
        
        with patch('src.engines.crewai.flow.backend_flow.CallbackManager') as mock_callback_manager:
            with patch.object(flow, 'flow', return_value=mock_crewai_flow):
                result = await flow.kickoff()
                
                assert result["success"] is True
                mock_callback_manager.cleanup_callbacks.assert_called_once_with(flow._config["callbacks"])

    # Test helper methods - lines 351-396
    def test_ensure_event_listeners_registered(self):
        """Test _ensure_event_listeners_registered method."""
        flow = BackendFlow()
        listeners = [Mock(), Mock()]
        
        with patch('src.engines.crewai.flow.backend_flow.CallbackManager') as mock_callback_manager:
            flow._ensure_event_listeners_registered(listeners)
            
            mock_callback_manager.ensure_event_listeners_registered.assert_called_once_with(listeners)

    @pytest.mark.asyncio
    async def test_configure_agent_and_tools(self):
        """Test _configure_agent_and_tools method."""
        flow = BackendFlow()
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        flow._repositories = {"agent": Mock()}
        
        agent_data = {"id": 1, "name": "Test Agent"}
        mock_agent = Mock()
        
        with patch('src.engines.crewai.flow.backend_flow.AgentConfig') as mock_agent_config:
            mock_agent_config.configure_agent_and_tools = AsyncMock(return_value=mock_agent)
            
            result = await flow._configure_agent_and_tools(agent_data)
            
            assert result == mock_agent
            mock_agent_config.configure_agent_and_tools.assert_called_once_with(
                agent_data=agent_data,
                flow_data=flow._flow_data,
                repositories=flow._repositories
            )

    @pytest.mark.asyncio
    async def test_configure_task(self):
        """Test _configure_task method."""
        flow = BackendFlow()
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        flow._repositories = {"task": Mock()}
        
        task_data = {"id": 1, "name": "Test Task"}
        agent = Mock()
        callback = Mock()
        mock_task = Mock()
        
        with patch('src.engines.crewai.flow.backend_flow.TaskConfig') as mock_task_config:
            mock_task_config.configure_task = AsyncMock(return_value=mock_task)
            
            result = await flow._configure_task(task_data, agent, callback)
            
            assert result == mock_task
            mock_task_config.configure_task.assert_called_once_with(
                task_data=task_data,
                agent=agent,
                task_output_callback=callback,
                flow_data=flow._flow_data,
                repositories=flow._repositories
            )

    @pytest.mark.asyncio
    async def test_configure_task_no_optional_params(self):
        """Test _configure_task method without optional parameters."""
        flow = BackendFlow()
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        flow._repositories = {"task": Mock()}
        
        task_data = {"id": 1, "name": "Test Task"}
        mock_task = Mock()
        
        with patch('src.engines.crewai.flow.backend_flow.TaskConfig') as mock_task_config:
            mock_task_config.configure_task = AsyncMock(return_value=mock_task)
            
            result = await flow._configure_task(task_data)
            
            assert result == mock_task
            mock_task_config.configure_task.assert_called_once_with(
                task_data=task_data,
                agent=None,
                task_output_callback=None,
                flow_data=flow._flow_data,
                repositories=flow._repositories
            )

    # Additional edge cases to ensure 100% coverage
    @pytest.mark.asyncio
    async def test_kickoff_result_update_dict(self):
        """Test kickoff method with dict result that gets updated."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_result = {"existing": "data"}
        mock_crewai_flow = self.create_mock_crewai_flow(
            method_return_values={'start_flow_node1': mock_result}
        )
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                # Dict result should update combined_results
                assert result["result"]["existing"]["content"] == "data"

    @pytest.mark.asyncio
    async def test_kickoff_result_with_method_name_key(self):
        """Test kickoff method with non-dict result using method name as key."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = {"nodes": [{"id": "node1"}]}
        
        mock_crewai_flow = self.create_mock_crewai_flow(
            method_return_values={'start_flow_node1': "string result"}
        )
        
        with patch.object(flow, 'flow', return_value=mock_crewai_flow):
            with patch('builtins.dir', return_value=['start_flow_node1', 'other_method']):
                result = await flow.kickoff()
                
                assert result["success"] is True
                # Non-dict result should use method name as key
                assert result["result"]["start_flow_node1"]["content"] == "string result"