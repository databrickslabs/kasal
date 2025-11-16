"""
Unit tests for BackendFlow improvements.

Tests for new features added to BackendFlow:
- kickoff_async() method
- plot() visualization
- Tracing support
"""
import pytest
import uuid
import os
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from datetime import datetime

from src.engines.crewai.flow.backend_flow import BackendFlow


# Mock classes
class MockCrewAIFlow:
    """Mock CrewAI Flow instance."""
    def __init__(self, has_kickoff_async=True, has_plot=True):
        self.state = {}
        self._has_kickoff_async = has_kickoff_async
        self._has_plot = has_plot

    def kickoff(self):
        return {"raw": "sync result", "token_usage": "100 tokens"}

    async def kickoff_async(self):
        if not self._has_kickoff_async:
            raise AttributeError("kickoff_async not available")
        return {"raw": "async result", "token_usage": "100 tokens"}

    def plot(self, filename="flow_diagram"):
        if not self._has_plot:
            raise AttributeError("plot not available")
        return f"{filename}.png"


@pytest.fixture
def mock_flow_data():
    """Create mock flow data."""
    return {
        'id': uuid.uuid4(),
        'name': 'Test Flow',
        'crew_id': uuid.uuid4(),
        'nodes': [
            {'id': 'node1', 'type': 'agent', 'data': {'label': 'Agent 1'}}
        ],
        'edges': [],
        'flow_config': {'startingPoints': [{'crewId': 'crew1', 'crewName': 'Test Crew'}]}
    }


class TestKickoffAsync:
    """Tests for BackendFlow.kickoff_async() method."""

    @pytest.mark.asyncio
    async def test_kickoff_async_with_native_support(self, mock_flow_data):
        """Test kickoff_async when CrewAI supports it natively."""
        flow = BackendFlow(job_id="test-job-123", flow_id=uuid.uuid4())
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {}}

        mock_crewai_flow = MockCrewAIFlow(has_kickoff_async=True)

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.kickoff_async()

            assert result['success'] is True
            assert 'result' in result
            assert result['flow_id'] == flow._flow_id
            # Verify it used kickoff_async
            assert result['result']['content'] == "async result"

    @pytest.mark.asyncio
    async def test_kickoff_async_fallback_to_sync(self, mock_flow_data):
        """Test kickoff_async falls back to sync when async not available."""
        flow = BackendFlow(job_id="test-job-123")
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {}}

        # Create mock without kickoff_async
        mock_crewai_flow = Mock()
        mock_crewai_flow.kickoff.return_value = {"raw": "sync fallback"}
        # Explicitly remove kickoff_async to simulate it not being available
        type(mock_crewai_flow).kickoff_async = PropertyMock(side_effect=AttributeError)

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.kickoff_async()

            assert result['success'] is True
            assert "sync fallback" in str(result['result'])
            mock_crewai_flow.kickoff.assert_called_once()

    @pytest.mark.asyncio
    async def test_kickoff_async_with_dict_result(self, mock_flow_data):
        """Test kickoff_async with dict result."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {}}

        mock_crewai_flow = Mock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value={"key": "value"})

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.kickoff_async()

            assert result['success'] is True
            assert result['result'] == {"key": "value"}

    @pytest.mark.asyncio
    async def test_kickoff_async_handles_execution_error(self, mock_flow_data):
        """Test kickoff_async handles execution errors."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {}}

        mock_crewai_flow = Mock()
        mock_crewai_flow.kickoff_async = AsyncMock(side_effect=Exception("Execution failed"))

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.kickoff_async()

            assert result['success'] is False
            assert 'error' in result
            assert 'Execution failed' in result['error']

    @pytest.mark.asyncio
    async def test_kickoff_async_handles_flow_creation_error(self):
        """Test kickoff_async handles flow creation errors."""
        flow = BackendFlow(job_id="test-job", flow_id=uuid.uuid4())
        flow._config = {'callbacks': {}}

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.side_effect = Exception("Flow creation failed")

            result = await flow.kickoff_async()

            assert result['success'] is False
            assert 'error' in result
            assert 'Flow creation failed' in result['error']

    @pytest.mark.asyncio
    async def test_kickoff_async_loads_flow_data(self):
        """Test kickoff_async loads flow data if not present."""
        flow = BackendFlow(job_id="test-job", flow_id=uuid.uuid4())
        flow._repositories = {'flow': MagicMock()}

        with patch.object(flow, 'load_flow') as mock_load, \
             patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:

            mock_load.return_value = {'id': uuid.uuid4(), 'name': 'Test'}
            mock_crewai_flow = MockCrewAIFlow()
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.kickoff_async()

            mock_load.assert_called_once()
            assert result['success'] is True

    @pytest.mark.asyncio
    async def test_kickoff_async_handles_flow_data_load_error(self):
        """Test kickoff_async handles errors when loading flow data."""
        flow = BackendFlow(job_id="test-job", flow_id=uuid.uuid4())
        flow._repositories = {'flow': MagicMock()}

        with patch.object(flow, 'load_flow') as mock_load:
            mock_load.side_effect = Exception("Load failed")

            result = await flow.kickoff_async()

            assert result['success'] is False
            assert 'Failed to load flow data' in result['error']


class TestPlot:
    """Tests for BackendFlow.plot() method."""

    @pytest.mark.asyncio
    async def test_plot_with_native_support(self, mock_flow_data):
        """Test plot when CrewAI supports it."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._output_dir = "/tmp/test"

        mock_crewai_flow = MockCrewAIFlow(has_plot=True)

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.plot(filename="test_diagram")

            assert result is not None
            assert "test_diagram" in result
            assert ".png" in result

    @pytest.mark.asyncio
    async def test_plot_without_native_support(self, mock_flow_data):
        """Test plot when CrewAI doesn't support it."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data

        # Mock flow without plot method
        mock_crewai_flow = Mock(spec=[])

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.plot()

            assert result is None

    @pytest.mark.asyncio
    async def test_plot_with_custom_output_dir(self, mock_flow_data):
        """Test plot uses custom output directory."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._output_dir = "/custom/path"

        mock_crewai_flow = MockCrewAIFlow(has_plot=True)

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method, \
             patch('os.path.join', return_value="/custom/path/custom_diagram"):
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.plot(filename="custom_diagram")

            assert result is not None

    @pytest.mark.asyncio
    async def test_plot_default_filename(self, mock_flow_data):
        """Test plot uses default filename."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._output_dir = "/tmp"

        mock_crewai_flow = MockCrewAIFlow(has_plot=True)

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.plot()

            assert result is not None
            assert "flow_diagram" in result

    @pytest.mark.asyncio
    async def test_plot_handles_error(self, mock_flow_data):
        """Test plot handles errors gracefully."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.side_effect = Exception("Plot failed")

            result = await flow.plot()

            assert result is None

    @pytest.mark.asyncio
    async def test_plot_with_none_output_dir(self, mock_flow_data):
        """Test plot when output_dir is None uses current directory."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._output_dir = None

        mock_crewai_flow = MockCrewAIFlow(has_plot=True)

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method:
            mock_flow_method.return_value = mock_crewai_flow

            result = await flow.plot(filename="test")

            assert result is not None


class TestTracing:
    """Tests for BackendFlow tracing support."""

    @pytest.mark.asyncio
    async def test_tracing_enabled_via_constructor(self, mock_flow_data):
        """Test tracing when enabled via constructor."""
        flow = BackendFlow(job_id="test-job", tracing=True)
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {}}

        mock_crewai_flow = MockCrewAIFlow()

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method, \
             patch('src.engines.crewai.flow.backend_flow.TraceManager') as MockTraceManager:

            mock_flow_method.return_value = mock_crewai_flow
            MockTraceManager.ensure_writer_started = AsyncMock()

            await flow.kickoff_async()

            # Should start trace writer
            MockTraceManager.ensure_writer_started.assert_called_once()

    @pytest.mark.asyncio
    async def test_tracing_enabled_via_config(self, mock_flow_data):
        """Test tracing when enabled via config."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {'start_trace_writer': True}}

        mock_crewai_flow = MockCrewAIFlow()

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method, \
             patch('src.engines.crewai.flow.backend_flow.TraceManager') as MockTraceManager:

            mock_flow_method.return_value = mock_crewai_flow
            MockTraceManager.ensure_writer_started = AsyncMock()

            await flow.kickoff()

            # Should start trace writer
            MockTraceManager.ensure_writer_started.assert_called_once()

    @pytest.mark.asyncio
    async def test_tracing_error_doesnt_stop_execution(self, mock_flow_data):
        """Test that tracing errors don't prevent execution."""
        flow = BackendFlow(job_id="test-job", tracing=True)
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {}}

        mock_crewai_flow = MockCrewAIFlow()

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method, \
             patch('src.engines.crewai.flow.backend_flow.TraceManager') as MockTraceManager:

            mock_flow_method.return_value = mock_crewai_flow
            MockTraceManager.ensure_writer_started = AsyncMock(side_effect=Exception("Trace error"))

            result = await flow.kickoff_async()

            # Should still succeed despite trace error
            assert result['success'] is True

    @pytest.mark.asyncio
    async def test_tracing_disabled_by_default(self, mock_flow_data):
        """Test tracing is disabled by default."""
        flow = BackendFlow(job_id="test-job")
        flow._flow_data = mock_flow_data
        flow._config = {'callbacks': {}}

        mock_crewai_flow = MockCrewAIFlow()

        with patch.object(flow, 'flow', new_callable=AsyncMock) as mock_flow_method, \
             patch('src.engines.crewai.flow.backend_flow.TraceManager') as MockTraceManager:

            mock_flow_method.return_value = mock_crewai_flow
            MockTraceManager.ensure_writer_started = AsyncMock()

            await flow.kickoff_async()

            # Should NOT start trace writer when disabled
            MockTraceManager.ensure_writer_started.assert_not_called()


class TestBackendFlowInitialization:
    """Tests for BackendFlow initialization with new parameters."""

    def test_init_with_tracing_enabled(self):
        """Test initialization with tracing enabled."""
        flow = BackendFlow(job_id="test-job", tracing=True)

        assert flow._tracing_enabled is True

    def test_init_with_tracing_disabled(self):
        """Test initialization with tracing disabled (default)."""
        flow = BackendFlow(job_id="test-job")

        assert flow._tracing_enabled is False

    def test_init_with_all_parameters(self):
        """Test initialization with all parameters."""
        flow_id = uuid.uuid4()
        flow = BackendFlow(job_id="test-job", flow_id=flow_id, tracing=True)

        assert flow._job_id == "test-job"
        assert flow._flow_id == flow_id
        assert flow._tracing_enabled is True
