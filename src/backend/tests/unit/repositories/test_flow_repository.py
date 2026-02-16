"""
Unit tests for FlowRepository.

Tests the functionality of flow repository including
CRUD operations, UUID handling, cascading deletes, and error handling.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from src.repositories.flow_repository import FlowRepository
from src.models.flow import Flow


# Mock flow model
class MockFlow:
    def __init__(self, id=None, name="Test Flow", crew_id=None, 
                 description="Test Description", created_at=None, updated_at=None):
        self.id = id or uuid.uuid4()
        self.name = name
        self.crew_id = crew_id or uuid.uuid4()
        self.description = description
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results, rowcount=0):
        self._scalars = MockScalars(results)
        self._rows = results if results and isinstance(results[0], tuple) else []
        self.rowcount = rowcount

    def scalars(self):
        return self._scalars

    def fetchall(self):
        return self._rows


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def flow_repository(mock_async_session):
    """Create a flow repository with async session."""
    return FlowRepository(session=mock_async_session)


@pytest.fixture
def sample_flows():
    """Create sample flows for testing."""
    crew_id_1 = uuid.uuid4()
    crew_id_2 = uuid.uuid4()
    return [
        MockFlow(id=uuid.uuid4(), name="Flow 1", crew_id=crew_id_1),
        MockFlow(id=uuid.uuid4(), name="Flow 2", crew_id=crew_id_1),
        MockFlow(id=uuid.uuid4(), name="Flow 3", crew_id=crew_id_2)
    ]


@pytest.fixture
def sample_flow_data():
    """Create sample flow data for creation."""
    return {
        "name": "new_flow",
        "description": "A new test flow",
        "crew_id": uuid.uuid4()
    }


class TestFlowRepositoryInit:
    """Test cases for FlowRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = FlowRepository(session=mock_async_session)
        
        assert repository.model == Flow
        assert repository.session == mock_async_session


class TestFlowRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, flow_repository, mock_async_session):
        """Test successful flow search by name."""
        flow = MockFlow(name="test_flow")
        mock_result = MockResult([flow])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_repository.find_by_name("test_flow")
        
        assert result == flow
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Flow)))
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, flow_repository, mock_async_session):
        """Test find by name when flow not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_repository.find_by_name("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_exception_handling(self, flow_repository, mock_async_session):
        """Test find by name with database exception."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await flow_repository.find_by_name("test_flow")


class TestFlowRepositoryFindByCrewId:
    """Test cases for find_by_crew_id method."""
    
    @pytest.mark.asyncio
    async def test_find_by_crew_id_success(self, flow_repository, mock_async_session, sample_flows):
        """Test successful flow search by crew ID."""
        crew_id = sample_flows[0].crew_id
        crew_flows = [flow for flow in sample_flows if flow.crew_id == crew_id]
        
        mock_result = MockResult(crew_flows)
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_repository.find_by_crew_id(crew_id)
        
        assert len(result) == len(crew_flows)
        assert all(flow.crew_id == crew_id for flow in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_crew_id_string_uuid(self, flow_repository, mock_async_session):
        """Test find by crew ID with string UUID."""
        crew_id = uuid.uuid4()
        flow = MockFlow(crew_id=crew_id)
        mock_result = MockResult([flow])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_repository.find_by_crew_id(str(crew_id))
        
        assert len(result) == 1
        assert result[0].crew_id == crew_id
    
    @pytest.mark.asyncio
    async def test_find_by_crew_id_invalid_uuid_string(self, flow_repository, mock_async_session):
        """Test find by crew ID with invalid UUID string."""
        result = await flow_repository.find_by_crew_id("invalid-uuid")
        
        assert result == []
        mock_async_session.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_find_by_crew_id_not_found(self, flow_repository, mock_async_session):
        """Test find by crew ID when no flows found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_repository.find_by_crew_id(uuid.uuid4())
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestFlowRepositoryFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, flow_repository, mock_async_session, sample_flows):
        """Test successful retrieval of all flows."""
        mock_result = MockResult(sample_flows)
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_repository.find_all()
        
        assert len(result) == len(sample_flows)
        assert result == sample_flows
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty_result(self, flow_repository, mock_async_session):
        """Test find all when no flows exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_repository.find_all()
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestFlowRepositoryDeleteWithExecutions:
    """Test cases for delete_with_executions method."""

    @pytest.mark.asyncio
    async def test_delete_with_executions_success(self, flow_repository, mock_async_session):
        """Test successful cascading delete of flow with executions."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)

        # Mock get method to return flow
        with patch.object(flow_repository, 'get', return_value=flow):
            # New implementation: delete executions + delete flow (2 queries)
            mock_result_with_rowcount = MagicMock()
            mock_result_with_rowcount.rowcount = 3
            mock_async_session.execute.side_effect = [
                mock_result_with_rowcount,  # Delete executions from executionhistory
                MagicMock()                  # Delete flow
            ]

            result = await flow_repository.delete_with_executions(flow_id)

            assert result is True
            # New implementation only executes 2 queries
            assert mock_async_session.execute.call_count == 2
            mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_with_executions_flow_not_found(self, flow_repository, mock_async_session):
        """Test delete when flow not found."""
        flow_id = uuid.uuid4()

        with patch.object(flow_repository, 'get', return_value=None):
            result = await flow_repository.delete_with_executions(flow_id)

            assert result is False
            mock_async_session.execute.assert_not_called()
            mock_async_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_with_executions_no_executions(self, flow_repository, mock_async_session):
        """Test delete when flow has no executions."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)

        with patch.object(flow_repository, 'get', return_value=flow):
            # Mock result with rowcount=0 (no executions deleted)
            mock_result_with_rowcount = MagicMock()
            mock_result_with_rowcount.rowcount = 0
            mock_async_session.execute.side_effect = [
                mock_result_with_rowcount,  # Delete executions (none found)
                MagicMock()                  # Delete flow
            ]

            result = await flow_repository.delete_with_executions(flow_id)

            assert result is True
            # Still executes 2 queries (delete executions + delete flow)
            assert mock_async_session.execute.call_count == 2
            mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_with_executions_large_execution_list(self, flow_repository, mock_async_session):
        """Test delete with many executions."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)

        with patch.object(flow_repository, 'get', return_value=flow):
            # Mock result with rowcount=75 (75 executions deleted)
            mock_result_with_rowcount = MagicMock()
            mock_result_with_rowcount.rowcount = 75
            mock_async_session.execute.side_effect = [
                mock_result_with_rowcount,  # Delete executions
                MagicMock()                  # Delete flow
            ]

            result = await flow_repository.delete_with_executions(flow_id)

            assert result is True
            # New implementation doesn't chunk - just 2 queries
            assert mock_async_session.execute.call_count == 2
            mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_with_executions_database_error(self, flow_repository, mock_async_session):
        """Test delete with database error during execution."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)

        with patch.object(flow_repository, 'get', return_value=flow):
            mock_async_session.execute.side_effect = Exception("Database error")

            with pytest.raises(Exception, match="Database error"):
                await flow_repository.delete_with_executions(flow_id)

            mock_async_session.rollback.assert_called_once()


class TestFlowRepositoryDeleteAll:
    """Test cases for delete_all method."""

    @pytest.mark.asyncio
    async def test_delete_all_success(self, flow_repository, mock_async_session):
        """Test successful delete all flows operation."""
        mock_async_session.execute.side_effect = [
            MagicMock(),  # Delete flow executions from executionhistory
            MagicMock()   # Delete flows
        ]

        await flow_repository.delete_all()

        # New implementation: 2 delete operations (executions + flows)
        assert mock_async_session.execute.call_count == 2
        mock_async_session.flush.assert_called_once()

        # Verify the order of deletions
        call_args_list = mock_async_session.execute.call_args_list
        assert len(call_args_list) == 2

        # Each call should have a text() query
        for call_args in call_args_list:
            query = call_args[0][0]
            assert hasattr(query, 'text')  # Should be a text() query

    @pytest.mark.asyncio
    async def test_delete_all_database_error(self, flow_repository, mock_async_session):
        """Test delete all with database error."""
        mock_async_session.execute.side_effect = Exception("Delete error")

        with pytest.raises(Exception, match="Delete error"):
            await flow_repository.delete_all()

        mock_async_session.rollback.assert_called_once()


@pytest.mark.skip(reason="SyncFlowRepository removed; async-only architecture")
class TestSyncFlowRepository:
    """Test cases for SyncFlowRepository."""
    
    @pytest.fixture
    def mock_sync_session(self):
        """Create a mock sync database session."""
        session = MagicMock()
        session.query.return_value = session
        session.filter.return_value = session
        session.first.return_value = None
        session.all.return_value = []
        session.delete.return_value = None
        session.commit.return_value = None
        return session
    
    @pytest.fixture
    def sync_flow_repository(self, mock_sync_session):
        """Create a sync flow repository."""
        return SyncFlowRepository(db=mock_sync_session)
    
    def test_sync_init_success(self, mock_sync_session):
        """Test successful sync repository initialization."""
        repository = SyncFlowRepository(db=mock_sync_session)
        assert repository.db == mock_sync_session
    
    def test_find_by_id_success(self, sync_flow_repository, mock_sync_session):
        """Test successful find by ID in sync repository."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)
        mock_sync_session.first.return_value = flow
        
        result = sync_flow_repository.find_by_id(flow_id)
        
        assert result == flow
        mock_sync_session.query.assert_called_once_with(Flow)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_by_id_string_uuid(self, sync_flow_repository, mock_sync_session):
        """Test find by ID with string UUID in sync repository."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)
        mock_sync_session.first.return_value = flow
        
        result = sync_flow_repository.find_by_id(str(flow_id))
        
        assert result == flow
    
    def test_find_by_id_invalid_uuid(self, sync_flow_repository, mock_sync_session):
        """Test find by ID with invalid UUID string."""
        result = sync_flow_repository.find_by_id("invalid-uuid")
        
        assert result is None
        mock_sync_session.query.assert_not_called()
    
    def test_find_by_name_sync(self, sync_flow_repository, mock_sync_session):
        """Test find by name in sync repository."""
        flow = MockFlow(name="test_flow")
        mock_sync_session.first.return_value = flow
        
        result = sync_flow_repository.find_by_name("test_flow")
        
        assert result == flow
        mock_sync_session.query.assert_called_once_with(Flow)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_by_crew_id_sync(self, sync_flow_repository, mock_sync_session):
        """Test find by crew ID in sync repository."""
        crew_id = uuid.uuid4()
        flows = [MockFlow(crew_id=crew_id)]
        mock_sync_session.all.return_value = flows
        
        result = sync_flow_repository.find_by_crew_id(crew_id)
        
        assert result == flows
        mock_sync_session.query.assert_called_once_with(Flow)
        mock_sync_session.filter.assert_called_once()
    
    def test_find_by_crew_id_invalid_uuid_sync(self, sync_flow_repository, mock_sync_session):
        """Test find by crew ID with invalid UUID in sync repository."""
        result = sync_flow_repository.find_by_crew_id("invalid-uuid")
        
        assert result == []
        mock_sync_session.query.assert_not_called()
    
    def test_find_all_sync(self, sync_flow_repository, mock_sync_session):
        """Test find all in sync repository."""
        flows = [MockFlow(), MockFlow()]
        mock_sync_session.all.return_value = flows
        
        result = sync_flow_repository.find_all()
        
        assert result == flows
        mock_sync_session.query.assert_called_once_with(Flow)
    
    def test_delete_all_sync(self, sync_flow_repository, mock_sync_session):
        """Test delete all in sync repository."""
        sync_flow_repository.delete_all()
        
        mock_sync_session.query.assert_called_once_with(Flow)
        mock_sync_session.delete.assert_called_once()
        mock_sync_session.commit.assert_called_once()


@pytest.mark.skip(reason="Sync repository factory removed; use async repositories")
class TestFlowRepositoryFactory:
    """Test cases for factory function."""
    
    def test_get_sync_flow_repository_factory(self):
        """Test the sync repository factory function."""
        from src.repositories.flow_repository import get_sync_flow_repository
        
        with patch('src.repositories.flow_repository.SessionLocal') as mock_session_local:
            mock_session = MagicMock()
            mock_session_local.return_value = mock_session
            
            repository = get_sync_flow_repository()
            
            assert isinstance(repository, SyncFlowRepository)
            assert repository.db == mock_session
            mock_session_local.assert_called_once()


class TestFlowRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_name_flow(self, flow_repository, mock_async_session):
        """Test the flow from create to find by name."""
        flow_data = {"name": "integration_flow", "crew_id": uuid.uuid4()}
        
        with patch('src.repositories.flow_repository.Flow') as mock_flow_class:
            created_flow = MockFlow(name="integration_flow")
            mock_flow_class.return_value = created_flow
            
            # Mock find_by_name for retrieval
            mock_result = MockResult([created_flow])
            mock_async_session.execute.return_value = mock_result
            
            # Create flow using inherited create method
            with patch.object(flow_repository, 'create', return_value=created_flow) as mock_create:
                create_result = await flow_repository.create(flow_data)
                
                # Find flow by name
                find_result = await flow_repository.find_by_name("integration_flow")
                
                assert create_result == created_flow
                assert find_result == created_flow
                mock_create.assert_called_once_with(flow_data)
                mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_crew_id_then_delete_flow(self, flow_repository, mock_async_session):
        """Test finding flows by crew ID then deleting them."""
        crew_id = uuid.uuid4()
        flows = [MockFlow(crew_id=crew_id), MockFlow(crew_id=crew_id)]

        # Mock find_by_crew_id
        mock_result = MockResult(flows)
        mock_async_session.execute.return_value = mock_result

        found_flows = await flow_repository.find_by_crew_id(crew_id)

        assert len(found_flows) == 2
        assert all(flow.crew_id == crew_id for flow in found_flows)

        # Now test deleting one of the flows
        flow_to_delete = found_flows[0]
        with patch.object(flow_repository, 'get', return_value=flow_to_delete):
            # Mock delete operations - new implementation uses 2 queries
            mock_result_with_rowcount = MagicMock()
            mock_result_with_rowcount.rowcount = 0
            mock_async_session.execute.side_effect = [
                mock_result_with_rowcount,  # Delete executions
                MagicMock()                  # Delete flow
            ]

            delete_result = await flow_repository.delete_with_executions(flow_to_delete.id)

            assert delete_result is True
            mock_async_session.flush.assert_called_once()


class TestFlowRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_database_error(self, flow_repository, mock_async_session):
        """Test find by name with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await flow_repository.find_by_name("test_flow")
    
    @pytest.mark.asyncio
    async def test_find_by_crew_id_database_error(self, flow_repository, mock_async_session):
        """Test find by crew ID with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await flow_repository.find_by_crew_id(uuid.uuid4())
    
    @pytest.mark.asyncio
    async def test_find_all_database_error(self, flow_repository, mock_async_session):
        """Test find all with database error."""
        mock_async_session.execute.side_effect = Exception("Database offline")
        
        with pytest.raises(Exception, match="Database offline"):
            await flow_repository.find_all()
    
    @pytest.mark.asyncio
    async def test_delete_with_executions_get_error(self, flow_repository, mock_async_session):
        """Test delete with executions when get method fails."""
        flow_id = uuid.uuid4()
        
        with patch.object(flow_repository, 'get', side_effect=Exception("Get failed")):
            with pytest.raises(Exception, match="Get failed"):
                await flow_repository.delete_with_executions(flow_id)
    
    @pytest.mark.asyncio
    async def test_delete_with_executions_flush_error(self, flow_repository, mock_async_session):
        """Test delete with executions when commit fails."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)

        with patch.object(flow_repository, 'get', return_value=flow):
            # Mock successful queries but failed commit
            mock_result_with_rowcount = MagicMock()
            mock_result_with_rowcount.rowcount = 0
            mock_async_session.execute.side_effect = [
                mock_result_with_rowcount,  # Delete executions
                MagicMock()                  # Delete flow
            ]
            mock_async_session.flush.side_effect = Exception("Flush failed")

            with pytest.raises(Exception, match="Flush failed"):
                await flow_repository.delete_with_executions(flow_id)

            mock_async_session.rollback.assert_called_once()


class TestFlowRepositoryUUIDHandling:
    """Test cases specifically for UUID handling."""
    
    @pytest.mark.asyncio
    async def test_uuid_conversion_edge_cases(self, flow_repository, mock_async_session):
        """Test various UUID conversion scenarios."""
        # Test empty string
        result = await flow_repository.find_by_crew_id("")
        assert result == []
        
        # Test None (should raise TypeError in real scenario)
        try:
            result = await flow_repository.find_by_crew_id(None)
            assert result == []  # Should handle gracefully
        except (TypeError, AttributeError):
            pass  # Expected behavior
        
        # Test malformed UUID
        result = await flow_repository.find_by_crew_id("not-a-uuid-at-all")
        assert result == []
    
    @pytest.mark.skip(reason="Sync repository removed; no sync UUID tests applicable")
    def test_sync_uuid_conversion_edge_cases(self):
        """Test UUID conversion in sync repository."""
        mock_session = MagicMock()
        sync_repo = SyncFlowRepository(db=mock_session)
        
        # Test empty string
        result = sync_repo.find_by_id("")
        assert result is None
        
        # Test malformed UUID
        result = sync_repo.find_by_crew_id("not-a-uuid")
        assert result == []


class TestFlowRepositoryQueryConstruction:
    """Test cases for query construction and SQL generation."""

    @pytest.mark.asyncio
    async def test_cascading_delete_sql_construction(self, flow_repository, mock_async_session):
        """Test that cascading delete constructs correct SQL queries."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id)

        with patch.object(flow_repository, 'get', return_value=flow):
            mock_result_with_rowcount = MagicMock()
            mock_result_with_rowcount.rowcount = 5

            mock_async_session.execute.side_effect = [
                mock_result_with_rowcount,  # Delete executions from executionhistory
                MagicMock()                  # Delete flow
            ]

            result = await flow_repository.delete_with_executions(flow_id)

            assert result is True

            # Verify 2 SQL queries were executed (new implementation)
            assert mock_async_session.execute.call_count == 2

            # Check that the first query is for deleting executions from executionhistory
            first_call = mock_async_session.execute.call_args_list[0]
            first_query = first_call[0][0]
            assert hasattr(first_query, 'text')
            assert "executionhistory" in str(first_query.text)

    @pytest.mark.asyncio
    async def test_delete_all_sql_order(self, flow_repository, mock_async_session):
        """Test that delete_all executes SQL in correct order."""
        mock_async_session.execute.side_effect = [
            MagicMock(),  # Delete flow executions from executionhistory
            MagicMock()   # Delete flows
        ]

        await flow_repository.delete_all()

        # Verify the correct order of SQL operations (2 queries now)
        call_args_list = mock_async_session.execute.call_args_list
        assert len(call_args_list) == 2

        # Each should be a text() query
        for call_args in call_args_list:
            query = call_args[0][0]
            assert hasattr(query, 'text')

        # Check the order by examining the SQL content
        exec_delete_query = call_args_list[0][0][0]
        flow_delete_query = call_args_list[1][0][0]

        assert "executionhistory" in str(exec_delete_query.text)
        assert "flows" in str(flow_delete_query.text)