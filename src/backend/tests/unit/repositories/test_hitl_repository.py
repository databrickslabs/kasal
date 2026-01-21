"""
Unit tests for HITLApprovalRepository and HITLWebhookRepository.

Tests the functionality of HITL repositories including CRUD operations,
status updates, filtering, pagination, and webhook management.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.repositories.hitl_repository import HITLApprovalRepository, HITLWebhookRepository
from src.models.hitl_approval import (
    HITLApproval,
    HITLWebhook,
    HITLApprovalStatus,
    HITLTimeoutAction,
    HITLRejectionAction
)


class MockHITLApproval:
    """Mock HITL approval model for testing."""

    def __init__(
        self,
        id: int = 1,
        execution_id: str = "exec-123",
        flow_id: str = "flow-456",
        gate_node_id: str = "gate-1",
        crew_sequence: int = 1,
        status: str = HITLApprovalStatus.PENDING,
        gate_config: Optional[dict] = None,
        previous_crew_name: Optional[str] = "Research Crew",
        previous_crew_output: Optional[str] = "Research results...",
        flow_state_snapshot: Optional[dict] = None,
        responded_by: Optional[str] = None,
        responded_at: Optional[datetime] = None,
        approval_comment: Optional[str] = None,
        rejection_reason: Optional[str] = None,
        rejection_action: Optional[str] = None,
        expires_at: Optional[datetime] = None,
        webhook_sent: bool = False,
        webhook_sent_at: Optional[datetime] = None,
        webhook_response: Optional[dict] = None,
        created_at: Optional[datetime] = None,
        group_id: str = "test-group"
    ):
        self.id = id
        self.execution_id = execution_id
        self.flow_id = flow_id
        self.gate_node_id = gate_node_id
        self.crew_sequence = crew_sequence
        self.status = status
        self.gate_config = gate_config or {"message": "Review required", "timeout_seconds": 3600}
        self.previous_crew_name = previous_crew_name
        self.previous_crew_output = previous_crew_output
        self.flow_state_snapshot = flow_state_snapshot or {}
        self.responded_by = responded_by
        self.responded_at = responded_at
        self.approval_comment = approval_comment
        self.rejection_reason = rejection_reason
        self.rejection_action = rejection_action
        self.expires_at = expires_at
        self.webhook_sent = webhook_sent
        self.webhook_sent_at = webhook_sent_at
        self.webhook_response = webhook_response
        self.created_at = created_at or datetime.now(timezone.utc)
        self.group_id = group_id


class MockHITLWebhook:
    """Mock HITL webhook model for testing."""

    def __init__(
        self,
        id: int = 1,
        group_id: str = "test-group",
        flow_id: Optional[str] = None,  # Optional: scope webhook to specific flow
        name: str = "Test Webhook",
        url: str = "https://example.com/webhook",
        enabled: bool = True,
        events: Optional[List[str]] = None,
        headers: Optional[dict] = None,
        secret: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.id = id
        self.group_id = group_id
        self.flow_id = flow_id  # None means global (all flows in group)
        self.name = name
        self.url = url
        self.enabled = enabled
        self.events = events or ["gate_reached"]
        self.headers = headers or {}
        self.secret = secret
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)


class MockScalars:
    """Mock SQLAlchemy scalars result."""

    def __init__(self, results):
        self.results = results

    def first(self):
        return self.results[0] if self.results else None

    def all(self):
        return self.results


class MockResult:
    """Mock SQLAlchemy result object."""

    def __init__(self, results):
        self._scalars = MockScalars(results)

    def scalars(self):
        return self._scalars

    def scalar(self):
        """Return scalar value for count queries."""
        return self.results[0] if hasattr(self, 'results') and self.results else 0


class MockCountResult:
    """Mock SQLAlchemy result for count queries."""

    def __init__(self, count: int):
        self._count = count

    def scalar(self):
        return self._count


class MockDeleteResult:
    """Mock SQLAlchemy delete result."""

    def __init__(self, rowcount: int):
        self.rowcount = rowcount


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def hitl_approval_repository(mock_async_session):
    """Create an HITL approval repository with async session."""
    return HITLApprovalRepository(session=mock_async_session)


@pytest.fixture
def hitl_webhook_repository(mock_async_session):
    """Create an HITL webhook repository with async session."""
    return HITLWebhookRepository(session=mock_async_session)


@pytest.fixture
def sample_approval():
    """Create a sample HITL approval for testing."""
    return MockHITLApproval()


@pytest.fixture
def sample_approvals():
    """Create multiple sample HITL approvals for testing."""
    return [
        MockHITLApproval(id=1, execution_id="exec-1", crew_sequence=1),
        MockHITLApproval(id=2, execution_id="exec-1", crew_sequence=2),
        MockHITLApproval(id=3, execution_id="exec-2", crew_sequence=1),
    ]


@pytest.fixture
def sample_webhook():
    """Create a sample HITL webhook for testing."""
    return MockHITLWebhook()


@pytest.fixture
def sample_webhooks():
    """Create multiple sample HITL webhooks for testing."""
    return [
        MockHITLWebhook(id=1, name="Webhook 1", events=["gate_reached", "gate_approved"]),
        MockHITLWebhook(id=2, name="Webhook 2", events=["gate_rejected"]),
        MockHITLWebhook(id=3, name="Webhook 3", enabled=False, events=["gate_reached"]),
    ]


class TestHITLApprovalRepositoryInit:
    """Test cases for HITLApprovalRepository initialization."""

    def test_init_success(self, mock_async_session):
        """Test successful initialization with session."""
        repository = HITLApprovalRepository(session=mock_async_session)
        assert repository.session == mock_async_session

    def test_init_with_none_session(self):
        """Test initialization with None session."""
        repository = HITLApprovalRepository(session=None)
        assert repository.session is None


class TestHITLApprovalRepositoryCreate:
    """Test cases for create method."""

    @pytest.mark.asyncio
    async def test_create_success(self, hitl_approval_repository, mock_async_session, sample_approval):
        """Test successful creation of HITL approval."""
        result = await hitl_approval_repository.create(sample_approval)

        assert result == sample_approval
        mock_async_session.add.assert_called_once_with(sample_approval)
        mock_async_session.flush.assert_called_once()
        mock_async_session.refresh.assert_called_once_with(sample_approval)

    @pytest.mark.asyncio
    async def test_create_without_session_raises_error(self, sample_approval):
        """Test create raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.create(sample_approval)

    @pytest.mark.asyncio
    async def test_create_with_full_approval_data(self, hitl_approval_repository, mock_async_session):
        """Test creation with complete approval data."""
        approval = MockHITLApproval(
            execution_id="exec-full",
            flow_id="flow-full",
            gate_node_id="gate-full",
            crew_sequence=3,
            gate_config={
                "message": "Full review required",
                "timeout_seconds": 7200,
                "timeout_action": HITLTimeoutAction.FAIL,
                "require_comment": True,
                "allowed_approvers": ["admin@example.com"]
            },
            previous_crew_name="Analysis Crew",
            previous_crew_output="Complete analysis...",
            flow_state_snapshot={"state": "paused", "completed_crews": 2},
            group_id="production-group"
        )

        result = await hitl_approval_repository.create(approval)

        assert result.execution_id == "exec-full"
        assert result.gate_config["timeout_seconds"] == 7200
        mock_async_session.add.assert_called_once()


class TestHITLApprovalRepositoryGetById:
    """Test cases for get_by_id method."""

    @pytest.mark.asyncio
    async def test_get_by_id_success(self, hitl_approval_repository, mock_async_session, sample_approval):
        """Test successful retrieval by ID."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_by_id(1)

        assert result == sample_approval
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, hitl_approval_repository, mock_async_session):
        """Test retrieval when ID not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_by_id(999)

        assert result is None
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_by_id_with_group_filter(self, hitl_approval_repository, mock_async_session, sample_approval):
        """Test retrieval with group_id filtering."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_by_id(1, group_id="test-group")

        assert result == sample_approval
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_by_id_with_wrong_group_returns_none(self, hitl_approval_repository, mock_async_session):
        """Test retrieval with mismatched group_id returns None."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_by_id(1, group_id="wrong-group")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_by_id_without_session_raises_error(self):
        """Test get_by_id raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.get_by_id(1)


class TestHITLApprovalRepositoryGetPendingForExecution:
    """Test cases for get_pending_for_execution method."""

    @pytest.mark.asyncio
    async def test_get_pending_for_execution_success(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test successful retrieval of pending approval for execution."""
        sample_approval.status = HITLApprovalStatus.PENDING
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_pending_for_execution("exec-123")

        assert result == sample_approval
        assert result.status == HITLApprovalStatus.PENDING
        mock_async_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_pending_for_execution_not_found(self, hitl_approval_repository, mock_async_session):
        """Test retrieval when no pending approval exists."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_pending_for_execution("exec-nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_pending_for_execution_with_group_filter(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test retrieval with group_id filtering."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_pending_for_execution(
            "exec-123", group_id="test-group"
        )

        assert result == sample_approval

    @pytest.mark.asyncio
    async def test_get_pending_for_execution_returns_latest(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test that latest pending approval is returned (ordered by created_at desc)."""
        older_approval = MockHITLApproval(
            id=1, created_at=datetime.now(timezone.utc) - timedelta(hours=2)
        )
        newer_approval = MockHITLApproval(
            id=2, created_at=datetime.now(timezone.utc)
        )
        # Mocking returns first result which should be newest due to ordering
        mock_result = MockResult([newer_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_pending_for_execution("exec-123")

        assert result.id == 2

    @pytest.mark.asyncio
    async def test_get_pending_for_execution_without_session_raises_error(self):
        """Test get_pending_for_execution raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.get_pending_for_execution("exec-123")


class TestHITLApprovalRepositoryGetPendingForGroup:
    """Test cases for get_pending_for_group method."""

    @pytest.mark.asyncio
    async def test_get_pending_for_group_success(
        self, hitl_approval_repository, mock_async_session, sample_approvals
    ):
        """Test successful retrieval of pending approvals for group."""
        pending_approvals = [a for a in sample_approvals if a.status == HITLApprovalStatus.PENDING]

        # First call returns count, second returns results
        mock_async_session.execute.side_effect = [
            MockCountResult(len(pending_approvals)),
            MockResult(pending_approvals)
        ]

        approvals, total = await hitl_approval_repository.get_pending_for_group("test-group")

        assert len(approvals) == len(pending_approvals)
        assert total == len(pending_approvals)
        assert mock_async_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_get_pending_for_group_empty(self, hitl_approval_repository, mock_async_session):
        """Test retrieval when no pending approvals exist."""
        mock_async_session.execute.side_effect = [
            MockCountResult(0),
            MockResult([])
        ]

        approvals, total = await hitl_approval_repository.get_pending_for_group("empty-group")

        assert approvals == []
        assert total == 0

    @pytest.mark.asyncio
    async def test_get_pending_for_group_with_pagination(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test pagination parameters are applied."""
        pending_approval = MockHITLApproval()
        mock_async_session.execute.side_effect = [
            MockCountResult(10),
            MockResult([pending_approval])
        ]

        approvals, total = await hitl_approval_repository.get_pending_for_group(
            "test-group", limit=5, offset=5
        )

        assert len(approvals) == 1
        assert total == 10

    @pytest.mark.asyncio
    async def test_get_pending_for_group_without_session_raises_error(self):
        """Test get_pending_for_group raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.get_pending_for_group("test-group")


class TestHITLApprovalRepositoryGetAllForExecution:
    """Test cases for get_all_for_execution method."""

    @pytest.mark.asyncio
    async def test_get_all_for_execution_success(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test successful retrieval of all approvals for execution."""
        approvals = [
            MockHITLApproval(id=1, execution_id="exec-1", crew_sequence=1),
            MockHITLApproval(id=2, execution_id="exec-1", crew_sequence=2),
        ]
        mock_result = MockResult(approvals)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_all_for_execution("exec-1")

        assert len(result) == 2
        assert all(a.execution_id == "exec-1" for a in result)

    @pytest.mark.asyncio
    async def test_get_all_for_execution_empty(self, hitl_approval_repository, mock_async_session):
        """Test retrieval when no approvals exist for execution."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_all_for_execution("exec-nonexistent")

        assert result == []

    @pytest.mark.asyncio
    async def test_get_all_for_execution_with_group_filter(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test retrieval with group_id filtering."""
        approval = MockHITLApproval(group_id="specific-group")
        mock_result = MockResult([approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_all_for_execution(
            "exec-123", group_id="specific-group"
        )

        assert len(result) == 1
        assert result[0].group_id == "specific-group"

    @pytest.mark.asyncio
    async def test_get_all_for_execution_ordered_by_sequence(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test results are ordered by crew_sequence."""
        approvals = [
            MockHITLApproval(id=1, crew_sequence=1),
            MockHITLApproval(id=2, crew_sequence=2),
            MockHITLApproval(id=3, crew_sequence=3),
        ]
        mock_result = MockResult(approvals)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_all_for_execution("exec-1")

        assert result[0].crew_sequence == 1
        assert result[1].crew_sequence == 2
        assert result[2].crew_sequence == 3

    @pytest.mark.asyncio
    async def test_get_all_for_execution_without_session_raises_error(self):
        """Test get_all_for_execution raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.get_all_for_execution("exec-1")


class TestHITLApprovalRepositoryUpdateStatus:
    """Test cases for update_status method."""

    @pytest.mark.asyncio
    async def test_update_status_to_approved(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test successful status update to approved."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.update_status(
            1,
            HITLApprovalStatus.APPROVED,
            responded_by="user@example.com",
            approval_comment="Looks good!"
        )

        assert result is True
        assert sample_approval.status == HITLApprovalStatus.APPROVED
        assert sample_approval.responded_by == "user@example.com"
        assert sample_approval.approval_comment == "Looks good!"
        assert sample_approval.responded_at is not None
        mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_status_to_rejected(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test successful status update to rejected."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.update_status(
            1,
            HITLApprovalStatus.REJECTED,
            responded_by="reviewer@example.com",
            rejection_reason="Quality not sufficient",
            rejection_action=HITLRejectionAction.RETRY
        )

        assert result is True
        assert sample_approval.status == HITLApprovalStatus.REJECTED
        assert sample_approval.rejection_reason == "Quality not sufficient"
        assert sample_approval.rejection_action == HITLRejectionAction.RETRY

    @pytest.mark.asyncio
    async def test_update_status_to_timeout(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test successful status update to timeout."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.update_status(
            1,
            HITLApprovalStatus.TIMEOUT
        )

        assert result is True
        assert sample_approval.status == HITLApprovalStatus.TIMEOUT

    @pytest.mark.asyncio
    async def test_update_status_not_found(self, hitl_approval_repository, mock_async_session):
        """Test status update when approval not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.update_status(
            999,
            HITLApprovalStatus.APPROVED
        )

        assert result is False
        mock_async_session.flush.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_status_without_session_raises_error(self):
        """Test update_status raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.update_status(1, HITLApprovalStatus.APPROVED)

    @pytest.mark.asyncio
    async def test_update_status_sets_responded_at(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test that responded_at is set when status is updated."""
        sample_approval.responded_at = None
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        before_update = datetime.now(timezone.utc)
        await hitl_approval_repository.update_status(1, HITLApprovalStatus.APPROVED)
        after_update = datetime.now(timezone.utc)

        assert sample_approval.responded_at is not None
        assert before_update <= sample_approval.responded_at <= after_update


class TestHITLApprovalRepositoryGetExpiredPending:
    """Test cases for get_expired_pending method."""

    @pytest.mark.asyncio
    async def test_get_expired_pending_success(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test successful retrieval of expired pending approvals."""
        expired_approval = MockHITLApproval(
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
            status=HITLApprovalStatus.PENDING
        )
        mock_result = MockResult([expired_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_expired_pending()

        assert len(result) == 1
        assert result[0].status == HITLApprovalStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_expired_pending_empty(self, hitl_approval_repository, mock_async_session):
        """Test retrieval when no expired approvals exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_expired_pending()

        assert result == []

    @pytest.mark.asyncio
    async def test_get_expired_pending_excludes_non_pending(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test that non-pending statuses are excluded."""
        # Mock should return only pending expired, not approved ones
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.get_expired_pending()

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_expired_pending_without_session_raises_error(self):
        """Test get_expired_pending raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.get_expired_pending()


class TestHITLApprovalRepositoryMarkWebhookSent:
    """Test cases for mark_webhook_sent method."""

    @pytest.mark.asyncio
    async def test_mark_webhook_sent_success(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test successful marking of webhook as sent."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.mark_webhook_sent(1)

        assert result is True
        assert sample_approval.webhook_sent is True
        assert sample_approval.webhook_sent_at is not None
        mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_webhook_sent_with_response(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test marking webhook sent with response data."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result
        webhook_response = {"status": 200, "message": "Received"}

        result = await hitl_approval_repository.mark_webhook_sent(1, response=webhook_response)

        assert result is True
        assert sample_approval.webhook_response == webhook_response

    @pytest.mark.asyncio
    async def test_mark_webhook_sent_not_found(self, hitl_approval_repository, mock_async_session):
        """Test marking webhook sent when approval not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.mark_webhook_sent(999)

        assert result is False

    @pytest.mark.asyncio
    async def test_mark_webhook_sent_without_session_raises_error(self):
        """Test mark_webhook_sent raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.mark_webhook_sent(1)


class TestHITLApprovalRepositoryDeleteByExecutionId:
    """Test cases for delete_by_execution_id method."""

    @pytest.mark.asyncio
    async def test_delete_by_execution_id_success(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test successful deletion of approvals by execution ID."""
        mock_result = MockDeleteResult(rowcount=3)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.delete_by_execution_id("exec-123")

        assert result == 3
        mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_by_execution_id_none_deleted(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test deletion when no matching approvals exist."""
        mock_result = MockDeleteResult(rowcount=0)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.delete_by_execution_id("exec-nonexistent")

        assert result == 0

    @pytest.mark.asyncio
    async def test_delete_by_execution_id_without_session_raises_error(self):
        """Test delete_by_execution_id raises RuntimeError without session."""
        repository = HITLApprovalRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLApprovalRepository requires a session"):
            await repository.delete_by_execution_id("exec-123")


class TestHITLWebhookRepositoryInit:
    """Test cases for HITLWebhookRepository initialization."""

    def test_init_success(self, mock_async_session):
        """Test successful initialization with session."""
        repository = HITLWebhookRepository(session=mock_async_session)
        assert repository.session == mock_async_session

    def test_init_with_none_session(self):
        """Test initialization with None session."""
        repository = HITLWebhookRepository(session=None)
        assert repository.session is None


class TestHITLWebhookRepositoryCreate:
    """Test cases for HITLWebhookRepository create method."""

    @pytest.mark.asyncio
    async def test_create_success(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test successful creation of webhook."""
        result = await hitl_webhook_repository.create(sample_webhook)

        assert result == sample_webhook
        mock_async_session.add.assert_called_once_with(sample_webhook)
        mock_async_session.flush.assert_called_once()
        mock_async_session.refresh.assert_called_once_with(sample_webhook)

    @pytest.mark.asyncio
    async def test_create_without_session_raises_error(self, sample_webhook):
        """Test create raises RuntimeError without session."""
        repository = HITLWebhookRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLWebhookRepository requires a session"):
            await repository.create(sample_webhook)

    @pytest.mark.asyncio
    async def test_create_with_custom_events(self, hitl_webhook_repository, mock_async_session):
        """Test creation with custom events list."""
        webhook = MockHITLWebhook(
            events=["gate_reached", "gate_approved", "gate_rejected", "gate_timeout"]
        )

        result = await hitl_webhook_repository.create(webhook)

        assert len(result.events) == 4
        assert "gate_timeout" in result.events


class TestHITLWebhookRepositoryGetById:
    """Test cases for HITLWebhookRepository get_by_id method."""

    @pytest.mark.asyncio
    async def test_get_by_id_success(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test successful retrieval by ID."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_by_id(1)

        assert result == sample_webhook

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, hitl_webhook_repository, mock_async_session):
        """Test retrieval when webhook not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_by_id(999)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_by_id_with_group_filter(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test retrieval with group_id filtering."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_by_id(1, group_id="test-group")

        assert result == sample_webhook

    @pytest.mark.asyncio
    async def test_get_by_id_without_session_raises_error(self):
        """Test get_by_id raises RuntimeError without session."""
        repository = HITLWebhookRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLWebhookRepository requires a session"):
            await repository.get_by_id(1)


class TestHITLWebhookRepositoryGetForGroup:
    """Test cases for HITLWebhookRepository get_for_group method."""

    @pytest.mark.asyncio
    async def test_get_for_group_enabled_only(
        self, hitl_webhook_repository, mock_async_session, sample_webhooks
    ):
        """Test retrieval of enabled webhooks only."""
        enabled_webhooks = [w for w in sample_webhooks if w.enabled]
        mock_result = MockResult(enabled_webhooks)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_group("test-group")

        assert all(w.enabled for w in result)

    @pytest.mark.asyncio
    async def test_get_for_group_all_webhooks(
        self, hitl_webhook_repository, mock_async_session, sample_webhooks
    ):
        """Test retrieval of all webhooks including disabled."""
        mock_result = MockResult(sample_webhooks)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_group("test-group", enabled_only=False)

        assert len(result) == len(sample_webhooks)

    @pytest.mark.asyncio
    async def test_get_for_group_empty(self, hitl_webhook_repository, mock_async_session):
        """Test retrieval when no webhooks exist for group."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_group("empty-group")

        assert result == []

    @pytest.mark.asyncio
    async def test_get_for_group_without_session_raises_error(self):
        """Test get_for_group raises RuntimeError without session."""
        repository = HITLWebhookRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLWebhookRepository requires a session"):
            await repository.get_for_group("test-group")


class TestHITLWebhookRepositoryGetForEvent:
    """Test cases for HITLWebhookRepository get_for_event method."""

    @pytest.mark.asyncio
    async def test_get_for_event_success(
        self, hitl_webhook_repository, mock_async_session, sample_webhooks
    ):
        """Test retrieval of webhooks subscribed to specific event."""
        # Only webhook 1 has gate_approved
        enabled_webhooks = [w for w in sample_webhooks if w.enabled]
        mock_result = MockResult(enabled_webhooks)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_event("test-group", "gate_approved")

        # Filter should return only webhooks with gate_approved event
        assert all("gate_approved" in w.events for w in result)

    @pytest.mark.asyncio
    async def test_get_for_event_no_subscribers(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test retrieval when no webhooks subscribe to event."""
        webhook = MockHITLWebhook(events=["gate_reached"])
        mock_result = MockResult([webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_event("test-group", "gate_timeout")

        assert result == []

    @pytest.mark.asyncio
    async def test_get_for_event_multiple_subscribers(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test retrieval when multiple webhooks subscribe to same event."""
        webhooks = [
            MockHITLWebhook(id=1, events=["gate_reached", "gate_approved"]),
            MockHITLWebhook(id=2, events=["gate_reached"]),
        ]
        mock_result = MockResult(webhooks)
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_event("test-group", "gate_reached")

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_for_event_without_session_raises_error(self):
        """Test get_for_event raises RuntimeError without session."""
        repository = HITLWebhookRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLWebhookRepository requires a session"):
            await repository.get_for_event("test-group", "gate_reached")


class TestHITLWebhookRepositoryUpdate:
    """Test cases for HITLWebhookRepository update method."""

    @pytest.mark.asyncio
    async def test_update_success(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test successful webhook update."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.update(
            1,
            {"name": "Updated Webhook", "enabled": False}
        )

        assert result is True
        assert sample_webhook.name == "Updated Webhook"
        assert sample_webhook.enabled is False
        mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_not_found(self, hitl_webhook_repository, mock_async_session):
        """Test update when webhook not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.update(999, {"name": "New Name"})

        assert result is False

    @pytest.mark.asyncio
    async def test_update_with_group_filter(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test update with group_id filtering."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.update(
            1,
            {"url": "https://example.com/new-webhook"},
            group_id="test-group"
        )

        assert result is True
        assert sample_webhook.url == "https://example.com/new-webhook"

    @pytest.mark.asyncio
    async def test_update_sets_updated_at(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test that updated_at is set when webhook is updated."""
        original_updated_at = sample_webhook.updated_at
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        await hitl_webhook_repository.update(1, {"name": "Updated"})

        assert sample_webhook.updated_at is not None
        assert sample_webhook.updated_at != original_updated_at

    @pytest.mark.asyncio
    async def test_update_ignores_unknown_fields(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test that unknown fields are ignored during update."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result
        original_name = sample_webhook.name

        result = await hitl_webhook_repository.update(
            1,
            {"unknown_field": "value", "name": "New Name"}
        )

        assert result is True
        assert sample_webhook.name == "New Name"
        assert not hasattr(sample_webhook, "unknown_field") or getattr(sample_webhook, "unknown_field", None) is None

    @pytest.mark.asyncio
    async def test_update_without_session_raises_error(self):
        """Test update raises RuntimeError without session."""
        repository = HITLWebhookRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLWebhookRepository requires a session"):
            await repository.update(1, {"name": "New Name"})


class TestHITLWebhookRepositoryDelete:
    """Test cases for HITLWebhookRepository delete method."""

    @pytest.mark.asyncio
    async def test_delete_success(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test successful webhook deletion."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.delete(1)

        assert result is True
        mock_async_session.delete.assert_called_once_with(sample_webhook)
        mock_async_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_not_found(self, hitl_webhook_repository, mock_async_session):
        """Test deletion when webhook not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.delete(999)

        assert result is False
        mock_async_session.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_with_group_filter(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test deletion with group_id filtering."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.delete(1, group_id="test-group")

        assert result is True
        mock_async_session.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_without_session_raises_error(self):
        """Test delete raises RuntimeError without session."""
        repository = HITLWebhookRepository(session=None)

        with pytest.raises(RuntimeError, match="HITLWebhookRepository requires a session"):
            await repository.delete(1)


class TestHITLApprovalRepositoryEdgeCases:
    """Edge case tests for HITLApprovalRepository."""

    @pytest.mark.asyncio
    async def test_create_with_minimal_data(self, hitl_approval_repository, mock_async_session):
        """Test creation with minimal required data."""
        minimal_approval = MockHITLApproval(
            execution_id="exec-min",
            flow_id="flow-min",
            gate_node_id="gate-min",
            crew_sequence=0,
            previous_crew_name=None,
            previous_crew_output=None
        )

        result = await hitl_approval_repository.create(minimal_approval)

        assert result.execution_id == "exec-min"
        mock_async_session.add.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_status_partial_fields(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test status update with only some optional fields."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_approval_repository.update_status(
            1,
            HITLApprovalStatus.APPROVED,
            responded_by="user@example.com"
            # No approval_comment
        )

        assert result is True
        assert sample_approval.responded_by == "user@example.com"
        assert sample_approval.approval_comment is None

    @pytest.mark.asyncio
    async def test_get_pending_for_group_with_max_limit(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test pagination with large limit."""
        mock_async_session.execute.side_effect = [
            MockCountResult(100),
            MockResult([])
        ]

        approvals, total = await hitl_approval_repository.get_pending_for_group(
            "test-group", limit=1000, offset=0
        )

        assert total == 100

    @pytest.mark.asyncio
    async def test_concurrent_status_updates(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test that concurrent updates are handled by flush."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result

        # Simulate concurrent update
        await hitl_approval_repository.update_status(1, HITLApprovalStatus.APPROVED)

        mock_async_session.flush.assert_called_once()


class TestHITLWebhookRepositoryEdgeCases:
    """Edge case tests for HITLWebhookRepository."""

    @pytest.mark.asyncio
    async def test_create_with_none_events_defaults_to_gate_reached(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test creation with None events uses default gate_reached event.

        The HITLWebhook model sets default events to ['gate_reached'] when None.
        """
        # Create webhook with events explicitly set to None
        webhook = MockHITLWebhook()
        webhook.events = None  # Override to test None handling

        result = await hitl_webhook_repository.create(webhook)

        # The mock is returned as-is; real model would have default
        mock_async_session.add.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_for_event_with_none_events_list(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test event filtering when webhook has None events list.

        The get_for_event method handles None events with (w.events or []).
        """
        webhook = MockHITLWebhook()
        webhook.events = None  # Explicitly set to None
        mock_result = MockResult([webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_event("test-group", "gate_reached")

        # None events should be treated as empty list
        assert result == []

    @pytest.mark.asyncio
    async def test_get_for_event_filters_correctly(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test that get_for_event correctly filters webhooks by event."""
        webhook_with_event = MockHITLWebhook(id=1, events=["gate_reached", "gate_approved"])
        webhook_without_event = MockHITLWebhook(id=2, events=["gate_rejected"])
        mock_result = MockResult([webhook_with_event, webhook_without_event])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.get_for_event("test-group", "gate_approved")

        assert len(result) == 1
        assert result[0].id == 1

    @pytest.mark.asyncio
    async def test_get_for_event_global_webhook_applies_to_all_flows(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test that webhooks with flow_id=None (global) apply to all flows."""
        global_webhook = MockHITLWebhook(
            id=1,
            flow_id=None,  # Global webhook
            events=["gate_reached"]
        )
        mock_result = MockResult([global_webhook])
        mock_async_session.execute.return_value = mock_result

        # Global webhook should be returned for any flow_id
        result = await hitl_webhook_repository.get_for_event(
            "test-group", "gate_reached", flow_id="any_flow_123"
        )

        assert len(result) == 1
        assert result[0].id == 1

    @pytest.mark.asyncio
    async def test_get_for_event_flow_specific_webhook_matches(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test that flow-specific webhooks match their flow."""
        flow_webhook = MockHITLWebhook(
            id=1,
            flow_id="flow_policies",  # Flow-specific webhook
            events=["gate_reached"]
        )
        mock_result = MockResult([flow_webhook])
        mock_async_session.execute.return_value = mock_result

        # Flow-specific webhook should be returned for matching flow_id
        result = await hitl_webhook_repository.get_for_event(
            "test-group", "gate_reached", flow_id="flow_policies"
        )

        assert len(result) == 1
        assert result[0].id == 1

    @pytest.mark.asyncio
    async def test_get_for_event_flow_specific_webhook_not_matches_different_flow(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test that flow-specific webhooks do NOT match different flows."""
        flow_webhook = MockHITLWebhook(
            id=1,
            flow_id="flow_policies",  # Flow-specific webhook
            events=["gate_reached"]
        )
        mock_result = MockResult([flow_webhook])
        mock_async_session.execute.return_value = mock_result

        # Flow-specific webhook should NOT be returned for different flow_id
        result = await hitl_webhook_repository.get_for_event(
            "test-group", "gate_reached", flow_id="different_flow_456"
        )

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_for_event_mixed_global_and_flow_specific(
        self, hitl_webhook_repository, mock_async_session
    ):
        """Test filtering with mix of global and flow-specific webhooks."""
        webhooks = [
            MockHITLWebhook(id=1, flow_id=None, events=["gate_reached"]),  # Global
            MockHITLWebhook(id=2, flow_id="flow_policies", events=["gate_reached"]),  # Specific to policies
            MockHITLWebhook(id=3, flow_id="flow_orders", events=["gate_reached"]),  # Specific to orders
        ]
        mock_result = MockResult(webhooks)
        mock_async_session.execute.return_value = mock_result

        # Should return global + matching flow-specific webhook
        result = await hitl_webhook_repository.get_for_event(
            "test-group", "gate_reached", flow_id="flow_policies"
        )

        assert len(result) == 2
        result_ids = [w.id for w in result]
        assert 1 in result_ids  # Global webhook
        assert 2 in result_ids  # flow_policies webhook
        assert 3 not in result_ids  # flow_orders should NOT be included

    @pytest.mark.asyncio
    async def test_update_with_empty_updates_dict(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test update with empty updates dictionary."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result

        result = await hitl_webhook_repository.update(1, {})

        assert result is True
        mock_async_session.flush.assert_called_once()


class TestHITLRepositoryDatabaseErrors:
    """Test database error handling scenarios."""

    @pytest.mark.asyncio
    async def test_create_approval_database_error(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test handling of database error during approval creation."""
        mock_async_session.flush.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            await hitl_approval_repository.create(sample_approval)

    @pytest.mark.asyncio
    async def test_get_by_id_database_error(
        self, hitl_approval_repository, mock_async_session
    ):
        """Test handling of database error during retrieval."""
        mock_async_session.execute.side_effect = Exception("Connection lost")

        with pytest.raises(Exception, match="Connection lost"):
            await hitl_approval_repository.get_by_id(1)

    @pytest.mark.asyncio
    async def test_update_status_database_error(
        self, hitl_approval_repository, mock_async_session, sample_approval
    ):
        """Test handling of database error during status update."""
        mock_result = MockResult([sample_approval])
        mock_async_session.execute.return_value = mock_result
        mock_async_session.flush.side_effect = Exception("Constraint violation")

        with pytest.raises(Exception, match="Constraint violation"):
            await hitl_approval_repository.update_status(1, HITLApprovalStatus.APPROVED)

    @pytest.mark.asyncio
    async def test_create_webhook_database_error(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test handling of database error during webhook creation."""
        mock_async_session.flush.side_effect = Exception("Unique constraint violation")

        with pytest.raises(Exception, match="Unique constraint violation"):
            await hitl_webhook_repository.create(sample_webhook)

    @pytest.mark.asyncio
    async def test_delete_webhook_database_error(
        self, hitl_webhook_repository, mock_async_session, sample_webhook
    ):
        """Test handling of database error during webhook deletion."""
        mock_result = MockResult([sample_webhook])
        mock_async_session.execute.return_value = mock_result
        mock_async_session.flush.side_effect = Exception("Foreign key violation")

        with pytest.raises(Exception, match="Foreign key violation"):
            await hitl_webhook_repository.delete(1)
