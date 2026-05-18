"""
Comprehensive unit tests for services/hitl_webhook_service.py
"""

import hashlib
import hmac
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, AsyncMock, MagicMock

from src.services.hitl_webhook_service import (
    HITLWebhookService,
    HITLWebhookServiceError,
    HITLWebhookNotFoundError,
    WEBHOOK_TIMEOUT_SECONDS,
)
from src.schemas.hitl import (
    HITLWebhookCreate,
    HITLWebhookUpdate,
    HITLWebhookEventEnum,
    HITLApprovalStatusEnum,
    HITLWebhookResponse,
)


def _make_webhook(
    id=1,
    group_id="grp-1",
    flow_id=None,
    name="Test Hook",
    url="https://example.com/webhook",
    enabled=True,
    events=None,
    headers=None,
    secret=None,
    created_at=None,
    updated_at=None,
):
    w = MagicMock()
    w.id = id
    w.group_id = group_id
    w.flow_id = flow_id
    w.name = name
    w.url = url
    w.enabled = enabled
    w.events = events or ["gate_reached"]
    w.headers = headers or {}
    w.secret = secret
    w.created_at = created_at or datetime.now(timezone.utc)
    w.updated_at = updated_at or datetime.now(timezone.utc)
    return w


def _make_approval(
    id=10,
    group_id="grp-1",
    execution_id="exec-1",
    flow_id="flow-1",
    gate_node_id="node-1",
    message="Please approve",
    previous_crew_name="Crew A",
    previous_crew_output="Some output",
    status="pending",
    responded_by=None,
    expires_at=None,
):
    a = MagicMock()
    a.id = id
    a.group_id = group_id
    a.execution_id = execution_id
    a.flow_id = flow_id
    a.gate_node_id = gate_node_id
    a.message = message
    a.previous_crew_name = previous_crew_name
    a.previous_crew_output = previous_crew_output
    a.status = status
    a.responded_by = responded_by
    a.expires_at = expires_at
    return a


@pytest.fixture
def mock_session():
    return AsyncMock()


@pytest.fixture
def mock_webhook_repo():
    return AsyncMock()


@pytest.fixture
def mock_approval_repo():
    return AsyncMock()


@pytest.fixture
def service(mock_session, mock_webhook_repo, mock_approval_repo):
    return HITLWebhookService(
        session=mock_session,
        webhook_repository=mock_webhook_repo,
        approval_repository=mock_approval_repo,
    )


class TestHITLWebhookServiceInit:
    """Tests for HITLWebhookService initialization."""

    def test_creates_webhook_repo_if_none(self, mock_session):
        with patch("src.services.hitl_webhook_service.HITLWebhookRepository") as MockRepo:
            with patch("src.services.hitl_webhook_service.HITLApprovalRepository"):
                svc = HITLWebhookService(session=mock_session)
        MockRepo.assert_called_once_with(mock_session)

    def test_uses_provided_repos(self, mock_session, mock_webhook_repo, mock_approval_repo):
        svc = HITLWebhookService(
            session=mock_session,
            webhook_repository=mock_webhook_repo,
            approval_repository=mock_approval_repo,
        )
        assert svc.webhook_repo is mock_webhook_repo
        assert svc.approval_repo is mock_approval_repo

    def test_session_stored(self, mock_session, service):
        assert service.session is mock_session


class TestCreateWebhook:
    """Tests for create_webhook."""

    @pytest.mark.asyncio
    async def test_creates_and_returns_response(self, service, mock_webhook_repo):
        webhook_data = HITLWebhookCreate(
            name="My Hook",
            url="https://example.com/hook",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_REACHED],
            headers={},
            secret=None,
        )

        mock_created = _make_webhook(id=5, group_id="grp-1", name="My Hook")
        mock_webhook_repo.create = AsyncMock(return_value=mock_created)

        result = await service.create_webhook(webhook_data, "grp-1")

        assert isinstance(result, HITLWebhookResponse)
        assert result.id == 5

    @pytest.mark.asyncio
    async def test_raises_on_db_error(self, service, mock_webhook_repo):
        from sqlalchemy.exc import SQLAlchemyError

        webhook_data = HITLWebhookCreate(
            name="My Hook",
            url="https://example.com/hook",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_REACHED],
        )
        mock_webhook_repo.create = AsyncMock(side_effect=SQLAlchemyError("db error"))

        with pytest.raises(HITLWebhookServiceError, match="Failed to create webhook"):
            await service.create_webhook(webhook_data, "grp-1")

    @pytest.mark.asyncio
    async def test_sets_group_id(self, service, mock_webhook_repo):
        webhook_data = HITLWebhookCreate(
            name="Hook",
            url="https://example.com/hook",
            enabled=True,
            events=[HITLWebhookEventEnum.GATE_APPROVED],
        )
        mock_created = _make_webhook(id=1, group_id="grp-99")
        mock_webhook_repo.create = AsyncMock(return_value=mock_created)

        result = await service.create_webhook(webhook_data, "grp-99")
        assert result.group_id == "grp-99"


class TestGetWebhook:
    """Tests for get_webhook."""

    @pytest.mark.asyncio
    async def test_returns_webhook_response(self, service, mock_webhook_repo):
        mock_webhook = _make_webhook(id=1)
        mock_webhook_repo.get_by_id = AsyncMock(return_value=mock_webhook)

        result = await service.get_webhook(1, "grp-1")
        assert isinstance(result, HITLWebhookResponse)
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_raises_not_found_when_missing(self, service, mock_webhook_repo):
        mock_webhook_repo.get_by_id = AsyncMock(return_value=None)

        with pytest.raises(HITLWebhookNotFoundError, match="Webhook 99 not found"):
            await service.get_webhook(99, "grp-1")


class TestListWebhooks:
    """Tests for list_webhooks."""

    @pytest.mark.asyncio
    async def test_returns_empty_list(self, service, mock_webhook_repo):
        mock_webhook_repo.get_for_group = AsyncMock(return_value=[])
        result = await service.list_webhooks("grp-1")
        assert result.items == []
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_returns_all_webhooks(self, service, mock_webhook_repo):
        hooks = [_make_webhook(id=i) for i in range(3)]
        mock_webhook_repo.get_for_group = AsyncMock(return_value=hooks)
        result = await service.list_webhooks("grp-1")
        assert result.total == 3
        assert len(result.items) == 3


class TestUpdateWebhook:
    """Tests for update_webhook."""

    @pytest.mark.asyncio
    async def test_updates_and_returns_updated(self, service, mock_webhook_repo):
        webhook_update = HITLWebhookUpdate(name="Updated Name")
        mock_webhook_repo.update = AsyncMock(return_value=True)

        updated_hook = _make_webhook(id=1, name="Updated Name")
        mock_webhook_repo.get_by_id = AsyncMock(return_value=updated_hook)

        result = await service.update_webhook(1, webhook_update, "grp-1")
        assert result.name == "Updated Name"

    @pytest.mark.asyncio
    async def test_raises_not_found_when_update_fails(self, service, mock_webhook_repo):
        webhook_update = HITLWebhookUpdate(name="X")
        mock_webhook_repo.update = AsyncMock(return_value=False)

        with pytest.raises(HITLWebhookNotFoundError):
            await service.update_webhook(99, webhook_update, "grp-1")

    @pytest.mark.asyncio
    async def test_only_passes_non_none_fields(self, service, mock_webhook_repo):
        # Update with only url
        webhook_update = HITLWebhookUpdate(url="https://new.example.com")
        mock_webhook_repo.update = AsyncMock(return_value=True)
        updated_hook = _make_webhook(id=1, url="https://new.example.com")
        mock_webhook_repo.get_by_id = AsyncMock(return_value=updated_hook)

        await service.update_webhook(1, webhook_update, "grp-1")
        call_args = mock_webhook_repo.update.call_args
        updates = call_args[0][1]  # positional second arg
        assert "url" in updates
        assert "name" not in updates


class TestDeleteWebhook:
    """Tests for delete_webhook."""

    @pytest.mark.asyncio
    async def test_returns_true_on_success(self, service, mock_webhook_repo):
        mock_webhook_repo.delete = AsyncMock(return_value=True)
        result = await service.delete_webhook(1, "grp-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_raises_not_found_when_not_deleted(self, service, mock_webhook_repo):
        mock_webhook_repo.delete = AsyncMock(return_value=False)
        with pytest.raises(HITLWebhookNotFoundError):
            await service.delete_webhook(99, "grp-1")


class TestGenerateSignature:
    """Tests for _generate_signature."""

    def test_returns_hex_string(self, service):
        sig = service._generate_signature('{"event": "test"}', "secret123")
        assert isinstance(sig, str)
        assert len(sig) == 64  # SHA256 hex = 64 chars

    def test_is_deterministic(self, service):
        sig1 = service._generate_signature("payload", "secret")
        sig2 = service._generate_signature("payload", "secret")
        assert sig1 == sig2

    def test_different_secrets_give_different_sigs(self, service):
        sig1 = service._generate_signature("payload", "secret1")
        sig2 = service._generate_signature("payload", "secret2")
        assert sig1 != sig2

    def test_matches_manual_hmac(self, service):
        payload = "test payload"
        secret = "mysecret"
        expected = hmac.new(
            secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        assert service._generate_signature(payload, secret) == expected


class TestBuildPayload:
    """Tests for _build_payload."""

    def test_returns_payload_with_event(self, service):
        approval = _make_approval()
        payload = service._build_payload(approval, HITLWebhookEventEnum.GATE_REACHED)
        assert payload.event == HITLWebhookEventEnum.GATE_REACHED

    def test_truncates_long_output(self, service):
        approval = _make_approval(previous_crew_output="A" * 600)
        payload = service._build_payload(approval, HITLWebhookEventEnum.GATE_APPROVED)
        assert len(payload.previous_crew_output_preview) <= 503  # 500 + "..."

    def test_output_preview_none_when_no_output(self, service):
        approval = _make_approval()
        approval.previous_crew_output = None
        payload = service._build_payload(approval, HITLWebhookEventEnum.GATE_REJECTED)
        assert payload.previous_crew_output_preview is None

    def test_status_none_for_pending(self, service):
        approval = _make_approval(status="pending")
        payload = service._build_payload(approval, HITLWebhookEventEnum.GATE_REACHED)
        assert payload.status is None

    def test_status_set_for_approved(self, service):
        approval = _make_approval(status="approved")
        payload = service._build_payload(approval, HITLWebhookEventEnum.GATE_APPROVED)
        assert payload.status == HITLApprovalStatusEnum.APPROVED

    def test_approval_url_included(self, service):
        approval = _make_approval()
        payload = service._build_payload(
            approval, HITLWebhookEventEnum.GATE_REACHED, approval_url="https://example.com/approve/10"
        )
        assert payload.approval_url == "https://example.com/approve/10"


class TestSendWebhook:
    """Tests for _send_webhook."""

    @pytest.mark.asyncio
    async def test_returns_success_on_200(self, service):
        webhook = _make_webhook(url="https://example.com/hook", secret=None)
        payload = MagicMock()
        payload.model_dump.return_value = {"event": "gate_reached"}

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.status_code = 200

        with patch("src.services.hitl_webhook_service.httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post = AsyncMock(return_value=mock_response)
            MockClient.return_value.__aenter__ = AsyncMock(return_value=instance)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await service._send_webhook(webhook, payload)

        assert result is not None
        assert result["success"] is True
        assert result["status_code"] == 200

    @pytest.mark.asyncio
    async def test_returns_failure_on_4xx(self, service):
        webhook = _make_webhook()
        payload = MagicMock()
        payload.model_dump.return_value = {}

        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 400
        mock_response.text = "Bad Request"

        with patch("src.services.hitl_webhook_service.httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post = AsyncMock(return_value=mock_response)
            MockClient.return_value.__aenter__ = AsyncMock(return_value=instance)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await service._send_webhook(webhook, payload)

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self, service):
        import httpx

        webhook = _make_webhook()
        payload = MagicMock()
        payload.model_dump.return_value = {}

        with patch("src.services.hitl_webhook_service.httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
            MockClient.return_value.__aenter__ = AsyncMock(return_value=instance)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await service._send_webhook(webhook, payload)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_request_error(self, service):
        import httpx

        webhook = _make_webhook()
        payload = MagicMock()
        payload.model_dump.return_value = {}

        with patch("src.services.hitl_webhook_service.httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post = AsyncMock(side_effect=httpx.RequestError("connection refused"))
            MockClient.return_value.__aenter__ = AsyncMock(return_value=instance)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await service._send_webhook(webhook, payload)

        assert result is None

    @pytest.mark.asyncio
    async def test_signature_header_added_when_secret_set(self, service):
        webhook = _make_webhook(secret="super_secret")
        payload = MagicMock()
        payload.model_dump.return_value = {"event": "gate_reached"}

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.status_code = 200

        captured_headers = {}

        async def capture_post(url, content, headers):
            captured_headers.update(headers)
            return mock_response

        with patch("src.services.hitl_webhook_service.httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post = capture_post
            MockClient.return_value.__aenter__ = AsyncMock(return_value=instance)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            await service._send_webhook(webhook, payload)

        assert "X-Kasal-Signature" in captured_headers

    @pytest.mark.asyncio
    async def test_custom_headers_merged(self, service):
        webhook = _make_webhook(headers={"X-Custom": "value"}, secret=None)
        payload = MagicMock()
        payload.model_dump.return_value = {}

        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.status_code = 200

        captured_headers = {}

        async def capture_post(url, content, headers):
            captured_headers.update(headers)
            return mock_response

        with patch("src.services.hitl_webhook_service.httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post = capture_post
            MockClient.return_value.__aenter__ = AsyncMock(return_value=instance)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            await service._send_webhook(webhook, payload)

        assert "X-Custom" in captured_headers


class TestSendNotification:
    """Tests for _send_notification (via public wrappers)."""

    @pytest.mark.asyncio
    async def test_returns_true_when_no_webhooks(self, service, mock_webhook_repo):
        mock_webhook_repo.get_for_event = AsyncMock(return_value=[])
        approval = _make_approval()
        result = await service._send_notification(approval, HITLWebhookEventEnum.GATE_REACHED)
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self, service, mock_webhook_repo):
        mock_webhook_repo.get_for_event = AsyncMock(side_effect=RuntimeError("db error"))
        approval = _make_approval()
        result = await service._send_notification(approval, HITLWebhookEventEnum.GATE_REACHED)
        assert result is False

    @pytest.mark.asyncio
    async def test_marks_webhook_sent_for_gate_reached(
        self, service, mock_webhook_repo, mock_approval_repo
    ):
        webhook = _make_webhook()
        mock_webhook_repo.get_for_event = AsyncMock(return_value=[webhook])
        mock_approval_repo.mark_webhook_sent = AsyncMock(return_value=None)
        approval = _make_approval()

        response_data = {"status_code": 200, "success": True}

        with patch.object(service, "_send_webhook", new_callable=AsyncMock, return_value=response_data):
            result = await service._send_notification(
                approval, HITLWebhookEventEnum.GATE_REACHED
            )

        assert result is True
        mock_approval_repo.mark_webhook_sent.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_false_when_all_webhooks_fail(
        self, service, mock_webhook_repo
    ):
        webhooks = [_make_webhook(id=i) for i in range(2)]
        mock_webhook_repo.get_for_event = AsyncMock(return_value=webhooks)
        approval = _make_approval()

        with patch.object(service, "_send_webhook", new_callable=AsyncMock, return_value=None):
            result = await service._send_notification(approval, HITLWebhookEventEnum.GATE_APPROVED)

        assert result is False


class TestNotificationPublicMethods:
    """Tests for the public notification methods."""

    @pytest.mark.asyncio
    async def test_send_gate_reached_notification(self, service):
        approval = _make_approval()
        with patch.object(service, "_send_notification", new_callable=AsyncMock, return_value=True) as mock_notify:
            result = await service.send_gate_reached_notification(approval, approval_url="https://example.com")

        mock_notify.assert_called_once_with(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_REACHED,
            approval_url="https://example.com",
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_send_gate_approved_notification(self, service):
        approval = _make_approval()
        with patch.object(service, "_send_notification", new_callable=AsyncMock, return_value=True) as mock_notify:
            result = await service.send_gate_approved_notification(approval)

        mock_notify.assert_called_once_with(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_APPROVED,
        )

    @pytest.mark.asyncio
    async def test_send_gate_rejected_notification(self, service):
        approval = _make_approval()
        with patch.object(service, "_send_notification", new_callable=AsyncMock, return_value=False) as mock_notify:
            result = await service.send_gate_rejected_notification(approval)

        mock_notify.assert_called_once_with(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_REJECTED,
        )

    @pytest.mark.asyncio
    async def test_send_gate_timeout_notification(self, service):
        approval = _make_approval()
        with patch.object(service, "_send_notification", new_callable=AsyncMock, return_value=True) as mock_notify:
            await service.send_gate_timeout_notification(approval)

        mock_notify.assert_called_once_with(
            approval=approval,
            event=HITLWebhookEventEnum.GATE_TIMEOUT,
        )
