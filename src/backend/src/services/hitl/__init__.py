"""
Human-in-the-loop: everything about pausing a run for a person.

- ``service``  — the approval records themselves (create, decide, expire, query)
- ``notify``   — telling the UI a run is waiting, in-process or from the subprocess
- ``webhook``  — outbound notifications to Slack/Teams/custom endpoints
- ``timeout``  — the background sweep that expires approvals nobody answered

Two callers gate on these: the tool-approval hook
(``engines/kasal/kernel/tool_approval.py``) and the task-review guardrail
(``services/guardrails/core/human_review_guardrail.py``). Both must fail CLOSED —
if an approval cannot be created, the tool or task does not run.
"""

from src.services.hitl.notify import notify_input_needed, notify_input_needed_sse
from src.services.hitl.service import (
    HITLApprovalAlreadyProcessedError,
    HITLApprovalExpiredError,
    HITLApprovalNotFoundError,
    HITLPermissionDeniedError,
    HITLService,
    HITLServiceError,
)
from src.services.hitl.timeout import (
    HITLTimeoutService,
    start_hitl_timeout_service,
    stop_hitl_timeout_service,
)
from src.services.hitl.webhook import (
    HITLWebhookNotFoundError,
    HITLWebhookService,
    HITLWebhookServiceError,
)

__all__ = [
    # Approvals
    "HITLService",
    "HITLServiceError",
    "HITLApprovalNotFoundError",
    "HITLApprovalAlreadyProcessedError",
    "HITLApprovalExpiredError",
    "HITLPermissionDeniedError",
    # Live notification
    "notify_input_needed",
    "notify_input_needed_sse",
    # Webhooks
    "HITLWebhookService",
    "HITLWebhookServiceError",
    "HITLWebhookNotFoundError",
    # Expiry sweep
    "HITLTimeoutService",
    "start_hitl_timeout_service",
    "stop_hitl_timeout_service",
]
