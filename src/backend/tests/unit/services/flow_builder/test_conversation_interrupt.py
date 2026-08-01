"""A human's decision reaching the flow that paused for it.

The gate already stopped the flow and recorded what the approver decided. What
did not exist was the return path: the resumed run restored its state and
carried on as though nothing had been asked, so a flow could pause for an answer
and had no way to read it.

Once the decision is a state channel, a gate condition reads it the way it reads
anything else — which is what makes an approval gate and a conversational turn
the same mechanism.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

from src.services.flow_builder.backend_flow import BackendFlow
from src.services.flow_builder.conversation.interrupt import (
    APPROVAL_CHANNEL,
    APPROVAL_CONFIG_KEY,
    approval_payload,
    declares_channel,
    interrupt_inputs,
)

WITH_CHANNEL = {
    "enabled": True,
    "model": {"type": "object", "properties": {"approval": {}, "topic": {}}},
}
WITHOUT_CHANNEL = {
    "enabled": True,
    "model": {"type": "object", "properties": {"topic": {}}},
}

DECISION = {"status": "approved", "comment": "ship it"}


def an_approval(**overrides):
    defaults = dict(
        status="approved",
        approval_comment="looks right",
        rejection_reason=None,
        rejection_action=None,
        responded_by="someone@example.com",
        responded_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
        gate_node_id="gate-1",
        # Fields the payload must NOT carry.
        id=42,
        webhook_response={"ok": True},
        group_id="group-1",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestPayload:
    def test_carries_what_a_flow_could_branch_on(self):
        payload = approval_payload(an_approval())

        assert payload["status"] == "approved"
        assert payload["comment"] == "looks right"
        assert payload["responded_by"] == "someone@example.com"
        assert payload["gate_node_id"] == "gate-1"

    def test_leaves_out_the_gate_s_own_bookkeeping(self):
        # Ids and webhook state are Kasal's business, not the flow's.
        payload = approval_payload(an_approval())

        assert "id" not in payload
        assert "webhook_response" not in payload
        assert "group_id" not in payload

    def test_a_rejection_carries_what_to_do_next(self):
        payload = approval_payload(
            an_approval(
                status="rejected",
                rejection_reason="wrong region",
                rejection_action="retry",
            )
        )

        assert payload["status"] == "rejected"
        assert payload["rejection_reason"] == "wrong region"
        assert payload["rejection_action"] == "retry"

    def test_the_timestamp_is_serializable(self):
        # It travels through a run config into a spawned subprocess, so it has
        # to survive JSON.
        assert approval_payload(an_approval())["responded_at"].startswith("2026-08-01")

    def test_no_response_yet_is_not_an_error(self):
        payload = approval_payload(an_approval(responded_at=None, responded_by=None))

        assert payload["responded_at"] is None


class TestChannelGate:
    def test_a_declared_channel_receives_the_decision(self):
        assert interrupt_inputs(WITH_CHANNEL, DECISION) == {APPROVAL_CHANNEL: DECISION}

    def test_an_undeclared_channel_receives_nothing(self):
        # A typed state RAISES on an input it has no channel for. That behaviour
        # is what makes a misspelled input visible, and it must not turn
        # "approve a gate" into "crash the resume".
        assert interrupt_inputs(WITHOUT_CHANNEL, DECISION) == {}

    def test_no_decision_writes_nothing(self):
        assert interrupt_inputs(WITH_CHANNEL, None) == {}
        assert interrupt_inputs(WITH_CHANNEL, {}) == {}

    def test_an_undeclared_state_writes_nothing(self):
        # A flow with no declared state runs on a dict, which would accept the
        # write — but a flow that declared nothing cannot have a condition
        # reading it either, so there is nothing to deliver.
        assert interrupt_inputs({}, DECISION) == {}
        assert interrupt_inputs(None, DECISION) == {}

    def test_declares_channel_reads_the_schema(self):
        assert declares_channel(WITH_CHANNEL) is True
        assert declares_channel(WITHOUT_CHANNEL) is False
        assert declares_channel({"model": "not a dict"}) is False


class TestOnTheRunConfig:
    @staticmethod
    def _flow(config):
        flow = BackendFlow(job_id="job-1")
        flow.config = config
        flow._flow_data = None
        return flow

    def test_a_resume_carries_the_decision_into_state(self):
        flow = self._flow(
            {
                "flow_config": {"state": WITH_CHANNEL},
                APPROVAL_CONFIG_KEY: DECISION,
                "resume_from_flow_uuid": "lineage-1",
            }
        )

        inputs = flow._kickoff_inputs()

        assert inputs[APPROVAL_CHANNEL] == DECISION
        # The resume still addresses the lineage it was told to.
        assert inputs["id"] == "lineage-1"

    def test_a_normal_run_carries_no_decision(self):
        flow = self._flow({"flow_config": {"state": WITH_CHANNEL}})

        assert APPROVAL_CHANNEL not in flow._kickoff_inputs()

    def test_the_flow_s_own_inputs_survive_alongside_it(self):
        flow = self._flow(
            {
                "flow_config": {"state": WITH_CHANNEL},
                APPROVAL_CONFIG_KEY: DECISION,
                "inputs": {"topic": "news"},
            }
        )

        inputs = flow._kickoff_inputs()

        assert inputs["topic"] == "news"
        assert inputs[APPROVAL_CHANNEL] == DECISION
