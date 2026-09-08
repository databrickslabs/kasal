import React, { useState } from 'react';
import { HITLService, HITLRejectionAction } from '../../../../api/execution/HITLService';
import { useSessionStore } from '../../../../app/sessions/sessionStore';

/**
 * Inline APPROVAL card for ChatMode — one card for every kind of gate.
 *
 * Named for the decision, not for one of the things that can ask for it. It
 * was `ToolApprovalCard`, and it now also serves task reviews and FLOW gates —
 * a flow paused between two crews is not an agent reaching for a tool, and a
 * reader told "the agent wants to run a tool" for a flow gate is being
 * misinformed by the component's own history.
 *
 * The chat shell never shows modal
 * dialogs, so an agent pausing on an approval-flagged tool renders this card
 * in the conversation instead. Approve lets the paused tool run; Deny tells
 * the agent "no" (the run continues without the tool). Denying / requesting
 * changes expands the same line with an inline feedback input — for
 * task_review gates the typed reason becomes the retry prompt the agent
 * re-runs the task with. The decision is persisted onto the message so
 * history shows what was decided.
 */

export interface ApprovalData {
  approval_id: string | number;
  job_id?: string;
  kind?: string; // "tool_call" (default) | "task_review" | "flow_gate"
  /** flow_gate: the step the flow is waiting to continue INTO. */
  step_name?: string;
  tool_name?: string;
  task_name?: string;
  agent_role?: string;
  tool_args?: Record<string, string>;
  output_preview?: string;
  message?: string;
  decided?: 'approved' | 'denied';
  /** Feedback the reviewer typed when denying / requesting changes. */
  decided_reason?: string;
  unavailable?: string;
  require_comment?: boolean;
  decided_action?: 'reject' | 'retry';
}

interface ApprovalCardProps {
  data: ApprovalData;
  messageId: string;
  onDecision?: (data: ApprovalData) => void;
}

/** Short echo of the typed feedback for the decided line (~60 chars). */
const echoOf = (reason: string): string =>
  reason.length > 60 ? `${reason.slice(0, 60).trimEnd()}…` : reason;

const ApprovalCard: React.FC<ApprovalCardProps> = ({ data, messageId, onDecision }) => {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Denying opens an inline feedback row (task_review feedback becomes the
  // retry prompt on the backend; tool_call reason is optional context).
  const [feedbackOpen, setFeedbackOpen] = useState(false);
  const [feedback, setFeedback] = useState('');
  const [comment, setComment] = useState('');
  const [retry, setRetry] = useState(false);
  const updateMessage = useSessionStore((s) => s.updateMessage);
  const decided = data.decided;
  const isTaskReview = data.kind === 'task_review';
  // A flow paused at a gate between two steps. Same card, same decision,
  // same endpoint — only the wording differs, because "the agent wants to
  // run a tool" describes nothing that is happening here.
  const isFlowGate = data.kind === 'flow_gate';

  const persistDecision = (decision: 'approved' | 'denied', reason?: string) => {
    const resultData = { ...data, decided: decision, ...(reason ? { decided_reason: reason } : {}), ...(decision === 'denied' && isFlowGate ? { decided_action: retry ? 'retry' as const : 'reject' as const } : {}) };
    if (onDecision) { onDecision(resultData); return; }
    updateMessage(messageId, {
      resultData,
    });
  };

  const approve = async () => {
    if (data.require_comment && !comment.trim()) { setError('A comment is required to approve'); return; }
    setBusy(true);
    setError(null);
    try {
      await HITLService.approveGate(Number(data.approval_id), comment.trim() ? { comment: comment.trim() } : {});
      persistDecision('approved');
    } catch (e: unknown) {
      setError((e as Error)?.message ?? 'Could not submit the decision');
    } finally {
      setBusy(false);
    }
  };

  const cancelFeedback = () => {
    setFeedbackOpen(false);
    setFeedback('');
    setError(null);
  };

  const submitDenial = async () => {
    const typed = feedback.trim();
    if (retry && !typed) { setError('Describe what should change before retrying'); return; }
    setBusy(true);
    setError(null);
    try {
      await HITLService.rejectGate(Number(data.approval_id), {
        // The reason literally becomes the retry prompt for task_review gates,
        // so send the typed feedback; fall back to a generic reason if empty.
        reason:
          typed || (isTaskReview ? 'Changes requested from chat' : 'Denied from chat'),
        ...(isFlowGate ? { action: retry ? HITLRejectionAction.RETRY : HITLRejectionAction.REJECT } : {}),
      });
      persistDecision('denied', typed || undefined);
      setFeedbackOpen(false);
      setFeedback('');
    } catch (e: unknown) {
      setError((e as Error)?.message ?? 'Could not submit the decision');
    } finally {
      setBusy(false);
    }
  };

  // Render as ONE activity-style text line (like the run-activity rows) —
  // no card chrome, no colored buttons; the detail part truncates so nothing
  // wraps. Two variants: tool_call ("wants to run X {args}") and task_review
  // ("finished task X — review the output"). Denying grows a second chrome-free
  // row with an underlined text field (Enter submits, Escape cancels).
  const args = data.tool_args ?? {};
  const argsJson = Object.keys(args).length > 0 ? JSON.stringify(args) : '';
  const detail = isTaskReview ? (data.output_preview ?? '') : isFlowGate ? (data.message ?? '') : argsJson;

  const linkClass =
    'underline underline-offset-2 disabled:opacity-50 hover:opacity-80 font-medium shrink-0';
  const linkStyle = { color: 'var(--text-primary)', background: 'transparent', border: 0, padding: 0, font: 'inherit', cursor: 'pointer' };

  return (
    <div className="my-1.5 px-1 text-[13px] leading-[1.7]" style={{ color: 'var(--text-muted)' }}>
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="shrink-0">
          ✋ {isFlowGate
            ? 'The flow is waiting to continue to'
            : isTaskReview
              ? 'Review the output of'
              : `${data.agent_role || 'The agent'} wants to run`}
        </span>
        <span className="font-medium min-w-0 break-words" style={{ color: 'var(--text-primary)' }}>
          {isFlowGate
            ? data.step_name || 'the next step'
            : (isTaskReview ? data.task_name : data.tool_name)
              || (isTaskReview ? 'the task' : 'a tool')}
        </span>
        {detail && (
          <span
            className="truncate min-w-0 font-mono text-[12px]"
            style={{ color: 'var(--text-muted)', background: 'transparent' }}
          >
            {detail}
          </span>
        )}
        {data.unavailable ? <span>— {data.unavailable}</span> : decided ? (
          <span className="truncate min-w-0">
            {decided === 'approved'
              ? '— approved'
              : isTaskReview
                ? data.decided_reason
                  ? `— changes requested: ${echoOf(data.decided_reason)}`
                  : '— changes requested, the task retries'
                : data.decided_action === 'retry' ? '— changes requested, the previous crew retries' : '— denied'}
          </span>
        ) : feedbackOpen ? null : (
          <>
            <span className="shrink-0">—</span>
            <button
              type="button"
              disabled={busy}
              onClick={() => void approve()}
              className={linkClass}
              style={linkStyle}
            >
              Approve
            </button>
            <span className="shrink-0">·</span>
            <button
              type="button"
              disabled={busy}
              onClick={() => { setRetry(false); setFeedbackOpen(true); }}
              className={linkClass}
              style={linkStyle}
            >
              {isTaskReview ? 'Request changes' : 'Deny'}
            </button>
            {isFlowGate && <button type="button" disabled={busy} className={linkClass} style={linkStyle}
              onClick={() => { setRetry(true); setFeedbackOpen(true); }}>Request changes &amp; retry</button>}
            {busy && <span className="shrink-0">…</span>}
          </>
        )}
        {error && <span className="truncate min-w-0"> — {error}</span>}
      </div>
      {!decided && !data.unavailable && data.require_comment && !feedbackOpen && <input
        aria-label="Approval comment" placeholder="Comment required to approve" value={comment}
        onChange={event => setComment(event.target.value)} disabled={busy}
        className="w-full mt-1 text-[13px]" style={{ background: 'transparent', color: 'var(--text-primary)', border: 0, borderBottom: '1px solid var(--border-color)' }} />}
      {!decided && !data.unavailable && feedbackOpen && (
        <div className="flex items-center gap-1.5 whitespace-nowrap overflow-hidden mt-0.5 pl-6">
          <input
            autoFocus
            type="text"
            value={feedback}
            disabled={busy}
            aria-label={isTaskReview ? 'What should change?' : 'Reason (optional)'}
            placeholder={isTaskReview ? 'What should change?' : 'Reason (optional)'}
            onChange={(e) => setFeedback(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                void submitDenial();
              } else if (e.key === 'Escape') {
                e.preventDefault();
                cancelFeedback();
              }
            }}
            className="flex-1 min-w-0 bg-transparent outline-none text-[13px] leading-[1.7] placeholder:opacity-60 disabled:opacity-50"
            style={{
              color: 'var(--text-primary)',
              border: 'none',
              borderBottom: '1px solid var(--border-color)',
              borderRadius: 0,
              padding: 0,
            }}
          />
          <button
            type="button"
            disabled={busy}
            onClick={() => void submitDenial()}
            className={linkClass}
            style={linkStyle}
          >
            Send
          </button>
          <span className="shrink-0">·</span>
          <button
            type="button"
            disabled={busy}
            onClick={cancelFeedback}
            className={linkClass}
            style={{ ...linkStyle, color: 'var(--text-muted)' }}
          >
            Cancel
          </button>
          {busy && <span className="shrink-0">…</span>}
        </div>
      )}
    </div>
  );
};

export default ApprovalCard;
