import React, { useState } from 'react';
import { HITLService } from '../../../../api/execution/HITLService';
import { useSessionStore } from '../../store/sessionStore';

/**
 * Inline tool-approval card for ChatMode — the chat shell never shows modal
 * dialogs, so an agent pausing on an approval-flagged tool renders this card
 * in the conversation instead. Approve lets the paused tool run; Deny tells
 * the agent "no" (the run continues without the tool). Denying / requesting
 * changes expands the same line with an inline feedback input — for
 * task_review gates the typed reason becomes the retry prompt the agent
 * re-runs the task with. The decision is persisted onto the message so
 * history shows what was decided.
 */

export interface ToolApprovalData {
  approval_id: string | number;
  job_id?: string;
  kind?: string; // "tool_call" (default) | "task_review"
  tool_name?: string;
  task_name?: string;
  agent_role?: string;
  tool_args?: Record<string, string>;
  output_preview?: string;
  message?: string;
  decided?: 'approved' | 'denied';
  /** Feedback the reviewer typed when denying / requesting changes. */
  decided_reason?: string;
}

interface ToolApprovalCardProps {
  data: ToolApprovalData;
  messageId: string;
}

/** Short echo of the typed feedback for the decided line (~60 chars). */
const echoOf = (reason: string): string =>
  reason.length > 60 ? `${reason.slice(0, 60).trimEnd()}…` : reason;

const ToolApprovalCard: React.FC<ToolApprovalCardProps> = ({ data, messageId }) => {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Denying opens an inline feedback row (task_review feedback becomes the
  // retry prompt on the backend; tool_call reason is optional context).
  const [feedbackOpen, setFeedbackOpen] = useState(false);
  const [feedback, setFeedback] = useState('');
  const updateMessage = useSessionStore((s) => s.updateMessage);
  const decided = data.decided;
  const isTaskReview = data.kind === 'task_review';

  const persistDecision = (decision: 'approved' | 'denied', reason?: string) => {
    updateMessage(messageId, {
      resultData: { ...data, decided: decision, ...(reason ? { decided_reason: reason } : {}) },
    });
  };

  const approve = async () => {
    setBusy(true);
    setError(null);
    try {
      await HITLService.approveGate(Number(data.approval_id), {});
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
    setBusy(true);
    setError(null);
    try {
      await HITLService.rejectGate(Number(data.approval_id), {
        // The reason literally becomes the retry prompt for task_review gates,
        // so send the typed feedback; fall back to a generic reason if empty.
        reason:
          typed || (isTaskReview ? 'Changes requested from chat' : 'Denied from chat'),
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
  const detail = isTaskReview ? (data.output_preview ?? '') : argsJson;

  const linkClass =
    'underline underline-offset-2 disabled:opacity-50 hover:opacity-80 font-medium shrink-0';

  return (
    <div className="my-1.5 px-1 text-[13px] leading-[1.7]" style={{ color: 'var(--text-muted)' }}>
      <div className="flex items-center gap-1.5 whitespace-nowrap overflow-hidden">
        <span className="shrink-0">
          ✋ {isTaskReview
            ? 'Review the output of'
            : `${data.agent_role || 'The agent'} wants to run`}
        </span>
        <span className="font-medium shrink-0" style={{ color: 'var(--text-primary)' }}>
          {(isTaskReview ? data.task_name : data.tool_name) || (isTaskReview ? 'the task' : 'a tool')}
        </span>
        {detail && (
          <span
            className="truncate min-w-0 font-mono text-[12px]"
            style={{ color: 'var(--text-muted)', background: 'transparent' }}
          >
            {detail}
          </span>
        )}
        {decided ? (
          <span className="truncate min-w-0">
            {decided === 'approved'
              ? '— approved'
              : isTaskReview
                ? data.decided_reason
                  ? `— changes requested: ${echoOf(data.decided_reason)}`
                  : '— changes requested, the task retries'
                : '— denied'}
          </span>
        ) : feedbackOpen ? null : (
          <>
            <span className="shrink-0">—</span>
            <button
              type="button"
              disabled={busy}
              onClick={() => void approve()}
              className={linkClass}
              style={{ color: 'var(--text-primary)' }}
            >
              Approve
            </button>
            <span className="shrink-0">·</span>
            <button
              type="button"
              disabled={busy}
              onClick={() => setFeedbackOpen(true)}
              className={linkClass}
              style={{ color: 'var(--text-primary)' }}
            >
              {isTaskReview ? 'Request changes' : 'Deny'}
            </button>
            {busy && <span className="shrink-0">…</span>}
          </>
        )}
        {error && <span className="truncate min-w-0"> — {error}</span>}
      </div>
      {!decided && feedbackOpen && (
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
            style={{ color: 'var(--text-primary)' }}
          >
            Send
          </button>
          <span className="shrink-0">·</span>
          <button
            type="button"
            disabled={busy}
            onClick={cancelFeedback}
            className={linkClass}
            style={{ color: 'var(--text-muted)' }}
          >
            Cancel
          </button>
          {busy && <span className="shrink-0">…</span>}
        </div>
      )}
    </div>
  );
};

export default ToolApprovalCard;
