import React, { useState } from 'react';
import { HITLService } from '../../../../api/HITLService';
import { useSessionStore } from '../../store/sessionStore';

/**
 * Inline tool-approval card for ChatMode — the chat shell never shows modal
 * dialogs, so an agent pausing on an approval-flagged tool renders this card
 * in the conversation instead. Approve lets the paused tool run; Deny tells
 * the agent "no" (the run continues without the tool). The decision is
 * persisted onto the message so history shows what was decided.
 */

export interface ToolApprovalData {
  approval_id: string | number;
  job_id?: string;
  tool_name?: string;
  agent_role?: string;
  tool_args?: Record<string, string>;
  message?: string;
  decided?: 'approved' | 'denied';
}

interface ToolApprovalCardProps {
  data: ToolApprovalData;
  messageId: string;
}

const ToolApprovalCard: React.FC<ToolApprovalCardProps> = ({ data, messageId }) => {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const updateMessage = useSessionStore((s) => s.updateMessage);
  const decided = data.decided;

  const persistDecision = (decision: 'approved' | 'denied') => {
    updateMessage(messageId, {
      resultData: { ...data, decided: decision },
    });
  };

  const decide = async (decision: 'approved' | 'denied') => {
    setBusy(true);
    setError(null);
    try {
      const approvalId = Number(data.approval_id);
      if (decision === 'approved') {
        await HITLService.approveGate(approvalId, {});
      } else {
        await HITLService.rejectGate(approvalId, {
          reason: 'Denied from chat',
        });
      }
      persistDecision(decision);
    } catch (e: unknown) {
      setError((e as Error)?.message ?? 'Could not submit the decision');
    } finally {
      setBusy(false);
    }
  };

  // Render as ONE activity-style text line (like the run-activity rows) —
  // no card chrome, no colored buttons; args truncate so nothing wraps.
  const args = data.tool_args ?? {};
  const argsJson = Object.keys(args).length > 0 ? JSON.stringify(args) : '';

  const linkClass =
    'underline underline-offset-2 disabled:opacity-50 hover:opacity-80 font-medium shrink-0';

  return (
    <div
      className="my-1.5 px-1 flex items-center gap-1.5 text-[13px] leading-[1.7] whitespace-nowrap overflow-hidden"
      style={{ color: 'var(--text-muted)' }}
    >
      <span className="shrink-0">✋ {data.agent_role || 'The agent'} wants to run</span>
      <span className="font-medium shrink-0" style={{ color: 'var(--text-primary)' }}>
        {data.tool_name || 'a tool'}
      </span>
      {argsJson && (
        <span
          className="truncate min-w-0 font-mono text-[12px]"
          style={{ color: 'var(--text-muted)', background: 'transparent' }}
        >
          {argsJson}
        </span>
      )}
      {decided ? (
        <span className="shrink-0">
          {decided === 'approved' ? '— approved' : '— denied'}
        </span>
      ) : (
        <>
          <span className="shrink-0">—</span>
          <button
            type="button"
            disabled={busy}
            onClick={() => void decide('approved')}
            className={linkClass}
            style={{ color: 'var(--text-primary)' }}
          >
            Approve
          </button>
          <span className="shrink-0">·</span>
          <button
            type="button"
            disabled={busy}
            onClick={() => void decide('denied')}
            className={linkClass}
            style={{ color: 'var(--text-primary)' }}
          >
            Deny
          </button>
          {busy && <span className="shrink-0">…</span>}
        </>
      )}
      {error && <span className="truncate min-w-0"> — {error}</span>}
    </div>
  );
};

export default ToolApprovalCard;
