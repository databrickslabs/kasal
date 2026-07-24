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

  const args = data.tool_args ?? {};
  const hasArgs = Object.keys(args).length > 0;

  return (
    <div
      className="my-2 rounded-lg border px-4 py-3 text-[14px] leading-[1.6]"
      style={{
        borderColor: 'var(--border, rgba(128,128,128,0.35))',
        color: 'var(--text-primary)',
        background: 'var(--surface-raised, rgba(128,128,128,0.06))',
      }}
    >
      <div className="font-semibold mb-1">
        ✋ Approval needed{data.agent_role ? ` — ${data.agent_role}` : ''}
      </div>
      <div className="mb-2">
        The agent wants to run <span className="font-semibold">{data.tool_name || 'a tool'}</span>.
        {decided ? '' : ' The run is paused until you decide.'}
      </div>
      {hasArgs && (
        <pre
          className="mb-2 max-h-40 overflow-auto rounded p-2 text-[12px]"
          style={{ background: 'var(--surface, rgba(128,128,128,0.1))' }}
        >
          {JSON.stringify(args, null, 2)}
        </pre>
      )}
      {decided ? (
        <div className="font-medium" style={{ color: decided === 'approved' ? 'var(--success, #22c55e)' : 'var(--danger, #ef4444)' }}>
          {decided === 'approved' ? '✓ Approved — the tool ran.' : '✗ Denied — the agent continued without it.'}
        </div>
      ) : (
        <div className="flex items-center gap-2">
          <button
            type="button"
            disabled={busy}
            onClick={() => void decide('approved')}
            className="rounded-md px-3 py-1.5 text-[13px] font-medium text-white disabled:opacity-50"
            style={{ background: 'var(--accent)' }}
          >
            Approve
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => void decide('denied')}
            className="rounded-md border px-3 py-1.5 text-[13px] font-medium disabled:opacity-50"
            style={{ borderColor: 'var(--border, rgba(128,128,128,0.35))', color: 'var(--text-primary)' }}
          >
            Deny
          </button>
          {busy && <span className="text-[12px] opacity-70">Submitting…</span>}
        </div>
      )}
      {error && (
        <div className="mt-1 text-[12px]" style={{ color: 'var(--danger, #ef4444)' }}>
          {error}
        </div>
      )}
    </div>
  );
};

export default ToolApprovalCard;
