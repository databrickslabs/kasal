import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import { HITLService, type HITLApprovalResponse } from '../../../../api/execution/HITLService';
import ApprovalCard, { type ApprovalData } from '../../../chat/components/Cards/ApprovalCard';
import { BuilderPreviewContext } from './BuilderPreviewContext';

function approvalData(approval: HITLApprovalResponse): ApprovalData {
  const config = approval.gate_config || {};
  const text = (key: string) => typeof config[key] === 'string' ? config[key] as string : undefined;
  return {
    approval_id: approval.id, job_id: approval.execution_id,
    kind: text('kind') || 'flow_gate', step_name: text('step_name'),
    task_name: text('task_name') || approval.previous_crew_name || undefined,
    tool_name: text('tool_name'), agent_role: text('agent_role'), message: text('message'),
    tool_args: config.tool_args && typeof config.tool_args === 'object' ? config.tool_args as Record<string, string> : undefined,
    require_comment: config.require_comment === true,
    decided_action: approval.rejection_action || undefined,
    output_preview: approval.previous_crew_output?.slice(0, 400),
    decided: approval.status === 'approved' ? 'approved' : ['rejected', 'retry'].includes(approval.status) ? 'denied' : undefined,
    decided_reason: approval.rejection_reason || undefined,
    unavailable: approval.status === 'timeout' || (approval.is_expired && approval.status === 'pending') ? 'Approval expired' : undefined,
  };
}

/** Server-backed approval history also restores pending gates after a refresh. */
export default function BuilderRunApprovals({ jobId, running }: { jobId: string; running: boolean }) {
  const preview = useContext(BuilderPreviewContext);
  const [approvals, setApprovals] = useState<HITLApprovalResponse[]>([]);
  const [decisions, setDecisions] = useState<Record<number, ApprovalData>>({});
  const [error, setError] = useState('');
  const [reviewing, setReviewing] = useState<number | null>(null);
  const request = useRef(0);
  const invalidate = useCallback(() => { request.current++; }, []);
  const groupId = localStorage.getItem('selectedGroupId');
  const alive = useRef(false);
  const refresh = useCallback(async () => {
    const version = ++request.current;
    try {
      const status = await HITLService.getExecutionHITLStatus(jobId);
      if (!alive.current || version !== request.current || localStorage.getItem('selectedGroupId') !== groupId) return;
      const byId = new Map((status.approval_history || []).map(item => [item.id, item]));
      if (status.pending_approval) byId.set(status.pending_approval.id, status.pending_approval);
      setApprovals([...byId.values()]);
      setError('');
      return status.has_pending_approval;
    } catch (failure) {
      if (!alive.current || version !== request.current) return;
      // Builder generation traces have no execution/approval record.
      if ((failure as { response?: { status?: number } })?.response?.status !== 404) setError('Could not load approval requests.');
    }
  }, [jobId, groupId]);

  useEffect(() => {
    alive.current = true;
    setApprovals([]); setDecisions({}); setError('');
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      const pending = await refresh();
      if (!cancelled && (running || pending)) timer = setTimeout(() => void poll(), 5000);
    };
    void poll();
    const requested = (event: Event) => {
      if ((event as CustomEvent).detail?.job_id === jobId) void refresh();
    };
    window.addEventListener('hitlRequest', requested);
    return () => { cancelled = true; alive.current = false; invalidate(); clearTimeout(timer); window.removeEventListener('hitlRequest', requested); };
  }, [jobId, running, refresh, invalidate]);

  const review = async (approval: HITLApprovalResponse) => {
    if (preview?.openApproval && approval.status === 'pending' && !approval.is_expired) {
      preview.openApproval(jobId, approval.id, () => { void refresh(); window.dispatchEvent(new CustomEvent('refreshRunHistory')); });
      return;
    }
    setReviewing(approval.id);
    try {
      const full = await HITLService.getApproval(approval.id, 'ui');
      if (alive.current && localStorage.getItem('selectedGroupId') === groupId) preview?.openResult?.({
        type: 'text', title: `Review · ${approval.previous_crew_name || 'Run output'}`,
        data: full.previous_crew_output || 'No output was recorded for this approval.', sourceMessageId: jobId,
      });
    } catch { if (alive.current) setError('Could not load the output for review.'); }
    finally { if (alive.current) setReviewing(null); }
  };

  return <div aria-label="Run approvals">
    {approvals.map(approval => <div key={approval.id}>
      <ApprovalCard messageId={`builder-hitl-${approval.id}`} data={decisions[approval.id] || approvalData(approval)} onDecision={data => {
        if (!alive.current) return;
        invalidate();
        setDecisions(previous => ({ ...previous, [approval.id]: data }));
        void refresh();
        window.dispatchEvent(new CustomEvent('refreshRunHistory'));
      }} />
      {(approval.has_previous_crew_output || approval.previous_crew_output) && preview?.openResult && <button type="button"
        disabled={reviewing !== null} onClick={() => void review(approval)} className="text-xs underline underline-offset-2 px-1 mb-1"
        style={{ color: 'var(--text-secondary)', background: 'transparent', border: 0 }}>
        {reviewing === approval.id ? 'Loading output…' : 'Review output'}
      </button>}
    </div>)}
    {error && <div role="alert" className="text-xs px-1" style={{ color: 'var(--text-secondary)' }}>{error}{' '}
      <button type="button" onClick={() => void refresh()} style={{ color: 'inherit', background: 'transparent', border: 0, textDecoration: 'underline' }}>Retry</button>
    </div>}
  </div>;
}
