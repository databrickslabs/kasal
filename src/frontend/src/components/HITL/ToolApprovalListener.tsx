import React, { useCallback, useEffect, useState } from 'react';
import HITLApprovalDialog from './HITLApprovalDialog';
import { HITLService } from '../../api/execution/HITLService';
import { useExecutionStore } from '../ChatMode/store/executionStore';

/**
 * Global listener for tool-call approval gates.
 *
 * The backend broadcasts SSE `hitl_request` when an agent pauses on an
 * approval-flagged tool; SSEConnectionManager re-dispatches it as a window
 * `hitlRequest` CustomEvent (this was previously dispatched with no listener).
 * This component opens the existing HITLApprovalDialog for that execution —
 * the dialog self-fetches the pending approval and renders the tool-call
 * variant. Flow step-gates keep their existing entry point (the
 * WAITING_FOR_APPROVAL status badge) and also work through this listener.
 */
const ToolApprovalListener: React.FC = () => {
  const [executionId, setExecutionId] = useState<string | null>(null);

  useEffect(() => {
    const onHitlRequest = (e: Event) => {
      const detail = (e as CustomEvent).detail as { job_id?: string } | undefined;
      if (!detail?.job_id) return;
      // Chat never shows modal dialogs: skip when the job is chat-owned OR
      // the chat shell is on screen at all (covers replayed events for jobs
      // the store no longer tracks).
      if (useExecutionStore.getState().jobOwnerOf(detail.job_id)) return;
      if (document.getElementById('kasal-chat-root')) return;
      const jobId = detail.job_id;
      // Only open for a LIVE pending approval — an SSE reconnect replays old
      // hitl_request events, and popping an expired gate helps no one.
      void HITLService.getExecutionHITLStatus(jobId)
        .then((status) => {
          if (status.has_pending_approval && !status.pending_approval?.is_expired) {
            setExecutionId(jobId);
          }
        })
        .catch(() => {
          /* stale/foreign job — never open on failure */
        });
    };
    window.addEventListener('hitlRequest', onHitlRequest);
    return () => window.removeEventListener('hitlRequest', onHitlRequest);
  }, []);

  const handleClose = useCallback(() => setExecutionId(null), []);

  if (!executionId) return null;
  return (
    <HITLApprovalDialog
      open
      executionId={executionId}
      onClose={handleClose}
    />
  );
};

export default ToolApprovalListener;
