import React, { useCallback, useEffect, useState } from 'react';
import HITLApprovalDialog from './HITLApprovalDialog';
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
      // Chat-owned runs render an INLINE approval card in the conversation
      // instead — the chat shell never shows modal dialogs.
      if (useExecutionStore.getState().jobOwnerOf(detail.job_id)) return;
      setExecutionId(detail.job_id);
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
