/**
 * SSE Connection Manager Component
 *
 * Manages a single global Server-Sent Events connection that receives
 * updates for all jobs. Feeds updates to the runStatus store.
 */

import { useEffect, memo, useCallback } from 'react';
import { toast } from 'react-hot-toast';
import { useRunStatusStore } from '../../store/runStatus';
import { useGlobalExecutionSSE } from '../../hooks/global/useSSE';

/**
 * Generate user-friendly error message based on error type
 */
const getErrorMessage = (error: any, jobId: string): string => {
  // Check for fatal errors (max reconnection attempts)
  if (error.isFatal) {
    return `Lost connection to job ${jobId}. Please refresh the page to reconnect.`;
  }

  // Check for parsing errors
  if (error.message === 'Failed to parse SSE message') {
    return `Received invalid data for job ${jobId}. Connection is being reestablished.`;
  }

  // Network-related errors
  if (error.type === 'error') {
    const attempt = error.reconnectAttempt || 1;
    const maxAttempts = error.maxAttempts || 5;
    return `Connection issue with job ${jobId}. Reconnecting (attempt ${attempt}/${maxAttempts})...`;
  }

  // Generic fallback
  return `Connection issue with job ${jobId}. Attempting to reconnect...`;
};

/**
 * Global SSE Connection Component
 *
 * Establishes connection to the global SSE stream (/executions/stream-all)
 * This ensures that ALL browsers receive updates for ALL jobs, solving the
 * synchronization problem where one browser doesn't see updates when a job
 * completes in another browser.
 */
const GlobalSSEConnection: React.FC = () => {
  // CRITICAL: Use selectors to prevent re-renders on every store update
  const handleSSEUpdate = useRunStatusStore(state => state.handleSSEUpdate);
  const setSSEConnected = useRunStatusStore(state => state.setSSEConnected);
  const setSSEError = useRunStatusStore(state => state.setSSEError);
  const addTrace = useRunStatusStore(state => state.addTrace);

  // CRITICAL: Wrap callbacks in useCallback to provide stable references
  const onMessage = useCallback((eventData: any) => {
    // Check group_id for security filtering
    const selectedGroupId = localStorage.getItem('selectedGroupId');

    // Handle execution updates from the global stream
    if (eventData.event === 'execution_update' && eventData.data) {
      // Only process updates for jobs in the current user's selected workspace
      if (eventData.data.group_id && eventData.data.group_id !== selectedGroupId) {
        console.log(
          `[GlobalSSE] Ignoring update for different group: ${eventData.data.group_id} (selected: ${selectedGroupId})`
        );
        return;
      }

      // Feed execution status updates to the store
      handleSSEUpdate(eventData.data);
    } else if (eventData.event === 'trace' && eventData.data) {
      // CRITICAL: Add trace events to the Zustand store for ShowTraceTimeline
      // This ensures traces are visible even for completed jobs that aren't in activeRuns
      const jobId = eventData.data.job_id;
      if (jobId) {
        // Always dispatch traceUpdate window event — consumers (useExecutionMonitoring,
        // flowExecutionStore) have their own job-specific guards for filtering.
        // This must happen before the group_id check so visual indicators work.
        window.dispatchEvent(new CustomEvent('traceUpdate', {
          detail: { jobId, trace: eventData.data }
        }));

        // Security filter by group_id for the store (ShowTraceTimeline data)
        if (eventData.data.group_id && eventData.data.group_id !== selectedGroupId) {
          return;
        }

        addTrace(jobId, eventData.data);
      }
    } else if (eventData.event === 'hitl_request' && eventData.data) {
      // Dispatch HITL request event
      const jobId = eventData.data.job_id;
      if (jobId) {
        window.dispatchEvent(new CustomEvent('hitlRequest', {
          detail: eventData.data
        }));
      }
    }
  }, [handleSSEUpdate, addTrace]);

  const onConnect = useCallback(() => {
    console.log('[GlobalSSE] Connected to global execution stream');
    setSSEConnected(true);
    setSSEError(null);
    toast.dismiss('sse-reconnect-global');
  }, [setSSEConnected, setSSEError]);

  const onDisconnect = useCallback(() => {
    console.log('[GlobalSSE] Disconnected from global execution stream');
  }, []);

  const onError = useCallback((error: any) => {
    if (error.isFatal) {
      console.error('[GlobalSSE] Fatal error:', error);
    } else {
      console.debug('[GlobalSSE] Connection interrupted, will reconnect');
    }

    const errorMessage = getErrorMessage(error, 'global stream');
    setSSEError(errorMessage);

    // Only show toast for fatal errors
    if (error.isFatal) {
      toast.error(errorMessage, {
        duration: 8000,
        position: 'bottom-right',
      });
    }
  }, [setSSEError]);

  const { connectionState } = useGlobalExecutionSSE(
    onMessage,
    {
      autoReconnect: true,
      maxReconnectAttempts: 10,
      reconnectDelay: 3000,
      onConnect,
      onDisconnect,
      onError,
    }
  );

  useEffect(() => {
    console.log(`[GlobalSSE] Connection state: ${connectionState}`);
  }, [connectionState]);

  // This component doesn't render anything
  return null;
};

/**
 * Main SSE Connection Manager
 *
 * Maintains a single global SSE connection that receives updates for all jobs.
 *
 * CRITICAL: Wrapped in React.memo to prevent re-renders when parent App component
 * re-renders due to user/group state changes. This keeps SSE connections stable.
 */
const SSEConnectionManagerComponent: React.FC = () => {
  const sseEnabled = useRunStatusStore(state => state.sseEnabled);

  // Don't establish connections if SSE is disabled
  if (!sseEnabled) {
    return null;
  }

  return (
    <>
      {/* Global SSE connection for all job updates */}
      <GlobalSSEConnection />
    </>
  );
};

// Memoize to prevent re-renders when App component re-renders
export const SSEConnectionManager = memo(SSEConnectionManagerComponent);

export default SSEConnectionManager;
