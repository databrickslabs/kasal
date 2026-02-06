/**
 * SSE Connection Manager Component
 *
 * Manages Server-Sent Events connections for active jobs.
 * Automatically connects to SSE streams for running jobs and
 * feeds updates to the runStatus store.
 */

import { useEffect, memo, useCallback } from 'react';
import { toast } from 'react-hot-toast';
import { useRunStatusStore } from '../../store/runStatus';
import { useExecutionSSE, useGlobalExecutionSSE } from '../../hooks/global/useSSE';

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

interface SSEConnectionForJob {
  jobId: string;
}

/**
 * Component that establishes SSE connection for a single job
 */
const SSEConnectionForJob: React.FC<SSEConnectionForJob> = ({ jobId }) => {
  // CRITICAL: Use selectors to prevent re-renders on every store update
  const handleSSEUpdate = useRunStatusStore(state => state.handleSSEUpdate);
  const registerSSEConnection = useRunStatusStore(state => state.registerSSEConnection);
  const unregisterSSEConnection = useRunStatusStore(state => state.unregisterSSEConnection);
  const setSSEConnected = useRunStatusStore(state => state.setSSEConnected);
  const setSSEError = useRunStatusStore(state => state.setSSEError);
  const addTrace = useRunStatusStore(state => state.addTrace);

  // CRITICAL: Wrap callbacks in useCallback to provide stable references
  // This prevents the useSSE hook from recreating its connect function,
  // which would trigger cleanup and cause disconnect/reconnect cycles
  const onMessage = useCallback((eventData: any) => {
    // Handle different event types
    if (eventData.event === 'execution_update') {
      // Feed execution status updates to the store
      handleSSEUpdate(eventData.data);
    } else if (eventData.event === 'trace') {
      // Store trace in Zustand store for real-time updates
      if (eventData.data) {
        console.log('[SSE] Received trace event for job', jobId, '- adding to store');
        addTrace(jobId, eventData.data);
        console.log('[SSE] Trace added to store');
      }
      // Also dispatch window event for backwards compatibility
      window.dispatchEvent(new CustomEvent('traceUpdate', {
        detail: { jobId, trace: eventData.data }
      }));
    } else {
      // Handle other events (e.g., data without specific event type)
      handleSSEUpdate(eventData.data);
    }
  }, [jobId, handleSSEUpdate, addTrace]);

  const onConnect = useCallback(() => {
    console.log(`[SSE] Connected to job ${jobId}`);
    registerSSEConnection(jobId);
    setSSEConnected(true);
    // Clear any previous errors on successful connection
    setSSEError(null);
  }, [jobId, registerSSEConnection, setSSEConnected, setSSEError]);

  const onDisconnect = useCallback(() => {
    console.log(`[SSE] Disconnected from job ${jobId}`);
    unregisterSSEConnection(jobId);
    // Check if there are any other active SSE connections
    const { activeSSEConnections } = useRunStatusStore.getState();
    if (activeSSEConnections.size === 0) {
      setSSEConnected(false);
    }
  }, [jobId, unregisterSSEConnection, setSSEConnected]);

  const onError = useCallback((error: any) => {
    console.error(`[SSE] Error for job ${jobId}:`, error);

    // Generate user-friendly error message
    const errorMessage = getErrorMessage(error, jobId);

    // Update store with error
    setSSEError(errorMessage);

    // Show toast notification only for fatal errors
    if (error.isFatal) {
      toast.error(errorMessage, {
        duration: 8000,
        position: 'bottom-right',
      });
    } else if (error.reconnectAttempt === 1) {
      // Show info toast on first reconnection attempt
      toast.loading(errorMessage, {
        id: `sse-reconnect-${jobId}`, // Use ID to prevent duplicate toasts
        duration: 3000,
        position: 'bottom-right',
      });
    }
  }, [jobId, setSSEError]);

  const { connectionState } = useExecutionSSE(
    jobId,
    onMessage,
    {
      autoReconnect: true,
      maxReconnectAttempts: 5,
      reconnectDelay: 1000,
      onConnect,
      onDisconnect,
      onError,
    }
  );

  useEffect(() => {
    console.log(`[SSE] Connection state for job ${jobId}: ${connectionState}`);
  }, [connectionState, jobId]);

  // This component doesn't render anything
  return null;
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
        // Security filter by group_id if available
        if (eventData.data.group_id && eventData.data.group_id !== selectedGroupId) {
          console.log(
            `[GlobalSSE] Ignoring trace for different group: ${eventData.data.group_id} (selected: ${selectedGroupId})`
          );
          return;
        }

        console.log('[GlobalSSE] Received trace event for job', jobId, '- adding to store');
        addTrace(jobId, eventData.data);

        // Also dispatch window event for backwards compatibility with useExecutionMonitoring
        window.dispatchEvent(new CustomEvent('traceUpdate', {
          detail: { jobId, trace: eventData.data }
        }));
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
  }, [setSSEConnected, setSSEError]);

  const onDisconnect = useCallback(() => {
    console.log('[GlobalSSE] Disconnected from global execution stream');
  }, []);

  const onError = useCallback((error: any) => {
    console.error('[GlobalSSE] Error:', error);

    // Generate user-friendly error message
    const errorMessage = getErrorMessage(error, 'global stream');

    // Update store with error
    setSSEError(errorMessage);

    // Show toast notification only for fatal errors
    if (error.isFatal) {
      toast.error(errorMessage, {
        duration: 8000,
        position: 'bottom-right',
      });
    } else if (error.reconnectAttempt === 1) {
      // Show info toast on first reconnection attempt
      toast.loading(errorMessage, {
        id: 'sse-reconnect-global',
        duration: 3000,
        position: 'bottom-right',
      });
    }
  }, [setSSEError]);

  const { connectionState } = useGlobalExecutionSSE(
    onMessage,
    {
      autoReconnect: true,
      maxReconnectAttempts: 5,
      reconnectDelay: 2000, // Slightly longer delay for global stream
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
 * Monitors active jobs and establishes SSE connections for each.
 * Also maintains a global SSE connection for cross-browser synchronization.
 *
 * CRITICAL: Wrapped in React.memo to prevent re-renders when parent App component
 * re-renders due to user/group state changes. This keeps SSE connections stable.
 */
const SSEConnectionManagerComponent: React.FC = () => {
  // CRITICAL: Use selectors to prevent unnecessary re-renders
  // Subscribing to the entire activeRuns object causes re-renders on every status update
  const activeJobIds = useRunStatusStore(state => Object.keys(state.activeRuns));
  const sseEnabled = useRunStatusStore(state => state.sseEnabled);

  // Don't establish connections if SSE is disabled
  if (!sseEnabled) {
    return null;
  }

  return (
    <>
      {/* Global SSE connection for cross-browser synchronization */}
      <GlobalSSEConnection />

      {/* Per-job SSE connections for detailed updates */}
      {activeJobIds.map((jobId) => (
        <SSEConnectionForJob key={jobId} jobId={jobId} />
      ))}
    </>
  );
};

// Memoize to prevent re-renders when App component re-renders
export const SSEConnectionManager = memo(SSEConnectionManagerComponent);

export default SSEConnectionManager;
