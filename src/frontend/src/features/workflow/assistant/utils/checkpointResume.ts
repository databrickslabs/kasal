import { useTabManagerStore } from '../../../../store/tabManager';
import { useCrewExecutionStore } from '../../../../store/crewExecution';
import type { Run } from '../../../../types/execution/run';

/** Capture the originating session before a resume request can outlive its pane. */
export function checkpointResumeHandler(run: Run): (jobId: string) => void {
  const tab = useTabManagerStore.getState().getActiveTab();
  const groupId = localStorage.getItem('selectedGroupId');
  return jobId => {
    if (tab?.chatSessionId) {
      useTabManagerStore.setState(state => ({ tabs: state.tabs.map(item => item.id === tab.id
        ? { ...item, executionJobIds: Array.from(new Set([...(item.executionJobIds || []), jobId])), executionStatus: 'running' as const }
        : item) }));
      try { sessionStorage.setItem(`kasal-builder-run:${groupId || 'default'}:${tab.chatSessionId}`, jobId); }
      catch { /* In-memory monitoring remains available when storage is blocked. */ }
    }
    // Never attach a late response to another session or teamspace.
    if (localStorage.getItem('selectedGroupId') === groupId && (!tab || useTabManagerStore.getState().activeTabId === tab.id)) {
      useCrewExecutionStore.getState().setJobId(jobId);
      window.dispatchEvent(new CustomEvent('jobCreated', { detail: {
        jobId, jobName: run.run_name, status: 'running', groupId,
        isFlow: run.execution_type === 'flow', sessionId: tab?.chatSessionId,
      } }));
    }
    window.dispatchEvent(new CustomEvent('refreshRunHistory'));
  };
}
