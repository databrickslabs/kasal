import { beforeEach, describe, expect, it, vi } from 'vitest';
import { checkpointResumeHandler } from './checkpointResume';
import { useTabManagerStore, type TabData } from '../../../../store/tabManager';
import { useCrewExecutionStore } from '../../../../store/crewExecution';
import type { Run } from '../../../../types/execution/run';

const run = { job_id: 'original', run_name: 'My flow', execution_type: 'flow' } as Run;
beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'team');
  sessionStorage.clear();
  useTabManagerStore.setState({ activeTabId: 'tab', tabs: [{ id: 'tab', chatSessionId: 'session', executionJobIds: ['original'], createdAt: new Date(), lastModified: new Date() } as TabData] });
  useCrewExecutionStore.setState({ jobId: null });
});
describe('checkpoint resume session recovery', () => {
  it('adds the new run to this session and announces it for trace monitoring', () => {
    const created = vi.fn();
    window.addEventListener('jobCreated', created);
    try {
      checkpointResumeHandler(run)('resumed');
      expect(useTabManagerStore.getState().tabs[0].executionJobIds).toEqual(['original', 'resumed']);
      expect(sessionStorage.getItem('kasal-builder-run:team:session')).toBe('resumed');
      expect(useCrewExecutionStore.getState().jobId).toBe('resumed');
      expect(created.mock.calls[0][0].detail).toMatchObject({ jobId: 'resumed', isFlow: true, groupId: 'team', sessionId: 'session' });
    } finally { window.removeEventListener('jobCreated', created); }
  });
  it.each(['session', 'teamspace'])('retains the source association after switching %s without hijacking the new view', kind => {
    const resumed = checkpointResumeHandler(run);
    if (kind === 'session') useTabManagerStore.setState({ activeTabId: 'other' });
    else localStorage.setItem('selectedGroupId', 'other-team');
    const created = vi.fn();
    window.addEventListener('jobCreated', created);
    try {
      resumed('resumed');
      expect(created).not.toHaveBeenCalled();
      expect(useTabManagerStore.getState().tabs[0].executionJobIds).toContain('resumed');
      expect(sessionStorage.getItem('kasal-builder-run:team:session')).toBe('resumed');
    } finally { window.removeEventListener('jobCreated', created); }
  });
});
