import { beforeEach, describe, expect, it, vi } from 'vitest';
import { checkpointResumeHandler } from './checkpointResume';
import { useBuilderCanvasStore, type BuilderCanvas } from '../../../../app/sessions/builderCanvasStore';
import { useCrewExecutionStore } from '../../../../store/crewExecution';
import type { Run } from '../../../../types/execution/run';

const run = { job_id: 'original', run_name: 'My flow', execution_type: 'flow' } as Run;
beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'team');
  sessionStorage.clear();
  useBuilderCanvasStore.setState({ activeCanvasId: 'tab', canvases: [{ id: 'tab', chatSessionId: 'session', executionJobIds: ['original'], createdAt: new Date(), lastModified: new Date() } as BuilderCanvas] });
  useCrewExecutionStore.setState({ jobId: null });
});
describe('checkpoint resume session recovery', () => {
  it('adds the new run to this session and announces it for trace monitoring', () => {
    const created = vi.fn();
    window.addEventListener('jobCreated', created);
    try {
      checkpointResumeHandler(run)('resumed');
      expect(useBuilderCanvasStore.getState().canvases[0].executionJobIds).toEqual(['original', 'resumed']);
      expect(sessionStorage.getItem('kasal-builder-run:team:session')).toBe('resumed');
      expect(useCrewExecutionStore.getState().jobId).toBe('resumed');
      expect(created.mock.calls[0][0].detail).toMatchObject({ jobId: 'resumed', isFlow: true, groupId: 'team', sessionId: 'session' });
    } finally { window.removeEventListener('jobCreated', created); }
  });
  it.each(['session', 'teamspace'])('retains the source association after switching %s without hijacking the new view', kind => {
    const resumed = checkpointResumeHandler(run);
    if (kind === 'session') useBuilderCanvasStore.setState({ activeCanvasId: 'other' });
    else localStorage.setItem('selectedGroupId', 'other-team');
    const created = vi.fn();
    window.addEventListener('jobCreated', created);
    try {
      resumed('resumed');
      expect(created).not.toHaveBeenCalled();
      expect(useBuilderCanvasStore.getState().canvases[0].executionJobIds).toContain('resumed');
      expect(sessionStorage.getItem('kasal-builder-run:team:session')).toBe('resumed');
    } finally { window.removeEventListener('jobCreated', created); }
  });
});
