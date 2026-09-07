import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { useScopedRuns } from './useScopedRuns';
import { useGroupStore } from '../../../store/groups';
import { useRunStatusStore } from '../../../store/runStatus';
import { runService, type Run } from '../../../api/execution/ExecutionHistoryService';

vi.mock('../../../api/execution/ExecutionHistoryService', () => ({ runService: { getRunByJobId: vi.fn() } }));
const run = (job_id: string, group_id = 'a', status = 'COMPLETED') => ({ id: job_id, job_id, group_id, status, created_at: '2026-09-01T10:00:00Z' } as Run);
beforeEach(() => {
  vi.clearAllMocks();
  useGroupStore.setState({ currentGroupId: 'a' });
  useRunStatusStore.setState({ runHistory: [] });
  vi.mocked(runService.getRunByJobId).mockImplementation(async id => run(id));
});
it('loads old linked executions outside the global page and excludes other sessions and groups', async () => {
  useRunStatusStore.setState({ runHistory: [run('other-session'), run('foreign', 'b')] });
  vi.mocked(runService.getRunByJobId).mockImplementation(async id => run(id, id === 'foreign' ? 'b' : 'a'));
  const { result } = renderHook(() => useScopedRuns(['old', 'foreign']));
  await waitFor(() => expect(result.current.runs.map(item => item.job_id)).toEqual(['old']));
  expect(runService.getRunByJobId).toHaveBeenCalledWith('old');
});
it('never substitutes global history for an empty session', async () => {
  useRunStatusStore.setState({ runHistory: [run('unrelated')] });
  const { result } = renderHook(() => useScopedRuns([]));
  expect(result.current.runs).toEqual([]);
  expect(runService.getRunByJobId).not.toHaveBeenCalled();
});
it('discards slow requests after a session or workspace change', async () => {
  let resolve!: (value: Run) => void;
  vi.mocked(runService.getRunByJobId).mockImplementation(id => id === 'slow' ? new Promise(done => { resolve = done; }) : Promise.resolve(run(id)));
  const { result, rerender } = renderHook(({ ids }) => useScopedRuns(ids), { initialProps: { ids: ['slow'] } });
  rerender({ ids: ['current'] });
  await waitFor(() => expect(result.current.runs[0]?.job_id).toBe('current'));
  await act(async () => resolve(run('slow')));
  expect(result.current.runs.map(item => item.job_id)).toEqual(['current']);
  act(() => useGroupStore.setState({ currentGroupId: 'b' }));
  expect(result.current.runs).toEqual([]);
});
it('uses live status updates without dropping historical executions', async () => {
  const { result } = renderHook(() => useScopedRuns(['old', 'live']));
  await waitFor(() => expect(result.current.runs).toHaveLength(2));
  act(() => useRunStatusStore.setState({ runHistory: [{ ...run('live', 'a', 'FAILED'), updated_at: '2026-09-01T11:00:00Z' }] }));
  expect(result.current.runs.find(item => item.job_id === 'live')?.status).toBe('FAILED');
  expect(result.current.runs).toHaveLength(2);
});
it('keeps loaded results visible and reports partial failures', async () => {
  vi.mocked(runService.getRunByJobId).mockImplementation(async id => id === 'gone' ? null : run(id));
  const { result } = renderHook(() => useScopedRuns(['old', 'gone']));
  await waitFor(() => expect(result.current.error).toContain('Some executions'));
  expect(result.current.runs.map(item => item.job_id)).toEqual(['old']);
});
