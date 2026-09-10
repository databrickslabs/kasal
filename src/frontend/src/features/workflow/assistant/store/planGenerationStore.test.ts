import { beforeEach, describe, expect, it, vi } from 'vitest';
import { waitFor } from '@testing-library/react';
import DispatcherService, { type DispatchResult } from '../../../../api/execution/DispatcherService';
import { waitForBuilderGeneration } from '../../../../api/execution/builderGeneration';
import { useGroupStore } from '../../../../store/groups';
import { startPlanGeneration, resumePlanGeneration, consumePlanGeneration, planGenerationKey, usePlanGenerationStore } from './planGenerationStore';

vi.mock('../../../../api/execution/DispatcherService', () => ({ default: { dispatch: vi.fn() } }));
vi.mock('../../../../api/execution/builderGeneration', () => ({ waitForBuilderGeneration: vi.fn() }));
const result = (name: string): DispatchResult => ({ dispatcher: { intent: 'generate_crew', confidence: 1, extracted_info: {} }, generation_result: { name }, service_called: null });
const deferred = <T,>() => { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; };
const key = (session: string) => planGenerationKey('team-a', session);
const record = (session: string) => usePlanGenerationStore.getState().requests[key(session)];

beforeEach(() => {
  vi.clearAllMocks();
  useGroupStore.setState({ currentGroupId: 'team-a' });
  usePlanGenerationStore.setState({ requests: {}, activityBySession: {} });
});

describe('session-owned plan generation', () => {
  it('keeps concurrent sessions independent, including before a job ID arrives', async () => {
    const a = deferred<DispatchResult>();
    const b = deferred<DispatchResult>();
    vi.mocked(DispatcherService.dispatch).mockImplementation(request => request.message === 'A' ? a.promise : b.promise);
    startPlanGeneration('team-a', 'session-a', { message: 'A', tools: ['4'], mcp_servers: ['postgres'] }, async () => {}, vi.fn());
    startPlanGeneration('team-a', 'session-b', { message: 'B', mcp_servers: ['studio'] }, async () => {}, vi.fn());
    await waitFor(() => expect(DispatcherService.dispatch).toHaveBeenCalledTimes(2));
    expect(vi.mocked(DispatcherService.dispatch).mock.calls[0][0]).toMatchObject({ session_id: 'session-a', tools: ['4'], mcp_servers: ['postgres'] });
    expect(vi.mocked(DispatcherService.dispatch).mock.calls[1][0]).toMatchObject({ session_id: 'session-b', mcp_servers: ['studio'] });
    const signalA = vi.mocked(DispatcherService.dispatch).mock.calls[0][2];
    expect(signalA?.aborted).toBe(false);
    a.resolve(result('Plan A'));
    await waitFor(() => expect(record('session-a').status).toBe('complete'));
    expect(record('session-a').result).toEqual(result('Plan A'));
    expect(record('session-b').status).toBe('running');
    const token = record('session-a').token;
    consumePlanGeneration(key('session-a'), token);
    expect(record('session-a')).toBeUndefined();
    expect(record('session-b')).toBeDefined();
    b.resolve(result('Plan B'));
    await waitFor(() => expect(record('session-b').status).toBe('complete'));
  });

  it('does not submit or observe a duplicate generation in the same session', async () => {
    const pending = deferred<DispatchResult>();
    vi.mocked(DispatcherService.dispatch).mockImplementation((_request, started) => { started?.('job-a'); return pending.promise; });
    startPlanGeneration('team-a', 'session-a', { message: 'A' }, async () => {}, vi.fn());
    await waitFor(() => expect(record('session-a').jobId).toBe('job-a'));
    startPlanGeneration('team-a', 'session-a', { message: 'Duplicate' }, async () => {}, vi.fn());
    resumePlanGeneration(key('session-a'));
    expect(DispatcherService.dispatch).toHaveBeenCalledTimes(1);
    expect(waitForBuilderGeneration).not.toHaveBeenCalled();
    pending.resolve(result('A'));
    await waitFor(() => expect(record('session-a').status).toBe('complete'));
    consumePlanGeneration(key('session-a'), record('session-a').token);
    expect(usePlanGenerationStore.getState().activityBySession[key('session-a')]).toEqual([{ jobId: 'job-a', createdAt: expect.any(String) }]);
  });

  it('reconnects a restored job without creating another plan', async () => {
    usePlanGenerationStore.setState({ requests: { [key('restored')]: { token: 'restored-token', groupId: 'team-a', sessionId: 'restored', jobId: 'existing-job', status: 'running' } } });
    vi.mocked(waitForBuilderGeneration).mockResolvedValue(result('Restored'));
    resumePlanGeneration(key('restored'));
    await waitFor(() => expect(record('restored').result).toEqual(result('Restored')));
    expect(waitForBuilderGeneration).toHaveBeenCalledWith('existing-job', expect.any(AbortSignal));
    expect(DispatcherService.dispatch).not.toHaveBeenCalled();
  });

  it('pauses observation across teamspaces and reconnects using the original team', async () => {
    vi.mocked(DispatcherService.dispatch).mockImplementation((_request, started, signal) => {
      started?.('job-a');
      return new Promise((_resolve, reject) => signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true }));
    });
    startPlanGeneration('team-a', 'session-a', { message: 'A' }, async () => {}, vi.fn());
    await waitFor(() => expect(record('session-a').jobId).toBe('job-a'));
    useGroupStore.setState({ currentGroupId: 'team-b' });
    await waitFor(() => expect(record('session-a').status).toBe('paused'));
    resumePlanGeneration(key('session-a'));
    expect(waitForBuilderGeneration).not.toHaveBeenCalled();
    useGroupStore.setState({ currentGroupId: 'team-a' });
    vi.mocked(waitForBuilderGeneration).mockResolvedValue(result('Returned'));
    resumePlanGeneration(key('session-a'));
    await waitFor(() => expect(record('session-a').status).toBe('complete'));
  });
});
