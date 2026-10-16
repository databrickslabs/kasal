/**
 * A run's answer must reach the conversation even when the live stream does not.
 *
 * These run the real execution and session stores (only the network is faked),
 * so "posted once" is counted on the conversation itself, not on a spy.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';

type StreamCb = (event: { event: string; data: Record<string, unknown> }) => void;
const net = vi.hoisted(() => ({
  streams: [] as { jobId: string; onEvent: StreamCb; onLost?: () => void }[],
  getExecution: vi.fn(),
  getExecutionStatus: vi.fn(),
  getSessionRunningJob: vi.fn(async (_sid: string): Promise<string | null> => null),
  clearSessionRunningJob: vi.fn(async (_sid: string) => {}),
}));

vi.mock('../api/streaming', () => ({
  streamExecution: (jobId: string, onEvent: StreamCb, onLost?: () => void) => {
    net.streams.push({ jobId, onEvent, onLost });
    return () => {};
  },
  streamGeneration: () => () => {},
}));
vi.mock('../api/executions', () => ({
  getExecution: (id: string) => net.getExecution(id),
  getExecutionStatus: (id: string) => net.getExecutionStatus(id),
}));
vi.mock('../persistence/sessionApi', () => ({
  initDb: vi.fn(async () => {}),
  assignUngroupedSessions: vi.fn(async () => {}),
  createSession: vi.fn(),
  listSessions: vi.fn(async () => []),
  deleteSession: vi.fn(async () => {}),
  renameSession: vi.fn(async () => {}),
  getSessionMessages: vi.fn(async () => []),
  addMessageToSession: vi.fn(async () => {}),
  updateMessageInSession: vi.fn(async () => {}),
  clearSessionMessages: vi.fn(async () => {}),
  saveSessionPreview: vi.fn(async () => {}),
  getSessionPreview: vi.fn(async () => undefined),
  deleteSessionPreview: vi.fn(async () => {}),
  setSessionRunningJob: vi.fn(async () => {}),
  getSessionRunningJob: (sid: string) => net.getSessionRunningJob(sid),
  clearSessionRunningJob: (sid: string) => net.clearSessionRunningJob(sid),
}));
vi.mock('../utils/sessionPreview', () => ({
  deriveSessionPreviews: vi.fn(async () => ({ history: [], current: null })),
}));

import { useChatRunStream } from './useChatRunStream';
import { useExecutionStore } from '../store/executionStore';
import { useSessionStore } from '../../../app/sessions/sessionStore';
import type { ChatMessage } from '../types/chat';

const settle = async () => {
  for (let i = 0; i < 10; i += 1) await act(async () => { await Promise.resolve(); });
};
const answersFor = (jobId: string) =>
  useSessionStore.getState().messages.filter(
    (m) => m.executionId === jobId && m.resultType !== 'crew_actions',
  );
const failures = () =>
  useSessionStore.getState().messages.filter((m) => m.content.startsWith('Execution failed'));

function mountOn(sessionId: string, messages: ChatMessage[] = []) {
  useSessionStore.setState({ currentSessionId: sessionId, messages });
  return renderHook(() => useChatRunStream({ pendingActionsRef: { current: null } }));
}

beforeEach(() => {
  net.streams.length = 0;
  vi.clearAllMocks();
  net.getSessionRunningJob.mockResolvedValue(null);
  useExecutionStore.setState({
    activeExecution: null, isExecuting: false, isLoading: false,
    executionOwnerSessionId: null, runningJobBySession: {},
  });
});
afterEach(() => vi.useRealTimers());

describe('a lost stream is not a failed run', () => {
  it('finishes from the stored row when the stream gives up on a run that completed', async () => {
    const { result } = mountOn('s-lost');
    act(() => result.current.handleStartExecutionStream('job-lost-done', 's-lost'));
    net.getExecution.mockResolvedValue({ status: 'completed', result: 'The real answer' });

    act(() => net.streams[0].onLost?.());
    await settle();

    expect(net.getExecution).toHaveBeenCalledWith('job-lost-done');
    expect(answersFor('job-lost-done').map((m) => m.content)).toEqual(['The real answer']);
    expect(failures()).toHaveLength(0);
    expect(useExecutionStore.getState().isExecuting).toBe(false);
  });

  it('keeps a still-running run alive and posts its later completion', async () => {
    const { result } = mountOn('s-lost-run');
    act(() => result.current.handleStartExecutionStream('job-lost-run', 's-lost-run'));
    net.getExecution.mockResolvedValue({ status: 'running' });

    act(() => net.streams[0].onLost?.());
    await settle();
    expect(failures()).toHaveLength(0);
    expect(useExecutionStore.getState().jobOwnerOf('job-lost-run')).toBe('s-lost-run');
    expect(useExecutionStore.getState().isExecuting).toBe(true);

    // The REST poller reports the real ending later.
    act(() => {
      window.dispatchEvent(new CustomEvent('jobCompleted', {
        detail: { jobId: 'job-lost-run', result: 'Arrived by polling' },
      }));
    });
    expect(answersFor('job-lost-run').map((m) => m.content)).toEqual(['Arrived by polling']);
    expect(failures()).toHaveLength(0);
  });

  it('reports a real failure the server recorded', async () => {
    const { result } = mountOn('s-lost-fail');
    act(() => result.current.handleStartExecutionStream('job-lost-fail', 's-lost-fail'));
    net.getExecution.mockResolvedValue({ status: 'failed', error: 'Tool crashed' });

    act(() => net.streams[0].onLost?.());
    await settle();

    expect(failures().map((m) => m.content)).toEqual(['Execution failed: Tool crashed']);
    // Stamped with its run, so a later reload knows it was reported.
    expect(failures()[0].executionId).toBe('job-lost-fail');
  });

  it('treats a run rejected at an approval gate as ended', async () => {
    const { result } = mountOn('s-lost-rejected');
    act(() => result.current.handleStartExecutionStream('job-lost-rejected', 's-lost-rejected'));
    net.getExecution.mockResolvedValue({ status: 'REJECTED' });

    act(() => net.streams[0].onLost?.());
    await settle();

    expect(failures().map((m) => m.content)).toEqual(['Execution failed: Rejected at an approval gate']);
    expect(useExecutionStore.getState().isExecuting).toBe(false);
  });
});

describe('returning to a run that finished while nobody watched', () => {
  const question: ChatMessage = { id: 'q1', role: 'user', content: 'Research X', timestamp: new Date() };

  it('posts the stored answer exactly once, even if a replay or the poller delivers it again', async () => {
    net.getSessionRunningJob.mockResolvedValue('job-away');
    net.getExecutionStatus.mockResolvedValue({ status: 'completed' });
    net.getExecution.mockResolvedValue({ status: 'completed', result: 'Stored answer' });

    mountOn('s-away', [question]);
    await settle();

    expect(net.getExecution).toHaveBeenCalledWith('job-away');
    expect(answersFor('job-away').map((m) => m.content)).toEqual(['Stored answer']);
    expect(net.clearSessionRunningJob).toHaveBeenCalledWith('s-away');

    // The SSE replay and the REST poller can both still deliver the same ending.
    act(() => {
      net.streams[0].onEvent({ event: 'execution_update', data: { status: 'completed', result: 'Stored answer' } });
      window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'job-away', result: 'Stored answer' } }));
    });
    expect(answersFor('job-away')).toHaveLength(1);
  });

  it('does not post again when the answer is already in the conversation', async () => {
    net.getSessionRunningJob.mockResolvedValue('job-seen');
    net.getExecutionStatus.mockResolvedValue({ status: 'completed' });
    const answer: ChatMessage = {
      id: 'a1', role: 'assistant', content: 'Earlier answer', timestamp: new Date(), executionId: 'job-seen',
    };

    mountOn('s-seen', [question, answer]);
    await settle();

    expect(net.getExecution).not.toHaveBeenCalled();
    expect(answersFor('job-seen').map((m) => m.content)).toEqual(['Earlier answer']);
    expect(net.clearSessionRunningJob).toHaveBeenCalledWith('s-seen');
    expect(useExecutionStore.getState().isExecuting).toBe(false);
  });

  it('does not post a reported failure a second time', async () => {
    net.getSessionRunningJob.mockResolvedValue('job-failed-seen');
    net.getExecutionStatus.mockResolvedValue({ status: 'failed' });
    const reported: ChatMessage = {
      id: 'f1', role: 'assistant', content: 'Execution failed: boom', timestamp: new Date(), executionId: 'job-failed-seen',
    };

    mountOn('s-failed-seen', [question, reported]);
    await settle();

    expect(net.getExecution).not.toHaveBeenCalled();
    expect(failures()).toHaveLength(1);
  });
});
