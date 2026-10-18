/**
 * The dispatcher handles one message at a time, and says so.
 *
 * `dispatching` is the guard as state, so the composer can hold a message
 * typed while one is in flight. A send that still reaches the guard (Build
 * instead, a library load) is quoted back rather than dropped silently.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useDispatcher } from './useDispatcher';
import { dispatch } from '../api/dispatcher';
import { useSessionStore } from '../../../app/sessions/sessionStore';
import type { DispatchResult } from '../types/dispatcher';

vi.mock('../api/dispatcher', () => ({ dispatch: vi.fn() }));

const mockedDispatch = vi.mocked(dispatch);

const options = () => ({
  saveUserMessage: vi.fn(async () => 'user-msg-id'),
  addMessage: vi.fn(() => 'msg-id'),
  addMessageToTargetSession: vi.fn(() => 'msg-id'),
  updateMessage: vi.fn(),
  updateMessageInTargetSession: vi.fn(),
  onStartGenerationStream: vi.fn(),
  onStartExecutionStream: vi.fn(),
  getCurrentSessionId: vi.fn(() => 'session-1'),
  ensureSession: vi.fn(async () => 'session-1'),
});

const answer: DispatchResult = {
  dispatcher: { intent: 'conversation', confidence: 1, extracted_info: {} },
  generation_result: { message: 'Done' },
  service_called: null,
};

/** A dispatch that answers only when the test says so, and rejects when aborted, as axios does. */
function holdDispatch(): () => Promise<void> {
  let finish!: () => void;
  mockedDispatch.mockImplementationOnce(
    (_message, _model, _tools, _settings, _prompt, signal) => new Promise((resolve, reject) => {
      finish = () => resolve(answer);
      signal?.addEventListener('abort', () => reject(new Error('canceled')));
    }),
  );
  return async () => { finish(); };
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.spyOn(console, 'log').mockImplementation(() => undefined);
  useSessionStore.setState({ messages: [] });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('useDispatcher while a message is in flight', () => {
  it('reports the dispatch in flight, and clears it when the answer lands', async () => {
    const finish = holdDispatch();
    const { result } = renderHook(() => useDispatcher(options()));
    expect(result.current.dispatching).toBe(false);

    let first!: Promise<void>;
    await act(async () => { first = result.current.sendMessage('first question'); });
    expect(result.current.dispatching).toBe(true);

    await act(async () => { await finish(); await first; });
    expect(result.current.dispatching).toBe(false);
  });

  it('quotes back a message that reaches it mid-dispatch instead of dropping it', async () => {
    const finish = holdDispatch();
    const { result } = renderHook(() => useDispatcher(options()));

    let first!: Promise<void>;
    await act(async () => { first = result.current.sendMessage('first question'); });
    await act(async () => { await result.current.sendMessage('second question\nwith detail'); });

    expect(mockedDispatch).toHaveBeenCalledTimes(1);
    const notice = useSessionStore.getState().messages.at(-1)?.content ?? '';
    expect(notice).toContain('still being sent');
    expect(notice).toContain('> second question\n> with detail');

    await act(async () => { await finish(); await first; });
  });

  it('cancel aborts the request and frees the chat for the next message', async () => {
    holdDispatch();
    const opts = options();
    const { result } = renderHook(() => useDispatcher(opts));

    let first!: Promise<void>;
    await act(async () => { first = result.current.sendMessage('first question'); });
    const signal = mockedDispatch.mock.calls[0][5] as AbortSignal;
    expect(signal.aborted).toBe(false);

    await act(async () => { result.current.cancelDispatch(); });
    expect(signal.aborted).toBe(true);
    expect(result.current.dispatching).toBe(false);
    await act(async () => { await first; });
    expect(opts.updateMessageInTargetSession).toHaveBeenCalledWith('session-1', expect.any(String),
      expect.objectContaining({ content: expect.stringContaining('Stopped waiting') }));

    // The next message is dispatched, and the cancelled one cannot release its guard.
    const finishSecond = holdDispatch();
    let second!: Promise<void>;
    await act(async () => { second = result.current.sendMessage('second question'); });
    expect(mockedDispatch).toHaveBeenCalledTimes(2);
    expect(result.current.dispatching).toBe(true);
    await act(async () => { await finishSecond(); await second; });
    expect(result.current.dispatching).toBe(false);
  });

  it('a dispatch that fails frees the chat too', async () => {
    mockedDispatch.mockRejectedValueOnce(new Error('Bad gateway'));
    const { result } = renderHook(() => useDispatcher(options()));
    await act(async () => { await result.current.sendMessage('first question'); });
    expect(result.current.dispatching).toBe(false);
  });
});
