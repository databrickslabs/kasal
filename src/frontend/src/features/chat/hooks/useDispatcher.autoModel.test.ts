import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useDispatcher } from './useDispatcher';
import { dispatch } from '../api/dispatcher';
import { useSessionStore } from '../../../app/sessions/sessionStore';
import type { DispatchResult } from '../types/dispatcher';

vi.mock('../api/dispatcher', () => ({ dispatch: vi.fn() }));

function options() {
  return {
    saveUserMessage: vi.fn(async () => 'user-msg-id'),
    addMessage: vi.fn(() => 'id'),
    addMessageToTargetSession: vi.fn(() => 'id'),
    updateMessage: vi.fn(),
    updateMessageInTargetSession: vi.fn(),
    onStartGenerationStream: vi.fn(),
    onStartExecutionStream: vi.fn(),
    onExecuteCrew: vi.fn(),
    onExecuteFlow: vi.fn(),
    onExecuteGenerated: vi.fn(),
    getCurrentSessionId: vi.fn(() => 'session-1'),
    ensureSession: vi.fn(async () => 'session-1'),
  };
}

function answer(extra: Partial<DispatchResult> = {}): DispatchResult {
  return {
    dispatcher: { intent: 'conversation', confidence: 1, extracted_info: {} },
    generation_result: { message: 'Done' },
    service_called: null,
    ...extra,
  } as DispatchResult;
}

describe('useDispatcher and Auto', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'log').mockImplementation(() => undefined);
    useSessionStore.setState({ addMessageToTargetSession: vi.fn(() => 'trace-id') } as never);
  });

  it('sends Auto as the model and shows what it picked', async () => {
    vi.mocked(dispatch).mockResolvedValue(
      answer({ model_selection: { requested: 'auto', model: 'databricks-claude-opus-5-5', status: 'selected' } }),
    );
    const { result } = renderHook(() => useDispatcher(options()));
    await act(async () => { await result.current.sendMessage('prove it', 'auto'); });
    expect(vi.mocked(dispatch).mock.calls[0][1]).toBe('auto');
    expect(useSessionStore.getState().addMessageToTargetSession).toHaveBeenCalledWith(
      'session-1', 'assistant', '',
      expect.objectContaining({
        resultData: expect.objectContaining({ label: 'Auto → databricks-claude-opus-5-5' }),
      }),
    );
  });

  it('shows nothing extra for an explicit model', async () => {
    vi.mocked(dispatch).mockResolvedValue(answer());
    const { result } = renderHook(() => useDispatcher(options()));
    await act(async () => { await result.current.sendMessage('hi', 'k1'); });
    expect(useSessionStore.getState().addMessageToTargetSession).not.toHaveBeenCalled();
  });
});
