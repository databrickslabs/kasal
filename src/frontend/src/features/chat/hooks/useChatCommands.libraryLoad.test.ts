/**
 * Opening a crew or flow from the library while a message is still being sent.
 *
 * The load is a dispatch of its own, in a fresh chat. With one in flight the
 * dispatcher refuses it, and the user was left in the new, empty chat. The
 * library now says so before switching anywhere.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useChatCommands } from './useChatCommands';
import { useSessionStore } from '../../../app/sessions/sessionStore';

const realCreateNewSession = useSessionStore.getState().createNewSession;

function setup(dispatching: boolean) {
  const dispatcher = {
    sendMessage: vi.fn(async () => undefined),
    isDispatching: { current: dispatching },
    dispatching,
    cancelDispatch: vi.fn(),
    setLastGenerated: vi.fn(),
  };
  const { result } = renderHook(() => useChatCommands({
    dispatcher,
    executionStream: { stopStream: vi.fn() },
    handleRefine: vi.fn(),
    lastGeneratedRef: { current: null },
    lastUserPromptRef: { current: '' },
    setPendingRun: vi.fn(),
  }));
  return { dispatcher, result };
}

const createNewSession = vi.fn(async () => 'new-session');

beforeEach(() => {
  vi.clearAllMocks();
  useSessionStore.setState({ messages: [], currentSessionId: 'session-1', createNewSession });
});

afterEach(() => {
  useSessionStore.setState({ createNewSession: realCreateNewSession });
});

describe('library load while a message is still being sent', () => {
  it('stays in the current chat and says why', async () => {
    const { dispatcher, result } = setup(true);

    await act(async () => { await result.current.handleLoadFromLibrary('crew', 'Weekly digest'); });

    expect(createNewSession).not.toHaveBeenCalled();
    expect(dispatcher.sendMessage).not.toHaveBeenCalled();
    expect(useSessionStore.getState().currentSessionId).toBe('session-1');
    expect(useSessionStore.getState().messages.at(-1)?.content).toContain('still being sent');
  });

  it('opens it in a new chat otherwise', async () => {
    const { dispatcher, result } = setup(false);

    await act(async () => { await result.current.handleLoadFromLibrary('crew', 'Weekly digest'); });

    expect(createNewSession).toHaveBeenCalledOnce();
    const [message, , , , , displayAs] = dispatcher.sendMessage.mock.calls[0] as unknown[];
    expect(message).toBe('/load crew Weekly digest');
    expect(displayAs).toBe('Open crew: Weekly digest');
  });
});
