import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useGenerationStream } from './useGenerationStream';
import type { StreamEvent } from '../api/streaming';

// Mock the streaming module so we control event emission and the close function.
vi.mock('../api/streaming', () => ({
  streamGeneration: vi.fn(),
}));

import { streamGeneration } from '../api/streaming';

const mockedStreamGeneration = vi.mocked(streamGeneration);

// Captured callbacks per streamGeneration invocation.
interface Captured {
  generationId: string;
  onEvent: (event: StreamEvent) => void;
  onError: (() => void) | undefined;
  close: ReturnType<typeof vi.fn>;
}

let captured: Captured[] = [];

function makeOptions() {
  return {
    onPlanReady: vi.fn(),
    onAgentDetail: vi.fn(),
    onTaskDetail: vi.fn(),
    onComplete: vi.fn(),
    onFailed: vi.fn(),
  };
}

beforeEach(() => {
  captured = [];
  mockedStreamGeneration.mockReset();
  mockedStreamGeneration.mockImplementation(
    (generationId: string, onEvent: (event: StreamEvent) => void, onError?: () => void) => {
      const close = vi.fn();
      captured.push({ generationId, onEvent, onError, close });
      return close;
    }
  );
  vi.spyOn(console, 'log').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

function last(): Captured {
  return captured[captured.length - 1];
}

describe('useGenerationStream', () => {
  it('starts a stream and passes the generationId to streamGeneration', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    expect(mockedStreamGeneration).toHaveBeenCalledTimes(1);
    expect(last().generationId).toBe('gen-1');
  });

  it('closes the previous stream when startStream is called again', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });
    const first = last();

    act(() => {
      result.current.startStream('gen-2');
    });

    // The previous close should have been invoked.
    expect(first.close).toHaveBeenCalledTimes(1);
    expect(mockedStreamGeneration).toHaveBeenCalledTimes(2);
    expect(last().generationId).toBe('gen-2');
  });

  it('handles plan_ready events', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const plan = { name: 'My Plan' };
    act(() => {
      last().onEvent({ event: 'plan_ready', data: plan });
    });

    expect(options.onPlanReady).toHaveBeenCalledWith(plan);
  });

  it('handles agent_detail events using the nested agent field', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const agent = { name: 'Agent A' };
    act(() => {
      last().onEvent({ event: 'agent_detail', data: { agent } });
    });

    expect(options.onAgentDetail).toHaveBeenCalledWith(agent);
  });

  it('handles agent_detail events falling back to the data object itself', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const data = { name: 'Agent B' };
    act(() => {
      last().onEvent({ event: 'agent_detail', data });
    });

    expect(options.onAgentDetail).toHaveBeenCalledWith(data);
  });

  it('handles task_detail events using the nested task field', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const task = { name: 'Task A' };
    act(() => {
      last().onEvent({ event: 'task_detail', data: { task } });
    });

    expect(options.onTaskDetail).toHaveBeenCalledWith(task);
  });

  it('handles task_detail events falling back to the data object itself', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const data = { name: 'Task B' };
    act(() => {
      last().onEvent({ event: 'task_detail', data });
    });

    expect(options.onTaskDetail).toHaveBeenCalledWith(data);
  });

  it('handles generation_complete with populated agents and tasks arrays', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const agents = [{ name: 'A1' }];
    const tasks = [{ name: 'T1' }];
    act(() => {
      last().onEvent({ event: 'generation_complete', data: { agents, tasks } });
    });

    expect(options.onComplete).toHaveBeenCalledWith({ agents, tasks });
    // Completing should stop the stream.
    expect(last().close).toHaveBeenCalledTimes(1);
  });

  it('falls back to streamed agents and tasks when generation_complete has empty arrays', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const agent = { name: 'streamed agent' };
    const task = { name: 'streamed task' };
    act(() => {
      last().onEvent({ event: 'agent_detail', data: { agent } });
      last().onEvent({ event: 'task_detail', data: { task } });
    });

    act(() => {
      last().onEvent({ event: 'generation_complete', data: { agents: [], tasks: [] } });
    });

    expect(options.onComplete).toHaveBeenCalledWith({
      agents: [agent],
      tasks: [task],
    });
  });

  it('falls back to streamed details when generation_complete has non-array agents/tasks', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    const agent = { name: 'streamed agent' };
    const task = { name: 'streamed task' };
    act(() => {
      last().onEvent({ event: 'agent_detail', data: { agent } });
      last().onEvent({ event: 'task_detail', data: { task } });
    });

    // agents/tasks not arrays -> rawAgents/rawTasks become [] -> fallback used.
    act(() => {
      last().onEvent({
        event: 'generation_complete',
        data: { agents: 'nope', tasks: 42 },
      });
    });

    expect(options.onComplete).toHaveBeenCalledWith({
      agents: [agent],
      tasks: [task],
    });
  });

  it('does not log fallback when there are no streamed details and complete arrays are empty', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onEvent({ event: 'generation_complete', data: { agents: [], tasks: [] } });
    });

    expect(options.onComplete).toHaveBeenCalledWith({ agents: [], tasks: [] });
  });

  it('handles generation_failed using the error field', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onEvent({ event: 'generation_failed', data: { error: 'boom' } });
    });

    expect(options.onFailed).toHaveBeenCalledWith('boom');
    expect(last().close).toHaveBeenCalledTimes(1);
  });

  it('handles generation_failed falling back to the message field', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onEvent({ event: 'generation_failed', data: { message: 'msg failure' } });
    });

    expect(options.onFailed).toHaveBeenCalledWith('msg failure');
  });

  it('handles generation_failed falling back to the default message', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onEvent({ event: 'generation_failed', data: {} });
    });

    expect(options.onFailed).toHaveBeenCalledWith('Generation failed');
  });

  it('handles entity_error events without throwing or invoking callbacks', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onEvent({ event: 'entity_error', data: { detail: 'fk error' } });
    });

    expect(console.warn).toHaveBeenCalledWith('[generationStream] entity_error:', {
      detail: 'fk error',
    });
    expect(options.onFailed).not.toHaveBeenCalled();
    expect(options.onComplete).not.toHaveBeenCalled();
  });

  it('ignores unknown / connected events (default switch path)', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onEvent({ event: 'connected', data: {} });
    });

    expect(options.onPlanReady).not.toHaveBeenCalled();
    expect(options.onAgentDetail).not.toHaveBeenCalled();
    expect(options.onTaskDetail).not.toHaveBeenCalled();
    expect(options.onComplete).not.toHaveBeenCalled();
    expect(options.onFailed).not.toHaveBeenCalled();
  });

  it('invokes onFailed via the error callback when generation has not completed', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onError?.();
    });

    expect(options.onFailed).toHaveBeenCalledWith('Connection lost during generation');
  });

  it('does not invoke onFailed via the error callback when generation already completed', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    act(() => {
      result.current.startStream('gen-1');
    });

    act(() => {
      last().onEvent({ event: 'generation_complete', data: { agents: [{ a: 1 }], tasks: [{ t: 1 }] } });
    });

    // completedRef is now true; the error callback should be a no-op.
    act(() => {
      last().onError?.();
    });

    expect(options.onFailed).not.toHaveBeenCalled();
  });

  it('stopStream closes the active stream and is safe to call when no stream is active', () => {
    const options = makeOptions();
    const { result } = renderHook(() => useGenerationStream(options));

    // Safe no-op before starting.
    act(() => {
      result.current.stopStream();
    });

    act(() => {
      result.current.startStream('gen-1');
    });
    const stream = last();

    act(() => {
      result.current.stopStream();
    });

    expect(stream.close).toHaveBeenCalledTimes(1);

    // Calling again is a no-op (closeRef was nulled).
    act(() => {
      result.current.stopStream();
    });
    expect(stream.close).toHaveBeenCalledTimes(1);
  });

  it('uses the latest options via the optionsRef effect after a re-render', () => {
    const first = makeOptions();
    const { result, rerender } = renderHook(
      ({ opts }) => useGenerationStream(opts),
      { initialProps: { opts: first } }
    );

    act(() => {
      result.current.startStream('gen-1');
    });

    const second = makeOptions();
    rerender({ opts: second });

    act(() => {
      last().onEvent({ event: 'plan_ready', data: { name: 'plan' } });
    });

    // The newest options object should be used, not the stale one.
    expect(second.onPlanReady).toHaveBeenCalledWith({ name: 'plan' });
    expect(first.onPlanReady).not.toHaveBeenCalled();
  });
});
