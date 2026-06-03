import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useExecutionStream } from './useExecutionStream';
import { streamExecution, StreamEvent } from '../api/streaming';

vi.mock('../api/streaming', () => ({
  streamExecution: vi.fn(),
}));

const mockedStreamExecution = vi.mocked(streamExecution);

type EventCb = (event: StreamEvent) => void;
type ConnLostCb = () => void;

interface Capture {
  jobId: string;
  onEvent: EventCb;
  onConnLost: ConnLostCb;
  close: ReturnType<typeof vi.fn>;
}

/**
 * Configure streamExecution mock to capture the callbacks passed by the hook
 * and return a fresh close function for each call. Returns an array that is
 * populated (one entry per startStream invocation).
 */
function setupStreamCapture(): Capture[] {
  const captures: Capture[] = [];
  mockedStreamExecution.mockImplementation(
    (jobId: string, onEvent: EventCb, onConnLost?: (e: Event) => void) => {
      const close = vi.fn();
      captures.push({
        jobId,
        onEvent,
        onConnLost: onConnLost as ConnLostCb,
        close,
      });
      return close;
    }
  );
  return captures;
}

function makeOptions() {
  return {
    onTrace: vi.fn(),
    onTaskOutput: vi.fn(),
    onStatusChange: vi.fn(),
    onComplete: vi.fn(),
    onError: vi.fn(),
  };
}

describe('useExecutionStream', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('startStream calls streamExecution with the jobId', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });

    expect(mockedStreamExecution).toHaveBeenCalledTimes(1);
    expect(captures[0].jobId).toBe('job-1');
  });

  it('handles the "connected" event by invoking onStatusChange', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'connected', data: { foo: 'bar' } });
    });

    expect(options.onStatusChange).toHaveBeenCalledWith('connected', {
      foo: 'bar',
    });
  });

  it('handles execution_update with completed status -> onComplete + stop', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const data = { status: 'COMPLETED', result: { answer: 42 } };
    act(() => {
      captures[0].onEvent({ event: 'execution_update', data });
    });

    expect(options.onStatusChange).toHaveBeenCalledWith('COMPLETED', data);
    expect(options.onComplete).toHaveBeenCalledWith(data);
    // stopStream closes the connection
    expect(captures[0].close).toHaveBeenCalledTimes(1);
  });

  it('handles execution_update with failed status -> onError with error field', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const data = { status: 'FAILED', error: 'boom' };
    act(() => {
      captures[0].onEvent({ event: 'execution_update', data });
    });

    expect(options.onError).toHaveBeenCalledWith('boom');
    expect(captures[0].close).toHaveBeenCalledTimes(1);
  });

  it('handles execution_update with stopped status and no error -> default error message', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'execution_update', data: { status: 'stopped' } });
    });

    expect(options.onError).toHaveBeenCalledWith('Execution stopped');
  });

  it('handles execution_update with running status (no completion branch)', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'execution_update', data: { status: 'RUNNING' } });
    });

    expect(options.onStatusChange).toHaveBeenCalledWith('RUNNING', { status: 'RUNNING' });
    expect(options.onComplete).not.toHaveBeenCalled();
    expect(options.onError).not.toHaveBeenCalled();
    expect(captures[0].close).not.toHaveBeenCalled();
  });

  it('handles execution_update with missing status -> empty string status', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'execution_update', data: {} });
    });

    expect(options.onStatusChange).toHaveBeenCalledWith('', {});
  });

  it('handles trace event using the message field', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const data = { message: 'hello trace' };
    act(() => {
      captures[0].onEvent({ event: 'trace', data });
    });

    expect(options.onTrace).toHaveBeenCalledWith('hello trace', data);
    expect(options.onTaskOutput).not.toHaveBeenCalled();
  });

  it('handles trace event falling back to the trace field', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const data = { trace: 'trace text' };
    act(() => {
      captures[0].onEvent({ event: 'trace', data });
    });

    expect(options.onTrace).toHaveBeenCalledWith('trace text', data);
  });

  it('handles trace event falling back to JSON.stringify of data', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const data = { something: 'else' };
    act(() => {
      captures[0].onEvent({ event: 'trace', data });
    });

    expect(options.onTrace).toHaveBeenCalledWith(JSON.stringify(data), data);
  });

  it('handles trace task_completed with metadata task_name and string output', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const data = {
      message: 'm',
      event_type: 'task_completed',
      trace_metadata: { task_name: 'My Task' },
      output: 'task output text',
    };
    act(() => {
      captures[0].onEvent({ event: 'trace', data });
    });

    expect(options.onTaskOutput).toHaveBeenCalledWith('My Task', 'task output text');
  });

  it('handles trace task_completed falling back to event_context and result (object output stringified)', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const resultObj = { value: 1 };
    const data = {
      message: 'm',
      event_type: 'task_completed',
      event_context: 'CtxTask',
      result: resultObj,
    };
    act(() => {
      captures[0].onEvent({ event: 'trace', data });
    });

    expect(options.onTaskOutput).toHaveBeenCalledWith('CtxTask', JSON.stringify(resultObj));
  });

  it('handles trace task_completed defaulting task name to "Task" and output to msg', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    const data = {
      message: 'fallback msg',
      event_type: 'task_completed',
    };
    act(() => {
      captures[0].onEvent({ event: 'trace', data });
    });

    expect(options.onTaskOutput).toHaveBeenCalledWith('Task', 'fallback msg');
  });

  it('does NOT call onTaskOutput for task_completed when onTaskOutput is undefined', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    // Remove the optional callback
    const { onTaskOutput, ...rest } = options;
    void onTaskOutput;
    const { result } = renderHook(() => useExecutionStream(rest));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({
        event: 'trace',
        data: { message: 'm', event_type: 'task_completed' },
      });
    });

    // onTrace still fires
    expect(rest.onTrace).toHaveBeenCalled();
  });

  it('handles trace with non-task_completed event_type (no task output)', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({
        event: 'trace',
        data: { message: 'm', event_type: 'task_started' },
      });
    });

    expect(options.onTaskOutput).not.toHaveBeenCalled();
  });

  it('handles error event when not yet completed -> onError with message + stop', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'error', data: { message: 'failure happened' } });
    });

    expect(options.onError).toHaveBeenCalledWith('failure happened');
    expect(captures[0].close).toHaveBeenCalledTimes(1);
  });

  it('handles error event with no message -> default "Unknown error"', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'error', data: {} });
    });

    expect(options.onError).toHaveBeenCalledWith('Unknown error');
  });

  it('error event after completion does NOT call onError again', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    // Complete first -> sets completedRef true and stops
    act(() => {
      captures[0].onEvent({ event: 'execution_update', data: { status: 'completed' } });
    });
    options.onError.mockClear();
    // Now an error event arrives
    act(() => {
      captures[0].onEvent({ event: 'error', data: { message: 'late error' } });
    });

    expect(options.onError).not.toHaveBeenCalled();
  });

  it('handles unhandled event types via the default branch', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'message', data: { foo: 'bar' } });
    });

    expect(options.onStatusChange).not.toHaveBeenCalled();
    expect(options.onTrace).not.toHaveBeenCalled();
    expect(options.onError).not.toHaveBeenCalled();
    expect(options.onComplete).not.toHaveBeenCalled();
  });

  it('connection lost callback calls onError when not completed', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onConnLost();
    });

    expect(options.onError).toHaveBeenCalledWith('Connection lost');
  });

  it('connection lost callback does NOT call onError when already completed', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      captures[0].onEvent({ event: 'execution_update', data: { status: 'completed' } });
    });
    options.onError.mockClear();
    act(() => {
      captures[0].onConnLost();
    });

    expect(options.onError).not.toHaveBeenCalled();
  });

  it('double startStream closes the previous stream first', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      result.current.startStream('job-2');
    });

    // The first stream's close was invoked when starting the second
    expect(captures[0].close).toHaveBeenCalledTimes(1);
    expect(mockedStreamExecution).toHaveBeenCalledTimes(2);
    expect(captures[1].jobId).toBe('job-2');
  });

  it('stopStream is a no-op when no active stream exists', () => {
    setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    // No startStream called; should not throw
    act(() => {
      result.current.stopStream();
    });

    expect(mockedStreamExecution).not.toHaveBeenCalled();
  });

  it('stopStream closes an active stream and clears the ref', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    act(() => {
      result.current.stopStream();
    });
    // Calling again does nothing (ref already cleared)
    act(() => {
      result.current.stopStream();
    });

    expect(captures[0].close).toHaveBeenCalledTimes(1);
  });

  it('cleans up the active stream on unmount', () => {
    const captures = setupStreamCapture();
    const options = makeOptions();
    const { result, unmount } = renderHook(() => useExecutionStream(options));

    act(() => {
      result.current.startStream('job-1');
    });
    unmount();

    expect(captures[0].close).toHaveBeenCalledTimes(1);
  });

  it('unmount cleanup is a no-op when there is no active stream', () => {
    setupStreamCapture();
    const options = makeOptions();
    const { unmount } = renderHook(() => useExecutionStream(options));

    expect(() => unmount()).not.toThrow();
    expect(mockedStreamExecution).not.toHaveBeenCalled();
  });

  it('uses the latest options via optionsRef after re-render', () => {
    const captures = setupStreamCapture();
    const first = makeOptions();
    const { result, rerender } = renderHook((opts) => useExecutionStream(opts), {
      initialProps: first,
    });

    act(() => {
      result.current.startStream('job-1');
    });

    const second = makeOptions();
    rerender(second);

    act(() => {
      captures[0].onEvent({ event: 'connected', data: { x: 1 } });
    });

    // The updated (second) options should be used
    expect(second.onStatusChange).toHaveBeenCalledWith('connected', { x: 1 });
    expect(first.onStatusChange).not.toHaveBeenCalled();
  });
});
