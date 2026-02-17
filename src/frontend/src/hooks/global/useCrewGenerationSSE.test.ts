import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock('../../config/api/ApiConfig', () => ({
  config: { apiUrl: 'http://test-api' },
}));

class MockEventSource {
  url: string;
  listeners: Record<string, ((e: MessageEvent) => void)[]> = {};
  readyState = 0;
  close = vi.fn();
  static CLOSED = 2;

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(event: string, handler: (e: MessageEvent) => void) {
    if (!this.listeners[event]) this.listeners[event] = [];
    this.listeners[event].push(handler);
  }

  /** Simulate dispatching an SSE event. */
  _emit(event: string, data?: string) {
    const handlers = this.listeners[event] || [];
    handlers.forEach((h) => h({ data } as MessageEvent));
  }

  onerror: (() => void) | null = null;

  static instances: MockEventSource[] = [];
  static reset() {
    MockEventSource.instances = [];
  }
}

(globalThis as Record<string, unknown>).EventSource = MockEventSource;

// Import after mocks are registered
import { useCrewGenerationSSE, CrewGenerationSSEHandlers } from './useCrewGenerationSSE';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockHandlers(): CrewGenerationSSEHandlers {
  return {
    onPlanReady: vi.fn(),
    onAgentDetail: vi.fn(),
    onTaskDetail: vi.fn(),
    onEntityError: vi.fn(),
    onDependenciesResolved: vi.fn(),
    onToolConfigNeeded: vi.fn(),
    onComplete: vi.fn(),
    onFailed: vi.fn(),
  };
}

/** Return the most recently created MockEventSource. */
function latestES(): MockEventSource {
  const all = MockEventSource.instances;
  return all[all.length - 1];
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useCrewGenerationSSE', () => {
  beforeEach(() => {
    MockEventSource.reset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // ---- test_initial_status_idle -------------------------------------------
  it('should have status "idle" when generationId is null', () => {
    const handlers = createMockHandlers();
    const { result } = renderHook(() => useCrewGenerationSSE(null, handlers));

    expect(result.current.status).toBe('idle');
    expect(MockEventSource.instances).toHaveLength(0);
  });

  // ---- test_connects_on_generationId --------------------------------------
  it('should create an EventSource with the correct URL when generationId is provided', () => {
    const handlers = createMockHandlers();
    renderHook(() => useCrewGenerationSSE('gen-42', handlers));

    expect(MockEventSource.instances).toHaveLength(1);
    expect(latestES().url).toBe('http://test-api/sse/generations/gen-42/stream');
  });

  // ---- test_status_connecting_then_streaming ------------------------------
  it('should transition status from "connecting" to "streaming" on the "connected" event', () => {
    const handlers = createMockHandlers();
    const { result } = renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    // Immediately after mount the status should be "connecting"
    expect(result.current.status).toBe('connecting');

    // Simulate the server sending the "connected" event
    act(() => {
      latestES()._emit('connected');
    });

    expect(result.current.status).toBe('streaming');
  });

  // ---- test_plan_ready_handler --------------------------------------------
  it('should parse JSON and call onPlanReady for "plan_ready" events', () => {
    const handlers = createMockHandlers();
    renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = { type: 'plan_ready', agents: [{ name: 'A', role: 'R' }], tasks: [] };

    act(() => {
      latestES()._emit('plan_ready', JSON.stringify(payload));
    });

    expect(handlers.onPlanReady).toHaveBeenCalledTimes(1);
    expect(handlers.onPlanReady).toHaveBeenCalledWith(payload);
  });

  // ---- test_agent_detail_handler ------------------------------------------
  it('should parse JSON and call onAgentDetail for "agent_detail" events', () => {
    const handlers = createMockHandlers();
    renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = { type: 'agent_detail', index: 0, agent: { id: 'a1', name: 'Agent' } };

    act(() => {
      latestES()._emit('agent_detail', JSON.stringify(payload));
    });

    expect(handlers.onAgentDetail).toHaveBeenCalledTimes(1);
    expect(handlers.onAgentDetail).toHaveBeenCalledWith(payload);
  });

  // ---- test_task_detail_handler -------------------------------------------
  it('should parse JSON and call onTaskDetail for "task_detail" events', () => {
    const handlers = createMockHandlers();
    renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = { type: 'task_detail', index: 1, task: { id: 't1', name: 'Task' } };

    act(() => {
      latestES()._emit('task_detail', JSON.stringify(payload));
    });

    expect(handlers.onTaskDetail).toHaveBeenCalledTimes(1);
    expect(handlers.onTaskDetail).toHaveBeenCalledWith(payload);
  });

  // ---- test_entity_error_handler ------------------------------------------
  it('should parse JSON and call onEntityError for "entity_error" events', () => {
    const handlers = createMockHandlers();
    renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = {
      type: 'entity_error',
      index: 0,
      entity_type: 'agent',
      name: 'BadAgent',
      error: 'oops',
    };

    act(() => {
      latestES()._emit('entity_error', JSON.stringify(payload));
    });

    expect(handlers.onEntityError).toHaveBeenCalledTimes(1);
    expect(handlers.onEntityError).toHaveBeenCalledWith(payload);
  });

  // ---- test_dependencies_resolved_handler ---------------------------------
  it('should parse JSON and call onDependenciesResolved for "dependencies_resolved" events', () => {
    const handlers = createMockHandlers();
    renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = {
      type: 'dependencies_resolved',
      task_id: 't-1',
      task_name: 'Task 1',
      context: ['t-0'],
    };

    act(() => {
      latestES()._emit('dependencies_resolved', JSON.stringify(payload));
    });

    expect(handlers.onDependenciesResolved).toHaveBeenCalledTimes(1);
    expect(handlers.onDependenciesResolved).toHaveBeenCalledWith(payload);
  });

  // ---- test_tool_config_needed_handler ------------------------------------
  it('should parse JSON and call onToolConfigNeeded for "tool_config_needed" events', () => {
    const handlers = createMockHandlers();
    renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = {
      type: 'tool_config_needed',
      task_id: 't-2',
      task_name: 'Task 2',
      tool_name: 'GenieTool',
      config_fields: ['spaceId'],
      suggested_space: null,
    };

    act(() => {
      latestES()._emit('tool_config_needed', JSON.stringify(payload));
    });

    expect(handlers.onToolConfigNeeded).toHaveBeenCalledTimes(1);
    expect(handlers.onToolConfigNeeded).toHaveBeenCalledWith(payload);
  });

  // ---- test_generation_complete_handler -----------------------------------
  it('should call onComplete, set status to "complete", and close the EventSource on "generation_complete"', () => {
    const handlers = createMockHandlers();
    const { result } = renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = {
      type: 'generation_complete',
      agents: [{ id: 'a1' }],
      tasks: [{ id: 't1' }],
    };

    act(() => {
      latestES()._emit('generation_complete', JSON.stringify(payload));
    });

    expect(handlers.onComplete).toHaveBeenCalledTimes(1);
    expect(handlers.onComplete).toHaveBeenCalledWith(payload);
    expect(result.current.status).toBe('complete');
    expect(latestES().close).toHaveBeenCalled();
  });

  // ---- test_generation_failed_handler -------------------------------------
  it('should call onFailed, set status to "failed", and close the EventSource on "generation_failed"', () => {
    const handlers = createMockHandlers();
    const { result } = renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const payload = { type: 'generation_failed', error: 'something broke' };

    act(() => {
      latestES()._emit('generation_failed', JSON.stringify(payload));
    });

    expect(handlers.onFailed).toHaveBeenCalledTimes(1);
    expect(handlers.onFailed).toHaveBeenCalledWith(payload);
    expect(result.current.status).toBe('failed');
    expect(latestES().close).toHaveBeenCalled();
  });

  // ---- test_parse_error_handled -------------------------------------------
  it('should not crash when event data contains invalid JSON', () => {
    const handlers = createMockHandlers();
    const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    // Send malformed JSON -- should be caught internally, no handler called
    act(() => {
      latestES()._emit('plan_ready', '<<<not json>>>');
    });

    expect(handlers.onPlanReady).not.toHaveBeenCalled();
    expect(consoleErrorSpy).toHaveBeenCalled();

    consoleErrorSpy.mockRestore();
  });

  // ---- test_disconnect_closes_event_source --------------------------------
  it('should close the EventSource when disconnect is called manually', () => {
    const handlers = createMockHandlers();
    const { result } = renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const es = latestES();

    act(() => {
      result.current.disconnect();
    });

    expect(es.close).toHaveBeenCalled();
  });

  // ---- test_cleanup_on_unmount --------------------------------------------
  it('should close the EventSource when the hook unmounts', () => {
    const handlers = createMockHandlers();
    const { unmount } = renderHook(() => useCrewGenerationSSE('gen-1', handlers));

    const es = latestES();

    unmount();

    expect(es.close).toHaveBeenCalled();
  });
});
