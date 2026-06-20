import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { StreamEvent } from '../api/streaming';
import {
  startGenerationStream,
  stopGenerationStream,
  stopAllGenerationStreams,
  isGenerationStreaming,
  GenerationStreamCallbacks,
} from './generationStreamManager';

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

function makeCallbacks(): GenerationStreamCallbacks & { [k: string]: ReturnType<typeof vi.fn> } {
  return {
    onPlanReady: vi.fn(),
    onAgentDetail: vi.fn(),
    onTaskDetail: vi.fn(),
    onComplete: vi.fn(),
    onExecutionStarted: vi.fn(),
    onFailed: vi.fn(),
  } as GenerationStreamCallbacks & { [k: string]: ReturnType<typeof vi.fn> };
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
  stopAllGenerationStreams(); // clear the module-level Map between tests
  vi.restoreAllMocks();
});

function streamFor(generationId: string): Captured {
  return [...captured].reverse().find((c) => c.generationId === generationId)!;
}

describe('generationStreamManager', () => {
  it('starts a stream and passes the generationId to streamGeneration', () => {
    startGenerationStream('gen-1', makeCallbacks());
    expect(mockedStreamGeneration).toHaveBeenCalledTimes(1);
    expect(streamFor('gen-1').generationId).toBe('gen-1');
    expect(isGenerationStreaming('gen-1')).toBe(true);
  });

  it('runs generations CONCURRENTLY — starting another does not close the first', () => {
    startGenerationStream('gen-1', makeCallbacks());
    const first = streamFor('gen-1');
    startGenerationStream('gen-2', makeCallbacks());

    // Both streams stay open — the first is NOT closed (that used to drop a
    // backgrounded run's stream and leave it stuck "Thinking...").
    expect(first.close).not.toHaveBeenCalled();
    expect(mockedStreamGeneration).toHaveBeenCalledTimes(2);
    expect(isGenerationStreaming('gen-1')).toBe(true);
    expect(isGenerationStreaming('gen-2')).toBe(true);
  });

  it('is idempotent — a second start for the same id refreshes callbacks, keeps one stream', () => {
    const cb1 = makeCallbacks();
    const cb2 = makeCallbacks();
    startGenerationStream('gen-1', cb1);
    startGenerationStream('gen-1', cb2);

    expect(mockedStreamGeneration).toHaveBeenCalledTimes(1);
    // The refreshed callbacks (cb2) receive events.
    streamFor('gen-1').onEvent({ event: 'plan_ready', data: { name: 'P' } });
    expect(cb2.onPlanReady).toHaveBeenCalledWith('gen-1', { name: 'P' });
    expect(cb1.onPlanReady).not.toHaveBeenCalled();
  });

  it('passes the generationId to every callback so events route per-generation', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    streamFor('gen-1').onEvent({ event: 'plan_ready', data: { name: 'My Plan' } });
    expect(cb.onPlanReady).toHaveBeenCalledWith('gen-1', { name: 'My Plan' });
  });

  it('handles agent_detail (nested agent field) and task_detail (nested task field)', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    const agent = { id: 'a1', name: 'Agent A' };
    const task = { id: 't1', name: 'Task A' };
    streamFor('gen-1').onEvent({ event: 'agent_detail', data: { agent } });
    streamFor('gen-1').onEvent({ event: 'task_detail', data: { task } });
    expect(cb.onAgentDetail).toHaveBeenCalledWith('gen-1', agent);
    expect(cb.onTaskDetail).toHaveBeenCalledWith('gen-1', task);
  });

  it('agent_detail / task_detail fall back to the data object itself', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    const agent = { id: 'b1', name: 'Agent B' };
    const task = { id: 't2', name: 'Task B' };
    streamFor('gen-1').onEvent({ event: 'agent_detail', data: agent });
    streamFor('gen-1').onEvent({ event: 'task_detail', data: task });
    expect(cb.onAgentDetail).toHaveBeenCalledWith('gen-1', agent);
    expect(cb.onTaskDetail).toHaveBeenCalledWith('gen-1', task);
  });

  it('dedups replayed plan/agent/task events (SSE reconnect re-sends the buffer)', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    const s = streamFor('gen-1');
    const plan = { name: 'P' };
    const agent = { id: 'a1', name: 'A1' };
    const task = { id: 't1', name: 'T1' };
    s.onEvent({ event: 'plan_ready', data: plan });
    s.onEvent({ event: 'agent_detail', data: { agent } });
    s.onEvent({ event: 'task_detail', data: { task } });
    // Replay after a reconnect — same ids, must be ignored.
    s.onEvent({ event: 'plan_ready', data: plan });
    s.onEvent({ event: 'agent_detail', data: { agent } });
    s.onEvent({ event: 'task_detail', data: { task } });

    expect(cb.onPlanReady).toHaveBeenCalledTimes(1);
    expect(cb.onAgentDetail).toHaveBeenCalledTimes(1);
    expect(cb.onTaskDetail).toHaveBeenCalledTimes(1);
  });

  it('handles generation_complete and closes the stream (single terminal event)', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    const s = streamFor('gen-1');
    const agents = [{ name: 'A1' }];
    const tasks = [{ name: 'T1' }];
    s.onEvent({ event: 'generation_complete', data: { agents, tasks } });

    expect(cb.onComplete).toHaveBeenCalledWith('gen-1', { agents, tasks });
    expect(s.close).toHaveBeenCalledTimes(1);
    expect(isGenerationStreaming('gen-1')).toBe(false);
  });

  it('observes the backend run from the execution_id folded into generation_complete', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    streamFor('gen-1').onEvent({
      event: 'generation_complete',
      data: { agents: [{ name: 'A1' }], tasks: [{ name: 'T1' }], execution_id: 'job-99', run_name: 'Run 99' },
    });

    expect(cb.onComplete).toHaveBeenCalledWith('gen-1', { agents: [{ name: 'A1' }], tasks: [{ name: 'T1' }] });
    expect(cb.onExecutionStarted).toHaveBeenCalledWith('gen-1', 'job-99', 'Run 99');
  });

  it('reports a failed backend launch (execution_error) via onFailed', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    streamFor('gen-1').onEvent({
      event: 'generation_complete',
      data: { agents: [{ name: 'A1' }], tasks: [], execution_error: 'no capacity' },
    });

    expect(cb.onComplete).toHaveBeenCalledWith('gen-1', { agents: [{ name: 'A1' }], tasks: [] });
    expect(cb.onExecutionStarted).not.toHaveBeenCalled();
    expect(cb.onFailed).toHaveBeenCalledWith('gen-1', 'no capacity');
  });

  it('falls back to streamed agents/tasks when generation_complete has empty or non-array arrays', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    const s = streamFor('gen-1');
    const agent = { id: 'a1', name: 'streamed agent' };
    const task = { id: 't1', name: 'streamed task' };
    s.onEvent({ event: 'agent_detail', data: { agent } });
    s.onEvent({ event: 'task_detail', data: { task } });
    s.onEvent({ event: 'generation_complete', data: { agents: 'nope', tasks: 42 } });

    expect(cb.onComplete).toHaveBeenCalledWith('gen-1', { agents: [agent], tasks: [task] });
  });

  it('handles generation_failed (error, message, default) and closes', () => {
    const cb1 = makeCallbacks();
    startGenerationStream('g1', cb1);
    streamFor('g1').onEvent({ event: 'generation_failed', data: { error: 'boom' } });
    expect(cb1.onFailed).toHaveBeenCalledWith('g1', 'boom');

    const cb2 = makeCallbacks();
    startGenerationStream('g2', cb2);
    streamFor('g2').onEvent({ event: 'generation_failed', data: { message: 'msg failure' } });
    expect(cb2.onFailed).toHaveBeenCalledWith('g2', 'msg failure');

    const cb3 = makeCallbacks();
    startGenerationStream('g3', cb3);
    streamFor('g3').onEvent({ event: 'generation_failed', data: {} });
    expect(cb3.onFailed).toHaveBeenCalledWith('g3', 'Generation failed');
  });

  it('logs entity_error without invoking callbacks; ignores connected/unknown', () => {
    const cb = makeCallbacks();
    startGenerationStream('gen-1', cb);
    streamFor('gen-1').onEvent({ event: 'entity_error', data: { detail: 'fk error' } });
    streamFor('gen-1').onEvent({ event: 'connected', data: {} });

    expect(console.warn).toHaveBeenCalledWith('[generationStream] entity_error:', { detail: 'fk error' });
    expect(cb.onComplete).not.toHaveBeenCalled();
    expect(cb.onFailed).not.toHaveBeenCalled();
  });

  it('error callback reports a lost connection only when not completed', () => {
    const cbA = makeCallbacks();
    startGenerationStream('g1', cbA);
    streamFor('g1').onError?.();
    expect(cbA.onFailed).toHaveBeenCalledWith('g1', 'Connection lost during generation');

    const cbB = makeCallbacks();
    startGenerationStream('g2', cbB);
    const s = streamFor('g2');
    s.onEvent({ event: 'generation_complete', data: { agents: [], tasks: [] } });
    s.onError?.(); // completed + closed → no-op
    expect(cbB.onFailed).not.toHaveBeenCalled();
  });

  it('stopGenerationStream closes that stream and is safe when none is active', () => {
    stopGenerationStream('gen-1'); // no-op before starting
    startGenerationStream('gen-1', makeCallbacks());
    const s = streamFor('gen-1');
    stopGenerationStream('gen-1');
    expect(s.close).toHaveBeenCalledTimes(1);
    expect(isGenerationStreaming('gen-1')).toBe(false);
    stopGenerationStream('gen-1'); // no-op again
    expect(s.close).toHaveBeenCalledTimes(1);
  });

  it('stopAllGenerationStreams closes every open stream', () => {
    startGenerationStream('g1', makeCallbacks());
    startGenerationStream('g2', makeCallbacks());
    const s1 = streamFor('g1');
    const s2 = streamFor('g2');
    stopAllGenerationStreams();
    expect(s1.close).toHaveBeenCalledTimes(1);
    expect(s2.close).toHaveBeenCalledTimes(1);
    expect(isGenerationStreaming('g1')).toBe(false);
    expect(isGenerationStreaming('g2')).toBe(false);
  });

  it('routes concurrent generations to their own callbacks by generationId', () => {
    const cbA = makeCallbacks();
    const cbB = makeCallbacks();
    startGenerationStream('gen-A', cbA);
    startGenerationStream('gen-B', cbB);
    streamFor('gen-A').onEvent({ event: 'plan_ready', data: { name: 'A' } });
    streamFor('gen-B').onEvent({ event: 'plan_ready', data: { name: 'B' } });

    expect(cbA.onPlanReady).toHaveBeenCalledWith('gen-A', { name: 'A' });
    expect(cbB.onPlanReady).toHaveBeenCalledWith('gen-B', { name: 'B' });
    expect(cbA.onPlanReady).toHaveBeenCalledTimes(1);
    expect(cbB.onPlanReady).toHaveBeenCalledTimes(1);
  });
});
