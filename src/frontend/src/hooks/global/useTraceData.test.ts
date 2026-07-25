/**
 * Tests for processTraces task grouping — specifically the task-less light/chat
 * agent path (Agent.kickoff_async), whose traces carry no crew task id and must
 * NOT be surfaced as an "Unassigned" task in the Execution Trace Timeline.
 */
import { describe, it, expect } from 'vitest';
import { processTraces } from './useTraceData';
import { Trace } from '../../types/trace';

let _id = 0;
const makeTrace = (overrides: Partial<Trace>): Trace => ({
  id: ++_id,
  event_source: 'Assistant',
  event_context: 'chat',
  event_type: 'response_run',
  output: { tool_name: 'Response', content: 'done' },
  created_at: new Date(2024, 0, 1, 0, 0, _id).toISOString(),
  ...overrides,
});

describe('processTraces — light/chat agent (task-less run)', () => {
  it('relabels the synthetic "Unassigned" bucket to the user request and flags it', () => {
    const traces: Trace[] = [
      makeTrace({ event_type: 'tool_usage', event_context: 'Top Swiss news today',
                  output: { tool_name: 'PerplexitySearch' } }),
      makeTrace({ event_type: 'perplexitysearch_run', event_context: 'Top Swiss news today',
                  output: { tool_name: 'PerplexitySearch', content: '...' } }),
      makeTrace({ event_type: 'response_run', event_context: 'Top Swiss news today',
                  output: { tool_name: 'Response', content: 'answer' } }),
    ];

    const result = processTraces(traces);

    expect(result.agents).toHaveLength(1);
    const agent = result.agents[0];
    expect(agent.agent).toBe('Assistant');
    expect(agent.tasks).toHaveLength(1);
    const task = agent.tasks[0];
    // Not framed as the literal "Unassigned" crew task...
    expect(task.taskName).not.toBe('Unassigned');
    // ...but as the user's request, and flagged so the UI drops the task chrome.
    expect(task.taskName).toBe('Top Swiss news today');
    expect(task.unassigned).toBe(true);
  });

  it('falls back to the agent name when no request context is present', () => {
    const traces: Trace[] = [
      makeTrace({ event_source: 'Helper', event_type: 'response_run',
                  // event_context === event_type → treated as "no context"
                  event_context: 'response_run' }),
    ];

    const result = processTraces(traces);
    const task = result.agents[0].tasks[0];
    expect(task.taskName).toBe('Helper');
    expect(task.unassigned).toBe(true);
  });
});

describe('processTraces — additive row durations', () => {
  it('row durations sum to the task span, even across filtered raw traces', () => {
    const t0 = new Date('2024-01-01T00:00:00.000Z').getTime();
    const at = (ms: number) => new Date(t0 + ms).toISOString();

    const traces: Trace[] = [
      makeTrace({ event_source: 'Worker', event_type: 'task_started', task_id: 'T1',
                  event_context: 'Do the work', created_at: at(0) }),
      makeTrace({ event_source: 'Worker', event_type: 'memory_retrieval', task_id: 'T1',
                  trace_metadata: { query_time_ms: 601, results_count: 6 },
                  output: { content: 'mem' }, created_at: at(100) }),
      makeTrace({ event_source: 'Worker', event_type: 'llm_call', task_id: 'T1',
                  trace_metadata: { model: 'm' }, created_at: at(500) }),
      makeTrace({ event_source: 'Worker', event_type: 'llm_response', task_id: 'T1',
                  output: { content: 'answer' }, created_at: at(6000) }),
      // Filtered raw trace (guardrail_started → null): its wall time must be
      // donated to the preceding visible row (the LLM Response), not vanish.
      makeTrace({ event_source: 'Worker', event_type: 'guardrail_started', task_id: 'T1',
                  created_at: at(6300) }),
      makeTrace({ event_source: 'Worker', event_type: 'llm_guardrail', task_id: 'T1',
                  trace_metadata: { success: true }, output: { content: 'ok' },
                  created_at: at(16000) }),
      makeTrace({ event_source: 'Worker', event_type: 'task_completed', task_id: 'T1',
                  event_context: 'Do the work', output: { content: 'done' },
                  created_at: at(17000) }),
    ];

    const result = processTraces(traces);
    const task = result.agents[0].tasks[0];

    // Task header duration = first-event → last-event span.
    expect(task.duration).toBe(17000);

    // Additivity: sum of the visible rows' wall slices equals the task span
    // (sub-50ms suppressed-row slack allowed, well within 200ms here).
    const sum = task.events.reduce((acc, e) => acc + (e.duration ?? 0), 0);
    expect(Math.abs(sum - task.duration)).toBeLessThanOrEqual(200);

    // The LLM Response row carries its TRUE gap to the next visible row
    // (response → guardrail), including the filtered guardrail_started slice.
    const response = task.events.find(e => e.type === 'llm_response');
    expect(response!.duration).toBe(10000);

    // Intrinsic op time is detail (intrinsicMs), not the column value: the
    // memory row's duration is its wall slice, not query_time_ms.
    const memory = task.events.find(e => e.type === 'memory_retrieval');
    expect(memory!.duration).toBe(400);
    expect(memory!.intrinsicMs).toBe(601);

    // Last row closes the span (task end − its own timestamp).
    const last = task.events[task.events.length - 1];
    expect(last.duration).toBe(0);
  });
});

describe('processTraces — crew run (has task ids)', () => {
  it('keeps a stray no-task trace under "Unassigned" (not flagged)', () => {
    const traces: Trace[] = [
      // A real task trace → the run HAS task ids.
      makeTrace({ event_source: 'Worker', event_type: 'task_completed',
                  event_context: 'Do the work', task_id: 'T1',
                  output: { content: 'result' } }),
      // A stray trace with no task id under the same agent.
      makeTrace({ event_source: 'Worker', event_type: 'sometool_run',
                  event_context: 'stray activity',
                  output: { tool_name: 'SomeTool' } }),
    ];

    const result = processTraces(traces);
    const agent = result.agents.find(a => a.agent === 'Worker');
    expect(agent).toBeDefined();
    const stray = agent!.tasks.find(t => t.taskName === 'Unassigned');
    // Crew runs still get an "Unassigned" bucket for unattributed traces,
    // and it is NOT flagged (so the UI keeps the task framing there).
    expect(stray).toBeDefined();
    expect(stray!.unassigned).toBe(false);
  });
});
