import { describe, it, expect } from 'vitest';
import { processTraceEvent } from './traceEventProcessors';
import type { Trace } from '../../types/execution/trace';

const TODOS = JSON.stringify({
  todos: [
    { id: '1', content: 'Create the swiss_tech_companies table', status: 'completed' },
    { id: '2', content: 'Research and gather 300 Swiss tech companies', status: 'in_progress' },
    { id: '3', content: 'Insert companies into the table', status: 'pending' },
  ],
});

const trace = (over: Partial<Trace>): Trace =>
  ({
    id: 1,
    event_source: 'Agent',
    event_context: '',
    event_type: 'tool_usage',
    output: {},
    created_at: '2026-08-18T20:06:31.000',
    ...over,
  } as Trace);

describe('a todo call renders as its plan, not as a tool', () => {
  it('reads the crew path, where the plan rides on tool_args', () => {
    const row = processTraceEvent(
      trace({
        event_type: 'tool_usage',
        output: { extra_data: { tool_name: 'todo', tool_args: TODOS } },
        trace_metadata: { operation: 'tool_started' },
      }),
    );
    expect(row?.type).toBe('plan_updated');
    expect(row?.description).toBe('Plan — 1/3 done · now: Research and gather 300 Swiss tech companies');
  });

  it('reads the light-agent path, where the same JSON is under output.input', () => {
    // This is the shape a CHAT run writes. Reading only tool_args is why a chat
    // run showed a bare "todo" / "todo (output)" pair with no plan in sight.
    const row = processTraceEvent(
      trace({ event_type: 'todo_run', output: { tool_name: 'todo', input: TODOS } }),
    );
    expect(row?.type).toBe('plan_updated');
    expect(row?.description).toContain('Plan — 1/3 done');
  });

  it('still names the row when the plan cannot be parsed', () => {
    const row = processTraceEvent(
      trace({ event_type: 'todo_run', output: { tool_name: 'todo', input: 'not json' } }),
    );
    expect(row?.description).toBe('Plan Updated');
  });

  it('leaves other tools alone', () => {
    const row = processTraceEvent(
      trace({
        output: { extra_data: { tool_name: 'postgres_execute_sql' } },
        trace_metadata: { operation: 'tool_started' },
      }),
    );
    expect(row?.description).toBe('postgres_execute_sql (input)');
  });
});

describe('one plan update draws one line', () => {
  it('collapses the engine event and the todo pair into the latest state', async () => {
    // A single update writes up to three rows — plan_updated, plus the todo
    // call's start and finish. Three identical "Plan — 2/5 done" lines pushed
    // the actual work out of view.
    const { processTraces } = await import('../../hooks/global/useTraceData');
    const at = (s: number) => `2026-08-18T20:06:${String(s).padStart(2, '0')}.000`;
    const processed = processTraces([
      trace({ id: 1, event_type: 'task_started', event_context: 'Do the thing', created_at: at(0) }),
      trace({
        id: 2,
        event_type: 'plan_updated',
        output: { extra_data: { plan_items: TODOS } },
        created_at: at(1),
      }),
      trace({
        id: 3,
        event_type: 'tool_usage',
        output: { extra_data: { tool_name: 'todo', tool_args: TODOS } },
        trace_metadata: { operation: 'tool_started' },
        created_at: at(2),
      }),
      trace({ id: 4, event_type: 'todo_run', output: { tool_name: 'todo', input: TODOS }, created_at: at(3) }),
      trace({
        id: 5,
        event_type: 'tool_usage',
        output: { extra_data: { tool_name: 'postgres_execute_sql' } },
        trace_metadata: { operation: 'tool_started' },
        created_at: at(4),
      }),
    ] as Trace[]);

    const rows = processed.agents.flatMap((a) => a.tasks.flatMap((t) => t.events));
    const plans = rows.filter((r) => r.type === 'plan_updated');
    expect(plans).toHaveLength(1);
    // The last of the run wins: it is the state the next row follows on from.
    expect(plans[0].traceId).toBe(4);
    // …and the work around it is untouched.
    expect(rows.some((r) => r.description === 'postgres_execute_sql (input)')).toBe(true);
  });
});
