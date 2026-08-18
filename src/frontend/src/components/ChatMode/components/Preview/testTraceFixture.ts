/**
 * A small `ProcessedTraces` for the tests of the surfaces that render one.
 *
 * Shaped like a real run — one agent, one task, an LLM call and a tool pair —
 * so a test asserts on what the timeline actually shows: the event descriptions,
 * not a narration of them.
 */
import type { ProcessedTraces, TraceEvent } from '../../../../types/execution/trace';

export function makeEvent(overrides: Partial<TraceEvent> = {}): TraceEvent {
  return {
    type: 'tool_result',
    description: 'postgres_execute_sql (output)',
    timestamp: new Date('2026-08-18T20:06:35.000Z'),
    traceId: 1,
    duration: 1800,
    output: 'CREATE TABLE',
    ...overrides,
  };
}

export function makeProcessedTraces(
  events: TraceEvent[] = [
    makeEvent({ type: 'llm', description: 'LLM Request — KAT-Coder (3,585 chars)', traceId: 1, duration: 3500 }),
    makeEvent({ type: 'tool_result', description: 'postgres_execute_sql (output)', traceId: 2 }),
  ],
  overrides: Partial<ProcessedTraces> = {},
): ProcessedTraces {
  const start = new Date('2026-08-18T20:06:31.000Z');
  const end = new Date('2026-08-18T20:09:43.000Z');
  return {
    globalStart: start,
    globalEnd: end,
    totalDuration: end.getTime() - start.getTime(),
    agents: [
      {
        agent: 'Database Researcher and Data Storage Specialist',
        startTime: start,
        endTime: end,
        duration: end.getTime() - start.getTime(),
        tasks: [
          {
            taskName: 'Research and collect 300 Swiss tech companies',
            fullDescription:
              'Research and collect 300 Swiss tech companies from Switzerland using web sources, design a PostgreSQL schema…',
            startTime: start,
            endTime: end,
            duration: end.getTime() - start.getTime(),
            events,
          },
        ],
      },
    ],
    globalEvents: { start: [], end: [] },
    crewSections: [{ agentIdxs: [0] }],
    timelineItems: [{ kind: 'agent', agentIdx: 0, nested: false }],
    ...overrides,
  };
}
