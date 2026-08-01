import { describe, it, expect } from 'vitest';
import { processTraces } from './useTraceData';

/**
 * Checkpoint WRITES were classified as run-level — correctly excluded from the
 * agent pass — and then never collected for rendering, so they fell between the
 * two and appeared nowhere. The backend had been writing them the whole time.
 */
const base = { id: 1, created_at: '2026-08-01T23:04:00Z', event_source: 'flow' };

describe('processTraces — checkpoint writes reach the timeline', () => {
  it('emits a flow state checkpoint', () => {
    const { timelineItems } = processTraces([
      { ...base, event_type: 'flow_started' },
      {
        ...base, id: 2, event_type: 'flow_checkpoint_saved',
        output: { extra_data: { method_name: 'turn_end' } },
      },
    ] as never);

    const saved = timelineItems.filter((i) => i.kind === 'checkpoint-saved');
    expect(saved).toHaveLength(1);
    expect((saved[0] as { unit?: string }).unit).toBe('turn_end');
  });

  it('emits a CREW unit checkpoint — the case that showed nothing at all', () => {
    const { timelineItems } = processTraces([
      { ...base, event_type: 'crew_started', event_source: 'crew' },
      {
        ...base, id: 2, event_type: 'checkpoint_unit_saved', event_source: 'crew',
        output: { extra_data: { kind: 'crew', unit_key: 'task-1' } },
      },
    ] as never);

    const saved = timelineItems.filter((i) => i.kind === 'checkpoint-saved');
    expect(saved).toHaveLength(1);
    expect((saved[0] as { unit?: string }).unit).toBe('task-1');
  });

  it('marks a FAILED write, which the run does not otherwise report', () => {
    const { timelineItems } = processTraces([
      {
        ...base, event_type: 'checkpoint_unit_saved',
        output: { extra_data: { unit_key: 'task-1', error: 'db down' } },
      },
    ] as never);

    expect((timelineItems[0] as { failed?: boolean }).failed).toBe(true);
  });

  it('parses output when polling returns the JSON column as a string', () => {
    const { timelineItems } = processTraces([
      {
        ...base, event_type: 'checkpoint_unit_saved',
        output: JSON.stringify({ extra_data: { unit_key: 'task-9' } }),
      },
    ] as never);

    expect((timelineItems[0] as { unit?: string }).unit).toBe('task-9');
  });

  it('does not fold a checkpoint into an agent group', () => {
    // It is bookkeeping ABOUT the run, not a step inside an agent's task.
    const { agents } = processTraces([
      {
        ...base, event_type: 'checkpoint_unit_saved',
        output: { extra_data: { unit_key: 'task-1' } },
      },
    ] as never);

    expect(agents.flatMap((a) => a.traces ?? [])).toHaveLength(0);
  });
});
