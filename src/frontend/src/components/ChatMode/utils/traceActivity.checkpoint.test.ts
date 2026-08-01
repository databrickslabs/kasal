import { describe, it, expect } from 'vitest';
import { buildTraceEntry } from './traceActivity';

/**
 * Checkpoint traces carry no text content — everything is in `extra_data` — so
 * they used to reach the generic branch, find an empty message and be dropped.
 * The Jobs timeline rendered them; the chat activity silently did not.
 */
describe('buildTraceEntry — checkpoint bookkeeping', () => {
  const saved = {
    event_type: 'flow_checkpoint_saved',
    event_source: 'flow',
    output: {
      duration_ms: 0.1,
      extra_data: { crew_name: 'agentic ai frameworks', method_name: 'turn_end' },
    },
  };

  it('renders a checkpoint WRITE (it used to vanish)', () => {
    // The message is empty, exactly as the backend sends it.
    const entry = buildTraceEntry('', saved);

    expect(entry).not.toBeNull();
    expect(entry!.label).toBe('Checkpoint saved');
    expect(entry!.sublabel).toBe('turn_end');
  });

  it('renders it when the trace arrived over polling, where output is a JSON string', () => {
    // The REST fallback returns the JSON column as a string; both transports
    // must produce the same entry or the deployed app shows less than dev.
    const entry = buildTraceEntry('', {
      ...saved,
      output: JSON.stringify(saved.output),
    });

    expect(entry?.label).toBe('Checkpoint saved');
  });

  it('names the crew whose answer was reused rather than re-run', () => {
    const entry = buildTraceEntry('', {
      event_type: 'crew_checkpoint_restored',
      event_source: 'flow',
      output: { extra_data: { crew_name: 'agentic ai frameworks' } },
    });

    expect(entry?.label).toBe('Restored from an earlier turn');
    expect(entry?.sublabel).toBe('agentic ai frameworks');
  });

  it('still drops genuinely empty traces', () => {
    expect(buildTraceEntry('', { event_type: 'something_else' })).toBeNull();
  });
});
