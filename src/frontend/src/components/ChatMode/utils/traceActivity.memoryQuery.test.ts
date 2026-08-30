import { describe, it, expect } from 'vitest';
import { buildTraceEntry } from './traceActivity';

/**
 * A memory recall's activity entry leads with the QUERY that was run against
 * the store, then the retrieved context — the same `query` key the crew/flow
 * bridge and the chat handlers stamp, so every path reads the same.
 */
describe('buildTraceEntry — memory recall shows its query', () => {
  const retrieved = {
    event_type: 'memory_retrieval',
    event_source: 'Assistant',
    output: {
      content: "[MemoryRecord(id='aaa', content='Aarau rave shooting')]",
      extra_data: { query: 'latest  news from\nSwitzerland', results_count: 1, query_time_ms: 41.2 },
    },
    trace_metadata: { query: 'latest  news from\nSwitzerland', results_count: 1, query_time_ms: 41.2 },
  };

  it('puts the normalised query ahead of the retrieved context', () => {
    const entry = buildTraceEntry('', retrieved)!;
    expect(entry).not.toBeNull();
    expect(entry.label).toBe('Memory');
    expect(entry.detail).toBe(
      "Query: latest news from Switzerland\n\n[MemoryRecord(id='aaa', content='Aarau rave shooting')]",
    );
    expect(entry.durationMs).toBeCloseTo(41.2);
  });

  it('falls back to the bare context for traces written before the query was stamped', () => {
    const entry = buildTraceEntry('', {
      ...retrieved,
      output: { content: 'ctx' },
      trace_metadata: { results_count: 1 },
    })!;
    expect(entry.detail).toBe('ctx');
  });

  it('still hides an empty recall entirely', () => {
    expect(
      buildTraceEntry('', { ...retrieved, output: { content: '[]', extra_data: { query: 'x' } } }),
    ).toBeNull();
  });
});
