import { describe, it, expect } from 'vitest';
import {
  MemoryRecord,
  coOccurrenceEdges,
  extractSavedIds,
  tracesCarryIds,
  deriveIndex,
  extractRecalledIds,
  recordsSavedInRun,
  runWindowFor,
  timeMs,
} from './memoryData';
import { Run } from '../../types/execution/run';

const rec = (over: Partial<MemoryRecord>): MemoryRecord => ({
  id: 'r1',
  content: 'c',
  scope: '/group/g/agent/Researcher/_crew_abc123',
  categories: [],
  importance: 0.5,
  private: false,
  metadata: {},
  created_at: '2026-06-21 13:00:00.123456',
  last_accessed: null,
  ...over,
});

describe('timeMs', () => {
  it('parses Python str(datetime) and ISO forms to the SAME UTC epoch', () => {
    // Space separator + microseconds (memory records) vs ISO (runs) — both naive UTC.
    expect(timeMs('2026-06-21 13:00:00.123456')).toBe(timeMs('2026-06-21T13:00:00.123Z'));
    expect(timeMs('2026-06-21T13:00:00')).toBe(Date.parse('2026-06-21T13:00:00Z'));
  });
  it('returns 0 for missing or invalid input', () => {
    expect(timeMs(null)).toBe(0);
    expect(timeMs('not a date')).toBe(0);
  });
});

describe('deriveIndex', () => {
  it('aggregates categories, agents and co-occurrence symmetrically', () => {
    const idx = deriveIndex([
      rec({ id: 'a', categories: ['MCP Tools', 'browser'], importance: 0.8 }),
      rec({ id: 'b', categories: ['mcp-tools'], importance: 0.4 }),
    ]);
    // 'MCP Tools' and 'mcp-tools' collapse to one normalised key.
    expect(idx.categories.get('mcp-tools')?.count).toBe(2);
    expect(idx.categories.get('browser')?.count).toBe(1);
    expect(idx.coOccurrence.get('mcp-tools')?.get('browser')).toBe(1);
    expect(idx.coOccurrence.get('browser')?.get('mcp-tools')).toBe(1);
    expect(idx.agents.get('Researcher')?.count).toBe(2);
    expect(idx.avgImportance).toBeCloseTo(0.6);
  });
});

describe('coOccurrenceEdges', () => {
  it('emits each undirected pair once', () => {
    const idx = deriveIndex([rec({ categories: ['a', 'b'] })]);
    const edges = coOccurrenceEdges(idx);
    expect(edges).toHaveLength(1);
    expect(edges[0].weight).toBe(1);
  });
});

describe('runWindowFor / recordsSavedInRun', () => {
  // Runs newest-first, as the callers store them.
  const runs = [
    { job_id: 'new', completed_at: '2026-06-21T14:00:00' },
    { job_id: 'old', completed_at: '2026-06-21T12:00:00' },
  ] as unknown as Run[];

  it('windows a run between the previous completion and its own (+buffer)', () => {
    const w = runWindowFor(runs, 'new');
    expect(w).not.toBeNull();
    expect(w!.start).toBe(timeMs('2026-06-21T12:00:00'));
    expect(w!.end).toBeGreaterThan(timeMs('2026-06-21T14:00:00'));
  });

  it('scopes records to the run that wrote them', () => {
    const inNew = rec({ id: 'x', created_at: '2026-06-21 13:30:00' });
    const inOld = rec({ id: 'y', created_at: '2026-06-21 11:30:00' });
    expect(recordsSavedInRun([inNew, inOld], runs, 'new')).toEqual([inNew]);
    expect(recordsSavedInRun([inNew, inOld], runs, 'old')).toEqual([inOld]);
  });

  it('returns everything when the run is unknown (no window)', () => {
    const all = [rec({ id: 'x' }), rec({ id: 'y' })];
    expect(recordsSavedInRun(all, runs, 'missing')).toEqual(all);
  });
});

describe('extractRecalledIds', () => {
  it('prefers structured record_ids (trace_metadata AND output.extra_data)', () => {
    // The trace CONTENT is capped at 8k chars — the tail results' ids fall off
    // (the "trace says 18 reads, pane shows 13" bug). Structured ids are
    // complete regardless of the cap.
    const ids = extractRecalledIds([
      {
        event_type: 'memory_retrieval',
        output: { content: 'truncated…', extra_data: { record_ids: ['aaa', 'bbb'] } },
        trace_metadata: { record_ids: ['ccc'] },
      },
    ]);
    expect([...ids].sort()).toEqual(['aaa', 'bbb', 'ccc']);
  });

  it("collects id='<uuid>' from memory_retrieval traces only", () => {
    const uuid = '123e4567-e89b-12d3-a456-426614174000';
    const ids = extractRecalledIds([
      { event_type: 'memory_retrieval_completed', output: `found id='${uuid}' score=0.9` },
      { event_type: 'llm_call', output: `id='999e4567-e89b-12d3-a456-426614174999'` },
    ]);
    expect([...ids]).toEqual([uuid]);
  });
  it('is empty for undefined traces', () => {
    expect(extractRecalledIds(undefined).size).toBe(0);
  });
});

describe('extractSavedIds', () => {
  it('collects record_id from memory_write traces only', () => {
    const ids = extractSavedIds([
      { event_type: 'memory_write', trace_metadata: { record_id: 'w1' } },
      { event_type: 'memory_write', output: { extra_data: { record_id: 'w2' } } },
      { event_type: 'memory_retrieval', trace_metadata: { record_id: 'nope' } },
    ]);
    expect([...ids].sort()).toEqual(['w1', 'w2']);
  });

  it('is empty for pre-id traces so callers fall back to the time window', () => {
    expect(extractSavedIds([{ event_type: 'memory_write', output: 'saved a thing' }]).size).toBe(0);
  });
});


describe('tracesCarryIds', () => {
  it('detects new-format runs by any id-stamped memory trace', () => {
    expect(
      tracesCarryIds([
        { event_type: 'memory_retrieval', trace_metadata: { record_ids: ['a'] } },
      ]),
    ).toBe(true);
    expect(
      tracesCarryIds([{ event_type: 'memory_write', trace_metadata: { record_id: 'w' } }]),
    ).toBe(true);
  });

  it('is false for old-format traces and non-memory events', () => {
    expect(tracesCarryIds([{ event_type: 'memory_retrieval', output: "id='x'" }])).toBe(false);
    expect(tracesCarryIds([{ event_type: 'llm_call', trace_metadata: { record_id: 'x' } }])).toBe(false);
    expect(tracesCarryIds(undefined)).toBe(false);
  });
});
