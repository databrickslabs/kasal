import { describe, it, expect } from 'vitest';
import {
  EMPTY_RUN_TRACE_FACTS,
  MemoryRecord,
  coOccurrenceEdges,
  contentMatches,
  extractSavedContents,
  extractSavedIds,
  deriveIndex,
  extractRecalledIds,
  isConsolidation,
  recordsForRun,
  runTraceFacts,
  timeMs,
} from './memoryData';

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

describe('extractSavedContents', () => {
  it('collects the written bodies from memory_write traces, whitespace-normalised', () => {
    const bodies = extractSavedContents([
      // crew/flow bridge: content + a `value` mirror of the same text
      {
        event_type: 'memory_write',
        output: { content: 'Swiss News Report —\n\nAarau shooting, five injured.' },
        trace_metadata: { value: 'Swiss News Report — Aarau shooting, five injured.' },
      },
      // chat path: capped body carries the truncation marker
      { event_type: 'memory_write', output: { content: 'User: create a presentation on…[truncated]' } },
      // not a write; too short to mean anything
      { event_type: 'memory_retrieval', output: { content: 'Swiss News Report — Aarau shooting, five injured.' } },
      { event_type: 'memory_write', output: { content: 'ok' } },
    ]);
    expect(bodies).toEqual([
      'Swiss News Report — Aarau shooting, five injured.',
      'User: create a presentation on',
    ]);
  });
  it('is empty for no traces', () => {
    expect(extractSavedContents(undefined)).toEqual([]);
  });
});

describe('contentMatches', () => {
  const saved = ['Swiss News Report — Aarau shooting, five injured. Federal Council'];
  it('matches a record whose body is the traced text, or a capped prefix of it', () => {
    expect(contentMatches(rec({ content: 'Swiss News Report — Aarau shooting, five injured. Federal Council' }), saved)).toBe(true);
    // The trace copy is capped; the stored record runs on.
    expect(contentMatches(rec({ content: 'Swiss News Report — Aarau shooting, five injured. Federal Council kept quotas' }), saved)).toBe(true);
    expect(contentMatches(rec({ content: 'Swiss News Report —\n\nAarau shooting, five injured. Federal Council' }), saved)).toBe(true);
  });
  it('rejects other bodies and anything too short to be evidence', () => {
    expect(contentMatches(rec({ content: 'Rony Fahed is a Lebanese former basketball player' }), saved)).toBe(false);
    expect(contentMatches(rec({ content: 'Swiss News' }), saved)).toBe(false);
  });
});

describe('runTraceFacts', () => {
  it('reads recalled ids, saved ids and saved bodies from one trace list', () => {
    const facts = runTraceFacts([
      { event_type: 'memory_retrieval', trace_metadata: { record_ids: ['a'] } },
      { event_type: 'memory_write', trace_metadata: { record_id: 'w' }, output: { content: 'The Federal Council kept the 2026 quotas unchanged.' } },
    ]);
    expect([...facts.recalledIds]).toEqual(['a']);
    expect([...facts.savedIds]).toEqual(['w']);
    expect(facts.savedContents).toEqual(['The Federal Council kept the 2026 quotas unchanged.']);
  });
  it('is the empty shape for no traces', () => {
    expect(runTraceFacts(undefined)).toEqual(EMPTY_RUN_TRACE_FACTS);
  });
});

describe('recordsForRun', () => {
  const wrote = rec({
    id: 'w1',
    created_at: '2026-06-21 11:59:00',
    source: 'crew_task',
    content: 'Swiss News Report — Aarau shooting, five injured. Federal Council kept quotas.',
  });
  const merged = rec({
    id: 'm1',
    created_at: '2026-06-21 11:59:30',
    source: 'consolidation',
    metadata: { merged_from: 2 },
    content: 'Rony Fahed is a Lebanese former professional basketball player.',
  });
  const chatMeanwhile = rec({
    id: 'c1',
    created_at: '2026-06-21 11:59:45',
    source: 'chat',
    content: 'User: create a presentation on the latest news from switzerland',
  });
  const store = [wrote, merged, chatMeanwhile];

  it("saved = exactly the ids the run's memory_write traces carry", () => {
    const facts = runTraceFacts([{ event_type: 'memory_write', trace_metadata: { record_id: 'w1' } }]);
    expect(recordsForRun(store, 'saved', facts)).toEqual([wrote]);
  });

  it('saved excludes consolidation output even when its write is traced under the run', () => {
    // End-of-run maintenance re-saves MERGED records under the run that
    // triggered it — the "Rony Fahed" record in a run about Swiss news.
    const facts = runTraceFacts([
      { event_type: 'memory_write', trace_metadata: { record_id: 'w1' } },
      { event_type: 'memory_write', trace_metadata: { record_id: 'm1' } },
    ]);
    expect(recordsForRun(store, 'saved', facts)).toEqual([wrote]);
  });

  it('saved is EMPTY when the run has written nothing — never other runs\' records', () => {
    // A run that has only just started, or whose recalls all came back
    // empty, has no id-stamped trace. It must NOT inherit what chat wrote in
    // the meantime (the "2 records · presentation creation" phantom).
    expect(recordsForRun(store, 'saved', EMPTY_RUN_TRACE_FACTS)).toEqual([]);
    const emptyRecall = runTraceFacts([
      { event_type: 'memory_retrieval', trace_metadata: { results_count: 0 } },
    ]);
    expect(recordsForRun(store, 'saved', emptyRecall)).toEqual([]);
  });

  it('saved falls back to the traced body for runs written before the id stamps — minus consolidation', () => {
    const facts = runTraceFacts([
      { event_type: 'memory_write', output: { content: 'Swiss News Report — Aarau shooting, five injured.' } },
      { event_type: 'memory_write', output: { content: 'Rony Fahed is a Lebanese former professional basketball player.' } },
    ]);
    expect(recordsForRun(store, 'saved', facts)).toEqual([wrote]);
  });

  it('saved includes a record stamped with this run\'s execution_id, even with no traces', () => {
    const stamped = rec({
      id: 's1',
      source: 'chat',
      metadata: { execution_id: 'run-9' },
      content: 'User: hi there Assistant: Hi! How can I help you today?',
    });
    expect(recordsForRun([...store, stamped], 'saved', EMPTY_RUN_TRACE_FACTS, 'run-9')).toEqual([stamped]);
    expect(recordsForRun([...store, stamped], 'saved', EMPTY_RUN_TRACE_FACTS, 'other')).toEqual([]);
  });

  it('recalled = the ids the retrieval traces carry, empty when none', () => {
    const facts = runTraceFacts([{ event_type: 'memory_retrieval', trace_metadata: { record_ids: ['c1', 'm1'] } }]);
    expect(recordsForRun(store, 'recalled', facts)).toEqual([merged, chatMeanwhile]);
    expect(recordsForRun(store, 'recalled', EMPTY_RUN_TRACE_FACTS)).toEqual([]);
  });
});

describe('isConsolidation', () => {
  it("keys off the record's own source, case-insensitively", () => {
    expect(isConsolidation(rec({ source: 'consolidation' }))).toBe(true);
    expect(isConsolidation(rec({ source: 'Consolidation' }))).toBe(true);
    expect(isConsolidation(rec({ source: 'crew_task' }))).toBe(false);
    expect(isConsolidation(rec({ source: null }))).toBe(false);
  });
});
