/**
 * Pure data layer shared by the Memory Browser dialog (MemoryRecordsBrowser)
 * and the ChatMode memory pane: record/index types, timestamp normalisation,
 * run windowing, concept aggregation and graph derivation. No React, no MUI —
 * everything here is a plain function so both hosts stay in lock-step on what
 * a run's memory IS while styling it their own way.
 */
export interface MemoryRecord {
  id: string | null;
  content: string;
  scope: string;
  categories: string[];
  importance: number;
  source?: string | null;
  private: boolean;
  metadata: Record<string, unknown>;
  created_at: string | null;
  last_accessed: string | null;
}

export interface RecordsResponse {
  backend: string;
  records: MemoryRecord[];
  count: number;
  // Total records available in the store for the active scope. The browser
  // pages through them with `offset` until loaded === total.
  total?: number;
}

// The concept/graph views need the WHOLE store at once (they aggregate every
// record into a single visualization that re-runs an expensive force
// simulation on each data change). So they fetch the entire remainder in ONE
// request — one round-trip, one simulation — capped at this many records.
export const BULK_FETCH = 5000;

/**
 * Parse a timestamp to epoch ms; 0 when missing/invalid.
 *
 * Memory records come from Python `str(datetime)` ("2026-06-21 13:00:00.123456"
 * — space separator + microseconds, which `Date.parse` rejects) while run
 * timestamps are ISO ("2026-06-21T13:02:00"). Both are naive UTC. We normalize
 * both to comparable UTC epochs: space→T, trim fractional seconds to ms, and
 * append 'Z' when no timezone is present (treat as UTC).
 */
export const timeMs = (iso: string | null | undefined): number => {
  if (!iso) return 0;
  let s = iso.trim().replace(' ', 'T').replace(/(\.\d{3})\d+/, '$1');
  const hasTz = /[Zz]$/.test(s) || /[+-]\d{2}:?\d{2}$/.test(s);
  if (!hasTz) s += 'Z';
  const t = Date.parse(s);
  return Number.isNaN(t) ? 0 : t;
};

/** Normalise a category label so variants collapse to the same key. */
export const normalizeCategory = (raw: string): string =>
  raw
    .toLowerCase()
    .replace(/[_\s]+/g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .trim();

/** Extract the agent name from a scope path of the form .../agent/<name>/... */
export const parseAgentFromScope = (scope: string): string | null => {
  const match = scope.match(/\/agent\/([^/]+)/);
  return match ? match[1] : null;
};

/**
 * Which agent wrote this record.
 *
 * The writing agent is stamped into `metadata.agent_role` — the scope path
 * stays the tenant boundary because the Databricks backend filters records on
 * an EXACT scope match. Older records only have the scope form, so fall back
 * to parsing it.
 */
export const recordAgent = (record: MemoryRecord): string | null => {
  const role = record.metadata?.agent_role;
  if (typeof role === 'string' && role.trim()) return role.trim();
  return parseAgentFromScope(record.scope);
};

/** Extract the crew hash from a scope path of the form .../_crew_<hash>... */
export const parseCrewFromScope = (scope: string): string | null => {
  const match = scope.match(/_crew_([0-9a-f]+)/i);
  return match ? match[1] : null;
};

export const formatRelative = (iso: string | null): string => {
  if (!iso) return '—';
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const diff = Date.now() - then;
  const min = 60_000;
  const hr = 60 * min;
  const day = 24 * hr;
  if (diff < min) return 'just now';
  if (diff < hr) return `${Math.round(diff / min)}m ago`;
  if (diff < day) return `${Math.round(diff / hr)}h ago`;
  if (diff < 7 * day) return `${Math.round(diff / day)}d ago`;
  return new Date(iso).toLocaleDateString();
};

export interface CategoryStat {
  key: string;          // normalised slug
  label: string;        // canonical display form (most common variant)
  variants: Set<string>;
  count: number;
  recordIds: Set<string>;
  totalImportance: number;
  avgImportance: number;
}

export interface AgentStat {
  name: string;
  count: number;
  crews: Set<string>;
  totalImportance: number;
  avgImportance: number;
}

export interface DerivedIndex {
  categories: Map<string, CategoryStat>;
  agents: Map<string, AgentStat>;
  crews: Map<string, number>;
  coOccurrence: Map<string, Map<string, number>>;
  avgImportance: number;
}

export function deriveIndex(records: MemoryRecord[]): DerivedIndex {
  const categories = new Map<string, CategoryStat>();
  const agents = new Map<string, AgentStat>();
  const crews = new Map<string, number>();
  const coOccurrence = new Map<string, Map<string, number>>();
  let totalImportance = 0;

  for (const record of records) {
    totalImportance += record.importance;
    const crew = parseCrewFromScope(record.scope);
    const agent = recordAgent(record);

    if (crew) {
      crews.set(crew, (crews.get(crew) ?? 0) + 1);
    }
    if (agent) {
      const stat = agents.get(agent) ?? {
        name: agent,
        count: 0,
        crews: new Set<string>(),
        totalImportance: 0,
        avgImportance: 0,
      };
      stat.count += 1;
      stat.totalImportance += record.importance;
      if (crew) stat.crews.add(crew);
      stat.avgImportance = stat.totalImportance / stat.count;
      agents.set(agent, stat);
    }

    const normalisedInRecord = new Set<string>();
    for (const raw of record.categories ?? []) {
      const key = normalizeCategory(raw);
      if (!key) continue;
      normalisedInRecord.add(key);
      const stat = categories.get(key) ?? {
        key,
        label: raw,
        variants: new Set<string>(),
        count: 0,
        recordIds: new Set<string>(),
        totalImportance: 0,
        avgImportance: 0,
      };
      stat.variants.add(raw);
      stat.count += 1;
      stat.totalImportance += record.importance;
      if (record.id) stat.recordIds.add(record.id);
      // Pick the most frequent raw form as the canonical label.
      if ([...stat.variants].length === 1 || raw.length < stat.label.length) {
        stat.label = raw;
      }
      stat.avgImportance = stat.totalImportance / stat.count;
      categories.set(key, stat);
    }

    // Build symmetric co-occurrence counts.
    const keys = [...normalisedInRecord];
    for (let i = 0; i < keys.length; i += 1) {
      for (let j = i + 1; j < keys.length; j += 1) {
        const [a, b] = [keys[i], keys[j]];
        const mapA = coOccurrence.get(a) ?? new Map<string, number>();
        mapA.set(b, (mapA.get(b) ?? 0) + 1);
        coOccurrence.set(a, mapA);
        const mapB = coOccurrence.get(b) ?? new Map<string, number>();
        mapB.set(a, (mapB.get(a) ?? 0) + 1);
        coOccurrence.set(b, mapB);
      }
    }
  }

  return {
    categories,
    agents,
    crews,
    coOccurrence,
    avgImportance: records.length ? totalImportance / records.length : 0,
  };
}

export const importanceColor = (v: number): string => {
  if (v >= 0.75) return '#6366f1'; // indigo — high
  if (v >= 0.6)  return '#3b82f6'; // blue
  if (v >= 0.45) return '#06b6d4'; // cyan
  return '#94a3b8';                // slate — low
};

export interface MemoryTrace {
  event_type?: string;
  output?: unknown;
  trace_metadata?: Record<string, unknown> | null;
}

/** Structured id lists a trace may carry (trace_metadata and/or output.extra_data). */
function structuredIds(tr: MemoryTrace, key: 'record_ids' | 'record_id'): string[] {
  const out: string[] = [];
  const sources: unknown[] = [
    (tr.trace_metadata as Record<string, unknown> | null | undefined)?.[key],
    (tr.output as { extra_data?: Record<string, unknown> } | null | undefined)?.extra_data?.[key],
  ];
  for (const v of sources) {
    if (typeof v === 'string') out.push(v);
    else if (Array.isArray(v)) for (const x of v) if (typeof x === 'string') out.push(x);
  }
  return out;
}

/**
 * Record ids a run RECALLED. Prefers the STRUCTURED `record_ids` the backend
 * stamps on memory_retrieval traces; falls back to parsing id='<uuid>' out of
 * the trace content for traces written before that existed. The fallback
 * UNDERCOUNTS on purpose-capped content (the trace text is truncated at 8k
 * chars, cutting the tail results' ids) — which is why structured ids win.
 */
export function extractRecalledIds(traces: MemoryTrace[] | undefined): Set<string> {
  const ids = new Set<string>();
  for (const tr of traces || []) {
    if (!/memory_retrieval|memory_query/.test(tr.event_type || '')) continue;
    for (const id of structuredIds(tr, 'record_ids')) ids.add(id);
    const text =
      typeof tr.output === 'string' ? tr.output : JSON.stringify(tr.output ?? '');
    for (const m of text.matchAll(/id='([0-9a-fA-F-]{36})'/g)) ids.add(m[1]);
  }
  return ids;
}

/**
 * Record ids a run WROTE, from the `record_id` its memory_write traces carry.
 * Old traces (before the id was stamped) yield an empty set — callers fall
 * back to the completed_at time window then.
 */
export function extractSavedIds(traces: MemoryTrace[] | undefined): Set<string> {
  const ids = new Set<string>();
  for (const tr of traces || []) {
    if (!/memory_write|memory_save/.test(tr.event_type || '')) continue;
    for (const id of structuredIds(tr, 'record_id')) ids.add(id);
  }
  return ids;
}

/**
 * Whitespace-normalised text, the form both a stored record and the trace
 * copy of it are compared in (the write path collapses whitespace; a trace
 * body may not).
 */
const normalizeContent = (text: unknown): string =>
  typeof text === 'string' ? text.replace(/\s+/g, ' ').trim() : '';

/** Shortest text worth matching on — below this "same prefix" means nothing. */
const MIN_CONTENT_MATCH_CHARS = 24;

/** Markers the trace writers append when they cap a body. */
const TRUNCATION_MARKERS = /(…\[truncated\]|\.\.\.|…)$/;

/**
 * The bodies a run's memory_write traces recorded — for runs traced BEFORE
 * the record-id stamps existed, the only exact evidence of what they wrote.
 * Read from the trace content and the bridge's `value` mirror; capped copies
 * (4k / 8k chars) are matched as prefixes by contentMatches.
 */
export function extractSavedContents(traces: MemoryTrace[] | undefined): string[] {
  const out = new Set<string>();
  for (const tr of traces || []) {
    if (!/memory_write|memory_save/.test(tr.event_type || '')) continue;
    const output = tr.output as { content?: unknown; extra_data?: Record<string, unknown> } | null;
    const candidates: unknown[] = [
      typeof tr.output === 'string' ? tr.output : output?.content,
      output?.extra_data?.value,
      (tr.trace_metadata as Record<string, unknown> | null | undefined)?.value,
    ];
    for (const c of candidates) {
      const text = normalizeContent(c).replace(TRUNCATION_MARKERS, '').trim();
      if (text.length >= MIN_CONTENT_MATCH_CHARS) out.add(text);
    }
  }
  return [...out];
}

/** A record whose body is (a capped prefix of) one the run's write traces recorded. */
export function contentMatches(record: MemoryRecord, savedContents: string[]): boolean {
  const body = normalizeContent(record.content);
  if (body.length < MIN_CONTENT_MATCH_CHARS) return false;
  return savedContents.some((saved) => {
    const n = Math.min(body.length, saved.length);
    return body.slice(0, n) === saved.slice(0, n);
  });
}

/** What a run's traces establish about its memory, read once from one fetch. */
export interface RunTraceFacts {
  /** Ids the run's memory_retrieval traces carry. */
  recalledIds: Set<string>;
  /** Ids the run's memory_write traces carry (runs traced after the id stamps). */
  savedIds: Set<string>;
  /** Bodies the run's memory_write traces recorded (the pre-id evidence). */
  savedContents: string[];
}

export const EMPTY_RUN_TRACE_FACTS: RunTraceFacts = {
  recalledIds: new Set(),
  savedIds: new Set(),
  savedContents: [],
};

export function runTraceFacts(traces: MemoryTrace[] | undefined): RunTraceFacts {
  return {
    recalledIds: extractRecalledIds(traces),
    savedIds: extractSavedIds(traces),
    savedContents: extractSavedContents(traces),
  };
}

/**
 * Maintenance output, not something the run wrote. End-of-run consolidation
 * (chat, agent builder and flow builder all schedule it) re-saves MERGED
 * records under the run that happened to trigger it — the memory_write trace
 * is real, so the id lands in `savedIds`, but the content spans other runs.
 * "What this run saved" excludes it; the record's own provenance is the
 * authority, which keeps the rule identical across all three paths.
 */
export const isConsolidation = (r: MemoryRecord): boolean =>
  (r.source || '').toLowerCase() === 'consolidation';

export type RunMemoryMode = 'saved' | 'recalled';

/**
 * A record that names the execution that wrote it — the strongest evidence
 * there is. Chat stamps `execution_id` on its records' metadata; crew/flow
 * task outputs are gaining the same stamp. Consolidation output never has it.
 */
export const writtenByRun = (r: MemoryRecord, runId: string | undefined): boolean =>
  Boolean(runId) && r.metadata?.execution_id === runId;

/**
 * Records scoped to ONE run under a mode — the single place both the chat
 * memory pane and the Memory Browser dialog derive it, so they cannot disagree.
 *
 * recalled: records whose ids the run's memory_retrieval traces carry.
 * saved:    ONLY what the evidence proves the run wrote — a record stamped
 *           with this run's execution_id, a record id its memory_write traces
 *           carry, or (runs traced before the id stamps) the recorded body.
 *           No such evidence means nothing saved, and that shows EMPTY: a run
 *           that has only just started, recalled nothing, or runs without
 *           memory must not inherit other runs' records. (A completed_at time
 *           window used to fill that gap; for the oldest run in a workspace it
 *           was the entire store, and for a running one, whatever chat wrote
 *           meanwhile.)
 */
export function recordsForRun(
  records: MemoryRecord[],
  mode: RunMemoryMode,
  facts: RunTraceFacts,
  runId?: string,
): MemoryRecord[] {
  if (mode === 'recalled') {
    if (facts.recalledIds.size === 0) return [];
    return records.filter((r) => r.id && facts.recalledIds.has(r.id));
  }
  const byId = (r: MemoryRecord) => Boolean(r.id && facts.savedIds.has(r.id));
  const byBody = (r: MemoryRecord) =>
    facts.savedIds.size === 0 && contentMatches(r, facts.savedContents);
  return records.filter(
    (r) => !isConsolidation(r) && (writtenByRun(r, runId) || byId(r) || byBody(r)),
  );
}

export function coOccurrenceEdges(
  index: DerivedIndex,
): { source: string; target: string; weight: number }[] {
  const seen = new Set<string>();
  const out: { source: string; target: string; weight: number }[] = [];
  for (const [src, map] of index.coOccurrence.entries()) {
    for (const [dst, weight] of map.entries()) {
      const key = src < dst ? `${src}|${dst}` : `${dst}|${src}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ source: src, target: dst, weight });
    }
  }
  return out;
}
