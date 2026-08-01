/**
 * Trace -> activity-step derivation.
 * 
 * The raw execution trace is a flat event stream; the chat renders it as ordered
 * run steps. Turning one into the other is pure, and it is the part most worth
 * testing on its own.
 */


export interface TraceEntry {
  label: string;
  sublabel?: string;
  durationMs?: number;
  source?: string;
  kind: 'tool_call' | 'tool_result' | 'event';
  detail?: string;
  timestamp: number;
  matchKey?: string;
}

/**
 * Build a stable key that pairs a tool_usage (start) event with its matching
 * `<tool>_run` (result) event, so they can be rendered as a single pill.
 */
export function toolMatchKey(name: unknown, args: unknown): string {
  const n = String(name || '').toLowerCase().replace(/[_\s]/g, '');
  let parsed: Record<string, unknown> = {};
  try {
    if (typeof args === 'string' && args.trim()) {
      parsed = JSON.parse(args.replace(/'/g, '"'));
    } else if (args && typeof args === 'object') {
      parsed = args as Record<string, unknown>;
    }
  } catch { /* ignore */ }
  const vals = Object.values(parsed)
    .filter((v) => typeof v === 'string' || typeof v === 'number')
    .join('|');
  return `${n}::${vals}`;
}

export function summarizeArgs(args: unknown): string | undefined {
  if (!args) return undefined;
  const clip = (s: string): string => (s.length > 80 ? `${s.slice(0, 80)}…` : s);
  let raw: unknown = args;
  if (typeof args === 'string') {
    try {
      raw = JSON.parse(args.replace(/'/g, '"'));
    } catch {
      // A non-JSON string is already a plain value (e.g. a bare query).
      const s = args.trim();
      return s ? clip(s) : undefined;
    }
  }
  // A bare list of strings (e.g. the URLs a reader visited) → "N pages".
  if (Array.isArray(raw)) {
    const items = raw.filter((x) => typeof x === 'string' && String(x).trim()).map((x) => String(x).trim());
    if (items.length === 1) return clip(items[0]);
    if (items.length > 1) return `${items.length} pages`;
    return undefined;
  }
  if (!raw || typeof raw !== 'object') return undefined;
  const parsed = raw as Record<string, unknown>;
  // Surface the ONE meaningful field a human cares about — the query / question /
  // topic / url — instead of dumping EVERY argument value as a CSV (which reads as
  // ", 10, 30, Switzerland news today, CH, moderate, …" to a non-technical user).
  const lower: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(parsed)) lower[k.toLowerCase()] = v;
  const PREFERRED = ['query', 'search_query', 'searchquery', 'q', 'question', 'prompt', 'search', 'topic', 'text', 'url', 'urls', 'website_url', 'task'];
  for (const key of PREFERRED) {
    const v = lower[key];
    if (typeof v === 'string' && v.trim()) return clip(v.trim());
    if (Array.isArray(v)) {
      const items = v.filter((x) => typeof x === 'string' && String(x).trim()).map((x) => String(x).trim());
      if (items.length === 1) return clip(items[0]);
      if (items.length > 1) return `${items.length} pages`;
    }
  }
  // Fallback: the single longest string value (the substantive one), not a CSV.
  const strings = Object.values(parsed).filter((v) => typeof v === 'string' && (v as string).trim()) as string[];
  if (!strings.length) return undefined;
  return clip(strings.reduce((a, b) => (b.length > a.length ? b : a)).trim());
}

/**
 * Turn a raw SSE trace event into a clean entry for the chat. Returns null
 * when the event is pure noise (LLM retries, token fragments, internal IDs).
 */
export function buildTraceEntry(
  message: string,
  data?: Record<string, unknown>,
): TraceEntry | null {
  const eventType = (data?.event_type as string) || '';
  const eventSource = (data?.event_source as string) || '';
  // `output` (and its nested `extra_data`) is a JSON column. Over the SSE path
  // (local dev) it arrives as a dict; over the REST polling fallback on
  // Postgres/asyncpg (Lakebase, deployed) the same column comes back as a JSON
  // STRING. Parse strings so memory + tool content render identically on both —
  // otherwise polled traces (the only transport on Databricks Apps, where SSE
  // is dead) would show no memory/tool results.
  const asObject = (v: unknown): Record<string, unknown> => {
    if (typeof v === 'string') {
      try {
        const parsed = JSON.parse(v);
        return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {};
      } catch {
        return {};
      }
    }
    return (v as Record<string, unknown>) || {};
  };
  const output = asObject(data?.output);
  const extra = asObject(output.extra_data);
  const metadata = asObject(data?.trace_metadata);
  const num = (o: Record<string, unknown>, k: string): number | undefined =>
    typeof o[k] === 'number' ? (o[k] as number) : undefined;
  // Prefer the backend's explicit duration; for MEMORY recall the real time lives
  // in trace_metadata (query_time_ms / retrieval_time_ms), matching the Job-History
  // trace. Without this, long memory reads (10–16s) showed as 0.0s in the timeline.
  const durationMs =
    num(output, 'duration_ms')
    ?? num(metadata, 'duration_ms')
    ?? num(metadata, 'query_time_ms')
    ?? num(metadata, 'retrieval_time_ms')
    ?? num(metadata, 'save_time_ms');
  const now = Date.now();

  // Hard-filter known noise events.
  if (eventType === 'llm_retry' || eventType === 'task_started') return null;

  // Tool/MCP failures (e.g. HTTP 403 connecting to a selected MCP server).
  // The run continues without those tools, so surface the error in the
  // activity instead of leaving it buried in backend logs.
  if (eventType === 'tool_error') {
    const errorText =
      (typeof output.content === 'string' && output.content.trim()) ||
      (typeof output.error === 'string' && output.error.trim()) ||
      message.trim() ||
      'Tool error';
    return {
      kind: 'event',
      label: errorText.length > 80 ? `⚠ ${errorText.slice(0, 77)}…` : `⚠ ${errorText}`,
      detail: errorText.length > 80 ? errorText : undefined,
      source: eventSource || undefined,
      timestamp: now,
    };
  }

  // Tool invocation (start of a call).
  if (eventType === 'tool_usage') {
    const toolName = (extra.tool_name as string) || 'tool';
    const rawArgs = (extra.tool_args as string) || '';
    return {
      kind: 'tool_call',
      label: toolName,
      sublabel: summarizeArgs(rawArgs),
      source: eventSource || undefined,
      detail: rawArgs || undefined,
      timestamp: now,
      matchKey: toolMatchKey(toolName, rawArgs),
    };
  }

  // Tool result event — backend names them like `perplexitytool_run`,
  // `scrapewebsitetool_run`, etc.
  if (eventType.endsWith('_run')) {
    const toolName = (output.tool_name as string) || eventType.replace(/_run$/, '');
    const content = typeof output.content === 'string' ? (output.content as string) : '';
    const input = output.input;
    return {
      kind: 'tool_result',
      label: toolName,
      sublabel: summarizeArgs(input),
      durationMs,
      source: eventSource || undefined,
      detail: content || undefined,
      timestamp: now,
      matchKey: toolMatchKey(toolName, input),
    };
  }

  // Memory retrieval — surface the RETRIEVED memories (the context), not just a
  // "searching…" ping. CrewAI's recall emits these with the matched memories in
  // output.content; without this branch they fall to the generic handler below
  // and the context is hidden (or dropped as JSON noise). The matching "Search
  // memory" tool result still shows the empty case on its own.
  if (eventType === 'memory_retrieval' || eventType === 'memory_retrieval_completed') {
    const content = typeof output.content === 'string' ? (output.content as string).trim() : '';
    const foundNothing = !content || /no relevant memories|no memories found|^\[\]$/i.test(content);
    if (foundNothing) return null; // nothing retrieved — don't add a redundant pill
    // For memory recall the REAL time is the query/retrieval time in metadata —
    // output.duration_ms is a tiny unrelated value, so it must NOT win here (that
    // made long recalls show 0.0s). Matches the Job-History trace's "Memory Read".
    const memoryDurationMs =
      num(metadata, 'query_time_ms') ?? num(metadata, 'retrieval_time_ms') ?? durationMs;
    return {
      kind: 'tool_result',
      // Same label so consecutive recalls group under one "Memory" line.
      label: 'Memory',
      sublabel: 'context retrieved',
      durationMs: memoryDurationMs,
      source: eventSource || undefined,
      detail: content,
      timestamp: now,
    };
  }

  // Checkpoint bookkeeping. These carry no text content at all — everything is
  // in `extra_data` — so without an explicit arm they reach the generic branch
  // below, find an empty message, and are dropped. The Jobs timeline renders
  // them; the chat activity silently did not.
  //
  // Worth showing for the same reason as in Jobs: "nothing was written" and "it
  // was written and ignored" are the two cases you most need to tell apart when
  // a resume does not pick up where it left off. A RESTORED crew matters more
  // still — it is a crew whose answer was reused instead of re-run, and the
  // timeline would otherwise imply it never happened.
  if (eventType === 'flow_checkpoint_saved') {
    const where = (extra.method_name as string) || (extra.crew_name as string) || '';
    return {
      kind: 'event',
      label: 'Checkpoint saved',
      sublabel: where || undefined,
      durationMs,
      source: eventSource || undefined,
      timestamp: now,
    };
  }
  if (eventType === 'crew_checkpoint_restored' || eventType === 'task_checkpoint_restored') {
    const name = (extra.crew_name as string) || (metadata.crew_name as string) || eventSource;
    return {
      kind: 'event',
      label: 'Restored from an earlier turn',
      sublabel: name || undefined,
      durationMs,
      source: eventSource || undefined,
      timestamp: now,
    };
  }

  // Strip raw JSON dumps and single-token fragments that have no useful label.
  const trimmed = message.trim();
  if (!trimmed) return null;
  // Raw JSON event payload — useless without parsing.
  if (trimmed.startsWith('{') && /"id":\s*\d+/.test(trimmed)) return null;
  // Single-word / sub-token fragments from agent streaming ("_usage", "s.", "Read").
  if (trimmed.length < 12 && !/\s/.test(trimmed)) return null;
  // Generic "Calling tools." status pings.
  if (/^calling tool/i.test(trimmed) && trimmed.length <= 30) return null;

  return {
    kind: 'event',
    label: trimmed.length > 80 ? trimmed.slice(0, 80) + '…' : trimmed,
    detail: trimmed.length > 80 ? trimmed : undefined,
    timestamp: now,
  };
}

/**
 * Rebuild the run-activity steps from the DURABLE execution traces (the
 * /traces/job rows), running each through {@link buildTraceEntry} — the SAME
 * mapping the live SSE path uses. This lets a refreshed session restore the full
 * tool context (memory recalls, SQL/Genie result tables, …) straight from the
 * database, independent of whatever the per-message copy retained.
 */
export function tracesToRunSteps(
  traces: { id?: number; event_type?: string; output?: unknown; trace_metadata?: unknown; event_source?: string }[],
): { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] {
  // Keep EVERY labeled kind (tool_result / event / tool_call) — anything the
  // user watched stream by must survive the restore. Unlike the live message
  // path (where a pending tool_call is PROMOTED in place to its result), the
  // durable trace rows keep BOTH the tool_usage start row and its `*_run`
  // result row — so suppress a tool_call whose label also has a result row,
  // keeping only genuinely dangling calls (tool started, no result recorded).
  const entries: { entry: NonNullable<ReturnType<typeof buildTraceEntry>>; id: string }[] = [];
  traces.forEach((t, idx) => {
    const entry = buildTraceEntry('', t as unknown as Record<string, unknown>);
    if (!entry || !entry.label) return;
    entries.push({ entry, id: `trace-${t.id ?? idx}` });
  });
  const resultLabels = new Set(
    entries.filter(({ entry }) => entry.kind === 'tool_result').map(({ entry }) => entry.label),
  );
  const steps: { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] = [];
  entries.forEach(({ entry, id }, idx) => {
    if (entry.kind === 'tool_call' && resultLabels.has(entry.label)) return;
    steps.push({
      id,
      label: entry.label,
      sublabel: entry.sublabel,
      detail: entry.detail,
      durationMs: entry.durationMs,
      timestamp: idx,
    });
  });
  return steps;
}

/**
 * The LATEST run segment's activity steps from the persistent chat trace
 * messages (everything after the last user message). Shows EVERY labeled step —
 * tool results AND tool calls / events — with no pruning/dedup: the user wants
 * each step the agent ran, even when two look similar (repeated memory recalls
 * or Genie queries sharing a SQL prefix), and anything shown while the run
 * streamed must not vanish when it ends (a pending tool_call message is
 * promoted in place to its result, so a call and its result never appear twice).
 */
export function deriveMessageActivitySteps(
  messages: { id?: string; role: string; resultType?: string; resultData?: unknown }[],
): { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] {
  let start = 0;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === 'user') { start = i + 1; break; }
  }
  const steps: { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] = [];
  messages.slice(start).forEach((m, idx) => {
    if (m.resultType !== 'trace') return;
    const t = m.resultData as Partial<TraceEntry> | undefined;
    if (!t || !t.label) return;
    steps.push({
      id: m.id || `step-${idx}`,
      label: t.label,
      sublabel: t.sublabel,
      detail: t.detail,
      durationMs: t.durationMs,
      timestamp: t.timestamp ?? idx,
    });
  });
  return steps;
}

/**
 * Which step list the activity views show: the durable DB-restored steps when
 * available (complete + survives a refresh), else the per-message steps (the
 * live source during a run) — but NEVER a swap that SHRINKS the visible list.
 * The messages hold everything the user watched stream by, so if the restored
 * rows reproduce fewer steps, the richer message-derived set wins.
 */
export function pickRunActivitySteps<T>(restored: T[] | undefined, messageSteps: T[]): T[] {
  if (!restored || !restored.length) return messageSteps;
  return restored.length >= messageSteps.length ? restored : messageSteps;
}

/**
 * Distill a task output into a concise chat-message body. Returns null when
 * the output is pure progress-noise that shouldn't appear in chat.
 */
/**
 * Produce a concise, human-friendly label for a task-output chat message.
 *
 * Traces report the task *name* as the full (interpolated) task description, so
 * a refine surfaces the entire "Improve the artifact below… CURRENT ARTIFACT:
 * <!DOCTYPE html>…" prompt — which dumps the whole artifact into the chat. We
 * special-case the refine prompt to "Refined artifact" and otherwise collapse
 * any over-long description to its first line, truncated.
 */
