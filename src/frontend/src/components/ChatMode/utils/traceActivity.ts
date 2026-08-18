/**
 * Trace -> activity-step derivation.
 * 
 * The raw execution trace is a flat event stream; the chat renders it as ordered
 * run steps. Turning one into the other is pure, and it is the part most worth
 * testing on its own.
 */

import { REASONING_VISIBLE_MODELS, isRedactedReasoning } from '../../Common/ReasoningPanel';

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

  // The model's reasoning/thinking. The backend splits it out of the answer
  // (core/llm/transport/response_parsing.split_message_content) so it never
  // lands in task output or memory — this is where it surfaces in the chat.
  // Rendered as a step whose `detail` holds the text, which the activity list
  // already shows collapsed behind a click, matching tool results.
  //
  // Only some models populate it (databricks-inkling, kimi-k2-7-code today).
  // Claude Fable 5 / Opus 5 send a reasoning block whose text Databricks
  // redacts, so they correctly produce nothing here.
  if (eventType === 'llm_call' || eventType === 'llm_response') {
    const reasoning = typeof extra.reasoning === 'string' ? extra.reasoning.trim() : '';
    if (isRedactedReasoning(reasoning)) {
      // The model DID reason; Anthropic on Databricks encrypted the trace. Say
      // that rather than dropping the step, which would imply no thinking
      // happened — and never render the sentinel itself.
      return {
        kind: 'event',
        label: 'Reasoning',
        sublabel: 'hidden by provider',
        detail:
          'This model reasoned before answering but returned only an encrypted '
          + 'signature, with no thinking text.\n\n'
          + 'For Anthropic Claude this is usually fixable: enable Extended '
          + 'Thinking on the model in Settings. Claude only returns thinking text '
          + 'when the request asks for it — `display` defaults to "omitted" on '
          + 'Claude 5, Fable 5, Opus 4.7 and Opus 4.8, which "returns thinking '
          + 'blocks with an empty thinking field":\n'
          + '  https://platform.claude.com/docs/en/build-with-claude/thinking'
          + '#controlling-thinking-display\n'
          + 'With it enabled, Kasal opts in and the summary comes back. No '
          + 'setting returns the raw chain of thought — it is always a summary.\n\n'
          + 'The GPT-5 family is different: it reasons but the trace is '
          + 'unobtainable. "While reasoning tokens are not visible via the API, '
          + 'they still occupy space in the model\'s context window and are '
          + 'billed as output tokens":\n'
          + '  https://developers.openai.com/api/docs/guides/reasoning\n'
          + 'Summaries exist only on the Responses API, which this endpoint does '
          + 'not expose.\n\n'
          + 'Reasoning is visible without any configuration on:\n'
          + REASONING_VISIBLE_MODELS.map((m) => `  • ${m}`).join('\n')
          + '\nLlama, Qwen and Gemma do not reason.',
        durationMs,
        source: eventSource || undefined,
        timestamp: now,
      };
    }
    if (reasoning) {
      return {
        kind: 'event',
        label: 'Reasoning',
        sublabel: `${reasoning.length.toLocaleString()} chars`,
        detail: reasoning,
        durationMs,
        source: eventSource || undefined,
        timestamp: now,
      };
    }
    // No reasoning on this row — fall through to the existing handling.
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
  if (eventType === 'checkpoint_unit_saved') {
    // A completed UNIT written to the run's checkpoint — a task for a crew, a
    // crew for a flow. Crews write these too; only the flow path used to show
    // anything, so a crew run's timeline claimed no checkpointing happened.
    const failed = typeof extra.error === 'string' && extra.error;
    return {
      kind: 'event',
      label: failed ? 'Checkpoint failed' : 'Checkpoint saved',
      sublabel: (extra.unit_key as string) || (extra.kind as string) || undefined,
      detail: failed || undefined,
      durationMs,
      source: eventSource || undefined,
      timestamp: now,
    };
  }
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
