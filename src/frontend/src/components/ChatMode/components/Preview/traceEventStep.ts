/**
 * One trace event, as the preview pane's step.
 *
 * The pane's detail view is keyed on {@link RunStep}; the timeline's rows are
 * `TraceEvent`s. This is the adapter between them, and deliberately the only
 * place the two models meet — including the decision about which SHAPE the
 * payload is (a plan, code, or prose), which `StepContent` then renders.
 */
import { extractPlanItems, type PlanItem } from '../../../Jobs/TracePlanView';
import type { TraceEvent } from '../../../../types/execution/trace';

/**
 * One step of a run, as the preview pane's master→detail view consumes it.
 *
 * Lives beside the adapter that builds it: it used to have its own module, left
 * over from when the chat derived a step list of its own. The derivation is
 * gone — the activity is the run's trace now — and a file holding one interface
 * under the name of a component that no longer exists is a wrong signpost.
 */
export interface RunStep {
  id: string;
  label: string;
  sublabel?: string;
  detail?: string;
  durationMs?: number;
  timestamp?: number;
  /** A `todo` / `plan_updated` step's checklist, when the event carried one —
   *  rendered as a checklist rather than as the raw payload it arrived in. */
  plan?: PlanItem[];
  /** The step's payload when it is code (a SQL statement, a JSON envelope) —
   *  shown as a code block instead of being flattened into prose lines. */
  code?: { language: string; text: string };
}

/**
 * The event's output as text.
 *
 * Traces read from the API carry the STORED output (not the 500-char pipe
 * preview the live SSE frame carries), so what the pane shows is the whole
 * thing. An object is pretty-printed rather than `[object Object]`.
 */
function outputText(output: TraceEvent['output']): string {
  if (output == null) return '';
  if (typeof output === 'string') {
    // The string itself may be the envelope (Postgres/asyncpg returns the JSON
    // column as text), in which case the prose inside is what to show.
    return unwrapEnvelope(output) ?? output;
  }
  const unwrapped = unwrapEnvelope(output);
  if (unwrapped !== null) return unwrapped;
  try {
    return JSON.stringify(output, null, 2);
  } catch {
    return String(output);
  }
}

/** Keys whose value IS the payload — the SQL a tool ran, the code it executed. */
const CODE_KEYS: Record<string, string> = {
  sql: 'sql',
  statement: 'sql',
  code: 'python',
  script: 'bash',
  command: 'bash',
};

/**
 * Keys that are just an envelope around the real answer.
 *
 * A tool that reads pages returns `{"result": "…## Sources…"}`. Rendered as
 * JSON that is a single enormous line with literal `\n` escapes in it — the
 * markdown inside is the thing worth reading, and the wrapper says nothing.
 */
const ENVELOPE_KEYS = new Set(['result', 'output', 'content', 'text', 'answer', 'response', 'message']);

/** The prose inside a single-string envelope, if that is all this payload is. */
function unwrapEnvelope(value: unknown): string | null {
  const parsed = parseMaybe(value);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
  const entries = Object.entries(parsed as Record<string, unknown>);
  if (entries.length !== 1) return null;
  const [key, inner] = entries[0];
  return ENVELOPE_KEYS.has(key) && typeof inner === 'string' ? inner : null;
}

function parseMaybe(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return value;
  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
}

/**
 * The step's payload as CODE, when it is code.
 *
 * A tool call's arguments arrive as a JSON envelope around the thing that
 * actually ran — `{"sql": "CREATE TABLE …"}`. Unwrapping a single-string
 * argument gives back the statement itself, which is what someone reading the
 * step wants to see; anything else is shown as formatted JSON. Prose is left
 * alone: it is markdown far more often than not, and belongs in the reader.
 */
export function codeFromTraceEvent(event: TraceEvent): { language: string; text: string } | null {
  const raw = event.extraData?.tool_args ?? event.extraData?.args;
  const args = parseMaybe(raw);
  if (args && typeof args === 'object' && !Array.isArray(args)) {
    const entries = Object.entries(args as Record<string, unknown>);
    const single = entries.length === 1 ? entries[0] : undefined;
    if (single && typeof single[1] === 'string' && CODE_KEYS[single[0]]) {
      return { language: CODE_KEYS[single[0]], text: single[1] };
    }
    return { language: 'json', text: JSON.stringify(args, null, 2) };
  }

  // An envelope around prose is not code — its contents are, more often than
  // not, markdown.
  if (unwrapEnvelope(event.output) !== null) return null;

  const output = parseMaybe(event.output);
  if (output && typeof output === 'object') {
    return { language: 'json', text: JSON.stringify(output, null, 2) };
  }
  return null;
}

export function traceEventToRunStep(event: TraceEvent): RunStep {
  // A plan arrives either as the engine's own `plan_items` or as the `todo`
  // tool's Python-repr arguments. Both are read by the same extractor the trace
  // dialog uses, so a checklist renders as a checklist on either surface —
  // rather than as the bracketed "[>] 1. …" text the payload literally holds.
  // `output` may be the rendered plan as a STRING — the todo tool's own result
  // — so it is offered to the extractor whatever shape it arrived in.
  const plan =
    extractPlanItems({ extra_data: event.extraData }) ?? extractPlanItems(event.output);

  // A plan wins over the code view: it IS the payload, read properly.
  const code = plan ? null : codeFromTraceEvent(event);

  return {
    ...(plan ? { plan } : {}),
    ...(code ? { code } : {}),
    id: event.traceId != null ? `trace-${event.traceId}` : `${event.type}-${event.timestamp.getTime()}`,
    label: event.description,
    detail: outputText(event.output),
    // The measured op time when there is one (a memory query, an MCP call);
    // the row's wall-time slice otherwise.
    durationMs: event.intrinsicMs ?? event.duration,
    timestamp: event.timestamp.getTime(),
  };
}
