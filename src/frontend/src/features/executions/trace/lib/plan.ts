export interface PlanItem {
  id: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | string;
  /**
   * A short title for the step, when the model wrote one.
   *
   * Not part of the tool's schema — models emit it unprompted alongside
   * `content`, and it is exactly what a checklist wants: "Research Databricks
   * in agentic banking" instead of the full sentence. Kept optional and
   * display-only; `content` remains the source of truth.
   */
  label?: string;
}

/**
 * Read a plan out of a trace event, whichever shape it arrived in.
 *
 * Two events carry a plan and they are NOT the same shape:
 *  - `plan_updated` — the engine's own event, clean JSON in
 *    `extra_data.plan_items`.
 *  - the `todo` tool_usage span — `extra_data.tool_args` is a Python **repr**
 *    string (single quotes, `True`/`None`), because that is how tool arguments
 *    are stringified on their way to the trace.
 *
 * The repr is parsed on a best-effort basis: it is display-only, so a failure
 * costs a nicer rendering, never correctness. Everything falls back to the raw
 * JSON view the dialog already has.
 */
export function extractPlanItems(output: unknown): PlanItem[] | null {
  // The RENDERED plan, which is what the `todo` tool returns as its result and
  // therefore the only copy some rows have: "Plan (1/5 completed):" followed by
  // "[x] 1. …" lines. Parsed last-resort but parsed, because otherwise a plan
  // whose JSON never reached the trace shows as its own bracket markers.
  if (typeof output === 'string') {
    return tryParse(output) ?? parseRenderedPlan(output);
  }
  if (typeof output !== 'object' || output === null) return null;
  const record = output as Record<string, unknown>;

  // The light-agent path writes the call's arguments under `input`, not under
  // `extra_data.tool_args` — reading only the latter is why a chat run's plan
  // was invisible on both the timeline row and the step's content.
  const input = record.input;
  if (typeof input === 'string' && input.includes('todos')) {
    const parsed = tryParse(pythonReprToJson(input));
    if (parsed) return parsed;
  }
  if (Array.isArray(record.todos)) return normalise(record.todos);

  // A result envelope: the rendered plan sits in `content`.
  if (typeof record.content === 'string') {
    const fromContent = parseRenderedPlan(record.content);
    if (fromContent) return fromContent;
  }

  const extra = record.extra_data as Record<string, unknown> | undefined;
  if (!extra) return null;

  // Preferred: the engine's structured payload.
  const structured = extra.plan_items;
  if (typeof structured === 'string' && structured.trim()) {
    const parsed = tryParse(structured);
    if (parsed) return parsed;
  }
  if (Array.isArray(structured)) return normalise(structured);

  // Fallback: the todo tool call's arguments, as a Python repr.
  const args = extra.tool_args;
  if (typeof args === 'string' && args.includes('todos')) {
    const parsed = tryParse(pythonReprToJson(args));
    if (parsed) return parsed;
    const wrapped = tryParseObject(pythonReprToJson(args));
    if (wrapped && Array.isArray(wrapped.todos)) return normalise(wrapped.todos);
  }
  return null;
}

/**
 * Python repr → JSON. Display-only, and deliberately conservative.
 *
 * Scanned character by character rather than regex-replaced, because a regex
 * cannot know which quote opened the string it is inside. Python picks the
 * delimiter per string: `'Research the market'`, but `"Research Databricks'
 * role"` the moment the content holds an apostrophe. A pattern matching
 * `'...'` treats that apostrophe as an opening delimiter, swallows the rest of
 * the line and produces JSON that will not parse — so one plan rendered as a
 * checklist and the next fell back to raw JSON, decided by nothing more than
 * whether the model happened to write a possessive.
 */
function pythonReprToJson(input: string): string {
  let out = '';
  let quote: "'" | '"' | null = null;

  for (let i = 0; i < input.length; i += 1) {
    const char = input[i];

    if (quote) {
      if (char === '\\') {
        // Preserve the escape pair as-is; the next char cannot close anything.
        out += char + (input[i + 1] ?? '');
        i += 1;
      } else if (char === quote) {
        out += '"';
        quote = null;
      } else if (char === '"') {
        // A double quote inside a single-quoted string has to be escaped once
        // the delimiter becomes a double quote.
        out += '\\"';
      } else {
        out += char;
      }
      continue;
    }

    if (char === "'" || char === '"') {
      quote = char;
      out += '"';
      continue;
    }

    out += char;
  }

  // Bare Python literals, only ever outside a string now.
  return out
    .replace(/\bNone\b/g, 'null')
    .replace(/\bTrue\b/g, 'true')
    .replace(/\bFalse\b/g, 'false');
}

/** Status markers the rendered plan uses, in the tool's own vocabulary. */
const RENDERED_STATUS: Record<string, string> = {
  x: 'completed',
  X: 'completed',
  '>': 'in_progress',
  '-': 'cancelled',
  '~': 'cancelled',
  ' ': 'pending',
  '': 'pending',
};

/**
 * The `todo` tool's own rendering, back into items.
 *
 * ```
 * Plan (1/5 completed):
 * [x] 1. Create the swiss_tech_companies table
 * [>] 2. Research and gather 300 Swiss tech companies
 * ```
 *
 * Returns null unless at least one line matches, so ordinary prose that happens
 * to contain a bracket is never mistaken for a plan.
 */
function parseRenderedPlan(text: string): PlanItem[] | null {
  const items: PlanItem[] = [];
  for (const line of text.split('\n')) {
    const match = /^\s*\[([^\]]?)\]\s*(?:(\d+)[.)]\s*)?(.+?)\s*$/.exec(line);
    if (!match) continue;
    const content = match[3];
    if (!content) continue;
    items.push({
      id: match[2] ?? String(items.length + 1),
      content,
      status: RENDERED_STATUS[match[1]] ?? 'pending',
    });
  }
  return items.length ? items : null;
}

function tryParse(text: string): PlanItem[] | null {
  try {
    const value = JSON.parse(text);
    if (Array.isArray(value)) return normalise(value);
    if (value && Array.isArray(value.todos)) return normalise(value.todos);
  } catch {
    /* display-only */
  }
  return null;
}

function tryParseObject(text: string): Record<string, unknown> | null {
  try {
    const value = JSON.parse(text);
    return typeof value === 'object' && value !== null ? value : null;
  } catch {
    return null;
  }
}

function normalise(rows: unknown[]): PlanItem[] | null {
  const items = rows
    .filter((row): row is Record<string, unknown> => typeof row === 'object' && row !== null)
    .map((row) => {
      const label = row.label == null ? '' : String(row.label).trim();
      return {
        id: String(row.id ?? ''),
        content: String(row.content ?? ''),
        status: String(row.status ?? 'pending'),
        ...(label ? { label } : {}),
      };
    })
    .filter((item) => item.content);
  return items.length ? items : null;
}
