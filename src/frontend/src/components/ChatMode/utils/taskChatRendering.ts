/**
 * Presenting a crew task in the chat: its label, header and output summary.
 * 
 * These already had a test file at this path (``taskChatRendering.test.ts``) while
 * the code itself still sat in ChatWorkspace.tsx — the module the test was written
 * against never existed. It does now.
 */
import { extractDocSummary } from './surfaceAdapter';
import type { PreviewContent } from '../components/Preview/PreviewPanel';
import { stripEmbeddedUiDocument } from './resultExtraction';

export function cleanTaskLabel(taskName: string): string {
  const name = (taskName || '').trim();
  if (!name) return 'Task';
  // The refine editor task description always starts with this sentinel.
  if (/^Improve the artifact below based on this instruction/i.test(name)) {
    return 'Refined artifact';
  }
  const firstLine = name.split('\n')[0].trim();
  return firstLine.length > 80 ? `${firstLine.slice(0, 80).trim()}…` : firstLine;
}

/**
 * The label announcing who is about to work, shown BEFORE their tokens stream.
 *
 * Prefers the agent's role (`event_source`) — "Presentation Lead" — because the
 * trace's task *name* is the fully interpolated task description, and using it
 * put 80 characters of prompt where a name belongs. Falls back to the collapsed
 * description when no role is attributed.
 */
export function taskHeaderLabel(agentRole: string, taskName: string): string {
  const role = (agentRole || '').trim();
  // Some sources report a module/class rather than a person-ish role; those read
  // worse than the task line, so only take a role that looks like one.
  if (role && role.length <= 60 && !/^(crew|kasal|engine|system)$/i.test(role)) {
    return role;
  }
  const fromTask = cleanTaskLabel(taskName);
  return fromTask === 'Task' ? '' : fromTask;
}

/**
 * True when a task's "output" is really its own description echoed back.
 *
 * Weak models restate the brief instead of answering, and the raw echo arrives
 * as a Python-ish repr — `description='…\\n\\nUS…'` — which rendered in chat as
 * the task title followed by the same text again, escapes and all. It carries
 * no information the header above it doesn't already give.
 */
export function isEchoedTaskSpec(output: string, taskName: string): boolean {
  const trimmed = output.trim();
  if (!trimmed) return false;
  // A repr of the task/agent object rather than an answer.
  if (/^(description|expected_output|raw|task|agent)\s*=\s*['"]/i.test(trimmed)) return true;
  if (!taskName) return false;
  // Or literally the task description (or its opening) restated as the answer.
  const head = taskName.trim().slice(0, 60).toLowerCase();
  return head.length >= 20 && trimmed.slice(0, 200).toLowerCase().includes(head);
}

/**
 * Turn literal escape sequences into real characters.
 *
 * Output that reaches chat via a repr shows "\\n\\n" as visible backslash-n
 * rather than a paragraph break.
 */
export function unescapeLiterals(text: string): string {
  if (!text.includes('\\n') && !text.includes('\\t') && !text.includes('\\"')) return text;
  return text
    .replace(/\\r\\n/g, '\n')
    .replace(/\\n/g, '\n')
    .replace(/\\t/g, '\t')
    .replace(/\\"/g, '"')
    .replace(/\\'/g, "'");
}

export function summarizeTaskOutput(
  raw: string,
  preview: PreviewContent | null,
  taskName = '',
): string | null {
  const trimmed = unescapeLiterals(raw).trim();
  if (!trimmed) return null;

  // The task restating itself is not an answer — the header already named it.
  if (isEchoedTaskSpec(trimmed, taskName)) return null;

  // Status noise like "Calling tools.", "Thinking...", "Using tool X" — skip.
  if (
    trimmed.length <= 120 &&
    /^(calling tool|thinking|processing|searching|using tool|executing|agent (started|thinking|finished)|tool call)/i.test(trimmed)
  ) {
    return null;
  }

  if (preview) {
    // Prefer the model's own one-liner (top-level "summary" in the A2UI doc);
    // fall back to the generic line when it didn't supply one.
    return extractDocSummary(trimmed) || 'Generated an app. View it in the preview pane.';
  }

  // Belt-and-suspenders: even if the surface wasn't extracted as a preview,
  // NEVER dump raw A2UI JSON into the chat. Strip any embedded UI document and
  // keep only the surrounding prose; if the doc markers still remain (it didn't
  // parse cleanly), collapse to the friendly line instead of the JSON blob.
  if (trimmed.includes('createSurface') || trimmed.includes('updateComponents')) {
    const prose = stripEmbeddedUiDocument(trimmed);
    if (prose.includes('createSurface') || prose.includes('updateComponents')) {
      return 'Generated an app. View it in the preview pane.';
    }
    return preview_(prose);
  }

  // Long plain-text outputs get collapsed too, otherwise they take over the chat.
  return preview_(trimmed);
}

/**
 * A step's output longer than this is shown as a preview rather than in full.
 *
 * It used to be 400, cut to 300 characters — about two sentences, which reads
 * as a truncated answer rather than a summary of a step. Two things have
 * changed since that number was chosen:
 *
 * - A runaway generation is bounded before it reaches here: every model config
 *   declares `max_output_tokens`, so the 197KB output this cap was really
 *   defending against cannot be produced in the first place. (A transport-side
 *   repetition detector briefly did this job and was removed — no LLM client
 *   ships one, and it could not see the drifting case anyway.)
 * - The FINAL answer is no longer affected at all: completion folds the full
 *   text into this message (`executionStore.supersedeTruncatedTail`), so what
 *   the cap now trims is only an INTERMEDIATE step in a multi-crew flow.
 *
 * An intermediate step should be readable — that is the entire reason it is
 * shown — so the budget is a few paragraphs rather than two sentences. It stays
 * bounded because several verbose steps still should not bury the conversation.
 */
const PREVIEW_TRIGGER = 2400;

/** How much of an over-long output to keep. */
const PREVIEW_CHARS = 2000;

/** Full text, or its opening with an ellipsis when it runs long. */
function preview_(text: string): string {
  if (text.length <= PREVIEW_TRIGGER) return text;
  return `${text.slice(0, PREVIEW_CHARS).trim()}…`;
}

/**
 * Strip an embedded A2UI document (createSurface/updateComponents payload)
 * from a final-answer text. The surface is rendered in the preview pane —
 * dumping its raw JSON into the chat hard-confuses business users. Keeps the
 * surrounding prose; falls back to a friendly line when nothing else remains.
 */
