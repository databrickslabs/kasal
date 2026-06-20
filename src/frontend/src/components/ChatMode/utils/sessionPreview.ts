/**
 * Derive the chat preview pane's content on demand from each run's stored
 * execution result — the single source of truth — instead of a separately
 * persisted preview copy.
 *
 * Every run stamps its `executionId` onto its chat message (see
 * executionStore.completeExecution). Here we walk those runs' stored
 * `execution.result` for an A2UI document (findUiDocument, the same recursive
 * detection Job History's "Show Result" uses) and hand the EXTRACTED, top-level
 * document to the preview pane — so the deliverable survives navigating away
 * mid-run, and the pane's Customize / Look / refine / download behave exactly as
 * they do for a freshly-streamed result.
 */
import { getExecution } from '../api/executions';
import { findUiDocument } from './uiDocument';
import type { ChatMessage } from '../types/chat';
import type { PreviewContent } from '../components/Preview/PreviewPanel';

// Page-session cache: executionId -> stored result (or null when missing).
// Results can be large, so switching sessions back and forth must not re-fetch.
const resultCache = new Map<string, unknown>();

async function fetchResult(jobId: string): Promise<unknown> {
  if (resultCache.has(jobId)) return resultCache.get(jobId) ?? null;
  let result: unknown = null;
  try {
    const exec = await getExecution(jobId);
    result = (exec as { result?: unknown })?.result ?? null;
  } catch {
    result = null; // best-effort — a missing/failed fetch just yields no preview
  }
  resultCache.set(jobId, result);
  return result;
}

/** Convert an extracted A2UI document node into a renderable preview entry. */
function toPreview(node: string | Record<string, unknown>): PreviewContent {
  return {
    type: 'ui',
    data: typeof node === 'string' ? node : JSON.stringify(node),
  };
}

/**
 * Build the preview history (and current = latest) for a session by deriving
 * each run's deliverable from its execution result. Lazy: only fetches the runs
 * present in THESE messages, and only once (cached). Runs without an A2UI
 * deliverable are skipped.
 */
export async function deriveSessionPreviews(
  messages: ChatMessage[],
): Promise<{ history: PreviewContent[]; current: PreviewContent | null }> {
  const jobIds: string[] = [];
  for (const m of messages) {
    if (m.executionId && !jobIds.includes(m.executionId)) jobIds.push(m.executionId);
  }

  const history: PreviewContent[] = [];
  for (const jobId of jobIds) {
    const result = await fetchResult(jobId);
    if (!result) continue;
    const node = findUiDocument(result);
    if (node) history.push(toPreview(node));
  }

  return { history, current: history.length ? history[history.length - 1] : null };
}
