/**
 * Derive the chat preview pane's content on demand from each run's stored
 * execution result — the single source of truth — instead of a separately
 * persisted preview copy.
 *
 * Every run stamps its `executionId` onto its chat message (see
 * executionStore.completeExecution). Here we walk those runs' stored
 * `execution.result` for a renderable A2UI surface (toSurface — the same boundary
 * Job History's "Show Result" uses, which accepts the new {text,a2ui} envelope, a
 * bare surface, or an older legacy doc) and hand it to the preview pane — so the
 * deliverable survives navigating away mid-run, and the pane's Customize / Look /
 * refine / download behave exactly as they do for a freshly-streamed result.
 */
import { getExecution } from '../api/executions';
import { toSurface } from './surfaceAdapter';
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


/**
 * Build the preview history (and current = latest) for a session by deriving
 * each run's deliverable from its execution result. Lazy: only fetches the runs
 * present in THESE messages, and only once (cached). Runs without an A2UI
 * deliverable are skipped.
 */
export async function deriveSessionPreviews(
  messages: ChatMessage[],
): Promise<{ history: PreviewContent[]; current: PreviewContent | null }> {
  const seen = new Set<string>();
  const history: PreviewContent[] = [];
  for (const m of messages) {
    // Prefer the surface persisted ON the message (`resultData` round-trips
    // through the session API and carries any user restyle `theme`) over the
    // pristine execution.result — otherwise a "Customize → Look" palette is
    // lost on every session switch. Gated on the a2ui card type so crew cards /
    // trace payloads are never mistaken for a deliverable.
    const local = m.resultType === 'a2ui' && m.resultData != null ? toSurface(m.resultData) : null;
    if (m.executionId) {
      if (seen.has(m.executionId)) continue;
      seen.add(m.executionId);
      let surface = local;
      if (!surface) {
        const result = await fetchResult(m.executionId);
        if (result) surface = toSurface(result);
      }
      if (surface) {
        history.push({ type: 'ui', data: JSON.stringify(surface), sourceMessageId: m.id });
      }
    } else if (local) {
      // An a2ui card with NO run anchor: an envelope-clobbered row (an old
      // partial update stripped executionId). Its surface — including any
      // restyle theme — is still the deliverable; don't drop it.
      history.push({ type: 'ui', data: JSON.stringify(local), sourceMessageId: m.id });
    }
  }

  return { history, current: history.length ? history[history.length - 1] : null };
}
