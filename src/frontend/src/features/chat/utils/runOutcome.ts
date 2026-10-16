/**
 * A run's real ending, read from its stored row.
 *
 * The chat's live stream is a transport, not the run. When the stream gives up,
 * or the page comes back to a run that ended while nobody was watching, the
 * execution row is what actually happened. /executions/{id} carries the result
 * (and its A2UI surface); /executions/{id}/status does not, which is why the
 * reconnect path used to clear a finished run without ever posting its answer.
 */
import { getExecution } from '../api/executions';
import { extractA2uiSurface, extractResultText } from './resultExtraction';
import type { ChatMessage } from '../types/chat';
import type { Surface } from '../../../shared/a2ui/index';

export type RunOutcome =
  | { kind: 'running' }
  | { kind: 'completed'; text: string; surface: Surface | null }
  | { kind: 'failed'; error: string };

export async function fetchRunOutcome(jobId: string): Promise<RunOutcome> {
  const row = (await getExecution(jobId)) as unknown as Record<string, unknown> | undefined;
  const status = String(row?.status || '').toLowerCase();
  if (status === 'completed') {
    return { kind: 'completed', text: extractResultText(row!), surface: extractA2uiSurface(row!) };
  }
  if (status === 'stopped' || status === 'cancelled') return { kind: 'failed', error: 'Execution stopped' };
  if (status === 'rejected') return { kind: 'failed', error: (row?.error as string) || 'Rejected at an approval gate' };
  if (status === 'failed' || status === 'error') {
    return { kind: 'failed', error: (row?.error as string) || 'Execution failed' };
  }
  return { kind: 'running' };
}

/** Every status a run does not leave (the backend's ExecutionStatus). */
export const TERMINAL_RUN_STATUSES = ['completed', 'failed', 'stopped', 'cancelled', 'rejected', 'error'];

/**
 * Whether the conversation already reports this run's ending.
 *
 * completeExecution stamps the run's id on the message the answer lands in, and
 * failExecution on its "Execution failed" line. The actions bar and trace rows
 * carry the id too, but they are not the ending.
 */
export function conversationHasAnswer(messages: ChatMessage[], jobId: string): boolean {
  return messages.some(
    (m) => m.executionId === jobId && m.resultType !== 'crew_actions' && m.resultType !== 'trace',
  );
}
