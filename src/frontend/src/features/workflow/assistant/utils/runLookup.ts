import { apiClient } from '../../../../shared/api/client';

/**
 * True only when the run no longer exists for this workspace (HTTP 404): it was
 * deleted, or belongs to another workspace. The run lookup the builder polls
 * returns null for a 404 and for a failed request alike, so a deleted run used
 * to keep the composer locked while the poll asked about it every 3 s forever.
 * Any other failure answers false: a transient error must not end a run.
 */
export async function isRunGone(jobId: string): Promise<boolean> {
  try {
    await apiClient.get(`/executions/${encodeURIComponent(jobId)}/status`);
    return false;
  } catch (error) {
    return (error as { response?: { status?: number } })?.response?.status === 404;
  }
}
