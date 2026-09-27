import { apiClient } from '../../shared/api/client';
import { waitForRunResult } from './runResult';

/** The run ID arrives before any LLM work, so its durable trace can be read live. */
export async function generateWithTrace<T>(
  mode: 'crew' | 'flow', request: unknown, onStarted: (jobId: string) => void, signal?: AbortSignal,
): Promise<T> {
  const { data } = await apiClient.post<{ generation_id: string }>(`/builder-generations/${mode}`, request, { signal });
  signal?.throwIfAborted();
  const jobId = data.generation_id;
  onStarted(jobId);
  return waitForBuilderGeneration<T>(jobId, signal);
}

/** Observe an existing durable job without submitting another generation. */
export async function waitForBuilderGeneration<T>(jobId: string, signal?: AbortSignal): Promise<T> {
  return waitForRunResult<T>(jobId, 'builder_result', signal);
}
