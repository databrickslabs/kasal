import { apiClient } from '../../shared/api/client';

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
  for (;;) {
    signal?.throwIfAborted();
    const { data: run } = await apiClient.get<{ status: string; result?: { builder_result?: T } | string; error?: string }>(`/executions/${jobId}`, { signal });
    const status = run.status.toUpperCase();
    if (status === 'COMPLETED') {
      const result = typeof run.result === 'string' ? JSON.parse(run.result) : run.result;
      if (!result || !('builder_result' in result)) throw new Error('Generation finished without a result');
      return result.builder_result as T;
    }
    if (['FAILED', 'CANCELLED', 'CANCELED', 'STOPPED'].includes(status)) throw new Error(run.error || 'Generation did not complete');
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => signal?.removeEventListener('abort', abort);
      const timer = setTimeout(() => { cleanup(); resolve(); }, 1000);
      const abort = () => { clearTimeout(timer); cleanup(); reject(signal?.reason || new Error('Generation cancelled')); };
      signal?.addEventListener('abort', abort, { once: true });
      if (signal?.aborted) abort();
    });
  }
}
