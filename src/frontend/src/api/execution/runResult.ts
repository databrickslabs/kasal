import { apiClient } from '../../shared/api/client';

const FAILED = ['FAILED', 'CANCELLED', 'CANCELED', 'STOPPED'];

/**
 * Poll a run started in the background until it ends; its answer is
 * `result[key]`. The run's job id arrives before any LLM work, so the caller
 * can read the run's durable trace live while this waits — builder turns
 * (`builder_result`) and skill drafts (`skill_draft`) both work this way.
 */
export async function waitForRunResult<T>(jobId: string, key: string, signal?: AbortSignal): Promise<T> {
  for (;;) {
    signal?.throwIfAborted();
    const { data: run } = await apiClient.get<{ status: string; result?: Record<string, unknown> | string; error?: string }>(`/executions/${jobId}`, { signal });
    const status = run.status.toUpperCase();
    if (status === 'COMPLETED') {
      const result = typeof run.result === 'string' ? JSON.parse(run.result) : run.result;
      if (!result || !(key in result)) throw new Error('Generation finished without a result');
      return result[key] as T;
    }
    if (FAILED.includes(status)) throw new Error(run.error || 'Generation did not complete');
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => signal?.removeEventListener('abort', abort);
      const timer = setTimeout(() => { cleanup(); resolve(); }, 1000);
      const abort = () => { clearTimeout(timer); cleanup(); reject(signal?.reason || new Error('Generation cancelled')); };
      signal?.addEventListener('abort', abort, { once: true });
      if (signal?.aborted) abort();
    });
  }
}
