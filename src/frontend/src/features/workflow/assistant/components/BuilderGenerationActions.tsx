import { useEffect, useState } from 'react';
import { apiClient } from '../../../../shared/api/client';
import { useMLflowEnabled } from '../../../../hooks/global/useMLflowEnabled';
import MLflowRunAction from '../../../chat/components/Cards/MLflowRunAction';

/** Planning has an activity record, but no workload result card. */
export default function BuilderGenerationActions({ jobId }: { jobId: string }) {
  const enabled = useMLflowEnabled();
  const [generation, setGeneration] = useState(false);
  useEffect(() => {
    setGeneration(false);
    if (!enabled) return;
    const controller = new AbortController();
    void apiClient.get<{ result?: { builder_result?: unknown }; status: string }>(`/executions/${jobId}`, { signal: controller.signal }).then(({ data }) => {
      if (!controller.signal.aborted) setGeneration(Boolean(data.result?.builder_result) && data.status.toLowerCase() === 'completed');
    }).catch(() => { /* The activity remains usable without a tracing destination. */ });
    return () => controller.abort();
  }, [jobId, enabled]);
  return generation ? <div className="flex mt-2"><MLflowRunAction executionId={jobId} /></div> : null;
}
