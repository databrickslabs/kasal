import { useEffect, type MutableRefObject } from 'react';
import { runService } from '../../../api/execution/ExecutionHistoryService';
import UCMVResultViewer, { type UCMVResult } from './UCMVResultViewer';

interface Props {
  result: Parameters<typeof UCMVResultViewer>[0]['result'];
  jobId: string;
  savedJobRef: MutableRefObject<string | null>;
  onSave: (result: UCMVResult) => Promise<void>;
}

/** Persist each displayed job once so later flow crews can read its metric view. */
export function UCMVResultWithAutoSave({ result, jobId, savedJobRef, onSave }: Props) {
  useEffect(() => {
    if (!jobId || savedJobRef.current === jobId) return;
    savedJobRef.current = jobId;
    runService.updateExecutionResult(jobId, result as unknown as Record<string, unknown>)
      .then(() => console.log('[ShowResult] Auto-saved UCMV result for validator'))
      .catch(err => console.warn('[ShowResult] Auto-save UCMV failed:', err));
  }, [jobId, result, savedJobRef]);

  return <UCMVResultViewer result={result} editable={!!jobId} onSave={jobId ? onSave : undefined} />;
}
