/**
 * Data for the chat's memory pane: what one run WROTE to memory and what it
 * RECALLED, derived exactly like the Memory Browser dialog derives it — both
 * consume the shared pure layer in MemoryBackend/memoryData, so the pane and
 * the browser can never disagree on what a run's memory is.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiClient } from '../../../config/api/ApiConfig';
import { runService } from '../../../api/execution/ExecutionHistoryService';
import { Run } from '../../../types/execution/run';
import {
  BULK_FETCH,
  DerivedIndex,
  MemoryRecord,
  RecordsResponse,
  MemoryTrace,
  coOccurrenceEdges,
  deriveIndex,
  extractRecalledIds,
  extractSavedIds,
  recordsSavedInRun,
  timeMs,
} from '../../MemoryBackend/memoryData';

export type MemoryMode = 'saved' | 'recalled';

export interface RunMemory {
  loading: boolean;
  error: string | null;
  backend: string;
  mode: MemoryMode;
  setMode: (m: MemoryMode) => void;
  /** Records scoped to the run under the active mode. */
  records: MemoryRecord[];
  index: DerivedIndex;
  edges: { source: string; target: string; weight: number }[];
  refresh: () => void;
}

export function useRunMemory(runId: string): RunMemory {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [backend, setBackend] = useState('');
  const [allRecords, setAllRecords] = useState<MemoryRecord[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [recalledIds, setRecalledIds] = useState<Set<string>>(new Set());
  const [savedIds, setSavedIds] = useState<Set<string>>(new Set());
  const [mode, setMode] = useState<MemoryMode>('saved');
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        // One bulk read (the graph aggregates everything anyway), the runs list
        // for the run's time window, and the run's retrieval traces — the three
        // sources the browser dialog uses, fetched together.
        runService.invalidateRunsCache();
        const [recordsResp, runsResp, tracesResp] = await Promise.all([
          apiClient.get<RecordsResponse>('/memory-backend/records', {
            params: { limit: BULK_FETCH, offset: 0 },
          }),
          runService.getRuns(100),
          apiClient
            .get<{ traces?: MemoryTrace[] }>(`/traces/job/${runId}`)
            .catch(() => ({ data: { traces: [] as MemoryTrace[] } })),
        ]);
        if (cancelled) return;
        setAllRecords(recordsResp.data.records || []);
        setBackend(recordsResp.data.backend || '');
        // Newest first — runWindowFor scans FORWARD from the run's index to
        // find the nearest older completed run, so the order is load-bearing.
        setRuns(
          [...(runsResp?.runs ?? [])].sort(
            (a, b) => timeMs(b.created_at) - timeMs(a.created_at),
          ),
        );
        setRecalledIds(extractRecalledIds(tracesResp.data?.traces));
        setSavedIds(extractSavedIds(tracesResp.data?.traces));
      } catch (err: unknown) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
          setAllRecords([]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [runId, reloadKey]);

  const records = useMemo(() => {
    if (mode === 'recalled') {
      if (recalledIds.size === 0) return [];
      return allRecords.filter((r) => r.id && recalledIds.has(r.id));
    }
    // Saved: exact when the run's memory_write traces carry record ids;
    // otherwise (older runs, pre-id traces) fall back to the completed_at
    // time window — which mis-scopes when chat runs overlap, so ids win.
    if (savedIds.size > 0) {
      return allRecords.filter((r) => r.id && savedIds.has(r.id));
    }
    return recordsSavedInRun(allRecords, runs, runId);
  }, [allRecords, runs, runId, mode, recalledIds, savedIds]);

  const index = useMemo(() => deriveIndex(records), [records]);
  const edges = useMemo(() => coOccurrenceEdges(index), [index]);
  const refresh = useCallback(() => setReloadKey((k) => k + 1), []);

  return { loading, error, backend, mode, setMode, records, index, edges, refresh };
}
