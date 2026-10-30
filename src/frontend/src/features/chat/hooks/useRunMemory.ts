/**
 * Data for the chat's memory pane: what one run WROTE to memory and what it
 * RECALLED, derived exactly like the Memory Browser dialog derives it — both
 * consume the shared pure layer in MemoryBackend/memoryData, so the pane and
 * the browser can never disagree on what a run's memory is.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiClient } from '../../../shared/api/client';
import {
  BULK_FETCH,
  DerivedIndex,
  EMPTY_RUN_TRACE_FACTS,
  MemoryRecord,
  RecordsResponse,
  MemoryTrace,
  RunMemoryMode,
  RunTraceFacts,
  coOccurrenceEdges,
  deriveIndex,
  recordsForRun,
  runTraceFacts,
} from '../../memory/components/memoryData';
import { factsForRun } from '../../memory/components/ontologyData';
import type {
  MemoryGraphEntity,
  MemoryGraphFact,
  MemoryGraphResponse,
} from '../../../types/config/memoryBackend';

export type MemoryMode = RunMemoryMode;

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
  /** Set when the teamspace stores a fact graph: the run's facts under the
   *  active mode, and the entities to draw them with. */
  graph: { entities: MemoryGraphEntity[]; facts: MemoryGraphFact[] } | null;
  refresh: () => void;
}

export function useRunMemory(runId: string): RunMemory {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [backend, setBackend] = useState('');
  const [allRecords, setAllRecords] = useState<MemoryRecord[]>([]);
  const [traceFacts, setTraceFacts] = useState<RunTraceFacts>(EMPTY_RUN_TRACE_FACTS);
  const [graphData, setGraphData] = useState<MemoryGraphResponse | null>(null);
  const [mode, setMode] = useState<MemoryMode>('saved');
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        // One bulk read (the graph aggregates everything anyway) and the run's
        // memory traces — the two sources the browser dialog uses, fetched
        // together. The traces are the ONLY thing that scopes records to the
        // run; there is no time window to feed a runs list into.
        const [recordsResp, tracesResp, graphResp] = await Promise.all([
          apiClient.get<RecordsResponse>('/memory-backend/records', {
            params: { limit: BULK_FETCH, offset: 0 },
          }),
          // Only the run's memory_* rows, and ALL of them: the default page is
          // 100 rows oldest-first, and a long run's memory_write rows land
          // last — cut off, the run would look like it saved nothing.
          apiClient
            .get<{ traces?: MemoryTrace[] }>(`/traces/job/${runId}`, {
              params: { limit: 15000, event_type_prefix: 'memory_' },
            })
            .catch(() => ({ data: { traces: [] as MemoryTrace[] } })),
          // Graph memory, when the teamspace uses it. A failure here must not
          // cost the record view, so it reads as "not a graph teamspace".
          apiClient
            .get<MemoryGraphResponse>('/memory-backend/graph', { params: { limit: 2000 } })
            .catch(() => ({ data: null })),
        ]);
        if (cancelled) return;
        setAllRecords(recordsResp.data.records || []);
        setBackend(recordsResp.data.backend || '');
        setTraceFacts(runTraceFacts(tracesResp.data?.traces));
        setGraphData(graphResp.data?.enabled ? graphResp.data : null);
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

  // The saved/recalled rule lives in recordsForRun — shared with the Memory
  // Browser dialog, so the pane and the dialog answer identically.
  const records = useMemo(
    () => recordsForRun(allRecords, mode, traceFacts, runId),
    [allRecords, mode, traceFacts, runId],
  );

  const index = useMemo(() => deriveIndex(records), [records]);
  const edges = useMemo(() => coOccurrenceEdges(index), [index]);
  const refresh = useCallback(() => setReloadKey((k) => k + 1), []);
  const graph = useMemo(
    () =>
      graphData && {
        entities: graphData.entities,
        facts: factsForRun(graphData.facts, mode, traceFacts, runId),
      },
    [graphData, mode, traceFacts, runId],
  );

  return { loading, error, backend, mode, setMode, records, index, edges, graph, refresh };
}
