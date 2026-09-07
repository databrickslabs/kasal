import { useCallback, useEffect, useMemo, useState } from 'react';
import { runService, type Run } from '../../../api/execution/ExecutionHistoryService';
import { useGroupStore } from '../../../store/groups';
import { useRunStatusStore } from '../../../store/runStatus';

/** Fetch linked executions directly so older session runs are not lost to global pagination. */
export function useScopedRuns(jobIds?: string[]) {
  const groupId = useGroupStore(state => state.currentGroupId);
  const liveRuns = useRunStatusStore(state => state.runHistory);
  const key = JSON.stringify(jobIds);
  const [snapshot, setSnapshot] = useState<{ key: string | undefined; groupId: string | null; runs: Run[]; error: string | null }>({ key: undefined, groupId: null, runs: [], error: null });
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(async () => { setRevision(value => value + 1); }, []);
  useEffect(() => {
    if (key === undefined || !groupId) return;
    let cancelled = false;
    const ids = JSON.parse(key) as string[];
    setLoading(ids.length > 0);
    const fetch = async () => {
      const runs: Run[] = [];
      let failed = false;
      // Bound concurrency when a long-lived session contains many runs.
      for (let index = 0; index < ids.length && !cancelled; index += 6) {
        const results = await Promise.allSettled(ids.slice(index, index + 6).map(id => runService.getRunByJobId(id)));
        for (const result of results) {
          if (result.status === 'fulfilled' && result.value) runs.push(result.value);
          else failed = true;
        }
      }
      if (!cancelled) {
        setSnapshot({ key, groupId, runs, error: failed ? 'Some executions could not be loaded. They may have been deleted or be temporarily unavailable.' : null });
        setLoading(false);
      }
    };
    void fetch();
    return () => { cancelled = true; };
  }, [key, groupId, revision]);
  const runs = useMemo(() => {
    const allowed = new Set(jobIds);
    const cached = snapshot.key === key && snapshot.groupId === groupId ? snapshot.runs : [];
    const byId = new Map(cached.map(run => [run.job_id, run]));
    for (const run of liveRuns) {
      const previous = byId.get(run.job_id);
      if (!previous || Date.parse(run.updated_at || run.created_at) >= Date.parse(previous.updated_at || previous.created_at)) byId.set(run.job_id, run);
    }
    return [...byId.values()].filter(run => allowed.has(run.job_id) && Boolean(groupId) && run.group_id === groupId);
  }, [jobIds, key, groupId, snapshot, liveRuns]);
  return { runs, loading, error: snapshot.key === key && snapshot.groupId === groupId ? snapshot.error : null, refresh };
}
