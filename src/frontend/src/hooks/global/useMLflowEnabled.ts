import { useEffect } from 'react';
import { useMLflowStore } from '../../store/mlflow';

/**
 * Whether MLflow is switched on for this workspace.
 *
 * Prompt optimization writes prompt versions to an MLflow registry, so its entry
 * points are hidden when MLflow is off rather than failing at the point of use.
 *
 * Backed by a Zustand store rather than a per-component fetch: the value is
 * written in the MLflow configuration section and read by the crew catalog, the
 * flow catalog and the Prompts tab. Fetching per component meant those three
 * kept showing stale state after a toggle until they happened to remount — an
 * "Optimize Prompts" action stayed visible after MLflow had been turned off.
 * Reading the store makes every consumer update the moment the toggle lands.
 *
 * Returns `null` while unknown, which callers must not treat as `false` — see
 * the store for why.
 */
export function useMLflowEnabled(): boolean | null {
  const enabled = useMLflowStore((s) => s.enabled);
  const ensureLoaded = useMLflowStore((s) => s.ensureLoaded);

  useEffect(() => {
    void ensureLoaded();
  }, [ensureLoaded]);

  return enabled;
}

export default useMLflowEnabled;
