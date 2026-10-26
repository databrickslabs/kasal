import { useExecutionStore } from './executionStore';
import { fetchEnabledSkills, pickSelectedSkills } from '../utils/skillSelection';

/**
 * Drop selected skill names that are no longer pickable — gone, disabled, or
 * globally enabled (those attach to every agent anyway). Returns the kept
 * selection; on a fetch failure the current selection is kept untouched (the
 * backend tolerates stale names). No request is made when nothing is selected.
 */
export async function reconcileSelectedSkills(): Promise<string[]> {
  const store = useExecutionStore.getState();
  const selected = store.selectedSkills;
  if (selected.length === 0) return selected;
  try {
    const enabled = await fetchEnabledSkills();
    const kept = pickSelectedSkills(selected, enabled);
    if (kept.length !== selected.length) store.setSelectedSkills(kept);
    return kept;
  } catch {
    return selected;
  }
}
