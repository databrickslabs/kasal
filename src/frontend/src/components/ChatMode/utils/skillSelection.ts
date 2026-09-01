import { Skill, SkillService } from '../../../api/tools/SkillService';
import { useExecutionStore } from '../store/executionStore';

/**
 * Shared skill-selection plumbing for ChatMode.
 *
 * The "+" menu stores picked skill NAMES and persists them across sessions —
 * which means a skill deleted, disabled, or promoted to globally-enabled since
 * it was picked would silently ride along on every later turn (the backend
 * drops unknown names with only a server-side warning, and a globally-enabled
 * skill is attached to every agent regardless, so "picking" it is a no-op that
 * inflates the badge). ``reconcileSelectedSkills`` prunes all three cases and
 * runs both when the picker opens and at dispatch time.
 */

let cache: { at: number; skills: Skill[] } | null = null;
const TTL_MS = 60_000;

/** The workspace's ENABLED skills, cached briefly so the picker's load and a
 * dispatch-time reconcile in the same minute cost one request. */
export async function fetchEnabledSkills(force = false): Promise<Skill[]> {
  if (!force && cache && Date.now() - cache.at < TTL_MS) return cache.skills;
  const all = await SkillService.list();
  const enabled = all.filter((s) => s.enabled);
  cache = { at: Date.now(), skills: enabled };
  return enabled;
}

/** Test hook / cache bust (e.g. after editing skills in Configuration). */
export function invalidateSkillsCache(): void {
  cache = null;
}

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
    const pickable = new Set(
      enabled.filter((s) => !s.global_enabled).map((s) => s.name),
    );
    const kept = selected.filter((n) => pickable.has(n));
    if (kept.length !== selected.length) store.setSelectedSkills(kept);
    return kept;
  } catch {
    return selected;
  }
}
