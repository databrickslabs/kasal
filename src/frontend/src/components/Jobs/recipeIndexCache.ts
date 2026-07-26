/**
 * Shared job→recipe index for the run list.
 *
 * Every visible row needs the same map of "which recipe was this run mined
 * into". Fetching it per row would turn one page of runs into one page of
 * requests, so the promise is fetched once and shared, and each mounted row
 * subscribes to the result.
 *
 * Lives in its own module rather than beside the component so the component
 * file exports only components — a module that exports both loses React Fast
 * Refresh, which matters in a codebase developed against a hot-reloading dev
 * server.
 */

import {
  RecipeJobEntry,
  WorkflowRecipeService,
} from '../../api/workflow/WorkflowRecipeService';

let indexPromise: Promise<Record<string, RecipeJobEntry>> | null = null;
let fetchedAt = 0;
const subscribers = new Set<() => void>();

/**
 * How long a fetched index stays trusted.
 *
 * Mining is a background sweep, so a run is mined SECONDS TO MINUTES after it
 * finishes — the row is on screen well before its recipe exists. A cache with no
 * expiry meant a freshly finished run never grew its control until a full page
 * reload, which reads exactly like the feature being broken. Short enough that
 * the control appears on the next list refresh, long enough that a page of rows
 * costs one request rather than one each.
 */
const TTL_MS = 20_000;

/** The index, shared across rows and refetched once it goes stale. Never
 *  rejects — a failed lookup degrades to "no run was mined", which renders
 *  nothing. */
export const loadRecipeIndex = (): Promise<Record<string, RecipeJobEntry>> => {
  if (!indexPromise) {
    fetchedAt = Date.now();
    indexPromise = WorkflowRecipeService.getByJob().catch(() => ({}));
  }
  return indexPromise;
};

/** Drop the cached index and re-read it in every mounted row. Call after any
 *  change to curation so the rows reflect what was just set. */
export const invalidateRecipeIndex = (): void => {
  indexPromise = null;
  fetchedAt = 0;
  subscribers.forEach((notify) => notify());
};

/**
 * Re-read the index if it has gone stale, otherwise do nothing.
 *
 * Called whenever the run list changes. Guarded by the TTL because the list
 * updates on every SSE status change — an unguarded refresh there would fire a
 * request per status transition.
 */
export const refreshRecipeIndexIfStale = (): void => {
  if (indexPromise && Date.now() - fetchedAt < TTL_MS) return;
  invalidateRecipeIndex();
};

export const subscribeToRecipeIndex = (notify: () => void): (() => void) => {
  subscribers.add(notify);
  return () => {
    subscribers.delete(notify);
  };
};
