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
 * A run is mined shortly after it finishes, so the row is on screen before its
 * recipe exists. A cache with no expiry meant a freshly finished run never grew
 * its control until a full page reload, which reads exactly like the feature
 * being broken. Short enough that the control appears on the next list refresh,
 * long enough that a page of rows costs one request rather than one each.
 */
const TTL_MS = 20_000;

/**
 * When to re-read after a run completes.
 *
 * The TTL alone is not enough: the index is refreshed when the run LIST
 * changes, and a run's completion is usually the last change there is — so a
 * refresh that lands before mining finishes is followed by nothing, and the
 * control stays missing until something unrelated moves the list. These are the
 * nudges that close that window. Two of them because mining is fast but not
 * instantaneous, and one badly-timed read would leave the row empty again.
 */
const POST_RUN_REFRESH_MS = [1_500, 5_000];

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

/**
 * A run just finished, so its recipe is being mined right now — re-read the
 * index once mining has had a moment, without waiting for the list to change
 * again. Registered at module scope: the cache is a singleton, and every row
 * that cares is already subscribed to it.
 */
if (typeof window !== 'undefined') {
  window.addEventListener('jobCompleted', () => {
    POST_RUN_REFRESH_MS.forEach((delay) =>
      setTimeout(() => invalidateRecipeIndex(), delay),
    );
  });
}

export const subscribeToRecipeIndex = (notify: () => void): (() => void) => {
  subscribers.add(notify);
  return () => {
    subscribers.delete(notify);
  };
};
