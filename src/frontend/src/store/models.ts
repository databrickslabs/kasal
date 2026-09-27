import { create } from 'zustand';
import type { EnabledModelRow, Models } from '../types/config/models';
import { fetchEnabledModelRows, toModelRecord } from '../api/config/EnabledModelsService';
import { DecisionConfigService } from '../api/config/DecisionConfigService';
import { ModelService } from '../api/config/ModelService';
import { getDefaultModel } from '../config/defaultModel';

/**
 * The workspace's enabled models, the default model and Auto's availability —
 * ONE list for every model menu (chat composer, builder assistant, agent form,
 * LLM dialog), kept live:
 *
 * - `notifyModelsChanged()` after any model or decision-model mutation
 *   (Configuration → Models), which also tells other open tabs;
 * - a forced reload on `group-changed` (enabled models are per workspace);
 * - `ensureFresh()` when a model menu opens, at most every MODELS_FRESH_MS;
 * - a lazy load the first time anything subscribes — which is also what makes a
 *   Vite hot reload (a new, empty store) refetch without a remount.
 *
 * A failed load keeps the last good list; `error` says it is stale.
 */

/** A menu opening refreshes at most this often (other admins, other tabs). */
export const MODELS_FRESH_MS = 30_000;
export const MODELS_CHANNEL = 'kasal-models';

export interface ModelsState {
  /** Enabled models as the server lists them (chat renders these). */
  models: EnabledModelRow[];
  /** The same models keyed by model key (builder surfaces render these). */
  modelRecord: Models;
  /** The server's default model, as of the last load. */
  defaultModel: string;
  /** The decision model can pick the model (Auto) in this workspace. */
  autoModelAvailable: boolean;
  loading: boolean;
  /** Set when the last load failed; the lists are then the last good ones. */
  error: string | null;
  /** Workspace the lists were loaded for; null until the first success. */
  loadedForGroup: string | null;
  /** Workspace and time of the last completed attempt (success or failure). */
  checkedGroup: string | null;
  checkedAt: number;
  /** Load now. Concurrent calls share one request unless `force` is set. */
  refresh: (options?: { force?: boolean }) => Promise<void>;
  /** Load when never loaded for this workspace, or older than `maxAgeMs`. */
  ensureFresh: (maxAgeMs?: number) => Promise<void>;
}

/** The workspace every request is scoped to (see shared/api/client.ts). */
export function currentModelsGroup(): string {
  try {
    return localStorage.getItem('selectedGroupId') ?? '';
  } catch {
    return '';
  }
}

function errorMessage(err: unknown): string {
  return err instanceof Error && err.message ? err.message : 'Could not load models';
}

let inflight: { group: string; seq: number; promise: Promise<void> } | null = null;
let requestSeq = 0;
let rawSubscribe: ((listener: (s: ModelsState, p: ModelsState) => void) => () => void) | null = null;

export const useModelsStore = create<ModelsState>()((set, get, api) => {
  // Lazy load on first use: any component subscribing (via the hook) makes
  // sure the list is loaded and not stale. Internal subscribers use
  // `subscribeToModels`, which does not fetch.
  const subscribe = api.subscribe;
  rawSubscribe = subscribe;
  api.subscribe = (listener) => {
    void Promise.resolve().then(() => get().ensureFresh());
    return subscribe(listener);
  };

  return {
    models: [],
    modelRecord: {},
    defaultModel: getDefaultModel(),
    autoModelAvailable: false,
    loading: false,
    error: null,
    loadedForGroup: null,
    checkedGroup: null,
    checkedAt: 0,

    refresh: ({ force = false } = {}) => {
      const group = currentModelsGroup();
      if (!force && inflight && inflight.group === group) return inflight.promise;
      const seq = ++requestSeq;
      set({ loading: true });
      const promise = (async () => {
        try {
          // A failed availability read means "Auto not available", as before.
          const [rows, autoAvailable] = await Promise.all([
            fetchEnabledModelRows(),
            DecisionConfigService.getConfig().then((c) => !!c?.available).catch(() => false),
          ]);
          if (seq !== requestSeq) return; // a newer request owns the result
          if (group !== currentModelsGroup()) {
            // The workspace changed under this request: its list is not ours.
            void get().refresh({ force: true });
            return;
          }
          set({
            models: rows,
            modelRecord: toModelRecord(rows),
            defaultModel: getDefaultModel(),
            autoModelAvailable: autoAvailable,
            loading: false,
            error: null,
            loadedForGroup: group,
            checkedGroup: group,
            checkedAt: Date.now(),
          });
        } catch (err) {
          if (seq !== requestSeq) return;
          set({ loading: false, error: errorMessage(err), checkedGroup: group, checkedAt: Date.now() });
        } finally {
          if (inflight?.seq === seq) inflight = null;
        }
      })();
      inflight = { group, seq, promise };
      return promise;
    },

    ensureFresh: (maxAgeMs = MODELS_FRESH_MS) => {
      const state = get();
      const group = currentModelsGroup();
      if (inflight && inflight.group === group) return inflight.promise;
      if (state.checkedGroup === group && Date.now() - state.checkedAt < maxAgeMs) {
        return Promise.resolve();
      }
      return state.refresh();
    },
  };
});

/** Subscribe without triggering a load (for stores that follow this one). */
export function subscribeToModels(listener: (state: ModelsState, previous: ModelsState) => void): () => void {
  return (rawSubscribe ?? useModelsStore.subscribe)(listener);
}

/** True once any consumer has used the store (a load was attempted). */
function inUse(): boolean {
  const s = useModelsStore.getState();
  return s.checkedGroup !== null || s.loading;
}

/** Legacy `ModelService` consumers cache enabled models for 30 minutes. */
function dropLegacyCaches(): void {
  try {
    ModelService.getInstance().clearCaches();
  } catch {
    /* a test double or an unavailable service: nothing cached to drop */
  }
}

let channel: BroadcastChannel | null = null;

/**
 * Call after ANY change to the models or the decision model — enable/disable,
 * add, edit, delete, bulk enable, the decision-model toggle, the Jev URL.
 * Reloads this tab's list now and tells the other open tabs to reload theirs.
 */
export function notifyModelsChanged(): Promise<void> {
  dropLegacyCaches();
  try {
    channel?.postMessage({ type: 'models-changed' });
  } catch {
    /* channel closed: other tabs catch up on their next menu open */
  }
  return useModelsStore.getState().refresh({ force: true });
}

function installLiveSync(): () => void {
  if (typeof window === 'undefined') return () => undefined;
  const onGroupChanged = () => {
    dropLegacyCaches();
    if (inUse()) void useModelsStore.getState().refresh({ force: true });
  };
  window.addEventListener('group-changed', onGroupChanged);
  // jsdom and some embedded browsers have no BroadcastChannel.
  if (typeof BroadcastChannel !== 'undefined') {
    try {
      channel = new BroadcastChannel(MODELS_CHANNEL);
      channel.onmessage = (event: MessageEvent) => {
        if ((event.data as { type?: string } | null)?.type !== 'models-changed') return;
        dropLegacyCaches();
        if (inUse()) void useModelsStore.getState().refresh({ force: true });
      };
    } catch {
      channel = null;
    }
  }
  return () => {
    window.removeEventListener('group-changed', onGroupChanged);
    channel?.close();
    channel = null;
  };
}

const teardownLiveSync = installLiveSync();

/** Remove this module's listeners (hot reload, and tests that reload it). */
export function stopModelsLiveSync(): void {
  teardownLiveSync();
  inflight = null;
}

// A hot reload creates a fresh store; drop this copy's listeners so the old
// module does not keep reloading a store nothing reads.
if (import.meta.hot) import.meta.hot.dispose(stopModelsLiveSync);

/** For tests: forget in-flight requests and reset to the never-loaded state. */
export function resetModelsStoreForTests(): void {
  inflight = null;
  requestSeq += 1;
  useModelsStore.setState({
    models: [],
    modelRecord: {},
    defaultModel: getDefaultModel(),
    autoModelAvailable: false,
    loading: false,
    error: null,
    loadedForGroup: null,
    checkedGroup: null,
    checkedAt: 0,
  });
}
