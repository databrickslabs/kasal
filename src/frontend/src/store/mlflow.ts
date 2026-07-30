import { create } from 'zustand';
import { apiClient } from '../config/api/ApiConfig';

/**
 * Whether MLflow is switched on, shared across every surface that gates on it.
 *
 * A store rather than a per-component fetch, because the value is WRITTEN in one
 * place (the MLflow configuration section) and READ in three others — the crew
 * catalog, the flow catalog and the Prompts tab. With a hook that fetched on
 * mount, toggling the setting left all three showing stale state until they
 * happened to remount, so an "Optimize Prompts" action stayed visible after
 * MLflow was turned off. Subscribers re-render the moment the toggle lands.
 *
 * `enabled` is `null` until the first load — deliberately distinct from `false`.
 * A consumer that treats "not known yet" as "disabled" makes its action flicker
 * out and back on every mount, which reads as a bug.
 */
interface MLflowStore {
  enabled: boolean | null;
  /** Guards against every mounting consumer firing its own request. */
  loading: boolean;
  /** Fetch once per session; later mounts read the cached value. */
  ensureLoaded: () => Promise<void>;
  /** Re-read from the server, e.g. after the settings section saves. */
  refresh: () => Promise<void>;
  /** Apply a known value immediately, so a toggle needs no round trip. */
  setEnabled: (enabled: boolean) => void;
}

async function load(set: (partial: Partial<MLflowStore>) => void) {
  set({ loading: true });
  try {
    const resp = await apiClient.get<{ enabled: boolean }>('/mlflow/settings');
    set({ enabled: Boolean(resp.data?.enabled) });
  } catch {
    // Unreachable settings means the feature cannot be confirmed to work, and
    // showing an action that will fail is worse than hiding one that would have.
    set({ enabled: false });
  } finally {
    set({ loading: false });
  }
}

export const useMLflowStore = create<MLflowStore>((set, get) => ({
  enabled: null,
  loading: false,

  ensureLoaded: async () => {
    if (get().enabled !== null || get().loading) return;
    await load(set);
  },

  refresh: async () => {
    await load(set);
  },

  setEnabled: (enabled: boolean) => set({ enabled }),
}));

export default useMLflowStore;
