import { apiClient } from '../config/api/ApiConfig';

/**
 * Per-workspace "Predefined UI" configuration. When enabled, crews produce
 * structured, design-system UI (rendered consistently in the chat preview)
 * instead of arbitrary HTML. The structured format conforms to the A2UI
 * protocol; naming here is intentionally UI-centric.
 */
export interface UIConfig {
  enabled: boolean;
  catalog_type: 'minimal' | 'full' | 'custom';
  catalog_json?: string | null;
  style_json?: string | null;
  id?: number | null;
  group_id?: string | null;
  created_by_email?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export type UIConfigUpdate = Pick<
  UIConfig,
  'enabled' | 'catalog_type' | 'catalog_json' | 'style_json'
>;

/** Notified with the freshest config whenever it changes (i.e. a save), so open
 *  chat surfaces re-theme immediately instead of waiting for a page reload. */
type Listener = (cfg: UIConfig | null) => void;

export class UIConfigService {
  private static baseUrl = '/ui-config';

  // Session cache shared by every consumer (the many inline A2UI surfaces + the
  // log console) so they don't each hit /ui-config. `undefined` = never fetched;
  // `null` = the fetch failed. The Configurator's save refreshes this in place.
  private static cache: UIConfig | null | undefined;
  private static inflight: Promise<UIConfig> | null = null;
  private static listeners = new Set<Listener>();

  /** Synchronous peek at the cached config (`undefined` until first fetched). Lets
   *  a hook seed its initial state on a cache hit without an async flash. */
  static peek(): UIConfig | null | undefined {
    return this.cache;
  }

  /**
   * Get the current workspace's Predefined UI configuration.
   *
   * Cached for the session so the many inline surfaces share one fetch. Pass
   * `force` to bypass the cache and refetch — the Configurator editor does this
   * so it always opens on the true server state.
   */
  static async getConfig(force = false): Promise<UIConfig> {
    if (!force && this.cache != null) return this.cache;
    if (force) this.inflight = null;
    if (!this.inflight) {
      this.inflight = apiClient
        .get<UIConfig>(this.baseUrl)
        .then((response) => {
          this.cache = response.data;
          this.inflight = null;
          return response.data;
        })
        .catch((err) => {
          this.inflight = null;
          throw err;
        });
    }
    return this.inflight;
  }

  /**
   * Update the current workspace's Predefined UI configuration (admins only).
   *
   * Refreshes the shared cache from the server response and notifies every
   * subscriber, so mounted chat surfaces re-theme at once — no page reload needed.
   */
  static async updateConfig(config: UIConfigUpdate): Promise<UIConfig> {
    const response = await apiClient.put<UIConfig>(this.baseUrl, config);
    this.cache = response.data;
    this.notify();
    return response.data;
  }

  /** Subscribe to config changes; returns an unsubscribe function. */
  static subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private static notify(): void {
    for (const listener of this.listeners) listener(this.cache ?? null);
  }
}
