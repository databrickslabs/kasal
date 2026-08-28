import { create } from 'zustand';
import { EngineConfigService } from '../api/config/EngineConfigService';

/**
 * Shared state for the Event Triggers feature toggle (Configuration → Engines).
 *
 * Backed by the `event_triggers_enabled` engine setting. Kept in Zustand so the
 * Configuration toggle and the workflow right-sidebar (which shows/hides the
 * Event Triggers action) stay in sync live — flip it in Configuration and the
 * sidebar updates without a refresh. Default OFF.
 */
interface EventTriggersState {
  enabled: boolean;
  loaded: boolean;
  loading: boolean;
  /** Fetch the setting once (no-op if already loaded/loading). */
  load: () => Promise<void>;
  /** Persist the new value to the backend and update the shared state. */
  setEnabled: (enabled: boolean) => Promise<void>;
}

export const useEventTriggersStore = create<EventTriggersState>((set, get) => ({
  enabled: false,
  loaded: false,
  loading: false,
  load: async () => {
    if (get().loaded || get().loading) return;
    set({ loading: true });
    try {
      const r = await EngineConfigService.getEventTriggersEnabled();
      set({ enabled: r.event_triggers_enabled, loaded: true });
    } catch {
      set({ loaded: true }); // default OFF on error
    } finally {
      set({ loading: false });
    }
  },
  setEnabled: async (enabled: boolean) => {
    await EngineConfigService.setEventTriggersEnabled(enabled);
    set({ enabled, loaded: true });
  },
}));
