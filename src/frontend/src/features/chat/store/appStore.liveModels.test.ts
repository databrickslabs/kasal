/**
 * The composer follows the shared live model list (store/models.ts) without
 * any loadModels call: an admin disabling the chosen model, or the decision
 * model coming or going, re-derives `selectedModel` (resolveChatModel).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../api/config/EnabledModelsService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../../api/config/EnabledModelsService')>()),
  fetchEnabledModelRows: vi.fn(async () => []),
}));

let models: typeof import('../../../store/models') | null = null;
const live = () => models!.useModelsStore;

/** Fresh chat and models stores, as after a page load. */
async function fresh() {
  models?.stopModelsLiveSync();
  vi.resetModules();
  const store = (await import('./appStore')).useAppStore;
  models = await import('../../../store/models');
  return store;
}

describe('appStore', () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => models?.stopModelsLiveSync());

  describe('follows the shared live model list', () => {
    const rows = (...keys: string[]) => keys.map((key, i) => ({ id: i, key, name: key }));
    const publish = (patch: Record<string, unknown>) =>
      live().setState({ loadedForGroup: '', defaultModel: 'k1', ...patch } as never);

    it('falls back when the selected model is disabled, and returns when re-enabled', async () => {
      const store = await fresh();
      store.getState().setSelectedModel('k2');
      publish({ models: rows('k1', 'k2'), autoModelAvailable: false });
      expect(store.getState().selectedModel).toBe('k2');
      publish({ models: rows('k1') });
      expect(store.getState().selectedModel).toBe('k1');
      publish({ models: rows('k1', 'k2') });
      expect(store.getState().selectedModel).toBe('k2');
    });

    it('falls back to Auto for a disabled choice when Auto is available', async () => {
      const store = await fresh();
      store.getState().setSelectedModel('k2');
      publish({ models: rows('k1'), autoModelAvailable: true });
      expect(store.getState().selectedModel).toBe('auto');
    });

    it('falls back to the default when Auto is lost while selected', async () => {
      const store = await fresh();
      publish({ models: rows('k1', 'k2'), autoModelAvailable: true });
      expect(store.getState().selectedModel).toBe('auto');
      publish({ autoModelAvailable: false });
      expect(store.getState().selectedModel).toBe('k1');
    });

    it('switches to Auto when it becomes available and the user never chose', async () => {
      const store = await fresh();
      publish({ models: rows('k1', 'k2'), autoModelAvailable: false });
      expect(store.getState().selectedModel).toBe('k1');
      publish({ autoModelAvailable: true });
      expect(store.getState().selectedModel).toBe('auto');
    });

    it('keeps an explicit choice when Auto becomes available', async () => {
      const store = await fresh();
      store.getState().setSelectedModel('k2');
      publish({ models: rows('k1', 'k2'), autoModelAvailable: false });
      publish({ autoModelAvailable: true });
      expect(store.getState().selectedModel).toBe('k2');
    });

    it('does nothing before a list has loaded', async () => {
      const store = await fresh();
      store.getState().setSelectedModel('k2');
      live().setState({ models: rows('k1'), loadedForGroup: null } as never);
      expect(store.getState().selectedModel).toBe('k2');
    });
  });
});
