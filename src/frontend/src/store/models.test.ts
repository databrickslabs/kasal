import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook } from '@testing-library/react';

const { fetchRows, getDecisionConfig, clearCaches } = vi.hoisted(() => ({
  fetchRows: vi.fn(),
  getDecisionConfig: vi.fn(),
  clearCaches: vi.fn(),
}));

vi.mock('../api/config/EnabledModelsService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../api/config/EnabledModelsService')>()),
  fetchEnabledModelRows: () => fetchRows(),
}));
vi.mock('../api/config/DecisionConfigService', () => ({
  DecisionConfigService: { getConfig: () => getDecisionConfig() },
}));
vi.mock('../api/config/ModelService', () => ({
  ModelService: { getInstance: () => ({ clearCaches }) },
}));

const row = (key: string) => ({
  id: 1, key, name: key, provider: 'databricks', temperature: null, context_window: null,
  max_output_tokens: null, extended_thinking: false, enabled: true, created_at: '', updated_at: '',
});

/** A controllable promise, to hold a request in flight. */
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

class FakeChannel {
  static instances: FakeChannel[] = [];
  onmessage: ((event: MessageEvent) => void) | null = null;
  posted: unknown[] = [];
  closed = false;
  constructor(public name: string) { FakeChannel.instances.push(this); }
  postMessage(data: unknown) { this.posted.push(data); }
  close() { this.closed = true; }
}

let loaded: typeof import('./models') | null = null;

/** A fresh module (and store), as after a page load or a hot reload. */
async function load() {
  loaded?.stopModelsLiveSync(); // as the hot-reload dispose does
  vi.resetModules();
  loaded = await import('./models');
  return loaded;
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe('store/models', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    localStorage.setItem('selectedGroupId', 'g1');
    fetchRows.mockResolvedValue([row('a'), row('b')]);
    getDecisionConfig.mockResolvedValue({ enabled: false, api_key_configured: false, available: false });
  });
  afterEach(() => {
    loaded?.stopModelsLiveSync();
    loaded = null;
    vi.unstubAllGlobals();
    FakeChannel.instances = [];
  });

  it('loads the list, the keyed record and the default', async () => {
    const { useModelsStore } = await load();
    await useModelsStore.getState().refresh();
    const s = useModelsStore.getState();
    expect(s.models.map((m) => m.key)).toEqual(['a', 'b']);
    expect(Object.keys(s.modelRecord)).toEqual(['a', 'b']);
    expect(s.loadedForGroup).toBe('g1');
    expect(s.loading).toBe(false);
    expect(s.error).toBeNull();
  });

  it('shares one request between concurrent refreshes; force starts a new one', async () => {
    const { useModelsStore } = await load();
    const { refresh } = useModelsStore.getState();
    await Promise.all([refresh(), refresh(), useModelsStore.getState().ensureFresh()]);
    expect(fetchRows).toHaveBeenCalledTimes(1);
    await refresh({ force: true });
    expect(fetchRows).toHaveBeenCalledTimes(2);
  });

  it('the latest forced request wins over an older one still in flight', async () => {
    const { useModelsStore } = await load();
    const slow = deferred<ReturnType<typeof row>[]>();
    fetchRows.mockReturnValueOnce(slow.promise).mockResolvedValueOnce([row('new')]);
    const first = useModelsStore.getState().refresh();
    await useModelsStore.getState().refresh({ force: true });
    slow.resolve([row('stale')]);
    await first;
    expect(useModelsStore.getState().models.map((m) => m.key)).toEqual(['new']);
  });

  it('reports Auto availability, and a failed availability read as unavailable', async () => {
    const { useModelsStore } = await load();
    getDecisionConfig.mockResolvedValueOnce({ enabled: true, api_key_configured: true, available: true });
    await useModelsStore.getState().refresh();
    expect(useModelsStore.getState().autoModelAvailable).toBe(true);
    getDecisionConfig.mockRejectedValueOnce(new Error('403'));
    await useModelsStore.getState().refresh({ force: true });
    expect(useModelsStore.getState().autoModelAvailable).toBe(false);
  });

  it('keeps the last good list when a load fails, and clears the error on success', async () => {
    const { useModelsStore } = await load();
    await useModelsStore.getState().refresh();
    fetchRows.mockRejectedValueOnce(new Error('offline'));
    await useModelsStore.getState().refresh({ force: true });
    let s = useModelsStore.getState();
    expect(s.models.map((m) => m.key)).toEqual(['a', 'b']);
    expect(s.error).toBe('offline');
    expect(s.loading).toBe(false);
    await useModelsStore.getState().refresh({ force: true });
    s = useModelsStore.getState();
    expect(s.error).toBeNull();
  });

  it('ensureFresh refreshes at most once per interval, and always for a new workspace', async () => {
    const { useModelsStore, MODELS_FRESH_MS } = await load();
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_000_000);
    await useModelsStore.getState().ensureFresh();
    await useModelsStore.getState().ensureFresh();
    expect(fetchRows).toHaveBeenCalledTimes(1);
    now.mockReturnValue(1_000_000 + MODELS_FRESH_MS - 1);
    await useModelsStore.getState().ensureFresh();
    expect(fetchRows).toHaveBeenCalledTimes(1);
    now.mockReturnValue(1_000_000 + MODELS_FRESH_MS + 1);
    await useModelsStore.getState().ensureFresh();
    expect(fetchRows).toHaveBeenCalledTimes(2);
    localStorage.setItem('selectedGroupId', 'g2');
    await useModelsStore.getState().ensureFresh();
    expect(fetchRows).toHaveBeenCalledTimes(3);
    expect(useModelsStore.getState().loadedForGroup).toBe('g2');
    now.mockRestore();
  });

  it('reloads on group-changed, for the new workspace', async () => {
    const { useModelsStore } = await load();
    await useModelsStore.getState().refresh();
    fetchRows.mockResolvedValueOnce([row('g2-model')]);
    localStorage.setItem('selectedGroupId', 'g2');
    window.dispatchEvent(new CustomEvent('group-changed', { detail: { groupId: 'g2' } }));
    await flush();
    await flush();
    const s = useModelsStore.getState();
    expect(s.loadedForGroup).toBe('g2');
    expect(s.models.map((m) => m.key)).toEqual(['g2-model']);
    // Legacy ModelService caches are per-tab, not per-workspace: drop them.
    expect(clearCaches).toHaveBeenCalled();
  });

  it('does not fetch on group-changed while nothing uses the store', async () => {
    await load();
    window.dispatchEvent(new CustomEvent('group-changed'));
    await flush();
    expect(fetchRows).not.toHaveBeenCalled();
  });

  it('discards a response for a workspace the user has since left', async () => {
    const { useModelsStore } = await load();
    const slow = deferred<ReturnType<typeof row>[]>();
    fetchRows.mockReturnValueOnce(slow.promise).mockResolvedValueOnce([row('g2-model')]);
    const pending = useModelsStore.getState().refresh();
    localStorage.setItem('selectedGroupId', 'g2');
    slow.resolve([row('g1-model')]);
    await pending;
    await flush();
    await flush();
    expect(useModelsStore.getState().loadedForGroup).toBe('g2');
    expect(useModelsStore.getState().models.map((m) => m.key)).toEqual(['g2-model']);
  });

  it('loads lazily on the first component subscription (a hot-reloaded, empty store refetches)', async () => {
    const { useModelsStore, subscribeToModels } = await load();
    // Internal followers (the chat's appStore) subscribe without fetching.
    const stop = subscribeToModels(() => undefined);
    await flush();
    expect(fetchRows).not.toHaveBeenCalled();
    stop();
    const { result } = renderHook(() => useModelsStore((s) => s.models));
    await vi.waitFor(() => expect(result.current.map((m) => m.key)).toEqual(['a', 'b']));
    expect(fetchRows).toHaveBeenCalledTimes(1);
  });

  describe('across tabs', () => {
    it('notifyModelsChanged reloads now and tells the other tabs', async () => {
      vi.stubGlobal('BroadcastChannel', FakeChannel);
      const { useModelsStore, notifyModelsChanged, MODELS_CHANNEL } = await load();
      await useModelsStore.getState().refresh();
      await notifyModelsChanged();
      expect(fetchRows).toHaveBeenCalledTimes(2);
      const channel = FakeChannel.instances.find((c) => c.name === MODELS_CHANNEL);
      expect(channel?.posted).toEqual([{ type: 'models-changed' }]);
    });

    it('a change broadcast from another tab reloads this tab', async () => {
      vi.stubGlobal('BroadcastChannel', FakeChannel);
      const { useModelsStore, MODELS_CHANNEL } = await load();
      await useModelsStore.getState().refresh();
      fetchRows.mockResolvedValueOnce([row('a')]);
      const channel = FakeChannel.instances.find((c) => c.name === MODELS_CHANNEL);
      channel?.onmessage?.({ data: { type: 'models-changed' } } as MessageEvent);
      await flush();
      expect(fetchRows).toHaveBeenCalledTimes(2);
      expect(clearCaches).toHaveBeenCalled();
      expect(useModelsStore.getState().models.map((m) => m.key)).toEqual(['a']);
      // Unrelated messages are ignored.
      channel?.onmessage?.({ data: { type: 'other' } } as MessageEvent);
      await flush();
      expect(fetchRows).toHaveBeenCalledTimes(2);
    });

    it('works without BroadcastChannel (jsdom, older embedded browsers)', async () => {
      vi.stubGlobal('BroadcastChannel', undefined);
      const { useModelsStore, notifyModelsChanged } = await load();
      await expect(notifyModelsChanged()).resolves.toBeUndefined();
      expect(useModelsStore.getState().models).toHaveLength(2);
    });
  });
});
