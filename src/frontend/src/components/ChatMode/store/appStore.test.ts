import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// --- Mocks for sibling modules ---
const updateClient = vi.fn();
const fetchEnabledModels = vi.fn();
const fetchEnabledTools = vi.fn();
const fetchWorkspaces = vi.fn();
const listSavedCrews = vi.fn();
const listSavedFlows = vi.fn();

vi.mock('../api/client', () => ({
  updateClient: (...args: unknown[]) => updateClient(...args),
}));
vi.mock('../api/models', () => ({
  fetchEnabledModels: (...args: unknown[]) => fetchEnabledModels(...args),
}));
vi.mock('../api/tools', () => ({
  fetchEnabledTools: (...args: unknown[]) => fetchEnabledTools(...args),
}));
vi.mock('../api/workspaces', () => ({
  fetchWorkspaces: (...args: unknown[]) => fetchWorkspaces(...args),
}));
vi.mock('../api/crews', () => ({
  listSavedCrews: (...args: unknown[]) => listSavedCrews(...args),
  listSavedFlows: (...args: unknown[]) => listSavedFlows(...args),
}));

const CONFIG_STORAGE_KEY = 'kasal-chat-config';
const MODEL_STORAGE_KEY = 'kasal-chat-model';
const THEME_STORAGE_KEY = 'kasal-chat-theme';

// Helper: import a fresh copy of the store module so module-level state
// (loadConfig / getStoredTheme / selectedModel IIFE) is recomputed.
async function freshStore() {
  vi.resetModules();
  const mod = await import('./appStore');
  return mod.useAppStore;
}

describe('appStore', () => {
  let matchMediaMatches: boolean;

  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    matchMediaMatches = false;
    // Re-define matchMedia so we control prefers-color-scheme.
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      configurable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: matchMediaMatches,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    });
    // Clean up any chat root left over.
    document.getElementById('kasal-chat-root')?.remove();
  });

  afterEach(() => {
    document.getElementById('kasal-chat-root')?.remove();
  });

  // ---------------------------------------------------------------------------
  // getStoredTheme branches
  // ---------------------------------------------------------------------------
  describe('getStoredTheme (initial theme)', () => {
    it('uses stored "dark" theme', async () => {
      localStorage.setItem(THEME_STORAGE_KEY, 'dark');
      const store = await freshStore();
      expect(store.getState().theme).toBe('dark');
    });

    it('uses stored "light" theme', async () => {
      localStorage.setItem(THEME_STORAGE_KEY, 'light');
      const store = await freshStore();
      expect(store.getState().theme).toBe('light');
    });

    it('falls back to matchMedia dark when nothing stored', async () => {
      matchMediaMatches = true;
      const store = await freshStore();
      expect(store.getState().theme).toBe('dark');
    });

    it('falls back to light when matchMedia does not match dark', async () => {
      matchMediaMatches = false;
      const store = await freshStore();
      expect(store.getState().theme).toBe('light');
    });

    it('falls back to matchMedia when localStorage.getItem throws', async () => {
      const spy = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
        throw new Error('boom');
      });
      matchMediaMatches = true;
      const store = await freshStore();
      expect(store.getState().theme).toBe('dark');
      spy.mockRestore();
    });

    it('falls back to light when matchMedia is undefined', async () => {
      // matchMedia?.(...) -> undefined optional chaining short-circuit.
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        configurable: true,
        value: undefined,
      });
      const store = await freshStore();
      expect(store.getState().theme).toBe('light');
    });
  });

  // ---------------------------------------------------------------------------
  // loadConfig branches (module-level)
  // ---------------------------------------------------------------------------
  describe('loadConfig (initial config)', () => {
    it('returns default config when nothing stored', async () => {
      const store = await freshStore();
      expect(store.getState().config).toEqual({
        apiUrl: '/api/v1',
        email: '',
        groupId: '',
        accessToken: '',
      });
    });

    it('returns stored config as-is when apiUrl is relative', async () => {
      const stored = { apiUrl: '/api/v1', email: 'a@b.com', groupId: 'g1', accessToken: 't' };
      localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(stored));
      const store = await freshStore();
      expect(store.getState().config).toEqual(stored);
    });

    it('resets apiUrl to default when stored apiUrl starts with http', async () => {
      const stored = { apiUrl: 'http://evil.example.com', email: 'a@b.com', groupId: '', accessToken: '' };
      localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(stored));
      const store = await freshStore();
      expect(store.getState().config.apiUrl).toBe('/api/v1');
      expect(store.getState().config.email).toBe('a@b.com');
    });

    it('handles config without apiUrl field (falsy apiUrl branch)', async () => {
      const stored = { email: 'x@y.com', groupId: '', accessToken: '' };
      localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(stored));
      const store = await freshStore();
      expect(store.getState().config.email).toBe('x@y.com');
      expect(store.getState().config.apiUrl).toBeUndefined();
    });

    it('falls back to default config on JSON parse error', async () => {
      localStorage.setItem(CONFIG_STORAGE_KEY, '{not valid json');
      const store = await freshStore();
      expect(store.getState().config).toEqual({
        apiUrl: '/api/v1',
        email: '',
        groupId: '',
        accessToken: '',
      });
    });
  });

  // ---------------------------------------------------------------------------
  // selectedModel IIFE branches
  // ---------------------------------------------------------------------------
  describe('selectedModel initial value', () => {
    it('reads stored model from localStorage', async () => {
      localStorage.setItem(MODEL_STORAGE_KEY, 'my-model');
      const store = await freshStore();
      expect(store.getState().selectedModel).toBe('my-model');
    });

    it('defaults to empty string when not stored', async () => {
      const store = await freshStore();
      expect(store.getState().selectedModel).toBe('');
    });

    it('defaults to empty string when getItem throws', async () => {
      const spy = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
        throw new Error('boom');
      });
      const store = await freshStore();
      expect(store.getState().selectedModel).toBe('');
      spy.mockRestore();
    });
  });

  // ---------------------------------------------------------------------------
  // init / applyTheme branches
  // ---------------------------------------------------------------------------
  describe('init', () => {
    it('applies theme to chat root element and calls updateClient', async () => {
      const root = document.createElement('div');
      root.id = 'kasal-chat-root';
      document.body.appendChild(root);

      localStorage.setItem(THEME_STORAGE_KEY, 'dark');
      const store = await freshStore();
      store.getState().init();

      expect(root.getAttribute('data-theme')).toBe('dark');
      expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');
      expect(updateClient).toHaveBeenCalledWith(store.getState().config);
    });

    it('applies theme without chat root element present', async () => {
      // No #kasal-chat-root in DOM.
      const store = await freshStore();
      expect(() => store.getState().init()).not.toThrow();
      expect(updateClient).toHaveBeenCalled();
    });

    it('swallows localStorage.setItem errors in applyTheme', async () => {
      const store = await freshStore();
      const spy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
        throw new Error('quota');
      });
      expect(() => store.getState().init()).not.toThrow();
      spy.mockRestore();
    });
  });

  // ---------------------------------------------------------------------------
  // loadModels branches
  // ---------------------------------------------------------------------------
  describe('loadModels', () => {
    it('sets models and auto-selects first model when none selected', async () => {
      const store = await freshStore();
      const models = [
        { id: 1, key: 'k1', name: 'M1' },
        { id: 2, key: 'k2', name: 'M2' },
      ];
      fetchEnabledModels.mockResolvedValue(models);

      await store.getState().loadModels();

      expect(store.getState().models).toBe(models);
      expect(store.getState().selectedModel).toBe('k1');
      expect(localStorage.getItem(MODEL_STORAGE_KEY)).toBe('k1');
    });

    it('does not override an already-selected model', async () => {
      localStorage.setItem(MODEL_STORAGE_KEY, 'preset');
      const store = await freshStore();
      const models = [{ id: 1, key: 'k1', name: 'M1' }];
      fetchEnabledModels.mockResolvedValue(models);

      await store.getState().loadModels();

      expect(store.getState().models).toBe(models);
      expect(store.getState().selectedModel).toBe('preset');
    });

    it('does not auto-select when model list is empty', async () => {
      const store = await freshStore();
      fetchEnabledModels.mockResolvedValue([]);

      await store.getState().loadModels();

      expect(store.getState().models).toEqual([]);
      expect(store.getState().selectedModel).toBe('');
    });

    it('swallows localStorage.setItem error during auto-select', async () => {
      const store = await freshStore();
      fetchEnabledModels.mockResolvedValue([{ id: 1, key: 'k1', name: 'M1' }]);
      const spy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
        throw new Error('quota');
      });

      await store.getState().loadModels();

      expect(store.getState().selectedModel).toBe('k1');
      spy.mockRestore();
    });

    it('swallows errors when fetch throws', async () => {
      const store = await freshStore();
      fetchEnabledModels.mockRejectedValue(new Error('no endpoint'));

      await store.getState().loadModels();

      expect(store.getState().models).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // loadTools branches
  // ---------------------------------------------------------------------------
  describe('loadTools', () => {
    it('sets tools and builds toolNameMap by id and title', async () => {
      const store = await freshStore();
      const tools = [
        { id: 10, title: 'Search', description: 'd', enabled: true },
        { id: 20, title: 'Calc', description: 'd', enabled: true },
      ];
      fetchEnabledTools.mockResolvedValue(tools);

      await store.getState().loadTools();

      expect(store.getState().tools).toBe(tools);
      expect(store.getState().toolNameMap).toEqual({
        '10': 'Search',
        Search: 'Search',
        '20': 'Calc',
        Calc: 'Calc',
      });
    });

    it('swallows errors when fetch throws', async () => {
      const store = await freshStore();
      fetchEnabledTools.mockRejectedValue(new Error('no endpoint'));

      await store.getState().loadTools();

      expect(store.getState().tools).toEqual([]);
      expect(store.getState().toolNameMap).toEqual({});
    });
  });

  // ---------------------------------------------------------------------------
  // loadWorkspaces branches
  // ---------------------------------------------------------------------------
  describe('loadWorkspaces', () => {
    it('returns early when there is no email', async () => {
      const store = await freshStore();
      // default config has empty email
      await store.getState().loadWorkspaces();

      expect(fetchWorkspaces).not.toHaveBeenCalled();
      expect(store.getState().workspaces).toEqual([]);
    });

    it('loads workspaces and auto-selects first when no groupId', async () => {
      const stored = { apiUrl: '/api/v1', email: 'a@b.com', groupId: '', accessToken: '' };
      localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(stored));
      const store = await freshStore();
      const ws = [
        { id: 'w1', name: 'Personal', user_role: null },
        { id: 'w2', name: 'Team', user_role: 'admin' },
      ];
      fetchWorkspaces.mockResolvedValue(ws);

      await store.getState().loadWorkspaces();

      expect(fetchWorkspaces).toHaveBeenCalledWith('a@b.com');
      expect(store.getState().workspaces).toBe(ws);
      expect(store.getState().config.groupId).toBe('w1');
      expect(updateClient).toHaveBeenCalledWith(
        expect.objectContaining({ groupId: 'w1', email: 'a@b.com' }),
      );
      expect(JSON.parse(localStorage.getItem(CONFIG_STORAGE_KEY)!).groupId).toBe('w1');
    });

    it('does not auto-select when a groupId is already set', async () => {
      const stored = { apiUrl: '/api/v1', email: 'a@b.com', groupId: 'existing', accessToken: '' };
      localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(stored));
      const store = await freshStore();
      const ws = [{ id: 'w1', name: 'Personal', user_role: null }];
      fetchWorkspaces.mockResolvedValue(ws);

      await store.getState().loadWorkspaces();

      expect(store.getState().workspaces).toBe(ws);
      expect(store.getState().config.groupId).toBe('existing');
    });

    it('does not auto-select when workspace list is empty', async () => {
      const stored = { apiUrl: '/api/v1', email: 'a@b.com', groupId: '', accessToken: '' };
      localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(stored));
      const store = await freshStore();
      fetchWorkspaces.mockResolvedValue([]);

      await store.getState().loadWorkspaces();

      expect(store.getState().workspaces).toEqual([]);
      expect(store.getState().config.groupId).toBe('');
    });

    it('swallows errors when fetch throws', async () => {
      const stored = { apiUrl: '/api/v1', email: 'a@b.com', groupId: '', accessToken: '' };
      localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(stored));
      const store = await freshStore();
      fetchWorkspaces.mockRejectedValue(new Error('no endpoint'));

      await store.getState().loadWorkspaces();

      expect(store.getState().workspaces).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // loadCatalog branches
  // ---------------------------------------------------------------------------
  describe('loadCatalog', () => {
    it('sets savedCrews and savedFlows from the api modules', async () => {
      const store = await freshStore();
      const crews = [{ id: 'c1', name: 'Crew One' }];
      const flows = [{ id: 'f1', name: 'Flow One' }];
      listSavedCrews.mockResolvedValue(crews);
      listSavedFlows.mockResolvedValue(flows);

      await store.getState().loadCatalog();

      expect(store.getState().savedCrews).toBe(crews);
      expect(store.getState().savedFlows).toBe(flows);
    });

    it('falls back to [] for crews when listSavedCrews rejects', async () => {
      const store = await freshStore();
      listSavedCrews.mockRejectedValue(new Error('no endpoint'));
      listSavedFlows.mockResolvedValue([{ id: 'f1', name: 'Flow One' }]);

      await expect(store.getState().loadCatalog()).resolves.toBeUndefined();

      expect(store.getState().savedCrews).toEqual([]);
      expect(store.getState().savedFlows).toEqual([{ id: 'f1', name: 'Flow One' }]);
    });

    it('falls back to [] for flows when listSavedFlows rejects', async () => {
      const store = await freshStore();
      listSavedCrews.mockResolvedValue([{ id: 'c1', name: 'Crew One' }]);
      listSavedFlows.mockRejectedValue(new Error('no endpoint'));

      await expect(store.getState().loadCatalog()).resolves.toBeUndefined();

      expect(store.getState().savedCrews).toEqual([{ id: 'c1', name: 'Crew One' }]);
      expect(store.getState().savedFlows).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // updateConfig branches
  // ---------------------------------------------------------------------------
  describe('updateConfig', () => {
    it('updates a config field, persists it, and calls updateClient', async () => {
      const store = await freshStore();

      store.getState().updateConfig('accessToken', 'secret');

      expect(store.getState().config.accessToken).toBe('secret');
      expect(updateClient).toHaveBeenCalledWith(
        expect.objectContaining({ accessToken: 'secret' }),
      );
      expect(JSON.parse(localStorage.getItem(CONFIG_STORAGE_KEY)!).accessToken).toBe('secret');
    });

    it('swallows localStorage.setItem error in saveConfig', async () => {
      const store = await freshStore();
      const spy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
        throw new Error('quota');
      });

      expect(() => store.getState().updateConfig('email', 'z@z.com')).not.toThrow();
      expect(store.getState().config.email).toBe('z@z.com');
      spy.mockRestore();
    });
  });

  // ---------------------------------------------------------------------------
  // theme actions
  // ---------------------------------------------------------------------------
  describe('toggleTheme', () => {
    it('toggles from light to dark', async () => {
      localStorage.setItem(THEME_STORAGE_KEY, 'light');
      const store = await freshStore();

      store.getState().toggleTheme();

      expect(store.getState().theme).toBe('dark');
      expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');
    });

    it('toggles from dark to light', async () => {
      localStorage.setItem(THEME_STORAGE_KEY, 'dark');
      const store = await freshStore();

      store.getState().toggleTheme();

      expect(store.getState().theme).toBe('light');
    });
  });

  describe('setTheme', () => {
    it('applies and sets the given theme', async () => {
      const root = document.createElement('div');
      root.id = 'kasal-chat-root';
      document.body.appendChild(root);
      const store = await freshStore();

      store.getState().setTheme('dark');

      expect(store.getState().theme).toBe('dark');
      expect(root.getAttribute('data-theme')).toBe('dark');
    });
  });

  // ---------------------------------------------------------------------------
  // setSelectedModel
  // ---------------------------------------------------------------------------
  describe('setSelectedModel', () => {
    it('sets the model and persists it', async () => {
      const store = await freshStore();

      store.getState().setSelectedModel('chosen');

      expect(store.getState().selectedModel).toBe('chosen');
      expect(localStorage.getItem(MODEL_STORAGE_KEY)).toBe('chosen');
    });

    it('swallows localStorage.setItem error', async () => {
      const store = await freshStore();
      const spy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
        throw new Error('quota');
      });

      expect(() => store.getState().setSelectedModel('chosen')).not.toThrow();
      expect(store.getState().selectedModel).toBe('chosen');
      spy.mockRestore();
    });
  });

  // ---------------------------------------------------------------------------
  // sidebar / settings setters & toggles
  // ---------------------------------------------------------------------------
  describe('sidebar and settings setters/toggles', () => {
    it('setSidebarOpen sets the value', async () => {
      const store = await freshStore();
      store.getState().setSidebarOpen(true);
      expect(store.getState().sidebarOpen).toBe(true);
      store.getState().setSidebarOpen(false);
      expect(store.getState().sidebarOpen).toBe(false);
    });

    it('toggleSidebar flips the value', async () => {
      const store = await freshStore();
      expect(store.getState().sidebarOpen).toBe(false);
      store.getState().toggleSidebar();
      expect(store.getState().sidebarOpen).toBe(true);
      store.getState().toggleSidebar();
      expect(store.getState().sidebarOpen).toBe(false);
    });

    it('setSettingsOpen sets the value', async () => {
      const store = await freshStore();
      store.getState().setSettingsOpen(true);
      expect(store.getState().settingsOpen).toBe(true);
      store.getState().setSettingsOpen(false);
      expect(store.getState().settingsOpen).toBe(false);
    });

    it('toggleSettings flips the value', async () => {
      const store = await freshStore();
      expect(store.getState().settingsOpen).toBe(false);
      store.getState().toggleSettings();
      expect(store.getState().settingsOpen).toBe(true);
      store.getState().toggleSettings();
      expect(store.getState().settingsOpen).toBe(false);
    });
  });
});
