import { create } from 'zustand';
import { AppConfig } from '../types/chat';
import { ModelConfigResponse } from '../types/dispatcher';
import { updateClient } from '../api/client';
import { fetchEnabledModels } from '../api/models';
import { fetchEnabledTools, ToolInfo } from '../api/tools';
import { fetchWorkspaces, Workspace } from '../api/workspaces';
import { listSavedCrews, listSavedFlows, CatalogItem } from '../api/crews';

const CONFIG_STORAGE_KEY = 'kasal-chat-config';
const MODEL_STORAGE_KEY = 'kasal-chat-model';
const THEME_STORAGE_KEY = 'kasal-chat-theme';

export type Theme = 'light' | 'dark';

function getStoredTheme(): Theme {
  try {
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === 'dark' || stored === 'light') return stored;
  } catch { /* */ }
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches
    ? 'dark'
    : 'light';
}

function applyTheme(theme: Theme): void {
  // Scope the theme to the chat container only, so it never overrides the
  // Material-UI theme applied to the rest of Kasal. Falls back to the root
  // element if the chat container isn't mounted yet.
  const chatRoot = document.getElementById('kasal-chat-root');
  if (chatRoot) {
    chatRoot.setAttribute('data-theme', theme);
  }
  try {
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch { /* */ }
}

function getDefaultApiUrl(): string {
  return import.meta.env.VITE_KASAL_API_URL || '/api/v1';
}

function loadConfig(): AppConfig {
  const defaultUrl = getDefaultApiUrl();
  try {
    const stored = localStorage.getItem(CONFIG_STORAGE_KEY);
    if (stored) {
      const parsed = JSON.parse(stored);
      if (parsed.apiUrl && parsed.apiUrl.startsWith('http')) {
        parsed.apiUrl = defaultUrl;
      }
      return parsed;
    }
  } catch {
    // ignore
  }
  return { apiUrl: defaultUrl, email: '', groupId: '', accessToken: '' };
}

function saveConfig(config: AppConfig): void {
  try {
    localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(config));
  } catch {
    // ignore
  }
}

interface AppState {
  config: AppConfig;
  theme: Theme;
  models: ModelConfigResponse[];
  tools: ToolInfo[];
  /** Map of tool ID (number or string) → tool title for quick lookup */
  toolNameMap: Record<string, string>;
  workspaces: Workspace[];
  /** Saved catalog shown in the rail library (replaces /list crews & /list flows) */
  savedCrews: CatalogItem[];
  savedFlows: CatalogItem[];
  selectedModel: string;
  sidebarOpen: boolean;
  settingsOpen: boolean;
}

interface AppActions {
  init: () => void;
  loadModels: () => Promise<void>;
  loadTools: () => Promise<void>;
  loadWorkspaces: () => Promise<void>;
  loadCatalog: () => Promise<void>;
  updateConfig: (field: keyof AppConfig, value: string) => void;
  toggleTheme: () => void;
  setTheme: (theme: Theme) => void;
  setSelectedModel: (model: string) => void;
  setSidebarOpen: (open: boolean) => void;
  toggleSidebar: () => void;
  setSettingsOpen: (open: boolean) => void;
  toggleSettings: () => void;
}

type AppStore = AppState & AppActions;

export const useAppStore = create<AppStore>((set, get) => ({
  // --- State ---
  config: loadConfig(),
  theme: getStoredTheme(),
  models: [],
  tools: [],
  toolNameMap: {},
  workspaces: [],
  savedCrews: [],
  savedFlows: [],
  selectedModel: (() => {
    try {
      return localStorage.getItem(MODEL_STORAGE_KEY) || '';
    } catch {
      return '';
    }
  })(),
  sidebarOpen: false,
  settingsOpen: false,

  // --- Actions ---
  init: () => {
    const state = get();
    applyTheme(state.theme);
    updateClient(state.config);
  },

  loadModels: async () => {
    try {
      const m = await fetchEnabledModels();
      const state = get();
      set({ models: m });
      if (m.length > 0 && !state.selectedModel) {
        const key = m[0].key;
        set({ selectedModel: key });
        try {
          localStorage.setItem(MODEL_STORAGE_KEY, key);
        } catch { /* */ }
      }
    } catch {
      // Models endpoint may not be available
    }
  },

  loadTools: async () => {
    try {
      const tools = await fetchEnabledTools();
      const nameMap: Record<string, string> = {};
      tools.forEach((t) => {
        nameMap[String(t.id)] = t.title;
        nameMap[t.title] = t.title; // identity mapping for tools already by name
      });
      set({ tools, toolNameMap: nameMap });
    } catch {
      // Tools endpoint may not be available
    }
  },

  loadWorkspaces: async () => {
    try {
      const state = get();
      if (!state.config.email) return;
      const ws = await fetchWorkspaces(state.config.email);
      set({ workspaces: ws });
      // Auto-select personal workspace if none selected
      if (ws.length > 0 && !state.config.groupId) {
        const updated = { ...state.config, groupId: ws[0].id };
        saveConfig(updated);
        updateClient(updated);
        set({ config: updated });
      }
    } catch {
      // Workspaces endpoint may not be available
    }
  },

  loadCatalog: async () => {
    const [crews, flows] = await Promise.all([
      listSavedCrews().catch(() => [] as CatalogItem[]),
      listSavedFlows().catch(() => [] as CatalogItem[]),
    ]);
    set({ savedCrews: crews, savedFlows: flows });
  },

  updateConfig: (field, value) => {
    set((state) => {
      const updated = { ...state.config, [field]: value };
      saveConfig(updated);
      updateClient(updated);
      return { config: updated };
    });
  },

  toggleTheme: () => {
    set((state) => {
      const next: Theme = state.theme === 'dark' ? 'light' : 'dark';
      applyTheme(next);
      return { theme: next };
    });
  },

  // Sync the chat theme from Kasal's theme store (dark-mode toggle).
  setTheme: (theme: Theme) => {
    applyTheme(theme);
    set({ theme });
  },

  setSelectedModel: (model) => {
    set({ selectedModel: model });
    try {
      localStorage.setItem(MODEL_STORAGE_KEY, model);
    } catch { /* */ }
  },

  setSidebarOpen: (open) => set({ sidebarOpen: open }),
  toggleSidebar: () => set((s) => ({ sidebarOpen: !s.sidebarOpen })),
  setSettingsOpen: (open) => set({ settingsOpen: open }),
  toggleSettings: () => set((s) => ({ settingsOpen: !s.settingsOpen })),
}));
