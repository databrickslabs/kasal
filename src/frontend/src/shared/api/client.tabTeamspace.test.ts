/**
 * Each browser tab works in its own teamspace.
 *
 * localStorage is shared by every tab of the origin, so a "tab" here is a fresh
 * module graph (vi.resetModules) with its own sessionStorage, over one shared
 * localStorage, exactly as the browser lays it out. Requests go through the
 * real axios client with a capturing adapter instead of the network.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AxiosError, type AxiosAdapter, type InternalAxiosRequestConfig } from 'axios';

// The real client and interceptors (setupTests stubs axios for everyone else).
vi.unmock('axios');

const groupsApi = vi.hoisted(() => ({ getMyGroups: vi.fn() }));
vi.mock('../../api/groups/GroupService', () => ({
  GroupService: { getInstance: () => groupsApi },
}));

const realSessionStorage = window.sessionStorage;

class TabStorage implements Storage {
  private items = new Map<string, string>();
  get length() { return this.items.size; }
  clear() { this.items.clear(); }
  getItem(key: string) { return this.items.has(key) ? this.items.get(key)! : null; }
  key(index: number) { return Array.from(this.items.keys())[index] ?? null; }
  removeItem(key: string) { this.items.delete(key); }
  setItem(key: string, value: string) { this.items.set(key, String(value)); }
}

interface Tab {
  storage: Storage;
  sent: InternalAxiosRequestConfig[];
  client: typeof import('./client').apiClient;
  groups: typeof import('../../store/groups').useGroupStore;
  user: typeof import('../../store/user').useUserStore;
}

/**
 * Open a tab: its own sessionStorage and module instances, the shared
 * localStorage. Pass an earlier tab's storage to reload that tab.
 */
async function openTab(
  respond?: (config: InternalAxiosRequestConfig) => Promise<unknown>, storage: Storage = new TabStorage(),
): Promise<Tab> {
  Object.defineProperty(window, 'sessionStorage', { value: storage, configurable: true });
  vi.resetModules();
  const { apiClient } = await import('./client');
  const { useGroupStore } = await import('../../store/groups');
  const { useUserStore } = await import('../../store/user');
  const sent: InternalAxiosRequestConfig[] = [];
  const adapter: AxiosAdapter = async (config) => {
    sent.push(config);
    if (respond) await respond(config);
    return { data: {}, status: 200, statusText: 'OK', headers: {}, config };
  };
  apiClient.defaults.adapter = adapter;
  return { storage, sent, client: apiClient, groups: useGroupStore, user: useUserStore };
}

const groupHeader = (config: InternalAxiosRequestConfig) => config.headers?.get?.('group_id') ?? config.headers?.group_id;

const signIn = (tab: Tab) =>
  tab.user.setState({ currentUser: { email: 'person@example.com', personal_group_id: 'personal' } as never });

beforeEach(() => {
  localStorage.clear();
  groupsApi.getMyGroups.mockReset().mockResolvedValue([]);
});

afterEach(() => {
  Object.defineProperty(window, 'sessionStorage', { value: realSessionStorage, configurable: true });
});

describe('per-tab teamspace', () => {
  it('keeps each tab on its own teamspace after another tab switches', async () => {
    localStorage.setItem('selectedGroupId', 'team_a');
    const tab1 = await openTab();
    const tab2 = await openTab();

    // Tab 2 switches. The browser tells the other tabs through a storage event.
    tab2.groups.getState().setCurrentGroup('team_b');
    window.dispatchEvent(new StorageEvent('storage', { key: 'selectedGroupId', oldValue: 'team_a', newValue: 'team_b' }));

    await tab1.client.post('/crews', { name: 'saved from tab 1' });
    await tab2.client.post('/crews', { name: 'saved from tab 2' });

    expect(groupHeader(tab1.sent[0])).toBe('team_a');
    expect(groupHeader(tab2.sent[0])).toBe('team_b');
    // Tabs opened later start from the last selection.
    expect(localStorage.getItem('selectedGroupId')).toBe('team_b');
  });

  it('does not write its own teamspace back over another tab\'s selection when it reloads its groups', async () => {
    localStorage.setItem('selectedGroupId', 'team_a');
    const tab1 = await openTab();
    const tab2 = await openTab();
    tab2.groups.getState().setCurrentGroup('team_b');

    tab1.user.setState({ currentUser: { email: 'person@example.com', personal_group_id: 'team_a' } as never });
    await tab1.groups.getState().fetchMyGroups();

    expect(tab1.groups.getState().currentGroupId).toBe('team_a');
    expect(localStorage.getItem('selectedGroupId')).toBe('team_b');
  });
});

describe('a tab opened before any teamspace was chosen', () => {
  it('keeps its own teamspace when another tab switches before its groups load', async () => {
    const tab1 = await openTab();
    const tab2 = await openTab();
    signIn(tab1); signIn(tab2);
    let finishTab2Groups!: (groups: unknown[]) => void;
    groupsApi.getMyGroups.mockImplementationOnce(() => new Promise(resolve => { finishTab2Groups = resolve; }));
    const tab2Groups = tab2.groups.getState().fetchMyGroups();

    await tab1.groups.getState().fetchMyGroups();
    tab1.groups.getState().setCurrentGroup('team_b');
    finishTab2Groups([{ id: 'team_b', name: 'B', status: 'active' }]);
    await tab2Groups;
    tab1.groups.getState().setCurrentGroup('team_c');
    await tab2.client.post('/crews', { name: 'saved from tab 2' });

    expect(tab2.groups.getState().currentGroupId).toBe('personal');
    expect(groupHeader(tab2.sent[tab2.sent.length - 1])).toBe('personal');
    // Tab 2 did not overwrite tab 1's choice as the default for new tabs.
    expect(localStorage.getItem('selectedGroupId')).toBe('team_c');
  });
});

describe('after the tab loses access to its teamspace', () => {
  const denyGone = async (config: InternalAxiosRequestConfig) => {
    if ((config.method ?? 'get') === 'get' && groupHeader(config) === 'team_gone') {
      throw new AxiosError('denied', 'ERR_BAD_REQUEST', config, null, {
        status: 403, statusText: 'Forbidden', headers: {}, config,
        data: { detail: 'Access denied: User does not have access to group team_gone' },
      });
    }
  };

  it('refuses a write instead of sending it without a teamspace, until the user picks one', async () => {
    localStorage.setItem('selectedGroupId', 'team_gone');
    const tab = await openTab(denyGone);
    // Same module graph as the tab's client (openTab reset the registry).
    const { subscribeToApiFailures } = await import('./errors');
    const failures = vi.fn();
    const unsubscribe = subscribeToApiFailures(failures);

    // The read recovers, as before.
    await tab.client.get('/crews');
    const sentBeforeWrite = tab.sent.length;

    await expect(tab.client.post('/crews', { name: 'x' })).rejects.toThrow(/teamspace/i);
    await expect(tab.client.put('/tasks/1', { tools: [] })).rejects.toThrow(/teamspace/i);
    unsubscribe();

    expect(tab.sent).toHaveLength(sentBeforeWrite);
    expect(tab.sent.some(config => config.method !== 'get')).toBe(false);
    expect(failures).toHaveBeenCalledWith(expect.objectContaining({ code: 'teamspace_required' }));

    // Picking a teamspace lets writes through again, under that teamspace.
    tab.groups.getState().setCurrentGroup('team_b');
    await tab.client.post('/crews', { name: 'x' });
    expect(groupHeader(tab.sent[tab.sent.length - 1])).toBe('team_b');
  });

  it('still refuses writes after that tab is reloaded', async () => {
    localStorage.setItem('selectedGroupId', 'team_gone');
    const tab = await openTab(denyGone);
    await tab.client.get('/crews');

    const reloaded = await openTab(undefined, tab.storage);

    await expect(reloaded.client.post('/crews', { name: 'x' })).rejects.toThrow(/teamspace/i);
    expect(reloaded.sent).toHaveLength(0);
  });

  it('keeps the tab\'s teamspace when a request that carried none is refused', async () => {
    localStorage.setItem('selectedGroupId', 'team_a');
    const tab = await openTab(async (config) => {
      if (config.url === '/users/me' && !groupHeader(config)) {
        throw new AxiosError('denied', 'ERR_BAD_REQUEST', config, null, {
          status: 403, statusText: 'Forbidden', headers: {}, config,
          data: { detail: 'No access to group' },
        });
      }
    });
    await expect(tab.client.get('/users/me')).rejects.toBeTruthy();

    await tab.client.post('/crews', { name: 'x' });

    expect(groupHeader(tab.sent[tab.sent.length - 1])).toBe('team_a');
  });
});
