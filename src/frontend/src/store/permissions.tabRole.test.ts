/**
 * Each browser tab keeps its own role across a reload.
 *
 * The role depends on the tab's teamspace, and each tab has its own (see
 * shared/api/tabGroup.ts). The persisted role lived in localStorage, which
 * every tab shares, so reloading one tab briefly showed the role another tab
 * had loaded for a different teamspace. A "tab" here is a fresh module graph
 * (vi.resetModules) with its own sessionStorage over one shared localStorage,
 * as the browser lays it out.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api/groups/GroupService', () => ({
  GroupService: { getInstance: () => ({ getMyGroups: vi.fn().mockResolvedValue([]) }) },
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

/** Open (or, given an earlier tab's storage, reload) a tab; return its permission store. */
async function openTab(storage: Storage) {
  Object.defineProperty(window, 'sessionStorage', { value: storage, configurable: true });
  vi.resetModules();
  const { usePermissionStore } = await import('./permissions');
  return usePermissionStore;
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  Object.defineProperty(window, 'sessionStorage', { value: realSessionStorage, configurable: true });
});

describe('permission store per tab', () => {
  it('two tabs with different roles each keep their own role across a reload', async () => {
    const adminTab = new TabStorage();
    const operatorTab = new TabStorage();

    (await openTab(adminTab)).getState().setUserRole('admin');
    (await openTab(operatorTab)).getState().setUserRole('operator');

    // Reload the first tab: it shows its own role, not the one the other tab loaded last.
    expect((await openTab(adminTab)).getState().userRole).toBe('admin');
    expect((await openTab(operatorTab)).getState().userRole).toBe('operator');
  });

  it('keeps the role out of the storage that every tab shares', async () => {
    (await openTab(new TabStorage())).getState().setUserRole('admin');
    expect(localStorage.getItem('permission-store-v2')).toBeNull();
  });

  it('clears the role an older version left in shared storage', async () => {
    localStorage.setItem('permission-store-v2', JSON.stringify({ state: { userRole: 'admin' }, version: 0 }));

    const store = await openTab(new TabStorage());

    expect(localStorage.getItem('permission-store-v2')).toBeNull();
    expect(store.getState().userRole).toBeNull();
  });
});
