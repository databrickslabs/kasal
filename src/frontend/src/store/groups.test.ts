import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { GroupWithRole } from '../api/GroupService';

const getMyGroups = vi.fn();

vi.mock('../api/GroupService', () => ({
  GroupService: { getInstance: () => ({ getMyGroups }) },
}));

vi.mock('./user', () => ({
  useUserStore: {
    getState: () => ({ currentUser: { email: 'nehme.tohme@databricks.com' } }),
  },
}));

// Personal workspace id the store derives from the user's email.
const PERSONAL = 'user_nehme_tohme_databricks_com';

async function freshStore() {
  vi.resetModules();
  const mod = await import('./groups');
  return mod.useGroupStore;
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

describe('useGroupStore.fetchMyGroups — stored-group validation', () => {
  it('falls back to the personal workspace when the stored group no longer exists', async () => {
    // e.g. right after a redeploy the workspace list is temporarily empty (Lakebase
    // not reconnected yet), so the previously-selected workspace is gone.
    localStorage.setItem('selectedGroupId', 'marketing_53f80242');
    getMyGroups.mockResolvedValue([]);
    const useGroupStore = await freshStore();

    await useGroupStore.getState().fetchMyGroups();

    expect(useGroupStore.getState().currentGroupId).toBe(PERSONAL);
    expect(localStorage.getItem('selectedGroupId')).toBe(PERSONAL);
  });

  it('keeps a valid stored group', async () => {
    localStorage.setItem('selectedGroupId', 'marketing_53f80242');
    getMyGroups.mockResolvedValue([
      { id: 'marketing_53f80242', name: 'Marketing', status: 'active' },
    ] as unknown as GroupWithRole[]);
    const useGroupStore = await freshStore();

    await useGroupStore.getState().fetchMyGroups();

    expect(useGroupStore.getState().currentGroupId).toBe('marketing_53f80242');
    expect(localStorage.getItem('selectedGroupId')).toBe('marketing_53f80242');
  });

  it('defaults to the personal workspace when nothing is stored', async () => {
    getMyGroups.mockResolvedValue([]);
    const useGroupStore = await freshStore();

    await useGroupStore.getState().fetchMyGroups();

    expect(useGroupStore.getState().currentGroupId).toBe(PERSONAL);
    expect(localStorage.getItem('selectedGroupId')).toBe(PERSONAL);
  });
});
