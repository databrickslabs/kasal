import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { GroupWithRole } from '../api/groups/GroupService';

const getMyGroups = vi.fn();

vi.mock('../api/groups/GroupService', () => ({
  GroupService: { getInstance: () => ({ getMyGroups }) },
}));

vi.mock('./user', () => ({
  useUserStore: {
    getState: () => ({ currentUser: { email: 'person@example.com', personal_group_id: 'user_0123456789abcdef0123456789abcdef' } }),
  },
}));

// Opaque ID supplied by the authenticated-user API.
const PERSONAL = 'user_0123456789abcdef0123456789abcdef';

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
    // A successful authoritative response confirms membership was removed.
    localStorage.setItem('selectedGroupId', 'marketing_53f80242');
    getMyGroups.mockResolvedValue([]);
    const useGroupStore = await freshStore();

    await useGroupStore.getState().fetchMyGroups();

    expect(useGroupStore.getState().currentGroupId).toBe(PERSONAL);
    expect(localStorage.getItem('selectedGroupId')).toBe(PERSONAL);
  });

  it('repairs a cached email-derived ID and notifies execution-history listeners', async () => {
    localStorage.setItem('selectedGroupId', 'user_person_example_com');
    getMyGroups.mockResolvedValue([]);
    const onChange = vi.fn();
    window.addEventListener('group-changed', onChange);
    try {
      const store = await freshStore();
      await store.getState().fetchMyGroups();
      expect(store.getState().currentGroupId).toBe(PERSONAL);
      expect(localStorage.getItem('selectedGroupId')).toBe(PERSONAL);
      expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ detail: { groupId: PERSONAL } }));
    } finally { window.removeEventListener('group-changed', onChange); }
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

it('preserves the teamspace and cached groups when membership lookup fails', async () => {
  localStorage.setItem('selectedGroupId', 'team');
  const store = await freshStore();
  const groups = [{ id: 'team', name: 'Team' }] as GroupWithRole[];
  store.setState({ groups });
  getMyGroups.mockRejectedValueOnce(new Error('temporary 503'));
  const onChange = vi.fn();
  window.addEventListener('group-changed', onChange);
  try {
    await store.getState().fetchMyGroups();
    expect(store.getState().currentGroupId).toBe('team');
    expect(store.getState().groups).toEqual(groups);
    expect(localStorage.getItem('selectedGroupId')).toBe('team');
    expect(onChange).not.toHaveBeenCalled();
    expect(store.getState().isLoading).toBe(false);
  } finally { window.removeEventListener('group-changed', onChange); }
});
it('ignores an older membership response that would reset the current teamspace', async () => {
  localStorage.setItem('selectedGroupId', 'team');
  const store = await freshStore();
  let finish!: (groups: GroupWithRole[]) => void;
  getMyGroups.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  const old = store.getState().fetchMyGroups();
  getMyGroups.mockResolvedValueOnce([{ id: 'team', name: 'Team' }]);
  await store.getState().fetchMyGroups();
  finish([]); await old;
  expect(store.getState().currentGroupId).toBe('team');
  expect(store.getState().groups.map(g => g.id)).toContain('team');
});
