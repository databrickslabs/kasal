import { describe, it, expect, vi } from 'vitest';
import { fetchWorkspaces } from './workspaces';
import { getClient } from './client';

vi.mock('./client', () => ({ getClient: vi.fn() }));
const PERSONAL = 'user_0123456789abcdef0123456789abcdef';

function mockApi(personalId: string | null = PERSONAL, groupsFail = false) {
  const get = vi.fn(async (url: string) => {
    if (url === '/users/me') return { data: { personal_group_id: personalId } };
    if (groupsFail) throw new Error('Groups unavailable');
    return { data: [{ id: 'team', name: 'Team', user_role: 'editor' }] };
  });
  vi.mocked(getClient).mockReturnValue({ get } as never);
  return get;
}

describe('fetchWorkspaces', () => {
  it('uses the authenticated allocation rather than an email-derived ID', async () => {
    const get = mockApi();
    expect(await fetchWorkspaces('person@example.com')).toEqual([
      { id: PERSONAL, name: 'Personal Space', user_role: null },
      { id: 'team', name: 'Team', user_role: 'editor' },
    ]);
    expect(get).toHaveBeenCalledWith('/users/me');
  });

  it('does not use a cached email as an authorization scope', async () => {
    mockApi();
    expect((await fetchWorkspaces('different@example.com'))[0].id).toBe(PERSONAL);
  });

  it('keeps the allocated personal workspace when memberships are unavailable', async () => {
    mockApi(PERSONAL, true);
    expect(await fetchWorkspaces('person@example.com')).toEqual([
      { id: PERSONAL, name: 'Personal Space', user_role: null },
    ]);
  });

  it('fails instead of inventing a personal ID if the server has no allocation', async () => {
    mockApi(null);
    await expect(fetchWorkspaces('person@example.com')).rejects.toThrow('allocation');
  });
});
