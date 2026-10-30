/**
 * Every user, not the first page.
 *
 * Teamspace management tells Personal Spaces apart by the users' stored ids;
 * GET /users answers a page at a time, so with more users than one page the
 * later users' Personal Spaces read as teamspaces.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { apiClient } from '../../shared/api/client';
import { UserService } from './UserService';

vi.mock('../../shared/api/client', () => ({ apiClient: { get: vi.fn() } }));

const user = (n: number) => ({ id: `u${n}`, email: `user${n}@example.com`, personal_group_id: `pg${n}` });

beforeEach(() => {
  vi.mocked(apiClient.get).mockReset();
});

describe('UserService.getAllUsers', () => {
  it('reads page after page until a short one', async () => {
    vi.mocked(apiClient.get)
      .mockResolvedValueOnce({ data: [user(1), user(2)] })
      .mockResolvedValueOnce({ data: [user(3), user(4)] })
      .mockResolvedValueOnce({ data: [user(5)] });

    const users = await UserService.getInstance().getAllUsers(2);

    expect(users.map(u => u.id)).toEqual(['u1', 'u2', 'u3', 'u4', 'u5']);
    const skips = vi.mocked(apiClient.get).mock.calls.map(call => (call[1] as { params: { skip: number } }).params.skip);
    expect(skips).toEqual([0, 2, 4]);
  });
});
