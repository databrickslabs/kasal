import { describe, it, expect, vi, beforeEach } from 'vitest';
import { generatePersonalWorkspaceId, fetchWorkspaces } from './workspaces';
import { getClient } from './client';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

const mockedGetClient = vi.mocked(getClient);

describe('generatePersonalWorkspaceId', () => {
  it('replaces @ with underscore and lowercases', () => {
    expect(generatePersonalWorkspaceId('Alice@Example')).toBe('user_alice_example');
  });

  it('replaces all dots, dashes and plus signs and lowercases', () => {
    expect(generatePersonalWorkspaceId('First.Last-Name+Tag@Sub.Domain')).toBe(
      'user_first_last_name_tag_sub_domain',
    );
  });

  it('handles email with no special characters', () => {
    expect(generatePersonalWorkspaceId('user')).toBe('user_user');
  });
});

describe('fetchWorkspaces', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns personal workspace first then groups when email is provided', async () => {
    const get = vi.fn().mockResolvedValue({
      data: [
        {
          id: 'group-1',
          name: 'Engineering',
          status: 'active',
          description: null,
          auto_created: false,
          created_by_email: null,
          created_at: '2024-01-01',
          updated_at: '2024-01-02',
          user_count: 5,
          user_role: 'admin',
        },
        {
          id: 'group-2',
          name: 'Design',
          status: 'active',
          description: 'desc',
          auto_created: true,
          created_by_email: 'a@b.com',
          created_at: '2024-01-01',
          updated_at: '2024-01-02',
          user_count: 3,
          user_role: null,
        },
      ],
    });
    mockedGetClient.mockReturnValue({ get } as never);

    const result = await fetchWorkspaces('john.doe@example.com');

    expect(get).toHaveBeenCalledWith('/groups/my-groups');
    expect(result).toEqual([
      {
        id: 'user_john_doe_example_com',
        name: 'Personal Space',
        user_role: null,
      },
      { id: 'group-1', name: 'Engineering', user_role: 'admin' },
      { id: 'group-2', name: 'Design', user_role: null },
    ]);
  });

  it('does not add personal workspace when email is empty', async () => {
    const get = vi.fn().mockResolvedValue({ data: [] });
    mockedGetClient.mockReturnValue({ get } as never);

    const result = await fetchWorkspaces('');

    expect(get).toHaveBeenCalledWith('/groups/my-groups');
    expect(result).toEqual([]);
  });

  it('still returns personal workspace when /groups/my-groups throws', async () => {
    const get = vi.fn().mockRejectedValue(new Error('not available'));
    mockedGetClient.mockReturnValue({ get } as never);

    const result = await fetchWorkspaces('jane@example.com');

    expect(get).toHaveBeenCalledWith('/groups/my-groups');
    expect(result).toEqual([
      {
        id: 'user_jane_example_com',
        name: 'Personal Space',
        user_role: null,
      },
    ]);
  });
});
