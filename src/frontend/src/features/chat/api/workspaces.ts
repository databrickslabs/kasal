import { getClient } from './client';

export interface Workspace {
  id: string;
  name: string;
  user_role: string | null;
}

interface GroupWithRoleResponse {
  id: string;
  name: string;
  status: string;
  description: string | null;
  auto_created: boolean;
  created_by_email: string | null;
  created_at: string;
  updated_at: string;
  user_count: number;
  user_role: string | null;
}

/** Personal workspace IDs are allocated by the server, never inferred from email. */
export async function fetchWorkspaces(_email: string): Promise<Workspace[]> {
  const client = getClient();
  const [identity, groups] = await Promise.all([
    client.get<{ personal_group_id?: string | null }>('/users/me'),
    client.get<GroupWithRoleResponse[]>('/groups/my-groups').catch(() => ({ data: [] })),
  ]);
  const personalId = identity.data.personal_group_id;
  if (!personalId) throw new Error('Personal workspace allocation is unavailable');
  return [
    { id: personalId, name: 'Personal Space', user_role: null },
    ...groups.data.filter(group => group.id !== personalId).map(group => ({
      id: group.id, name: group.name, user_role: group.user_role,
    })),
  ];
}
