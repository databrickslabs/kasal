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

export function generatePersonalWorkspaceId(email: string): string {
  const sanitized = email
    .replace('@', '_')
    .replace(/\./g, '_')
    .replace(/-/g, '_')
    .replace(/\+/g, '_');
  return `user_${sanitized}`.toLowerCase();
}

export async function fetchWorkspaces(email: string): Promise<Workspace[]> {
  const workspaces: Workspace[] = [];

  // Always add personal workspace first
  if (email) {
    workspaces.push({
      id: generatePersonalWorkspaceId(email),
      name: 'Personal Workspace',
      user_role: null,
    });
  }

  try {
    const response = await getClient().get<GroupWithRoleResponse[]>('/groups/my-groups');
    for (const group of response.data) {
      workspaces.push({
        id: group.id,
        name: group.name,
        user_role: group.user_role,
      });
    }
  } catch {
    // Groups endpoint may not be available
  }

  return workspaces;
}
