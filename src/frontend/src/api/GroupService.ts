import { ApiService } from './ApiService';

// Types for group management
export interface Group {
  id: string;
  name: string;
  email_domain: string;
  status: 'active' | 'suspended' | 'archived';
  description?: string;
  auto_created: boolean;
  created_by_email?: string;
  created_at: string;
  updated_at: string;
  user_count: number;
}

export interface GroupUser {
  id: string;
  group_id: string;
  user_id: string;
  email: string;
  role: 'admin' | 'manager' | 'user' | 'viewer';
  status: 'active' | 'inactive' | 'suspended';
  joined_at: string;
  auto_created: boolean;
  created_at: string;
  updated_at: string;
}

export interface CreateGroupRequest {
  name: string;
  email_domain: string;
  description?: string;
}

export interface UpdateGroupRequest {
  name?: string;
  description?: string;
  status?: 'active' | 'suspended' | 'archived';
}

export interface AssignUserRequest {
  user_email: string;
  role: 'admin' | 'manager' | 'user' | 'viewer';
}

export interface UpdateGroupUserRequest {
  role?: 'admin' | 'manager' | 'user' | 'viewer';
  status?: 'active' | 'inactive' | 'suspended';
}

export class GroupService {
  private static instance: GroupService;

  private constructor() {
    // No need to store ApiService as instance variable since it's a static object
  }

  public static getInstance(): GroupService {
    if (!GroupService.instance) {
      GroupService.instance = new GroupService();
    }
    return GroupService.instance;
  }

  /**
   * Get all groups
   */
  async getGroups(skip = 0, limit = 100): Promise<Group[]> {
    try {
      const response = await ApiService.get(`/groups/?skip=${skip}&limit=${limit}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching groups:', error);
      throw new Error('Failed to fetch groups');
    }
  }

  /**
   * Get a specific group by ID
   */
  async getGroup(groupId: string): Promise<Group> {
    try {
      const response = await ApiService.get(`/groups/${groupId}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching group ${groupId}:`, error);
      throw new Error('Failed to fetch group');
    }
  }

  /**
   * Create a new group
   */
  async createGroup(groupData: CreateGroupRequest): Promise<Group> {
    try {
      const response = await ApiService.post('/groups/', groupData);
      return response.data;
    } catch (error) {
      console.error('Error creating group:', error);
      throw new Error('Failed to create group');
    }
  }

  /**
   * Update a group
   */
  async updateGroup(groupId: string, groupData: UpdateGroupRequest): Promise<Group> {
    try {
      const response = await ApiService.put(`/groups/${groupId}`, groupData);
      return response.data;
    } catch (error) {
      console.error(`Error updating group ${groupId}:`, error);
      throw new Error('Failed to update group');
    }
  }

  /**
   * Delete a group
   */
  async deleteGroup(groupId: string): Promise<void> {
    try {
      await ApiService.delete(`/groups/${groupId}`);
    } catch (error) {
      console.error(`Error deleting group ${groupId}:`, error);
      throw new Error('Failed to delete group');
    }
  }

  /**
   * Get users in a group
   */
  async getGroupUsers(groupId: string, skip = 0, limit = 100): Promise<GroupUser[]> {
    try {
      const response = await ApiService.get(`/groups/${groupId}/users?skip=${skip}&limit=${limit}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching users for group ${groupId}:`, error);
      throw new Error('Failed to fetch group users');
    }
  }

  /**
   * Assign a user to a group
   */
  async assignUserToGroup(groupId: string, userData: AssignUserRequest): Promise<GroupUser> {
    try {
      const response = await ApiService.post(`/groups/${groupId}/users`, userData);
      return response.data;
    } catch (error) {
      console.error(`Error assigning user to group ${groupId}:`, error);
      throw new Error('Failed to assign user to group');
    }
  }

  /**
   * Update a user's role/status in a group
   */
  async updateGroupUser(
    groupId: string, 
    userId: string, 
    userData: UpdateGroupUserRequest
  ): Promise<GroupUser> {
    try {
      const response = await ApiService.put(`/groups/${groupId}/users/${userId}`, userData);
      return response.data;
    } catch (error) {
      console.error(`Error updating user ${userId} in group ${groupId}:`, error);
      throw new Error('Failed to update group user');
    }
  }

  /**
   * Remove a user from a group
   */
  async removeUserFromGroup(groupId: string, userId: string): Promise<void> {
    try {
      await ApiService.delete(`/groups/${groupId}/users/${userId}`);
    } catch (error) {
      console.error(`Error removing user ${userId} from group ${groupId}:`, error);
      throw new Error('Failed to remove user from group');
    }
  }

  /**
   * Get group statistics
   */
  async getGroupStats(): Promise<{
    total_groups: number;
    active_groups: number;
    total_users: number;
    groups_by_status: Record<string, number>;
  }> {
    try {
      const response = await ApiService.get('/groups/stats');
      return response.data;
    } catch (error) {
      console.error('Error fetching group statistics:', error);
      throw new Error('Failed to fetch group statistics');
    }
  }

  // Legacy compatibility methods (can be removed after full migration)
  async getTenants(skip = 0, limit = 100): Promise<Group[]> {
    return this.getGroups(skip, limit);
  }

  async getTenant(tenantId: string): Promise<Group> {
    return this.getGroup(tenantId);
  }

  async createTenant(tenantData: CreateGroupRequest): Promise<Group> {
    return this.createGroup(tenantData);
  }

  async updateTenant(tenantId: string, tenantData: UpdateGroupRequest): Promise<Group> {
    return this.updateGroup(tenantId, tenantData);
  }

  async deleteTenant(tenantId: string): Promise<void> {
    return this.deleteGroup(tenantId);
  }

  async getTenantUsers(tenantId: string, skip = 0, limit = 100): Promise<GroupUser[]> {
    return this.getGroupUsers(tenantId, skip, limit);
  }

  async assignUserToTenant(tenantId: string, userData: AssignUserRequest): Promise<GroupUser> {
    return this.assignUserToGroup(tenantId, userData);
  }

  async updateTenantUser(
    tenantId: string, 
    userId: string, 
    userData: UpdateGroupUserRequest
  ): Promise<GroupUser> {
    return this.updateGroupUser(tenantId, userId, userData);
  }

  async removeUserFromTenant(tenantId: string, userId: string): Promise<void> {
    return this.removeUserFromGroup(tenantId, userId);
  }

  async getTenantStats(): Promise<{
    total_groups: number;
    active_groups: number;
    total_users: number;
    groups_by_status: Record<string, number>;
  }> {
    return this.getGroupStats();
  }
}