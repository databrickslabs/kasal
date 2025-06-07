import { ApiService } from './ApiService';

export interface Role {
  id: string;
  name: string;
  description: string;
  created_at: string;
  updated_at: string;
}

export interface Privilege {
  id: string;
  name: string;
  description: string;
  created_at: string;
}

export interface UserRole {
  id: string;
  user_id: string;
  role_id: string;
  assigned_at: string;
  assigned_by?: string;
  user_email?: string;
  role_name?: string;
}

export interface User {
  id: string;
  username: string;
  email: string;
  status: string;
  created_at: string;
  roles?: Role[];
  privileges?: string[];
}

export interface DatabricksAdminSyncResult {
  success: boolean;
  admin_emails: string[];
  processed_users: Array<{
    email: string;
    user_created: boolean;
    role_assigned: boolean;
    already_admin: boolean;
    error?: string;
  }>;
  errors: string[];
}

export interface AdminEmailsResult {
  admin_emails: string[];
  source: 'databricks' | 'fallback';
  is_local_dev: boolean;
  databricks_config: {
    app_name: string;
    host_configured: boolean;
    token_configured: boolean;
  };
}

export interface UserAdminStatus {
  email: string;
  user_exists: boolean;
  user_id?: string;
  has_admin_access: boolean;
  admin_tenants?: string[];
}

export class RoleService {
  private static instance: RoleService;

  private constructor() {
    // Private constructor for singleton pattern
  }

  public static getInstance(): RoleService {
    if (!RoleService.instance) {
      RoleService.instance = new RoleService();
    }
    return RoleService.instance;
  }

  // Roles API
  async getRoles(): Promise<Role[]> {
    const response = await ApiService.get('/roles/');
    return response.data || [];
  }

  async getRole(roleId: string): Promise<Role> {
    const response = await ApiService.get(`/roles/${roleId}`);
    return response.data;
  }

  async createRole(role: Omit<Role, 'id' | 'created_at' | 'updated_at'>): Promise<Role> {
    const response = await ApiService.post('/roles/', role);
    return response.data;
  }

  async updateRole(roleId: string, role: Partial<Role>): Promise<Role> {
    const response = await ApiService.put(`/roles/${roleId}`, role);
    return response.data;
  }

  async deleteRole(roleId: string): Promise<void> {
    await ApiService.delete(`/roles/${roleId}`);
  }

  // Privileges API
  async getPrivileges(): Promise<Privilege[]> {
    const response = await ApiService.get('/privileges/');
    return response.data || [];
  }

  async getRolePrivileges(roleId: string): Promise<Privilege[]> {
    const response = await ApiService.get(`/roles/${roleId}/privileges`);
    return response.data || [];
  }

  async assignPrivilegeToRole(roleId: string, privilegeId: string): Promise<void> {
    await ApiService.post(`/roles/${roleId}/privileges`, { privilege_id: privilegeId });
  }

  async removePrivilegeFromRole(roleId: string, privilegeId: string): Promise<void> {
    await ApiService.delete(`/roles/${roleId}/privileges/${privilegeId}`);
  }

  // Users API
  async getUsers(): Promise<User[]> {
    const response = await ApiService.get('/users/');
    return response.data || [];
  }

  async getUser(userId: string): Promise<User> {
    const response = await ApiService.get(`/users/${userId}`);
    return response.data;
  }

  async getUserRoles(userId: string): Promise<Role[]> {
    const response = await ApiService.get(`/user-roles/users/${userId}/roles`);
    return response.data || [];
  }

  async getUserPrivileges(userId: string): Promise<string[]> {
    const response = await ApiService.get(`/user-roles/users/${userId}/privileges`);
    return response.data || [];
  }

  async assignRoleToUser(userId: string, roleId: string): Promise<void> {
    await ApiService.post(`/user-roles/`, { user_id: userId, role_id: roleId });
  }

  async removeRoleFromUser(userId: string, roleId: string): Promise<void> {
    await ApiService.delete(`/user-roles/${userId}/roles/${roleId}`);
  }

  // User-Role assignments
  async getUserRoleAssignments(): Promise<UserRole[]> {
    const response = await ApiService.get('/user-roles/');
    return response.data || [];
  }

  async getUsersWithRole(roleName: string): Promise<User[]> {
    const response = await ApiService.get(`/roles/${roleName}/users`);
    return response.data || [];
  }

  // Databricks admin sync
  async syncDatabricksAdminRoles(): Promise<DatabricksAdminSyncResult> {
    const response = await ApiService.post('/admin/databricks-roles/sync');
    return response.data;
  }

  async getDatabricksAdminEmails(): Promise<AdminEmailsResult> {
    const response = await ApiService.get('/admin/databricks-roles/admin-emails');
    return response.data;
  }

  async checkUserAdminStatus(email: string): Promise<UserAdminStatus> {
    const response = await ApiService.get(`/admin/databricks-roles/check-admin/${encodeURIComponent(email)}`);
    return response.data;
  }
}