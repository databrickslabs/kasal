import { apiClient } from '../../shared/api/client';

// User types
export interface User {
  id: string;
  email: string;
  display_name?: string | null;
  role: string;
  status: string;
  is_system_admin: boolean;
  is_personal_workspace_manager: boolean;
  created_at: string;
  updated_at: string;
  last_login: string | null;
}

export interface DirectoryPerson {
  email: string;
  display_name: string | null;
}

export interface UserPermissionUpdate {
  is_system_admin?: boolean;
  is_personal_workspace_manager?: boolean;
}

export class UserService {
  private static instance: UserService;

  static getInstance(): UserService {
    if (!UserService.instance) {
      UserService.instance = new UserService();
    }
    return UserService.instance;
  }

  /**
   * Get all users (system admin only)
   */
  async getUsers(search = '', skip = 0, limit = 100): Promise<User[]> {
    const response = await apiClient.get<User[]>('/users', { params: { search, skip, limit } });
    return response.data;
  }

  async provisionUser(email: string): Promise<User> {
    const response = await apiClient.post<User>('/users', { email });
    return response.data;
  }

  async searchDirectory(search: string): Promise<DirectoryPerson[]> {
    const response = await apiClient.get<DirectoryPerson[]>('/users/directory', { params: { search } });
    return response.data;
  }

  /**
   * Get current user profile
   */
  async getCurrentUser(): Promise<User> {
    const response = await apiClient.get<User>('/users/me');
    return response.data;
  }

  /**
   * Update user permissions (system admin only)
   */
  async updateUserPermissions(userId: string, permissions: UserPermissionUpdate): Promise<User> {
    const response = await apiClient.put<User>(`/users/${userId}/permissions`, permissions);
    return response.data;
  }

  /**
   * Get user by ID (admin only)
   */
  async getUser(userId: string): Promise<User> {
    const response = await apiClient.get<User>(`/users/${userId}`);
    return response.data;
  }

  /**
   * Delete user (admin only)
   */
  async deleteUser(userId: string): Promise<void> {
    await apiClient.delete(`/users/${userId}`);
  }
}
