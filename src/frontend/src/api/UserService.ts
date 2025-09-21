import { apiClient } from '../config/api/ApiConfig';

// User types
export interface User {
  id: string;
  email: string;
  role: string;
  status: string;
  is_system_admin: boolean;
  is_personal_workspace_manager: boolean;
  created_at: string;
  updated_at: string;
  last_login: string | null;
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
  async getUsers(): Promise<User[]> {
    const response = await apiClient.get<User[]>('/users');
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