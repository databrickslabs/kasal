import { ApiService } from './ApiService';

// Types for tenant management
export interface Tenant {
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

export interface TenantUser {
  id: string;
  tenant_id: string;
  user_id: string;
  email: string;
  role: 'admin' | 'manager' | 'user' | 'viewer';
  status: 'active' | 'inactive' | 'suspended';
  joined_at: string;
  auto_created: boolean;
  created_at: string;
  updated_at: string;
}

export interface CreateTenantRequest {
  name: string;
  email_domain: string;
  description?: string;
}

export interface UpdateTenantRequest {
  name?: string;
  description?: string;
  status?: 'active' | 'suspended' | 'archived';
}

export interface AssignUserRequest {
  user_email: string;
  role: 'admin' | 'manager' | 'user' | 'viewer';
}

export interface UpdateTenantUserRequest {
  role?: 'admin' | 'manager' | 'user' | 'viewer';
  status?: 'active' | 'inactive' | 'suspended';
}

export class TenantService {
  private static instance: TenantService;

  private constructor() {
    // No need to store ApiService as instance variable since it's a static object
  }

  public static getInstance(): TenantService {
    if (!TenantService.instance) {
      TenantService.instance = new TenantService();
    }
    return TenantService.instance;
  }

  /**
   * Get all tenants
   */
  async getTenants(skip = 0, limit = 100): Promise<Tenant[]> {
    try {
      const response = await ApiService.get(`/tenants/?skip=${skip}&limit=${limit}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching tenants:', error);
      throw new Error('Failed to fetch tenants');
    }
  }

  /**
   * Get a specific tenant by ID
   */
  async getTenant(tenantId: string): Promise<Tenant> {
    try {
      const response = await ApiService.get(`/tenants/${tenantId}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching tenant ${tenantId}:`, error);
      throw new Error('Failed to fetch tenant');
    }
  }

  /**
   * Create a new tenant
   */
  async createTenant(tenantData: CreateTenantRequest): Promise<Tenant> {
    try {
      const response = await ApiService.post('/tenants/', tenantData);
      return response.data;
    } catch (error) {
      console.error('Error creating tenant:', error);
      throw new Error('Failed to create tenant');
    }
  }

  /**
   * Update a tenant
   */
  async updateTenant(tenantId: string, tenantData: UpdateTenantRequest): Promise<Tenant> {
    try {
      const response = await ApiService.put(`/tenants/${tenantId}`, tenantData);
      return response.data;
    } catch (error) {
      console.error(`Error updating tenant ${tenantId}:`, error);
      throw new Error('Failed to update tenant');
    }
  }

  /**
   * Delete a tenant
   */
  async deleteTenant(tenantId: string): Promise<void> {
    try {
      await ApiService.delete(`/tenants/${tenantId}`);
    } catch (error) {
      console.error(`Error deleting tenant ${tenantId}:`, error);
      throw new Error('Failed to delete tenant');
    }
  }

  /**
   * Get users in a tenant
   */
  async getTenantUsers(tenantId: string, skip = 0, limit = 100): Promise<TenantUser[]> {
    try {
      const response = await ApiService.get(`/tenants/${tenantId}/users?skip=${skip}&limit=${limit}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching users for tenant ${tenantId}:`, error);
      throw new Error('Failed to fetch tenant users');
    }
  }

  /**
   * Assign a user to a tenant
   */
  async assignUserToTenant(tenantId: string, userData: AssignUserRequest): Promise<TenantUser> {
    try {
      const response = await ApiService.post(`/tenants/${tenantId}/users`, userData);
      return response.data;
    } catch (error) {
      console.error(`Error assigning user to tenant ${tenantId}:`, error);
      throw new Error('Failed to assign user to tenant');
    }
  }

  /**
   * Update a user's role/status in a tenant
   */
  async updateTenantUser(
    tenantId: string, 
    userId: string, 
    userData: UpdateTenantUserRequest
  ): Promise<TenantUser> {
    try {
      const response = await ApiService.put(`/tenants/${tenantId}/users/${userId}`, userData);
      return response.data;
    } catch (error) {
      console.error(`Error updating user ${userId} in tenant ${tenantId}:`, error);
      throw new Error('Failed to update tenant user');
    }
  }

  /**
   * Remove a user from a tenant
   */
  async removeUserFromTenant(tenantId: string, userId: string): Promise<void> {
    try {
      await ApiService.delete(`/tenants/${tenantId}/users/${userId}`);
    } catch (error) {
      console.error(`Error removing user ${userId} from tenant ${tenantId}:`, error);
      throw new Error('Failed to remove user from tenant');
    }
  }

  /**
   * Get tenant statistics
   */
  async getTenantStats(): Promise<{
    total_tenants: number;
    active_tenants: number;
    total_users: number;
    tenants_by_status: Record<string, number>;
  }> {
    try {
      const response = await ApiService.get('/tenants/stats');
      return response.data;
    } catch (error) {
      console.error('Error fetching tenant statistics:', error);
      throw new Error('Failed to fetch tenant statistics');
    }
  }
}