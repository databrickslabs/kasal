import React, { createContext, useContext, useEffect, useState } from 'react';
import { GroupService } from '../api/GroupService';
import type { GroupUser } from '../api/GroupService';

// Define permission types based on backend privileges
export const Permissions = {
  // Group Management
  GROUP_CREATE: 'group:create',
  GROUP_READ: 'group:read',
  GROUP_UPDATE: 'group:update',
  GROUP_DELETE: 'group:delete',
  GROUP_MANAGE_USERS: 'group:manage_users',

  // Agent Management
  AGENT_CREATE: 'agent:create',
  AGENT_READ: 'agent:read',
  AGENT_UPDATE: 'agent:update',
  AGENT_DELETE: 'agent:delete',

  // Task Management
  TASK_CREATE: 'task:create',
  TASK_READ: 'task:read',
  TASK_UPDATE: 'task:update',
  TASK_DELETE: 'task:delete',
  TASK_EXECUTE: 'task:execute',

  // Crew Management
  CREW_CREATE: 'crew:create',
  CREW_READ: 'crew:read',
  CREW_UPDATE: 'crew:update',
  CREW_DELETE: 'crew:delete',
  CREW_EXECUTE: 'crew:execute',

  // Execution Management
  EXECUTION_CREATE: 'execution:create',
  EXECUTION_READ: 'execution:read',
  EXECUTION_MANAGE: 'execution:manage',

  // Configuration
  SETTINGS_READ: 'settings:read',
  SETTINGS_UPDATE: 'settings:update',
  TOOL_CREATE: 'tool:create',
  TOOL_READ: 'tool:read',
  TOOL_UPDATE: 'tool:update',
  TOOL_DELETE: 'tool:delete',
  TOOL_CONFIGURE: 'tool:configure',
  MODEL_CREATE: 'model:create',
  MODEL_READ: 'model:read',
  MODEL_UPDATE: 'model:update',
  MODEL_DELETE: 'model:delete',
  MODEL_CONFIGURE: 'model:configure',
  API_KEY_CREATE: 'api_key:create',
  API_KEY_READ: 'api_key:read',
  API_KEY_UPDATE: 'api_key:update',
  API_KEY_DELETE: 'api_key:delete',
  API_KEY_MANAGE: 'api_key:manage',

  // User Management
  USER_INVITE: 'user:invite',
  USER_REMOVE: 'user:remove',
  USER_UPDATE_ROLE: 'user:update_role',
} as const;

type PermissionType = typeof Permissions[keyof typeof Permissions];
type UserRole = 'admin' | 'editor' | 'operator';

// Define role-permission mappings (matching backend)
const ROLE_PERMISSIONS: Record<UserRole, PermissionType[]> = {
  admin: [
    // Admin has all permissions
    ...Object.values(Permissions)
  ],
  editor: [
    // Editor permissions - can build and modify workflows
    Permissions.AGENT_CREATE,
    Permissions.AGENT_READ,
    Permissions.AGENT_UPDATE,
    Permissions.AGENT_DELETE,
    Permissions.TASK_CREATE,
    Permissions.TASK_READ,
    Permissions.TASK_UPDATE,
    Permissions.TASK_DELETE,
    Permissions.CREW_CREATE,
    Permissions.CREW_READ,
    Permissions.CREW_UPDATE,
    Permissions.CREW_DELETE,
    Permissions.TASK_EXECUTE,
    Permissions.CREW_EXECUTE,
    Permissions.EXECUTION_CREATE,
    Permissions.EXECUTION_READ,
    Permissions.TOOL_READ,
    Permissions.TOOL_CONFIGURE,
    Permissions.MODEL_READ,
    Permissions.MODEL_CONFIGURE,
    Permissions.SETTINGS_READ,
    Permissions.API_KEY_READ,
  ],
  operator: [
    // Operator permissions - can execute and monitor
    Permissions.AGENT_READ,
    Permissions.TASK_READ,
    Permissions.CREW_READ,
    Permissions.TASK_EXECUTE,
    Permissions.CREW_EXECUTE,
    Permissions.EXECUTION_CREATE,
    Permissions.EXECUTION_READ,
    Permissions.EXECUTION_MANAGE,
    Permissions.TOOL_READ,
    Permissions.MODEL_READ,
    Permissions.SETTINGS_READ,
  ],
};

interface PermissionContextType {
  userRole: UserRole | null;
  permissions: Set<PermissionType>;
  hasPermission: (permission: PermissionType | PermissionType[]) => boolean;
  hasAnyPermission: (permissions: PermissionType[]) => boolean;
  hasAllPermissions: (permissions: PermissionType[]) => boolean;
  canCreate: boolean;
  canEdit: boolean;
  canDelete: boolean;
  canExecute: boolean;
  canManageUsers: boolean;
  canConfigureSystem: boolean;
  isLoading: boolean;
}

const PermissionContext = createContext<PermissionContextType | undefined>(undefined);

export const PermissionProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [userRole, setUserRole] = useState<UserRole | null>(null);
  const [permissions, setPermissions] = useState<Set<PermissionType>>(new Set());
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const loadUserPermissions = async () => {
      try {
        // Get current group from localStorage
        const selectedGroupId = localStorage.getItem('selectedGroupId');
        if (!selectedGroupId) {
          setIsLoading(false);
          return;
        }

        // Get user's role in the current group
        const groupService = GroupService.getInstance();
        const groupUsers = await groupService.getGroupUsers(selectedGroupId);

        // Get current user email
        const userEmail = localStorage.getItem('userEmail') || '';
        const currentUser = groupUsers.find(gu => gu.email === userEmail);

        if (currentUser) {
          const role = currentUser.role.toLowerCase() as UserRole;
          setUserRole(role);
          setPermissions(new Set(ROLE_PERMISSIONS[role]));
        }
      } catch (error) {
        console.error('Failed to load user permissions:', error);
      } finally {
        setIsLoading(false);
      }
    };

    loadUserPermissions();
  }, []);

  const hasPermission = (permission: PermissionType | PermissionType[]): boolean => {
    if (Array.isArray(permission)) {
      return permission.some(p => permissions.has(p));
    }
    return permissions.has(permission);
  };

  const hasAnyPermission = (perms: PermissionType[]): boolean => {
    return perms.some(p => permissions.has(p));
  };

  const hasAllPermissions = (perms: PermissionType[]): boolean => {
    return perms.every(p => permissions.has(p));
  };

  const value: PermissionContextType = {
    userRole,
    permissions,
    hasPermission,
    hasAnyPermission,
    hasAllPermissions,
    // Computed permissions for common checks
    canCreate: hasAnyPermission([
      Permissions.AGENT_CREATE,
      Permissions.TASK_CREATE,
      Permissions.CREW_CREATE,
    ]),
    canEdit: hasAnyPermission([
      Permissions.AGENT_UPDATE,
      Permissions.TASK_UPDATE,
      Permissions.CREW_UPDATE,
    ]),
    canDelete: hasAnyPermission([
      Permissions.AGENT_DELETE,
      Permissions.TASK_DELETE,
      Permissions.CREW_DELETE,
    ]),
    canExecute: hasAnyPermission([
      Permissions.TASK_EXECUTE,
      Permissions.CREW_EXECUTE,
    ]),
    canManageUsers: hasAnyPermission([
      Permissions.USER_INVITE,
      Permissions.USER_REMOVE,
      Permissions.USER_UPDATE_ROLE,
      Permissions.GROUP_MANAGE_USERS,
    ]),
    canConfigureSystem: hasAnyPermission([
      Permissions.SETTINGS_UPDATE,
      Permissions.MODEL_CREATE,
      Permissions.MODEL_UPDATE,
      Permissions.API_KEY_CREATE,
    ]),
    isLoading,
  };

  return (
    <PermissionContext.Provider value={value}>
      {children}
    </PermissionContext.Provider>
  );
};

export const usePermissions = () => {
  const context = useContext(PermissionContext);
  if (context === undefined) {
    throw new Error('usePermissions must be used within a PermissionProvider');
  }
  return context;
};

// Export a higher-order component for protecting routes
export const withPermission = (
  Component: React.ComponentType<any>,
  requiredPermission: PermissionType | PermissionType[]
) => {
  return (props: any) => {
    const { hasPermission, isLoading } = usePermissions();

    if (isLoading) {
      return <div>Loading permissions...</div>;
    }

    if (!hasPermission(requiredPermission)) {
      return (
        <div style={{ padding: '20px', textAlign: 'center' }}>
          <h3>Access Denied</h3>
          <p>You don't have permission to access this feature.</p>
        </div>
      );
    }

    return <Component {...props} />;
  };
};