import { create } from 'zustand';
import { devtools, persist } from 'zustand/middleware';
import { GroupService } from '../api/GroupService';
import { useUserStore } from './user';

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
  MCP_CREATE: 'mcp:create',
  MCP_READ: 'mcp:read',
  MCP_UPDATE: 'mcp:update',
  MCP_DELETE: 'mcp:delete',
  MCP_CONFIGURE: 'mcp:configure',
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

export type PermissionType = typeof Permissions[keyof typeof Permissions];
export type UserRole = 'admin' | 'editor' | 'operator';

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
    Permissions.MCP_READ,
    Permissions.MCP_CONFIGURE,
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

interface PermissionState {
  // State
  userRole: UserRole | null;
  permissions: Set<PermissionType>;
  isLoading: boolean;
  error: string | null;
  lastUpdated: number | null;
  isSystemAdmin: boolean;
  isPersonalWorkspaceManager: boolean;

  // Actions
  loadPermissions: (groupId?: string) => Promise<void>;
  setUserRole: (role: UserRole) => void;
  clearPermissions: () => void;
  refreshPermissions: () => Promise<void>;
  setUserLevelPermissions: (isSystemAdmin: boolean, isPersonalWorkspaceManager: boolean) => void;

  // Permission checks
  hasPermission: (permission: PermissionType | PermissionType[]) => boolean;
  hasAnyPermission: (permissions: PermissionType[]) => boolean;
  hasAllPermissions: (permissions: PermissionType[]) => boolean;

  // Computed permissions
  canCreate: () => boolean;
  canEdit: () => boolean;
  canDelete: () => boolean;
  canExecute: () => boolean;
  canManageUsers: () => boolean;
  canConfigureSystem: () => boolean;
  canConfigureWorkspace: () => boolean;
  canManageSystemUsers: () => boolean;

  // UI helpers
  isAdmin: () => boolean;
  isEditor: () => boolean;
  isOperator: () => boolean;
  isWorkspaceAdmin: () => boolean;
  getVisibleMenuItems: () => string[];
  getDisabledFeatures: () => string[];
}

export const usePermissionStore = create<PermissionState>()(
  devtools(
    persist(
      (set, get) => ({
        // Initial state
        userRole: null,
        permissions: new Set(),
        isLoading: false,
        error: null,
        lastUpdated: null,
        isSystemAdmin: false,
        isPersonalWorkspaceManager: false,

        // Load permissions for a group
        loadPermissions: async (groupId?: string) => {
          set({ isLoading: true, error: null });

          try {
            // Get current user from the user store
            let currentUser = useUserStore.getState().currentUser;

            // If no current user, try to fetch it
            if (!currentUser) {
              console.log('PermissionStore - No current user, fetching...');
              await useUserStore.getState().fetchCurrentUser();
              currentUser = useUserStore.getState().currentUser;

              if (currentUser) {
                console.log('User info fetched for permissions:', { id: currentUser.id, email: currentUser.email });
              } else {
                console.log('PermissionStore - Failed to fetch user, setting default permissions');
                // Set default permissions and exit early
                set({
                  userRole: 'editor',
                  permissions: new Set(ROLE_PERMISSIONS['editor']),
                  isLoading: false,
                  lastUpdated: Date.now(),
                  error: null,
                  isSystemAdmin: false,
                  isPersonalWorkspaceManager: false,
                });
                return;
              }
            }

            // Use the current user we already have (either existing or newly fetched)
            const currentUserFromStore = currentUser;
            const userId = currentUserFromStore?.id || localStorage.getItem('userId');
            const userEmail = currentUserFromStore?.email || localStorage.getItem('userEmail') || '';

            // Load user-level permissions from the user store
            const userIsSystemAdmin = currentUserFromStore?.is_system_admin || false;
            const userIsPersonalWorkspaceManager = currentUserFromStore?.is_personal_workspace_manager || false;

            console.log('PermissionStore - User permissions:', {
              email: currentUserFromStore?.email,
              userIsSystemAdmin,
              userIsPersonalWorkspaceManager
            });

            // Update the user-level permissions in the store
            set({
              isSystemAdmin: userIsSystemAdmin,
              isPersonalWorkspaceManager: userIsPersonalWorkspaceManager,
            });

            // Get the selected group from localStorage (set by workspace selector)
            const targetGroupId = groupId || localStorage.getItem('selectedGroupId');

            // If it's a personal workspace, determine role based on permissions
            if (targetGroupId && targetGroupId.startsWith('user_')) {
              // Check if user has personal workspace manager permission
              const effectiveRole = userIsSystemAdmin || userIsPersonalWorkspaceManager ? 'admin' : 'editor';
              const rolePermissions = ROLE_PERMISSIONS[effectiveRole];
              console.log(`Permission loaded: ${effectiveRole} permissions for personal workspace ${targetGroupId}`);

              set({
                userRole: effectiveRole,
                permissions: new Set(rolePermissions),
                isLoading: false,
                lastUpdated: Date.now(),
                error: null
              });
              return;
            }

            // For shared workspaces, fetch the actual role from the backend
            if (targetGroupId && userId) {
              try {
                const groups = await GroupService.getInstance().getMyGroups();
                const targetGroup = groups.find(g => g.id === targetGroupId);

                if (targetGroup?.user_role) {
                  const role = targetGroup.user_role.toLowerCase() as UserRole;
                  const rolePermissions = ROLE_PERMISSIONS[role];

                  console.log(`Permission loaded: ${role} permissions for group ${targetGroupId}`);

                  set({
                    userRole: role,
                    permissions: new Set(rolePermissions),
                    isLoading: false,
                    lastUpdated: Date.now(),
                    error: null
                  });
                  return;
                }
              } catch (error) {
                console.warn('Failed to fetch group roles, defaulting to editor:', error);
              }
            }

            // Default to editor permissions if no group context
            const rolePermissions = ROLE_PERMISSIONS['editor'];
            console.log(`Permission loaded: Default editor permissions for user ${userEmail}`);

            set({
              userRole: 'editor',
              permissions: new Set(rolePermissions),
              isLoading: false,
              lastUpdated: Date.now(),
              error: null
            });
          } catch (error) {
            console.error('Failed to load permissions:', error);
            set({
              isLoading: false,
              error: error instanceof Error ? error.message : 'Failed to load permissions',
              permissions: new Set()
            });
          }
        },

        // Set user role manually (for testing or override)
        setUserRole: (role: UserRole) => {
          const rolePermissions = ROLE_PERMISSIONS[role];
          set({
            userRole: role,
            permissions: new Set(rolePermissions),
            lastUpdated: Date.now(),
            error: null
          });
        },

        // Set user-level permissions
        setUserLevelPermissions: (isSystemAdmin: boolean, isPersonalWorkspaceManager: boolean) => {
          set({
            isSystemAdmin,
            isPersonalWorkspaceManager,
          });
        },

        // Clear all permissions
        clearPermissions: () => {
          set({
            userRole: null,
            permissions: new Set(),
            lastUpdated: null,
            error: null,
            isSystemAdmin: false,
            isPersonalWorkspaceManager: false
          });
        },

        // Refresh permissions from backend
        refreshPermissions: async () => {
          const { loadPermissions } = get();
          await loadPermissions();
        },

        // Permission check methods
        hasPermission: (permission: PermissionType | PermissionType[]) => {
          const { permissions } = get();
          if (Array.isArray(permission)) {
            return permission.some(p => permissions.has(p));
          }
          return permissions.has(permission);
        },

        hasAnyPermission: (perms: PermissionType[]) => {
          const { permissions } = get();
          return perms.some(p => permissions.has(p));
        },

        hasAllPermissions: (perms: PermissionType[]) => {
          const { permissions } = get();
          return perms.every(p => permissions.has(p));
        },

        // Computed permission helpers
        canCreate: () => {
          const { hasAnyPermission } = get();
          return hasAnyPermission([
            Permissions.AGENT_CREATE,
            Permissions.TASK_CREATE,
            Permissions.CREW_CREATE,
            Permissions.TOOL_CREATE,
          ]);
        },

        canEdit: () => {
          const { hasAnyPermission } = get();
          return hasAnyPermission([
            Permissions.AGENT_UPDATE,
            Permissions.TASK_UPDATE,
            Permissions.CREW_UPDATE,
            Permissions.TOOL_UPDATE,
          ]);
        },

        canDelete: () => {
          const { hasAnyPermission } = get();
          return hasAnyPermission([
            Permissions.AGENT_DELETE,
            Permissions.TASK_DELETE,
            Permissions.CREW_DELETE,
            Permissions.TOOL_DELETE,
          ]);
        },

        canExecute: () => {
          const { hasAnyPermission } = get();
          return hasAnyPermission([
            Permissions.TASK_EXECUTE,
            Permissions.CREW_EXECUTE,
          ]);
        },

        canManageUsers: () => {
          const { hasAnyPermission } = get();
          return hasAnyPermission([
            Permissions.USER_INVITE,
            Permissions.USER_REMOVE,
            Permissions.USER_UPDATE_ROLE,
            Permissions.GROUP_MANAGE_USERS,
          ]);
        },

        canConfigureSystem: () => {
          const { hasAnyPermission } = get();
          return hasAnyPermission([
            Permissions.SETTINGS_UPDATE,
            Permissions.MODEL_CREATE,
            Permissions.MODEL_UPDATE,
            Permissions.API_KEY_CREATE,
            Permissions.API_KEY_MANAGE,
          ]);
        },

        canConfigureWorkspace: () => {
          const { userRole, isSystemAdmin } = get();
          return isSystemAdmin || userRole === 'admin';
        },

        canManageSystemUsers: () => {
          const { isSystemAdmin } = get();
          return isSystemAdmin;
        },

        // Role check helpers
        isAdmin: () => get().userRole === 'admin',
        isEditor: () => get().userRole === 'editor',
        isOperator: () => get().userRole === 'operator',
        isWorkspaceAdmin: () => {
          const { userRole, isSystemAdmin, isPersonalWorkspaceManager } = get();
          const groupId = localStorage.getItem('selectedGroupId');
          const isPersonalWorkspace = groupId?.startsWith('user_');

          if (isSystemAdmin) return true;
          if (isPersonalWorkspace && isPersonalWorkspaceManager) return true;
          return userRole === 'admin';
        },

        // UI helpers for navigation
        getVisibleMenuItems: () => {
          const { hasPermission, userRole } = get();
          const menuItems: string[] = ['dashboard', 'executions']; // Always visible

          if (hasPermission(Permissions.CREW_READ)) menuItems.push('crews');
          if (hasPermission(Permissions.TASK_READ)) menuItems.push('tasks');
          if (hasPermission(Permissions.AGENT_READ)) menuItems.push('agents');
          if (hasPermission(Permissions.TOOL_READ)) menuItems.push('tools');

          if (hasPermission(Permissions.SETTINGS_READ)) menuItems.push('settings');
          if (userRole === 'admin') {
            menuItems.push('users', 'groups', 'security');
          }

          return menuItems;
        },

        // Get list of disabled features for current role
        getDisabledFeatures: () => {
          const { userRole } = get();
          const disabled: string[] = [];

          if (userRole === 'operator') {
            disabled.push(
              'create-crew', 'edit-crew', 'delete-crew',
              'create-agent', 'edit-agent', 'delete-agent',
              'create-task', 'edit-task', 'delete-task',
              'system-settings', 'user-management'
            );
          } else if (userRole === 'editor') {
            disabled.push('user-management', 'system-settings', 'api-key-management');
          }

          return disabled;
        },
      }),
      {
        name: 'permission-store-v2', // Updated to force cache refresh after 3-tier migration
        partialize: (state) => ({
          userRole: state.userRole,
          lastUpdated: state.lastUpdated,
        }),
      }
    ),
    {
      name: 'PermissionStore',
    }
  )
);