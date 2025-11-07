import { useEffect } from 'react';
import { usePermissionStore } from '../store/permissions';

/**
 * Hook to automatically load and refresh permissions
 * This hook should be used at the app level to ensure permissions are always loaded
 */
export const usePermissionLoader = () => {
  const { loadPermissions, userRole, isLoading } = usePermissionStore();

  useEffect(() => {
    // Load permissions on mount
    loadPermissions();

    // Set up event listener for group changes
    const handleGroupChange = (event: Event) => {
      const customEvent = event as CustomEvent<{ groupId: string }>;
      loadPermissions(customEvent.detail.groupId);
    };

    // Listen for storage events (when selectedGroupId changes)
    const handleStorageChange = (e: StorageEvent) => {
      if (e.key === 'selectedGroupId' && e.newValue) {
        loadPermissions(e.newValue);
      }
    };

    window.addEventListener('group-changed', handleGroupChange);
    window.addEventListener('storage', handleStorageChange);

    return () => {
      window.removeEventListener('group-changed', handleGroupChange);
      window.removeEventListener('storage', handleStorageChange);
    };
  }, [loadPermissions]);

  return { userRole, isLoading };
};

/**
 * Hook to use specific permission checks in components
 */
export const usePermissions = () => {
  const store = usePermissionStore();

  return {
    // State
    userRole: store.userRole,
    permissions: store.permissions,
    isLoading: store.isLoading,
    error: store.error,

    // Permission checks
    hasPermission: store.hasPermission,
    hasAnyPermission: store.hasAnyPermission,
    hasAllPermissions: store.hasAllPermissions,

    // Computed permissions
    canCreate: store.canCreate(),
    canEdit: store.canEdit(),
    canDelete: store.canDelete(),
    canExecute: store.canExecute(),
    canManageUsers: store.canManageUsers(),
    canConfigureSystem: store.canConfigureSystem(),

    // Role checks
    isAdmin: store.isAdmin(),
    isEditor: store.isEditor(),
    isOperator: store.isOperator(),

    // UI helpers
    visibleMenuItems: store.getVisibleMenuItems(),
    disabledFeatures: store.getDisabledFeatures(),

    // Actions
    refreshPermissions: store.refreshPermissions,
  };
};

/**
 * Hook for menu/navigation visibility
 */
export const useNavigationPermissions = () => {
  const { getVisibleMenuItems, userRole } = usePermissionStore();

  return {
    visibleMenuItems: getVisibleMenuItems(),
    userRole,
    showAdminMenu: userRole === 'admin',
    showEditorFeatures: userRole === 'admin' || userRole === 'editor',
    showOperatorFeatures: true, // All roles can see operator features
  };
};

/**
 * Hook for form/edit permissions
 */
export const useEditPermissions = () => {
  const { canEdit, canCreate, canDelete, userRole } = usePermissionStore();

  return {
    canEdit: canEdit(),
    canCreate: canCreate(),
    canDelete: canDelete(),
    isReadOnly: userRole === 'operator',
    userRole,
  };
};