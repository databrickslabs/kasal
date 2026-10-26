import { useShallow } from 'zustand/react/shallow';
import { useEffect } from 'react';
import { usePermissionStore } from '../store/permissions';

/**
 * Hook to automatically load and refresh permissions
 * This hook should be used at the app level to ensure permissions are always loaded
 */
export const usePermissionLoader = () => {
  const { loadPermissions, userRole, isLoading } = usePermissionStore(useShallow(state => ({
    loadPermissions: state.loadPermissions,
    userRole: state.userRole,
    isLoading: state.isLoading,
  })));

  useEffect(() => {
    // Load permissions on mount
    loadPermissions();

    // Set up event listener for group changes
    const handleGroupChange = (event: Event) => {
      const customEvent = event as CustomEvent<{ groupId: string }>;
      loadPermissions(customEvent.detail.groupId);
    };

    // Only this tab's own switch counts. A switch in another tab changes the
    // shared localStorage key (a 'storage' event here) but not this tab's
    // teamspace, so this tab keeps its permissions.
    window.addEventListener('group-changed', handleGroupChange);

    return () => {
      window.removeEventListener('group-changed', handleGroupChange);
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
