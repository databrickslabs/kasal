import React from 'react';
import { usePermissionStore, Permissions } from '../../store/permissions';
import { Tooltip, Box, Typography } from '@mui/material';
import LockIcon from '@mui/icons-material/Lock';

type PermissionType = typeof Permissions[keyof typeof Permissions];

interface PermissionGuardProps {
  children: React.ReactNode;
  permission?: PermissionType | PermissionType[];
  fallback?: React.ReactNode;
  showLocked?: boolean;
  disableOnly?: boolean;
  tooltip?: string;
  requireAll?: boolean; // Require all permissions vs any permission
}

/**
 * PermissionGuard component to conditionally render or disable components based on permissions
 * Uses Zustand store for efficient permission state management
 *
 * @param permission - Required permission(s) to render/enable the children
 * @param fallback - Component to render when permission is denied (default: nothing)
 * @param showLocked - Show a locked state instead of hiding (default: false)
 * @param disableOnly - Disable the component instead of hiding it (default: false)
 * @param tooltip - Custom tooltip message for disabled state
 * @param requireAll - Require all permissions when multiple are provided (default: false)
 */
export const PermissionGuard: React.FC<PermissionGuardProps> = ({
  children,
  permission,
  fallback = null,
  showLocked = false,
  disableOnly = false,
  tooltip,
  requireAll = false,
}) => {
  const { hasPermission, hasAllPermissions, isLoading } = usePermissionStore();

  if (isLoading) {
    return null;
  }

  // Check permissions
  let hasRequiredPermission = true;
  if (permission) {
    if (Array.isArray(permission) && requireAll) {
      hasRequiredPermission = hasAllPermissions(permission);
    } else {
      hasRequiredPermission = hasPermission(permission);
    }
  }

  // If user has permission, render children normally
  if (hasRequiredPermission) {
    return <>{children}</>;
  }

  // If disableOnly is true, wrap children in a disabled state
  if (disableOnly && React.isValidElement(children)) {
    const disabledElement = React.cloneElement(children as React.ReactElement<any>, {
      disabled: true,
      style: { ...children.props.style, opacity: 0.5, cursor: 'not-allowed' }
    });

    if (tooltip) {
      return (
        <Tooltip title={tooltip || "You don't have permission to use this feature"}>
          <span>{disabledElement}</span>
        </Tooltip>
      );
    }

    return disabledElement;
  }

  // If showLocked is true, show a locked state
  if (showLocked) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: 2,
          opacity: 0.5,
        }}
      >
        <LockIcon sx={{ fontSize: 48, color: 'text.disabled', mb: 1 }} />
        <Typography variant="caption" color="text.disabled">
          {tooltip || 'Requires higher permissions'}
        </Typography>
      </Box>
    );
  }

  // Return fallback or nothing
  return <>{fallback}</>;
};

/**
 * PermissionBoundary component for protecting entire sections
 * Uses Zustand store for permission checks
 */
export const PermissionBoundary: React.FC<{
  children: React.ReactNode;
  permission: PermissionType | PermissionType[];
  message?: string;
  requireAll?: boolean;
}> = ({ children, permission, message, requireAll = false }) => {
  const { hasPermission, hasAllPermissions, userRole } = usePermissionStore();

  const hasAccess = Array.isArray(permission) && requireAll
    ? hasAllPermissions(permission)
    : hasPermission(permission);

  if (!hasAccess) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          minHeight: '400px',
          gap: 2,
        }}
      >
        <LockIcon sx={{ fontSize: 64, color: 'text.disabled' }} />
        <Typography variant="h6" color="text.secondary">
          Access Restricted
        </Typography>
        <Typography variant="body2" color="text.disabled" textAlign="center">
          {message || `This feature requires additional permissions. Your current role: ${userRole}`}
        </Typography>
      </Box>
    );
  }

  return <>{children}</>;
};

/**
 * Hook for conditionally showing/hiding UI elements
 * Directly uses the Zustand store
 */
export const usePermissionVisibility = (permission: PermissionType | PermissionType[]) => {
  const { hasPermission } = usePermissionStore();
  return hasPermission(permission);
};

/**
 * Component for role-based rendering
 */
export const RoleGuard: React.FC<{
  children: React.ReactNode;
  allowedRoles: Array<'admin' | 'editor' | 'operator'>;
  fallback?: React.ReactNode;
}> = ({ children, allowedRoles, fallback = null }) => {
  const userRole = usePermissionStore(state => state.userRole);

  if (!userRole || !allowedRoles.includes(userRole)) {
    return <>{fallback}</>;
  }

  return <>{children}</>;
};

// Re-export Permissions for convenience
export { Permissions } from '../../store/permissions';