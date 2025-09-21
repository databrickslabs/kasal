import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Box,
  IconButton,
  Chip,
  Typography,
  CircularProgress,
  Tooltip,
  Menu,
  MenuItem,
  Avatar,
  ListItemIcon,
  ListItemText,
  Divider
} from '@mui/material';
import {
  WorkspacesOutlined as WorkspacesIcon,
  HomeOutlined as HomeIcon,
  GroupsOutlined as GroupsIcon
} from '@mui/icons-material';
import { GroupService, GroupWithRole } from '../../api/GroupService';
import toast from 'react-hot-toast';
import { useRunStatusStore } from '../../store/runStatus';
import { useUserStore } from '../../store/user';

const GroupSelector: React.FC = () => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [groups, setGroups] = useState<GroupWithRole[]>([]);
  const [loading, setLoading] = useState(false);
  const [currentGroup, setCurrentGroup] = useState<GroupWithRole | null>(null);
  const [isSwitching, setIsSwitching] = useState(false);
  const clearRunHistory = useRunStatusStore(state => state.clearRunHistory);

  // Get current user from Zustand store
  const { currentUser, isLoadingUser, fetchCurrentUser } = useUserStore(state => ({
    currentUser: state.currentUser,
    isLoadingUser: state.isLoading,
    fetchCurrentUser: state.fetchCurrentUser
  }));

  const open = Boolean(anchorEl);

  const fetchUserGroups = useCallback(async () => {
    // Don't fetch if we don't have a user yet
    if (!currentUser?.email) {
      return;
    }

    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      let userGroups: GroupWithRole[] = [];

      try {
        userGroups = await groupService.getMyGroups();
      } catch (error) {
        console.warn('Could not fetch user groups, using empty list:', error);
        userGroups = [];
      }

      // Get user email from Zustand store (which fetches from backend with X-Forwarded-Email)
      const currentUserEmail = currentUser.email;

      const emailDomain = currentUserEmail.split('@')[1] || '';
      const emailUser = currentUserEmail.split('@')[0] || '';

      // Create the primary personal group ID format (e.g., user_admin_admin_com)
      const primaryGroupId = `user_${emailUser}_${emailDomain.replace(/\./g, '_')}`;

      // Groups already come with roles from backend
      let allGroups: GroupWithRole[] = userGroups;

      // Always add the personal workspace as the first option
      const personalGroup: GroupWithRole = {
        id: primaryGroupId,
        name: 'My Workspace', // Better UX name
        status: 'active',
        auto_created: true,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        user_count: 1,
        user_role: undefined  // Personal workspace doesn't have group roles
      };

      // Add personal group at the beginning
      allGroups = [personalGroup, ...allGroups.filter(g => g.id !== primaryGroupId)];

      setGroups(allGroups);

      // Set current group from localStorage or use personal group
      const savedGroupId = localStorage.getItem('selectedGroupId');
      if (allGroups.length > 0) {
        const selectedGroup = savedGroupId
          ? allGroups.find(g => g.id === savedGroupId) || personalGroup
          : personalGroup;

        // Only update if it's different from current group
        if (!currentGroup || currentGroup.id !== selectedGroup.id) {
          setCurrentGroup(selectedGroup);
          localStorage.setItem('selectedGroupId', selectedGroup.id);
        }
      }
    } catch (error) {
      console.error('Failed to fetch user groups:', error);
      toast.error('Failed to load groups');
    } finally {
      setLoading(false);
    }
  }, [currentUser]); // Only depend on currentUser, not currentGroup

  // Fetch user once when component mounts
  useEffect(() => {
    if (!currentUser) {
      fetchCurrentUser();
    }
  }, []); // Only run once on mount

  // Fetch groups when user changes
  useEffect(() => {
    if (currentUser?.email) {
      console.log('User email changed, fetching groups for:', currentUser.email);
      fetchUserGroups();
    }
  }, [currentUser?.email, fetchUserGroups]);

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const switchToGroup = async (group: GroupWithRole) => {
    try {
      // Don't switch if we're already on this group
      if (currentGroup?.id === group.id) {
        handleClose();
        return;
      }

      // Set switching state to prevent flickering
      setIsSwitching(true);

      console.log('Switching to group:', group.id, group.name);

      // Clear the run history store before switching groups
      clearRunHistory();

      // Set the group context in localStorage
      localStorage.setItem('selectedGroupId', group.id);

      // Also store the user email if this is a personal workspace
      if (group.id.startsWith('user_')) {
        localStorage.setItem('userEmail', group.name);
      }

      // Log what we're setting
      console.log('Set selectedGroupId:', group.id);

      // Update the current group immediately to show the change
      setCurrentGroup(group);
      handleClose();

      // Show success message with the actual group ID being used
      const displayName = group.id.startsWith('user_')
        ? 'My Workspace'
        : `${group.name} workspace`;
      toast.success(`Switched to ${displayName}`);

      // Small delay then reload to apply new context
      setTimeout(() => {
        window.location.reload();
      }, 300);  // Reduced delay for faster transition
    } catch (error) {
      console.error('Failed to switch group:', error);
      toast.error('Failed to switch group');
    }
  };


  const getRoleColor = (role?: string): "error" | "primary" | "success" | "default" => {
    switch (role) {
      case 'ADMIN':
        return 'error';
      case 'EDITOR':
        return 'success';
      case 'OPERATOR':
      default:
        return 'default';
    }
  };

  // Memoize the avatar to prevent re-renders, but update when email changes
  const avatarElement = useMemo(() => {
    if (!currentGroup) return null;

    if (currentGroup.id.startsWith('user_')) {
      // Personal workspace icon
      return (
        <HomeIcon
          fontSize="small"
          sx={{
            color: 'primary.main'
          }}
        />
      );
    }

    // Shared workspace icon
    return (
      <WorkspacesIcon
        fontSize="small"
        sx={{
          color: 'text.secondary'
        }}
      />
    );
  }, [currentGroup?.id, currentGroup?.name, currentUser?.email]);

  if (loading || isSwitching || isLoadingUser) {
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', p: 0.5 }}>
        <CircularProgress size={24} />
      </Box>
    );
  }

  if (!currentGroup) {
    // Show a placeholder while waiting for groups to load
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', p: 0.5 }}>
        <Avatar
          sx={{
            width: 24,
            height: 24,
            fontSize: '0.75rem',
            bgcolor: 'grey.400',
          }}
        >
          ?
        </Avatar>
      </Box>
    );
  }

  return (
    <>
      <Tooltip
        title={
          currentGroup.id.startsWith('user_')
            ? `My Workspace (${currentUser?.email})`
            : `${currentGroup.name} - Shared Workspace`
        }
        enterDelay={500}
        leaveDelay={200}
        disableInteractive
        placement="bottom"
      >
        <IconButton
          id="group-selector-button"
          aria-controls={open ? 'group-menu' : undefined}
          aria-haspopup="true"
          aria-expanded={open ? 'true' : undefined}
          onClick={handleClick}
          size="small"
          sx={{
            p: 0.5,
            transition: 'background-color 0.2s',
            '&:hover': {
              backgroundColor: 'action.hover',
            }
          }}
        >
          {avatarElement}
        </IconButton>
      </Tooltip>
      <Menu
        id="group-menu"
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        disableScrollLock={true}  // Prevents body scroll lock and ResizeObserver issues
        keepMounted={false}        // Unmount when closed to save resources and prevent issues
        TransitionProps={{         // Proper transition configuration
          timeout: 350,
        }}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
        slotProps={{
          paper: {
            elevation: 2,
            sx: {
              minWidth: 280,
              maxHeight: 400,
              mt: 0.5,
              overflow: 'auto',  // Changed from 'visible' to 'auto' for better scrolling
              filter: 'drop-shadow(0px 2px 8px rgba(0,0,0,0.08))',
            },
          }
        }}
        MenuListProps={{
          'aria-labelledby': 'group-selector-button',
          sx: { py: 0 }
        }}
      >
        <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider' }}>
          <Typography variant="subtitle2" color="text.secondary">
            Switch Workspace
          </Typography>
        </Box>
        {groups.length > 0 && <Divider />}
        {groups.map((group) => {
          const isPersonalWorkspace = group.id.startsWith('user_');
          const isSelected = currentGroup?.id === group.id;

          return (
            <MenuItem
              key={group.id}
              onClick={() => switchToGroup(group)}
              selected={isSelected}
              sx={{
                minHeight: 48,
                px: 2,
                py: 1,
                '&.Mui-selected': {
                  backgroundColor: 'action.selected',
                  '&:hover': {
                    backgroundColor: 'action.selected',
                  }
                }
              }}
            >
              <ListItemIcon sx={{ minWidth: 36 }}>
                {isPersonalWorkspace ? (
                  <HomeIcon
                    fontSize="small"
                    sx={{
                      color: isSelected ? 'primary.main' : 'text.secondary'
                    }}
                  />
                ) : (
                  <GroupsIcon
                    fontSize="small"
                    sx={{
                      color: isSelected ? 'primary.main' : 'text.secondary'
                    }}
                  />
                )}
              </ListItemIcon>
              <ListItemText
                primary={
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Typography
                      variant="body2"
                      sx={{ fontWeight: isSelected ? 600 : 400 }}
                    >
                      {isPersonalWorkspace ? 'My Workspace' : group.name}
                    </Typography>
                    {isSelected && (
                      <Chip
                        label="Active"
                        size="small"
                        color="primary"
                        sx={{ height: 20 }}
                      />
                    )}
                  </Box>
                }
                secondary={
                  <Typography variant="caption" color="text.secondary">
                    {isPersonalWorkspace
                      ? `Personal - ${currentUser?.email}`
                      : `Shared workspace`}
                  </Typography>
                }
                sx={{ my: 0 }}
              />
              {group.user_role && !isPersonalWorkspace && (
                <Chip
                  label={group.user_role}
                  size="small"
                  color={getRoleColor(group.user_role)}
                  sx={{ ml: 1 }}
                />
              )}
            </MenuItem>
          );
        })}
        {groups.length === 0 && (
          <MenuItem disabled>
            <Typography variant="body2" color="text.secondary">
              No groups available
            </Typography>
          </MenuItem>
        )}
      </Menu>
    </>
  );
};

export default GroupSelector;