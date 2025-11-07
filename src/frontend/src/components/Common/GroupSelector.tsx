import React, { useState, useEffect, useMemo } from 'react';
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
import { GroupWithRole } from '../../api/GroupService';
import toast from 'react-hot-toast';
import { useRunStatusStore } from '../../store/runStatus';
import { useUserStore } from '../../store/user';
import { useGroupStore } from '../../store/groups';

const GroupSelector: React.FC = () => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  // Use Zustand store instead of local state
  const groups = useGroupStore(s => s.groups);
  const loading = useGroupStore(s => s.isLoading);
  const currentGroup = useGroupStore(s => s.getCurrentGroup());
  const fetchMyGroups = useGroupStore(s => s.fetchMyGroups);
  const setCurrentGroupId = useGroupStore(s => s.setCurrentGroup);
  const [isSwitching, setIsSwitching] = useState(false);
  const clearRunHistory = useRunStatusStore(state => state.clearRunHistory);

  // Get current user from Zustand store
  const { currentUser, isLoadingUser, fetchCurrentUser } = useUserStore(state => ({
    currentUser: state.currentUser,
    isLoadingUser: state.isLoading,
    fetchCurrentUser: state.fetchCurrentUser
  }));

  const open = Boolean(anchorEl);



  // Fetch user once when component mounts
  useEffect(() => {
    if (!currentUser) {
      fetchCurrentUser();
    }
  }, [currentUser, fetchCurrentUser]); // Include dependencies

  // Fetch groups when user changes
  useEffect(() => {
    if (currentUser?.email) {
      fetchMyGroups();
    }
  }, [currentUser?.email, fetchMyGroups]);

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

      // Update selected group in the global store (persists to localStorage + fires event)
      setCurrentGroupId(group.id);
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
  }, [currentGroup?.id, currentGroup?.name, currentUser?.email, currentGroup]);

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