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
import { GroupWithRole } from '../../api/groups/GroupService';
import toast from 'react-hot-toast';
import { useRunStatusStore } from '../../store/runStatus';
import { useUserStore } from '../../store/user';
import { useGroupStore } from '../../store/groups';
import { useUILayoutStore } from '../../store/uiLayout';

// Kasal's brand accent (chat.css --accent), used ONLY while chat mode is
// active — the builder canvases are MUI-themed and keep their original
// primary colors. CSS vars can't reach here (outside #kasal-chat-root).
const KASAL_ACCENT = '#FF3621';
const KASAL_ACCENT_SOFT = 'rgba(255, 54, 33, 0.08)';
const KASAL_ACCENT_SOFT_HOVER = 'rgba(255, 54, 33, 0.12)';

const GroupSelector: React.FC = () => {
  const appMode = useUILayoutStore((st) => st.appMode);
  const isChatMode = appMode === 'chat';
  const accent = isChatMode ? KASAL_ACCENT : 'primary.main';
  const accentSoft = isChatMode ? KASAL_ACCENT_SOFT : 'action.selected';
  const accentSoftHover = isChatMode ? KASAL_ACCENT_SOFT_HOVER : 'action.selected';
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
        ? 'Personal Space'
        : `${group.name} teamspace`;
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
      // Personal workspace icon — kept muted/gray to match the mode-switcher
      // grid icon sitting just to its left.
      return (
        <HomeIcon
          fontSize="small"
          sx={{
            color: 'text.secondary'
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
  }, [currentGroup]);

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
      <IconButton
          id="group-selector-button"
          aria-controls={open ? 'group-menu' : undefined}
          aria-haspopup="true"
          aria-expanded={open ? 'true' : undefined}
          onClick={handleClick}
          size="small"
          aria-label={
            currentGroup.id.startsWith('user_')
              ? `Personal Space (${currentUser?.email})`
              : `${currentGroup.name} - Shared Teamspace`
          }
          sx={{
            p: 0.5,
            borderRadius: 2,
            transition: 'background-color 0.2s',
            '&:hover': {
              backgroundColor: 'action.hover',
            }
          }}
        >
          {avatarElement}
        </IconButton>
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
              mt: 1,
              borderRadius: 3,
              border: '1px solid',
              borderColor: 'divider',
              boxShadow: '0 12px 32px rgba(16,24,40,0.10), 0 2px 8px rgba(16,24,40,0.06)',
              overflow: 'auto',  // Changed from 'visible' to 'auto' for better scrolling
            },
          }
        }}
        MenuListProps={{
          'aria-labelledby': 'group-selector-button',
          sx: { py: 0.75 }
        }}
      >
        <Box sx={{ px: 2, pt: 1, pb: 0.5 }}>
          <Typography
            sx={{ fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase' }}
            color="text.secondary"
          >
            Switch Teamspace
          </Typography>
        </Box>
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
                mx: 0.75,
                px: 1.5,
                py: 1,
                borderRadius: 2,
                '&.Mui-selected': {
                  backgroundColor: accentSoft,
                  '&:hover': {
                    backgroundColor: accentSoftHover,
                  }
                }
              }}
            >
              <ListItemIcon sx={{ minWidth: 36 }}>
                {isPersonalWorkspace ? (
                  <HomeIcon
                    fontSize="small"
                    sx={{
                      color: isSelected ? accent : 'text.secondary'
                    }}
                  />
                ) : (
                  <GroupsIcon
                    fontSize="small"
                    sx={{
                      color: isSelected ? accent : 'text.secondary'
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
                      {isPersonalWorkspace ? 'Personal Space' : group.name}
                    </Typography>
                    {isSelected && (
                      <Chip
                        label="Active"
                        size="small"
                        color={isChatMode ? undefined : 'primary'}
                        sx={
                          isChatMode
                            ? {
                                height: 20,
                                fontWeight: 600,
                                color: KASAL_ACCENT,
                                backgroundColor: KASAL_ACCENT_SOFT,
                              }
                            : { height: 20 }
                        }
                      />
                    )}
                  </Box>
                }
                secondary={
                  <Typography variant="caption" color="text.secondary">
                    {isPersonalWorkspace
                      ? `Personal - ${currentUser?.email}`
                      : `Shared teamspace`}
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