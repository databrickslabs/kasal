import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Button,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Alert,
  Snackbar,
  Card,
  CardContent,
  CardHeader,
  Grid,
  Tooltip,
  Paper,
  Divider,
  Avatar,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  ListItemSecondaryAction,
  Fab,
  Zoom,
  LinearProgress,
  Menu,
  MenuList,
  DialogContentText,
} from '@mui/material';
import { GroupService, Group, GroupUser, CreateGroupRequest, AssignUserRequest } from '../../api/GroupService';
import { UserService, User } from '../../api/UserService';
import { useGroupStore } from '../../store/groups';
import {
  Add as AddIcon,
  Group as GroupIcon,
  Person as PersonIcon,
  Groups as GroupsIcon,
  PersonAdd as PersonAddIcon,
  Search as SearchIcon,
  MoreVert as MoreVertIcon,
  Security as SecurityIcon,
  AdminPanelSettings as AdminIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  Warning as WarningIcon,
  Lock as LockIcon,
  Workspaces as WorkspacesIcon,
} from '@mui/icons-material';
import { usePermissionStore } from '../../store/permissions';


const GroupManagement: React.FC = () => {
  // Check permissions
  const { userRole, isLoading: permissionsLoading } = usePermissionStore(state => ({
    userRole: state.userRole,
    isLoading: state.isLoading
  }));

  const [groups, setGroups] = useState<Group[]>([]);
  const [selectedGroup, setSelectedGroup] = useState<Group | null>(null);
  const [groupUsers, setGroupUsers] = useState<GroupUser[]>([]);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [assignUserDialogOpen, setAssignUserDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [groupToDelete, setGroupToDelete] = useState<Group | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [_viewMode, _setViewMode] = useState<'groups' | 'users'>('groups');
  const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedMenuGroup, setSelectedMenuGroup] = useState<Group | null>(null);
  const [userMenuAnchorEl, setUserMenuAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedMenuUser, setSelectedMenuUser] = useState<GroupUser | null>(null);
  const [removeUserDialogOpen, setRemoveUserDialogOpen] = useState(false);
  const [userToRemove, setUserToRemove] = useState<GroupUser | null>(null);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error' | 'warning',
  });
  const [editingUserId, setEditingUserId] = useState<string | null>(null);
  const [editingUserRole, setEditingUserRole] = useState<'admin' | 'editor' | 'operator'>('operator');
  const [availableUsers, setAvailableUsers] = useState<User[]>([]);
  const [loadingUsers, setLoadingUsers] = useState(false);

  // Group store for refreshing GroupSelector
  const refreshGroupStore = useGroupStore(s => s.refresh);

  // Form states
  const [newGroup, setNewGroup] = useState<CreateGroupRequest>({
    name: '',
    description: '',
  });
  const [newUserAssignment, setNewUserAssignment] = useState<AssignUserRequest>({
    user_email: '',
    role: 'operator',
  });
  const [selectedUserIds, setSelectedUserIds] = useState<string[]>([]);

  // Computed values
  const totalUsers = groups.reduce((sum, group) => sum + (group.user_count || 0), 0);
  const filteredGroups = groups.filter(group =>
    group.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const loadGroups = useCallback(async () => {
    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      const groupsData = await groupService.getGroups();
      setGroups(groupsData);
    } catch (error) {
      console.error('Error loading workspaces:', error);
      showNotification('Failed to load workspaces', 'error');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadGroups();
  }, [loadGroups]);

  const loadAvailableUsers = useCallback(async () => {
    setLoadingUsers(true);
    try {
      const userService = UserService.getInstance();
      const usersData = await userService.getUsers();
      setAvailableUsers(usersData);
    } catch (error) {
      console.error('Error loading users:', error);
      showNotification('Failed to load users', 'error');
    } finally {
      setLoadingUsers(false);
    }
  }, []);

  // Load users when the assign user dialog opens
  useEffect(() => {
    if (assignUserDialogOpen) {
      loadAvailableUsers();
    }
  }, [assignUserDialogOpen, loadAvailableUsers]);


  const loadGroupUsers = async (groupId: string) => {
    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      const usersData = await groupService.getGroupUsers(groupId);
      console.log('Loaded users for group', groupId, ':', usersData);
      setGroupUsers(usersData);
    } catch (error) {
      console.error('Error loading workspace members:', error);
      showNotification('Failed to load team members', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateGroup = async () => {
    if (!newGroup.name) {
      showNotification('Please fill in all required fields', 'warning');
      return;
    }

    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      await groupService.createGroup(newGroup);
      
      setCreateDialogOpen(false);
      setNewGroup({ name: '', description: '' });
      showNotification('Workspace created successfully', 'success');
      loadGroups();
    } catch (error) {
      console.error('Error creating workspace:', error);
      showNotification('Failed to create workspace', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleAssignUser = async () => {
    if (!selectedUserIds.length || !selectedGroup) {
      showNotification('Please select at least one user and role', 'warning');
      return;
    }

    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      let successCount = 0;
      let errorCount = 0;

      // Process each selected user
      for (const userId of selectedUserIds) {
        try {
          // Find the user's email
          const selectedUser = availableUsers.find(user => user.id === userId);
          if (!selectedUser) {
            console.error(`User with ID ${userId} not found`);
            errorCount++;
            continue;
          }

          const userAssignment = {
            user_email: selectedUser.email,
            role: newUserAssignment.role
          };

          await groupService.assignUserToGroup(selectedGroup.id, userAssignment);
          successCount++;
        } catch (error) {
          console.error(`Error assigning user ${userId}:`, error);
          errorCount++;
        }
      }

      setAssignUserDialogOpen(false);
      setNewUserAssignment({ user_email: '', role: 'operator' });
      setSelectedUserIds([]);

      // Show appropriate notification based on results
      if (errorCount === 0) {
        showNotification(`${successCount} member${successCount > 1 ? 's' : ''} added successfully`, 'success');
      } else if (successCount === 0) {
        showNotification('Failed to add any members', 'error');
      } else {
        showNotification(`${successCount} member${successCount > 1 ? 's' : ''} added, ${errorCount} failed`, 'warning');
      }

      loadGroupUsers(selectedGroup.id);

      // Refresh GroupSelector to show updated workspace list
      await refreshGroupStore();
    } catch (error) {
      console.error('Error in bulk user assignment:', error);
      showNotification('Failed to add members', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteGroup = async () => {
    if (!groupToDelete) return;

    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      await groupService.deleteGroup(groupToDelete.id);
      
      setDeleteDialogOpen(false);
      setGroupToDelete(null);
      // Clear selected group if it was the one being deleted
      if (selectedGroup?.id === groupToDelete.id) {
        setSelectedGroup(null);
        setGroupUsers([]);
      }
      showNotification('Workspace deleted successfully', 'success');
      loadGroups();
    } catch (error) {
      console.error('Error deleting workspace:', error);
      showNotification('Failed to delete workspace', 'error');
    } finally {
      setLoading(false);
    }
  };

  const openDeleteDialog = (group: Group) => {
    setGroupToDelete(group);
    setDeleteDialogOpen(true);
    handleCloseMenu();
  };

  const handleOpenMenu = (event: React.MouseEvent<HTMLElement>, group: Group) => {
    event.stopPropagation();
    setMenuAnchorEl(event.currentTarget);
    setSelectedMenuGroup(group);
  };

  const handleCloseMenu = () => {
    setMenuAnchorEl(null);
    setSelectedMenuGroup(null);
  };

  const handleOpenUserMenu = (event: React.MouseEvent<HTMLElement>, user: GroupUser) => {
    event.stopPropagation();
    setUserMenuAnchorEl(event.currentTarget);
    setSelectedMenuUser(user);
  };

  const handleCloseUserMenu = () => {
    setUserMenuAnchorEl(null);
    setSelectedMenuUser(null);
  };

  const openRemoveUserDialog = (user: GroupUser) => {
    setUserToRemove(user);
    setRemoveUserDialogOpen(true);
    handleCloseUserMenu();
  };

  const handleRemoveUser = async () => {
    if (!userToRemove || !selectedGroup) return;

    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      await groupService.removeUserFromGroup(selectedGroup.id, userToRemove.user_id || userToRemove.id);

      setRemoveUserDialogOpen(false);
      setUserToRemove(null);
      showNotification('Member removed successfully', 'success');
      loadGroupUsers(selectedGroup.id);
    } catch (error) {
      console.error('Error removing member from workspace:', error);
      showNotification('Failed to remove member', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleUpdateUserRole = async (userId: string, newRole: 'admin' | 'editor' | 'operator') => {
    if (!selectedGroup) return;

    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      await groupService.updateGroupUser(selectedGroup.id, userId, { role: newRole });

      showNotification(`Member role updated to ${newRole}`, 'success');
      setEditingUserId(null);
      await loadGroupUsers(selectedGroup.id);
    } catch (error) {
      console.error('Error updating user role:', error);
      showNotification('Failed to update member role', 'error');
    } finally {
      setLoading(false);
    }
  };

  const startEditingRole = (userId: string, currentRole: 'admin' | 'editor' | 'operator') => {
    setEditingUserId(userId);
    setEditingUserRole(currentRole);
  };

  const cancelEditingRole = () => {
    setEditingUserId(null);
  };

  const showNotification = (message: string, severity: 'success' | 'error' | 'warning') => {
    setNotification({ open: true, message, severity });
  };


  const getRoleColor = (role: string): 'default' | 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning' => {
    switch (role) {
      case 'admin':
        return 'error';
      case 'manager':
        return 'warning';
      case 'user':
        return 'primary';
      case 'viewer':
        return 'default';
      default:
        return 'default';
    }
  };

  const getRoleIcon = (role: string) => {
    switch (role) {
      case 'admin':
        return <AdminIcon fontSize="small" />;
      case 'editor':
        return <EditIcon fontSize="small" />;
      case 'operator':
        return <PersonIcon fontSize="small" />;
      default:
        return <PersonIcon fontSize="small" />;
    }
  };

  const getRoleDescription = (role: string) => {
    switch (role) {
      case 'admin':
        return 'Full system control';
      case 'editor':
        return 'Can build and modify workflows';
      case 'operator':
        return 'Can execute workflows and monitor';
      default:
        return 'Standard access';
    }
  };

  // Show loading state while permissions are being checked
  if (permissionsLoading) {
    return (
      <Box sx={{ p: 3, textAlign: 'center' }}>
        <LinearProgress sx={{ mb: 2 }} />
        <Typography color="text.secondary">Checking permissions...</Typography>
      </Box>
    );
  }

  // Only allow admin users to access this component
  if (userRole !== 'admin') {
    return (
      <Box sx={{ p: 3, textAlign: 'center' }}>
        <Avatar sx={{ bgcolor: 'error.light', mx: 'auto', mb: 2, width: 60, height: 60 }}>
          <LockIcon fontSize="large" />
        </Avatar>
        <Typography variant="h6" gutterBottom>Access Denied</Typography>
        <Typography variant="body2" color="text.secondary">
          You do not have permission to manage workspaces.
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
          Only administrators can manage workspaces and team members.
        </Typography>
      </Box>
    );
  }

  return (
    <><Box>
      {/* Header Section */}
      <Box sx={{ mb: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Avatar sx={{ bgcolor: 'primary.main', mr: 2 }}>
              <WorkspacesIcon />
            </Avatar>
            <Box>
              <Typography variant="h5" fontWeight="600" color="text.primary">
                Workspaces
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Create team workspaces for collaboration • {groups.length} workspaces • {totalUsers} members
              </Typography>
            </Box>
          </Box>
          <Zoom in={!loading}>
            <Fab
              color="primary"
              size="medium"
              onClick={() => setCreateDialogOpen(true)}
              disabled={loading}
              sx={{ boxShadow: 3 }}
            >
              <AddIcon />
            </Fab>
          </Zoom>
        </Box>
        
        {loading && <LinearProgress sx={{ mb: 2 }} />}
      </Box>


      {/* Search and Filter Section */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                size="small"
                placeholder="Search workspaces by name..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                InputProps={{
                  startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} />,
                }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              {/* View mode buttons removed - always show workspaces */}
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* Info Alert */}
      <Alert 
        severity="info" 
        icon={<SecurityIcon />}
        sx={{ mb: 3, borderRadius: 2 }}
      >
        <Typography variant="body2">
          <strong>Secure Workspaces:</strong> Each workspace provides isolated data and workflows.
          Users can belong to multiple workspaces and switch between them easily.
        </Typography>
      </Alert>

      {/* Main Content Area - Always show workspaces */}
        <Grid container spacing={3}>
          {/* Workspaces List */}
          <Grid item xs={12} lg={8}>
            <Card>
              <CardHeader
                title={
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="h6">Workspaces ({filteredGroups.length})</Typography>
                    <Button
                      variant="outlined"
                      size="small"
                      startIcon={<AddIcon />}
                      onClick={() => setCreateDialogOpen(true)}
                    >
                      New Workspace
                    </Button>
                  </Box>
                }
              />
              <Divider />
              <List>
                {filteredGroups.length === 0 ? (
                  <Box sx={{ textAlign: 'center', py: 6 }}>
                    <GroupsIcon sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
                    <Typography variant="h6" color="text.secondary" gutterBottom>
                      {searchTerm ? 'No groups found' : 'No groups yet'}
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                      {searchTerm 
                        ? 'Try adjusting your search terms' 
                        : 'Create your first group to get started'
                      }
                    </Typography>
                    {!searchTerm && (
                      <Button
                        variant="contained"
                        startIcon={<AddIcon />}
                        onClick={() => setCreateDialogOpen(true)}
                      >
                        Create First Workspace
                      </Button>
                    )}
                  </Box>
                ) : (
                  filteredGroups.map((group, index) => (
                    <React.Fragment key={group.id}>
                      <ListItem
                        sx={{
                          py: 2,
                          cursor: 'pointer',
                          '&:hover': { bgcolor: 'action.hover' },
                          borderRadius: 1,
                          mx: 1,
                        }}
                        onClick={() => {
                          setSelectedGroup(group);
                          loadGroupUsers(group.id);
                        }}
                      >
                        <ListItemAvatar>
                          <Avatar sx={{ bgcolor: group.status === 'active' ? 'success.main' : 'grey.400' }}>
                            <GroupIcon />
                          </Avatar>
                        </ListItemAvatar>
                        <ListItemText
                          primary={
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="subtitle1" fontWeight="medium">
                                {group.name}
                              </Typography>
                              {group.auto_created && (
                                <Chip
                                  label="Auto-created"
                                  size="small"
                                  variant="outlined"
                                  sx={{ height: 20 }}
                                />
                              )}
                            </Box>
                          }
                          secondary={
                            <Typography variant="body2" color="text.secondary">
                              {group.user_count || 0} users • Created {new Date(group.created_at || Date.now()).toLocaleDateString()}
                            </Typography>
                          }
                        />
                        <ListItemSecondaryAction>
                          <Box sx={{ display: 'flex', gap: 1 }}>
                            <Tooltip title="Manage Users">
                              <IconButton
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setSelectedGroup(group);
                                  loadGroupUsers(group.id);
                                }}
                              >
                                <PersonIcon />
                              </IconButton>
                            </Tooltip>
                            <Tooltip title="Group Actions">
                              <IconButton
                                onClick={(e) => handleOpenMenu(e, group)}
                              >
                                <MoreVertIcon />
                              </IconButton>
                            </Tooltip>
                          </Box>
                        </ListItemSecondaryAction>
                      </ListItem>
                      {index < filteredGroups.length - 1 && <Divider variant="inset" component="li" />}
                    </React.Fragment>
                  ))
                )}
              </List>
            </Card>
          </Grid>

          {/* Group Users Panel */}
          <Grid item xs={12} lg={4}>
            <Card sx={{ height: 'fit-content', position: 'sticky', top: 16 }}>
              <CardHeader
                title={
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <PersonIcon />
                    <Typography variant="h6">
                      {selectedGroup ? `${selectedGroup.name} Members` : 'Select a Workspace'}
                    </Typography>
                  </Box>
                }
                action={
                  selectedGroup && (
                    <Button
                      variant="contained"
                      size="small"
                      startIcon={<PersonAddIcon />}
                      onClick={() => setAssignUserDialogOpen(true)}
                      disabled={loading}
                    >
                      Add User
                    </Button>
                  )
                }
              />
              <Divider />
              
              {selectedGroup ? (
                <Box>
                  {/* Group Info */}
                  <Box sx={{ p: 2, bgcolor: 'grey.50' }}>
                    <Typography variant="subtitle2" color="primary" gutterBottom>
                      Group Information
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      <strong>Total Users:</strong> {selectedGroup.user_count || 0}
                    </Typography>
                  </Box>
                  
                  {/* Users List */}
                  <List>
                    {groupUsers.length === 0 ? (
                      <Box sx={{ textAlign: 'center', py: 4 }}>
                        <PersonIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 1 }} />
                        <Typography variant="body2" color="text.secondary" gutterBottom>
                          No users in this group yet
                        </Typography>
                        <Button
                          variant="outlined"
                          size="small"
                          startIcon={<PersonAddIcon />}
                          onClick={() => setAssignUserDialogOpen(true)}
                        >
                          Add First User
                        </Button>
                      </Box>
                    ) : (
                      groupUsers.map((user, index) => (
                        <React.Fragment key={user.id}>
                          <ListItem>
                            <ListItemAvatar>
                              <Avatar sx={{ bgcolor: getRoleColor(user.role) + '.main' }}>
                                {getRoleIcon(user.role)}
                              </Avatar>
                            </ListItemAvatar>
                            <ListItemText
                              primary={
                                <Typography variant="subtitle2">
                                  {user.email}
                                </Typography>
                              }
                              secondary={
                                <Box>
                                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
                                    {editingUserId === (user.user_id || user.id) ? (
                                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                        <FormControl size="small" sx={{ minWidth: 120 }}>
                                          <Select
                                            value={editingUserRole}
                                            onChange={(e) => setEditingUserRole(e.target.value as 'admin' | 'editor' | 'operator')}
                                            sx={{ height: 28 }}
                                          >
                                            <MenuItem value="admin">Admin</MenuItem>
                                            <MenuItem value="editor">Editor</MenuItem>
                                            <MenuItem value="operator">Operator</MenuItem>
                                          </Select>
                                        </FormControl>
                                        <Button
                                          size="small"
                                          variant="contained"
                                          onClick={() => handleUpdateUserRole(user.user_id || user.id, editingUserRole)}
                                          disabled={loading}
                                          sx={{ height: 28, minWidth: 50 }}
                                        >
                                          Save
                                        </Button>
                                        <Button
                                          size="small"
                                          variant="outlined"
                                          onClick={cancelEditingRole}
                                          disabled={loading}
                                          sx={{ height: 28, minWidth: 50 }}
                                        >
                                          Cancel
                                        </Button>
                                      </Box>
                                    ) : (
                                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                        <Chip
                                          icon={getRoleIcon(user.role)}
                                          label={user.role}
                                          size="small"
                                          color={getRoleColor(user.role)}
                                          sx={{ height: 20 }}
                                        />
                                        <Tooltip title="Edit Role">
                                          <IconButton
                                            size="small"
                                            onClick={() => startEditingRole(user.user_id || user.id, user.role)}
                                            sx={{ padding: '2px' }}
                                          >
                                            <EditIcon sx={{ fontSize: 16 }} />
                                          </IconButton>
                                        </Tooltip>
                                      </Box>
                                    )}
                                  </Box>
                                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
                                    {getRoleDescription(user.role)}
                                    {user.auto_created && ' • Auto-assigned'}
                                  </Typography>
                                </Box>
                              }
                            />
                            <ListItemSecondaryAction>
                              <Tooltip title="User Actions">
                                <IconButton 
                                  size="small"
                                  onClick={(e) => handleOpenUserMenu(e, user)}
                                >
                                  <MoreVertIcon fontSize="small" />
                                </IconButton>
                              </Tooltip>
                            </ListItemSecondaryAction>
                          </ListItem>
                          {index < groupUsers.length - 1 && <Divider variant="inset" component="li" />}
                        </React.Fragment>
                      ))
                    )}
                  </List>
                </Box>
              ) : (
                <Box sx={{ textAlign: 'center', py: 6 }}>
                  <WorkspacesIcon sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
                  <Typography variant="h6" color="text.secondary" gutterBottom>
                    Select a Workspace
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Click on a workspace to view and manage team members
                  </Typography>
                </Box>
              )}
            </Card>
          </Grid>
        </Grid>

      {/* Actions Menu */}
      <Menu
        anchorEl={menuAnchorEl}
        open={Boolean(menuAnchorEl)}
        onClose={handleCloseMenu}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
      >
        <MenuList>
          <MenuItem
            onClick={() => selectedMenuGroup && openDeleteDialog(selectedMenuGroup)}
            sx={{ color: 'error.main' }}
          >
            <DeleteIcon sx={{ mr: 1 }} fontSize="small" />
            Delete Workspace
          </MenuItem>
        </MenuList>
      </Menu>

      {/* User Actions Menu */}
      <Menu
        anchorEl={userMenuAnchorEl}
        open={Boolean(userMenuAnchorEl)}
        onClose={handleCloseUserMenu}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
      >
        <MenuList>
          <MenuItem
            onClick={() => selectedMenuUser && openRemoveUserDialog(selectedMenuUser)}
            sx={{ color: 'error.main' }}
          >
            <PersonIcon sx={{ mr: 1 }} fontSize="small" />
            Remove from Workspace
          </MenuItem>
        </MenuList>
      </Menu>

      {/* Remove User Confirmation Dialog */}
      <Dialog
        open={removeUserDialogOpen}
        onClose={() => setRemoveUserDialogOpen(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: { borderRadius: 2 }
        }}
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Avatar sx={{ bgcolor: 'warning.main' }}>
              <WarningIcon />
            </Avatar>
            <Box>
              <Typography variant="h6">Remove User from Group</Typography>
              <Typography variant="body2" color="text.secondary">
                This will revoke the user&apos;s access to this group
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Alert severity="warning" sx={{ mb: 2 }}>
            <Typography variant="body2">
              <strong>Warning:</strong> Removing this user will immediately revoke their access to all data and workflows in this group.
            </Typography>
          </Alert>
          <DialogContentText>
            Are you sure you want to remove <strong>{userToRemove?.email}</strong> from the group <strong>&ldquo;{selectedGroup?.name}&rdquo;</strong>?
            <br /><br />
            They will no longer be able to access any data or workflows within this group.
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, gap: 1 }}>
          <Button
            onClick={() => setRemoveUserDialogOpen(false)}
            variant="outlined"
            disabled={loading}
          >
            Cancel
          </Button>
          <Button
            onClick={handleRemoveUser}
            variant="contained"
            color="warning"
            disabled={loading}
            startIcon={loading ? undefined : <PersonIcon />}
          >
            {loading ? 'Removing...' : 'Remove User'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog
        open={deleteDialogOpen}
        onClose={() => setDeleteDialogOpen(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: { borderRadius: 2 }
        }}
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Avatar sx={{ bgcolor: 'error.main' }}>
              <WarningIcon />
            </Avatar>
            <Box>
              <Typography variant="h6">Delete Group</Typography>
              <Typography variant="body2" color="text.secondary">
                This action cannot be undone
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Alert severity="warning" sx={{ mb: 2 }}>
            <Typography variant="body2">
              <strong>Warning:</strong> Deleting this group will permanently remove all associated data and user access.
            </Typography>
          </Alert>
          <DialogContentText>
            Are you sure you want to delete the group <strong>&ldquo;{groupToDelete?.name}&rdquo;</strong>?
            {groupToDelete?.user_count && groupToDelete.user_count > 0 && (
              <>
                <br /><br />
                This group currently has <strong>{groupToDelete.user_count} user(s)</strong> assigned to it.
              </>
            )}
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, gap: 1 }}>
          <Button
            onClick={() => setDeleteDialogOpen(false)}
            variant="outlined"
            disabled={loading}
          >
            Cancel
          </Button>
          <Button
            onClick={handleDeleteGroup}
            variant="contained"
            color="error"
            disabled={loading}
            startIcon={loading ? undefined : <DeleteIcon />}
          >
            {loading ? 'Deleting...' : 'Delete Group'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Create Workspace Dialog */}
      <Dialog 
        open={createDialogOpen} 
        onClose={() => setCreateDialogOpen(false)}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: { borderRadius: 2 }
        }}
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Avatar sx={{ bgcolor: 'primary.main' }}>
              <WorkspacesIcon />
            </Avatar>
            <Box>
              <Typography variant="h6">Create New Workspace</Typography>
              <Typography variant="body2" color="text.secondary">
                Set up a collaborative workspace for your team
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 3, mt: 1 }}>
            Workspaces provide secure, isolated environments for teams. Members can belong to multiple workspaces.
          </Alert>
          
          <Grid container spacing={3}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Workspace Name"
                value={newGroup.name}
                onChange={(e) => setNewGroup({ ...newGroup, name: e.target.value })}
                placeholder="e.g., Product Team, Marketing, Engineering"
                required
                InputProps={{
                  startAdornment: <GroupIcon sx={{ mr: 1, color: 'text.secondary' }} />
                }}
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Description (Optional)"
                value={newGroup.description}
                onChange={(e) => setNewGroup({ ...newGroup, description: e.target.value })}
                multiline
                rows={3}
                placeholder="Describe the purpose of this workspace..."
                helperText="Help others understand this workspace's purpose"
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, gap: 1 }}>
          <Button 
            onClick={() => setCreateDialogOpen(false)}
            variant="outlined"
            disabled={loading}
          >
            Cancel
          </Button>
          <Button 
            onClick={handleCreateGroup} 
            variant="contained"
            disabled={loading || !newGroup.name}
            startIcon={loading ? undefined : <AddIcon />}
          >
            {loading ? 'Creating...' : 'Create Workspace'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Assign User Dialog */}
      <Dialog 
        open={assignUserDialogOpen} 
        onClose={() => setAssignUserDialogOpen(false)}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: { borderRadius: 2 }
        }}
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Avatar sx={{ bgcolor: 'secondary.main' }}>
              <PersonAddIcon />
            </Avatar>
            <Box>
              <Typography variant="h6">Add Members to {selectedGroup?.name}</Typography>
              <Typography variant="body2" color="text.secondary">
                Select multiple team members and set their permissions
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 3, mt: 1 }}>
            Team members will gain access to this workspace&apos;s data and workflows based on their role.
          </Alert>

          <Alert severity="warning" sx={{ mb: 3 }}>
            <Typography variant="body2">
              <strong>Note:</strong> Users must log in to Kasal at least once to appear in this list.
              If you don&apos;t see the user you&apos;re looking for, ask them to visit the application first.
            </Typography>
          </Alert>

          <Grid container spacing={3}>
            <Grid item xs={12}>
              <FormControl fullWidth required>
                <InputLabel>Select Users</InputLabel>
                <Select
                  multiple
                  value={selectedUserIds}
                  label="Select Users"
                  onChange={(e) => setSelectedUserIds(typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value)}
                  disabled={loadingUsers}
                  renderValue={(selected) => (
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {selected.map((userId) => {
                        const user = availableUsers.find(u => u.id === userId);
                        return (
                          <Chip
                            key={userId}
                            label={user?.email || userId}
                            size="small"
                            onDelete={() => {
                              setSelectedUserIds(selectedUserIds.filter(id => id !== userId));
                            }}
                            deleteIcon={<DeleteIcon />}
                          />
                        );
                      })}
                    </Box>
                  )}
                >
                  {loadingUsers ? (
                    <MenuItem disabled>
                      <Typography color="text.secondary">Loading users...</Typography>
                    </MenuItem>
                  ) : availableUsers.length === 0 ? (
                    <MenuItem disabled>
                      <Typography color="text.secondary">No users found - ask team members to log in first</Typography>
                    </MenuItem>
                  ) : (
                    availableUsers
                      .filter(user => !groupUsers.some(gu => gu.email === user.email))
                      .map((user) => (
                        <MenuItem key={user.id} value={user.id}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                            <PersonIcon fontSize="small" />
                            <Box>
                              <Typography variant="body2">{user.email}</Typography>
                              <Typography variant="caption" color="text.secondary">
                                Last login: {user.last_login ? new Date(user.last_login).toLocaleDateString() : 'Never'}
                              </Typography>
                            </Box>
                          </Box>
                        </MenuItem>
                      ))
                  )}
                </Select>
                <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                  Hold Ctrl/Cmd to select multiple users. Only users who have logged in to Kasal will appear in this list.
                </Typography>
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <FormControl fullWidth required>
                <InputLabel>Role & Permissions</InputLabel>
                <Select
                  value={newUserAssignment.role}
                  label="Role & Permissions"
                  onChange={(e) => setNewUserAssignment({ ...newUserAssignment, role: e.target.value as 'admin' | 'editor' | 'operator' })}
                  startAdornment={getRoleIcon(newUserAssignment.role)}
                >
                  <MenuItem value="operator">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <PersonIcon fontSize="small" />
                      <Box>
                        <Typography variant="body2" fontWeight="medium">Operator</Typography>
                        <Typography variant="caption" color="text.secondary">Execute workflows and monitor execution</Typography>
                      </Box>
                    </Box>
                  </MenuItem>
                  <MenuItem value="editor">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <EditIcon fontSize="small" />
                      <Box>
                        <Typography variant="body2" fontWeight="medium">Editor</Typography>
                        <Typography variant="caption" color="text.secondary">Build and modify AI agent workflows</Typography>
                      </Box>
                    </Box>
                  </MenuItem>
                  <MenuItem value="admin">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <AdminIcon fontSize="small" />
                      <Box>
                        <Typography variant="body2" fontWeight="medium">Admin</Typography>
                        <Typography variant="caption" color="text.secondary">Full system control and configuration</Typography>
                      </Box>
                    </Box>
                  </MenuItem>
                </Select>
              </FormControl>
            </Grid>
          </Grid>
          
          {/* Role Description */}
          <Paper sx={{ p: 2, mt: 2, bgcolor: 'grey.50' }}>
            <Typography variant="subtitle2" color="primary" gutterBottom>
              Selected Role: {newUserAssignment.role.charAt(0).toUpperCase() + newUserAssignment.role.slice(1)}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {getRoleDescription(newUserAssignment.role)}
            </Typography>
          </Paper>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, gap: 1 }}>
          <Button 
            onClick={() => setAssignUserDialogOpen(false)}
            variant="outlined"
            disabled={loading}
          >
            Cancel
          </Button>
          <Button
            onClick={handleAssignUser}
            variant="contained"
            disabled={loading || !selectedUserIds.length}
            startIcon={loading ? undefined : <PersonAddIcon />}
          >
            {loading ? 'Adding...' : `Add ${selectedUserIds.length} Member${selectedUserIds.length !== 1 ? 's' : ''}`}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={() => setNotification({ ...notification, open: false })}
      >
        <Alert
          onClose={() => setNotification({ ...notification, open: false })}
          severity={notification.severity}
          sx={{ width: '100%' }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
    </>
  );
};

export default GroupManagement;