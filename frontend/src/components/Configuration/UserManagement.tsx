import React, { useState, useCallback, memo } from 'react';
import {
  Box,
  Typography,
  Button,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Card,
  CardContent,
  CardHeader,
  Grid,
  Paper,
  Divider,
  Avatar,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  LinearProgress,
  OutlinedInput,
  Checkbox,
  ListItemText,
} from '@mui/material';
import {
  Person as PersonIcon,
  Business as BusinessIcon,
  Groups as GroupsIcon,
  PersonAdd as PersonAddIcon,
  AdminPanelSettings as AdminIcon,
  Visibility as ViewIcon,
  ManageAccounts as ManagerIcon,
  Edit as EditIcon,
  Close as CloseIcon,
} from '@mui/icons-material';

import { GroupService } from '../../api/GroupService';
import type { Group, GroupUser } from '../../api/GroupService';
import { 
  RoleService, 
  Role, 
  User, 
  UserRole, 
} from '../../api/RoleService';

interface UserManagementProps {
  searchTerm: string;
  loading: boolean;
  setLoading: (loading: boolean) => void;
  showNotification: (message: string, severity: 'success' | 'error' | 'warning') => void;
  users: User[];
  userRoles: UserRole[];
  userGroups: GroupUser[];
  groups: Group[];
  roles: Role[];
  onUsersChange: () => void;
}


const UserManagement: React.FC<UserManagementProps> = ({
  searchTerm,
  loading,
  setLoading,
  showNotification,
  users,
  userRoles,
  userGroups,
  groups,
  roles,
  onUsersChange,
}) => {
  // Dialog states
  const [userRoleDialogOpen, setUserRoleDialogOpen] = useState(false);
  const [assignGroupDialogOpen, setAssignGroupDialogOpen] = useState(false);
  const [editUserDialogOpen, setEditUserDialogOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<User | null>(null);

  // Form states
  const [selectedUserForRole, setSelectedUserForRole] = useState('');
  const [selectedRoleForUser, setSelectedRoleForUser] = useState('');
  const [selectedUserForGroup, setSelectedUserForGroup] = useState('');
  const [selectedGroupForUser, setSelectedGroupForUser] = useState('');

  // Edit user states
  const [editUserGroups, setEditUserGroups] = useState<GroupUser[]>([]);
  const [editUserRoles, setEditUserRoles] = useState<UserRole[]>([]);
  const [selectedGroupsForEdit, setSelectedGroupsForEdit] = useState<string[]>([]);
  const [selectedRolesForEdit, setSelectedRolesForEdit] = useState<string[]>([]);

  const loadUserGroups = useCallback(async (user: User) => {
    try {
      const groupService = GroupService.getInstance();
      const allUserGroups: GroupUser[] = [];
      
      // Check each group to see if user is a member
      for (const group of groups) {
        try {
          const groupUsers = await groupService.getGroupUsers(group.id);
          const userInGroup = groupUsers.find(gu => gu.user_id === user.id);
          if (userInGroup) {
            allUserGroups.push(userInGroup);
          }
        } catch (error) {
          // Group might not exist or user might not have access
          continue;
        }
      }
      
      setEditUserGroups(allUserGroups);
      setSelectedGroupsForEdit(allUserGroups.map(ug => ug.group_id));
    } catch (error) {
      console.error('Error loading user groups:', error);
    }
  }, [groups]);

  const loadUserRoles = useCallback(async (user: User) => {
    try {
      const filteredUserRoles = userRoles.filter(ur => ur.user_id === user.id);
      setEditUserRoles(filteredUserRoles);
      setSelectedRolesForEdit(filteredUserRoles.map(ur => ur.role_id));
    } catch (error) {
      console.error('Error loading user roles:', error);
    }
  }, [userRoles]);

  // Load user-specific data when editing
  const loadUserData = useCallback(async (user: User) => {
    if (!user) return;
    
    setLoading(true);
    try {
      await Promise.all([
        loadUserGroups(user),
        loadUserRoles(user),
      ]);
    } catch (error) {
      console.error('Error loading user data:', error);
      showNotification('Failed to load user data', 'error');
    } finally {
      setLoading(false);
    }
  }, [setLoading, showNotification, loadUserGroups, loadUserRoles]);

  // User assignment functions
  const handleAssignRoleToUser = async () => {
    if (!selectedUserForRole || !selectedRoleForUser) {
      showNotification('Please select both user and role', 'warning');
      return;
    }

    setLoading(true);
    try {
      const roleService = RoleService.getInstance();
      await roleService.assignRoleToUser(selectedUserForRole, selectedRoleForUser);
      
      setUserRoleDialogOpen(false);
      setSelectedUserForRole('');
      setSelectedRoleForUser('');
      showNotification('Role assigned successfully', 'success');
      onUsersChange();
    } catch (error) {
      console.error('Error assigning role:', error);
      showNotification('Failed to assign role', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleAssignUserToGroup = async () => {
    if (!selectedUserForGroup || !selectedGroupForUser) {
      showNotification('Please select both user and group', 'warning');
      return;
    }

    setLoading(true);
    try {
      const groupService = GroupService.getInstance();
      const user = users.find(u => u.id === selectedUserForGroup);
      if (!user) {
        showNotification('User not found', 'error');
        return;
      }

      await groupService.assignUserToGroup(selectedGroupForUser, {
        user_email: user.email,
        role: 'user', // Default role
      });
      
      setAssignGroupDialogOpen(false);
      setSelectedUserForGroup('');
      setSelectedGroupForUser('');
      showNotification('User assigned to group successfully', 'success');
      onUsersChange();
    } catch (error) {
      console.error('Error assigning user to group:', error);
      showNotification('Failed to assign user to group', 'error');
    } finally {
      setLoading(false);
    }
  };

  // Edit user functions
  const handleEditUser = (user: User) => {
    setEditingUser(user);
    setEditUserDialogOpen(true);
    loadUserData(user);
  };

  const handleGroupSelectionChange = (event: { target: { value: string | string[] } }) => {
    const value = event.target.value;
    setSelectedGroupsForEdit(typeof value === 'string' ? value.split(',') : value);
  };

  const handleRoleSelectionChange = (event: { target: { value: string | string[] } }) => {
    const value = event.target.value;
    setSelectedRolesForEdit(typeof value === 'string' ? value.split(',') : value);
  };

  const handleUpdateUserAssignments = async () => {
    if (!editingUser) return;

    setLoading(true);
    try {
      const roleService = RoleService.getInstance();
      const groupService = GroupService.getInstance();

      // Update role assignments
      const currentRoleIds = editUserRoles.map(ur => ur.role_id);
      const rolesToAdd = selectedRolesForEdit.filter(id => !currentRoleIds.includes(id));
      const rolesToRemove = currentRoleIds.filter(id => !selectedRolesForEdit.includes(id));

      // Add new roles
      for (const roleId of rolesToAdd) {
        await roleService.assignRoleToUser(editingUser.id, roleId);
      }

      // Remove roles
      for (const roleId of rolesToRemove) {
        await roleService.removeRoleFromUser(editingUser.id, roleId);
      }

      // Update tenant assignments
      const currentGroupIds = editUserGroups.map(ug => ug.group_id);
      const groupsToAdd = selectedGroupsForEdit.filter(id => !currentGroupIds.includes(id));
      const groupsToRemove = currentGroupIds.filter(id => !selectedGroupsForEdit.includes(id));

      // Add to new tenants
      for (const groupId of groupsToAdd) {
        await groupService.assignUserToGroup(groupId, {
          user_email: editingUser.email,
          role: 'user', // Default role
        });
      }

      // Remove from tenants
      for (const groupId of groupsToRemove) {
        await groupService.removeUserFromGroup(groupId, editingUser.id);
      }

      setEditUserDialogOpen(false);
      setEditingUser(null);
      setEditUserGroups([]);
      setEditUserRoles([]);
      setSelectedGroupsForEdit([]);
      setSelectedRolesForEdit([]);
      showNotification('User assignments updated successfully', 'success');
      onUsersChange();
    } catch (error) {
      console.error('Error updating user assignments:', error);
      showNotification('Failed to update user assignments', 'error');
    } finally {
      setLoading(false);
    }
  };

  // Filter functions
  const filteredUsers = users.filter(user =>
    user.email.toLowerCase().includes(searchTerm.toLowerCase()) ||
    user.username.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Utility functions
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
      case 'manager':
        return <ManagerIcon fontSize="small" />;
      case 'user':
        return <PersonIcon fontSize="small" />;
      case 'viewer':
        return <ViewIcon fontSize="small" />;
      default:
        return <PersonIcon fontSize="small" />;
    }
  };

  return (
    <>
      {/* User Role Assignments */}
      <Grid container spacing={3}>
        <Grid item xs={12} md={8}>
          <Card>
            <CardHeader
              title={
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <Typography variant="h6">User Assignments</Typography>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <Button
                      variant="contained"
                      size="small"
                      startIcon={<PersonAddIcon />}
                      onClick={() => setUserRoleDialogOpen(true)}
                    >
                      Assign Role
                    </Button>
                    <Button
                      variant="outlined"
                      size="small"
                      startIcon={<GroupsIcon />}
                      onClick={() => {
                        setSelectedUserForGroup('');
                        setSelectedGroupForUser('');
                        setAssignGroupDialogOpen(true);
                      }}
                    >
                      Assign Group
                    </Button>
                  </Box>
                </Box>
              }
            />
            <Divider />
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>User</TableCell>
                    <TableCell>Groups</TableCell>
                    <TableCell>Roles</TableCell>
                    <TableCell>Assigned Date</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredUsers.map((user) => {
                    const userRoleAssignments = userRoles.filter(ur => ur.user_id === user.id);
                    const userGroupAssignments = userGroups.filter(ug => ug.user_id === user.id);
                    return (
                      <TableRow key={user.id}>
                        <TableCell>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Avatar sx={{ width: 32, height: 32 }}>
                              <PersonIcon fontSize="small" />
                            </Avatar>
                            <Box>
                              <Typography variant="body2" fontWeight="medium">
                                {user.email}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {user.username}
                              </Typography>
                            </Box>
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                            {userGroupAssignments.map((groupAssignment) => {
                              const group = groups.find(g => g.id === groupAssignment.group_id);
                              return (
                                <Chip
                                  key={groupAssignment.id}
                                  label={group?.name || 'Unknown Group'}
                                  size="small"
                                  color="info"
                                  icon={<BusinessIcon fontSize="small" />}
                                />
                              );
                            })}
                            {userGroupAssignments.length === 0 && (
                              <Typography variant="caption" color="text.secondary">
                                No groups assigned
                              </Typography>
                            )}
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                            {userRoleAssignments.map((assignment) => {
                              const role = roles.find(r => r.id === assignment.role_id);
                              return (
                                <Chip
                                  key={assignment.id}
                                  label={role?.name || 'Unknown'}
                                  size="small"
                                  color={getRoleColor(role?.name || '')}
                                  icon={getRoleIcon(role?.name || '')}
                                />
                              );
                            })}
                            {userRoleAssignments.length === 0 && (
                              <Typography variant="caption" color="text.secondary">
                                No roles assigned
                              </Typography>
                            )}
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Typography variant="caption" color="text.secondary">
                            {userRoleAssignments.length > 0 
                              ? new Date(userRoleAssignments[0].assigned_at).toLocaleDateString()
                              : '-'
                            }
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <IconButton size="small" onClick={() => handleEditUser(user)}>
                            <EditIcon />
                          </IconButton>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          </Card>
        </Grid>
        <Grid item xs={12} md={4}>
          <Card>
            <CardHeader title="User Statistics" />
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                <Typography variant="body2">Total Users:</Typography>
                <Typography variant="body2" fontWeight="bold">{users.length}</Typography>
              </Box>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                <Typography variant="body2">Admin Users:</Typography>
                <Typography variant="body2" fontWeight="bold">
                  {userRoles.filter(ur => {
                    const role = roles.find(r => r.id === ur.role_id);
                    return role?.name === 'admin';
                  }).length}
                </Typography>
              </Box>
              <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography variant="body2">Unassigned Users:</Typography>
                <Typography variant="body2" fontWeight="bold">
                  {users.filter(user => !userRoles.some(ur => ur.user_id === user.id)).length}
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Assign Role to User Dialog */}
      <Dialog open={userRoleDialogOpen} onClose={() => setUserRoleDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Assign Role to User</DialogTitle>
        <DialogContent>
          <FormControl fullWidth margin="normal" required>
            <InputLabel>User</InputLabel>
            <Select
              value={selectedUserForRole}
              label="User"
              onChange={(e) => setSelectedUserForRole(e.target.value)}
            >
              {users.map((user) => (
                <MenuItem key={user.id} value={user.id}>
                  {user.email} ({user.username})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <FormControl fullWidth margin="normal" required>
            <InputLabel>Role</InputLabel>
            <Select
              value={selectedRoleForUser}
              label="Role"
              onChange={(e) => setSelectedRoleForUser(e.target.value)}
            >
              {roles.map((role) => (
                <MenuItem key={role.id} value={role.id}>
                  {role.name} - {role.description}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setUserRoleDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleAssignRoleToUser} variant="contained" disabled={loading}>
            Assign Role
          </Button>
        </DialogActions>
      </Dialog>

      {/* Assign User to Group Dialog */}
      <Dialog 
        open={assignGroupDialogOpen} 
        onClose={() => {
          setAssignGroupDialogOpen(false);
          setSelectedUserForGroup('');
          setSelectedGroupForUser('');
        }} 
        maxWidth="sm" 
        fullWidth
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <GroupsIcon />
          Assign User to Group
        </DialogTitle>
        <DialogContent>
          <FormControl fullWidth margin="normal" required>
            <InputLabel>User</InputLabel>
            <Select
              value={selectedUserForGroup}
              label="User"
              onChange={(e) => {
                setSelectedUserForGroup(e.target.value);
                setSelectedGroupForUser(''); // Clear group selection when user changes
              }}
            >
              {users.map((user) => {
                const userGroupCount = userGroups.filter(ug => ug.user_id === user.id).length;
                return (
                  <MenuItem key={user.id} value={user.id}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <PersonIcon fontSize="small" />
                      <Box sx={{ flexGrow: 1 }}>
                        <Typography variant="body2">{user.email}</Typography>
                        <Typography variant="caption" color="text.secondary">
                          {user.username} • {userGroupCount} group{userGroupCount !== 1 ? 's' : ''}
                        </Typography>
                      </Box>
                    </Box>
                  </MenuItem>
                );
              })}
            </Select>
          </FormControl>
          <FormControl fullWidth margin="normal" required>
            <InputLabel>Group</InputLabel>
            <Select
              value={selectedGroupForUser}
              label="Group"
              onChange={(e) => setSelectedGroupForUser(e.target.value)}
            >
              {groups.filter(group => {
                // Only show groups the selected user is not already a member of
                if (!selectedUserForGroup) return true;
                return !userGroups.some(ug => ug.user_id === selectedUserForGroup && ug.group_id === group.id);
              }).map((group) => (
                <MenuItem key={group.id} value={group.id}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <BusinessIcon fontSize="small" />
                    <Box>
                      <Typography variant="body2">{group.name}</Typography>
                      <Typography variant="caption" color="text.secondary">
                        {group.email_domain} • {group.user_count || 0} users
                      </Typography>
                    </Box>
                  </Box>
                </MenuItem>
              ))}
            </Select>
            {selectedUserForGroup && groups.filter(group => {
              return !userGroups.some(ug => ug.user_id === selectedUserForGroup && ug.group_id === group.id);
            }).length === 0 && (
              <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
                This user is already a member of all available groups
              </Typography>
            )}
          </FormControl>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => {
            setAssignGroupDialogOpen(false);
            setSelectedUserForGroup('');
            setSelectedGroupForUser('');
          }}>Cancel</Button>
          <Button 
            onClick={handleAssignUserToGroup} 
            variant="contained" 
            disabled={loading || !selectedUserForGroup || !selectedGroupForUser}
            startIcon={<GroupsIcon />}
          >
            Assign to Group
          </Button>
        </DialogActions>
      </Dialog>

      {/* Edit User Assignments Dialog */}
      <Dialog 
        open={editUserDialogOpen} 
        onClose={() => setEditUserDialogOpen(false)} 
        maxWidth="md" 
        fullWidth
        PaperProps={{
          sx: { minHeight: '70vh' }
        }}
      >
        <DialogTitle sx={{ pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
            <Avatar sx={{ bgcolor: 'primary.main' }}>
              <PersonIcon />
            </Avatar>
            <Box>
              <Typography variant="h6">Edit User Assignments</Typography>
              <Typography variant="body2" color="text.secondary">
                Manage group memberships and role assignments for {editingUser?.email}
              </Typography>
            </Box>
            <Box sx={{ flexGrow: 1 }} />
            <IconButton onClick={() => setEditUserDialogOpen(false)} size="small">
              <CloseIcon />
            </IconButton>
          </Box>
          
          {/* User Info */}
          {editingUser && (
            <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                <Avatar sx={{ bgcolor: 'secondary.main' }}>
                  <PersonIcon />
                </Avatar>
                <Box>
                  <Typography variant="subtitle1" fontWeight="medium">
                    {editingUser.email}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Username: {editingUser.username} • Status: {editingUser.status}
                  </Typography>
                </Box>
              </Box>
            </Paper>
          )}
        </DialogTitle>
        
        <DialogContent sx={{ px: 3 }}>
          {loading && <LinearProgress sx={{ mb: 2 }} />}
          
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Typography variant="h6" sx={{ mb: 2 }}>Group Memberships</Typography>
              <FormControl fullWidth>
                <InputLabel>Select Groups</InputLabel>
                <Select
                  multiple
                  value={selectedGroupsForEdit}
                  onChange={handleGroupSelectionChange}
                  input={<OutlinedInput label="Select Groups" />}
                  renderValue={(selected) => (
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {selected.map((groupId) => {
                        const group = groups.find(g => g.id === groupId);
                        return (
                          <Chip 
                            key={groupId} 
                            label={group?.name || 'Unknown'} 
                            size="small"
                            color="info"
                            icon={<BusinessIcon fontSize="small" />}
                          />
                        );
                      })}
                    </Box>
                  )}
                >
                  {groups.map((group) => (
                    <MenuItem key={group.id} value={group.id}>
                      <Checkbox checked={selectedGroupsForEdit.indexOf(group.id) > -1} />
                      <ListItemText primary={group.name} secondary={group.email_domain} />
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            
            <Grid item xs={12} md={6}>
              <Typography variant="h6" sx={{ mb: 2 }}>Role Assignments</Typography>
              <FormControl fullWidth>
                <InputLabel>Select Roles</InputLabel>
                <Select
                  multiple
                  value={selectedRolesForEdit}
                  onChange={handleRoleSelectionChange}
                  input={<OutlinedInput label="Select Roles" />}
                  renderValue={(selected) => (
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {selected.map((roleId) => {
                        const role = roles.find(r => r.id === roleId);
                        return (
                          <Chip 
                            key={roleId} 
                            label={role?.name || 'Unknown'} 
                            size="small"
                            color={getRoleColor(role?.name || '')}
                            icon={getRoleIcon(role?.name || '')}
                          />
                        );
                      })}
                    </Box>
                  )}
                >
                  {roles.map((role) => (
                    <MenuItem key={role.id} value={role.id}>
                      <Checkbox checked={selectedRolesForEdit.indexOf(role.id) > -1} />
                      <ListItemText primary={role.name} secondary={role.description} />
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
          </Grid>
          
          {/* Summary */}
          <Paper sx={{ p: 2, mt: 3, bgcolor: 'grey.50' }}>
            <Typography variant="subtitle2" color="primary" gutterBottom>
              Assignment Summary
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={6}>
                <Typography variant="body2">
                  <strong>Groups:</strong> {selectedGroupsForEdit.length} selected
                </Typography>
              </Grid>
              <Grid item xs={6}>
                <Typography variant="body2">
                  <strong>Roles:</strong> {selectedRolesForEdit.length} selected
                </Typography>
              </Grid>
            </Grid>
          </Paper>
        </DialogContent>
        
        <DialogActions sx={{ px: 3, py: 2 }}>
          <Button onClick={() => setEditUserDialogOpen(false)} disabled={loading}>
            Cancel
          </Button>
          <Button 
            onClick={handleUpdateUserAssignments} 
            variant="contained" 
            disabled={loading}
          >
            Update Assignments
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default memo(UserManagement);