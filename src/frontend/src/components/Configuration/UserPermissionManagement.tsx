import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Switch,
  Alert,
  CircularProgress,
  Chip,
  Tooltip,
  FormControlLabel,
  Card,
  CardContent,
  CardHeader,
  Avatar,
  Snackbar,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  DialogContentText,
} from '@mui/material';
import AdminPanelSettingsIcon from '@mui/icons-material/AdminPanelSettings';
import PersonIcon from '@mui/icons-material/Person';
import WorkspacesIcon from '@mui/icons-material/Workspaces';
import DeleteIcon from '@mui/icons-material/Delete';
import WarningIcon from '@mui/icons-material/Warning';
import { UserService, User, UserPermissionUpdate } from '../../api/UserService';

interface NotificationState {
  open: boolean;
  message: string;
  severity: 'success' | 'error' | 'warning' | 'info';
}

function UserPermissionManagement(): JSX.Element {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [updating, setUpdating] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [userToDelete, setUserToDelete] = useState<User | null>(null);
  const [notification, setNotification] = useState<NotificationState>({
    open: false,
    message: '',
    severity: 'info'
  });

  const userService = UserService.getInstance();

  useEffect(() => {
    loadUsers();
  }, []);

  const loadUsers = async () => {
    try {
      setLoading(true);
      const usersData = await userService.getUsers();
      setUsers(usersData);
    } catch (error) {
      console.error('Error loading users:', error);
      showNotification('Failed to load users', 'error');
    } finally {
      setLoading(false);
    }
  };

  const showNotification = (message: string, severity: NotificationState['severity']) => {
    setNotification({ open: true, message, severity });
  };

  const handlePermissionChange = async (
    userId: string,
    permission: keyof UserPermissionUpdate,
    value: boolean
  ) => {
    try {
      setUpdating(userId);

      const update: UserPermissionUpdate = {
        [permission]: value
      };

      const updatedUser = await userService.updateUserPermissions(userId, update);

      // Update the user in the local state
      setUsers(users.map(user =>
        user.id === userId ? updatedUser : user
      ));

      const permissionName = permission === 'is_system_admin'
        ? 'System Admin'
        : 'Personal Workspace Manager';

      showNotification(
        `${permissionName} permission ${value ? 'granted' : 'revoked'} for ${updatedUser.email}`,
        'success'
      );
    } catch (error) {
      console.error('Error updating user permissions:', error);
      showNotification('Failed to update user permissions', 'error');
    } finally {
      setUpdating(null);
    }
  };

  const openDeleteDialog = (user: User) => {
    setUserToDelete(user);
    setDeleteDialogOpen(true);
  };

  const handleDeleteUser = async () => {
    if (!userToDelete) return;

    try {
      setDeleting(userToDelete.id);
      await userService.deleteUser(userToDelete.id);

      // Remove the user from the local state
      setUsers(users.filter(user => user.id !== userToDelete.id));

      setDeleteDialogOpen(false);
      setUserToDelete(null);
      showNotification(`User ${userToDelete.email} has been deleted successfully`, 'success');
    } catch (error) {
      console.error('Error deleting user:', error);
      showNotification('Failed to delete user', 'error');
    } finally {
      setDeleting(null);
    }
  };

  const getUserRoleBadges = (user: User) => {
    const badges = [];

    if (user.is_system_admin) {
      badges.push(
        <Chip
          key="sys"
          label="System Admin"
          color="error"
          size="small"
          icon={<AdminPanelSettingsIcon />}
        />
      );
    }

    if (user.is_personal_workspace_manager) {
      badges.push(
        <Chip
          key="pw"
          label="Personal Workspace Manager"
          color="primary"
          size="small"
          icon={<WorkspacesIcon />}
        />
      );
    }

    if (badges.length === 0) {
      badges.push(
        <Chip
          key="std"
          label="Standard User"
          color="default"
          size="small"
          icon={<PersonIcon />}
        />
      );
    }

    return <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>{badges}</Box>;
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
        <Typography variant="body1" sx={{ ml: 2 }}>Loading users...</Typography>
      </Box>
    );
  }

  return (
    <Box>
      <Card>
        <CardHeader
          title={
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <Avatar sx={{ bgcolor: 'error.main', mr: 2 }}>
                <AdminPanelSettingsIcon />
              </Avatar>
              <Box>
                <Typography variant="h5" fontWeight="600" color="text.primary">
                  User Permission Management
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Grant system-wide and personal workspace permissions to users
                </Typography>
              </Box>
            </Box>
          }
        />

        <CardContent>
          <Alert severity="warning" sx={{ mb: 3 }}>
            <Typography variant="body2">
              <strong>System Admin Only:</strong> Only you can manage these permissions.
              Use carefully as these grant significant access rights.
            </Typography>
          </Alert>

          <Typography variant="h6" gutterBottom sx={{ mt: 2, mb: 2 }}>
            Permission Overview
          </Typography>

          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: 2, mb: 3 }}>
            <Paper sx={{ p: 2, border: '1px solid', borderColor: 'error.light' }}>
              <Typography variant="subtitle1" color="error.main" fontWeight="600">
                System Admin
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Full platform control, can manage all workspaces and grant permissions
              </Typography>
            </Paper>

            <Paper sx={{ p: 2, border: '1px solid', borderColor: 'primary.light' }}>
              <Typography variant="subtitle1" color="primary.main" fontWeight="600">
                Personal Workspace Manager
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Can configure Databricks, Memory, and Volumes in their personal workspace
              </Typography>
            </Paper>
          </Box>

          <TableContainer component={Paper} sx={{ mt: 2 }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>User</TableCell>
                  <TableCell>Current Permissions</TableCell>
                  <TableCell align="center">System Admin</TableCell>
                  <TableCell align="center">Personal Workspace Manager</TableCell>
                  <TableCell>Last Login</TableCell>
                  <TableCell align="center">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {users.map((user) => (
                  <TableRow key={user.id} hover>
                    <TableCell>
                      <Box>
                        <Typography variant="body1" fontWeight="500">
                          {user.email}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          ID: {user.id}
                        </Typography>
                      </Box>
                    </TableCell>

                    <TableCell>
                      {getUserRoleBadges(user)}
                    </TableCell>

                    <TableCell align="center">
                      <Tooltip title="Grant/revoke system-wide administrative access">
                        <FormControlLabel
                          control={
                            <Switch
                              checked={user.is_system_admin}
                              onChange={(e) => handlePermissionChange(
                                user.id,
                                'is_system_admin',
                                e.target.checked
                              )}
                              disabled={updating === user.id}
                              color="error"
                            />
                          }
                          label=""
                        />
                      </Tooltip>
                    </TableCell>

                    <TableCell align="center">
                      <Tooltip title="Allow user to configure their personal workspace">
                        <FormControlLabel
                          control={
                            <Switch
                              checked={user.is_personal_workspace_manager}
                              onChange={(e) => handlePermissionChange(
                                user.id,
                                'is_personal_workspace_manager',
                                e.target.checked
                              )}
                              disabled={updating === user.id}
                              color="primary"
                            />
                          }
                          label=""
                        />
                      </Tooltip>
                    </TableCell>

                    <TableCell>
                      <Typography variant="body2" color="text.secondary">
                        {user.last_login
                          ? new Date(user.last_login).toLocaleDateString()
                          : 'Never'
                        }
                      </Typography>
                    </TableCell>

                    <TableCell align="center">
                      <Tooltip title="Delete user permanently">
                        <IconButton
                          color="error"
                          onClick={() => openDeleteDialog(user)}
                          disabled={deleting === user.id}
                          size="small"
                        >
                          {deleting === user.id ? (
                            <CircularProgress size={20} />
                          ) : (
                            <DeleteIcon />
                          )}
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          {users.length === 0 && (
            <Box textAlign="center" py={4}>
              <Typography variant="h6" color="text.secondary">
                No users found
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Users will appear here once they log in to the system
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Delete User Confirmation Dialog */}
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
              <Typography variant="h6">Delete User</Typography>
              <Typography variant="body2" color="text.secondary">
                This action cannot be undone
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Alert severity="error" sx={{ mb: 2 }}>
            <Typography variant="body2">
              <strong>Warning:</strong> This will permanently delete the user and all their data.
              This action cannot be undone.
            </Typography>
          </Alert>
          <DialogContentText>
            Are you sure you want to delete the user <strong>{userToDelete?.email}</strong>?
            <br /><br />
            This will permanently remove:
            • User account and authentication
            • All workspace memberships
            • User permissions and settings
            • Any associated data
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ p: 2.5, gap: 1 }}>
          <Button
            onClick={() => setDeleteDialogOpen(false)}
            variant="outlined"
            disabled={deleting !== null}
          >
            Cancel
          </Button>
          <Button
            onClick={handleDeleteUser}
            variant="contained"
            color="error"
            disabled={deleting !== null}
            startIcon={deleting ? <CircularProgress size={16} /> : <DeleteIcon />}
          >
            {deleting ? 'Deleting...' : 'Delete User'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={() => setNotification({ ...notification, open: false })}
        message={notification.message}
      />
    </Box>
  );
}

export default UserPermissionManagement;