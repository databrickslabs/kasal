import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
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
  Grid,
  Tooltip,
} from '@mui/material';
import { TenantService, Tenant, TenantUser, CreateTenantRequest, AssignUserRequest } from '../../api/TenantService';
import {
  Add as AddIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  Group as GroupIcon,
  Person as PersonIcon,
  Business as BusinessIcon,
  CheckCircle as ActiveIcon,
  Pause as SuspendedIcon,
  Archive as ArchivedIcon,
} from '@mui/icons-material';


const TenantManagement: React.FC = () => {
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [selectedTenant, setSelectedTenant] = useState<Tenant | null>(null);
  const [tenantUsers, setTenantUsers] = useState<TenantUser[]>([]);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [assignUserDialogOpen, setAssignUserDialogOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error' | 'warning',
  });

  // Form states
  const [newTenant, setNewTenant] = useState<CreateTenantRequest>({
    name: '',
    email_domain: '',
    description: '',
  });
  const [newUserAssignment, setNewUserAssignment] = useState<AssignUserRequest>({
    user_email: '',
    role: 'user',
  });

  const loadTenants = useCallback(async () => {
    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      const tenantsData = await tenantService.getTenants();
      setTenants(tenantsData);
    } catch (error) {
      console.error('Error loading tenants:', error);
      showNotification('Failed to load tenants', 'error');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadTenants();
  }, [loadTenants]);

  const loadTenantUsers = async (tenantId: string) => {
    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      const usersData = await tenantService.getTenantUsers(tenantId);
      setTenantUsers(usersData);
    } catch (error) {
      console.error('Error loading tenant users:', error);
      showNotification('Failed to load tenant users', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateTenant = async () => {
    if (!newTenant.name || !newTenant.email_domain) {
      showNotification('Please fill in all required fields', 'warning');
      return;
    }

    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      await tenantService.createTenant(newTenant);
      
      setCreateDialogOpen(false);
      setNewTenant({ name: '', email_domain: '', description: '' });
      showNotification('Tenant created successfully', 'success');
      loadTenants();
    } catch (error) {
      console.error('Error creating tenant:', error);
      showNotification('Failed to create tenant', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleAssignUser = async () => {
    if (!newUserAssignment.user_email || !selectedTenant) {
      showNotification('Please fill in all required fields', 'warning');
      return;
    }

    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      await tenantService.assignUserToTenant(selectedTenant.id, newUserAssignment);
      
      setAssignUserDialogOpen(false);
      setNewUserAssignment({ user_email: '', role: 'user' });
      showNotification('User assigned successfully', 'success');
      loadTenantUsers(selectedTenant.id);
    } catch (error) {
      console.error('Error assigning user:', error);
      showNotification('Failed to assign user', 'error');
    } finally {
      setLoading(false);
    }
  };

  const showNotification = (message: string, severity: 'success' | 'error' | 'warning') => {
    setNotification({ open: true, message, severity });
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'active':
        return <ActiveIcon color="success" fontSize="small" />;
      case 'suspended':
        return <SuspendedIcon color="warning" fontSize="small" />;
      case 'archived':
        return <ArchivedIcon color="disabled" fontSize="small" />;
      default:
        return undefined;
    }
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

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
        <GroupIcon sx={{ mr: 1, color: 'primary.main' }} />
        <Typography variant="h6">Tenant Management</Typography>
        <Box sx={{ ml: 'auto' }}>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => setCreateDialogOpen(true)}
            disabled={loading}
          >
            Create Tenant
          </Button>
        </Box>
      </Box>

      <Alert severity="info" sx={{ mb: 3 }}>
        <Typography variant="body2">
          <strong>Manual Tenant Management:</strong> Create tenants manually and assign users by email. 
          This provides full control over data isolation and can later evolve into Unity Catalog integration.
        </Typography>
      </Alert>

      <Grid container spacing={3}>
        {/* Tenants List */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Tenants ({tenants.length})
              </Typography>
              <TableContainer>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Name</TableCell>
                      <TableCell>Domain</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Users</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {tenants.map((tenant) => (
                      <TableRow key={tenant.id}>
                        <TableCell>
                          <Box sx={{ display: 'flex', alignItems: 'center' }}>
                            <BusinessIcon fontSize="small" sx={{ mr: 1, color: 'text.secondary' }} />
                            <Box>
                              <Typography variant="body2" fontWeight="medium">
                                {tenant.name}
                              </Typography>
                              {tenant.auto_created && (
                                <Typography variant="caption" color="text.secondary">
                                  Auto-created
                                </Typography>
                              )}
                            </Box>
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Typography variant="body2" color="text.secondary">
                            {tenant.email_domain}
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <Chip
                            icon={getStatusIcon(tenant.status)}
                            label={tenant.status}
                            size="small"
                            color={tenant.status === 'active' ? 'success' : 'default'}
                          />
                        </TableCell>
                        <TableCell>
                          <Typography variant="body2">
                            {tenant.user_count || 0}
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <Tooltip title="View Users">
                            <IconButton 
                              size="small"
                              onClick={() => {
                                setSelectedTenant(tenant);
                                loadTenantUsers(tenant.id);
                              }}
                            >
                              <PersonIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>

        {/* Tenant Users */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6">
                  {selectedTenant ? `${selectedTenant.name} Users` : 'Select a Tenant'}
                </Typography>
                {selectedTenant && (
                  <Button
                    variant="outlined"
                    size="small"
                    startIcon={<AddIcon />}
                    onClick={() => setAssignUserDialogOpen(true)}
                    sx={{ ml: 'auto' }}
                    disabled={loading}
                  >
                    Assign User
                  </Button>
                )}
              </Box>
              
              {selectedTenant ? (
                <TableContainer>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Email</TableCell>
                        <TableCell>Role</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {tenantUsers.map((user) => (
                        <TableRow key={user.id}>
                          <TableCell>
                            <Box>
                              <Typography variant="body2">
                                {user.email}
                              </Typography>
                              {user.auto_created && (
                                <Typography variant="caption" color="text.secondary">
                                  Auto-assigned
                                </Typography>
                              )}
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Chip
                              label={user.role}
                              size="small"
                              color={getRoleColor(user.role)}
                            />
                          </TableCell>
                          <TableCell>
                            <Chip
                              label={user.status}
                              size="small"
                              color={user.status === 'active' ? 'success' : 'default'}
                            />
                          </TableCell>
                          <TableCell>
                            <Tooltip title="Edit User">
                              <IconButton size="small">
                                <EditIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                            <Tooltip title="Remove User">
                              <IconButton size="small" color="error">
                                <DeleteIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              ) : (
                <Box sx={{ textAlign: 'center', py: 4, color: 'text.secondary' }}>
                  <PersonIcon sx={{ fontSize: 48, mb: 1 }} />
                  <Typography>
                    Select a tenant to view and manage users
                  </Typography>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Create Tenant Dialog */}
      <Dialog 
        open={createDialogOpen} 
        onClose={() => setCreateDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Create New Tenant</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 1 }}>
            <TextField
              fullWidth
              label="Tenant Name"
              value={newTenant.name}
              onChange={(e) => setNewTenant({ ...newTenant, name: e.target.value })}
              sx={{ mb: 2 }}
              placeholder="e.g., Team Alpha, Marketing Department"
            />
            <TextField
              fullWidth
              label="Email Domain"
              value={newTenant.email_domain}
              onChange={(e) => setNewTenant({ ...newTenant, email_domain: e.target.value })}
              sx={{ mb: 2 }}
              placeholder="e.g., alpha.databricks.com or team-alpha"
              helperText="Domain identifier for this tenant (can be virtual)"
            />
            <TextField
              fullWidth
              label="Description"
              value={newTenant.description}
              onChange={(e) => setNewTenant({ ...newTenant, description: e.target.value })}
              multiline
              rows={3}
              placeholder="Optional description for this tenant..."
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateDialogOpen(false)}>Cancel</Button>
          <Button 
            onClick={handleCreateTenant} 
            variant="contained"
            disabled={loading}
          >
            Create Tenant
          </Button>
        </DialogActions>
      </Dialog>

      {/* Assign User Dialog */}
      <Dialog 
        open={assignUserDialogOpen} 
        onClose={() => setAssignUserDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Assign User to {selectedTenant?.name}</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 1 }}>
            <TextField
              fullWidth
              label="User Email"
              value={newUserAssignment.user_email}
              onChange={(e) => setNewUserAssignment({ ...newUserAssignment, user_email: e.target.value })}
              sx={{ mb: 2 }}
              placeholder="user@databricks.com"
            />
            <FormControl fullWidth>
              <InputLabel>Role</InputLabel>
              <Select
                value={newUserAssignment.role}
                label="Role"
                onChange={(e) => setNewUserAssignment({ ...newUserAssignment, role: e.target.value as 'admin' | 'manager' | 'user' | 'viewer' })}
              >
                <MenuItem value="viewer">Viewer (Read-only)</MenuItem>
                <MenuItem value="user">User (Can execute workflows)</MenuItem>
                <MenuItem value="manager">Manager (Can manage workflows)</MenuItem>
                <MenuItem value="admin">Admin (Full control)</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAssignUserDialogOpen(false)}>Cancel</Button>
          <Button 
            onClick={handleAssignUser} 
            variant="contained"
            disabled={loading}
          >
            Assign User
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
  );
};

export default TenantManagement;