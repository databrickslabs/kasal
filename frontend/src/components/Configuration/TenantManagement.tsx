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
  Stepper,
  Step,
  StepLabel,
  StepContent,
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
import { TenantService, Tenant, TenantUser, CreateTenantRequest, AssignUserRequest } from '../../api/TenantService';
import {
  Add as AddIcon,
  Group as GroupIcon,
  Person as PersonIcon,
  Business as BusinessIcon,
  CheckCircle as ActiveIcon,
  Pause as SuspendedIcon,
  Archive as ArchivedIcon,
  Groups as GroupsIcon,
  PersonAdd as PersonAddIcon,
  Info as InfoIcon,
  Search as SearchIcon,
  MoreVert as MoreVertIcon,
  Security as SecurityIcon,
  AdminPanelSettings as AdminIcon,
  Visibility as ViewIcon,
  ManageAccounts as ManagerIcon,
  Delete as DeleteIcon,
  Warning as WarningIcon,
} from '@mui/icons-material';


const TenantManagement: React.FC = () => {
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [selectedTenant, setSelectedTenant] = useState<Tenant | null>(null);
  const [tenantUsers, setTenantUsers] = useState<TenantUser[]>([]);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [assignUserDialogOpen, setAssignUserDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [tenantToDelete, setTenantToDelete] = useState<Tenant | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [viewMode, setViewMode] = useState<'overview' | 'groups' | 'users'>('overview');
  const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedMenuTenant, setSelectedMenuTenant] = useState<Tenant | null>(null);
  const [userMenuAnchorEl, setUserMenuAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedMenuUser, setSelectedMenuUser] = useState<TenantUser | null>(null);
  const [removeUserDialogOpen, setRemoveUserDialogOpen] = useState(false);
  const [userToRemove, setUserToRemove] = useState<TenantUser | null>(null);
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

  // Computed values
  const totalUsers = tenants.reduce((sum, tenant) => sum + (tenant.user_count || 0), 0);
  const filteredTenants = tenants.filter(tenant => 
    tenant.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    tenant.email_domain.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const loadTenants = useCallback(async () => {
    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      const tenantsData = await tenantService.getTenants();
      setTenants(tenantsData);
    } catch (error) {
      console.error('Error loading tenants:', error);
      showNotification('Failed to load groups', 'error');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadTenants();
  }, [loadTenants]);

  // Auto-switch to groups view when groups exist
  useEffect(() => {
    if (tenants.length > 0 && viewMode === 'overview') {
      setViewMode('groups');
    }
  }, [tenants.length, viewMode]);

  const loadTenantUsers = async (tenantId: string) => {
    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      const usersData = await tenantService.getTenantUsers(tenantId);
      setTenantUsers(usersData);
    } catch (error) {
      console.error('Error loading tenant users:', error);
      showNotification('Failed to load group users', 'error');
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
      showNotification('Group created successfully', 'success');
      loadTenants();
    } catch (error) {
      console.error('Error creating tenant:', error);
      showNotification('Failed to create group', 'error');
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

  const handleDeleteGroup = async () => {
    if (!tenantToDelete) return;

    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      await tenantService.deleteTenant(tenantToDelete.id);
      
      setDeleteDialogOpen(false);
      setTenantToDelete(null);
      // Clear selected tenant if it was the one being deleted
      if (selectedTenant?.id === tenantToDelete.id) {
        setSelectedTenant(null);
        setTenantUsers([]);
      }
      showNotification('Group deleted successfully', 'success');
      loadTenants();
    } catch (error) {
      console.error('Error deleting group:', error);
      showNotification('Failed to delete group', 'error');
    } finally {
      setLoading(false);
    }
  };

  const openDeleteDialog = (tenant: Tenant) => {
    setTenantToDelete(tenant);
    setDeleteDialogOpen(true);
    handleCloseMenu();
  };

  const handleOpenMenu = (event: React.MouseEvent<HTMLElement>, tenant: Tenant) => {
    event.stopPropagation();
    setMenuAnchorEl(event.currentTarget);
    setSelectedMenuTenant(tenant);
  };

  const handleCloseMenu = () => {
    setMenuAnchorEl(null);
    setSelectedMenuTenant(null);
  };

  const handleOpenUserMenu = (event: React.MouseEvent<HTMLElement>, user: TenantUser) => {
    event.stopPropagation();
    setUserMenuAnchorEl(event.currentTarget);
    setSelectedMenuUser(user);
  };

  const handleCloseUserMenu = () => {
    setUserMenuAnchorEl(null);
    setSelectedMenuUser(null);
  };

  const openRemoveUserDialog = (user: TenantUser) => {
    setUserToRemove(user);
    setRemoveUserDialogOpen(true);
    handleCloseUserMenu();
  };

  const handleRemoveUser = async () => {
    if (!userToRemove || !selectedTenant) return;

    setLoading(true);
    try {
      const tenantService = TenantService.getInstance();
      await tenantService.removeUserFromTenant(selectedTenant.id, userToRemove.id);
      
      setRemoveUserDialogOpen(false);
      setUserToRemove(null);
      showNotification('User removed from group successfully', 'success');
      loadTenantUsers(selectedTenant.id);
    } catch (error) {
      console.error('Error removing user from group:', error);
      showNotification('Failed to remove user from group', 'error');
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

  const getRoleDescription = (role: string) => {
    switch (role) {
      case 'admin':
        return 'Full administrative access';
      case 'manager':
        return 'Can manage workflows and users';
      case 'user':
        return 'Can create and execute workflows';
      case 'viewer':
        return 'Read-only access';
      default:
        return 'Standard access';
    }
  };

  return (
    <Box>
      {/* Header Section */}
      <Box sx={{ mb: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Avatar sx={{ bgcolor: 'primary.main', mr: 2 }}>
              <GroupsIcon />
            </Avatar>
            <Box>
              <Typography variant="h5" fontWeight="600" color="text.primary">
                Group Management
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Organize users into groups for secure data isolation
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

      {/* Quick Stats Overview */}
      <Grid container spacing={2} sx={{ mb: 4 }}>
        <Grid item xs={12} sm={6}>
          <Card sx={{ bgcolor: 'primary.light', color: 'primary.contrastText' }}>
            <CardContent sx={{ textAlign: 'center', py: 2 }}>
              <GroupsIcon sx={{ fontSize: 32, mb: 1 }} />
              <Typography variant="h4" fontWeight="bold">{tenants.length}</Typography>
              <Typography variant="body2">Total Groups</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6}>
          <Card sx={{ bgcolor: 'info.light', color: 'info.contrastText' }}>
            <CardContent sx={{ textAlign: 'center', py: 2 }}>
              <PersonIcon sx={{ fontSize: 32, mb: 1 }} />
              <Typography variant="h4" fontWeight="bold">{totalUsers}</Typography>
              <Typography variant="body2">Total Users</Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Search and Filter Section */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                size="small"
                placeholder="Search groups by name or domain..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                InputProps={{
                  startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} />,
                }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <Box sx={{ display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
                <Button
                  variant={viewMode === 'overview' ? 'contained' : 'outlined'}
                  size="small"
                  onClick={() => setViewMode('overview')}
                  startIcon={<InfoIcon />}
                >
                  Overview
                </Button>
                <Button
                  variant={viewMode === 'groups' ? 'contained' : 'outlined'}
                  size="small"
                  onClick={() => setViewMode('groups')}
                  startIcon={<GroupsIcon />}
                >
                  Groups ({filteredTenants.length})
                </Button>
              </Box>
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
          <strong>Secure Group-Based Access:</strong> Each group provides isolated data access. 
          Users can belong to multiple groups and will see data from all their assigned groups.
        </Typography>
      </Alert>

      {/* Main Content Area */}
      {viewMode === 'overview' && (
        <Card>
          <CardHeader
            title={
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <GroupsIcon sx={{ mr: 1 }} />
                Getting Started with Groups
              </Box>
            }
            subheader="Follow these steps to set up your first group"
          />
          <CardContent>
            <Stepper orientation="vertical">
              <Step active>
                <StepLabel>Create Your First Group</StepLabel>
                <StepContent>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    Click the + button to create a new group. Give it a meaningful name and domain identifier.
                  </Typography>
                  <Button
                    variant="contained"
                    startIcon={<AddIcon />}
                    onClick={() => setCreateDialogOpen(true)}
                    size="small"
                  >
                    Create Group
                  </Button>
                </StepContent>
              </Step>
              <Step>
                <StepLabel>Assign Users to Groups</StepLabel>
                <StepContent>
                  <Typography variant="body2" color="text.secondary">
                    Add users by their email addresses and assign appropriate roles (Admin, Manager, User, or Viewer).
                  </Typography>
                </StepContent>
              </Step>
              <Step>
                <StepLabel>Data Isolation in Action</StepLabel>
                <StepContent>
                  <Typography variant="body2" color="text.secondary">
                    Users will automatically see only data from groups they belong to. No additional configuration needed.
                  </Typography>
                </StepContent>
              </Step>
            </Stepper>
          </CardContent>
        </Card>
      )}

      {viewMode === 'groups' && (
        <Grid container spacing={3}>
          {/* Groups List */}
          <Grid item xs={12} lg={8}>
            <Card>
              <CardHeader
                title={
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Typography variant="h6">Groups ({filteredTenants.length})</Typography>
                    <Button
                      variant="outlined"
                      size="small"
                      startIcon={<AddIcon />}
                      onClick={() => setCreateDialogOpen(true)}
                    >
                      New Group
                    </Button>
                  </Box>
                }
              />
              <Divider />
              <List>
                {filteredTenants.length === 0 ? (
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
                        Create First Group
                      </Button>
                    )}
                  </Box>
                ) : (
                  filteredTenants.map((tenant, index) => (
                    <React.Fragment key={tenant.id}>
                      <ListItem
                        sx={{
                          py: 2,
                          cursor: 'pointer',
                          '&:hover': { bgcolor: 'action.hover' },
                          borderRadius: 1,
                          mx: 1,
                        }}
                        onClick={() => {
                          setSelectedTenant(tenant);
                          loadTenantUsers(tenant.id);
                        }}
                      >
                        <ListItemAvatar>
                          <Avatar sx={{ bgcolor: tenant.status === 'active' ? 'success.main' : 'grey.400' }}>
                            <BusinessIcon />
                          </Avatar>
                        </ListItemAvatar>
                        <ListItemText
                          primary={
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="subtitle1" fontWeight="medium">
                                {tenant.name}
                              </Typography>
                              <Chip
                                icon={getStatusIcon(tenant.status)}
                                label={tenant.status}
                                size="small"
                                color={tenant.status === 'active' ? 'success' : 'default'}
                                sx={{ height: 20 }}
                              />
                              {tenant.auto_created && (
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
                            <Box>
                              <Typography variant="body2" color="text.secondary">
                                Domain: {tenant.email_domain}
                              </Typography>
                              <Typography variant="body2" color="text.secondary">
                                {tenant.user_count || 0} users • Created {new Date(tenant.created_at || Date.now()).toLocaleDateString()}
                              </Typography>
                            </Box>
                          }
                        />
                        <ListItemSecondaryAction>
                          <Box sx={{ display: 'flex', gap: 1 }}>
                            <Tooltip title="Manage Users">
                              <IconButton
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setSelectedTenant(tenant);
                                  loadTenantUsers(tenant.id);
                                }}
                              >
                                <PersonIcon />
                              </IconButton>
                            </Tooltip>
                            <Tooltip title="Group Actions">
                              <IconButton
                                onClick={(e) => handleOpenMenu(e, tenant)}
                              >
                                <MoreVertIcon />
                              </IconButton>
                            </Tooltip>
                          </Box>
                        </ListItemSecondaryAction>
                      </ListItem>
                      {index < filteredTenants.length - 1 && <Divider variant="inset" component="li" />}
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
                      {selectedTenant ? `${selectedTenant.name} Users` : 'Select a Group'}
                    </Typography>
                  </Box>
                }
                action={
                  selectedTenant && (
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
              
              {selectedTenant ? (
                <Box>
                  {/* Group Info */}
                  <Box sx={{ p: 2, bgcolor: 'grey.50' }}>
                    <Typography variant="subtitle2" color="primary" gutterBottom>
                      Group Information
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      <strong>Domain:</strong> {selectedTenant.email_domain}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      <strong>Status:</strong> {selectedTenant.status}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      <strong>Total Users:</strong> {selectedTenant.user_count || 0}
                    </Typography>
                  </Box>
                  
                  {/* Users List */}
                  <List>
                    {tenantUsers.length === 0 ? (
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
                      tenantUsers.map((user, index) => (
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
                                    <Chip
                                      icon={getRoleIcon(user.role)}
                                      label={user.role}
                                      size="small"
                                      color={getRoleColor(user.role)}
                                      sx={{ height: 20 }}
                                    />
                                    <Chip
                                      icon={getStatusIcon(user.status)}
                                      label={user.status}
                                      size="small"
                                      color={user.status === 'active' ? 'success' : 'default'}
                                      sx={{ height: 20 }}
                                    />
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
                          {index < tenantUsers.length - 1 && <Divider variant="inset" component="li" />}
                        </React.Fragment>
                      ))
                    )}
                  </List>
                </Box>
              ) : (
                <Box sx={{ textAlign: 'center', py: 6 }}>
                  <GroupIcon sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
                  <Typography variant="h6" color="text.secondary" gutterBottom>
                    Select a Group
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Click on a group from the list to view and manage its users
                  </Typography>
                </Box>
              )}
            </Card>
          </Grid>
        </Grid>
      )}

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
            onClick={() => selectedMenuTenant && openDeleteDialog(selectedMenuTenant)}
            sx={{ color: 'error.main' }}
          >
            <DeleteIcon sx={{ mr: 1 }} fontSize="small" />
            Delete Group
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
            Remove from Group
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
            Are you sure you want to remove <strong>{userToRemove?.email}</strong> from the group <strong>&ldquo;{selectedTenant?.name}&rdquo;</strong>?
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
            Are you sure you want to delete the group <strong>&ldquo;{tenantToDelete?.name}&rdquo;</strong>?
            {tenantToDelete?.user_count && tenantToDelete.user_count > 0 && (
              <>
                <br /><br />
                This group currently has <strong>{tenantToDelete.user_count} user(s)</strong> assigned to it.
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

      {/* Create Group Dialog */}
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
              <GroupsIcon />
            </Avatar>
            <Box>
              <Typography variant="h6">Create New Group</Typography>
              <Typography variant="body2" color="text.secondary">
                Set up a new group for secure data isolation
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 3, mt: 1 }}>
            Groups provide isolated data access. Users can belong to multiple groups and will see data from all their assigned groups.
          </Alert>
          
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Group Name"
                value={newTenant.name}
                onChange={(e) => setNewTenant({ ...newTenant, name: e.target.value })}
                placeholder="e.g., Team Alpha, Marketing Department"
                required
                InputProps={{
                  startAdornment: <GroupIcon sx={{ mr: 1, color: 'text.secondary' }} />
                }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Domain Identifier"
                value={newTenant.email_domain}
                onChange={(e) => setNewTenant({ ...newTenant, email_domain: e.target.value })}
                placeholder="e.g., team-alpha, marketing"
                required
                helperText="Unique identifier for this group"
                InputProps={{
                  startAdornment: <BusinessIcon sx={{ mr: 1, color: 'text.secondary' }} />
                }}
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Description (Optional)"
                value={newTenant.description}
                onChange={(e) => setNewTenant({ ...newTenant, description: e.target.value })}
                multiline
                rows={3}
                placeholder="Describe the purpose of this group..."
                helperText="Help other admins understand what this group is for"
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
            onClick={handleCreateTenant} 
            variant="contained"
            disabled={loading || !newTenant.name || !newTenant.email_domain}
            startIcon={loading ? undefined : <AddIcon />}
          >
            {loading ? 'Creating...' : 'Create Group'}
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
              <Typography variant="h6">Add User to {selectedTenant?.name}</Typography>
              <Typography variant="body2" color="text.secondary">
                Assign a user and set their role permissions
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 3, mt: 1 }}>
            Users will gain access to data and workflows within this group based on their assigned role.
          </Alert>
          
          <Grid container spacing={3}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="User Email Address"
                value={newUserAssignment.user_email}
                onChange={(e) => setNewUserAssignment({ ...newUserAssignment, user_email: e.target.value })}
                placeholder="user@company.com"
                required
                type="email"
                InputProps={{
                  startAdornment: <PersonIcon sx={{ mr: 1, color: 'text.secondary' }} />
                }}
                helperText="Enter the email address of the user you want to add"
              />
            </Grid>
            <Grid item xs={12}>
              <FormControl fullWidth required>
                <InputLabel>Role & Permissions</InputLabel>
                <Select
                  value={newUserAssignment.role}
                  label="Role & Permissions"
                  onChange={(e) => setNewUserAssignment({ ...newUserAssignment, role: e.target.value as 'admin' | 'manager' | 'user' | 'viewer' })}
                  startAdornment={getRoleIcon(newUserAssignment.role)}
                >
                  <MenuItem value="viewer">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <ViewIcon fontSize="small" />
                      <Box>
                        <Typography variant="body2" fontWeight="medium">Viewer</Typography>
                        <Typography variant="caption" color="text.secondary">Read-only access to workflows and data</Typography>
                      </Box>
                    </Box>
                  </MenuItem>
                  <MenuItem value="user">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <PersonIcon fontSize="small" />
                      <Box>
                        <Typography variant="body2" fontWeight="medium">User</Typography>
                        <Typography variant="caption" color="text.secondary">Can create and execute workflows</Typography>
                      </Box>
                    </Box>
                  </MenuItem>
                  <MenuItem value="manager">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <ManagerIcon fontSize="small" />
                      <Box>
                        <Typography variant="body2" fontWeight="medium">Manager</Typography>
                        <Typography variant="caption" color="text.secondary">Can manage workflows and other users</Typography>
                      </Box>
                    </Box>
                  </MenuItem>
                  <MenuItem value="admin">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <AdminIcon fontSize="small" />
                      <Box>
                        <Typography variant="body2" fontWeight="medium">Admin</Typography>
                        <Typography variant="caption" color="text.secondary">Full administrative access</Typography>
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
            disabled={loading || !newUserAssignment.user_email}
            startIcon={loading ? undefined : <PersonAddIcon />}
          >
            {loading ? 'Adding...' : 'Add User'}
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