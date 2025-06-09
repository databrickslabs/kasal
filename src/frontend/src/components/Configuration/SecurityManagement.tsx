import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Button,
  TextField,
  Chip,
  Alert,
  Snackbar,
  Card,
  CardContent,
  CardHeader,
  Grid,
  Tooltip,
  Paper,
  Avatar,
  LinearProgress,
  Tabs,
  Tab,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import {
  Groups as GroupsIcon,
  Search as SearchIcon,
  Security as SecurityIcon,
  AdminPanelSettings as AdminIcon,
  VpnKey as PrivilegeIcon,
  Sync as SyncIcon,
  CloudSync as CloudSyncIcon,
  ExpandMore as ExpandMoreIcon,
} from '@mui/icons-material';

import { GroupService } from '../../api/GroupService';
import type { Group, GroupUser } from '../../api/GroupService';
import { 
  RoleService, 
  Role, 
  Privilege, 
  User, 
  UserRole, 
  DatabricksAdminSyncResult, 
  AdminEmailsResult, 
} from '../../api/RoleService';
import RoleManagement from './RoleManagement';
import GroupManagement from './GroupManagement';
import UserManagement from './UserManagement';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`security-tabpanel-${index}`}
      aria-labelledby={`security-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ py: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

const SecurityManagement: React.FC = () => {
  // Tab state
  const [activeTab, setActiveTab] = useState(0);
  
  // Common state
  const [loading, setLoading] = useState(false);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error' | 'warning',
  });
  const [searchTerm, setSearchTerm] = useState('');


  // Shared State for tabs
  const [roles, setRoles] = useState<Role[]>([]);
  const [_privileges, setPrivileges] = useState<Privilege[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [userRoles, setUserRoles] = useState<UserRole[]>([]);
  const [userGroups, setUserGroups] = useState<GroupUser[]>([]);
  const [groups, setGroups] = useState<Group[]>([]); // Still needed for User Assignments tab
  // Databricks Sync State
  const [_syncDialogOpen, _setSyncDialogOpen] = useState(false);
  const [adminEmails, setAdminEmails] = useState<AdminEmailsResult | null>(null);
  const [syncResult, setSyncResult] = useState<DatabricksAdminSyncResult | null>(null);
  

  const showNotification = useCallback((message: string, severity: 'success' | 'error' | 'warning') => {
    setNotification({ open: true, message, severity });
  }, []);

  // Load functions for shared data
  const loadGroups = useCallback(async () => {
    try {
      const groupService = GroupService.getInstance();
      const groupsData = await groupService.getGroups();
      setGroups(groupsData);
      return groupsData;
    } catch (error) {
      console.error('Error loading groups:', error);
      showNotification('Failed to load groups', 'error');
      return [];
    }
  }, [showNotification]);

  const loadRoles = useCallback(async () => {
    try {
      const roleService = RoleService.getInstance();
      const rolesData = await roleService.getRoles();
      setRoles(rolesData);
    } catch (error) {
      console.error('Error loading roles:', error);
      showNotification('Failed to load roles', 'error');
    }
  }, [showNotification]);

  const loadPrivileges = useCallback(async () => {
    try {
      const roleService = RoleService.getInstance();
      const privilegesData = await roleService.getPrivileges();
      setPrivileges(privilegesData);
    } catch (error) {
      console.error('Error loading privileges:', error);
      showNotification('Failed to load privileges', 'error');
    }
  }, [showNotification]);

  const loadUsers = useCallback(async () => {
    try {
      const roleService = RoleService.getInstance();
      const usersData = await roleService.getUsers();
      setUsers(usersData);
    } catch (error) {
      console.error('Error loading users:', error);
      showNotification('Failed to load users', 'error');
    }
  }, [showNotification]);

  const loadUserRoles = useCallback(async () => {
    try {
      const roleService = RoleService.getInstance();
      const userRolesData = await roleService.getUserRoleAssignments();
      setUserRoles(userRolesData);
    } catch (error) {
      console.error('Error loading user roles:', error);
      showNotification('Failed to load user role assignments', 'error');
    }
  }, [showNotification]);

  const loadUserGroups = useCallback(async (groupsToLoad?: Group[]) => {
    try {
      // Load all group users to show group memberships
      const groupService = GroupService.getInstance();
      const allUserGroups: GroupUser[] = [];
      
      if (groupsToLoad && groupsToLoad.length > 0) {
        for (const group of groupsToLoad) {
          const groupUsers = await groupService.getGroupUsers(group.id);
          allUserGroups.push(...groupUsers);
        }
        setUserGroups(allUserGroups);
      }
    } catch (error) {
      console.error('Error loading user groups:', error);
      showNotification('Failed to load user group memberships', 'error');
    }
  }, [showNotification]);

  const loadDatabricksAdminEmails = useCallback(async () => {
    try {
      const roleService = RoleService.getInstance();
      const emailsData = await roleService.getDatabricksAdminEmails();
      setAdminEmails(emailsData);
    } catch (error) {
      console.error('Error loading admin emails:', error);
      showNotification('Failed to load admin emails', 'error');
    }
  }, [showNotification]);

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      try {
        const loadedGroups = await loadGroups();
        await Promise.all([
          loadRoles(),
          loadPrivileges(),
          loadUsers(),
          loadUserRoles(),
          loadDatabricksAdminEmails(),
        ]);
        // Load user groups after groups are loaded
        if (loadedGroups && loadedGroups.length > 0) {
          await loadUserGroups(loadedGroups);
        }
      } finally {
        setLoading(false);
      }
    };
    loadData();
  }, [loadGroups, loadRoles, loadPrivileges, loadUsers, loadUserRoles, loadDatabricksAdminEmails, loadUserGroups]);

  // Databricks sync functions
  const handleSyncDatabricksRoles = async () => {
    setLoading(true);
    try {
      const roleService = RoleService.getInstance();
      const result = await roleService.syncDatabricksAdminRoles();
      setSyncResult(result);
      
      if (result.success) {
        showNotification('Databricks admin roles synchronized successfully', 'success');
        await loadUsers();
        await loadUserRoles();
      } else {
        showNotification(`Sync failed: ${result.errors.join(', ')}`, 'error');
      }
    } catch (error) {
      console.error('Error syncing Databricks roles:', error);
      showNotification('Failed to sync Databricks admin roles', 'error');
    } finally {
      setLoading(false);
    }
  };





  return (
    <Box>
      {/* Header Section */}
      <Box sx={{ mb: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Avatar sx={{ bgcolor: 'primary.main', mr: 2 }}>
              <SecurityIcon />
            </Avatar>
            <Box>
              <Typography variant="h5" fontWeight="600" color="text.primary">
                Security Management
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Manage roles, permissions, and group-based access control
              </Typography>
            </Box>
          </Box>
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Tooltip title="Sync Databricks Admin Roles">
              <Button
                variant="outlined"
                startIcon={<CloudSyncIcon />}
                onClick={handleSyncDatabricksRoles}
                disabled={loading}
              >
                Sync Databricks
              </Button>
            </Tooltip>
          </Box>
        </Box>
        
        {loading && <LinearProgress sx={{ mb: 2 }} />}
      </Box>

      {/* Navigation Tabs */}
      <Paper sx={{ mb: 3 }}>
        <Tabs
          value={activeTab}
          onChange={(_, newValue) => setActiveTab(newValue)}
          indicatorColor="primary"
          textColor="primary"
          variant="fullWidth"
        >
          <Tab
            icon={<GroupsIcon />}
            label="Group Management"
            iconPosition="start"
            sx={{ minHeight: 64 }}
          />
          <Tab
            icon={<AdminIcon />}
            label="Role Management"
            iconPosition="start"
            sx={{ minHeight: 64 }}
          />
          <Tab
            icon={<PrivilegeIcon />}
            label="User Management"
            iconPosition="start"
            sx={{ minHeight: 64 }}
          />
          <Tab
            icon={<CloudSyncIcon />}
            label="Databricks Sync"
            iconPosition="start"
            sx={{ minHeight: 64 }}
          />
        </Tabs>
      </Paper>

      {/* Search Bar */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <TextField
            fullWidth
            size="small"
            placeholder="Search users, roles, groups..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{
              startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} />,
            }}
          />
        </CardContent>
      </Card>

      {/* Tab Panels */}
      <TabPanel value={activeTab} index={0}>
        {/* Group Management */}
        <GroupManagement />
      </TabPanel>

      <TabPanel value={activeTab} index={1}>
        {/* Role Management */}
        <RoleManagement
          searchTerm={searchTerm}
          loading={loading}
          setLoading={setLoading}
          showNotification={showNotification}
          userRoles={userRoles}
          onRolesChange={loadUserRoles}
        />
      </TabPanel>

      <TabPanel value={activeTab} index={2}>
        {/* User Management */}
        <UserManagement
          searchTerm={searchTerm}
          loading={loading}
          setLoading={setLoading}
          showNotification={showNotification}
          users={users}
          userRoles={userRoles}
          userGroups={userGroups}
          groups={groups}
          roles={roles}
          onUsersChange={async () => {
            await loadUsers();
            await loadUserRoles();
            await loadUserGroups(groups);
          }}
        />
      </TabPanel>

      <TabPanel value={activeTab} index={3}>
        {/* Databricks Sync */}
        <Grid container spacing={3}>
          <Grid item xs={12} md={8}>
            <Card>
              <CardHeader
                title={
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <CloudSyncIcon />
                    <Typography variant="h6">Databricks Admin Sync</Typography>
                  </Box>
                }
              />
              <CardContent>
                <Alert severity="info" sx={{ mb: 3 }}>
                  Automatically sync users with &quot;CAN_MANAGE&quot; permission from your Databricks app to the admin role.
                </Alert>
                
                {adminEmails && (
                  <Accordion sx={{ mb: 2 }}>
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Typography variant="subtitle2">
                        Admin Emails ({adminEmails.admin_emails.length}) - Source: {adminEmails.source}
                      </Typography>
                    </AccordionSummary>
                    <AccordionDetails>
                      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                        {adminEmails.admin_emails.map((email) => (
                          <Chip key={email} label={email} size="small" />
                        ))}
                      </Box>
                    </AccordionDetails>
                  </Accordion>
                )}

                {syncResult && (
                  <Accordion sx={{ mb: 2 }}>
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Typography variant="subtitle2">
                        Last Sync Result - {syncResult.success ? 'Success' : 'Failed'}
                      </Typography>
                    </AccordionSummary>
                    <AccordionDetails>
                      <Box>
                        <Typography variant="body2" gutterBottom>
                          Processed {syncResult.processed_users.length} users
                        </Typography>
                        {syncResult.processed_users.map((user, index) => (
                          <Box key={index} sx={{ mb: 1 }}>
                            <Typography variant="caption">
                              {user.email}: {user.user_created ? 'Created' : 'Existing'} user, 
                              {user.role_assigned ? ' assigned admin role' : user.already_admin ? ' already admin' : ' no role assigned'}
                              {user.error && ` (Error: ${user.error})`}
                            </Typography>
                          </Box>
                        ))}
                        {syncResult.errors.length > 0 && (
                          <Alert severity="error" sx={{ mt: 2 }}>
                            Errors: {syncResult.errors.join(', ')}
                          </Alert>
                        )}
                      </Box>
                    </AccordionDetails>
                  </Accordion>
                )}

                <Box sx={{ display: 'flex', gap: 2 }}>
                  <Button
                    variant="contained"
                    startIcon={<SyncIcon />}
                    onClick={handleSyncDatabricksRoles}
                    disabled={loading}
                  >
                    Sync Admin Roles
                  </Button>
                  <Button
                    variant="outlined"
                    onClick={loadDatabricksAdminEmails}
                    disabled={loading}
                  >
                    Refresh Admin Emails
                  </Button>
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} md={4}>
            <Card>
              <CardHeader title="Databricks Configuration" />
              <CardContent>
                {adminEmails && (
                  <Box>
                    <Typography variant="body2" gutterBottom>
                      <strong>Environment:</strong> {adminEmails.is_local_dev ? 'Development' : 'Production'}
                    </Typography>
                    <Typography variant="body2" gutterBottom>
                      <strong>App Name:</strong> {adminEmails.databricks_config.app_name || 'Not configured'}
                    </Typography>
                    <Typography variant="body2" gutterBottom>
                      <strong>Host:</strong> {adminEmails.databricks_config.host_configured ? '✓ Configured' : '✗ Missing'}
                    </Typography>
                    <Typography variant="body2" gutterBottom>
                      <strong>Token:</strong> {adminEmails.databricks_config.token_configured ? '✓ Configured' : '✗ Missing'}
                    </Typography>
                  </Box>
                )}
              </CardContent>
            </Card>
          </Grid>
        </Grid>
      </TabPanel>

      {/* Dialogs */}
      



      {/* Notification Snackbar */}
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

export default SecurityManagement;