import React, { useState, useEffect, useCallback, memo } from 'react';
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
  Chip,
  Card,
  CardContent,
  CardHeader,
  Grid,
  Paper,
  Divider,
  Avatar,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Checkbox,
  FormControlLabel,
  FormGroup,
} from '@mui/material';
import {
  Add as AddIcon,
  AdminPanelSettings as AdminIcon,
  Visibility as ViewIcon,
  ManageAccounts as ManagerIcon,
  VpnKey as PrivilegeIcon,
  ExpandMore as ExpandMoreIcon,
  Edit as EditIcon,
  Person as PersonIcon,
} from '@mui/icons-material';

import { 
  RoleService, 
  Role, 
  Privilege, 
  UserRole, 
} from '../../api/RoleService';

interface RoleManagementProps {
  searchTerm: string;
  loading: boolean;
  setLoading: (loading: boolean) => void;
  showNotification: (message: string, severity: 'success' | 'error' | 'warning') => void;
  userRoles: UserRole[];
  onRolesChange: () => void;
}

const RoleManagement: React.FC<RoleManagementProps> = ({
  searchTerm,
  loading,
  setLoading,
  showNotification,
  userRoles,
  onRolesChange,
}) => {
  // Role Management State
  const [roles, setRoles] = useState<Role[]>([]);
  const [privileges, setPrivileges] = useState<Privilege[]>([]);
  const [roleDialogOpen, setRoleDialogOpen] = useState(false);
  const [editingRole, setEditingRole] = useState<Role | null>(null);
  const [editRoleDialogOpen, setEditRoleDialogOpen] = useState(false);
  const [rolePrivileges, setRolePrivileges] = useState<Privilege[]>([]);
  const [selectedPrivileges, setSelectedPrivileges] = useState<string[]>([]);
  const [newRole, setNewRole] = useState({ name: '', description: '' });

  // Load functions
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

  useEffect(() => {
    const loadData = async () => {
      await Promise.all([
        loadRoles(),
        loadPrivileges(),
      ]);
    };
    loadData();
  }, [loadRoles, loadPrivileges]);

  // Role management functions
  const handleCreateRole = async () => {
    if (!newRole.name || !newRole.description) {
      showNotification('Please fill in all required fields', 'warning');
      return;
    }

    setLoading(true);
    try {
      const roleService = RoleService.getInstance();
      await roleService.createRole(newRole);
      
      setRoleDialogOpen(false);
      setNewRole({ name: '', description: '' });
      showNotification('Role created successfully', 'success');
      await loadRoles();
      onRolesChange();
    } catch (error) {
      console.error('Error creating role:', error);
      showNotification('Failed to create role', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleEditRole = async (role: Role) => {
    setEditingRole(role);
    setLoading(true);
    try {
      const roleService = RoleService.getInstance();
      const privileges = await roleService.getRolePrivileges(role.id);
      setRolePrivileges(privileges);
      setSelectedPrivileges(privileges.map(p => p.id));
      setEditRoleDialogOpen(true);
    } catch (error) {
      console.error('Error loading role privileges:', error);
      showNotification('Failed to load role privileges', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleUpdateRole = async () => {
    if (!editingRole) return;

    setLoading(true);
    try {
      const roleService = RoleService.getInstance();
      
      // Update role details
      await roleService.updateRole(editingRole.id, {
        name: editingRole.name,
        description: editingRole.description
      });

      // Update privileges
      const currentPrivilegeIds = rolePrivileges.map(p => p.id);
      const privilegesToAdd = selectedPrivileges.filter(id => !currentPrivilegeIds.includes(id));
      const privilegesToRemove = currentPrivilegeIds.filter(id => !selectedPrivileges.includes(id));

      // Add new privileges
      for (const privilegeId of privilegesToAdd) {
        await roleService.assignPrivilegeToRole(editingRole.id, privilegeId);
      }

      // Remove privileges
      for (const privilegeId of privilegesToRemove) {
        await roleService.removePrivilegeFromRole(editingRole.id, privilegeId);
      }
      
      setEditRoleDialogOpen(false);
      setEditingRole(null);
      setRolePrivileges([]);
      setSelectedPrivileges([]);
      showNotification('Role updated successfully', 'success');
      await loadRoles();
      onRolesChange();
    } catch (error) {
      console.error('Error updating role:', error);
      showNotification('Failed to update role', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handlePrivilegeToggle = (privilegeId: string) => {
    setSelectedPrivileges(prev => 
      prev.includes(privilegeId)
        ? prev.filter(id => id !== privilegeId)
        : [...prev, privilegeId]
    );
  };

  const handleAreaToggle = (areaPrivileges: Privilege[]) => {
    const areaPrivilegeIds = areaPrivileges.map(p => p.id);
    const allSelected = areaPrivilegeIds.every(id => selectedPrivileges.includes(id));
    
    if (allSelected) {
      // Deselect all in this area
      setSelectedPrivileges(prev => prev.filter(id => !areaPrivilegeIds.includes(id)));
    } else {
      // Select all in this area
      setSelectedPrivileges(prev => Array.from(new Set([...prev, ...areaPrivilegeIds])));
    }
  };

  // Filter functions
  const filteredRoles = roles.filter(role =>
    role.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    role.description.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Utility functions
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
      {/* Role Management */}
      <Grid container spacing={3}>
        <Grid item xs={12} md={8}>
          <Card>
            <CardHeader
              title={
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <Typography variant="h6">Roles ({filteredRoles.length})</Typography>
                  <Button
                    variant="contained"
                    size="small"
                    startIcon={<AddIcon />}
                    onClick={() => setRoleDialogOpen(true)}
                  >
                    New Role
                  </Button>
                </Box>
              }
            />
            <Divider />
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Role Name</TableCell>
                    <TableCell>Description</TableCell>
                    <TableCell>Users</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredRoles.map((role) => (
                    <TableRow key={role.id}>
                      <TableCell>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          {getRoleIcon(role.name)}
                          <Typography variant="body2" fontWeight="medium">
                            {role.name}
                          </Typography>
                        </Box>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2" color="text.secondary">
                          {role.description}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={userRoles.filter(ur => ur.role_id === role.id).length}
                          size="small"
                          color="primary"
                        />
                      </TableCell>
                      <TableCell>
                        <IconButton size="small" onClick={() => handleEditRole(role)}>
                          <EditIcon />
                        </IconButton>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Card>
        </Grid>
        <Grid item xs={12} md={4}>
          <Card>
            <CardHeader title="Privileges" />
            <CardContent>
              <List dense>
                {privileges.slice(0, 10).map((privilege) => (
                  <ListItem key={privilege.id}>
                    <ListItemAvatar>
                      <Avatar sx={{ bgcolor: 'secondary.main', width: 32, height: 32 }}>
                        <PrivilegeIcon fontSize="small" />
                      </Avatar>
                    </ListItemAvatar>
                    <ListItemText
                      primary={privilege.name}
                      secondary={privilege.description}
                      primaryTypographyProps={{ variant: 'body2' }}
                      secondaryTypographyProps={{ variant: 'caption' }}
                    />
                  </ListItem>
                ))}
              </List>
              {privileges.length > 10 && (
                <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                  And {privileges.length - 10} more...
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Create Role Dialog */}
      <Dialog open={roleDialogOpen} onClose={() => setRoleDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Create New Role</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Role Name"
            value={newRole.name}
            onChange={(e) => setNewRole({ ...newRole, name: e.target.value })}
            margin="normal"
            required
          />
          <TextField
            fullWidth
            label="Description"
            value={newRole.description}
            onChange={(e) => setNewRole({ ...newRole, description: e.target.value })}
            margin="normal"
            multiline
            rows={3}
            required
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setRoleDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleCreateRole} variant="contained" disabled={loading}>
            Create Role
          </Button>
        </DialogActions>
      </Dialog>

      {/* Edit Role Dialog */}
      <Dialog open={editRoleDialogOpen} onClose={() => setEditRoleDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>Edit Role</DialogTitle>
        <DialogContent>
          {editingRole && (
            <>
              <TextField
                fullWidth
                label="Role Name"
                value={editingRole.name}
                onChange={(e) => setEditingRole({ ...editingRole, name: e.target.value })}
                margin="normal"
                required
              />
              <TextField
                fullWidth
                label="Description"
                value={editingRole.description}
                onChange={(e) => setEditingRole({ ...editingRole, description: e.target.value })}
                margin="normal"
                multiline
                rows={3}
                required
              />
              
              <Typography variant="h6" sx={{ mt: 3, mb: 2 }}>Privileges</Typography>
              <Paper sx={{ maxHeight: 400, overflow: 'auto' }}>
                {(() => {
                  // Group privileges by area
                  const privilegeGroups = privileges.reduce((groups, privilege) => {
                    const area = privilege.name.split(':')[0];
                    if (!groups[area]) {
                      groups[area] = [];
                    }
                    groups[area].push(privilege);
                    return groups;
                  }, {} as Record<string, typeof privileges>);

                  // Define area order and display names
                  const areaConfig: Record<string, { name: string; description: string }> = {
                    group: { name: 'Group Management', description: 'Manage organizational groups and user assignments' },
                    tenant: { name: 'Tenant Management (Legacy)', description: 'Legacy tenant management (backward compatibility)' },
                    agent: { name: 'Agent Management', description: 'Create and manage AI agents' },
                    task: { name: 'Task Management', description: 'Create and execute individual tasks' },
                    crew: { name: 'Crew Management', description: 'Manage multi-agent workflows' },
                    execution: { name: 'Execution Management', description: 'Monitor and control executions' },
                    tool: { name: 'Tool Management', description: 'Create and configure tools for agents' },
                    model: { name: 'Model Configuration', description: 'Configure AI models and endpoints' },
                    mcp: { name: 'MCP Servers', description: 'Model Context Protocol server management' },
                    api_key: { name: 'API Key Management', description: 'Manage API keys and authentication' },
                    settings: { name: 'General Settings', description: 'System configuration and preferences' },
                    user: { name: 'User Management', description: 'Invite and manage users within groups' },
                  };

                  const orderedAreas = Object.keys(areaConfig).filter(area => privilegeGroups[area]);

                  return orderedAreas.map((area) => {
                    const config = areaConfig[area];
                    const areaPrivileges = privilegeGroups[area];
                    const selectedCount = areaPrivileges.filter(p => selectedPrivileges.includes(p.id)).length;
                    
                    return (
                      <Accordion key={area} defaultExpanded={area === 'group'}>
                        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                            <Box sx={{ flexGrow: 1 }}>
                              <Typography variant="subtitle1" fontWeight="medium">
                                {config.name}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {config.description}
                              </Typography>
                            </Box>
                            <Chip 
                              label={`${selectedCount}/${areaPrivileges.length}`}
                              size="small"
                              color={selectedCount === areaPrivileges.length ? 'success' : selectedCount > 0 ? 'primary' : 'default'}
                            />
                          </Box>
                        </AccordionSummary>
                        <AccordionDetails>
                          <Box sx={{ mb: 2 }}>
                            <Button
                              size="small"
                              variant="outlined"
                              onClick={() => handleAreaToggle(areaPrivileges)}
                              sx={{ mb: 1 }}
                            >
                              {selectedCount === areaPrivileges.length ? 'Deselect All' : 'Select All'}
                            </Button>
                          </Box>
                          <FormGroup>
                            {areaPrivileges.map((privilege) => (
                              <FormControlLabel
                                key={privilege.id}
                                control={
                                  <Checkbox
                                    checked={selectedPrivileges.includes(privilege.id)}
                                    onChange={() => handlePrivilegeToggle(privilege.id)}
                                  />
                                }
                                label={
                                  <Box>
                                    <Typography variant="body2" fontWeight="medium">
                                      {privilege.name.split(':')[1]?.replace('_', ' ') || privilege.name}
                                    </Typography>
                                    <Typography variant="caption" color="text.secondary">
                                      {privilege.description}
                                    </Typography>
                                  </Box>
                                }
                              />
                            ))}
                          </FormGroup>
                        </AccordionDetails>
                      </Accordion>
                    );
                  });
                })()}
              </Paper>
              
              <Box sx={{ mt: 2 }}>
                <Typography variant="body2" color="text.secondary">
                  Selected {selectedPrivileges.length} of {privileges.length} privileges
                </Typography>
              </Box>
            </>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditRoleDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleUpdateRole} variant="contained" disabled={loading}>
            Update Role
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default memo(RoleManagement);