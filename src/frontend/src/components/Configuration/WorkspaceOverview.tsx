import { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Alert,
  Chip,
  Card,
  CardContent,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Button,
} from '@mui/material';
import CloudIcon from '@mui/icons-material/Cloud';
import MemoryIcon from '@mui/icons-material/Memory';
import StorageIcon from '@mui/icons-material/Storage';
import GroupIcon from '@mui/icons-material/Group';
import AdminPanelSettingsIcon from '@mui/icons-material/AdminPanelSettings';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import WarningIcon from '@mui/icons-material/Warning';
import BuildIcon from '@mui/icons-material/Build';
import { GroupService } from '../../api/GroupService';
import { DatabricksService, DatabricksConfig } from '../../api/DatabricksService';
import { MemoryBackendService } from '../../api/MemoryBackendService';
import { MemoryBackendConfig } from '../../types/memoryBackend';
import { usePermissionStore } from '../../store/permissions';
import { ToolService, Tool as ServiceTool } from '../../api/ToolService';
import { MCPService } from '../../api/MCPService';
import type { MCPServerConfig } from '../Configuration/MCP/MCPConfiguration';

interface WorkspaceInfo {
  name: string;
  description?: string;
  is_default?: boolean;
  member_count?: number;
  created_at?: string;
}

interface WorkspaceOverviewProps {
  onConfigureSection?: (section: string) => void;
}

function WorkspaceOverview({ onConfigureSection }: WorkspaceOverviewProps): JSX.Element {
  const [loading, setLoading] = useState(true);
  const [workspaceInfo, setWorkspaceInfo] = useState<WorkspaceInfo | null>(null);
  const [databricksConfig, setDatabricksConfig] = useState<DatabricksConfig | null>(null);
  const [memoryConfig, setMemoryConfig] = useState<MemoryBackendConfig | null>(null);
  const [tools, setTools] = useState<ServiceTool[]>([]);
  const [mcpServers, setMcpServers] = useState<MCPServerConfig[]>([]);
  const [error, setError] = useState<string | null>(null);

  const selectedGroupId = localStorage.getItem('selectedGroupId');
  const isPersonalWorkspace = selectedGroupId?.startsWith('user_');
  const userRole = usePermissionStore(state => state.userRole);

  useEffect(() => {
    const loadInfo = async () => {
      await loadWorkspaceInfo();
    };
    loadInfo();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGroupId]);

  const loadWorkspaceInfo = async () => {
    try {
      setLoading(true);
      setError(null);

      // Load workspace/group information
      if (selectedGroupId && !isPersonalWorkspace) {
        const groups = await GroupService.getInstance().getMyGroups();
        const currentGroup = groups.find(g => g.id === selectedGroupId);
        if (currentGroup) {
          setWorkspaceInfo({
            name: currentGroup.name,
            description: currentGroup.description,
            member_count: currentGroup.user_count,
            created_at: currentGroup.created_at
          });
        }
      } else {
        // Personal workspace info
        setWorkspaceInfo({
          name: 'Personal Workspace',
          description: 'Your personal workspace',
          is_default: true,
          member_count: 1,
        });
      }

      // Load Databricks configuration
      try {
        const dbConfig = await DatabricksService.getInstance().getDatabricksConfig();
        setDatabricksConfig(dbConfig);
      } catch (err) {
        console.log('No Databricks configuration found');
      }

      // Load Memory Backend configuration
      try {
        const memConfig = await MemoryBackendService.getConfig();
        setMemoryConfig(memConfig);
      } catch (err) {
        console.log('No memory backend configuration found');
      }

      // Load Tools enabled for this workspace (effective list from backend)
      try {
        const enabled = await ToolService.listEnabledTools();
        setTools(enabled);
      } catch (err) {
        console.log('No enabled tools found');
      }

      // Load MCP servers for this workspace (effective list)
      try {
        const mcpResp = await MCPService.getInstance().getMcpServers();
        setMcpServers(mcpResp.servers || []);
      } catch (err) {
        console.log('No MCP servers found');
      }

    } catch (err) {
      console.error('Error loading workspace info:', err);
      setError('Failed to load workspace information');
    } finally {
      setLoading(false);
    }
  };

  const getStatusIcon = (configured: boolean) => {
    if (configured) {
      return <CheckCircleIcon color="success" fontSize="small" />;
    }
    return <WarningIcon color="warning" fontSize="small" />;
  };

  const getStatusLabel = (configured: boolean) => {
    return configured ? (
      <Chip label="Configured" size="small" color="success" variant="outlined" />
    ) : (
      <Chip label="Not Configured" size="small" color="warning" variant="outlined" />
    );
  };

  // Tools returned here are already the enabled, effective set for this workspace
  const enabledTools = tools;

  // Compute effective MCP servers (prefer workspace overrides over base)
  const effectiveMcpServers = (() => {
    const byName = new Map<string, MCPServerConfig>();
    for (const s of mcpServers) {
      const existing = byName.get(s.name);
      if (!existing) {
        byName.set(s.name, s);
      } else if (!existing.group_id && s.group_id) {
        // Prefer workspace-specific override when present
        byName.set(s.name, s);
      }
    }
    return Array.from(byName.values());
  })();

  const enabledMcpServers = effectiveMcpServers.filter(s => s.enabled !== false);

  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <Typography>Loading workspace information...</Typography>

      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: 3 }}>
        <Alert severity="error">{error}</Alert>
      </Box>
    );
  }

  return (
    <Box>
      {/* Workspace Header */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h5" gutterBottom>
          Workspace Configuration Overview
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Manage your workspace settings and configurations
        </Typography>
      </Box>

      {/* Workspace Info Card */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
            <GroupIcon sx={{ mr: 1, color: 'primary.main' }} />
            <Typography variant="h6">
              {workspaceInfo?.name || 'Workspace'}
            </Typography>
          </Box>

          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Workspace Type
              </Typography>
              <Typography variant="body1">
                {isPersonalWorkspace ? 'Personal' : 'Team'}
              </Typography>
            </Grid>

            <Grid item xs={12} md={6}>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Your Role
              </Typography>
              <Chip
                label={userRole || 'Unknown'}
                size="small"
                color={userRole === 'admin' ? 'primary' : 'default'}
                icon={userRole === 'admin' ? <AdminPanelSettingsIcon /> : undefined}
              />
            </Grid>

            {!isPersonalWorkspace && (
              <>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Members
                  </Typography>
                  <Typography variant="body1">
                    {workspaceInfo?.member_count || 0} members
                  </Typography>
                </Grid>

                <Grid item xs={12} md={6}>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Created
                  </Typography>
                  <Typography variant="body1">
                    {workspaceInfo?.created_at
                      ? new Date(workspaceInfo.created_at).toLocaleDateString()
                      : 'N/A'}
                  </Typography>
                </Grid>
              </>
            )}
          </Grid>
        </CardContent>
      </Card>

      {/* Configuration Status */}
      <Typography variant="h6" sx={{ mb: 2 }}>
        Configuration Status
      </Typography>

      <Grid container spacing={2}>
        {/* Databricks Configuration */}
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <CloudIcon sx={{ mr: 1, color: 'primary.main' }} />
              <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                Databricks Integration
              </Typography>
              {getStatusLabel(!!databricksConfig)}
            </Box>

            {databricksConfig ? (
              <List dense>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(!!databricksConfig.warehouse_id)}
                  </ListItemIcon>
                  <ListItemText
                    primary="SQL Warehouse"
                    secondary={databricksConfig.warehouse_id || 'Not configured'}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(!!databricksConfig.catalog)}
                  </ListItemIcon>
                  <ListItemText
                    primary="Catalog"
                    secondary={databricksConfig.catalog || 'Not configured'}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(databricksConfig.enabled)}
                  </ListItemIcon>
                  <ListItemText
                    primary="Status"
                    secondary={databricksConfig.enabled ? 'Enabled' : 'Disabled'}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(!!databricksConfig.mlflow_enabled)}
                  </ListItemIcon>
                  <ListItemText
                    primary="MLflow Tracing"
                    secondary={databricksConfig.mlflow_enabled ? 'Enabled' : 'Disabled'}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(!!databricksConfig.evaluation_enabled)}
                  </ListItemIcon>
                  <ListItemText
                    primary="MLflow Evaluation"
                    secondary={databricksConfig.evaluation_enabled ? 'Enabled' : 'Disabled'}
                  />
                </ListItem>


              </List>
            ) : (
              <Typography variant="body2" color="text.secondary">
                Databricks is not configured for this workspace
              </Typography>
            )}
          </Paper>
        </Grid>

        {/* Memory Backend Configuration */}
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <MemoryIcon sx={{ mr: 1, color: 'primary.main' }} />
              <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                Memory Backend
              </Typography>
              {getStatusLabel(!!memoryConfig)}
            </Box>

            {memoryConfig ? (
              <List dense>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(true)}
                  </ListItemIcon>
                  <ListItemText
                    primary="Backend Type"
                    secondary={memoryConfig.backend_type || 'Default'}
                  />

                </ListItem>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(memoryConfig.is_default || false)}
                  </ListItemIcon>
                  <ListItemText
                    primary="Default Configuration"
                    secondary={memoryConfig.is_default ? 'Yes' : 'No'}
                  />
                </ListItem>
                {memoryConfig.backend_type === 'databricks' && (
                  <ListItem>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      {getStatusIcon(!!memoryConfig.databricks_config?.endpoint_name)}
                    </ListItemIcon>
                    <ListItemText
                      primary="Vector Endpoint"
                      secondary={memoryConfig.databricks_config?.endpoint_name || 'Not configured'}
                    />
                  </ListItem>
                )}
              </List>
            ) : (
              <Typography variant="body2" color="text.secondary">
                Memory backend is not configured for this workspace
              </Typography>
            )}
          </Paper>
        </Grid>

        {/* Volume Configuration (Future) */}
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <StorageIcon sx={{ mr: 1, color: 'primary.main' }} />
              <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                Volume Storage
              </Typography>
              {getStatusLabel(databricksConfig?.volume_enabled || false)}
            </Box>

            {databricksConfig?.volume_enabled ? (
              <List dense>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(!!databricksConfig.volume_path)}
                  </ListItemIcon>
                  <ListItemText
                    primary="Volume Path"
                    secondary={databricksConfig.volume_path || 'Not configured'}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(true)}
                  </ListItemIcon>
                  <ListItemText
                    primary="File Format"
                    secondary={databricksConfig.volume_file_format || 'json'}
                  />
                </ListItem>
              </List>

            ) : (
              <Typography variant="body2" color="text.secondary">
                Volume storage is not configured for this workspace
              </Typography>
            )}
          </Paper>
        </Grid>

        {/* Knowledge Volume (Future) */}
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <StorageIcon sx={{ mr: 1, color: 'primary.main' }} />
              <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                Knowledge Base
              </Typography>
              {getStatusLabel(databricksConfig?.knowledge_volume_enabled || false)}
            </Box>

            {databricksConfig?.knowledge_volume_enabled ? (
              <List dense>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(!!databricksConfig.knowledge_volume_path)}
                  </ListItemIcon>
                  <ListItemText
                    primary="Knowledge Path"
                    secondary={databricksConfig.knowledge_volume_path || 'Not configured'}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon sx={{ minWidth: 36 }}>
                    {getStatusIcon(true)}
                  </ListItemIcon>
                  <ListItemText
                    primary="Chunk Size"
                    secondary={`${databricksConfig.knowledge_chunk_size || 1000} tokens`}
                  />
                </ListItem>
              </List>
            ) : (
              <Typography variant="body2" color="text.secondary">
                Knowledge base is not configured for this workspace
              </Typography>
            )}
          </Paper>
        </Grid>
        {/* Tools Enabled in Workspace */}
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <BuildIcon sx={{ mr: 1, color: 'primary.main' }} />
              <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                Tools
              </Typography>
              <Chip label={`${enabledTools.length} enabled`} size="small" color={enabledTools.length > 0 ? 'success' : 'warning'} variant="outlined" />
            </Box>

            {enabledTools.length > 0 ? (
              <List dense>
                {enabledTools.map(tool => (
                  <ListItem key={`${tool.title}-${tool.id}`}>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      {getStatusIcon(true)}
                    </ListItemIcon>
                    <ListItemText
                      primary={tool.title}
                      secondary={tool.group_id ? 'Workspace override' : 'Base'}
                    />
                    {tool.group_id && <Chip label="Workspace" size="small" color="primary" variant="outlined" />}
                  </ListItem>
                ))}
              </List>
            ) : (
              <Typography variant="body2" color="text.secondary">
                No tools enabled for this workspace
              </Typography>
            )}
          </Paper>
        </Grid>

        {/* MCP Servers Enabled in Workspace */}
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <BuildIcon sx={{ mr: 1, color: 'primary.main' }} />
              <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                MCP Servers
              </Typography>
              <Chip label={`${enabledMcpServers.length} enabled`} size="small" color={enabledMcpServers.length > 0 ? 'success' : 'warning'} variant="outlined" />
              {onConfigureSection && (
                <Button size="small" sx={{ ml: 1 }} onClick={() => onConfigureSection('mcp')}>
                  Configure
                </Button>
              )}
            </Box>

            {enabledMcpServers.length > 0 ? (
              <List dense>
                {enabledMcpServers.map(server => (
                  <ListItem key={`${server.name}-${server.id}`}>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      {getStatusIcon(true)}
                    </ListItemIcon>
                    <ListItemText
                      primary={server.name}
                      secondary={server.group_id ? 'Workspace override' : 'Base'}
                    />
                    {server.group_id && <Chip label="Workspace" size="small" color="primary" variant="outlined" />}
                  </ListItem>
                ))}
              </List>
            ) : (
              <Typography variant="body2" color="text.secondary">
                No MCP servers enabled for this workspace
              </Typography>
            )}
          </Paper>
        </Grid>

      </Grid>

      {/* Admin Notice */}
      {userRole === 'admin' && (
        <Alert severity="info" sx={{ mt: 3 }}>
          <Typography variant="body2">
            As a workspace admin, you can configure all workspace-specific settings including
            Databricks integration, memory backend, and storage volumes. Use the navigation
            menu to access each configuration section.
          </Typography>
        </Alert>
      )}
    </Box>
  );
}

export default WorkspaceOverview;