import React, { useState, useEffect } from 'react';
import {
  Typography,
  Box,
  Alert,
  TextField,
  Button,
  CircularProgress,
  Switch,
  FormControlLabel,
  Grid,
  Slider,
  Tooltip as MuiTooltip,
  IconButton,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  Chip,
  SelectChangeEvent,
  Snackbar,
} from '@mui/material';
import CloudIcon from '@mui/icons-material/Cloud';
import InfoIcon from '@mui/icons-material/Info';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import CloseIcon from '@mui/icons-material/Close';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import { useTranslation } from 'react-i18next';
import { MCPService } from '../../../api/MCPService';

// Define MCP Server configuration interface
export interface MCPServerConfig {
  id: string;
  name: string;
  enabled: boolean;
  server_url: string;
  api_key: string;
  server_type: string;  // "sse" or "streamable"
  auth_type?: string;  // "api_key" or "databricks_obo"
  timeout_seconds: number;
  max_retries: number;
  rate_limit: number;
  command?: string;  // Command for stdio server type
  args?: string[];   // Arguments for stdio server type
  session_id?: string;  // Session ID for streamable server type
  additional_config?: Record<string, unknown>;  // Additional configuration parameters
}

export const DEFAULT_MCP_CONFIG: MCPServerConfig = {
  id: '',
  name: 'Default MCP Server',
  enabled: false,
  server_url: '',
  api_key: '',
  server_type: 'streamable',  // Default to Streamable HTTP server type
  auth_type: 'api_key',  // Default to API key authentication
  timeout_seconds: 30,
  max_retries: 3,
  rate_limit: 60,
  command: '',
  args: [],
  additional_config: {}
};

// Define MCP configuration to store multiple servers
export interface MCPConfiguration {
  servers: MCPServerConfig[];
  global_enabled: boolean;
}

export const DEFAULT_MCP_CONFIGURATION: MCPConfiguration = {
  servers: [],
  global_enabled: false
};

// MCPConfiguration component doesn't need props currently

interface ServerEditDialogProps {
  open: boolean;
  onClose: () => void;
  server: MCPServerConfig | null;
  onSave: (server: MCPServerConfig) => void;
  isNew?: boolean;
}

// Server edit dialog component
const ServerEditDialog: React.FC<ServerEditDialogProps> = ({
  open,
  onClose,
  server,
  onSave,
  isNew = false
}) => {
  const { t } = useTranslation();
  const [editedServer, setEditedServer] = useState<MCPServerConfig | null>(server);
  const [originalApiKey, setOriginalApiKey] = useState<string>('');
  const [testingConnection, setTestingConnection] = useState(false);
  const [connectionTestResult, setConnectionTestResult] = useState<{
    tested: boolean;
    success: boolean;
    message: string;
  }>({ tested: false, success: false, message: '' });

  useEffect(() => {
    setEditedServer(server);
    // Store the original API key for comparison
    if (server) {
      console.log('Edit Dialog Debug - Loading server:', {
        serverId: server.id,
        serverName: server.name,
        apiKey: server.api_key,
        apiKeyLength: server.api_key?.length || 0,
        isNew
      });
      setOriginalApiKey(server.api_key || '');
    }
    // Reset connection test result when dialog opens/closes or server changes
    setConnectionTestResult({ tested: false, success: false, message: '' });
  }, [server]);

  if (!editedServer) return null;

  const handleTextChange = (field: keyof MCPServerConfig) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const value = event.target.value;
    setEditedServer(prev => prev ? {
      ...prev,
      [field]: value
    } : null);
  };

  const handleSelectChange = (field: keyof MCPServerConfig) => (
    event: SelectChangeEvent<string>
  ) => {
    const value = event.target.value;
    setEditedServer(prev => prev ? {
      ...prev,
      [field]: value
    } : null);
  };

  const handleSliderChange = (field: keyof MCPServerConfig) => (
    _event: Event, 
    newValue: number | number[]
  ) => {
    setEditedServer(prev => prev ? {
      ...prev,
      [field]: newValue as number
    } : null);
  };

  const handleSave = () => {
    if (editedServer) {
      // For existing servers, only include API key if it has been changed
      const serverToSave = { ...editedServer };
      
      console.log('Save Debug:', {
        isNew,
        currentApiKey: editedServer.api_key,
        originalApiKey,
        areEqual: editedServer.api_key === originalApiKey,
        apiKeyChanged: !isNew && editedServer.api_key !== originalApiKey
      });
      
      if (!isNew && editedServer.api_key === originalApiKey) {
        // API key hasn't changed, remove it from the update payload
        console.log('Removing API key from update payload - unchanged');
        delete (serverToSave as any).api_key;
      } else if (!isNew) {
        console.log('Including API key in update payload - changed');
      }
      
      onSave(serverToSave);
      onClose();
    }
  };

  const handleTestConnection = async () => {
    if (!editedServer) return;
    
    setTestingConnection(true);
    setConnectionTestResult({ tested: false, success: false, message: '' });
    
    try {
      const mcpService = MCPService.getInstance();
      const result = await mcpService.testConnection(editedServer);
      setConnectionTestResult({
        tested: true,
        success: result.success,
        message: result.message
      });
    } catch (error) {
      setConnectionTestResult({
        tested: true,
        success: false,
        message: error instanceof Error ? error.message : 'Connection test failed'
      });
    } finally {
      setTestingConnection(false);
    }
  };



  return (
    <Dialog 
      open={open} 
      onClose={onClose}
      fullWidth
      maxWidth="md"
      PaperProps={{ 
        sx: { 
          borderRadius: 2,
          boxShadow: '0 8px 32px rgba(0,0,0,0.12)'
        } 
      }}
    >
      <DialogTitle sx={{
        borderBottom: '1px solid',
        borderColor: 'divider',
        p: 3,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <CloudIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: '1.5rem' }} />
          <Typography variant="h5">
            {isNew 
              ? t('configuration.mcp.addServer', { defaultValue: 'Add MCP Server' })
              : t('configuration.mcp.editServer', { defaultValue: 'Edit MCP Server' })}
          </Typography>
        </Box>
        <IconButton onClick={onClose}>
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent sx={{ p: 3 }}>
        <Grid container spacing={3} sx={{ mt: 0.5 }}>
          <Grid item xs={12}>
            <TextField
              label={t('configuration.mcp.serverName', { defaultValue: 'Server Name' })}
              value={editedServer.name}
              onChange={handleTextChange('name')}
              fullWidth
              required
              sx={{ 
                '& .MuiOutlinedInput-root': {
                  borderRadius: 1.5,
                }
              }}
            />
          </Grid>
          
          <Grid item xs={12} md={9}>
            <TextField
              label={t('configuration.mcp.serverUrl', { defaultValue: 'Server URL' })}
              value={editedServer.server_url}
              onChange={handleTextChange('server_url')}
              fullWidth
              required
              helperText={t('configuration.mcp.serverUrlHelp', { defaultValue: 'Full URL of the MCP server endpoint' })}
              sx={{ 
                '& .MuiOutlinedInput-root': {
                  borderRadius: 1.5,
                }
              }}
            />
          </Grid>

          <Grid item xs={12} md={3}>
            <FormControl fullWidth>
              <InputLabel id="server-type-label">Server Type</InputLabel>
              <Select
                labelId="server-type-label"
                value={editedServer.server_type || 'streamable'}
                label="Server Type"
                onChange={handleSelectChange('server_type')}
              >
                <MenuItem value="streamable">Streamable HTTP</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          
          {editedServer.server_type === 'streamable' && (
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel id="auth-type-label">Authentication Type</InputLabel>
                <Select
                  labelId="auth-type-label"
                  value={editedServer.auth_type || 'api_key'}
                  label="Authentication Type"
                  onChange={handleSelectChange('auth_type')}
                >
                  <MenuItem value="api_key">API Key</MenuItem>
                  <MenuItem value="databricks_obo">Databricks OBO</MenuItem>
                </Select>
              </FormControl>
            </Grid>
          )}
          
          {editedServer.server_type === 'streamable' && editedServer.auth_type !== 'databricks_obo' && (
            <Grid item xs={12} md={editedServer.auth_type === 'databricks_obo' ? 12 : 6}>
              <TextField
                label={t('configuration.mcp.apiKey', { defaultValue: 'API Key' })}
                value={editedServer.api_key}
                onChange={handleTextChange('api_key')}
                fullWidth
                type="password"
                required
                helperText={t('configuration.mcp.apiKeyHelp', { defaultValue: 'Authentication key for the MCP server' })}
                sx={{ 
                  '& .MuiOutlinedInput-root': {
                    borderRadius: 1.5,
                  }
                }}
              />
            </Grid>
          )}

          {editedServer.server_type === 'streamable' && (
            <Grid item xs={12}>
              <TextField
                label={t('configuration.mcp.sessionId', { defaultValue: 'Session ID (Optional)' })}
                value={editedServer.session_id || ''}
                onChange={handleTextChange('session_id')}
                fullWidth
                helperText={t('configuration.mcp.sessionIdHelp', { defaultValue: 'Optional session ID for maintaining state across requests. Leave empty for stateless connections.' })}
                sx={{ 
                  '& .MuiOutlinedInput-root': {
                    borderRadius: 1.5,
                  }
                }}
              />
            </Grid>
          )}

          <Grid item xs={12}>
            <Typography variant="subtitle2" gutterBottom>
              {t('configuration.mcp.advanced', { defaultValue: 'Advanced Settings' })}
            </Typography>
          </Grid>

          <Grid item xs={12} md={6}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
              <Typography variant="body2">
                {t('configuration.mcp.timeout', { defaultValue: 'Timeout (seconds)' })}
              </Typography>
              <MuiTooltip title={t('configuration.mcp.timeoutHelp', { defaultValue: 'Maximum time to wait for server response' })}>
                <InfoIcon fontSize="small" sx={{ ml: 1, color: 'text.secondary', fontSize: '0.9rem' }} />
              </MuiTooltip>
            </Box>
            <Slider
              value={editedServer.timeout_seconds}
              onChange={handleSliderChange('timeout_seconds')}
              min={5}
              max={120}
              step={5}
              marks={[
                { value: 5, label: '5s' },
                { value: 30, label: '30s' },
                { value: 60, label: '60s' },
                { value: 120, label: '120s' },
              ]}
              valueLabelDisplay="auto"
            />
          </Grid>
          
          <Grid item xs={12} md={6}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
              <Typography variant="body2">
                {t('configuration.mcp.maxRetries', { defaultValue: 'Max Retries' })}
              </Typography>
              <MuiTooltip title={t('configuration.mcp.maxRetriesHelp', { defaultValue: 'Number of retry attempts on failure' })}>
                <InfoIcon fontSize="small" sx={{ ml: 1, color: 'text.secondary', fontSize: '0.9rem' }} />
              </MuiTooltip>
            </Box>
            <Slider
              value={editedServer.max_retries}
              onChange={handleSliderChange('max_retries')}
              min={0}
              max={10}
              step={1}
              marks={[
                { value: 0, label: '0' },
                { value: 3, label: '3' },
                { value: 5, label: '5' },
                { value: 10, label: '10' },
              ]}
              valueLabelDisplay="auto"
            />
          </Grid>
          
          <Grid item xs={12} md={6}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
              <Typography variant="body2">
                {t('configuration.mcp.rateLimit', { defaultValue: 'Rate Limit (RPM)' })}
              </Typography>
              <MuiTooltip title={t('configuration.mcp.rateLimitHelp', { defaultValue: 'Maximum requests per minute to the MCP server' })}>
                <InfoIcon fontSize="small" sx={{ ml: 1, color: 'text.secondary', fontSize: '0.9rem' }} />
              </MuiTooltip>
            </Box>
            <Slider
              value={editedServer.rate_limit}
              onChange={handleSliderChange('rate_limit')}
              min={10}
              max={600}
              step={10}
              marks={[
                { value: 10, label: '10' },
                { value: 60, label: '60' },
                { value: 300, label: '300' },
                { value: 600, label: '600' },
              ]}
              valueLabelDisplay="auto"
            />
          </Grid>
        </Grid>
        
        {/* Connection Test Result */}
        {connectionTestResult.tested && (
          <Box sx={{ mt: 2 }}>
            <Alert 
              severity={connectionTestResult.success ? 'success' : 'error'}
              icon={connectionTestResult.success ? <CheckCircleIcon /> : <ErrorIcon />}
            >
              {connectionTestResult.message}
            </Alert>
          </Box>
        )}
      </DialogContent>
      <DialogActions sx={{ p: 3, pt: 1, display: 'flex', justifyContent: 'space-between' }}>
        <Button
          onClick={handleTestConnection}
          disabled={
            testingConnection || 
            !editedServer.server_url?.trim() || 
            (editedServer.server_type === 'streamable' && 
             editedServer.auth_type !== 'databricks_obo' && 
             !editedServer.api_key?.trim())
          }
          startIcon={testingConnection ? <CircularProgress size={16} /> : <CloudIcon />}
        >
          {testingConnection 
            ? t('configuration.mcp.testingConnection', { defaultValue: 'Testing...' })
            : t('configuration.mcp.testConnection', { defaultValue: 'Test Connection' })
          }
        </Button>
        <Box>
          <Button onClick={onClose} color="inherit" sx={{ mr: 1 }}>
            {t('common.cancel', { defaultValue: 'Cancel' })}
          </Button>
          <Button 
            onClick={handleSave} 
            variant="contained"
            color="primary"
          >
            {t('common.save', { defaultValue: 'Save' })}
          </Button>
        </Box>
      </DialogActions>
    </Dialog>
  );
};

const MCPConfiguration: React.FC = () => {
  const { t } = useTranslation();
  const [mcpConfig, setMcpConfig] = useState<MCPConfiguration>(DEFAULT_MCP_CONFIGURATION);
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [currentServer, setCurrentServer] = useState<MCPServerConfig | null>(null);
  const [isNewServer, setIsNewServer] = useState(false);
  const [notification, setNotification] = useState({
    message: '',
    open: false,
    severity: 'success' as 'success' | 'error',
  });
  const [_loading, set_Loading] = useState(false);

  const loadMcpServers = async () => {
    set_Loading(true);
    try {
      const mcpService = MCPService.getInstance();
      const response = await mcpService.getMcpServers();
      
      // Update the mcpConfig with the servers from the API
      setMcpConfig(prevConfig => ({
        ...prevConfig,
        servers: response.servers || []
      }));
      
      // Try to load global settings as well
      try {
        const globalSettings = await mcpService.getGlobalSettings();
        setMcpConfig(prevConfig => ({
          ...prevConfig,
          global_enabled: globalSettings.global_enabled
        }));
      } catch (error) {
        console.warn('Could not load global MCP settings:', error);
      }
      
    } catch (error) {
      console.error('Error loading MCP servers:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to load MCP servers',
        severity: 'error',
      });
    } finally {
      set_Loading(false);
    }
  };

  useEffect(() => {
    loadMcpServers();
  }, []);

  const handleGlobalToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const checked = event.target.checked;
    setMcpConfig(prev => ({
      ...prev,
      global_enabled: checked
    }));
    
    try {
      const mcpService = MCPService.getInstance();
      await mcpService.updateGlobalSettings({ global_enabled: checked });
    } catch (error) {
      console.error('Error updating global MCP settings:', error);
      // Revert UI state if API call fails
      setMcpConfig(prev => ({
        ...prev,
        global_enabled: !checked
      }));
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to update global MCP settings',
        severity: 'error',
      });
    }
  };

  const handleServerToggle = (serverId: string) => async (
    _event: React.ChangeEvent<HTMLInputElement>
  ) => {
    try {
      const mcpService = MCPService.getInstance();
      await mcpService.toggleMcpServerEnabled(serverId);
      
      // Reload servers to get updated state
      await loadMcpServers();
      
    } catch (error) {
      console.error(`Error toggling MCP server ${serverId}:`, error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to toggle server state',
        severity: 'error',
      });
    }
  };

  const handleEditServer = async (server: MCPServerConfig) => {
    try {
      // Fetch full server details with decrypted API key
      const mcpService = MCPService.getInstance();
      const fullServer = await mcpService.getMcpServer(server.id);
      
      console.log('Edit server - fetched full details:', {
        serverId: server.id,
        listApiKey: server.api_key,
        fullApiKey: fullServer?.api_key,
        fullApiKeyLength: fullServer?.api_key?.length || 0
      });
      
      if (fullServer) {
        setCurrentServer(fullServer);
        setIsNewServer(false);
        setEditDialogOpen(true);
      }
    } catch (error) {
      console.error('Error fetching server details for edit:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to load server details',
        severity: 'error',
      });
    }
  };

  const handleAddServer = () => {
    setCurrentServer({
      ...DEFAULT_MCP_CONFIG,
      id: new Date().getTime().toString(),
      enabled: true,
      auth_type: 'api_key'  // Ensure default auth type is set
    });
    setIsNewServer(true);
    setEditDialogOpen(true);
  };

  const handleDeleteServer = async (serverId: string) => {
    try {
      const mcpService = MCPService.getInstance();
      await mcpService.deleteMcpServer(serverId);
      
      // Reload servers after successful deletion
      await loadMcpServers();
      
      setNotification({
        open: true,
        message: 'MCP Server deleted successfully',
        severity: 'success',
      });
    } catch (error) {
      console.error(`Error deleting MCP server ${serverId}:`, error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to delete MCP server',
        severity: 'error',
      });
    }
  };

  const handleSaveServer = async (updatedServer: MCPServerConfig) => {
    try {
      const mcpService = MCPService.getInstance();
      
      if (isNewServer) {
        // Create new server
        await mcpService.createMcpServer(updatedServer);
      } else {
        // Update existing server
        await mcpService.updateMcpServer(updatedServer.id, updatedServer);
      }
      
      // Reload servers after successful save
      await loadMcpServers();
      
      setNotification({
        open: true,
        message: `MCP Server ${isNewServer ? 'created' : 'updated'} successfully`,
        severity: 'success',
      });
      
      setEditDialogOpen(false);
    } catch (error) {
      console.error('Error saving MCP server:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to save MCP server',
        severity: 'error',
      });
    }
  };


  return (
    <Box>
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'space-between', 
        mb: 3 
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <CloudIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: '1.4rem' }} />
          <Typography variant="h6">
            {t('configuration.mcp.title', { defaultValue: 'MCP Server Configuration' })}
          </Typography>
          {_loading && <CircularProgress size={20} sx={{ ml: 2 }} />}
        </Box>
        <FormControlLabel
          control={
            <Switch
              checked={mcpConfig.global_enabled}
              onChange={handleGlobalToggle}
              color="primary"
            />
          }
          label={t('configuration.mcp.globalEnable', { defaultValue: 'Enable MCP Servers' })}
        />
      </Box>
      
      <Paper 
        variant="outlined" 
        sx={{ 
          p: 3, 
          mb: 3, 
          bgcolor: 'background.paper',
          borderRadius: 2
        }}
      >
        <Box sx={{ 
          display: 'flex', 
          justifyContent: 'space-between', 
          alignItems: 'center',
          mb: 2
        }}>
          <Typography variant="subtitle1" fontWeight="medium">
            {t('configuration.mcp.servers', { defaultValue: 'MCP Servers' })}
          </Typography>
          <Button
            variant="contained"
            size="small"
            startIcon={<AddIcon />}
            onClick={handleAddServer}
          >
            {t('configuration.mcp.addServer', { defaultValue: 'Add Server' })}
          </Button>
        </Box>
        
        <Box sx={{ mt: 2 }}>
          {mcpConfig.servers.length === 0 ? (
            <Typography variant="body2" color="text.secondary" align="center" sx={{ py: 3 }}>
              {t('configuration.mcp.noServers', { defaultValue: 'No MCP servers configured yet.' })}
            </Typography>
          ) : (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              {mcpConfig.servers
                .slice() // Create a copy of the array to avoid modifying the original
                .sort((a, b) => a.name.localeCompare(b.name)) // Sort by name alphabetically
                .map((server) => (
                <Paper 
                  key={server.id} 
                  variant="outlined" 
                  sx={{ 
                    p: 2,
                    borderRadius: 1.5,
                    transition: 'all 0.2s', 
                    '&:hover': { 
                      boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
                      borderColor: 'primary.main' 
                    }
                  }}
                >
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', mb: 0.5 }}>
                        <Typography variant="subtitle1" fontWeight="medium">
                          {server.name}
                        </Typography>
                        <Chip 
                          label="STREAMABLE" 
                          size="small" 
                          color="secondary" 
                          variant="outlined"
                          sx={{ ml: 1.5, fontSize: '0.7rem', height: 20 }}
                        />
                      </Box>
                      <Typography variant="body2" color="text.secondary">
                        {server.server_url}
                      </Typography>
                    </Box>
                    
                    <Box sx={{ display: 'flex', alignItems: 'center' }}>
                      <FormControlLabel
                        control={
                          <Switch
                            size="small"
                            checked={server.enabled}
                            onChange={handleServerToggle(server.id)}
                            disabled={!mcpConfig.global_enabled}
                          />
                        }
                        label={
                          <Typography variant="body2">
                            {server.enabled ? t('common.enabled', { defaultValue: 'Enabled' }) : t('common.disabled', { defaultValue: 'Disabled' })}
                          </Typography>
                        }
                      />
                      <IconButton
                        size="small"
                        onClick={() => handleEditServer(server)}
                        sx={{ ml: 1 }}
                      >
                        <EditIcon fontSize="small" />
                      </IconButton>
                      <IconButton
                        size="small"
                        onClick={() => handleDeleteServer(server.id)}
                        sx={{ ml: 0.5 }}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  </Box>
                </Paper>
              ))}
            </Box>
          )}
        </Box>
      </Paper>
      
      
      {/* Server Edit Dialog */}
      <ServerEditDialog
        open={editDialogOpen}
        onClose={() => setEditDialogOpen(false)}
        server={currentServer}
        onSave={handleSaveServer}
        isNew={isNewServer}
      />
      
      {/* Notification */}
      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={() => setNotification(prev => ({ ...prev, open: false }))}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert severity={notification.severity} onClose={() => setNotification(prev => ({ ...prev, open: false }))}>
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default MCPConfiguration; 