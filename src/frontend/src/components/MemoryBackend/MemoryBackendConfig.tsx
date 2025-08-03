/**
 * Memory Backend Configuration Component
 * 
 * This component allows users to configure the vector database backend
 * for AI agent memory storage.
 */

import React, { useEffect, useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  FormControl,
  FormControlLabel,
  FormLabel,
  RadioGroup,
  Radio,
  TextField,
  Button,
  Alert,
  CircularProgress,
  Collapse,
  IconButton,
  Tooltip,
  Select,
  MenuItem,
  InputLabel,
  Switch,
  Grid,
  Chip,
  Divider,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import {
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  Refresh as RefreshIcon,
  Storage as StorageIcon,
  Add as AddIcon,
} from '@mui/icons-material';

import { useMemoryBackendStore } from '../../store/memoryBackend';
import { 
  MemoryBackendType, 
  getBackendDisplayName, 
  getBackendDescription,
  DatabricksMemoryConfig,
} from '../../types/memoryBackend';
import { DefaultMemoryBackendService } from '../../api/DefaultMemoryBackendService';
import { MemoryBackendService } from '../../api/MemoryBackendService';

interface MemoryBackendConfigProps {
  onConfigChange?: (isValid: boolean) => void;
  embedded?: boolean; // If true, shows a more compact version
}

export const MemoryBackendConfig: React.FC<MemoryBackendConfigProps> = ({
  onConfigChange,
  embedded = false,
}) => {
  const {
    config,
    error,
    connectionTestResult,
    isTestingConnection,
    availableIndexes,
    isLoadingIndexes,
    validationErrors,
    updateConfig,
    updateDatabricksConfig,
    testDatabricksConnection,
    loadAvailableIndexes,
    validateConfig,
    clearError,
  } = useMemoryBackendStore();

  const [expandedSections, setExpandedSections] = useState({
    authentication: false,
    advanced: false,
  });

  // Index creation dialog state
  const [createIndexDialog, setCreateIndexDialog] = useState({
    open: false,
    indexType: 'short_term' as 'short_term' | 'long_term' | 'entity' | 'document',
    catalog: '',
    schema: '',
    tableName: '',
    primaryKey: 'id',
    creating: false,
    error: '',
    success: '',
  });

  // Validate on config changes
  useEffect(() => {
    const validate = async () => {
      const isValid = await validateConfig();
      onConfigChange?.(isValid);
    };
    validate();
  }, [config, validateConfig, onConfigChange]);

  const toggleSection = (section: keyof typeof expandedSections) => {
    setExpandedSections((prev) => ({
      ...prev,
      [section]: !prev[section],
    }));
  };

  const handleCreateIndex = async () => {
    setCreateIndexDialog((prev) => ({ ...prev, creating: true, error: '', success: '' }));
    
    try {
      const result = await MemoryBackendService.createDatabricksIndex(
        config.databricks_config as DatabricksMemoryConfig,
        createIndexDialog.indexType,
        createIndexDialog.catalog,
        createIndexDialog.schema,
        createIndexDialog.tableName,
        createIndexDialog.primaryKey
      );

      if (result.success) {
        setCreateIndexDialog((prev) => ({
          ...prev,
          success: result.message,
          error: '',
          creating: false,
        }));
        
        // Reload available indexes
        await loadAvailableIndexes();
        
        // Close dialog after a short delay
        setTimeout(() => {
          setCreateIndexDialog({
            open: false,
            indexType: 'short_term',
            catalog: '',
            schema: '',
            tableName: '',
            primaryKey: 'id',
            creating: false,
            error: '',
            success: '',
          });
        }, 2000);
      } else {
        setCreateIndexDialog((prev) => ({
          ...prev,
          error: result.message,
          creating: false,
        }));
      }
    } catch (error) {
      setCreateIndexDialog((prev) => ({
        ...prev,
        error: 'Failed to create index',
        creating: false,
      }));
    }
  };

  const handleBackendTypeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const newType = event.target.value as MemoryBackendType;
    updateConfig({ 
      backend_type: newType,
      databricks_config: newType === MemoryBackendType.DATABRICKS 
        ? { 
            endpoint_name: '',
            short_term_index: '',
            embedding_dimension: 768,
            auth_type: 'default',
          }
        : undefined,
    });
  };

  const renderDatabricksConfig = () => {
    if (config.backend_type !== MemoryBackendType.DATABRICKS) return null;

    const databricksConfig = config.databricks_config || {
      endpoint_name: '',
      document_endpoint_name: '',
      short_term_index: '',
      long_term_index: '',
      entity_index: '',
      document_index: '',
      workspace_url: '',
      personal_access_token: '',
      service_principal_client_id: '',
      service_principal_client_secret: '',
      auth_type: 'default' as const,
      embedding_dimension: 768
    };

    return (
      <Box sx={{ mt: 3 }}>
        <Typography variant="h6" sx={{ mb: 2, display: 'flex', alignItems: 'center' }}>
          <StorageIcon sx={{ mr: 1 }} />
          Databricks Vector Search Configuration
        </Typography>

        <Grid container spacing={3}>
          {/* Endpoint Configuration */}
          <Grid item xs={12}>
            <Typography variant="subtitle2" sx={{ mb: 2 }}>
              Vector Search Endpoints
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Memory Endpoint (Direct Access)"
              value={databricksConfig.endpoint_name || ''}
              onChange={(e) => updateDatabricksConfig({ endpoint_name: e.target.value })}
              error={validationErrors.some((err) => err.includes('endpoint'))}
              helperText="Direct Access endpoint for dynamic memory (short-term, long-term, entity)"
              required
            />
          </Grid>
          
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Document Endpoint (Storage Optimized)"
              value={databricksConfig.document_endpoint_name || ''}
              onChange={(e) => updateDatabricksConfig({ document_endpoint_name: e.target.value })}
              helperText="Storage Optimized endpoint for static document embeddings (optional)"
            />
          </Grid>

          <Grid item xs={12} md={6}>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <TextField
                fullWidth
                label="Workspace URL"
                value={databricksConfig.workspace_url || ''}
                onChange={(e) => updateDatabricksConfig({ workspace_url: e.target.value })}
                helperText="Optional: Uses environment variable if not provided"
                placeholder="https://your-workspace.databricks.com"
              />
              <Tooltip title="Test Connection">
                <span>
                  <IconButton
                    onClick={testDatabricksConnection}
                    disabled={!databricksConfig.endpoint_name || isTestingConnection}
                    color="primary"
                  >
                    {isTestingConnection ? (
                      <CircularProgress size={24} />
                    ) : (
                      <RefreshIcon />
                    )}
                  </IconButton>
                </span>
              </Tooltip>
            </Box>
          </Grid>

          {/* Connection Test Result */}
          {connectionTestResult && (
            <Grid item xs={12}>
              <Alert 
                severity={connectionTestResult.success ? 'success' : 'error'}
                icon={connectionTestResult.success ? <CheckCircleIcon /> : <ErrorIcon />}
              >
                <Typography variant="body2">
                  {connectionTestResult.message}
                </Typography>
                {connectionTestResult.details?.indexes_found && (
                  <Typography variant="caption" sx={{ mt: 1, display: 'block' }}>
                    Found {connectionTestResult.details.indexes_found.length} indexes
                  </Typography>
                )}
              </Alert>
            </Grid>
          )}

          {/* Index Selection */}
          <Grid item xs={12}>
            <Typography variant="subtitle2" sx={{ mb: 2 }}>
              Memory Index Configuration
            </Typography>
          </Grid>

          <Grid item xs={12} md={4}>
            <FormControl fullWidth required>
              <InputLabel>Short-term Memory Index</InputLabel>
              <Select
                value={databricksConfig.short_term_index || ''}
                onChange={(e) => updateDatabricksConfig({ short_term_index: e.target.value })}
                label="Short-term Memory Index"
              >
                <MenuItem value="">
                  <em>Select an index</em>
                </MenuItem>
                {availableIndexes.map((index) => (
                  <MenuItem key={index.name} value={index.name}>
                    {index.name}
                    <Chip 
                      label={`${index.dimension}D`} 
                      size="small" 
                      sx={{ ml: 1 }} 
                    />
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>

          <Grid item xs={12} md={4}>
            <FormControl fullWidth>
              <InputLabel>Long-term Memory Index</InputLabel>
              <Select
                value={databricksConfig.long_term_index || ''}
                onChange={(e) => updateDatabricksConfig({ long_term_index: e.target.value })}
                label="Long-term Memory Index"
              >
                <MenuItem value="">
                  <em>None</em>
                </MenuItem>
                {availableIndexes.map((index) => (
                  <MenuItem key={index.name} value={index.name}>
                    {index.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>

          <Grid item xs={12} md={4}>
            <FormControl fullWidth>
              <InputLabel>Entity Memory Index</InputLabel>
              <Select
                value={databricksConfig.entity_index || ''}
                onChange={(e) => updateDatabricksConfig({ entity_index: e.target.value })}
                label="Entity Memory Index"
              >
                <MenuItem value="">
                  <em>None</em>
                </MenuItem>
                {availableIndexes.map((index) => (
                  <MenuItem key={index.name} value={index.name}>
                    {index.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          
          {/* Document Index - only show if document endpoint is configured */}
          {databricksConfig.document_endpoint_name && (
            <Grid item xs={12}>
              <FormControl fullWidth>
                <InputLabel>Document Embeddings Index</InputLabel>
                <Select
                  value={databricksConfig.document_index || ''}
                  onChange={(e) => updateDatabricksConfig({ document_index: e.target.value })}
                  label="Document Embeddings Index"
                >
                  <MenuItem value="">
                    <em>None</em>
                  </MenuItem>
                  {availableIndexes.map((index) => (
                    <MenuItem key={index.name} value={index.name}>
                      {index.name}
                    </MenuItem>
                  ))}
                </Select>
                <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
                  Used for static document embeddings on the Storage Optimized endpoint
                </Typography>
              </FormControl>
            </Grid>
          )}

          {/* Load Indexes Button and Create Index Button */}
          <Grid item xs={12}>
            <Box sx={{ display: 'flex', gap: 2 }}>
              <Button
                variant="outlined"
                onClick={loadAvailableIndexes}
                disabled={!databricksConfig.endpoint_name || isLoadingIndexes}
                startIcon={isLoadingIndexes ? <CircularProgress size={20} /> : <RefreshIcon />}
              >
                {isLoadingIndexes ? 'Loading...' : 'Load Available Indexes'}
              </Button>
              <Button
                variant="outlined"
                color="primary"
                onClick={() => setCreateIndexDialog({ ...createIndexDialog, open: true })}
                disabled={!databricksConfig.endpoint_name}
                startIcon={<AddIcon />}
              >
                Create New Index
              </Button>
            </Box>
          </Grid>

          {/* Authentication Section */}
          <Grid item xs={12}>
            <Box
              sx={{
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 1,
                p: 2,
                mt: 1,
              }}
            >
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  cursor: 'pointer',
                }}
                onClick={() => toggleSection('authentication')}
              >
                <Typography variant="subtitle1">
                  Authentication Settings
                </Typography>
                <IconButton size="small">
                  {expandedSections.authentication ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                </IconButton>
              </Box>

              <Collapse in={expandedSections.authentication}>
                <Box sx={{ mt: 2 }}>
                  <FormControl component="fieldset">
                    <FormLabel component="legend">Authentication Type</FormLabel>
                    <RadioGroup
                      value={databricksConfig.auth_type || 'default'}
                      onChange={(e) => updateDatabricksConfig({ auth_type: e.target.value as 'default' | 'pat' | 'service_principal' })}
                    >
                      <FormControlLabel 
                        value="default" 
                        control={<Radio />} 
                        label="Default (Environment Variables)" 
                      />
                      <FormControlLabel 
                        value="pat" 
                        control={<Radio />} 
                        label="Personal Access Token" 
                      />
                      <FormControlLabel 
                        value="service_principal" 
                        control={<Radio />} 
                        label="Service Principal" 
                      />
                    </RadioGroup>
                  </FormControl>

                  {databricksConfig.auth_type === 'pat' && (
                    <TextField
                      fullWidth
                      type="password"
                      label="Personal Access Token"
                      value={databricksConfig.personal_access_token || ''}
                      onChange={(e) => updateDatabricksConfig({ personal_access_token: e.target.value })}
                      sx={{ mt: 2 }}
                    />
                  )}

                  {databricksConfig.auth_type === 'service_principal' && (
                    <>
                      <TextField
                        fullWidth
                        label="Client ID"
                        value={databricksConfig.service_principal_client_id || ''}
                        onChange={(e) => updateDatabricksConfig({ service_principal_client_id: e.target.value })}
                        sx={{ mt: 2 }}
                      />
                      <TextField
                        fullWidth
                        type="password"
                        label="Client Secret"
                        value={databricksConfig.service_principal_client_secret || ''}
                        onChange={(e) => updateDatabricksConfig({ service_principal_client_secret: e.target.value })}
                        sx={{ mt: 2 }}
                      />
                    </>
                  )}
                </Box>
              </Collapse>
            </Box>
          </Grid>

          {/* Advanced Settings */}
          <Grid item xs={12}>
            <Box
              sx={{
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 1,
                p: 2,
              }}
            >
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  cursor: 'pointer',
                }}
                onClick={() => toggleSection('advanced')}
              >
                <Typography variant="subtitle1">
                  Advanced Settings
                </Typography>
                <IconButton size="small">
                  {expandedSections.advanced ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                </IconButton>
              </Box>

              <Collapse in={expandedSections.advanced}>
                <Box sx={{ mt: 2 }}>
                  <TextField
                    fullWidth
                    type="number"
                    label="Embedding Dimension"
                    value={databricksConfig.embedding_dimension || 768}
                    onChange={(e) => updateDatabricksConfig({ 
                      embedding_dimension: parseInt(e.target.value) || 768 
                    })}
                    helperText="The dimension of your embedding vectors"
                  />
                </Box>
              </Collapse>
            </Box>
          </Grid>
        </Grid>
      </Box>
    );
  };

  return (
    <Paper sx={{ p: embedded ? 2 : 3 }}>
      {!embedded && (
        <Typography variant="h5" sx={{ mb: 3 }}>
          Memory Backend Configuration
        </Typography>
      )}
      
      {embedded && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="h6" sx={{ mb: 1 }}>
            Default Memory Backend for All Agents
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Configure the default memory storage backend that will be used by all new agents.
            This setting determines where agent memories (short-term, long-term, and entity) are stored.
          </Typography>
        </Box>
      )}

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={clearError}>
          {error}
        </Alert>
      )}

      {/* Backend Type Selection */}
      <FormControl component="fieldset">
        <FormLabel component="legend">Memory Storage Backend</FormLabel>
        <RadioGroup
          value={config.backend_type}
          onChange={handleBackendTypeChange}
        >
          {Object.values(MemoryBackendType).map((type) => (
            <FormControlLabel
              key={type}
              value={type}
              control={<Radio />}
              label={
                <Box>
                  <Typography variant="body1">
                    {getBackendDisplayName(type)}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {getBackendDescription(type)}
                  </Typography>
                </Box>
              }
            />
          ))}
        </RadioGroup>
      </FormControl>

      {/* Memory Type Toggles */}
      <Box sx={{ mt: 3, mb: 2 }}>
        <Typography variant="subtitle1" sx={{ mb: 1 }}>
          Enabled Memory Types
        </Typography>
        <FormControlLabel
          control={
            <Switch
              checked={config.enable_short_term ?? true}
              onChange={(e) => updateConfig({ enable_short_term: e.target.checked })}
            />
          }
          label="Short-term Memory"
        />
        <FormControlLabel
          control={
            <Switch
              checked={config.enable_long_term ?? true}
              onChange={(e) => updateConfig({ enable_long_term: e.target.checked })}
            />
          }
          label="Long-term Memory"
        />
        <FormControlLabel
          control={
            <Switch
              checked={config.enable_entity ?? true}
              onChange={(e) => updateConfig({ enable_entity: e.target.checked })}
            />
          }
          label="Entity Memory"
        />
      </Box>

      <Divider sx={{ my: 3 }} />

      {/* Backend-specific configuration */}
      {renderDatabricksConfig()}

      {/* Validation Errors */}
      {validationErrors.length > 0 && (
        <Alert severity="error" sx={{ mt: 2 }}>
          <Typography variant="body2">Please fix the following errors:</Typography>
          <ul>
            {validationErrors.map((error, index) => (
              <li key={index}>{error}</li>
            ))}
          </ul>
        </Alert>
      )}
      
      {/* Save as Default Button for Global Configuration */}
      {embedded && (
        <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
          <Button
            variant="contained"
            color="primary"
            onClick={() => {
              DefaultMemoryBackendService.getInstance().setDefaultConfig(config);
              // Show success message
              const event = new CustomEvent('show-notification', {
                detail: {
                  message: 'Default memory backend configuration saved',
                  severity: 'success'
                }
              });
              window.dispatchEvent(event);
            }}
            disabled={validationErrors.length > 0}
          >
            Save as Default
          </Button>
        </Box>
      )}

      {/* Create Index Dialog */}
      <Dialog
        open={createIndexDialog.open}
        onClose={() => setCreateIndexDialog({ ...createIndexDialog, open: false })}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Create Databricks Vector Search Index</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            {createIndexDialog.error && (
              <Alert severity="error" sx={{ mb: 2 }}>
                {createIndexDialog.error}
              </Alert>
            )}
            {createIndexDialog.success && (
              <Alert severity="success" sx={{ mb: 2 }}>
                {createIndexDialog.success}
              </Alert>
            )}
            
            <Alert 
              severity={createIndexDialog.indexType === 'document' ? 'warning' : 'info'} 
              sx={{ mb: 2 }}
            >
              <Typography variant="body2">
                {createIndexDialog.indexType === 'document' ? (
                  <>
                    <strong>Note:</strong> Document indexes require a Storage Optimized endpoint.
                    <br />
                    Make sure you have configured a document endpoint and that a Delta table exists
                    for this index. Document indexes use Delta Sync for better read performance.
                  </>
                ) : (
                  <>
                    <strong>Important:</strong> This will create a Direct Access index on your memory endpoint.
                    <br />
                    Direct Access indexes are perfect for dynamic AI agent memory (short-term, long-term, entity).
                  </>
                )}
              </Typography>
            </Alert>
            
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <FormControl fullWidth>
                  <InputLabel>Index Type</InputLabel>
                  <Select
                    value={createIndexDialog.indexType}
                    onChange={(e) => setCreateIndexDialog({
                      ...createIndexDialog,
                      indexType: e.target.value as 'short_term' | 'long_term' | 'entity' | 'document'
                    })}
                    label="Index Type"
                  >
                    <MenuItem value="short_term">Short-term Memory</MenuItem>
                    <MenuItem value="long_term">Long-term Memory</MenuItem>
                    <MenuItem value="entity">Entity Memory</MenuItem>
                    <MenuItem value="document">Document Embeddings</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Catalog"
                  value={createIndexDialog.catalog}
                  onChange={(e) => setCreateIndexDialog({
                    ...createIndexDialog,
                    catalog: e.target.value
                  })}
                  helperText="The Unity Catalog name (e.g., 'ml')"
                  required
                />
              </Grid>
              
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Schema"
                  value={createIndexDialog.schema}
                  onChange={(e) => setCreateIndexDialog({
                    ...createIndexDialog,
                    schema: e.target.value
                  })}
                  helperText="The schema name (e.g., 'agents')"
                  required
                />
              </Grid>
              
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Table Name"
                  value={createIndexDialog.tableName}
                  onChange={(e) => setCreateIndexDialog({
                    ...createIndexDialog,
                    tableName: e.target.value
                  })}
                  helperText={`Table name for ${createIndexDialog.indexType} memory`}
                  required
                />
              </Grid>
              
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Primary Key Column"
                  value={createIndexDialog.primaryKey}
                  onChange={(e) => setCreateIndexDialog({
                    ...createIndexDialog,
                    primaryKey: e.target.value
                  })}
                  helperText="The primary key column name (default: 'id')"
                />
              </Grid>
              
              <Grid item xs={12}>
                <Typography variant="caption" color="text.secondary">
                  This will create a Direct Access Vector Search index. The underlying Delta table will be created automatically.
                  <br />
                  Index name: {createIndexDialog.catalog}.{createIndexDialog.schema}.{createIndexDialog.tableName}
                </Typography>
              </Grid>
            </Grid>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button 
            onClick={() => setCreateIndexDialog({ ...createIndexDialog, open: false })}
            disabled={createIndexDialog.creating}
          >
            Cancel
          </Button>
          <Button
            onClick={handleCreateIndex}
            variant="contained"
            color="primary"
            disabled={
              createIndexDialog.creating ||
              !createIndexDialog.catalog ||
              !createIndexDialog.schema ||
              !createIndexDialog.tableName
            }
            startIcon={createIndexDialog.creating ? <CircularProgress size={20} /> : null}
          >
            {createIndexDialog.creating ? 'Creating...' : 'Create Index'}
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
};