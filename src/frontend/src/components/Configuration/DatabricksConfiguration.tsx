import React, { useState, useEffect } from 'react';
import {
  Typography,
  Box,
  Alert,
  TextField,
  Button,
  Snackbar,
  CircularProgress,
  Stack,
  FormControlLabel,
  Switch,
  Divider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Paper,
  Tooltip,
} from '@mui/material';
import SaveIcon from '@mui/icons-material/Save';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import StorageIcon from '@mui/icons-material/Storage';
import { useTranslation } from 'react-i18next';
import { DatabricksService, DatabricksConfig, DatabricksTokenStatus, DatabricksConnectionStatus } from '../../api/DatabricksService';
import { MemoryBackendService } from '../../api/MemoryBackendService';
import { MemoryBackendType, isValidMemoryBackendConfig } from '../../types/memoryBackend';

interface DatabricksConfigurationProps {
  onSaved?: () => void;
}

const DatabricksConfiguration: React.FC<DatabricksConfigurationProps> = ({ onSaved }) => {
  const { t } = useTranslation();
  const [config, setConfig] = useState<DatabricksConfig>({
    workspace_url: '',
    warehouse_id: '',
    catalog: '',
    schema: '',

    enabled: false,
    apps_enabled: false,
    volume_enabled: false,
    volume_path: 'main.default.task_outputs',
    volume_file_format: 'json',
    volume_create_date_dirs: true,
    // Knowledge source volume configuration
    knowledge_volume_enabled: false,
    knowledge_volume_path: 'main.default.knowledge',
    knowledge_chunk_size: 1000,
    knowledge_chunk_overlap: 200,
  });
  const [loading, setLoading] = useState(false);
  const [tokenStatus, setTokenStatus] = useState<DatabricksTokenStatus | null>(null);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error',
  });
  const [connectionStatus, setConnectionStatus] = useState<DatabricksConnectionStatus | null>(null);
  const [checkingConnection, setCheckingConnection] = useState(false);
  const [isMemoryBackendConfigured, setIsMemoryBackendConfigured] = useState<boolean>(false);
  const [memoryBackendType, setMemoryBackendType] = useState<MemoryBackendType | null>(null);

  // Function to check memory backend configuration for knowledge sources
  const checkMemoryBackendConfig = async () => {
    try {
      const memoryConfig = await MemoryBackendService.getConfig();
      if (memoryConfig && isValidMemoryBackendConfig(memoryConfig)) {
        // Knowledge sources ONLY work with Databricks Vector Search, not with ChromaDB
        const isConfigured = memoryConfig.backend_type === MemoryBackendType.DATABRICKS &&
          memoryConfig.databricks_config?.endpoint_name &&
          memoryConfig.databricks_config?.short_term_index;

        setIsMemoryBackendConfigured(!!isConfigured);
        setMemoryBackendType(memoryConfig.backend_type);
      } else {
        setIsMemoryBackendConfigured(false);
        setMemoryBackendType(null);
      }
    } catch (error) {
      console.error('Error checking memory backend configuration:', error);
      setIsMemoryBackendConfigured(false);
      setMemoryBackendType(null);
    }
  };

  useEffect(() => {
    const loadConfig = async () => {
      try {
        const databricksService = DatabricksService.getInstance();
        const savedConfig = await databricksService.getDatabricksConfig();
        if (savedConfig) {
          setConfig(savedConfig);
          // Check token status if Databricks is enabled
          if (savedConfig.enabled) {
            const status = await databricksService.checkPersonalTokenRequired();
            setTokenStatus(status);
          }
        }
      } catch (error) {
        console.error('Error loading configuration:', error);
      }
    };

    loadConfig();
    checkMemoryBackendConfig();
  }, []);

  const handleSaveConfig = async () => {
    // If Databricks is enabled but apps are disabled, validate all required fields
    if (config.enabled && !config.apps_enabled) {
      const requiredFields = {
        'Warehouse ID': config.warehouse_id?.trim(),
        'Catalog': config.catalog?.trim(),
        'Schema': config.schema?.trim(),

      };

      const emptyFields = Object.entries(requiredFields)
        .filter(([_, value]) => !value)
        .map(([field]) => field);

      if (emptyFields.length > 0) {
        setNotification({
          open: true,
          message: `Please fill in all required fields: ${emptyFields.join(', ')}`,
          severity: 'error',
        });
        return;
      }
    }

    setLoading(true);
    try {
      const databricksService = DatabricksService.getInstance();
      const savedConfig = await databricksService.setDatabricksConfig(config);
      setConfig(savedConfig);

      // Dispatch event to notify other components about configuration change
      window.dispatchEvent(new CustomEvent('databricks-config-updated', {
        detail: { config: savedConfig }
      }));

      // Check token status after saving if Databricks is enabled
      if (savedConfig.enabled) {
        const status = await databricksService.checkPersonalTokenRequired();
        setTokenStatus(status);
      } else {
        setTokenStatus(null);
      }
      
      setNotification({
        open: true,
        message: t('configuration.databricks.saved', { defaultValue: 'Databricks configuration saved successfully' }),
        severity: 'success',
      });
      
      if (onSaved) {
        onSaved();
      }
    } catch (error) {
      console.error('Error saving Databricks configuration:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to save Databricks configuration',
        severity: 'error',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleInputChange = (field: keyof DatabricksConfig) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setConfig(prev => ({
      ...prev,
      [field]: event.target.value
    }));
  };

  const handleDatabricksToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const newEnabled = event.target.checked;
    setConfig(prev => ({
      ...prev,
      enabled: newEnabled
    }));
    
    // Check token status when enabling Databricks
    if (newEnabled) {
      try {
        const databricksService = DatabricksService.getInstance();
        const status = await databricksService.checkPersonalTokenRequired();
        setTokenStatus(status);
      } catch (error) {
        console.error('Error checking token status:', error);
      }
    } else {
      setTokenStatus(null);
    }
  };

  const handleAppsToggle = (event: React.ChangeEvent<HTMLInputElement>) => {
    setConfig(prev => ({
      ...prev,
      apps_enabled: event.target.checked
    }));
  };

  const handleCloseNotification = () => {
    setNotification({ ...notification, open: false });
  };

  const handleCheckConnection = async () => {
    setCheckingConnection(true);
    try {
      const databricksService = DatabricksService.getInstance();
      const status = await databricksService.checkDatabricksConnection();
      setConnectionStatus(status);
    } catch (error) {
      console.error('Error checking connection:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to check Databricks connection',
        severity: 'error',
      });
    } finally {
      setCheckingConnection(false);
    }
  };

  return (
    <Box>
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'space-between', 
        mb: 2 
      }}>
        <Typography variant="subtitle1" fontWeight="medium">
          {t('configuration.databricks.title')}
        </Typography>
        <FormControlLabel
          control={
            <Switch
              checked={config.enabled}
              onChange={handleDatabricksToggle}
              color="primary"
            />
          }
          label={config.enabled ? t('common.enabled') : t('common.disabled')}
        />
      </Box>
      
      {tokenStatus && tokenStatus.personal_token_required && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          {tokenStatus.message}
        </Alert>
      )}
      
      <Stack spacing={2} sx={{ mb: 3 }}>
        <Box sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between', 
          mb: 1 
        }}>
          <Typography variant="subtitle2" color="text.secondary">
            {t('configuration.databricks.apps.title', { defaultValue: 'Databricks Apps Integration' })}
          </Typography>
          <FormControlLabel
            control={
              <Switch
                checked={config.apps_enabled}
                onChange={handleAppsToggle}
                color="primary"
                disabled={!config.enabled}
              />
            }
            label={config.apps_enabled ? t('common.enabled') : t('common.disabled')}
          />
        </Box>

        <TextField
          label={t('configuration.databricks.workspaceUrl')}
          value={config.workspace_url}
          onChange={handleInputChange('workspace_url')}
          fullWidth
          disabled={loading || !config.enabled || config.apps_enabled}
          size="small"
          helperText={config.apps_enabled ? t('configuration.databricks.workspaceUrl.disabled', { defaultValue: 'Not required when using Databricks Apps' }) : ''}
        />

        <TextField
          label={t('configuration.databricks.warehouseId')}
          value={config.warehouse_id}
          onChange={handleInputChange('warehouse_id')}
          fullWidth
          disabled={loading || !config.enabled || config.apps_enabled}
          size="small"
        />

        <TextField
          label={t('configuration.databricks.catalog')}
          value={config.catalog}
          onChange={handleInputChange('catalog')}
          fullWidth
          disabled={loading || !config.enabled || config.apps_enabled}
          size="small"
        />

        <TextField
          label={t('configuration.databricks.schema')}
          value={config.schema}
          onChange={handleInputChange('schema')}
          fullWidth
          disabled={loading || !config.enabled || config.apps_enabled}
          size="small"
        />


      </Stack>

      {/* Volume Configuration Section */}
      <Divider sx={{ my: 3 }} />
      
      <Box sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <StorageIcon sx={{ mr: 1, color: 'primary.main' }} />
          <Typography variant="subtitle1" fontWeight="medium">
            {t('configuration.databricks.volume.title', { defaultValue: 'Volume Uploads Configuration' })}
          </Typography>
        </Box>

        <Alert severity="info" sx={{ mb: 2 }}>
          {t('configuration.databricks.volume.description', { 
            defaultValue: 'Configure Databricks Volume settings for task outputs. When enabled, all task outputs will be automatically uploaded to the specified volume path. Tasks can override these settings individually.'
          })}
        </Alert>

        <Stack spacing={2}>
          <FormControlLabel
            control={
              <Switch
                checked={config.volume_enabled || false}
                onChange={(e) => setConfig(prev => ({ ...prev, volume_enabled: e.target.checked }))}
                color="primary"
                disabled={!config.enabled}
              />
            }
            label={
              <Box>
                <Typography variant="body1">
                  {t('configuration.databricks.volume.enable', { defaultValue: 'Enable Volume Uploads for All Tasks' })}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {t('configuration.databricks.volume.enableDescription', { defaultValue: 'Automatically upload task outputs to Databricks Volumes' })}
                </Typography>
              </Box>
            }
          />

          <TextField
            label={t('configuration.databricks.volume.path', { defaultValue: 'Volume Path' })}
            value={config.volume_path || 'main.default.task_outputs'}
            onChange={(e) => setConfig(prev => ({ ...prev, volume_path: e.target.value }))}
            fullWidth
            disabled={loading || !config.enabled || !config.volume_enabled}
            size="small"
            helperText={t('configuration.databricks.volume.pathHelp', { 
              defaultValue: 'Format: catalog.schema.volume (e.g., main.default.task_outputs)'
            })}
            placeholder="catalog.schema.volume"
          />

          <FormControl fullWidth size="small" disabled={loading || !config.enabled || !config.volume_enabled}>
            <InputLabel>{t('configuration.databricks.volume.format', { defaultValue: 'Default File Format' })}</InputLabel>
            <Select
              value={config.volume_file_format || 'json'}
              onChange={(e) => setConfig(prev => ({ ...prev, volume_file_format: e.target.value as 'json' | 'csv' | 'txt' }))}
              label={t('configuration.databricks.volume.format', { defaultValue: 'Default File Format' })}
            >
              <MenuItem value="json">JSON</MenuItem>
              <MenuItem value="csv">CSV</MenuItem>
              <MenuItem value="txt">Text</MenuItem>
            </Select>
          </FormControl>

          <FormControlLabel
            control={
              <Switch
                checked={config.volume_create_date_dirs !== false}
                onChange={(e) => setConfig(prev => ({ ...prev, volume_create_date_dirs: e.target.checked }))}
                color="primary"
                disabled={!config.enabled || !config.volume_enabled}
              />
            }
            label={
              <Box>
                <Typography variant="body1">
                  {t('configuration.databricks.volume.dateDirs', { defaultValue: 'Create Date-based Directories' })}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {t('configuration.databricks.volume.dateDirsDescription', { defaultValue: 'Organize outputs in YYYY/MM/DD folder structure' })}
                </Typography>
              </Box>
            }
          />

          {config.volume_enabled && config.enabled && (
            <Alert severity="info">
              <Typography variant="body2" sx={{ mb: 1 }}>
                <strong>{t('configuration.databricks.volume.example', { defaultValue: 'Example output path:' })}</strong>
              </Typography>
              <Typography variant="body2" component="code" sx={{ 
                display: 'block', 
                p: 1, 
                bgcolor: 'grey.100',
                borderRadius: 1,
                fontFamily: 'monospace',
                fontSize: '0.875rem'
              }}>
                /Volumes/{(config.volume_path || 'main.default.task_outputs').replace(/\./g, '/')}/[execution_name]
                {config.volume_create_date_dirs && '/YYYY/MM/DD'}
                /task_output.{config.volume_file_format || 'json'}
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                {t('configuration.databricks.volume.executionNote', { 
                  defaultValue: 'Note: [execution_name] will be replaced with the actual execution or run name'
                })}
              </Typography>
            </Alert>
          )}
        </Stack>
      </Box>

      {/* Knowledge Source Volume Configuration Section */}
      <Divider sx={{ my: 3 }} />
      
      <Box sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <StorageIcon sx={{ mr: 1, color: 'secondary.main' }} />
          <Typography variant="subtitle1" fontWeight="medium">
            {t('configuration.databricks.knowledge.title', { defaultValue: 'Knowledge Source Volume Configuration' })}
          </Typography>
        </Box>

        <Alert severity="info" sx={{ mb: 2 }}>
          {t('configuration.databricks.knowledge.description', { 
            defaultValue: 'Configure Databricks Volume settings for knowledge sources (RAG). When enabled, uploaded knowledge files will be stored in the specified volume and made available to AI agents during execution.'
          })}
        </Alert>

        <Stack spacing={2}>
          <Tooltip
            title={
              !isMemoryBackendConfigured
                ? `Knowledge sources require Databricks Vector Search memory backend configuration. ${memoryBackendType === MemoryBackendType.DEFAULT ? 'ChromaDB backend does not support knowledge sources.' : 'Vector Search backend is not properly configured.'}`
                : ''
            }
            placement="right"
            arrow
          >
            <FormControlLabel
              control={
                <Switch
                  checked={config.knowledge_volume_enabled || false}
                  onChange={(e) => setConfig(prev => ({ ...prev, knowledge_volume_enabled: e.target.checked }))}
                  color="secondary"
                  disabled={!config.enabled || !isMemoryBackendConfigured}
                />
              }
              label={
                <Box>
                  <Typography variant="body1" color={!isMemoryBackendConfigured ? 'text.disabled' : 'inherit'}>
                    {t('configuration.databricks.knowledge.enable', { defaultValue: 'Enable Knowledge Source Volume' })}
                  </Typography>
                  <Typography variant="caption" color={!isMemoryBackendConfigured ? 'text.disabled' : 'text.secondary'}>
                    {t('configuration.databricks.knowledge.enableDescription', { defaultValue: 'Store and retrieve knowledge files from Databricks Volumes for RAG' })}
                  </Typography>
                  {!isMemoryBackendConfigured && (
                    <Typography variant="caption" color="warning.main" sx={{ display: 'block', mt: 0.5 }}>
                      ⚠️ Requires memory backend configuration (Vector Search or default ChromaDB)
                    </Typography>
                  )}
                </Box>
              }
            />
          </Tooltip>

          <TextField
            label={t('configuration.databricks.knowledge.path', { defaultValue: 'Knowledge Volume Path' })}
            value={config.knowledge_volume_path || 'main.default.knowledge'}
            onChange={(e) => setConfig(prev => ({ ...prev, knowledge_volume_path: e.target.value }))}
            fullWidth
            disabled={loading || !config.enabled || !config.knowledge_volume_enabled || !isMemoryBackendConfigured}
            size="small"
            helperText={t('configuration.databricks.knowledge.pathHelp', { 
              defaultValue: 'Format: catalog.schema.volume (e.g., main.default.knowledge)'
            })}
            placeholder="catalog.schema.volume"
          />

          <TextField
            label={t('configuration.databricks.knowledge.chunkSize', { defaultValue: 'Chunk Size' })}
            value={config.knowledge_chunk_size || 1000}
            onChange={(e) => setConfig(prev => ({ ...prev, knowledge_chunk_size: parseInt(e.target.value) || 1000 }))}
            fullWidth
            disabled={loading || !config.enabled || !config.knowledge_volume_enabled || !isMemoryBackendConfigured}
            size="small"
            type="number"
            helperText={t('configuration.databricks.knowledge.chunkSizeHelp', { 
              defaultValue: 'Size of text chunks for knowledge processing (default: 1000 characters)'
            })}
          />

          <TextField
            label={t('configuration.databricks.knowledge.chunkOverlap', { defaultValue: 'Chunk Overlap' })}
            value={config.knowledge_chunk_overlap || 200}
            onChange={(e) => setConfig(prev => ({ ...prev, knowledge_chunk_overlap: parseInt(e.target.value) || 200 }))}
            fullWidth
            disabled={loading || !config.enabled || !config.knowledge_volume_enabled || !isMemoryBackendConfigured}
            size="small"
            type="number"
            helperText={t('configuration.databricks.knowledge.chunkOverlapHelp', { 
              defaultValue: 'Overlap between chunks to maintain context (default: 200 characters)'
            })}
          />

          {config.knowledge_volume_enabled && config.enabled && (
            <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50' }}>
              <Typography variant="body2" sx={{ mb: 1 }}>
                <strong>{t('configuration.databricks.knowledge.structure', { defaultValue: 'Knowledge files will be organized as:' })}</strong>
              </Typography>
              <Typography variant="body2" component="code" sx={{ 
                display: 'block', 
                p: 1, 
                bgcolor: 'white',
                border: '1px solid',
                borderColor: 'grey.300',
                borderRadius: 1,
                fontFamily: 'monospace',
                fontSize: '0.875rem'
              }}>
                /Volumes/{(config.knowledge_volume_path || 'main.default.knowledge').replace(/\./g, '/')}/[group_id]/[execution_id]/[files]
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                {t('configuration.databricks.knowledge.structureNote', { 
                  defaultValue: 'Files are organized by group and execution ID for proper isolation and access control'
                })}
              </Typography>
            </Paper>
          )}
        </Stack>
      </Box>

      <Box sx={{ 
        display: 'flex', 
        justifyContent: 'space-between',
        mt: 2,
        mb: 2
      }}>
        <Button
          variant="outlined"
          startIcon={checkingConnection ? <CircularProgress size={18} /> : null}
          onClick={handleCheckConnection}
          disabled={checkingConnection || !config.enabled}
          size="medium"
        >
          {checkingConnection ? t('common.checking') : t('configuration.databricks.checkConnection', { defaultValue: 'Check Connection' })}
        </Button>
        
        <Button
          variant="contained"
          startIcon={loading ? <CircularProgress size={18} /> : <SaveIcon fontSize="small" />}
          onClick={handleSaveConfig}
          disabled={loading}
          size="medium"
        >
          {loading ? t('common.loading') : t('common.save')}
        </Button>
      </Box>
      
      {connectionStatus && (
        <Alert 
          severity={connectionStatus.connected ? "success" : "error"} 
          sx={{ mb: 2 }}
          icon={connectionStatus.connected ? <CheckCircleOutlineIcon /> : undefined}
        >
          {connectionStatus.message}
        </Alert>
      )}

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={handleCloseNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert
          onClose={handleCloseNotification}
          severity={notification.severity}
          sx={{ width: '100%' }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default DatabricksConfiguration; 