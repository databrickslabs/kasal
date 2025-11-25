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
import { useKnowledgeConfigStore } from '../../store/knowledgeConfigStore';
import { ToolService } from '../../api/ToolService';

import apiClient from '../../config/api/ApiConfig';
import { AxiosError } from 'axios';

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

    // MLflow configuration
    mlflow_enabled: false,
    mlflow_experiment_name: 'kasal-crew-execution-traces',
    evaluation_enabled: false,
    evaluation_judge_model: '',

    // Volume configuration fields
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
  const [workspaceUrlFromBackend, setWorkspaceUrlFromBackend] = useState<string>('');

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

        // Fetch workspace URL from backend environment
        try {
          const envInfo = await databricksService.getDatabricksEnvironment();
          if (envInfo.databricks_host) {
            setWorkspaceUrlFromBackend(envInfo.databricks_host);
          }
        } catch (error) {
          console.error('Error fetching Databricks environment:', error);
        }

        const savedConfig = await databricksService.getDatabricksConfig();
        if (savedConfig) {
          setConfig(savedConfig);
          // Check token status if Databricks is enabled
          if (savedConfig.enabled) {
            const status = await databricksService.checkPersonalTokenRequired();
            setTokenStatus(status);
          }

          // Sync tool state on load
          const DATABRICKS_KNOWLEDGE_SEARCH_TOOL_ID = 36;
          try {
            const tools = await ToolService.listTools();
            const knowledgeTool = tools.find(t => t.id === DATABRICKS_KNOWLEDGE_SEARCH_TOOL_ID);

            if (knowledgeTool) {
              // Check memory backend configuration
              const memoryConfig = await MemoryBackendService.getConfig();
              const isMemoryConfigured = memoryConfig &&
                isValidMemoryBackendConfig(memoryConfig) &&
                memoryConfig.backend_type === MemoryBackendType.DATABRICKS &&
                memoryConfig.databricks_config?.endpoint_name &&
                memoryConfig.databricks_config?.short_term_index;

              const shouldBeEnabled = savedConfig.enabled && savedConfig.knowledge_volume_enabled && isMemoryConfigured;

              // Only toggle if state doesn't match
              if (knowledgeTool.enabled !== shouldBeEnabled) {
                await ToolService.toggleToolEnabled(DATABRICKS_KNOWLEDGE_SEARCH_TOOL_ID);
                console.log(`Initial sync: DatabricksKnowledgeSearchTool ${shouldBeEnabled ? 'enabled' : 'disabled'}`);
              }
            }
          } catch (toolError) {
            console.error('Failed to sync tool state on load:', toolError);
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
    // If Databricks is enabled, validate all required fields
    if (config.enabled) {
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
      // Ensure workspace_url from backend environment is included in the config
      const configToSave = {
        ...config,
        workspace_url: workspaceUrlFromBackend || config.workspace_url
      };
      const savedConfig = await databricksService.setDatabricksConfig(configToSave);
      setConfig(savedConfig);

      // Sync DatabricksKnowledgeSearchTool enabled state with knowledge_volume_enabled
      // Tool ID 36 is DatabricksKnowledgeSearchTool
      const DATABRICKS_KNOWLEDGE_SEARCH_TOOL_ID = 36;

      try {
        // Get current tool state
        const tools = await ToolService.listTools();
        const knowledgeTool = tools.find(t => t.id === DATABRICKS_KNOWLEDGE_SEARCH_TOOL_ID);

        if (knowledgeTool) {
          const shouldBeEnabled = savedConfig.enabled && savedConfig.knowledge_volume_enabled && isMemoryBackendConfigured;

          // Only toggle if state doesn't match
          if (knowledgeTool.enabled !== shouldBeEnabled) {
            await ToolService.toggleToolEnabled(DATABRICKS_KNOWLEDGE_SEARCH_TOOL_ID);
            console.log(`DatabricksKnowledgeSearchTool ${shouldBeEnabled ? 'enabled' : 'disabled'} based on knowledge volume configuration`);
          }
        }
      } catch (toolError) {
        console.error('Failed to sync DatabricksKnowledgeSearchTool state:', toolError);
        // Don't fail the entire save operation if tool sync fails
      }

      // Refresh knowledge configuration store
      const { refreshConfiguration } = useKnowledgeConfigStore.getState();
      refreshConfiguration();

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


  const handleMlflowToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const newEnabled = event.target.checked;
    // Optimistically update UI state
    setConfig(prev => ({ ...prev, mlflow_enabled: newEnabled }));

    // Try to persist immediately via MLflow endpoint (doesn't require full config payload)
    try {
      await apiClient.post('/mlflow/status', { enabled: newEnabled });
    } catch (error) {
      const axErr = error as AxiosError;
      // If no Databricks config exists yet, backend returns 404; advise user to save main config
      if (axErr?.response?.status === 404) {
        setNotification({
          open: true,
          message: t('configuration.databricks.mlflow.saveFirst', { defaultValue: 'Please save Databricks settings first to persist MLflow.' }),
          severity: 'error',
        });
      } else {
        console.error('Failed to persist MLflow toggle:', error);
      }
    }
  };

  const handleEvaluationToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const newEnabled = event.target.checked;
    // Optimistically update UI state
    setConfig(prev => ({ ...prev, evaluation_enabled: newEnabled }));

    try {
      await apiClient.post('/mlflow/evaluation-status', { enabled: newEnabled });
    } catch (error) {
      const axErr = error as AxiosError;
      if (axErr?.response?.status === 404) {
        setNotification({
          open: true,
          message: t('configuration.databricks.mlflow.saveFirst', { defaultValue: 'Please save Databricks settings first to persist MLflow Evaluation.' }),
          severity: 'error',
        });
      } else {
        console.error('Failed to persist MLflow evaluation toggle:', error);
      }
    }
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
        <Divider sx={{ my: 2 }} />

        {/* MLflow Tracking Section */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Typography variant="subtitle2" color="text.secondary">
            {t('configuration.databricks.mlflow.title', { defaultValue: 'MLflow Tracking' })}
          </Typography>
          <FormControlLabel
            control={
              <Switch
                checked={!!config.mlflow_enabled}
                onChange={handleMlflowToggle}
                color="primary"
                disabled={!config.enabled}
              />
            }
            label={config.mlflow_enabled ? t('common.enabled') : t('common.disabled')}
          />
        </Box>

        {/* MLflow Experiment Name */}
        {config.mlflow_enabled && (
          <TextField
            label={t('configuration.databricks.mlflow.experimentName', { defaultValue: 'MLflow Experiment Name' })}
            value={config.mlflow_experiment_name || 'kasal-crew-execution-traces'}
            onChange={handleInputChange('mlflow_experiment_name')}
            fullWidth
            disabled={loading || !config.enabled || !config.mlflow_enabled}
            size="small"
            placeholder="kasal-crew-execution-traces"
            helperText={t('configuration.databricks.mlflow.experimentNameHelp', { defaultValue: 'Name of the MLflow experiment for crew execution traces' })}
          />
        )}

        {/* MLflow Evaluation Section */}
        {config.mlflow_enabled && (
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mt: 1 }}>
          <Typography variant="subtitle2" color="text.secondary">
            {t('configuration.databricks.evaluation.title', { defaultValue: 'MLflow Evaluation' })}
          </Typography>
          <FormControlLabel
            control={
              <Switch
                checked={!!config.evaluation_enabled}
                onChange={handleEvaluationToggle}
                color="primary"
                disabled={!config.enabled || !config.mlflow_enabled}
              />
            }
            label={config.evaluation_enabled ? t('common.enabled') : t('common.disabled')}
          />

        {/* Databricks Judge Model for MLflow Evaluation */}
        {config.mlflow_enabled && config.evaluation_enabled && (
          <TextField
            sx={{ mt: 1 }}
            label={t('configuration.databricks.evaluation.judgeModel', { defaultValue: 'Databricks Judge Endpoint (databricks:/...)' })}
            value={config.evaluation_judge_model || ''}
            onChange={handleInputChange('evaluation_judge_model')}
            fullWidth
            disabled={loading || !config.enabled}
            size="small"
            placeholder="databricks:/your-judge-endpoint"
            helperText={t('configuration.databricks.evaluation.judgeModelHelp', { defaultValue: 'Route for the judge model served on Databricks. Must start with databricks:/. Used to score evaluations.' })}
          />
        )}

        </Box>
        )}

        <Divider sx={{ my: 2 }} />

        <TextField
          label={t('configuration.databricks.workspaceUrl')}
          value={workspaceUrlFromBackend || config.workspace_url}
          onChange={handleInputChange('workspace_url')}
          fullWidth
          disabled={true}
          size="small"
          helperText={t('configuration.databricks.workspaceUrl.info', {
            defaultValue: 'Workspace URL is automatically configured from backend (DATABRICKS_HOST environment variable)'
          })}
        />

        <TextField
          label={t('configuration.databricks.warehouseId')}
          value={config.warehouse_id}
          onChange={handleInputChange('warehouse_id')}
          fullWidth
          disabled={loading || !config.enabled}
          size="small"
          required
        />

        <TextField
          label={t('configuration.databricks.catalog')}
          value={config.catalog}
          onChange={handleInputChange('catalog')}
          fullWidth
          disabled={loading || !config.enabled}
          size="small"
          required
        />

        <TextField
          label={t('configuration.databricks.schema')}
          value={config.schema}
          onChange={handleInputChange('schema')}
          fullWidth
          disabled={loading || !config.enabled}
          size="small"
          required
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