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
} from '@mui/material';
import SaveIcon from '@mui/icons-material/Save';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import { useTranslation } from 'react-i18next';
import { DatabricksService, DatabricksConfig, DatabricksTokenStatus, DatabricksConnectionStatus } from '../../api/DatabricksService';

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
    secret_scope: '',
    enabled: false,
    apps_enabled: false,
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
  }, []);

  const handleSaveConfig = async () => {
    // If Databricks is enabled but apps are disabled, validate all required fields
    if (config.enabled && !config.apps_enabled) {
      const requiredFields = {
        'Warehouse ID': config.warehouse_id?.trim(),
        'Catalog': config.catalog?.trim(),
        'Schema': config.schema?.trim(),
        'Secret Scope': config.secret_scope?.trim(),
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
          disabled={loading || !config.enabled}
          size="small"
        />

        <TextField
          label={t('configuration.databricks.catalog')}
          value={config.catalog}
          onChange={handleInputChange('catalog')}
          fullWidth
          disabled={loading || !config.enabled}
          size="small"
        />

        <TextField
          label={t('configuration.databricks.schema')}
          value={config.schema}
          onChange={handleInputChange('schema')}
          fullWidth
          disabled={loading || !config.enabled}
          size="small"
        />

        <TextField
          label={t('configuration.databricks.secretScope')}
          value={config.secret_scope}
          onChange={handleInputChange('secret_scope')}
          fullWidth
          disabled={loading || !config.enabled}
          size="small"
        />
      </Stack>

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