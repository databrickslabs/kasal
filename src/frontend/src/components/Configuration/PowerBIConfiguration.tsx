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
  Paper,
  Link,
} from '@mui/material';
import SaveIcon from '@mui/icons-material/Save';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import BarChartIcon from '@mui/icons-material/BarChart';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import apiClient from '../../config/api/ApiConfig';

interface PowerBIConfigurationProps {
  onSaved?: () => void;
}

interface PowerBIConfig {
  tenant_id: string;
  client_id: string;
  workspace_id: string;
  semantic_model_id: string;
  enabled: boolean;
}

interface PowerBIStatus {
  configured: boolean;
  enabled: boolean;
  message: string;
}

const PowerBIConfiguration: React.FC<PowerBIConfigurationProps> = ({ onSaved }) => {
  const [config, setConfig] = useState<PowerBIConfig>({
    tenant_id: '',
    client_id: '',
    workspace_id: '',
    semantic_model_id: '',
    enabled: false,
  });
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<PowerBIStatus | null>(null);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error',
  });

  useEffect(() => {
    const loadConfig = async () => {
      try {
        const response = await apiClient.get<PowerBIConfig>('/powerbi/config');
        setConfig(response.data);

        // Load status
        const statusResponse = await apiClient.get<PowerBIStatus>('/powerbi/status');
        setStatus(statusResponse.data);
      } catch (error) {
        console.error('Error loading Power BI configuration:', error);
      }
    };

    loadConfig();
  }, []);

  const handleSaveConfig = async () => {
    if (config.enabled && (!config.tenant_id || !config.client_id)) {
      setNotification({
        open: true,
        message: 'Tenant ID and Client ID are required when Power BI is enabled',
        severity: 'error',
      });
      return;
    }

    setLoading(true);
    try {
      await apiClient.post('/powerbi/config', config);

      setNotification({
        open: true,
        message: 'Power BI configuration saved successfully',
        severity: 'success',
      });

      // Reload status
      const statusResponse = await apiClient.get<PowerBIStatus>('/powerbi/status');
      setStatus(statusResponse.data);

      if (onSaved) {
        onSaved();
      }
    } catch (error: any) {
      setNotification({
        open: true,
        message: error.response?.data?.detail || 'Error saving Power BI configuration',
        severity: 'error',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleFieldChange = (field: keyof PowerBIConfig) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setConfig({
      ...config,
      [field]: event.target.value,
    });
  };

  const handleEnabledToggle = (event: React.ChangeEvent<HTMLInputElement>) => {
    setConfig({
      ...config,
      enabled: event.target.checked,
    });
  };

  return (
    <Paper elevation={2} sx={{ p: 3 }}>
      <Box display="flex" alignItems="center" mb={3}>
        <BarChartIcon sx={{ fontSize: 40, mr: 2, color: 'primary.main' }} />
        <Box flex={1}>
          <Typography variant="h5" gutterBottom>
            Power BI Integration
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Configure Power BI DAX query integration for your workspace
          </Typography>
        </Box>
      </Box>

      <Divider sx={{ mb: 3 }} />

      {/* Status Display */}
      {status && (
        <Alert
          severity={status.configured && status.enabled ? 'success' : 'info'}
          sx={{ mb: 3 }}
          icon={status.configured && status.enabled ? <CheckCircleOutlineIcon /> : undefined}
        >
          {status.message}
        </Alert>
      )}

      {/* Configuration Help */}
      <Alert severity="info" sx={{ mb: 3 }} icon={<InfoOutlinedIcon />}>
        <Typography variant="body2" gutterBottom>
          <strong>Power BI Configuration Requirements:</strong>
        </Typography>
        <Typography variant="body2" component="div">
          <ol style={{ margin: '8px 0', paddingLeft: '20px' }}>
            <li>
              <strong>Azure AD Application</strong>: Register an app in Azure AD and note the Tenant ID and Client ID
            </li>
            <li>
              <strong>Credentials</strong>: Set the following in API Keys (Configuration → API Keys):
              <ul style={{ marginTop: '4px' }}>
                <li><code>POWERBI_USERNAME</code> - Service account username</li>
                <li><code>POWERBI_PASSWORD</code> - Service account password</li>
                <li><code>POWERBI_CLIENT_SECRET</code> - Azure AD app client secret (optional)</li>
              </ul>
            </li>
            <li>
              <strong>Workspace Access</strong>: Grant the service account access to your Power BI workspace
            </li>
          </ol>
        </Typography>
        <Link
          href="/docs/powerbi_integration.md"
          target="_blank"
          sx={{ fontSize: '0.875rem', mt: 1, display: 'inline-block' }}
        >
          View full documentation →
        </Link>
      </Alert>

      <Stack spacing={3}>
        {/* Enable Toggle */}
        <FormControlLabel
          control={
            <Switch
              checked={config.enabled}
              onChange={handleEnabledToggle}
              color="primary"
            />
          }
          label={
            <Box>
              <Typography variant="body1">Enable Power BI Integration</Typography>
              <Typography variant="body2" color="text.secondary">
                Allow agents to execute DAX queries against Power BI semantic models
              </Typography>
            </Box>
          }
        />

        <Divider />

        {/* Azure AD Configuration */}
        <Typography variant="h6" gutterBottom>
          Azure AD Configuration
        </Typography>

        <TextField
          label="Tenant ID *"
          value={config.tenant_id}
          onChange={handleFieldChange('tenant_id')}
          fullWidth
          required={config.enabled}
          disabled={!config.enabled}
          helperText="Your Azure AD tenant ID (GUID)"
          placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        />

        <TextField
          label="Client ID (Application ID) *"
          value={config.client_id}
          onChange={handleFieldChange('client_id')}
          fullWidth
          required={config.enabled}
          disabled={!config.enabled}
          helperText="Azure AD application (client) ID"
          placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        />

        <Divider />

        {/* Power BI Configuration */}
        <Typography variant="h6" gutterBottom>
          Power BI Workspace Configuration
        </Typography>

        <TextField
          label="Workspace ID"
          value={config.workspace_id}
          onChange={handleFieldChange('workspace_id')}
          fullWidth
          disabled={!config.enabled}
          helperText="Default Power BI workspace ID (optional, can be specified per query)"
          placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        />

        <TextField
          label="Semantic Model ID"
          value={config.semantic_model_id}
          onChange={handleFieldChange('semantic_model_id')}
          fullWidth
          disabled={!config.enabled}
          helperText="Default semantic model ID (optional, can be specified per query)"
          placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        />

        {/* Save Button */}
        <Box display="flex" justifyContent="flex-end" gap={2} mt={2}>
          <Button
            variant="contained"
            color="primary"
            onClick={handleSaveConfig}
            disabled={loading}
            startIcon={loading ? <CircularProgress size={20} /> : <SaveIcon />}
          >
            {loading ? 'Saving...' : 'Save Configuration'}
          </Button>
        </Box>
      </Stack>

      {/* Notification Snackbar */}
      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={() => setNotification({ ...notification, open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert
          onClose={() => setNotification({ ...notification, open: false })}
          severity={notification.severity}
          sx={{ width: '100%' }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </Paper>
  );
};

export default PowerBIConfiguration;
