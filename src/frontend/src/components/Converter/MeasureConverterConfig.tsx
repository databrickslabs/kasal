/**
 * Measure Converter Configuration Component
 * Universal converter with dropdown-based FROM/TO selection
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Switch,
  FormControlLabel,
  Button,
  Alert,
  CircularProgress,
  Divider,
  Grid,
  Chip,
  SelectChangeEvent,
} from '@mui/material';
import {
  PlayArrow as RunIcon,
  Save as SaveIcon,
  Logout as LogoutIcon,
  Key as KeyIcon,
  Person as PersonIcon,
  Login as LoginIcon,
  CheckCircle as CheckCircleIcon,
} from '@mui/icons-material';
import type {
  MeasureConversionConfig,
  ConversionFormat,
  InboundFormat,
  OutboundFormat,
  SQLDialect,
  PowerBIAuthMethod,
} from '../../types/converter';
import { ConverterService } from '../../api/ConverterService';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';
import toast from 'react-hot-toast';

interface MeasureConverterConfigProps {
  onRun?: (config: MeasureConversionConfig) => void;
  onSave?: (config: MeasureConversionConfig, name: string) => void;
  initialConfig?: Partial<MeasureConversionConfig>;
}

/**
 * Helper function to convert format codes to display names
 */
const getFormatDisplayName = (format: ConversionFormat): string => {
  const displayNames: Record<ConversionFormat, string> = {
    'powerbi': 'Power BI',
    'yaml': 'YAML',
    'dax': 'DAX',
    'sql': 'SQL',
    'uc_metrics': 'UC Metrics',
    'tableau': 'Tableau',
    'excel': 'Excel'
  };
  return displayNames[format] || format;
};

export const MeasureConverterConfig: React.FC<MeasureConverterConfigProps> = ({
  onRun,
  onSave,
  initialConfig,
}) => {
  const [config, setConfig] = useState<MeasureConversionConfig>({
    inbound_connector: 'powerbi',
    outbound_format: 'dax',
    powerbi_auth_method: 'service_principal',
    powerbi_include_hidden: false,
    sql_dialect: 'databricks',
    sql_include_comments: true,
    sql_process_structures: true,
    dax_process_structures: true,
    result_as_answer: false,
    ...initialConfig,
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | undefined>();
  const [configName, setConfigName] = useState('');
  const [showSaveDialog, setShowSaveDialog] = useState(false);

  // Power BI OAuth hook - only used when auth_method is 'user_oauth'
  // Uses separate OAuth client ID field (different from Service Principal client_id)
  const {
    accessToken: oauthAccessToken,
    isAuthenticated: isOAuthAuthenticated,
    isLoading: isOAuthLoading,
    error: oauthError,
    userEmail,
    signIn: oauthSignIn,
    signOut: oauthSignOut,
  } = usePowerBIOAuth({
    // This client ID should be configured in your Azure App Registration for OAuth
    clientId: config.powerbi_oauth_client_id || '',
  });

  // Update config when prop changes
  useEffect(() => {
    if (initialConfig) {
      setConfig(prev => ({ ...prev, ...initialConfig }));
    }
  }, [initialConfig]);

  const handleInboundChange = (event: SelectChangeEvent<InboundFormat>) => {
    setConfig({
      ...config,
      inbound_connector: event.target.value as InboundFormat,
    });
  };

  const handleOutboundChange = (event: SelectChangeEvent<OutboundFormat>) => {
    setConfig({
      ...config,
      outbound_format: event.target.value as OutboundFormat,
    });
  };

  const handleRun = async () => {
    // Validation
    if (config.inbound_connector === 'powerbi') {
      if (!config.powerbi_semantic_model_id || !config.powerbi_group_id) {
        setError('Power BI requires: Dataset ID and Workspace ID');
        return;
      }

      // Validate based on auth method
      const authMethod = config.powerbi_auth_method || 'service_principal';
      if (authMethod === 'service_principal') {
        if (!config.powerbi_tenant_id || !config.powerbi_client_id || !config.powerbi_client_secret) {
          setError('Service Principal authentication requires: Tenant ID, Client ID, and Client Secret');
          return;
        }
      } else if (authMethod === 'user_oauth') {
        // Accept either: OAuth sign-in, or manually pasted access token
        const hasOAuthToken = isOAuthAuthenticated || oauthAccessToken;
        const hasManualToken = config.powerbi_access_token;
        if (!hasOAuthToken && !hasManualToken) {
          setError('Please either sign in with Microsoft or paste an access token');
          return;
        }
      }
    } else if (config.inbound_connector === 'yaml') {
      if (!config.yaml_content && !config.yaml_file_path) {
        setError('YAML requires either content or file path');
        return;
      }
    }

    setError(undefined);
    setIsLoading(true);

    // Build final config with access token if using OAuth
    const finalConfig = { ...config };
    if (config.inbound_connector === 'powerbi' && config.powerbi_auth_method === 'user_oauth') {
      // Prefer OAuth token if authenticated, otherwise use manually pasted token
      if (oauthAccessToken) {
        finalConfig.powerbi_access_token = oauthAccessToken;
      }
      // Clear SPN credentials when using OAuth (token is already set)
      delete finalConfig.powerbi_tenant_id;
      delete finalConfig.powerbi_client_id;
      delete finalConfig.powerbi_client_secret;
      delete finalConfig.powerbi_oauth_client_id; // Don't send OAuth client ID to backend
    }

    try {
      // Call the provided onRun callback if exists
      if (onRun) {
        await onRun(finalConfig);
        toast.success('Conversion started successfully');
      } else {
        // Or create a job directly
        const job = await ConverterService.createJob({
          source_format: finalConfig.inbound_connector,
          target_format: finalConfig.outbound_format,
          configuration: finalConfig,
          name: `${getFormatDisplayName(finalConfig.inbound_connector)} → ${getFormatDisplayName(finalConfig.outbound_format)}`,
        });
        toast.success(`Job created: ${job.id}`);
      }
    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Conversion failed';
      setError(errorMessage);
      toast.error(errorMessage);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSave = async () => {
    if (!configName.trim()) {
      toast.error('Please enter a configuration name');
      return;
    }

    setIsLoading(true);
    try {
      if (onSave) {
        await onSave(config, configName);
      } else {
        await ConverterService.saveConfiguration({
          name: configName,
          source_format: config.inbound_connector,
          target_format: config.outbound_format,
          configuration: config,
          description: `${getFormatDisplayName(config.inbound_connector)} to ${getFormatDisplayName(config.outbound_format)} conversion`,
        });
      }
      toast.success('Configuration saved successfully');
      setShowSaveDialog(false);
      setConfigName('');
    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Save failed';
      toast.error(errorMessage);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Paper sx={{ p: 3 }}>
      <Typography variant="h5" gutterBottom>
        Measure Conversion Pipeline
      </Typography>
      <Typography variant="body2" color="text.secondary" paragraph>
        Universal converter with flexible source and target selection
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(undefined)}>
          {error}
        </Alert>
      )}

      <Divider sx={{ my: 2 }} />

      {/* ===== INBOUND CONNECTOR SELECTION ===== */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          <Chip label="1" size="small" sx={{ mr: 1 }} /> Inbound Connector (Source)
        </Typography>

        <FormControl fullWidth margin="normal">
          <InputLabel>Source Format</InputLabel>
          <Select
            value={config.inbound_connector}
            onChange={handleInboundChange}
            label="Source Format"
          >
            <MenuItem value="powerbi">Power BI</MenuItem>
            <MenuItem value="yaml">YAML Definition</MenuItem>
            <MenuItem value="tableau" disabled>Tableau (Coming Soon)</MenuItem>
            <MenuItem value="excel" disabled>Excel (Coming Soon)</MenuItem>
          </Select>
        </FormControl>

        {/* Power BI Configuration */}
        {config.inbound_connector === 'powerbi' && (
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Dataset/Semantic Model ID *"
                value={config.powerbi_semantic_model_id || ''}
                onChange={(e) => setConfig({ ...config, powerbi_semantic_model_id: e.target.value })}
                helperText="Power BI dataset ID to extract measures from"
                required
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Workspace ID *"
                value={config.powerbi_group_id || ''}
                onChange={(e) => setConfig({ ...config, powerbi_group_id: e.target.value })}
                helperText="Power BI workspace ID containing the dataset"
                required
              />
            </Grid>

            {/* Authentication Method Selector */}
            <Grid item xs={12}>
              <Divider sx={{ my: 1 }} />
              <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 2, mt: 1 }}>
                Authentication Method
              </Typography>
              <Box sx={{ display: 'flex', gap: 2 }}>
                <Button
                  variant={config.powerbi_auth_method === 'service_principal' ? 'contained' : 'outlined'}
                  startIcon={<KeyIcon />}
                  onClick={() => setConfig({ ...config, powerbi_auth_method: 'service_principal' })}
                  sx={{ flex: 1 }}
                >
                  Service Principal
                </Button>
                <Button
                  variant={config.powerbi_auth_method === 'user_oauth' ? 'contained' : 'outlined'}
                  startIcon={<PersonIcon />}
                  onClick={() => setConfig({ ...config, powerbi_auth_method: 'user_oauth' })}
                  sx={{ flex: 1 }}
                >
                  User OAuth
                </Button>
              </Box>
            </Grid>

            {/* Service Principal Authentication */}
            {config.powerbi_auth_method === 'service_principal' && (
              <>
                <Grid item xs={12}>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                    Enter your Azure AD Service Principal credentials
                  </Typography>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Tenant ID *"
                    value={config.powerbi_tenant_id || ''}
                    onChange={(e) => setConfig({ ...config, powerbi_tenant_id: e.target.value })}
                    helperText="Azure AD tenant ID"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Client ID *"
                    value={config.powerbi_client_id || ''}
                    onChange={(e) => setConfig({ ...config, powerbi_client_id: e.target.value })}
                    helperText="Application/Client ID"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Client Secret *"
                    value={config.powerbi_client_secret || ''}
                    onChange={(e) => setConfig({ ...config, powerbi_client_secret: e.target.value })}
                    type="password"
                    helperText="Client secret for service principal"
                    required
                  />
                </Grid>
              </>
            )}

            {/* User OAuth Authentication */}
            {config.powerbi_auth_method === 'user_oauth' && (
              <>
                <Grid item xs={12}>
                  <Alert severity="info" sx={{ mb: 1 }}>
                    <Typography variant="body2" gutterBottom>
                      Sign in with your Microsoft account to access Power BI with your own permissions.
                    </Typography>
                    <Typography variant="body2">
                      <strong>Option 1:</strong> If you have an Azure AD app registration, enter its Client ID below.
                    </Typography>
                    <Typography variant="body2">
                      <strong>Option 2:</strong> Use{' '}
                      <a
                        href="https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset?tryIt=true"
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{ color: 'inherit' }}
                      >
                        Microsoft's interactive API docs
                      </a>
                      {' '}to get an access token, then paste it below.
                    </Typography>
                  </Alert>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="OAuth Client ID (Optional)"
                    value={config.powerbi_oauth_client_id || ''}
                    onChange={(e) => setConfig({ ...config, powerbi_oauth_client_id: e.target.value })}
                    helperText="Your Azure AD app Client ID for OAuth sign-in"
                    disabled={isOAuthAuthenticated}
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Access Token (Alternative)"
                    value={config.powerbi_access_token || ''}
                    onChange={(e) => setConfig({ ...config, powerbi_access_token: e.target.value })}
                    helperText="Paste a token from Microsoft's Try It page"
                    type="password"
                    disabled={isOAuthAuthenticated}
                  />
                </Grid>
                <Grid item xs={12}>
                  {oauthError && (
                    <Alert severity="error" sx={{ mb: 2 }}>
                      {oauthError}
                    </Alert>
                  )}
                  {!isOAuthAuthenticated && !config.powerbi_access_token ? (
                    <Button
                      variant="contained"
                      color="primary"
                      startIcon={isOAuthLoading ? <CircularProgress size={20} /> : <LoginIcon />}
                      onClick={oauthSignIn}
                      disabled={isOAuthLoading || !config.powerbi_oauth_client_id}
                      fullWidth
                      sx={{ py: 1.5 }}
                    >
                      {isOAuthLoading ? 'Signing in...' : 'Sign in with Microsoft'}
                    </Button>
                  ) : config.powerbi_access_token && !isOAuthAuthenticated ? (
                    <Alert severity="success" icon={<CheckCircleIcon />}>
                      <Typography variant="body2">
                        Access token provided manually
                      </Typography>
                    </Alert>
                  ) : (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, p: 2, bgcolor: 'success.light', borderRadius: 1 }}>
                      <CheckCircleIcon color="success" />
                      <Box sx={{ flexGrow: 1 }}>
                        <Typography variant="body2" fontWeight="bold" color="success.dark">
                          Signed in as {userEmail || 'Microsoft User'}
                        </Typography>
                        <Typography variant="caption" color="success.dark">
                          OAuth token obtained successfully
                        </Typography>
                      </Box>
                      <Button
                        variant="outlined"
                        size="small"
                        startIcon={<LogoutIcon />}
                        onClick={oauthSignOut}
                        color="inherit"
                      >
                        Sign Out
                      </Button>
                    </Box>
                  )}
                </Grid>
              </>
            )}

            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Filter Pattern (Optional)"
                value={config.powerbi_filter_pattern || ''}
                onChange={(e) => setConfig({ ...config, powerbi_filter_pattern: e.target.value })}
                helperText="Regex pattern to filter measures"
              />
            </Grid>
            <Grid item xs={12}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.powerbi_include_hidden || false}
                    onChange={(e) => setConfig({ ...config, powerbi_include_hidden: e.target.checked })}
                  />
                }
                label="Include Hidden Measures"
              />
            </Grid>
          </Grid>
        )}

        {/* YAML Configuration */}
        {config.inbound_connector === 'yaml' && (
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="YAML Content"
                value={config.yaml_content || ''}
                onChange={(e) => setConfig({ ...config, yaml_content: e.target.value })}
                multiline
                rows={10}
                helperText="Paste YAML KPI definition content here"
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="YAML File Path (Alternative)"
                value={config.yaml_file_path || ''}
                onChange={(e) => setConfig({ ...config, yaml_file_path: e.target.value })}
                helperText="Or provide path to YAML file"
              />
            </Grid>
          </Grid>
        )}
      </Box>

      <Divider sx={{ my: 3 }} />

      {/* ===== OUTBOUND FORMAT SELECTION ===== */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          <Chip label="2" size="small" sx={{ mr: 1 }} /> Outbound Format (Target)
        </Typography>

        <FormControl fullWidth margin="normal">
          <InputLabel>Target Format</InputLabel>
          <Select
            value={config.outbound_format}
            onChange={handleOutboundChange}
            label="Target Format"
          >
            <MenuItem value="dax">DAX (Power BI)</MenuItem>
            <MenuItem value="sql">SQL (Multiple Dialects)</MenuItem>
            <MenuItem value="uc_metrics">Unity Catalog Metrics</MenuItem>
          </Select>
        </FormControl>

        {/* SQL Configuration */}
        {config.outbound_format === 'sql' && (
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>SQL Dialect</InputLabel>
                <Select
                  value={config.sql_dialect || 'databricks'}
                  onChange={(e) => setConfig({ ...config, sql_dialect: e.target.value as SQLDialect })}
                  label="SQL Dialect"
                >
                  <MenuItem value="databricks">Databricks</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.sql_include_comments !== false}
                    onChange={(e) => setConfig({ ...config, sql_include_comments: e.target.checked })}
                  />
                }
                label="Include Comments"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={config.sql_process_structures !== false}
                    onChange={(e) => setConfig({ ...config, sql_process_structures: e.target.checked })}
                  />
                }
                label="Process Time Intelligence Structures"
              />
            </Grid>
          </Grid>
        )}

        {/* DAX Configuration */}
        {config.outbound_format === 'dax' && (
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12}>
              <FormControlLabel
                control={
                  <Switch
                    checked={config.dax_process_structures !== false}
                    onChange={(e) => setConfig({ ...config, dax_process_structures: e.target.checked })}
                  />
                }
                label="Process Time Intelligence Structures"
              />
            </Grid>
          </Grid>
        )}
      </Box>

      <Divider sx={{ my: 3 }} />

      {/* ===== GENERAL OPTIONS ===== */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          General Options
        </Typography>

        <Grid container spacing={2}>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Definition Name (Optional)"
              value={config.definition_name || ''}
              onChange={(e) => setConfig({ ...config, definition_name: e.target.value })}
              helperText="Custom name for the KPI definition"
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <FormControlLabel
              control={
                <Switch
                  checked={config.result_as_answer || false}
                  onChange={(e) => setConfig({ ...config, result_as_answer: e.target.checked })}
                />
              }
              label="Return Result as Answer"
            />
          </Grid>
        </Grid>
      </Box>

      {/* ===== ACTION BUTTONS ===== */}
      <Box sx={{ display: 'flex', gap: 2, mt: 3 }}>
        <Button
          variant="contained"
          color="primary"
          startIcon={isLoading ? <CircularProgress size={20} /> : <RunIcon />}
          onClick={handleRun}
          disabled={isLoading}
          size="large"
        >
          Run Conversion
        </Button>

        {!showSaveDialog ? (
          <Button
            variant="outlined"
            startIcon={<SaveIcon />}
            onClick={() => setShowSaveDialog(true)}
            disabled={isLoading}
          >
            Save Configuration
          </Button>
        ) : (
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexGrow: 1 }}>
            <TextField
              size="small"
              label="Configuration Name"
              value={configName}
              onChange={(e) => setConfigName(e.target.value)}
              sx={{ flexGrow: 1 }}
            />
            <Button
              variant="contained"
              size="small"
              onClick={handleSave}
              disabled={isLoading || !configName.trim()}
            >
              Save
            </Button>
            <Button
              variant="text"
              size="small"
              onClick={() => {
                setShowSaveDialog(false);
                setConfigName('');
              }}
            >
              Cancel
            </Button>
          </Box>
        )}
      </Box>
    </Paper>
  );
};

export default MeasureConverterConfig;
