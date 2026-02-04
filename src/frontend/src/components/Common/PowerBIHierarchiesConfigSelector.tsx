/**
 * Power BI Hierarchies Configuration Selector Component
 *
 * Provides configuration UI for the Power BI Hierarchies Tool.
 * Extracts hierarchies from Power BI/Fabric semantic models using the Fabric API getDefinition
 * endpoint (TMDL format) and generates Unity Catalog dimension views.
 */

import React from 'react';
import {
  Box,
  Typography,
  TextField,
  FormControlLabel,
  Checkbox,
  Divider,
  ToggleButtonGroup,
  ToggleButton,
  Alert,
  Chip,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Button
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';

// Authentication method type
export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface PowerBIHierarchiesConfig {
  // Configuration mode
  mode?: 'static' | 'dynamic';
  // Power BI / Fabric Configuration
  workspace_id?: string;
  dataset_id?: string;
  // Authentication method
  auth_method?: PowerBIAuthMethod;
  // Service Principal authentication (must have SemanticModel.ReadWrite.All permission)
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  // Service Account authentication (username + password)
  username?: string;
  password?: string;
  // User OAuth authentication
  oauth_client_id?: string; // Azure AD app client ID for OAuth
  access_token?: string;
  // Unity Catalog Target
  target_catalog?: string;
  target_schema?: string;
  // Output Options
  skip_system_tables?: boolean;
  include_hidden?: boolean;
  // Index signature for compatibility
  [key: string]: string | boolean | undefined;
}

interface PowerBIHierarchiesConfigSelectorProps {
  value: PowerBIHierarchiesConfig;
  onChange: (config: PowerBIHierarchiesConfig) => void;
  disabled?: boolean;
}

export const PowerBIHierarchiesConfigSelector: React.FC<PowerBIHierarchiesConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  // OAuth hook for User OAuth authentication - pass the client ID from config
  const { accessToken, isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = usePowerBIOAuth({
    clientId: value.oauth_client_id || ''
  });

  const handleFieldChange = (field: keyof PowerBIHierarchiesConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleAuthMethodChange = (_event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => {
    if (newMethod !== null) {
      const updatedConfig: PowerBIHierarchiesConfig = {
        ...value,
        auth_method: newMethod
      };

      // Clear credentials when switching methods
      if (newMethod === 'user_oauth') {
        updatedConfig.tenant_id = undefined;
        updatedConfig.client_id = undefined;
        updatedConfig.client_secret = undefined;
        updatedConfig.username = undefined;
        updatedConfig.password = undefined;
        // Set access token if authenticated
        if (accessToken) {
          updatedConfig.access_token = accessToken;
        }
      } else if (newMethod === 'service_principal') {
        updatedConfig.access_token = undefined;
        updatedConfig.username = undefined;
        updatedConfig.password = undefined;
      } else if (newMethod === 'service_account') {
        updatedConfig.access_token = undefined;
        updatedConfig.client_secret = undefined;
      }

      onChange(updatedConfig);
    }
  };

  // Update access token when OAuth state changes
  React.useEffect(() => {
    if (value.auth_method === 'user_oauth' && accessToken) {
      onChange({
        ...value,
        access_token: accessToken
      });
    }
  }, [accessToken, value.auth_method]);

  // Helper to check if a value is a placeholder
  const isPlaceholder = (val: string | undefined): boolean => {
    if (!val) return false;
    return /^\{[a-z_]+\}$/.test(val);
  };

  const handleModeChange = (_event: React.MouseEvent<HTMLElement>, newMode: 'static' | 'dynamic' | null) => {
    if (newMode !== null) {
      const updatedConfig: PowerBIHierarchiesConfig = {
        ...value,
        mode: newMode
      };

      // When switching to dynamic mode, auto-populate placeholders
      if (newMode === 'dynamic') {
        // Set Power BI parameters to placeholders
        updatedConfig.workspace_id = '{workspace_id}';
        updatedConfig.dataset_id = '{dataset_id}';
        // Auth can be either SPN or access_token in dynamic mode
        updatedConfig.tenant_id = '{tenant_id}';
        updatedConfig.client_id = '{client_id}';
        updatedConfig.client_secret = '{client_secret}';
        updatedConfig.access_token = '{access_token}';
        // Unity Catalog target can also be dynamic
        updatedConfig.target_catalog = '{target_catalog}';
        updatedConfig.target_schema = '{target_schema}';
        // Keep output options as static values
        updatedConfig.skip_system_tables = value.skip_system_tables !== false;
        updatedConfig.include_hidden = value.include_hidden || false;
      } else {
        // When switching to static mode, clear placeholder values
        // Only clear if the current value is a placeholder pattern
        if (isPlaceholder(value.workspace_id)) updatedConfig.workspace_id = '';
        if (isPlaceholder(value.dataset_id)) updatedConfig.dataset_id = '';
        if (isPlaceholder(value.tenant_id)) updatedConfig.tenant_id = '';
        if (isPlaceholder(value.client_id)) updatedConfig.client_id = '';
        if (isPlaceholder(value.client_secret)) updatedConfig.client_secret = '';
        if (isPlaceholder(value.access_token)) updatedConfig.access_token = '';
        if (isPlaceholder(value.target_catalog)) updatedConfig.target_catalog = 'main';
        if (isPlaceholder(value.target_schema)) updatedConfig.target_schema = 'default';
      }

      onChange(updatedConfig);
    }
  };

  const mode = value.mode || 'static';
  const authMethod = value.auth_method || 'service_principal';

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Mode Toggle */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
          Configuration Mode
        </Typography>
        <ToggleButtonGroup
          value={mode}
          exclusive
          onChange={handleModeChange}
          disabled={disabled}
          fullWidth
          size="small"
        >
          <ToggleButton value="static">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Static
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Configure all parameters in UI
              </Typography>
            </Box>
          </ToggleButton>
          <ToggleButton value="dynamic">
            <Box sx={{ textAlign: 'center', py: 0.5 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Dynamic
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Parameters from execution inputs
              </Typography>
            </Box>
          </ToggleButton>
        </ToggleButtonGroup>
      </Box>

      <Divider sx={{ my: 1 }} />

      {/* Dynamic Mode Info */}
      {mode === 'dynamic' && (
        <Alert severity="info" sx={{ mb: 1 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
            Dynamic Parameters Mode
          </Typography>
          <Typography variant="caption" component="div">
            Parameters will be resolved from execution inputs at runtime.
            Use this when calling from external apps (e.g., Databricks Apps).
          </Typography>

          <Box sx={{ mt: 1.5, p: 1, bgcolor: 'rgba(0,0,0,0.04)', borderRadius: 1 }}>
            <Typography variant="caption" sx={{ fontWeight: 600 }}>
              Required inputs:
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.5 }}>
              <Chip label="workspace_id" size="small" color="primary" variant="outlined" />
              <Chip label="dataset_id" size="small" color="primary" variant="outlined" />
            </Box>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
              Optional: <Chip label="target_catalog" size="small" variant="outlined" sx={{ ml: 0.5 }} />
              <Chip label="target_schema" size="small" variant="outlined" sx={{ ml: 0.5 }} />
            </Typography>
          </Box>

          <Box sx={{ mt: 1.5, p: 1, bgcolor: 'rgba(0,0,0,0.04)', borderRadius: 1 }}>
            <Typography variant="caption" sx={{ fontWeight: 600 }}>
              Authentication (choose one):
            </Typography>
            <Box sx={{ mt: 0.5 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                <Chip label="access_token" size="small" color="success" />
                <Typography variant="caption">← User OAuth (recommended for user context)</Typography>
              </Box>
              <Typography variant="caption" color="text.secondary" sx={{ pl: 1, display: 'block', mb: 0.5 }}>
                <em>or</em>
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Chip label="tenant_id" size="small" variant="outlined" />
                <Chip label="client_id" size="small" variant="outlined" />
                <Chip label="client_secret" size="small" variant="outlined" />
                <Typography variant="caption">← Service Principal</Typography>
              </Box>
            </Box>
          </Box>

          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            <strong>Example with OAuth:</strong>{' '}
            <code style={{ fontSize: '0.7rem' }}>
              {`{ "workspace_id": "...", "dataset_id": "...", "access_token": "eyJ..." }`}
            </code>
          </Typography>
        </Alert>
      )}

      {/* Static Mode: Full Configuration Forms */}
      {mode === 'static' && (
        <>
          {/* Power BI / Fabric Configuration */}
          <Typography variant="subtitle2" color="primary" sx={{ fontWeight: 600 }}>
            Power BI / Fabric Configuration
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Workspace ID"
              value={value.workspace_id || ''}
              onChange={(e) => handleFieldChange('workspace_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Power BI/Fabric workspace ID containing the semantic model"
              size="small"
            />
            <TextField
              label="Dataset ID (Semantic Model ID)"
              value={value.dataset_id || ''}
              onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Semantic model/dataset ID to extract hierarchies from"
              size="small"
            />
          </Box>

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Authentication Method
            </Typography>
          </Divider>

          {/* Auth Method Toggle */}
          <ToggleButtonGroup
            value={authMethod}
            exclusive
            onChange={handleAuthMethodChange}
            disabled={disabled}
            fullWidth
            size="small"
          >
            <ToggleButton value="service_principal">
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
                <SecurityIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Service Principal
                </Typography>
              </Box>
            </ToggleButton>
            <ToggleButton value="service_account">
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
                <PersonIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  Service Account
                </Typography>
              </Box>
            </ToggleButton>
            <ToggleButton value="user_oauth">
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
                <LoginIcon sx={{ fontSize: 18, mb: 0.5 }} />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  User OAuth
                </Typography>
              </Box>
            </ToggleButton>
          </ToggleButtonGroup>

          {/* Service Principal Authentication Fields */}
          {authMethod === 'service_principal' && (
            <>
              <Alert severity="warning" variant="outlined" sx={{ mb: 1 }}>
                <Typography variant="caption">
                  <strong>Important:</strong> The Service Principal must have <strong>SemanticModel.ReadWrite.All</strong> permissions
                  or be a <strong>member of the workspace</strong>. This tool uses the Fabric API <code>getDefinition</code> endpoint
                  which requires appropriate access to fetch TMDL definitions.
                </Typography>
              </Alert>

              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <TextField
                  label="Tenant ID"
                  value={value.tenant_id || ''}
                  onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Azure AD tenant ID"
                  size="small"
                />
                <TextField
                  label="Client ID"
                  value={value.client_id || ''}
                  onChange={(e) => handleFieldChange('client_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Application/Client ID with SemanticModel.ReadWrite.All permission"
                  size="small"
                />
                <TextField
                  label="Client Secret"
                  value={value.client_secret || ''}
                  onChange={(e) => handleFieldChange('client_secret', e.target.value)}
                  disabled={disabled}
                  required
                  type="password"
                  fullWidth
                  helperText="Client secret for service principal"
                  size="small"
                />
              </Box>
            </>
          )}

          {/* Service Account Authentication Fields */}
          {authMethod === 'service_account' && (
            <>
              <Alert severity="info" variant="outlined" sx={{ mb: 1 }}>
                <Typography variant="caption">
                  <strong>Service Account:</strong> Use a user account (username + password) instead of Service Principal.
                  This is useful when Service Principal doesn't have sufficient permissions to access Power BI data.
                  Requires an Azure AD app with delegated permissions.
                </Typography>
              </Alert>

              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <TextField
                  label="Tenant ID"
                  value={value.tenant_id || ''}
                  onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Azure AD tenant ID"
                  size="small"
                />
                <TextField
                  label="Client ID"
                  value={value.client_id || ''}
                  onChange={(e) => handleFieldChange('client_id', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Azure AD application Client ID (with delegated permissions)"
                  size="small"
                />
                <TextField
                  label="Username (UPN)"
                  value={value.username || ''}
                  onChange={(e) => handleFieldChange('username', e.target.value)}
                  disabled={disabled}
                  required
                  fullWidth
                  helperText="Service account email/UPN (e.g., user@domain.com)"
                  size="small"
                />
                <TextField
                  label="Password"
                  value={value.password || ''}
                  onChange={(e) => handleFieldChange('password', e.target.value)}
                  disabled={disabled}
                  required
                  type="password"
                  fullWidth
                  helperText="Service account password"
                  size="small"
                />
              </Box>
            </>
          )}

          {/* User OAuth Authentication */}
          {authMethod === 'user_oauth' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Alert severity="info" variant="outlined">
                <Typography variant="caption" component="div">
                  <strong>Option 1:</strong> If you have an Azure AD app, enter its Client ID and sign in.
                  <br />
                  <strong>Option 2:</strong> Get a token from{' '}
                  <a
                    href="https://learn.microsoft.com/en-us/rest/api/fabric/semanticmodel/items/get-semantic-model-definition?tryIt=true"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    Microsoft's API docs (Try It)
                  </a>
                  {' '}and paste it below.
                </Typography>
              </Alert>

              <TextField
                label="OAuth Client ID (Optional)"
                value={value.oauth_client_id || ''}
                onChange={(e) => handleFieldChange('oauth_client_id', e.target.value)}
                disabled={disabled || isAuthenticated}
                fullWidth
                helperText="Your Azure AD app Client ID for OAuth sign-in"
                size="small"
              />

              <TextField
                label="Access Token (Alternative)"
                value={value.access_token || ''}
                onChange={(e) => handleFieldChange('access_token', e.target.value)}
                disabled={disabled || isAuthenticated}
                fullWidth
                type="password"
                helperText="Paste a token from Microsoft's Try It page"
                size="small"
              />

              {oauthError && (
                <Alert severity="error" variant="outlined">
                  <Typography variant="caption">{oauthError}</Typography>
                </Alert>
              )}

              {!isAuthenticated && !value.access_token ? (
                <Button
                  variant="contained"
                  onClick={signIn}
                  disabled={disabled || oauthLoading || !value.oauth_client_id}
                  startIcon={<LoginIcon />}
                  fullWidth
                >
                  {oauthLoading ? 'Signing in...' : 'Sign in with Microsoft'}
                </Button>
              ) : value.access_token && !isAuthenticated ? (
                <Alert severity="success" icon={<CheckCircleIcon />}>
                  <Typography variant="body2">Access token provided</Typography>
                </Alert>
              ) : (
                <>
                  <Alert severity="success" icon={<CheckCircleIcon />}>
                    <Typography variant="body2">
                      Signed in as <strong>{userEmail || 'User'}</strong>
                    </Typography>
                  </Alert>
                  <Button
                    variant="outlined"
                    onClick={signOut}
                    disabled={disabled}
                    size="small"
                  >
                    Sign Out
                  </Button>
                </>
              )}
            </Box>
          )}

          <Divider sx={{ my: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Unity Catalog Target
            </Typography>
          </Divider>

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Target Catalog"
              value={value.target_catalog || 'main'}
              onChange={(e) => handleFieldChange('target_catalog', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Unity Catalog catalog name for dimension views"
              size="small"
            />
            <TextField
              label="Target Schema"
              value={value.target_schema || 'default'}
              onChange={(e) => handleFieldChange('target_schema', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Unity Catalog schema name for dimension views"
              size="small"
            />
          </Box>
        </>
      )}

      {/* Advanced Options (shown in both modes) */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Output Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.skip_system_tables !== false}
                  onChange={(e) => handleFieldChange('skip_system_tables', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Skip system tables (LocalDateTable, DateTableTemplate, etc.)</Typography>}
            />
            <FormControlLabel
              control={
                <Checkbox
                  checked={value.include_hidden || false}
                  onChange={(e) => handleFieldChange('include_hidden', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={<Typography variant="body2">Include hidden hierarchies</Typography>}
            />
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Extracts hierarchies from Power BI/Fabric semantic models using the Fabric API
          <code>getDefinition</code> endpoint which returns TMDL format. Generates Unity Catalog
          dimension views with <code>hierarchy_path</code> column and metadata tables.
        </Typography>
        <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
          <li>Extracts all hierarchy definitions (levels, columns, ordinals)</li>
          <li>Generates CREATE VIEW statements with CONCAT-based hierarchy_path</li>
          <li>Creates _metadata_hierarchies table DDL and INSERT statements</li>
          <li>Supports drill-down path visualization</li>
          <li>Filters system date hierarchies by default</li>
        </Box>
      </Alert>
    </Box>
  );
};
