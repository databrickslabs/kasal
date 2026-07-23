/**
 * Measure Converter Configuration Selector Component
 *
 * Provides PBI credentials + workspace/dataset configuration for the Measure Conversion Pipeline tool.
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
  Button
} from '@mui/material';
import LoginIcon from '@mui/icons-material/Login';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import { usePowerBIOAuth } from '../../hooks/usePowerBIOAuth';

// Authentication method type
export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface MeasureConverterConfig {
  // Power BI inbound params
  powerbi_semantic_model_id?: string;
  powerbi_group_id?: string;
  // Authentication method
  powerbi_auth_method?: PowerBIAuthMethod;
  // Power BI Service Principal authentication
  powerbi_tenant_id?: string;
  powerbi_client_id?: string;
  powerbi_client_secret?: string;
  // Power BI Service Account authentication
  powerbi_username?: string;
  powerbi_password?: string;
  // Power BI User OAuth authentication
  powerbi_oauth_client_id?: string; // Azure AD app client ID for OAuth
  powerbi_access_token?: string;
  // Power BI other settings
  powerbi_include_hidden?: boolean;
  powerbi_filter_pattern?: string;
  // SQL outbound params
  sql_dialect?: string;
  sql_include_comments?: boolean;
  sql_process_structures?: boolean;
  // DAX outbound params
  dax_process_structures?: boolean;
  // General
  definition_name?: string;
  // Index signature for compatibility with Record<string, unknown>
  [key: string]: string | boolean | undefined;
}

interface MeasureConverterConfigSelectorProps {
  value: MeasureConverterConfig;
  onChange: (config: MeasureConverterConfig) => void;
  disabled?: boolean;
}

export const MeasureConverterConfigSelector: React.FC<MeasureConverterConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  // OAuth hook for User OAuth authentication - pass the client ID from config
  const { accessToken, isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = usePowerBIOAuth({
    clientId: value.powerbi_oauth_client_id || ''
  });

  const handleFieldChange = (field: keyof MeasureConverterConfig, fieldValue: string | boolean) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  const handleAuthMethodChange = (_event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => {
    if (newMethod !== null) {
      const updatedConfig: MeasureConverterConfig = {
        ...value,
        powerbi_auth_method: newMethod
      };

      // Clear credentials when switching methods
      if (newMethod === 'user_oauth') {
        updatedConfig.powerbi_tenant_id = undefined;
        updatedConfig.powerbi_client_id = undefined;
        updatedConfig.powerbi_client_secret = undefined;
        updatedConfig.powerbi_username = undefined;
        updatedConfig.powerbi_password = undefined;
        // Set access token if authenticated
        if (accessToken) {
          updatedConfig.powerbi_access_token = accessToken;
        }
      } else if (newMethod === 'service_principal') {
        updatedConfig.powerbi_access_token = undefined;
        updatedConfig.powerbi_username = undefined;
        updatedConfig.powerbi_password = undefined;
      } else if (newMethod === 'service_account') {
        updatedConfig.powerbi_access_token = undefined;
        updatedConfig.powerbi_client_secret = undefined;
      }

      onChange(updatedConfig);
    }
  };

  // Update access token when OAuth state changes
  React.useEffect(() => {
    if (value.powerbi_auth_method === 'user_oauth' && accessToken) {
      onChange({
        ...value,
        powerbi_access_token: accessToken
      });
    }
  }, [accessToken, value.powerbi_auth_method]);

  const authMethod = value.powerbi_auth_method || 'service_principal';

  // CRITICAL: Initialize auth_method on component mount if auth_method is missing
  // This handles existing crews that were created before auth_method was added
  React.useEffect(() => {
    if (!value.powerbi_auth_method) {
      // Only set default on mount, not on every render
      onChange({
        ...value,
        powerbi_auth_method: 'service_principal'
      });
    }
  }, []); // Empty deps = run once on mount

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Power BI Configuration */}
      <Typography variant="subtitle2" color="primary" sx={{ fontWeight: 600 }}>
        Power BI Configuration
      </Typography>

      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        <TextField
          label="Dataset/Semantic Model ID"
          value={value.powerbi_semantic_model_id || ''}
          onChange={(e) => handleFieldChange('powerbi_semantic_model_id', e.target.value)}
          disabled={disabled}
          required
          fullWidth
          helperText="Power BI dataset identifier"
          size="small"
        />
        <TextField
          label="Workspace/Group ID"
          value={value.powerbi_group_id || ''}
          onChange={(e) => handleFieldChange('powerbi_group_id', e.target.value)}
          disabled={disabled}
          required
          fullWidth
          helperText="Power BI workspace identifier"
          size="small"
        />

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
            <TextField
              label="Tenant ID"
              value={value.powerbi_tenant_id || ''}
              onChange={(e) => handleFieldChange('powerbi_tenant_id', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Azure AD tenant ID"
              size="small"
            />
            <TextField
              label="Client ID"
              value={value.powerbi_client_id || ''}
              onChange={(e) => handleFieldChange('powerbi_client_id', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Application/Client ID"
              size="small"
            />
            <TextField
              label="Client Secret"
              value={value.powerbi_client_secret || ''}
              onChange={(e) => handleFieldChange('powerbi_client_secret', e.target.value)}
              disabled={disabled}
              type="password"
              fullWidth
              helperText="Client secret for service principal"
              size="small"
            />
          </>
        )}

        {/* Service Account Authentication Fields */}
        {authMethod === 'service_account' && (
          <>
            <Alert severity="info" variant="outlined" sx={{ mb: 1 }}>
              <Typography variant="caption">
                <strong>Service Account:</strong> Use a user account (username + password) instead of Service Principal.
                Useful when Service Principal doesn't have sufficient permissions.
              </Typography>
            </Alert>
            <TextField
              label="Tenant ID"
              value={value.powerbi_tenant_id || ''}
              onChange={(e) => handleFieldChange('powerbi_tenant_id', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Azure AD tenant ID"
              size="small"
            />
            <TextField
              label="Client ID"
              value={value.powerbi_client_id || ''}
              onChange={(e) => handleFieldChange('powerbi_client_id', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Azure AD application Client ID (with delegated permissions)"
              size="small"
            />
            <TextField
              label="Username (UPN)"
              value={value.powerbi_username || ''}
              onChange={(e) => handleFieldChange('powerbi_username', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Service account email/UPN (e.g., user@domain.com)"
              size="small"
            />
            <TextField
              label="Password"
              value={value.powerbi_password || ''}
              onChange={(e) => handleFieldChange('powerbi_password', e.target.value)}
              disabled={disabled}
              type="password"
              fullWidth
              helperText="Service account password"
              size="small"
            />
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
                  href="https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-dataset?tryIt=true"
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
              value={value.powerbi_oauth_client_id || ''}
              onChange={(e) => handleFieldChange('powerbi_oauth_client_id', e.target.value)}
              disabled={disabled || isAuthenticated}
              fullWidth
              helperText="Your Azure AD app Client ID for OAuth sign-in"
              size="small"
            />

            <TextField
              label="Access Token (Alternative)"
              value={value.powerbi_access_token || ''}
              onChange={(e) => handleFieldChange('powerbi_access_token', e.target.value)}
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

            {!isAuthenticated && !value.powerbi_access_token ? (
              <Button
                variant="contained"
                onClick={signIn}
                disabled={disabled || oauthLoading || !value.powerbi_oauth_client_id}
                startIcon={<LoginIcon />}
                fullWidth
              >
                {oauthLoading ? 'Signing in...' : 'Sign in with Microsoft'}
              </Button>
            ) : value.powerbi_access_token && !isAuthenticated ? (
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

        <Divider sx={{ my: 1 }} />

        <FormControlLabel
          control={
            <Checkbox
              checked={value.powerbi_include_hidden || false}
              onChange={(e) => handleFieldChange('powerbi_include_hidden', e.target.checked)}
              disabled={disabled}
            />
          }
          label="Include hidden measures"
        />
        <TextField
          label="Filter Pattern (Regex)"
          value={value.powerbi_filter_pattern || ''}
          onChange={(e) => handleFieldChange('powerbi_filter_pattern', e.target.value)}
          disabled={disabled}
          fullWidth
          helperText="Optional regex pattern to filter measure names"
          size="small"
        />
      </Box>
    </Box>
  );
};
