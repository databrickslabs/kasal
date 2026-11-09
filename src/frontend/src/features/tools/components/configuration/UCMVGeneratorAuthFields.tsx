import React from 'react';
import {
  Box,
  Typography,
  TextField,
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
import type { usePowerBIOAuth } from '../../../../hooks/usePowerBIOAuth';
import type { PowerBIAuthMethod, UCMetricViewGeneratorConfig } from './ucmvGeneratorConfigTypes';

/**
 * Power BI workspace + authentication fields for the API input mode of the
 * UC Metric View Generator (service principal, service account, user OAuth).
 */
interface UCMVGeneratorAuthFieldsProps {
  value: UCMetricViewGeneratorConfig;
  disabled: boolean;
  authMethod: PowerBIAuthMethod;
  onFieldChange: (field: keyof UCMetricViewGeneratorConfig, fieldValue: string | boolean | number) => void;
  onAuthMethodChange: (event: React.MouseEvent<HTMLElement>, newMethod: PowerBIAuthMethod | null) => void;
  oauth: ReturnType<typeof usePowerBIOAuth>;
}

export const UCMVGeneratorAuthFields: React.FC<UCMVGeneratorAuthFieldsProps> = ({
  value,
  disabled,
  authMethod,
  onFieldChange,
  onAuthMethodChange,
  oauth,
}) => {
  const { isAuthenticated, signIn, signOut, userEmail, isLoading: oauthLoading, error: oauthError } = oauth;
  return (
    <>
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(156, 39, 176)' }}>
        Power BI Workspace Configuration
      </Typography>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        <TextField
          label="Workspace ID"
          value={value.workspace_id || ''}
          onChange={(e) => onFieldChange('workspace_id', e.target.value)}
          disabled={disabled}
          required
          fullWidth
          helperText="Power BI workspace ID"
          size="small"
        />
        <TextField
          label="Dataset ID (Optional)"
          value={value.dataset_id || ''}
          onChange={(e) => onFieldChange('dataset_id', e.target.value)}
          disabled={disabled}
          fullWidth
          helperText="Specific dataset to extract (leave empty to scan all)"
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
        onChange={onAuthMethodChange}
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
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <TextField
            label="Tenant ID"
            value={value.tenant_id || ''}
            onChange={(e) => onFieldChange('tenant_id', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            helperText="Azure AD tenant ID"
            size="small"
          />
          <TextField
            label="Client ID"
            value={value.client_id || ''}
            onChange={(e) => onFieldChange('client_id', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            helperText="Application/Client ID"
            size="small"
          />
          <TextField
            label="Client Secret"
            value={value.client_secret || ''}
            onChange={(e) => onFieldChange('client_secret', e.target.value)}
            disabled={disabled}
            required
            type="password"
            fullWidth
            helperText="Client secret for service principal"
            size="small"
          />
        </Box>
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
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Tenant ID"
              value={value.tenant_id || ''}
              onChange={(e) => onFieldChange('tenant_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Azure AD tenant ID"
              size="small"
            />
            <TextField
              label="Client ID"
              value={value.client_id || ''}
              onChange={(e) => onFieldChange('client_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Azure AD application Client ID (with delegated permissions)"
              size="small"
            />
            <TextField
              label="Username (UPN)"
              value={value.username || ''}
              onChange={(e) => onFieldChange('username', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="Service account email/UPN (e.g., user@domain.com)"
              size="small"
            />
            <TextField
              label="Password"
              value={value.password || ''}
              onChange={(e) => onFieldChange('password', e.target.value)}
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
                href="https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result?tryIt=true"
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
            onChange={(e) => onFieldChange('oauth_client_id', e.target.value)}
            disabled={disabled || isAuthenticated}
            fullWidth
            helperText="Your Azure AD app Client ID for OAuth sign-in"
            size="small"
          />

          <TextField
            label="Access Token (Alternative)"
            value={value.access_token || ''}
            onChange={(e) => onFieldChange('access_token', e.target.value)}
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

      <TextField
        label="PBI API Base URL (Optional)"
        value={value.pbi_api_base_url || ''}
        onChange={(e) => onFieldChange('pbi_api_base_url', e.target.value)}
        disabled={disabled}
        fullWidth
        helperText="Custom Power BI API base URL (leave empty for default)"
        size="small"
      />
    </>
  );
};
