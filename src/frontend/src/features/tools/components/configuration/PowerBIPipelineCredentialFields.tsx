/**
 * Power BI connection + BOTH credential sets used by the pipeline extraction.
 *
 * The Pipeline Config Generator (Tool 90) needs a non-admin credential set
 * (Execute Queries API) and an admin set (Admin Scanner API), each a Service
 * Principal or a Service Account. The UCMV Drift Monitor (Tool 98) runs that same
 * extraction, so both forms render these fields from here — one source for the
 * field names the backend reads.
 */

import React from 'react';
import {
  Box,
  Typography,
  TextField,
  Alert,
  ToggleButtonGroup,
  ToggleButton,
} from '@mui/material';

export type PipelineAuthMethod = 'service_principal' | 'service_account';

export interface PowerBIPipelineCredentials {
  workspace_id?: string;
  dataset_id?: string;
  // A single auth_method applies to BOTH credential sets (the backend
  // auto-detects SP vs SA per set but the UI keeps it simple).
  auth_method?: PipelineAuthMethod;
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;   // Service Principal
  username?: string;        // Service Account
  password?: string;        // Service Account
  admin_client_id?: string;
  admin_client_secret?: string;   // Service Principal
  admin_username?: string;        // Service Account
  admin_password?: string;        // Service Account
}

interface Props<T extends PowerBIPipelineCredentials> {
  value: T;
  onChange: (config: T) => void;
  disabled?: boolean;
}

export function PowerBIPipelineCredentialFields<T extends PowerBIPipelineCredentials>({
  value,
  onChange,
  disabled = false,
}: Props<T>) {
  const handleFieldChange = (field: keyof PowerBIPipelineCredentials, fieldValue: string) => {
    onChange({ ...value, [field]: fieldValue });
  };

  // Default to Service Principal to preserve prior behaviour when unset.
  const authMethod: PipelineAuthMethod = value.auth_method || 'service_principal';
  const isSA = authMethod === 'service_account';

  const handleAuthMethodChange = (
    _e: React.MouseEvent<HTMLElement>,
    newMethod: PipelineAuthMethod | null,
  ) => {
    if (!newMethod) return;  // ignore de-select
    const updated: T = { ...value, auth_method: newMethod };
    if (newMethod === 'service_principal') {
      // Clear Service Account fields on both sets.
      updated.username = undefined;
      updated.password = undefined;
      updated.admin_username = undefined;
      updated.admin_password = undefined;
    }
    // NOTE: switching to Service Account does NOT clear client_secret /
    // admin_client_secret — they remain an optional SP fallback (the backend
    // uses SP if provided when an SA can't reach an API).
    onChange(updated);
  };

  return (
    <>
      {/* PBI Configuration */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(25, 118, 210)' }}>
        Power BI Configuration
      </Typography>
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Workspace ID"
          value={value.workspace_id || ''}
          onChange={(e) => handleFieldChange('workspace_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="PBI Workspace GUID"
        />
        <TextField
          label="Dataset ID"
          value={value.dataset_id || ''}
          onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="PBI Dataset / Semantic Model GUID"
        />
      </Box>

      {/* Auth method toggle */}
      <Box>
        <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 0.5 }}>
          Authentication method
        </Typography>
        <ToggleButtonGroup
          value={authMethod}
          exclusive
          onChange={handleAuthMethodChange}
          size="small"
          disabled={disabled}
        >
          <ToggleButton value="service_principal">Service Principal</ToggleButton>
          <ToggleButton value="service_account">Service Account</ToggleButton>
        </ToggleButtonGroup>
        <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
          <Typography variant="caption" component="div">
            <strong>Service Principal:</strong> use an app registration (Client ID + Client Secret).<br />
            <strong>Service Account:</strong> use a user account (Client ID + username + password).
            The Client Secret stays available as an optional SP fallback.
          </Typography>
        </Alert>
      </Box>

      {/* Non-Admin credentials */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(76, 175, 80)' }}>
        Non-Admin {isSA ? 'Service Account' : 'Service Principal'} (Execute Queries API)
      </Typography>
      <TextField
        label="Tenant ID"
        value={value.tenant_id || ''}
        onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
        disabled={disabled}
        fullWidth
        size="small"
        required
        helperText="Azure AD Tenant ID (shared by both credential sets)"
      />
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Client ID"
          value={value.client_id || ''}
          onChange={(e) => handleFieldChange('client_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="Workspace member with SemanticModel.ReadWrite.All"
        />
        {isSA ? (
          <TextField
            label="Client Secret (optional SP fallback)"
            value={value.client_secret || ''}
            onChange={(e) => handleFieldChange('client_secret', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            type="password"
            helperText="Optional — used as SP fallback if the SA can't reach an API"
          />
        ) : (
          <TextField
            label="Client Secret"
            value={value.client_secret || ''}
            onChange={(e) => handleFieldChange('client_secret', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            required
            type="password"
            helperText="Non-admin SP secret"
          />
        )}
      </Box>
      {isSA && (
        <Box sx={{ display: 'flex', gap: 2 }}>
          <TextField
            label="Username (UPN)"
            value={value.username || ''}
            onChange={(e) => handleFieldChange('username', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            required
            helperText="Service Account username / UPN"
          />
          <TextField
            label="Password"
            value={value.password || ''}
            onChange={(e) => handleFieldChange('password', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            required
            type="password"
            helperText="Service Account password"
          />
        </Box>
      )}

      {/* Admin credentials */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(255, 152, 0)' }}>
        Admin {isSA ? 'Service Account' : 'Service Principal'} (Admin Scanner API)
      </Typography>
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Admin Client ID"
          value={value.admin_client_id || ''}
          onChange={(e) => handleFieldChange('admin_client_id', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          required
          helperText="Power BI Admin with Tenant.Read.All"
        />
        {isSA ? (
          <TextField
            label="Admin Client Secret (optional SP fallback)"
            value={value.admin_client_secret || ''}
            onChange={(e) => handleFieldChange('admin_client_secret', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            type="password"
            helperText="Optional — used as SP fallback if the admin SA can't reach the Admin Scanner"
          />
        ) : (
          <TextField
            label="Admin Client Secret"
            value={value.admin_client_secret || ''}
            onChange={(e) => handleFieldChange('admin_client_secret', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            required
            type="password"
            helperText="Admin SP secret"
          />
        )}
      </Box>
      {isSA && (
        <Box sx={{ display: 'flex', gap: 2 }}>
          <TextField
            label="Admin Username (UPN)"
            value={value.admin_username || ''}
            onChange={(e) => handleFieldChange('admin_username', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            required
            helperText="Admin Service Account username / UPN"
          />
          <TextField
            label="Admin Password"
            value={value.admin_password || ''}
            onChange={(e) => handleFieldChange('admin_password', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            required
            type="password"
            helperText="Admin Service Account password"
          />
        </Box>
      )}
    </>
  );
}
