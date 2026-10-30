/**
 * Power BI connection fields for RECONCILIATION.
 *
 * Reconciliation pulls the ground-truth numbers from the live Power BI model, so
 * it needs a workspace id, a dataset/semantic-model id, and credentials — exactly
 * the keys the iterative loop forwards to the reconciliation tool
 * (workspace_id → pbi_workspace_id, dataset_id → pbi_dataset_id, tenant_id/
 * client_id/client_secret/username/password/access_token → pbi_*).
 *
 * These are the SAME config keys the API-mode "Power BI Workspace Configuration"
 * block writes. In JSON mode (the typical pipeline handoff) that block is hidden,
 * which left reconciliation with nowhere to enter credentials — this surfaces them
 * in the Reconciliation tab so a JSON-mode run can still reach Power BI.
 */

import React from 'react';
import {
  Box,
  Typography,
  TextField,
  ToggleButtonGroup,
  ToggleButton,
  Alert
} from '@mui/material';
import SecurityIcon from '@mui/icons-material/Security';
import PersonIcon from '@mui/icons-material/Person';
import VpnKeyIcon from '@mui/icons-material/VpnKey';
import type {
  UCMetricViewGeneratorConfig,
  PowerBIAuthMethod
} from './UCMetricViewGeneratorConfigSelector';

interface PowerBIReconConnectionFieldsProps {
  value: UCMetricViewGeneratorConfig;
  onChange: (config: UCMetricViewGeneratorConfig) => void;
  disabled?: boolean;
}

export const PowerBIReconConnectionFields: React.FC<PowerBIReconConnectionFieldsProps> = ({
  value,
  onChange,
  disabled = false
}) => {
  const authMethod: PowerBIAuthMethod = value.auth_method || 'service_principal';

  const handleFieldChange = (
    field: keyof UCMetricViewGeneratorConfig,
    fieldValue: string
  ) => {
    onChange({ ...value, [field]: fieldValue });
  };

  const handleAuthMethodChange = (
    _event: React.MouseEvent<HTMLElement>,
    newMethod: PowerBIAuthMethod | null
  ) => {
    if (newMethod === null) return;
    const updated: UCMetricViewGeneratorConfig = { ...value, auth_method: newMethod };
    // Clear credentials irrelevant to the chosen method so stale values aren't
    // forwarded to the reconciler.
    if (newMethod === 'service_principal') {
      updated.username = undefined;
      updated.password = undefined;
      updated.access_token = undefined;
    } else if (newMethod === 'service_account') {
      updated.client_secret = undefined;
      updated.access_token = undefined;
    } else if (newMethod === 'user_oauth') {
      updated.tenant_id = undefined;
      updated.client_id = undefined;
      updated.client_secret = undefined;
      updated.username = undefined;
      updated.password = undefined;
    }
    onChange(updated);
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="caption" color="text.secondary">
        The Power BI model is the ground truth reconciliation compares against. These are the
        same credentials used for live extraction — in JSON/handoff mode they must be entered
        here, on the step that reconciles.
      </Typography>

      <TextField
        label="Power BI workspace ID"
        value={value.workspace_id || ''}
        onChange={(e) => handleFieldChange('workspace_id', e.target.value)}
        disabled={disabled}
        required
        fullWidth
        size="small"
        helperText="Required — the Power BI workspace to query."
      />
      <TextField
        label="Power BI dataset / semantic-model ID"
        value={value.dataset_id || ''}
        onChange={(e) => handleFieldChange('dataset_id', e.target.value)}
        disabled={disabled}
        required
        fullWidth
        size="small"
        helperText="Required for reconciliation — the dataset DAX queries run against."
      />

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
            <Typography variant="body2" sx={{ fontWeight: 600 }}>Service Principal</Typography>
          </Box>
        </ToggleButton>
        <ToggleButton value="service_account">
          <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
            <PersonIcon sx={{ fontSize: 18, mb: 0.5 }} />
            <Typography variant="body2" sx={{ fontWeight: 600 }}>Service Account</Typography>
          </Box>
        </ToggleButton>
        <ToggleButton value="user_oauth">
          <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 0.5 }}>
            <VpnKeyIcon sx={{ fontSize: 18, mb: 0.5 }} />
            <Typography variant="body2" sx={{ fontWeight: 600 }}>Access Token</Typography>
          </Box>
        </ToggleButton>
      </ToggleButtonGroup>

      {authMethod === 'service_principal' && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <TextField
            label="Tenant ID"
            value={value.tenant_id || ''}
            onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            size="small"
            helperText="Azure AD tenant ID"
          />
          <TextField
            label="Client ID"
            value={value.client_id || ''}
            onChange={(e) => handleFieldChange('client_id', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            size="small"
            helperText="Application/Client ID"
          />
          <TextField
            label="Client Secret"
            value={value.client_secret || ''}
            onChange={(e) => handleFieldChange('client_secret', e.target.value)}
            disabled={disabled}
            required
            type="password"
            fullWidth
            size="small"
            helperText="Client secret for the service principal"
          />
        </Box>
      )}

      {authMethod === 'service_account' && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <TextField
            label="Tenant ID"
            value={value.tenant_id || ''}
            onChange={(e) => handleFieldChange('tenant_id', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            size="small"
            helperText="Azure AD tenant ID"
          />
          <TextField
            label="Client ID"
            value={value.client_id || ''}
            onChange={(e) => handleFieldChange('client_id', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            size="small"
            helperText="Azure AD application Client ID (with delegated permissions)"
          />
          <TextField
            label="Username (UPN)"
            value={value.username || ''}
            onChange={(e) => handleFieldChange('username', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            size="small"
            helperText="Service account email/UPN (e.g. user@domain.com)"
          />
          <TextField
            label="Password"
            value={value.password || ''}
            onChange={(e) => handleFieldChange('password', e.target.value)}
            disabled={disabled}
            required
            type="password"
            fullWidth
            size="small"
            helperText="Service account password"
          />
        </Box>
      )}

      {authMethod === 'user_oauth' && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <Alert severity="info" variant="outlined">
            <Typography variant="caption">
              Paste a Power BI access token (e.g. from{' '}
              <a
                href="https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result?tryIt=true"
                target="_blank"
                rel="noopener noreferrer"
              >
                Microsoft&apos;s API docs (Try It)
              </a>
              ). For an interactive sign-in, use API mode&apos;s Power BI configuration.
            </Typography>
          </Alert>
          <TextField
            label="Access Token"
            value={value.access_token || ''}
            onChange={(e) => handleFieldChange('access_token', e.target.value)}
            disabled={disabled}
            required
            type="password"
            fullWidth
            size="small"
            helperText="Bearer token for the Power BI REST API"
          />
        </Box>
      )}
    </Box>
  );
};
