/**
 * Pipeline Config Generator Configuration Selector Component
 *
 * Provides configuration UI for the Pipeline Config Generator tool (Tool 90).
 * Calls 4 PBI APIs directly — no LLM intermediation.
 * Requires two Service Principals: non-admin (Execute Queries) and admin (Admin Scanner).
 */

import React, { useState } from 'react';
import {
  Box,
  Typography,
  TextField,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  ToggleButtonGroup,
  ToggleButton,
  Switch,
  FormControlLabel,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  CircularProgress
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { DatabricksService } from '../../api/DatabricksService';

interface WarehouseOption { id: string; name: string; state: string; }

export type PipelineAuthMethod = 'service_principal' | 'service_account';

export interface PipelineConfigGeneratorConfig {
  // PBI Configuration
  workspace_id?: string;
  dataset_id?: string;
  report_id?: string;
  // Auth method — a single choice applies to BOTH credential sets (matches the
  // backend, which auto-detects SP vs SA per set but the UI keeps it simple).
  auth_method?: PipelineAuthMethod;
  // Non-Admin credentials (Execute Queries API)
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;   // Service Principal
  username?: string;        // Service Account
  password?: string;        // Service Account
  // Admin credentials (Admin Scanner API)
  admin_client_id?: string;
  admin_client_secret?: string;   // Service Principal
  admin_username?: string;        // Service Account
  admin_password?: string;        // Service Account
  // Target
  catalog?: string;
  schema_name?: string;
  // Optional warehouse + LLM enrichment (opt-in). When enabled + a warehouse is
  // chosen, config-gen runs SELECT DISTINCT to fill flag-column filter_sets and,
  // for cross-fact merges, one LLM call to draft fact_join_map. Slower / tokens.
  enable_enrichment?: boolean;
  warehouse_id?: string;
  databricks_host?: string;
  // Index signature for compatibility (boolean added for enable_enrichment)
  [key: string]: string | boolean | undefined;
}

interface PipelineConfigGeneratorConfigSelectorProps {
  value: PipelineConfigGeneratorConfig;
  onChange: (config: PipelineConfigGeneratorConfig) => void;
  disabled?: boolean;
}

export const PipelineConfigGeneratorConfigSelector: React.FC<PipelineConfigGeneratorConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false
}) => {
  const handleFieldChange = (field: keyof PipelineConfigGeneratorConfig, fieldValue: string) => {
    onChange({
      ...value,
      [field]: fieldValue
    });
  };

  // A single auth_method applies to both credential sets. Default to Service
  // Principal to preserve prior behaviour when unset.
  const authMethod: PipelineAuthMethod = value.auth_method || 'service_principal';
  const isSA = authMethod === 'service_account';

  const handleAuthMethodChange = (
    _e: React.MouseEvent<HTMLElement>,
    newMethod: PipelineAuthMethod | null,
  ) => {
    if (!newMethod) return;  // ignore de-select
    const updated: PipelineConfigGeneratorConfig = { ...value, auth_method: newMethod };
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

  // ── Optional warehouse + LLM enrichment ──
  const enrichEnabled = value.enable_enrichment === true;
  const [warehouses, setWarehouses] = useState<WarehouseOption[]>([]);
  const [connectLoading, setConnectLoading] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);

  const handleEnrichmentToggle = (_e: React.ChangeEvent<HTMLInputElement>, checked: boolean) => {
    const updated: PipelineConfigGeneratorConfig = { ...value, enable_enrichment: checked };
    if (!checked) {
      // Turning enrichment off clears the warehouse so a stale id can't trigger
      // the backend warehouse/LLM pass (defense-in-depth alongside the backend gate).
      updated.warehouse_id = undefined;
    }
    onChange(updated);
  };

  const handleConnect = async () => {
    setConnectLoading(true);
    setConnectError(null);
    const host = value.databricks_host || undefined;
    try {
      setWarehouses(await DatabricksService.listWarehouses(host));
    } catch (err) {
      setConnectError(err instanceof Error ? err.message : 'Connection failed');
    } finally {
      setConnectLoading(false);
    }
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Info note */}
      <Alert severity="info" variant="outlined">
        <Typography variant="caption">
          This tool calls 4 Power BI APIs directly to generate <code>pipeline_config.json</code> with
          all 26 config keys. No LLM intermediation &mdash; no data truncation.
          Requires two credential sets (non-admin + admin), each a Service Principal
          or a Service Account (selected below).
        </Typography>
      </Alert>

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

      {/* Target Configuration */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(76, 175, 80)' }}>
        Target Configuration
      </Typography>
      <Box sx={{ display: 'flex', gap: 2 }}>
        <TextField
          label="Target Catalog"
          value={value.catalog || ''}
          onChange={(e) => handleFieldChange('catalog', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          helperText="Unity Catalog name (default: main)"
        />
        <TextField
          label="Target Schema"
          value={value.schema_name || ''}
          onChange={(e) => handleFieldChange('schema_name', e.target.value)}
          disabled={disabled}
          fullWidth
          size="small"
          helperText="UC Schema name (default: default)"
        />
      </Box>

      {/* Optional: Warehouse + LLM enrichment */}
      <FormControlLabel
        sx={{ mt: 1 }}
        control={
          <Switch
            checked={enrichEnabled}
            onChange={handleEnrichmentToggle}
            disabled={disabled}
            color="warning"
          />
        }
        label={
          <Box>
            <Typography variant="body2" sx={{ fontWeight: 600 }}>
              Warehouse + LLM enrichment (optional)
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Runs SQL against your warehouse to resolve flag-column filter values and, for
              cross-fact merges, one LLM call to draft join strategy. Slower and consumes tokens.
              Leave off for the fast, deterministic, LLM-free default.
            </Typography>
          </Box>
        }
      />
      {enrichEnabled && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, pl: 4 }}>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
            <TextField
              label="Workspace Host (optional)"
              value={value.databricks_host || ''}
              onChange={(e) => handleFieldChange('databricks_host', e.target.value)}
              disabled={disabled}
              fullWidth
              size="small"
              helperText="Defaults to the authenticated workspace"
            />
            <Button
              variant="outlined"
              size="small"
              onClick={handleConnect}
              disabled={disabled || connectLoading}
              sx={{ mt: 0.5, whiteSpace: 'nowrap' }}
            >
              {connectLoading ? <CircularProgress size={18} /> : 'Connect'}
            </Button>
          </Box>
          <FormControl fullWidth size="small">
            <InputLabel>SQL Warehouse</InputLabel>
            <Select
              label="SQL Warehouse"
              value={value.warehouse_id || ''}
              onChange={(e) => handleFieldChange('warehouse_id', e.target.value)}
              disabled={disabled}
            >
              {warehouses.length === 0 && value.warehouse_id && (
                <MenuItem value={value.warehouse_id}>{value.warehouse_id}</MenuItem>
              )}
              {warehouses.map((w) => (
                <MenuItem key={w.id} value={w.id}>{w.name} ({w.id})</MenuItem>
              ))}
            </Select>
          </FormControl>
          {connectError && (
            <Alert severity="error" variant="outlined">{connectError}</Alert>
          )}
        </Box>
      )}

      {/* Optional: Report ID */}
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">Optional: Report Metadata</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <TextField
            label="Report ID"
            value={value.report_id || ''}
            onChange={(e) => handleFieldChange('report_id', e.target.value)}
            disabled={disabled}
            fullWidth
            size="small"
            helperText="PBIR Report GUID for visual display names and dimension ordering (optional)"
          />
        </AccordionDetails>
      </Accordion>

      {/* Info about what the tool does */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption" component="div">
          Calls 4 PBI APIs directly and produces all 26 pipeline_config keys:
        </Typography>
        <Typography variant="caption" component="div" sx={{ mt: 0.5 }}>
          <strong>API 1</strong>: INFO.VIEW.RELATIONSHIPS() &rarr; join_key_map, enrichment_joins, dim_alias_map<br />
          <strong>API 2</strong>: $SYSTEM.MDSCHEMA_MEASURES &rarr; switch_decompositions, filter_sets, measure_resolutions<br />
          <strong>API 3</strong>: Admin Scanner &rarr; column_metadata, dimension_exclusions, period_dim_priority<br />
          <strong>API 4</strong>: Report Definition (optional) &rarr; measure_metadata, dimension_metadata
        </Typography>
      </Alert>
    </Box>
  );
};
