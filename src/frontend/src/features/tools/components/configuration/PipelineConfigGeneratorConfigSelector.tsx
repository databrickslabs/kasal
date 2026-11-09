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
import { DatabricksService } from '../../../../api/databricks/DatabricksService';
import {
  PowerBIPipelineCredentialFields,
  type PipelineAuthMethod,
} from './PowerBIPipelineCredentialFields';

export type { PipelineAuthMethod };

interface WarehouseOption { id: string; name: string; state: string; }

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

      <PowerBIPipelineCredentialFields value={value} onChange={onChange} disabled={disabled} />

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
