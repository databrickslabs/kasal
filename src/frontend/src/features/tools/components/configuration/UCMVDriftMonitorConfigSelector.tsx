/**
 * UCMV Drift Monitor Configuration Selector (Tool 98).
 *
 * Input form for the drift check: which DEPLOYED metric views to monitor (1..N
 * views around one report), the SQL warehouse used to read their definitions, and
 * the same Power BI connection the Pipeline Config Generator uses to extract
 * today's model.
 */

import React, { useState } from 'react';
import {
  Box,
  Typography,
  TextField,
  Alert,
  Switch,
  FormControlLabel,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  CircularProgress,
} from '@mui/material';
import { DatabricksService } from '../../../../api/databricks/DatabricksService';
import {
  PowerBIPipelineCredentialFields,
  type PowerBIPipelineCredentials,
} from './PowerBIPipelineCredentialFields';

interface WarehouseOption { id: string; name: string; state: string; }

export interface UCMVDriftMonitorConfig extends PowerBIPipelineCredentials {
  /** catalog.schema.view names (or Catalog Explorer URLs), one per line. */
  ucmv_names?: string;
  warehouse_id?: string;
  databricks_host?: string;
  report_id?: string;
  llm_compare_legacy?: boolean;
  apply_suspected_changes?: boolean;
  llm_model?: string;
  [key: string]: string | boolean | undefined;
}

interface Props {
  value: UCMVDriftMonitorConfig;
  onChange: (config: UCMVDriftMonitorConfig) => void;
  disabled?: boolean;
}

const VIEW_NAME_RE = /^\s*[^.\s]+\.[^.\s]+\.[^.\s]+\s*$/;

/** Lines of the views field that are neither a 3-part name nor a Catalog Explorer URL. */
// eslint-disable-next-line react-refresh/only-export-components
export function invalidViewLines(text: string | undefined): string[] {
  return (text || '')
    .split(/[\n,;]+/)
    .map((l) => l.trim())
    .filter((l) => l && !VIEW_NAME_RE.test(l) && !/\/explore\/data\/[^/]+\/[^/]+\/[^/?#]+/.test(l));
}

export const UCMVDriftMonitorConfigSelector: React.FC<Props> = ({
  value = {},
  onChange,
  disabled = false,
}) => {
  const set = (field: keyof UCMVDriftMonitorConfig, v: string | boolean) =>
    onChange({ ...value, [field]: v });

  const [warehouses, setWarehouses] = useState<WarehouseOption[]>([]);
  const [connectLoading, setConnectLoading] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);

  const handleConnect = async () => {
    setConnectLoading(true);
    setConnectError(null);
    try {
      setWarehouses(await DatabricksService.listWarehouses(value.databricks_host || undefined));
    } catch (err) {
      setConnectError(err instanceof Error ? err.message : 'Connection failed');
    } finally {
      setConnectLoading(false);
    }
  };

  const invalid = invalidViewLines(value.ucmv_names);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Alert severity="info" variant="outlined">
        <Typography variant="caption">
          Compares the <strong>deployed</strong> metric views below with the <strong>current</strong> Power BI
          model. The deployed YAML is treated as the verified baseline: the proposal only adds new
          measures and re-translates measures whose DAX changed &mdash; nothing else is modified.
          Read-only: deploy the reviewed proposal with the “4 · Deploy Metric Views” crew.
        </Typography>
      </Alert>

      {/* Monitored metric views */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(25, 118, 210)' }}>
        Metric Views to Monitor
      </Typography>
      <TextField
        label="Deployed metric views"
        value={value.ucmv_names || ''}
        onChange={(e) => set('ucmv_names', e.target.value)}
        disabled={disabled}
        fullWidth
        size="small"
        required
        multiline
        minRows={3}
        placeholder={'catalog.schema.mv_sales\ncatalog.schema.mv_inventory'}
        error={invalid.length > 0}
        helperText={
          invalid.length > 0
            ? `Not a catalog.schema.view name: ${invalid.join(', ')}`
            : 'One per line — all views generated from the same Power BI report. Catalog Explorer URLs work too.'
        }
      />

      {/* Databricks connection */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(76, 175, 80)' }}>
        Databricks (reads the deployed definitions)
      </Typography>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
        <TextField
          label="Workspace Host (optional)"
          value={value.databricks_host || ''}
          onChange={(e) => set('databricks_host', e.target.value)}
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
      <FormControl fullWidth size="small" required>
        <InputLabel>SQL Warehouse</InputLabel>
        <Select
          label="SQL Warehouse"
          value={value.warehouse_id || ''}
          onChange={(e) => set('warehouse_id', e.target.value)}
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
      {connectError && <Alert severity="error" variant="outlined">{connectError}</Alert>}

      {/* Power BI — same extraction as the Pipeline Config Generator */}
      <PowerBIPipelineCredentialFields value={value} onChange={onChange} disabled={disabled} />
      <TextField
        label="Report ID (optional)"
        value={value.report_id || ''}
        onChange={(e) => set('report_id', e.target.value)}
        disabled={disabled}
        fullWidth
        size="small"
        helperText="Improves measure DAX quality; auto-discovered from the dataset when empty"
      />

      {/* Options */}
      <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Change detection</Typography>
      <FormControlLabel
        control={
          <Switch
            checked={value.llm_compare_legacy !== false}
            onChange={(_e, checked) => set('llm_compare_legacy', checked)}
            disabled={disabled}
          />
        }
        label={
          <Box>
            <Typography variant="body2" sx={{ fontWeight: 600 }}>
              LLM check for views generated before DAX fingerprinting
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Newer views record a fingerprint of their source DAX, so changes are detected exactly.
              Older views are compared by an LLM (batched; consumes tokens). Off = only new/removed
              measures are detected for older views.
            </Typography>
          </Box>
        }
      />
      <FormControlLabel
        control={
          <Switch
            checked={value.apply_suspected_changes === true}
            onChange={(_e, checked) => set('apply_suspected_changes', checked)}
            disabled={disabled}
            color="warning"
          />
        }
        label={
          <Box>
            <Typography variant="body2" sx={{ fontWeight: 600 }}>
              Also patch LLM-suspected changes
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Off (recommended): a measure the LLM only suspects has changed is shown with its
              suggested SQL, but the verified measure stays in the proposal unchanged.
            </Typography>
          </Box>
        }
      />
      <TextField
        label="LLM model"
        value={value.llm_model || ''}
        onChange={(e) => set('llm_model', e.target.value)}
        disabled={disabled}
        fullWidth
        size="small"
        placeholder="databricks-claude-sonnet-4-5"
        helperText="Used for DAX→SQL translation of new/changed measures and the legacy comparison"
      />
    </Box>
  );
};
