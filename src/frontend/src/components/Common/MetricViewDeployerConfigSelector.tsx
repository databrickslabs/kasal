/**
 * Metric View Deployer Configuration Selector (Tool 88)
 *
 * Configures warehouse, catalog/schema, and dry_run toggle for the
 * MetricViewDeployerTool. YAML/SQL specs are auto-injected from the
 * UC Metric View Generator output — no manual input needed.
 */

import React, { useState } from 'react';
import {
  Box,
  Typography,
  TextField,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Button,
  Alert,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  CircularProgress,
  Switch,
  FormControlLabel,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { DatabricksService } from '../../api/DatabricksService';

export interface MetricViewDeployerConfig {
  databricks_host?: string;
  warehouse_id?: string;
  catalog?: string;
  schema_name?: string;
  dry_run?: boolean;
  [key: string]: string | boolean | undefined;
}

interface WarehouseOption {
  id: string;
  name: string;
  state: string;
}

interface MetricViewDeployerConfigSelectorProps {
  value: MetricViewDeployerConfig;
  onChange: (config: MetricViewDeployerConfig) => void;
  disabled?: boolean;
}

export const MetricViewDeployerConfigSelector: React.FC<MetricViewDeployerConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false,
}) => {
  const [warehouses, setWarehouses] = useState<WarehouseOption[]>([]);
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [connectLoading, setConnectLoading] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [catalogSearch, setCatalogSearch] = useState('');
  const [schemaSearch, setSchemaSearch] = useState('');

  const handleField = (field: keyof MetricViewDeployerConfig, val: string | boolean) => {
    onChange({ ...value, [field]: val });
  };

  const handleConnect = async () => {
    setConnectLoading(true);
    setConnectError(null);
    const host = value.databricks_host || undefined;
    try {
      const [wh, cats] = await Promise.all([
        DatabricksService.listWarehouses(host),
        DatabricksService.listCatalogs(host),
      ]);
      setWarehouses(wh);
      setCatalogs(cats);
      setSchemas([]);
    } catch (err) {
      setConnectError(err instanceof Error ? err.message : 'Connection failed');
    } finally {
      setConnectLoading(false);
    }
  };

  const handleCatalogChange = async (catalog: string) => {
    handleField('catalog', catalog);
    handleField('schema_name', '');
    setSchemas([]);
    if (!catalog) return;
    setSchemaLoading(true);
    try {
      const host = value.databricks_host || undefined;
      setSchemas(await DatabricksService.listSchemas(catalog, host));
    } catch {
      // non-fatal
    } finally {
      setSchemaLoading(false);
    }
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* ── Connection ───────────────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Connection</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
              <TextField
                label="Workspace URL (optional)"
                value={value.databricks_host || ''}
                onChange={(e) => handleField('databricks_host', e.target.value)}
                disabled={disabled}
                fullWidth
                size="small"
                helperText="Leave blank to use Kasal Settings / DATABRICKS_HOST."
                placeholder="https://adb-123456789.7.azuredatabricks.net"
              />
              <Button
                variant="contained"
                size="small"
                onClick={handleConnect}
                disabled={disabled || connectLoading}
                sx={{ mt: 0.5, minWidth: 100, whiteSpace: 'nowrap' }}
              >
                {connectLoading ? <CircularProgress size={16} color="inherit" /> : 'Connect'}
              </Button>
            </Box>
            {connectError && <Alert severity="error" variant="outlined">{connectError}</Alert>}
            <FormControl fullWidth size="small">
              <InputLabel>Warehouse *</InputLabel>
              <Select
                label="Warehouse *"
                value={value.warehouse_id || ''}
                onChange={(e) => handleField('warehouse_id', e.target.value)}
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
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── Catalog & Schema ─────────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Catalog &amp; Schema</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <FormControl fullWidth size="small">
              <InputLabel>Catalog *</InputLabel>
              <Select
                label="Catalog *"
                value={value.catalog || ''}
                onChange={(e) => handleCatalogChange(e.target.value)}
                disabled={disabled}
                onClose={() => setCatalogSearch('')}
                MenuProps={{ autoFocus: false }}
              >
                <MenuItem
                  disableRipple
                  onKeyDown={(e) => e.stopPropagation()}
                  sx={{ p: 1, '&:hover': { background: 'transparent' }, cursor: 'default' }}
                >
                  <TextField
                    size="small" fullWidth placeholder="Search catalogs…"
                    value={catalogSearch}
                    onChange={(e) => setCatalogSearch(e.target.value)}
                    autoFocus onClick={(e) => e.stopPropagation()}
                  />
                </MenuItem>
                {catalogs.length === 0 && value.catalog && !catalogSearch && (
                  <MenuItem value={value.catalog}>{value.catalog}</MenuItem>
                )}
                {(catalogSearch
                  ? catalogs.filter((c) => c.toLowerCase().includes(catalogSearch.toLowerCase()))
                  : catalogs
                ).map((c) => <MenuItem key={c} value={c}>{c}</MenuItem>)}
                {catalogs.length > 0 && catalogSearch &&
                  catalogs.filter((c) => c.toLowerCase().includes(catalogSearch.toLowerCase())).length === 0 && (
                  <MenuItem disabled>No matches</MenuItem>
                )}
              </Select>
            </FormControl>
            <FormControl fullWidth size="small">
              <InputLabel>Schema *</InputLabel>
              <Select
                label="Schema *"
                value={value.schema_name || ''}
                onChange={(e) => handleField('schema_name', e.target.value)}
                disabled={disabled || schemaLoading}
                startAdornment={schemaLoading ? <CircularProgress size={14} sx={{ mr: 1 }} /> : undefined}
                onClose={() => setSchemaSearch('')}
                MenuProps={{ autoFocus: false }}
              >
                <MenuItem
                  disableRipple
                  onKeyDown={(e) => e.stopPropagation()}
                  sx={{ p: 1, '&:hover': { background: 'transparent' }, cursor: 'default' }}
                >
                  <TextField
                    size="small" fullWidth placeholder="Search schemas…"
                    value={schemaSearch}
                    onChange={(e) => setSchemaSearch(e.target.value)}
                    autoFocus onClick={(e) => e.stopPropagation()}
                  />
                </MenuItem>
                {schemas.length === 0 && value.schema_name && !schemaSearch && (
                  <MenuItem value={value.schema_name}>{value.schema_name}</MenuItem>
                )}
                {(schemaSearch
                  ? schemas.filter((s) => s.toLowerCase().includes(schemaSearch.toLowerCase()))
                  : schemas
                ).map((s) => <MenuItem key={s} value={s}>{s}</MenuItem>)}
                {schemas.length > 0 && schemaSearch &&
                  schemas.filter((s) => s.toLowerCase().includes(schemaSearch.toLowerCase())).length === 0 && (
                  <MenuItem disabled>No matches</MenuItem>
                )}
              </Select>
            </FormControl>
          </Box>
          {(catalogs.length === 0 || schemas.length === 0) && (
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
              Connect to workspace above to auto-populate dropdowns.
            </Typography>
          )}
        </AccordionDetails>
      </Accordion>

      {/* ── Deployment Settings ───────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Deployment Settings</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={value.dry_run === true}
                  onChange={(e) => handleField('dry_run', e.target.checked)}
                  disabled={disabled}
                  color="warning"
                />
              }
              label={
                <Box>
                  <Typography variant="body2">Dry Run (validate only)</Typography>
                  <Typography variant="caption" color="text.secondary">
                    When on, metric views are validated but NOT deployed to Databricks.
                    Turn off to actually create/update the metric views.
                  </Typography>
                </Box>
              }
            />
            {value.dry_run !== true && (
              <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
                Deployment is <strong>enabled</strong> — metric views will be created or updated in
                catalog <strong>{value.catalog || '(not set)'}</strong>.
                {value.schema_name && <> schema <strong>{value.schema_name}</strong>.</>}
              </Alert>
            )}
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info box */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          How it works in the flow:
        </Typography>
        <Typography variant="caption">
          The YAML/SQL specs are automatically passed from the UC Metric View Generator output —
          no manual input needed. This tool deploys them to Databricks before the Genie Space
          Generator runs.
        </Typography>
      </Alert>
    </Box>
  );
};
