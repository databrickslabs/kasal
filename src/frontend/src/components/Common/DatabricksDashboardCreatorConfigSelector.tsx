/**
 * Databricks Dashboard Creator Configuration Selector Component (Tool 95)
 *
 * Configures the creation of Databricks AI/BI (Lakeview) dashboards from
 * visual-to-UCMV mappings. The visual_mappings_json is auto-injected from
 * the previous flow step (tool 94).
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

export interface DatabricksDashboardCreatorConfig {
  dashboard_title?: string;
  catalog?: string;
  schema_name?: string;
  warehouse_id?: string;
  databricks_host?: string;
  parent_path?: string;
  publish_dashboard?: boolean | string;
  [key: string]: string | boolean | undefined;
}

interface WarehouseOption { id: string; name: string; state: string; }

interface DatabricksDashboardCreatorConfigSelectorProps {
  value: DatabricksDashboardCreatorConfig;
  onChange: (config: DatabricksDashboardCreatorConfig) => void;
  disabled?: boolean;
}

export const DatabricksDashboardCreatorConfigSelector: React.FC<DatabricksDashboardCreatorConfigSelectorProps> = ({
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

  const handleField = (field: keyof DatabricksDashboardCreatorConfig, val: string | boolean) => {
    onChange({ ...value, [field]: val });
  };

  const handleConnect = async () => {
    setConnectLoading(true);
    setConnectError(null);
    const host = value.databricks_host as string | undefined || undefined;
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
    onChange({ ...value, catalog, schema_name: '' });
    setSchemas([]);
    if (!catalog) return;
    setSchemaLoading(true);
    try {
      setSchemas(await DatabricksService.listSchemas(catalog, value.databricks_host as string | undefined || undefined));
    } catch { /* non-fatal */ }
    finally { setSchemaLoading(false); }
  };

  const publishChecked = value.publish_dashboard === true || value.publish_dashboard === 'true';

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* ── Connection ───────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Connection</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
              <TextField
                label="Workspace URL (optional)"
                value={value.databricks_host as string || ''}
                onChange={(e) => handleField('databricks_host', e.target.value)}
                disabled={disabled}
                fullWidth size="small"
                helperText="Leave blank to use Kasal Settings / DATABRICKS_HOST."
                placeholder="https://adb-123456789.7.azuredatabricks.net"
              />
              <Button variant="contained" size="small" onClick={handleConnect}
                disabled={disabled || connectLoading} sx={{ mt: 0.5, minWidth: 100, whiteSpace: 'nowrap' }}>
                {connectLoading ? <CircularProgress size={16} color="inherit" /> : 'Connect'}
              </Button>
            </Box>
            {connectError && <Alert severity="error" variant="outlined">{connectError}</Alert>}
            <FormControl fullWidth size="small">
              <InputLabel>Warehouse *</InputLabel>
              <Select label="Warehouse *" value={value.warehouse_id as string || ''}
                onChange={(e) => handleField('warehouse_id', e.target.value)} disabled={disabled}>
                {warehouses.length === 0 && value.warehouse_id && (
                  <MenuItem value={value.warehouse_id as string}>{value.warehouse_id as string}</MenuItem>
                )}
                {warehouses.map((w) => (
                  <MenuItem key={w.id} value={w.id}>{w.name} ({w.id})</MenuItem>
                ))}
              </Select>
            </FormControl>
            {warehouses.length === 0 && !value.warehouse_id && (
              <Typography variant="caption" color="text.secondary">
                Connect to populate the warehouse list, or type the warehouse ID manually after connecting.
              </Typography>
            )}
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── Dashboard Identity ───────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Dashboard Identity</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Dashboard Title"
              value={value.dashboard_title as string || ''}
              onChange={(e) => handleField('dashboard_title', e.target.value)}
              disabled={disabled} fullWidth size="small" required
              helperText="Display name for the Lakeview dashboard"
              placeholder="Production Dashboard"
            />
            <TextField
              label="Parent Path"
              value={value.parent_path as string || '/Workspace/Shared'}
              onChange={(e) => handleField('parent_path', e.target.value)}
              disabled={disabled} fullWidth size="small"
              helperText="Workspace folder path where the dashboard will be created"
              placeholder="/Workspace/Shared"
            />
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── Catalog & Schema ─────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Catalog &amp; Schema</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <FormControl fullWidth size="small">
              <InputLabel>Catalog</InputLabel>
              <Select label="Catalog" value={value.catalog as string || ''}
                onChange={(e) => handleCatalogChange(e.target.value)} disabled={disabled}
                onClose={() => setCatalogSearch('')} MenuProps={{ autoFocus: false }}>
                <MenuItem disableRipple onKeyDown={(e) => e.stopPropagation()}
                  sx={{ p: 1, '&:hover': { background: 'transparent' }, cursor: 'default' }}>
                  <TextField size="small" fullWidth placeholder="Search catalogs…"
                    value={catalogSearch} onChange={(e) => setCatalogSearch(e.target.value)}
                    autoFocus onClick={(e) => e.stopPropagation()} />
                </MenuItem>
                {catalogs.length === 0 && value.catalog && !catalogSearch && (
                  <MenuItem value={value.catalog as string}>{value.catalog as string}</MenuItem>
                )}
                {(catalogSearch ? catalogs.filter(c => c.toLowerCase().includes(catalogSearch.toLowerCase())) : catalogs)
                  .map(c => <MenuItem key={c} value={c}>{c}</MenuItem>)}
              </Select>
            </FormControl>
            <FormControl fullWidth size="small">
              <InputLabel>Schema</InputLabel>
              <Select label="Schema" value={value.schema_name as string || ''}
                onChange={(e) => handleField('schema_name', e.target.value)}
                disabled={disabled || schemaLoading}
                startAdornment={schemaLoading ? <CircularProgress size={14} sx={{ mr: 1 }} /> : undefined}
                onClose={() => setSchemaSearch('')} MenuProps={{ autoFocus: false }}>
                <MenuItem disableRipple onKeyDown={(e) => e.stopPropagation()}
                  sx={{ p: 1, '&:hover': { background: 'transparent' }, cursor: 'default' }}>
                  <TextField size="small" fullWidth placeholder="Search schemas…"
                    value={schemaSearch} onChange={(e) => setSchemaSearch(e.target.value)}
                    autoFocus onClick={(e) => e.stopPropagation()} />
                </MenuItem>
                {schemas.length === 0 && value.schema_name && !schemaSearch && (
                  <MenuItem value={value.schema_name as string}>{value.schema_name as string}</MenuItem>
                )}
                {(schemaSearch ? schemas.filter(s => s.toLowerCase().includes(schemaSearch.toLowerCase())) : schemas)
                  .map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
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

      {/* ── Options ──────────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <FormControlLabel
            control={
              <Switch
                checked={publishChecked}
                onChange={(e) => handleField('publish_dashboard', e.target.checked)}
                disabled={disabled}
              />
            }
            label={
              <Box>
                <Typography variant="body2">Publish dashboard after creation</Typography>
                <Typography variant="caption" color="text.secondary">
                  When enabled, embeds credentials so users without direct data access can view the dashboard.
                </Typography>
              </Box>
            }
          />
        </AccordionDetails>
      </Accordion>

      <Alert severity="info" variant="outlined">
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>How it works:</Typography>
        <Typography variant="caption">
          This tool reads the visual-to-UCMV mappings (auto-injected from tool 94 via flow injection)
          and creates a Databricks AI/BI dashboard with correct widget types per visual type
          (bar, line, table, counter). Call with ZERO arguments — all mapping data is flow-injected.
          Configure warehouse_id, dashboard_title, and optionally catalog/schema for context.
        </Typography>
      </Alert>
    </Box>
  );
};
