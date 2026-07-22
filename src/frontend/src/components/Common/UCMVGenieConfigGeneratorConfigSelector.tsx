/**
 * UCMV Genie Space Config Generator Configuration Selector (Tool 93)
 *
 * Configures the auto-generation of Genie Space configuration from
 * deployed UC Metric Views. The tool uses an LLM to produce instructions,
 * sample questions, and example SQLs from the UCMV definitions.
 *
 * If genie_config_override is provided, the LLM step is skipped.
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
  Tabs,
  Tab,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { DatabricksService } from '../../api/DatabricksService';

export interface UCMVGenieConfigGeneratorConfig {
  space_title?: string;
  catalog?: string;
  schema_name?: string;
  warehouse_id?: string;
  databricks_host?: string;
  llm_model?: string;
  genie_config_override?: string;
  [key: string]: string | undefined;
}

interface WarehouseOption { id: string; name: string; state: string; }

interface UCMVGenieConfigGeneratorConfigSelectorProps {
  value: UCMVGenieConfigGeneratorConfig;
  onChange: (config: UCMVGenieConfigGeneratorConfig) => void;
  disabled?: boolean;
}

export const UCMVGenieConfigGeneratorConfigSelector: React.FC<UCMVGenieConfigGeneratorConfigSelectorProps> = ({
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
  const [overrideTab, setOverrideTab] = useState(0);
  const [overridePaste, setOverridePaste] = useState('');
  const [overrideError, setOverrideError] = useState<string | null>(null);
  const [overrideSuccess, setOverrideSuccess] = useState<string | null>(null);

  const handleField = (field: keyof UCMVGenieConfigGeneratorConfig, val: string) => {
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
    onChange({ ...value, catalog, schema_name: '' });
    setSchemas([]);
    if (!catalog) return;
    setSchemaLoading(true);
    try {
      setSchemas(await DatabricksService.listSchemas(catalog, value.databricks_host || undefined));
    } catch { /* non-fatal */ }
    finally { setSchemaLoading(false); }
  };

  const applyOverride = (content: string) => {
    try {
      const parsed = JSON.parse(content);
      onChange({ ...value, genie_config_override: JSON.stringify(parsed) });
      setOverrideError(null);
      setOverrideSuccess('Config override loaded — LLM auto-generation will be skipped');
    } catch {
      setOverrideError('Invalid JSON');
    }
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* ── Connection ─────────────────────────────────────────────────────── */}
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
              <Select label="Warehouse *" value={value.warehouse_id || ''}
                onChange={(e) => handleField('warehouse_id', e.target.value)} disabled={disabled}>
                {warehouses.length === 0 && value.warehouse_id && (
                  <MenuItem value={value.warehouse_id}>{value.warehouse_id}</MenuItem>
                )}
                {warehouses.map((w) => <MenuItem key={w.id} value={w.id}>{w.name} ({w.id})</MenuItem>)}
              </Select>
            </FormControl>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── Space Identity ─────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Space Identity</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <TextField
            label="Space Title"
            value={value.space_title || ''}
            onChange={(e) => handleField('space_title', e.target.value)}
            disabled={disabled} required fullWidth size="small"
            helperText="Display name for the Genie space"
          />
        </AccordionDetails>
      </Accordion>

      {/* ── Catalog & Schema ───────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Catalog &amp; Schema (where UCMVs are deployed)</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <FormControl fullWidth size="small">
              <InputLabel>Catalog *</InputLabel>
              <Select label="Catalog *" value={value.catalog || ''}
                onChange={(e) => handleCatalogChange(e.target.value)} disabled={disabled}
                onClose={() => setCatalogSearch('')} MenuProps={{ autoFocus: false }}>
                <MenuItem disableRipple onKeyDown={(e) => e.stopPropagation()}
                  sx={{ p: 1, '&:hover': { background: 'transparent' }, cursor: 'default' }}>
                  <TextField size="small" fullWidth placeholder="Search catalogs…"
                    value={catalogSearch} onChange={(e) => setCatalogSearch(e.target.value)}
                    autoFocus onClick={(e) => e.stopPropagation()} />
                </MenuItem>
                {catalogs.length === 0 && value.catalog && !catalogSearch && (
                  <MenuItem value={value.catalog}>{value.catalog}</MenuItem>
                )}
                {(catalogSearch ? catalogs.filter(c => c.toLowerCase().includes(catalogSearch.toLowerCase())) : catalogs)
                  .map(c => <MenuItem key={c} value={c}>{c}</MenuItem>)}
              </Select>
            </FormControl>
            <FormControl fullWidth size="small">
              <InputLabel>Schema *</InputLabel>
              <Select label="Schema *" value={value.schema_name || ''}
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
                  <MenuItem value={value.schema_name}>{value.schema_name}</MenuItem>
                )}
                {(schemaSearch ? schemas.filter(s => s.toLowerCase().includes(schemaSearch.toLowerCase())) : schemas)
                  .map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
              </Select>
            </FormControl>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── LLM Model ──────────────────────────────────────────────────────── */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>LLM Settings (optional)</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <TextField
            label="LLM Model"
            value={value.llm_model || 'databricks-claude-sonnet-4'}
            onChange={(e) => handleField('llm_model', e.target.value)}
            disabled={disabled} fullWidth size="small"
            helperText="Databricks model endpoint to use for config generation"
          />
        </AccordionDetails>
      </Accordion>

      {/* ── Manual Override ────────────────────────────────────────────────── */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Manual Config Override (optional — skips LLM generation)
            {value.genie_config_override && (
              <Typography component="span" variant="caption" color="primary" sx={{ ml: 1 }}>✓ loaded</Typography>
            )}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Upload a GenieSpaceConfig JSON (e.g. from <code>genie_space_config_example_iom004.json</code>)
              to skip LLM auto-generation and use your own instructions/questions/SQLs.
            </Typography>
            <Tabs value={overrideTab} onChange={(_, v) => { setOverrideTab(v); setOverrideError(null); setOverrideSuccess(null); }}>
              <Tab label="Upload JSON file" />
              <Tab label="Paste JSON" />
            </Tabs>
            {overrideTab === 0 && (
              <Button variant="outlined" component="label" size="small" disabled={disabled}>
                Choose JSON file
                <input type="file" accept=".json,application/json" hidden onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (!file) return;
                  const reader = new FileReader();
                  reader.onload = (ev) => { applyOverride(ev.target?.result as string); };
                  reader.readAsText(file);
                  e.target.value = '';
                }} />
              </Button>
            )}
            {overrideTab === 1 && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                <TextField label="Paste GenieSpaceConfig JSON" value={overridePaste}
                  onChange={(e) => setOverridePaste(e.target.value)}
                  disabled={disabled} fullWidth multiline minRows={4} size="small"
                  placeholder={'{\n  "space_title": "...",\n  "text_instructions": "...",\n  ...\n}'} />
                <Button variant="contained" size="small" onClick={() => applyOverride(overridePaste)}
                  disabled={disabled || !overridePaste.trim()} sx={{ alignSelf: 'flex-start' }}>
                  Apply
                </Button>
              </Box>
            )}
            {overrideSuccess && <Alert severity="success" variant="outlined">{overrideSuccess}</Alert>}
            {overrideError && <Alert severity="error" variant="outlined">{overrideError}</Alert>}
            {value.genie_config_override && (
              <Button size="small" color="warning" onClick={() => {
                onChange({ ...value, genie_config_override: undefined });
                setOverrideSuccess(null);
              }}>
                Clear override (re-enable LLM generation)
              </Button>
            )}
          </Box>
        </AccordionDetails>
      </Accordion>

      <Alert severity="info" variant="outlined">
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>How it works:</Typography>
        <Typography variant="caption">
          The tool reads the deployed metric view YAMLs from the previous flow step,
          then uses an LLM to generate business-friendly instructions, sample questions,
          and example SQL queries with MEASURE() syntax. The output is automatically
          passed to the Genie Space Generator in the next step.
        </Typography>
      </Alert>
    </Box>
  );
};
