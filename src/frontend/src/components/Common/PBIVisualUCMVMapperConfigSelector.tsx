/**
 * PBI Visual-UCMV Mapper Configuration Selector Component (Tool 94)
 *
 * Configures the LLM-based mapping of Power BI report visuals to deployed
 * UC Metric View metric views. All heavy inputs (report_references_json and
 * ucmv_output) are flow-injected — this UI only needs catalog/schema/model.
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

export interface PBIVisualUCMVMapperConfig {
  catalog?: string;
  schema_name?: string;
  dashboard_title?: string;
  databricks_host?: string;
  llm_model?: string;
  report_references_override?: string;
  [key: string]: string | undefined;
}

interface WarehouseOption { id: string; name: string; state: string; }

interface PBIVisualUCMVMapperConfigSelectorProps {
  value: PBIVisualUCMVMapperConfig;
  onChange: (config: PBIVisualUCMVMapperConfig) => void;
  disabled?: boolean;
}

export const PBIVisualUCMVMapperConfigSelector: React.FC<PBIVisualUCMVMapperConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false,
}) => {
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

  const handleField = (field: keyof PBIVisualUCMVMapperConfig, val: string) => {
    onChange({ ...value, [field]: val });
  };

  const handleConnect = async () => {
    setConnectLoading(true);
    setConnectError(null);
    const host = value.databricks_host || undefined;
    try {
      const cats = await DatabricksService.listCatalogs(host);
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
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── Catalog & Schema ─────────────────────────────────────────────── */}
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
          {(catalogs.length === 0 || schemas.length === 0) && (
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
              Connect to workspace above to auto-populate dropdowns.
            </Typography>
          )}
        </AccordionDetails>
      </Accordion>

      {/* ── Dashboard Title ──────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Dashboard</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <TextField
            label="Dashboard Title"
            value={value.dashboard_title || ''}
            onChange={(e) => handleField('dashboard_title', e.target.value)}
            disabled={disabled} fullWidth size="small"
            helperText="Display name for the resulting Lakeview dashboard"
            placeholder="Production Dashboard"
          />
        </AccordionDetails>
      </Accordion>

      {/* ── LLM Settings ────────────────────────────────────────────────── */}
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
            helperText="Databricks model endpoint used to map PBI measures to UCMV measures"
          />
        </AccordionDetails>
      </Accordion>

      {/* ── Report References Override ───────────────────────────────────── */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            PBI Report References Override (optional — skips tool 78)
            {value.report_references_override && (
              <Typography component="span" variant="caption" color="primary" sx={{ ml: 1 }}>✓ loaded</Typography>
            )}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Upload the JSON output from tool 78 (Power BI Report References) to skip live PBI extraction.
              Download it from the HITL gate after tool 78 runs, edit if needed, then upload here.
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
                  reader.onload = (ev) => {
                    try {
                      const parsed = JSON.parse(ev.target?.result as string);
                      onChange({ ...value, report_references_override: JSON.stringify(parsed) });
                      setOverrideSuccess(`Loaded "${file.name}" — will skip tool 78`);
                      setOverrideError(null);
                    } catch { setOverrideError('Invalid JSON file'); }
                  };
                  reader.readAsText(file);
                  e.target.value = '';
                }} />
              </Button>
            )}
            {overrideTab === 1 && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                <TextField label="Paste tool 78 JSON output" value={overridePaste}
                  onChange={(e) => setOverridePaste(e.target.value)}
                  disabled={disabled} fullWidth multiline minRows={4} size="small"
                  placeholder={'{\n  "reports": [...]\n}'} />
                <Button variant="contained" size="small" disabled={disabled || !overridePaste.trim()}
                  sx={{ alignSelf: 'flex-start' }}
                  onClick={() => {
                    try {
                      const parsed = JSON.parse(overridePaste);
                      onChange({ ...value, report_references_override: JSON.stringify(parsed) });
                      setOverrideSuccess('Tool 78 output loaded — will skip live PBI extraction');
                      setOverridePaste('');
                      setOverrideError(null);
                    } catch { setOverrideError('Invalid JSON'); }
                  }}>Apply</Button>
              </Box>
            )}
            {overrideSuccess && <Alert severity="success" variant="outlined">{overrideSuccess}</Alert>}
            {overrideError && <Alert severity="error" variant="outlined">{overrideError}</Alert>}
            {value.report_references_override && (
              <Button size="small" color="warning" onClick={() => {
                onChange({ ...value, report_references_override: undefined });
                setOverrideSuccess(null);
              }}>Clear override (re-enable tool 78)</Button>
            )}
          </Box>
        </AccordionDetails>
      </Accordion>

      <Alert severity="info" variant="outlined">
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>How it works:</Typography>
        <Typography variant="caption">
          This tool reads the PBI report references (auto-injected from tool 78) and the deployed
          UC Metric View definitions (auto-injected from the previous flow step). It uses an LLM
          to semantically match each visual's Power BI measures to UCMV SQL measures and generates
          Databricks SQL with MEASURE() syntax. Call with ZERO arguments — all data is flow-injected.
        </Typography>
      </Alert>
    </Box>
  );
};
