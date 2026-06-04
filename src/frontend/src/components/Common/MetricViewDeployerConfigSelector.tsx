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
  Tabs,
  Tab,
  Chip,
  IconButton,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import DeleteIcon from '@mui/icons-material/Delete';
import { DatabricksService } from '../../api/DatabricksService';

export interface MetricViewDeployerConfig {
  databricks_host?: string;
  warehouse_id?: string;
  catalog?: string;
  schema_name?: string;
  dry_run?: boolean;
  yaml_specs_json?: string;
  catalog_remap?: string;
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

  // ── Manual YAML override state ──────────────────────────────────────────────
  const [yamlTab, setYamlTab] = useState(0);
  const [yamlPaste, setYamlPaste] = useState('');
  const [yamlError, setYamlError] = useState<string | null>(null);
  const [yamlSuccess, setYamlSuccess] = useState<string | null>(null);
  const [uploadedFiles, setUploadedFiles] = useState<Record<string, string>>(() => {
    try { return value.yaml_specs_json ? JSON.parse(value.yaml_specs_json) : {}; } catch { return {}; }
  });

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
    onChange({ ...value, catalog, schema_name: '' });
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

  // ── YAML upload helpers ────────────────────────────────────────────────────

  const applyYamlSpecs = (specs: Record<string, string>) => {
    setUploadedFiles(specs);
    onChange({ ...value, yaml_specs_json: JSON.stringify(specs) });
    setYamlError(null);
    setYamlSuccess(`${Object.keys(specs).length} metric view(s) loaded`);
  };

  const handleYamlFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files || []);
    if (!files.length) return;
    const readers = files.map(file => new Promise<{ key: string; yaml: string }>((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const key = file.name.replace(/\.(yml|yaml)$/i, '');
        resolve({ key, yaml: e.target?.result as string });
      };
      reader.onerror = reject;
      reader.readAsText(file);
    }));
    Promise.all(readers).then(results => {
      const merged = { ...uploadedFiles };
      results.forEach(({ key, yaml }) => { merged[key] = yaml; });
      applyYamlSpecs(merged);
    }).catch(() => setYamlError('Failed to read one or more files'));
    event.target.value = '';
  };

  const handleYamlPasteApply = () => {
    try {
      const parsed = JSON.parse(yamlPaste);
      if (typeof parsed !== 'object' || Array.isArray(parsed)) {
        setYamlError('Expected a JSON object: { "table_key": "yaml string", ... }');
        return;
      }
      applyYamlSpecs(parsed as Record<string, string>);
      setYamlPaste('');
    } catch {
      setYamlError('Invalid JSON — expected { "table_key": "yaml string", ... }');
    }
  };

  const removeYamlEntry = (key: string) => {
    const updated = { ...uploadedFiles };
    delete updated[key];
    applyYamlSpecs(updated);
    if (Object.keys(updated).length === 0) setYamlSuccess(null);
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

      {/* ── Manual YAML Override ─────────────────────────────────────────────────── */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Manual YAML Override
            {Object.keys(uploadedFiles).length > 0 && (
              <Chip label={`${Object.keys(uploadedFiles).length} loaded`} size="small" color="primary" sx={{ ml: 1 }} />
            )}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Upload corrected YAML files or paste a JSON spec dict to override the auto-generated YAML from the flow.
              Useful when you've manually fixed catalog references or SQL aliases.
            </Typography>
            <Tabs value={yamlTab} onChange={(_, v) => { setYamlTab(v); setYamlError(null); setYamlSuccess(null); }}>
              <Tab label="Upload .yml files" />
              <Tab label="Paste JSON spec" />
            </Tabs>

            {yamlTab === 0 && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                <Button variant="outlined" component="label" size="small" disabled={disabled}>
                  Upload YAML file(s)
                  <input type="file" accept=".yml,.yaml" multiple hidden onChange={handleYamlFileUpload} />
                </Button>
                <Typography variant="caption" color="text.secondary">
                  Each file name (without .yml) becomes the table key, e.g. <code>fact_pe002.yml</code> → key <code>fact_pe002</code>
                </Typography>
              </Box>
            )}

            {yamlTab === 1 && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                <TextField
                  label="Paste yaml_specs_json"
                  value={yamlPaste}
                  onChange={(e) => setYamlPaste(e.target.value)}
                  disabled={disabled}
                  fullWidth multiline minRows={5} size="small"
                  placeholder={'{\n  "fact_pe002": "version: \'1.1\'\\nsource: ..."\n}'}
                />
                <Button variant="contained" size="small" onClick={handleYamlPasteApply}
                  disabled={disabled || !yamlPaste.trim()} sx={{ alignSelf: 'flex-start' }}>
                  Apply
                </Button>
              </Box>
            )}

            {yamlSuccess && <Alert severity="success" variant="outlined">{yamlSuccess}</Alert>}
            {yamlError && <Alert severity="error" variant="outlined">{yamlError}</Alert>}

            {Object.keys(uploadedFiles).length > 0 && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                <Typography variant="caption" sx={{ fontWeight: 600 }}>Loaded views:</Typography>
                {Object.keys(uploadedFiles).map(k => (
                  <Box key={k} sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Chip label={k} size="small" variant="outlined" />
                    <Typography variant="caption" color="text.secondary">
                      {uploadedFiles[k].split('\n').length} lines
                    </Typography>
                    <IconButton size="small" onClick={() => removeYamlEntry(k)} disabled={disabled}>
                      <DeleteIcon fontSize="small" />
                    </IconButton>
                  </Box>
                ))}
              </Box>
            )}
          </Box>
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
            <TextField
              label="Catalog Remap (optional)"
              value={value.catalog_remap || ''}
              onChange={(e) => handleField('catalog_remap', e.target.value)}
              disabled={disabled}
              fullWidth size="small"
              helperText='Replace source catalogs in YAML before deploying. JSON dict, e.g. {"dc_datalake_prod_001": "david_test_metrics"}'
              placeholder='{"dc_datalake_prod_001": "david_test_metrics"}'
            />
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
