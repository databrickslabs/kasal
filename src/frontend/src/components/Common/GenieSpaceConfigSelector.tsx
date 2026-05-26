/**
 * Genie Space Generator Configuration Selector Component
 *
 * Provides configuration UI for the Genie Space Generator Tool (92).
 * Configures the Genie space title, catalog/schema, warehouse, tables,
 * instructions, join specs, sample questions, and SQL snippets.
 */

import React, { useState } from 'react';
import {
  Box,
  Typography,
  TextField,
  Divider,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Button,
  IconButton,
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
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import { DatabricksService } from '../../api/DatabricksService';

export interface GenieSpaceConfig {
  space_title?: string;
  catalog?: string;
  schema_name?: string;
  warehouse_id?: string;
  databricks_host?: string;
  additional_tables?: string;
  text_instructions?: string;
  join_specs_json?: string;
  sample_questions?: string;
  sql_expressions_json?: string;
  sql_measures_json?: string;
  sql_filters_json?: string;
  example_sqls_json?: string;
  [key: string]: string | undefined;
}

interface JoinSpec {
  left_table: string;
  right_table: string;
  join_condition: string;
}

interface SqlSnippet {
  display_name: string;
  sql: string;
}

interface SqlMeasure {
  display_name: string;
  sql: string;
  instruction: string;
}

interface ExampleSql {
  question: string;
  sql: string;
}

interface WarehouseOption {
  id: string;
  name: string;
  state: string;
}

interface GenieSpaceConfigSelectorProps {
  value: GenieSpaceConfig;
  onChange: (config: GenieSpaceConfig) => void;
  disabled?: boolean;
}

export const GenieSpaceConfigSelector: React.FC<GenieSpaceConfigSelectorProps> = ({
  value = {},
  onChange,
  disabled = false,
}) => {
  // ── Connection state ────────────────────────────────────────────────────────
  const [warehouses, setWarehouses] = useState<WarehouseOption[]>([]);
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [connectLoading, setConnectLoading] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [catalogSearch, setCatalogSearch] = useState('');
  const [schemaSearch, setSchemaSearch] = useState('');

  // ── JSON load state ─────────────────────────────────────────────────────────
  const [jsonTab, setJsonTab] = useState(0);
  const [jsonPasteText, setJsonPasteText] = useState('');
  const [jsonError, setJsonError] = useState<string | null>(null);
  const [jsonSuccess, setJsonSuccess] = useState<string | null>(null);

  // ── Structured sub-state ────────────────────────────────────────────────────
  const [joinSpecs, setJoinSpecs] = useState<JoinSpec[]>(() => {
    try { return value.join_specs_json ? JSON.parse(value.join_specs_json) : []; } catch { return []; }
  });
  const [sqlExpressions, setSqlExpressions] = useState<SqlSnippet[]>(() => {
    try { return value.sql_expressions_json ? JSON.parse(value.sql_expressions_json) : []; } catch { return []; }
  });
  const [sqlMeasures, setSqlMeasures] = useState<SqlMeasure[]>(() => {
    try { return value.sql_measures_json ? JSON.parse(value.sql_measures_json) : []; } catch { return []; }
  });
  const [sqlFilters, setSqlFilters] = useState<SqlSnippet[]>(() => {
    try { return value.sql_filters_json ? JSON.parse(value.sql_filters_json) : []; } catch { return []; }
  });
  const [exampleSqls, setExampleSqls] = useState<ExampleSql[]>(() => {
    try { return value.example_sqls_json ? JSON.parse(value.example_sqls_json) : []; } catch { return []; }
  });

  const handleField = (field: keyof GenieSpaceConfig, fieldValue: string) => {
    onChange({ ...value, [field]: fieldValue });
  };

  // ── Connect button ──────────────────────────────────────────────────────────

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

  // ── Catalog change → fetch schemas ─────────────────────────────────────────

  const handleCatalogChange = async (catalog: string) => {
    handleField('catalog', catalog);
    handleField('schema_name', '');
    setSchemas([]);
    if (!catalog) return;
    setSchemaLoading(true);
    try {
      const host = value.databricks_host || undefined;
      const schemalist = await DatabricksService.listSchemas(catalog, host);
      setSchemas(schemalist);
    } catch {
      // Non-fatal: user can still type manually
    } finally {
      setSchemaLoading(false);
    }
  };

  // ── JSON load (file or paste) ───────────────────────────────────────────────

  const applyJsonConfig = (parsed: Record<string, unknown>) => {
    const merged: GenieSpaceConfig = { ...value };
    const jsonFields: (keyof GenieSpaceConfig)[] = [
      'space_title', 'catalog', 'schema_name', 'warehouse_id', 'databricks_host',
      'additional_tables', 'text_instructions', 'sample_questions',
      'join_specs_json', 'sql_expressions_json', 'sql_measures_json',
      'sql_filters_json', 'example_sqls_json',
    ];
    for (const f of jsonFields) {
      if (f in parsed && typeof parsed[f] === 'string' && (parsed[f] as string) !== '') {
        merged[f] = parsed[f] as string;
      }
    }

    // Sync local sub-states
    try { if (merged.join_specs_json) setJoinSpecs(JSON.parse(merged.join_specs_json)); } catch { /* */ }
    try { if (merged.sql_expressions_json) setSqlExpressions(JSON.parse(merged.sql_expressions_json)); } catch { /* */ }
    try { if (merged.sql_measures_json) setSqlMeasures(JSON.parse(merged.sql_measures_json)); } catch { /* */ }
    try { if (merged.sql_filters_json) setSqlFilters(JSON.parse(merged.sql_filters_json)); } catch { /* */ }
    try { if (merged.example_sqls_json) setExampleSqls(JSON.parse(merged.example_sqls_json)); } catch { /* */ }

    onChange(merged);
    setJsonError(null);
  };

  const countAppliedFields = (parsed: Record<string, unknown>) => {
    const jsonFields = ['space_title','catalog','schema_name','warehouse_id','databricks_host',
      'additional_tables','text_instructions','sample_questions',
      'join_specs_json','sql_expressions_json','sql_measures_json','sql_filters_json','example_sqls_json'];
    return jsonFields.filter(f => f in parsed && typeof parsed[f] === 'string').length;
  };

  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const parsed = JSON.parse(e.target?.result as string);
        const n = countAppliedFields(parsed);
        applyJsonConfig(parsed);
        setJsonSuccess(`Loaded "${file.name}" — ${n} field${n !== 1 ? 's' : ''} applied`);
      } catch {
        setJsonError('Invalid JSON file');
        setJsonSuccess(null);
      }
    };
    reader.readAsText(file);
    event.target.value = '';
  };

  const handlePasteApply = () => {
    try {
      const parsed = JSON.parse(jsonPasteText);
      const n = countAppliedFields(parsed);
      applyJsonConfig(parsed);
      setJsonPasteText('');
      setJsonSuccess(`${n} field${n !== 1 ? 's' : ''} applied from pasted JSON`);
    } catch {
      setJsonError('Invalid JSON — check syntax and try again');
      setJsonSuccess(null);
    }
  };

  // ── Join Specs ────────────────────────────────────────────────────────────────

  const updateJoinSpecs = (updated: JoinSpec[]) => {
    setJoinSpecs(updated);
    onChange({ ...value, join_specs_json: JSON.stringify(updated) });
  };
  const addJoinSpec = () => updateJoinSpecs([...joinSpecs, { left_table: '', right_table: '', join_condition: '' }]);
  const removeJoinSpec = (i: number) => updateJoinSpecs(joinSpecs.filter((_, idx) => idx !== i));
  const updateJoinSpec = (i: number, field: keyof JoinSpec, v: string) =>
    updateJoinSpecs(joinSpecs.map((js, idx) => idx === i ? { ...js, [field]: v } : js));

  // ── SQL Expressions ───────────────────────────────────────────────────────────

  const updateSqlExpressions = (updated: SqlSnippet[]) => {
    setSqlExpressions(updated);
    onChange({ ...value, sql_expressions_json: JSON.stringify(updated) });
  };
  const addSqlExpression = () => updateSqlExpressions([...sqlExpressions, { display_name: '', sql: '' }]);
  const removeSqlExpression = (i: number) => updateSqlExpressions(sqlExpressions.filter((_, idx) => idx !== i));
  const updateSqlExpression = (i: number, field: keyof SqlSnippet, v: string) =>
    updateSqlExpressions(sqlExpressions.map((e, idx) => idx === i ? { ...e, [field]: v } : e));

  // ── SQL Measures ──────────────────────────────────────────────────────────────

  const updateSqlMeasures = (updated: SqlMeasure[]) => {
    setSqlMeasures(updated);
    onChange({ ...value, sql_measures_json: JSON.stringify(updated) });
  };
  const addSqlMeasure = () => updateSqlMeasures([...sqlMeasures, { display_name: '', sql: '', instruction: '' }]);
  const removeSqlMeasure = (i: number) => updateSqlMeasures(sqlMeasures.filter((_, idx) => idx !== i));
  const updateSqlMeasure = (i: number, field: keyof SqlMeasure, v: string) =>
    updateSqlMeasures(sqlMeasures.map((m, idx) => idx === i ? { ...m, [field]: v } : m));

  // ── SQL Filters ───────────────────────────────────────────────────────────────

  const updateSqlFilters = (updated: SqlSnippet[]) => {
    setSqlFilters(updated);
    onChange({ ...value, sql_filters_json: JSON.stringify(updated) });
  };
  const addSqlFilter = () => updateSqlFilters([...sqlFilters, { display_name: '', sql: '' }]);
  const removeSqlFilter = (i: number) => updateSqlFilters(sqlFilters.filter((_, idx) => idx !== i));
  const updateSqlFilter = (i: number, field: keyof SqlSnippet, v: string) =>
    updateSqlFilters(sqlFilters.map((f, idx) => idx === i ? { ...f, [field]: v } : f));

  // ── Example SQLs ──────────────────────────────────────────────────────────────

  const updateExampleSqls = (updated: ExampleSql[]) => {
    setExampleSqls(updated);
    onChange({ ...value, example_sqls_json: JSON.stringify(updated) });
  };
  const addExampleSql = () => updateExampleSqls([...exampleSqls, { question: '', sql: '' }]);
  const removeExampleSql = (i: number) => updateExampleSqls(exampleSqls.filter((_, idx) => idx !== i));
  const updateExampleSql = (i: number, field: keyof ExampleSql, v: string) =>
    updateExampleSqls(exampleSqls.map((e, idx) => idx === i ? { ...e, [field]: v } : e));

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* ── ① Connection ──────────────────────────────────────────────────────── */}
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
                helperText="Override workspace URL. Leave blank to use Kasal Settings / DATABRICKS_HOST."
                size="small"
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
              <InputLabel>Warehouse</InputLabel>
              <Select
                label="Warehouse"
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
            {warehouses.length === 0 && !value.warehouse_id && (
              <Typography variant="caption" color="text.secondary">
                Enter workspace URL and click Connect to populate the warehouse list, or type the ID directly after connecting.
              </Typography>
            )}
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── ② Space Identity ──────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Space Identity</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <TextField
            label="Space Title"
            value={value.space_title || ''}
            onChange={(e) => handleField('space_title', e.target.value)}
            disabled={disabled}
            required
            fullWidth
            helperText="Display name for the Genie space"
            size="small"
          />
        </AccordionDetails>
      </Accordion>

      {/* ── ③ Catalog & Schema ────────────────────────────────────────────────── */}
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
                    size="small"
                    fullWidth
                    placeholder="Search catalogs…"
                    value={catalogSearch}
                    onChange={(e) => setCatalogSearch(e.target.value)}
                    autoFocus
                    onClick={(e) => e.stopPropagation()}
                  />
                </MenuItem>
                {catalogs.length === 0 && value.catalog && !catalogSearch && (
                  <MenuItem value={value.catalog}>{value.catalog}</MenuItem>
                )}
                {(catalogSearch
                  ? catalogs.filter((c) => c.toLowerCase().includes(catalogSearch.toLowerCase()))
                  : catalogs
                ).map((c) => (
                  <MenuItem key={c} value={c}>{c}</MenuItem>
                ))}
                {catalogs.length > 0 &&
                  catalogSearch &&
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
                    size="small"
                    fullWidth
                    placeholder="Search schemas…"
                    value={schemaSearch}
                    onChange={(e) => setSchemaSearch(e.target.value)}
                    autoFocus
                    onClick={(e) => e.stopPropagation()}
                  />
                </MenuItem>
                {schemas.length === 0 && value.schema_name && !schemaSearch && (
                  <MenuItem value={value.schema_name}>{value.schema_name}</MenuItem>
                )}
                {(schemaSearch
                  ? schemas.filter((s) => s.toLowerCase().includes(schemaSearch.toLowerCase()))
                  : schemas
                ).map((s) => (
                  <MenuItem key={s} value={s}>{s}</MenuItem>
                ))}
                {schemas.length > 0 &&
                  schemaSearch &&
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

      {/* ── ④ Load Config from JSON ───────────────────────────────────────────── */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Load Config from JSON (optional)</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Upload or paste a Genie Space config JSON to populate all fields at once. Existing values are overwritten only for matching keys.
            </Typography>
            <Tabs value={jsonTab} onChange={(_, v) => { setJsonTab(v); setJsonError(null); setJsonSuccess(null); }}>
              <Tab label="Upload File" />
              <Tab label="Paste JSON" />
            </Tabs>
            {jsonTab === 0 && (
              <Box>
                <Button variant="outlined" component="label" size="small" disabled={disabled}>
                  Choose JSON file
                  <input type="file" accept=".json,application/json" hidden onChange={handleFileUpload} />
                </Button>
              </Box>
            )}
            {jsonTab === 1 && (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                <TextField
                  label="Paste JSON here"
                  value={jsonPasteText}
                  onChange={(e) => setJsonPasteText(e.target.value)}
                  disabled={disabled}
                  fullWidth
                  multiline
                  minRows={5}
                  size="small"
                  placeholder={'{\n  "space_title": "My Space",\n  "catalog": "my_catalog",\n  ...\n}'}
                />
                <Button
                  variant="contained"
                  size="small"
                  onClick={handlePasteApply}
                  disabled={disabled || !jsonPasteText.trim()}
                  sx={{ alignSelf: 'flex-start' }}
                >
                  Apply
                </Button>
              </Box>
            )}
            {jsonSuccess && <Alert severity="success" variant="outlined">{jsonSuccess}</Alert>}
            {jsonError && <Alert severity="error" variant="outlined">{jsonError}</Alert>}
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── ⑤ Tables ───────────────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Tables</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Alert severity="info" variant="outlined">
              <Typography variant="caption">
                Metric views deployed by the UCMV Generator are added automatically from the flow output.
                Add extra dimension or lookup tables below.
              </Typography>
            </Alert>
            <TextField
              label="Additional Tables"
              value={value.additional_tables || ''}
              onChange={(e) => handleField('additional_tables', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              minRows={3}
              helperText="One fully-qualified table name per line, e.g. my_catalog.my_schema.dim_customer"
              size="small"
              placeholder="my_catalog.my_schema.dim_customer&#10;my_catalog.my_schema.dim_date"
            />
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── ⑥ Instructions & Questions ─────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Instructions &amp; Sample Questions</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Text Instructions"
              value={value.text_instructions || ''}
              onChange={(e) => handleField('text_instructions', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              minRows={4}
              helperText="General instructions for the Genie space (data descriptions, business rules, etc.)"
              size="small"
              placeholder="This space contains sales KPIs for the EMEA region. Revenue figures are in EUR..."
            />
            <TextField
              label="Sample Questions"
              value={value.sample_questions || ''}
              onChange={(e) => handleField('sample_questions', e.target.value)}
              disabled={disabled}
              fullWidth
              multiline
              minRows={3}
              helperText="One question per line. These appear as suggestions to users."
              size="small"
              placeholder="What was total revenue last month?&#10;Show top 10 customers by order count&#10;How does Q3 compare to Q2?"
            />
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── ⑦ Join Specs ───────────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Join Specs{joinSpecs.length > 0 ? ` (${joinSpecs.length})` : ' (optional)'}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {joinSpecs.map((js, i) => (
              <Box key={i} sx={{ display: 'flex', flexDirection: 'column', gap: 1, p: 1.5, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Typography variant="caption" color="text.secondary">Join {i + 1}</Typography>
                  <IconButton size="small" onClick={() => removeJoinSpec(i)} disabled={disabled}>
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Box>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <TextField label="Left Table" value={js.left_table} onChange={(e) => updateJoinSpec(i, 'left_table', e.target.value)} disabled={disabled} fullWidth size="small" placeholder="catalog.schema.table" />
                  <TextField label="Right Table" value={js.right_table} onChange={(e) => updateJoinSpec(i, 'right_table', e.target.value)} disabled={disabled} fullWidth size="small" placeholder="catalog.schema.table" />
                </Box>
                <TextField label="Join Condition" value={js.join_condition} onChange={(e) => updateJoinSpec(i, 'join_condition', e.target.value)} disabled={disabled} fullWidth size="small" placeholder="left_table.customer_id = right_table.customer_id" />
              </Box>
            ))}
            <Button startIcon={<AddIcon />} onClick={addJoinSpec} disabled={disabled} variant="outlined" size="small" sx={{ alignSelf: 'flex-start' }}>
              Add Join Spec
            </Button>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── ⑧ SQL Snippets ─────────────────────────────────────────────────────── */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            SQL Snippets{(sqlExpressions.length + sqlMeasures.length + sqlFilters.length) > 0
              ? ` (${sqlExpressions.length + sqlMeasures.length + sqlFilters.length})`
              : ' (optional)'}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>

            {/* Expressions */}
            <Box>
              <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>Expressions</Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                {sqlExpressions.map((expr, i) => (
                  <Box key={i} sx={{ display: 'flex', flexDirection: 'column', gap: 1, p: 1.5, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                    <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
                      <TextField label="Display Name" value={expr.display_name} onChange={(e) => updateSqlExpression(i, 'display_name', e.target.value)} disabled={disabled} size="small" sx={{ flex: 1 }} />
                      <IconButton size="small" onClick={() => removeSqlExpression(i)} disabled={disabled}><DeleteIcon fontSize="small" /></IconButton>
                    </Box>
                    <TextField label="SQL" value={expr.sql} onChange={(e) => updateSqlExpression(i, 'sql', e.target.value)} disabled={disabled} fullWidth multiline minRows={2} size="small" placeholder="SUM(revenue)" />
                  </Box>
                ))}
                <Button startIcon={<AddIcon />} onClick={addSqlExpression} disabled={disabled} variant="outlined" size="small" sx={{ alignSelf: 'flex-start' }}>Add Expression</Button>
              </Box>
            </Box>

            <Divider />

            {/* Measures */}
            <Box>
              <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>Measures</Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                {sqlMeasures.map((m, i) => (
                  <Box key={i} sx={{ display: 'flex', flexDirection: 'column', gap: 1, p: 1.5, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                    <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
                      <TextField label="Display Name" value={m.display_name} onChange={(e) => updateSqlMeasure(i, 'display_name', e.target.value)} disabled={disabled} size="small" sx={{ flex: 1 }} />
                      <IconButton size="small" onClick={() => removeSqlMeasure(i)} disabled={disabled}><DeleteIcon fontSize="small" /></IconButton>
                    </Box>
                    <TextField label="SQL" value={m.sql} onChange={(e) => updateSqlMeasure(i, 'sql', e.target.value)} disabled={disabled} fullWidth multiline minRows={2} size="small" />
                    <TextField label="Instruction" value={m.instruction} onChange={(e) => updateSqlMeasure(i, 'instruction', e.target.value)} disabled={disabled} fullWidth size="small" helperText="Optional natural language guidance for the LLM" />
                  </Box>
                ))}
                <Button startIcon={<AddIcon />} onClick={addSqlMeasure} disabled={disabled} variant="outlined" size="small" sx={{ alignSelf: 'flex-start' }}>Add Measure</Button>
              </Box>
            </Box>

            <Divider />

            {/* Filters */}
            <Box>
              <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>Filters</Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                {sqlFilters.map((f, i) => (
                  <Box key={i} sx={{ display: 'flex', flexDirection: 'column', gap: 1, p: 1.5, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                    <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
                      <TextField label="Display Name" value={f.display_name} onChange={(e) => updateSqlFilter(i, 'display_name', e.target.value)} disabled={disabled} size="small" sx={{ flex: 1 }} />
                      <IconButton size="small" onClick={() => removeSqlFilter(i)} disabled={disabled}><DeleteIcon fontSize="small" /></IconButton>
                    </Box>
                    <TextField label="SQL" value={f.sql} onChange={(e) => updateSqlFilter(i, 'sql', e.target.value)} disabled={disabled} fullWidth multiline minRows={2} size="small" placeholder="region = 'EMEA'" />
                  </Box>
                ))}
                <Button startIcon={<AddIcon />} onClick={addSqlFilter} disabled={disabled} variant="outlined" size="small" sx={{ alignSelf: 'flex-start' }}>Add Filter</Button>
              </Box>
            </Box>

          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── ⑨ Example Queries ───────────────────────────────────────────────────── */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Example Queries{exampleSqls.length > 0 ? ` (${exampleSqls.length})` : ' (optional)'}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Provide question + SQL pairs to help Genie understand your data model.
            </Typography>
            {exampleSqls.map((eq, i) => (
              <Box key={i} sx={{ display: 'flex', flexDirection: 'column', gap: 1, p: 1.5, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Typography variant="caption" color="text.secondary">Example {i + 1}</Typography>
                  <IconButton size="small" onClick={() => removeExampleSql(i)} disabled={disabled}><DeleteIcon fontSize="small" /></IconButton>
                </Box>
                <TextField label="Question" value={eq.question} onChange={(e) => updateExampleSql(i, 'question', e.target.value)} disabled={disabled} fullWidth size="small" placeholder="What was total revenue last month?" />
                <TextField label="SQL" value={eq.sql} onChange={(e) => updateExampleSql(i, 'sql', e.target.value)} disabled={disabled} fullWidth multiline minRows={3} size="small" placeholder="SELECT SUM(revenue) FROM ..." />
              </Box>
            ))}
            <Button startIcon={<AddIcon />} onClick={addExampleSql} disabled={disabled} variant="outlined" size="small" sx={{ alignSelf: 'flex-start' }}>
              Add Example Query
            </Button>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Info */}
      <Alert severity="info" variant="outlined" sx={{ mt: 1 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
          What this tool does:
        </Typography>
        <Typography variant="caption">
          Deploys a Genie Space on top of UC Metric Views generated by the UCMV Generator tool.
          The space is idempotent — running again PATCHes the existing space rather than creating a duplicate.
        </Typography>
      </Alert>
    </Box>
  );
};
