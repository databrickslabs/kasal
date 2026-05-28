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
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { DatabricksService } from '../../api/DatabricksService';

export interface PBIVisualUCMVMapperConfig {
  catalog?: string;
  schema_name?: string;
  dashboard_title?: string;
  databricks_host?: string;
  llm_model?: string;
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
