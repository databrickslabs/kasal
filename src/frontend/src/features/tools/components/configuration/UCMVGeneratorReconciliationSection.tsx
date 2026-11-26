import React from 'react';
import {
  Box,
  Typography,
  TextField,
  FormControlLabel,
  Checkbox,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { PowerBIReconConnectionFields } from './PowerBIReconConnectionFields';
import type { UCMetricViewGeneratorConfig } from './ucmvGeneratorConfigTypes';

/**
 * Reconciliation (iterative Power BI cell comparison) and metadata-lint settings
 * for the UC Metric View Generator.
 */
interface UCMVGeneratorReconciliationSectionProps {
  value: UCMetricViewGeneratorConfig;
  onChange: (config: UCMetricViewGeneratorConfig) => void;
  onFieldChange: (field: keyof UCMetricViewGeneratorConfig, fieldValue: string | boolean | number) => void;
  disabled: boolean;
  mode: 'api' | 'json';
}

export const UCMVGeneratorReconciliationSection: React.FC<UCMVGeneratorReconciliationSectionProps> = ({
  value,
  onChange,
  onFieldChange,
  disabled,
  mode,
}) => {
  // Local validation state for naming_config JSON field
  const [namingConfigError, setNamingConfigError] = React.useState<string>('');
  return (
      <Accordion sx={{ mt: 1 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2">
            Reconciliation &amp; Metadata Lint
            {value.enable_reconciliation ? ' (enabled)' : ''}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Databricks SQL warehouse ID"
              value={value.warehouse_id || ''}
              onChange={(e) => onFieldChange('warehouse_id', e.target.value)}
              disabled={disabled}
              fullWidth
              size="small"
              helperText="Required for reconciliation (queries the deployed metric view)."
            />

            <TextField
              label="Reference years"
              value={value.reference_years || ''}
              onChange={(e) => onFieldChange('reference_years', e.target.value)}
              disabled={disabled}
              fullWidth
              size="small"
              placeholder="[2025, 2026]"
              helperText="Optional — the fiscal year(s) to compare cell-by-cell, e.g. [2025, 2026]. Leave blank to compare across all years."
            />

            <FormControlLabel
              control={
                <Checkbox
                  checked={value.enable_reconciliation || false}
                  onChange={(e) => onFieldChange('enable_reconciliation', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={
                <Box>
                  <Typography variant="body2">Reconcile against Power BI (iterative)</Typography>
                  <Typography variant="caption" color="text.secondary">
                    After each generation cycle, deploy the metric view and compare it cell-by-cell against
                    the live Power BI model; feed mismatches back to the model to refine. Requires a
                    Databricks SQL warehouse and Power BI credentials (workspace&nbsp;ID, dataset&nbsp;ID,
                    and authentication — entered below in JSON mode). Reference years are optional.
                  </Typography>
                </Box>
              }
            />

            {value.enable_reconciliation && (
              <Box sx={{ pl: 3, display: 'flex', flexDirection: 'column', gap: 2, borderLeft: '2px solid', borderColor: 'divider' }}>
                {mode === 'json' ? (
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'rgb(156, 39, 176)' }}>
                      Power BI connection (reconciliation ground truth)
                    </Typography>
                    <PowerBIReconConnectionFields value={value} onChange={onChange} disabled={disabled} />
                  </Box>
                ) : (
                  <Alert severity="info" variant="outlined">
                    <Typography variant="caption">
                      Reconciliation uses the Power BI credentials from the <strong>Power BI Workspace
                      Configuration</strong> above (workspace&nbsp;ID, dataset&nbsp;ID, and authentication).
                    </Typography>
                  </Alert>
                )}
                <TextField
                  label="Max reconciliation cycles"
                  type="number"
                  value={value.max_reconciliation_cycles ?? 5}
                  onChange={(e) => {
                    const parsed = parseInt(e.target.value, 10);
                    if (!isNaN(parsed)) onFieldChange('max_reconciliation_cycles', Math.min(10, Math.max(1, parsed)));
                  }}
                  disabled={disabled}
                  fullWidth
                  size="small"
                  inputProps={{ min: 1, max: 10 }}
                />
                <TextField
                  label="Target cell-alignment %"
                  type="number"
                  value={value.reconciliation_target_pct ?? 100}
                  onChange={(e) => {
                    const parsed = parseFloat(e.target.value);
                    if (!isNaN(parsed)) onFieldChange('reconciliation_target_pct', Math.min(100, Math.max(0, parsed)));
                  }}
                  disabled={disabled}
                  fullWidth
                  size="small"
                  inputProps={{ min: 0, max: 100, step: 1 }}
                  helperText="Stop early once overall cell alignment reaches this percentage."
                />
              </Box>
            )}

            <FormControlLabel
              control={
                <Checkbox
                  checked={value.prune_non_visual_measures || false}
                  onChange={(e) => onFieldChange('prune_non_visual_measures', e.target.checked)}
                  disabled={disabled}
                  size="small"
                />
              }
              label={
                <Box>
                  <Typography variant="body2">Only translate measures shown in a visual (triage)</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Skip measures not drawn or filtered in any report visual — directly or through a
                    dependency chain — so legacy/unused measures don&apos;t clutter the output. Base
                    column aggregates are always kept. No effect unless the report&apos;s visual-usage
                    data is available.
                  </Typography>
                </Box>
              }
            />

            <TextField
              label="Naming / period config (JSON, optional)"
              value={value.naming_config || ''}
              onChange={(e) => {
                const raw = e.target.value;
                if (raw.trim() === '') {
                  setNamingConfigError('');
                } else {
                  try {
                    JSON.parse(raw);
                    setNamingConfigError('');
                  } catch {
                    setNamingConfigError('Invalid JSON — fix before saving.');
                  }
                }
                onFieldChange('naming_config', raw);
              }}
              disabled={disabled}
              fullWidth
              multiline
              rows={9}
              size="small"
              error={!!namingConfigError}
              placeholder={`{
  "lint": {
    "scenario_suffixes": ["actual", "budget", "forecast", "py"],
    "unit_suffixes": ["hours", "cases", "hl"],
    "dimension_alias_groups": [["comp_code", "company_code"]]
  },
  "period": {
    "column_overrides": { "latest_month_label": "IsCurrentMonth" }
  }
}`}
              helperText={
                namingConfigError ||
                'Optional — leave blank to use the built-in defaults. Per-model vocabulary for ' +
                'the metadata lint (scenario/unit suffixes, dimension alias groups) and the calendar ' +
                'period-folding (column_overrides). The greyed example shows the exact shape; type over it.'
              }
              InputProps={{
                sx: { fontFamily: 'monospace', fontSize: '0.75rem' }
              }}
            />
          </Box>
        </AccordionDetails>
      </Accordion>
  );
};
