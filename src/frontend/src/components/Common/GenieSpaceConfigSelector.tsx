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
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';

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
  const [joinSpecs, setJoinSpecs] = useState<JoinSpec[]>(() => {
    try {
      return value.join_specs_json ? JSON.parse(value.join_specs_json) : [];
    } catch {
      return [];
    }
  });

  const [sqlExpressions, setSqlExpressions] = useState<SqlSnippet[]>(() => {
    try {
      return value.sql_expressions_json ? JSON.parse(value.sql_expressions_json) : [];
    } catch {
      return [];
    }
  });

  const [sqlMeasures, setSqlMeasures] = useState<SqlMeasure[]>(() => {
    try {
      return value.sql_measures_json ? JSON.parse(value.sql_measures_json) : [];
    } catch {
      return [];
    }
  });

  const [sqlFilters, setSqlFilters] = useState<SqlSnippet[]>(() => {
    try {
      return value.sql_filters_json ? JSON.parse(value.sql_filters_json) : [];
    } catch {
      return [];
    }
  });

  const [exampleSqls, setExampleSqls] = useState<ExampleSql[]>(() => {
    try {
      return value.example_sqls_json ? JSON.parse(value.example_sqls_json) : [];
    } catch {
      return [];
    }
  });

  const handleField = (field: keyof GenieSpaceConfig, fieldValue: string) => {
    onChange({ ...value, [field]: fieldValue });
  };

  // ── Join Specs ────────────────────────────────────────────────────────────────

  const updateJoinSpecs = (updated: JoinSpec[]) => {
    setJoinSpecs(updated);
    onChange({ ...value, join_specs_json: JSON.stringify(updated) });
  };

  const addJoinSpec = () => updateJoinSpecs([...joinSpecs, { left_table: '', right_table: '', join_condition: '' }]);

  const removeJoinSpec = (i: number) => updateJoinSpecs(joinSpecs.filter((_, idx) => idx !== i));

  const updateJoinSpec = (i: number, field: keyof JoinSpec, v: string) => {
    const updated = joinSpecs.map((js, idx) => idx === i ? { ...js, [field]: v } : js);
    updateJoinSpecs(updated);
  };

  // ── SQL Expressions ───────────────────────────────────────────────────────────

  const updateSqlExpressions = (updated: SqlSnippet[]) => {
    setSqlExpressions(updated);
    onChange({ ...value, sql_expressions_json: JSON.stringify(updated) });
  };

  const addSqlExpression = () => updateSqlExpressions([...sqlExpressions, { display_name: '', sql: '' }]);

  const removeSqlExpression = (i: number) => updateSqlExpressions(sqlExpressions.filter((_, idx) => idx !== i));

  const updateSqlExpression = (i: number, field: keyof SqlSnippet, v: string) => {
    const updated = sqlExpressions.map((e, idx) => idx === i ? { ...e, [field]: v } : e);
    updateSqlExpressions(updated);
  };

  // ── SQL Measures ──────────────────────────────────────────────────────────────

  const updateSqlMeasures = (updated: SqlMeasure[]) => {
    setSqlMeasures(updated);
    onChange({ ...value, sql_measures_json: JSON.stringify(updated) });
  };

  const addSqlMeasure = () => updateSqlMeasures([...sqlMeasures, { display_name: '', sql: '', instruction: '' }]);

  const removeSqlMeasure = (i: number) => updateSqlMeasures(sqlMeasures.filter((_, idx) => idx !== i));

  const updateSqlMeasure = (i: number, field: keyof SqlMeasure, v: string) => {
    const updated = sqlMeasures.map((m, idx) => idx === i ? { ...m, [field]: v } : m);
    updateSqlMeasures(updated);
  };

  // ── SQL Filters ───────────────────────────────────────────────────────────────

  const updateSqlFilters = (updated: SqlSnippet[]) => {
    setSqlFilters(updated);
    onChange({ ...value, sql_filters_json: JSON.stringify(updated) });
  };

  const addSqlFilter = () => updateSqlFilters([...sqlFilters, { display_name: '', sql: '' }]);

  const removeSqlFilter = (i: number) => updateSqlFilters(sqlFilters.filter((_, idx) => idx !== i));

  const updateSqlFilter = (i: number, field: keyof SqlSnippet, v: string) => {
    const updated = sqlFilters.map((f, idx) => idx === i ? { ...f, [field]: v } : f);
    updateSqlFilters(updated);
  };

  // ── Example SQLs ──────────────────────────────────────────────────────────────

  const updateExampleSqls = (updated: ExampleSql[]) => {
    setExampleSqls(updated);
    onChange({ ...value, example_sqls_json: JSON.stringify(updated) });
  };

  const addExampleSql = () => updateExampleSqls([...exampleSqls, { question: '', sql: '' }]);

  const removeExampleSql = (i: number) => updateExampleSqls(exampleSqls.filter((_, idx) => idx !== i));

  const updateExampleSql = (i: number, field: keyof ExampleSql, v: string) => {
    const updated = exampleSqls.map((e, idx) => idx === i ? { ...e, [field]: v } : e);
    updateExampleSqls(updated);
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* ── 1. Basic Setup ──────────────────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Basic Setup</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
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
            <TextField
              label="Warehouse ID"
              value={value.warehouse_id || ''}
              onChange={(e) => handleField('warehouse_id', e.target.value)}
              disabled={disabled}
              required
              fullWidth
              helperText="SQL warehouse ID used to run Genie queries"
              size="small"
            />
            <TextField
              label="Workspace URL (optional)"
              value={value.databricks_host || ''}
              onChange={(e) => handleField('databricks_host', e.target.value)}
              disabled={disabled}
              fullWidth
              helperText="Override workspace URL (e.g. https://adb-123.azuredatabricks.net). Leave blank to use workspace from Kasal Settings / DATABRICKS_HOST env var."
              size="small"
              placeholder="https://adb-123456789.7.azuredatabricks.net"
            />
            <Box sx={{ display: 'flex', gap: 2 }}>
              <TextField
                label="Catalog"
                value={value.catalog || ''}
                onChange={(e) => handleField('catalog', e.target.value)}
                disabled={disabled}
                required
                fullWidth
                helperText="UC catalog"
                size="small"
              />
              <TextField
                label="Schema"
                value={value.schema_name || ''}
                onChange={(e) => handleField('schema_name', e.target.value)}
                disabled={disabled}
                required
                fullWidth
                helperText="UC schema"
                size="small"
              />
            </Box>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── 2. Tables ───────────────────────────────────────────────────────────── */}
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

      {/* ── 3. Instructions & Questions ─────────────────────────────────────────── */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Instructions & Sample Questions</Typography>
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

      {/* ── 4. Join Specs ───────────────────────────────────────────────────────── */}
      <Accordion>
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
                  <TextField
                    label="Left Table"
                    value={js.left_table}
                    onChange={(e) => updateJoinSpec(i, 'left_table', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    size="small"
                    placeholder="catalog.schema.table"
                  />
                  <TextField
                    label="Right Table"
                    value={js.right_table}
                    onChange={(e) => updateJoinSpec(i, 'right_table', e.target.value)}
                    disabled={disabled}
                    fullWidth
                    size="small"
                    placeholder="catalog.schema.table"
                  />
                </Box>
                <TextField
                  label="Join Condition"
                  value={js.join_condition}
                  onChange={(e) => updateJoinSpec(i, 'join_condition', e.target.value)}
                  disabled={disabled}
                  fullWidth
                  size="small"
                  placeholder="left_table.customer_id = right_table.customer_id"
                />
              </Box>
            ))}
            <Button
              startIcon={<AddIcon />}
              onClick={addJoinSpec}
              disabled={disabled}
              variant="outlined"
              size="small"
              sx={{ alignSelf: 'flex-start' }}
            >
              Add Join Spec
            </Button>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* ── 5. SQL Snippets ─────────────────────────────────────────────────────── */}
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

      {/* ── 6. Example Queries ───────────────────────────────────────────────────── */}
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
                <TextField
                  label="Question"
                  value={eq.question}
                  onChange={(e) => updateExampleSql(i, 'question', e.target.value)}
                  disabled={disabled}
                  fullWidth
                  size="small"
                  placeholder="What was total revenue last month?"
                />
                <TextField
                  label="SQL"
                  value={eq.sql}
                  onChange={(e) => updateExampleSql(i, 'sql', e.target.value)}
                  disabled={disabled}
                  fullWidth
                  multiline
                  minRows={3}
                  size="small"
                  placeholder="SELECT SUM(revenue) FROM ..."
                />
              </Box>
            ))}
            <Button
              startIcon={<AddIcon />}
              onClick={addExampleSql}
              disabled={disabled}
              variant="outlined"
              size="small"
              sx={{ alignSelf: 'flex-start' }}
            >
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
