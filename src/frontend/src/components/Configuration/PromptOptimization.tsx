import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Collapse,
  Divider,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Snackbar,
  TextField,
  Typography,
} from '@mui/material';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import { useTranslation } from 'react-i18next';
import {
  PromptOptimizationRun,
  PromptOptimizationService,
} from '../../api/PromptOptimizationService';
import { ModelService } from '../../api/ModelService';

import { OPTIMIZABLE_TEMPLATES } from './optimizableTemplates';

const POLL_INTERVAL_MS = 5000;

const statusColor = (
  status: PromptOptimizationRun['status'],
): 'default' | 'info' | 'success' | 'error' => {
  switch (status) {
    case 'completed':
      return 'success';
    case 'failed':
      return 'error';
    case 'running':
      return 'info';
    default:
      return 'default';
  }
};

interface PromptOptimizationProps {
  /** Scope the panel to one template (set when opened from a template row's
   *  Optimize action): the picker is locked and runs are filtered to it. */
  fixedTemplate?: string;
}

const PromptOptimization: React.FC<PromptOptimizationProps> = ({ fixedTemplate }) => {
  const { t } = useTranslation();
  const [templateName, setTemplateName] = useState(
    fixedTemplate && OPTIMIZABLE_TEMPLATES.some((tpl) => tpl.name === fixedTemplate)
      ? fixedTemplate
      : OPTIMIZABLE_TEMPLATES[0].name,
  );

  // Follow the scoped template when the dialog is reopened for another row.
  useEffect(() => {
    if (fixedTemplate && OPTIMIZABLE_TEMPLATES.some((tpl) => tpl.name === fixedTemplate)) {
      setTemplateName(fixedTemplate);
    }
  }, [fixedTemplate]);
  const [model, setModel] = useState('');
  const [models, setModels] = useState<string[]>([]);
  const [budget, setBudget] = useState(40);
  const [starting, setStarting] = useState(false);
  const [runs, setRuns] = useState<PromptOptimizationRun[]>([]);
  const [expandedRun, setExpandedRun] = useState<string | null>(null);
  const [applying, setApplying] = useState<string | null>(null);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error',
  });
  const pollRef = useRef<number | null>(null);

  const refreshRuns = useCallback(async () => {
    try {
      setRuns(await PromptOptimizationService.listRuns());
    } catch {
      /* transient — keep the last known list */
    }
  }, []);

  // Load enabled models for the model picker + initial runs.
  useEffect(() => {
    (async () => {
      try {
        const enabled = await ModelService.getInstance().getEnabledModels();
        setModels(Object.keys(enabled));
      } catch {
        setModels([]);
      }
      await refreshRuns();
    })();
  }, [refreshRuns]);

  // Poll while any run is active.
  const hasActiveRun = runs.some((r) => r.status === 'pending' || r.status === 'running');
  useEffect(() => {
    if (!hasActiveRun) return;
    pollRef.current = window.setInterval(refreshRuns, POLL_INTERVAL_MS);
    return () => {
      if (pollRef.current) window.clearInterval(pollRef.current);
    };
  }, [hasActiveRun, refreshRuns]);

  const handleStart = async () => {
    setStarting(true);
    try {
      const started = await PromptOptimizationService.startOptimization({
        template_name: templateName,
        model: model || undefined,
        max_metric_calls: budget,
      });
      setNotification({
        open: true,
        message: t('configuration.promptOptimization.started', {
          defaultValue: `Optimization started on ${started.dataset_size} examples`,
        }),
        severity: 'success',
      });
      setExpandedRun(started.run_id);
      await refreshRuns();
    } catch (error: unknown) {
      const detail =
        (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        'Failed to start optimization';
      setNotification({ open: true, message: detail, severity: 'error' });
    } finally {
      setStarting(false);
    }
  };

  const handleApply = async (runId: string) => {
    setApplying(runId);
    try {
      await PromptOptimizationService.applyRun(runId);
      setNotification({
        open: true,
        message: t('configuration.promptOptimization.applied', {
          defaultValue:
            'Optimized template applied as a group override — see Prompt Instructions',
        }),
        severity: 'success',
      });
      await refreshRuns();
    } catch {
      setNotification({ open: true, message: 'Failed to apply template', severity: 'error' });
    } finally {
      setApplying(null);
    }
  };

  // Scoped mode shows only the target template's runs.
  const visibleRuns = fixedTemplate
    ? runs.filter((r) => r.template_name === fixedTemplate)
    : runs;

  return (
    <Box>
      {/* No panel title — the hosting Prompts surface names this view. */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="body2" color="textSecondary" sx={{ mb: 2 }}>
            {t('configuration.promptOptimization.description', {
              defaultValue:
                'Optimize a seeded prompt template with GEPA: training examples are mined ' +
                'from logged usage, candidate templates are scored against format and ' +
                'correctness, and the winner is proposed for review. Applying writes a ' +
                'group-scoped override — the base template is never modified.',
            })}
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center' }}>
            {!fixedTemplate && (
              <FormControl size="small" sx={{ minWidth: 280 }}>
                <InputLabel>Template</InputLabel>
                <Select
                  value={templateName}
                  label="Template"
                  onChange={(e) => setTemplateName(e.target.value)}
                >
                  {OPTIMIZABLE_TEMPLATES.map((tpl) => (
                    <MenuItem key={tpl.name} value={tpl.name}>
                      {tpl.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            )}
            <FormControl size="small" sx={{ minWidth: 260 }}>
              <InputLabel>Model</InputLabel>
              <Select value={model} label="Model" onChange={(e) => setModel(e.target.value)}>
                <MenuItem value="">
                  <em>Default (dispatcher fast model)</em>
                </MenuItem>
                {models.map((key) => (
                  <MenuItem key={key} value={key}>
                    {key}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <TextField
              size="small"
              type="number"
              label="Budget (LLM evals)"
              value={budget}
              onChange={(e) =>
                setBudget(Math.max(8, Math.min(400, Number(e.target.value) || 40)))
              }
              sx={{ width: 160 }}
            />
            <Button
              variant="contained"
              onClick={handleStart}
              disabled={starting || hasActiveRun}
              startIcon={starting ? <CircularProgress size={16} /> : <AutoFixHighIcon />}
            >
              {starting
                ? 'Starting…'
                : hasActiveRun
                  ? 'Run in progress…'
                  : 'Start optimization'}
            </Button>
          </Box>
        </CardContent>
      </Card>

      <Typography variant="subtitle1" sx={{ mb: 1 }}>
        {t('configuration.promptOptimization.runs', { defaultValue: 'Runs' })}
      </Typography>
      {visibleRuns.length === 0 && (
        <Typography variant="body2" color="textSecondary">
          {t('configuration.promptOptimization.noRuns', {
            defaultValue: 'No optimization runs yet.',
          })}
        </Typography>
      )}
      {visibleRuns.map((run) => (
        <Card key={run.run_id} sx={{ mb: 1.5 }}>
          <CardContent sx={{ pb: '12px !important' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, flexWrap: 'wrap' }}>
              <Chip size="small" label={run.status} color={statusColor(run.status)} />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                {run.template_name}
              </Typography>
              <Typography variant="caption" color="textSecondary">
                {run.dataset_size} examples
                {run.model ? ` · ${run.model}` : ''}
                {run.created_at ? ` · ${new Date(run.created_at).toLocaleString()}` : ''}
              </Typography>
              {run.status === 'running' && <CircularProgress size={14} />}
              {typeof run.initial_score === 'number' && typeof run.final_score === 'number' && (
                <Chip
                  size="small"
                  variant="outlined"
                  label={`score ${run.initial_score.toFixed(2)} → ${run.final_score.toFixed(2)}`}
                />
              )}
              {run.applied && <Chip size="small" color="success" variant="outlined" label="applied" />}
              <Box sx={{ flexGrow: 1 }} />
              {run.status === 'completed' && (
                <Button
                  size="small"
                  variant="outlined"
                  disabled={run.applied || applying === run.run_id}
                  onClick={() => handleApply(run.run_id)}
                >
                  {applying === run.run_id ? 'Applying…' : run.applied ? 'Applied' : 'Apply'}
                </Button>
              )}
              <Button
                size="small"
                onClick={() =>
                  setExpandedRun(expandedRun === run.run_id ? null : run.run_id)
                }
                endIcon={expandedRun === run.run_id ? <ExpandLessIcon /> : <ExpandMoreIcon />}
              >
                Details
              </Button>
            </Box>
            <Collapse in={expandedRun === run.run_id}>
              <Divider sx={{ my: 1.5 }} />
              {run.error && (
                <Alert severity="error" sx={{ mb: 1.5 }}>
                  {run.error}
                </Alert>
              )}
              {run.optimized_template ? (
                <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                  <Box sx={{ flex: 1, minWidth: 320 }}>
                    <Typography variant="caption" color="textSecondary">
                      Baseline template
                    </Typography>
                    <TextField
                      fullWidth
                      multiline
                      minRows={8}
                      maxRows={16}
                      value={run.baseline_template || ''}
                      InputProps={{ readOnly: true, sx: { fontFamily: 'monospace', fontSize: 12 } }}
                    />
                  </Box>
                  <Box sx={{ flex: 1, minWidth: 320 }}>
                    <Typography variant="caption" color="textSecondary">
                      Optimized proposal
                    </Typography>
                    <TextField
                      fullWidth
                      multiline
                      minRows={8}
                      maxRows={16}
                      value={run.optimized_template}
                      InputProps={{ readOnly: true, sx: { fontFamily: 'monospace', fontSize: 12 } }}
                    />
                  </Box>
                </Box>
              ) : (
                !run.error && (
                  <Typography variant="body2" color="textSecondary">
                    {run.status === 'running' || run.status === 'pending'
                      ? 'Optimizing — candidate templates are being generated and scored…'
                      : 'No proposal available.'}
                  </Typography>
                )
              )}
            </Collapse>
          </CardContent>
        </Card>
      ))}

      <Snackbar
        open={notification.open}
        autoHideDuration={5000}
        onClose={() => setNotification((n) => ({ ...n, open: false }))}
      >
        <Alert
          severity={notification.severity}
          onClose={() => setNotification((n) => ({ ...n, open: false }))}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default PromptOptimization;
