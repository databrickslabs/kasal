import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  Typography,
  Box,
  CircularProgress,
  Button,
  Tooltip,
  Divider,
  Theme,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import PreviewIcon from '@mui/icons-material/Preview';
import TimelineIcon from '@mui/icons-material/Timeline';
import TerminalIcon from '@mui/icons-material/Terminal';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { ShowTraceProps } from '../../types/trace';

import apiClient from '../../config/api/ApiConfig';
import { useUserPreferencesStore } from '../../store/userPreferencesStore';
import { useTranslation } from 'react-i18next';
import ShowLogs from './ShowLogs';
import { executionLogService, LogEntry } from '../../api/ExecutionLogs';
import { useRunResult } from '../../hooks/global/useExecutionResult';
import { useTraceData } from '../../hooks/global/useTraceData';
import TraceTimelineContent from './TraceTimelineContent';

const ShowTraceTimeline: React.FC<ShowTraceProps> = ({
  open,
  onClose,
  runId,
  run,
  onViewResult,
  onShowLogs: _onShowLogs,
}) => {
  const { t } = useTranslation();
  const { useNewExecutionUI } = useUserPreferencesStore();
  const { showRunResult } = useRunResult();

  const [showLogsDialog, setShowLogsDialog] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [evaluationEnabled, setEvaluationEnabled] = useState<boolean>(false);
  const [isEvaluationRunning, setIsEvaluationRunning] = useState(false);

  // Use extracted trace data hook
  const traceData = useTraceData({
    runId,
    jobId: run?.job_id,
    runStatus: run?.status,
    isActive: open,
  });

  // Handle opening logs dialog
  const handleOpenLogs = async () => {
    if (!run) return;

    setShowLogsDialog(true);
    setIsLoadingLogs(true);
    setLogsError(null);

    try {
      const fetchedLogs = await executionLogService.getHistoricalLogs(run.job_id);
      const logEntries: LogEntry[] = fetchedLogs.map(log => ({
        id: log.id,
        output: log.output,
        content: log.content,
        timestamp: log.timestamp,
        logType: log.type
      }));
      setLogs(logEntries);
    } catch (error) {
      console.error('Error fetching logs:', error);
      setLogsError('Failed to fetch logs');
    } finally {
      setIsLoadingLogs(false);
    }
  };

  // Compute MLflow Traces deep link
  const getMlflowTracesUrl = useCallback(async (): Promise<string> => {
    try {
      let workspaceUrl = '';
      let workspaceId: string | undefined;
      try {
        const envResp = await apiClient.get('/databricks/environment');
        workspaceId = envResp?.data?.workspace_id || undefined;
        const envHost = envResp?.data?.databricks_host;
        if (!workspaceUrl && envHost) {
          workspaceUrl = envHost.startsWith('http') ? envHost : `https://${envHost}`;
        }
      } catch (e) {
        console.warn('Unable to get Databricks environment info');
      }

      if (!workspaceUrl || workspaceUrl === 'compute' || !workspaceUrl.includes('.')) {
        if (typeof window !== 'undefined' && window.location.hostname.includes('databricks')) {
          workspaceUrl = `https://${window.location.hostname}`;
        } else {
          try {
            const resp = await apiClient.get('/databricks/workspace-url');
            workspaceUrl = resp.data?.workspace_url || '';
          } catch (e) {
            console.warn('Unable to determine Databricks workspace URL');
          }
        }
      }

      if (!workspaceUrl) return '#';

      if (workspaceUrl.endsWith('/')) workspaceUrl = workspaceUrl.slice(0, -1);
      if (!workspaceUrl.startsWith('http')) workspaceUrl = `https://${workspaceUrl}`;

      let experimentId: string | undefined;
      try {
        const expResp = await apiClient.get('/mlflow/experiment-info');
        experimentId = expResp?.data?.experiment_id || undefined;
      } catch (e) {
        console.warn('Unable to resolve MLflow experiment id');
      }

      if (experimentId) {
        const o = workspaceId ? `?o=${encodeURIComponent(workspaceId)}` : '';
        return `${workspaceUrl}/ml/experiments/${encodeURIComponent(experimentId)}/traces${o}`;
      }

      return `${workspaceUrl}/mlflow/experiments`;
    } catch (err) {
      console.error('Failed to build MLflow traces URL:', err);
      return '#';
    }
  }, []);

  // Get specific trace URL with trace ID if available
  const getSpecificTraceUrl = useCallback(async (): Promise<string> => {
    if (!run) return '#';

    try {
      const resp = await apiClient.get('/mlflow/trace-link', { params: { job_id: run.job_id } });
      const url = resp?.data?.url as string | undefined;
      if (url) return url;
    } catch (e) {
      // ignore and fallback
    }

    try {
      let workspaceUrl = '';
      let workspaceId: string | undefined;
      try {
        const envResp = await apiClient.get('/databricks/environment');
        workspaceId = envResp?.data?.workspace_id || undefined;
        const envHost = envResp?.data?.databricks_host;
        if (!workspaceUrl && envHost) {
          workspaceUrl = envHost.startsWith('http') ? envHost : `https://${envHost}`;
        }
      } catch (e) {
        console.warn('Unable to get Databricks environment info');
      }

      if (!workspaceUrl || workspaceUrl === 'compute' || !workspaceUrl.includes('.')) {
        if (typeof window !== 'undefined' && window.location.hostname.includes('databricks')) {
          workspaceUrl = `https://${window.location.hostname}`;
        } else {
          try {
            const resp = await apiClient.get('/databricks/workspace-url');
            workspaceUrl = resp.data?.workspace_url || '';
          } catch (e) {
            console.warn('Unable to determine Databricks workspace URL');
          }
        }
      }

      if (!workspaceUrl) return '#';

      if (workspaceUrl.endsWith('/')) workspaceUrl = workspaceUrl.slice(0, -1);
      if (!workspaceUrl.startsWith('http')) workspaceUrl = `https://${workspaceUrl}`;

      let experimentId: string | undefined;
      try {
        const expResp = await apiClient.get('/mlflow/experiment-info');
        experimentId = expResp?.data?.experiment_id || undefined;
      } catch (e) {
        console.warn('Unable to resolve MLflow experiment id');
      }

      if (!experimentId) {
        return getMlflowTracesUrl();
      }

      let traceId: string | undefined;
      if (run.mlflow_trace_id) {
        traceId = run.mlflow_trace_id;
      }

      if (traceId) {
        const o = workspaceId ? `?o=${encodeURIComponent(workspaceId)}` : '';
        const selectedParam = `&selectedEvaluationId=${encodeURIComponent(traceId)}`;
        return `${workspaceUrl}/ml/experiments/${encodeURIComponent(experimentId)}/traces${o}${selectedParam}`;
      }

      return getMlflowTracesUrl();
    } catch (err) {
      console.error('Failed to build specific trace URL:', err);
      return getMlflowTracesUrl();
    }
  }, [run, getMlflowTracesUrl]);

  // Load evaluation toggle
  useEffect(() => {
    if (!open) return;
    (async () => {
      try {
        const resp = await apiClient.get('/mlflow/evaluation-status');
        setEvaluationEnabled(!!resp?.data?.enabled);
      } catch (e) {
        setEvaluationEnabled(false);
      }
    })();
  }, [open]);

  // Trigger evaluation and open the MLflow run page
  const triggerEvaluationAndOpen = async () => {
    if (!run || isEvaluationRunning) return;

    setIsEvaluationRunning(true);
    try {
      const ev = await apiClient.post('/mlflow/evaluate', { job_id: run.job_id });
      const evalRunId = ev?.data?.run_id as string | undefined;
      const experimentId = ev?.data?.experiment_id as string | undefined;

      if (!evalRunId || !experimentId) {
        console.error('Evaluation did not return run_id/experiment_id', ev?.data);
        return;
      }

      let workspaceUrl = '';
      let workspaceId: string | undefined;
      try {
        const envResp = await apiClient.get('/databricks/environment');
        workspaceId = envResp?.data?.workspace_id || undefined;
        const envHost = envResp?.data?.databricks_host;
        if (!workspaceUrl && envHost) {
          workspaceUrl = envHost.startsWith('http') ? envHost : `https://${envHost}`;
        }
      } catch (e) { console.warn('Unable to get Databricks environment info for evaluation'); }
      if (!workspaceUrl || workspaceUrl === 'compute' || !workspaceUrl.includes('.')) {
        if (typeof window !== 'undefined' && window.location.hostname.includes('databricks')) {
          workspaceUrl = `https://${window.location.hostname}`;
        } else {
          try {
            const resp = await apiClient.get('/databricks/workspace-url');
            workspaceUrl = resp.data?.workspace_url || '';
          } catch (e) { console.warn('Unable to determine Databricks workspace URL'); }
        }
      }
      if (!workspaceUrl) return;
      if (workspaceUrl.endsWith('/')) workspaceUrl = workspaceUrl.slice(0, -1);
      if (!workspaceUrl.startsWith('http')) workspaceUrl = `https://${workspaceUrl}`;

      const o = workspaceId ? `?o=${encodeURIComponent(workspaceId)}` : '';
      const url = `${workspaceUrl}/ml/experiments/${encodeURIComponent(experimentId)}/runs/${encodeURIComponent(evalRunId)}${o}`;
      window.open(url, '_blank', 'noopener');
    } catch (e) {
      console.error('Failed to trigger evaluation:', e);
    } finally {
      setIsEvaluationRunning(false);
    }
  };

  if (!open) return null;

  return (
    <>
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: {
          minHeight: '80vh',
          maxHeight: '90vh'
        }
      }}
    >
      <DialogTitle sx={{ m: 0, p: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Execution Trace Timeline</Typography>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            {/* Show action buttons only in new UI mode and when we have the run data */}
            {useNewExecutionUI && run && (
              <>
                <Tooltip title={t('runHistory.actions.viewResult')}>
                  <span>
                    <IconButton
                      size="small"
                      onClick={() => {
                        if (onViewResult && run) {
                          showRunResult(run);
                        }
                      }}
                      color="primary"
                      disabled={['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')}
                    >
                      <PreviewIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>
                <Tooltip title="MLflow Trace">
                  <span>
                    <IconButton
                      size="small"
                      onClick={async () => {
                        const url = await getSpecificTraceUrl();
                        if (url && url !== '#') {
                          window.open(url, '_blank', 'noopener');
                        }
                      }}
                      color="primary"
                      disabled={['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')}
                    >
                      <TimelineIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>

                <Tooltip title={t('runHistory.actions.viewLogs')}>
                  <span>
                    <IconButton
                      size="small"
                      onClick={handleOpenLogs}
                      color="primary"
                      disabled={['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')}
                    >
                      <TerminalIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>

                <Divider orientation="vertical" flexItem sx={{ mx: 1 }} />
                <Button
                  size="small"
                  variant="outlined"
                  startIcon={isEvaluationRunning ? <CircularProgress size={16} /> : <PlayArrowIcon fontSize="small" />}
                  onClick={triggerEvaluationAndOpen}
                  color="primary"
                  disabled={
                    isEvaluationRunning ||
                    !evaluationEnabled ||
                    ['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')
                  }
                >
                  {isEvaluationRunning ? 'Running Evaluation...' : 'Run MLflow Evaluation'}
                </Button>
                <Divider orientation="vertical" flexItem sx={{ mx: 1 }} />
              </>
            )}
            <IconButton
              aria-label="close"
              onClick={onClose}
              sx={{
                color: (theme: Theme) => theme.palette.grey[500],
              }}
            >
              <CloseIcon />
            </IconButton>
          </Box>
        </Box>
      </DialogTitle>

      <DialogContent dividers sx={{ p: 0 }}>
        <TraceTimelineContent {...traceData} />
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose} size="small">
          Close
        </Button>
      </DialogActions>
    </Dialog>

    {/* Show Logs Dialog */}
    {useNewExecutionUI && run && showLogsDialog && (
      <ShowLogs
        open={showLogsDialog}
        onClose={() => setShowLogsDialog(false)}
        logs={logs}
        jobId={run.job_id}
        isConnecting={isLoadingLogs}
        connectionError={logsError}
      />
    )}
  </>
  );
};

export default ShowTraceTimeline;
