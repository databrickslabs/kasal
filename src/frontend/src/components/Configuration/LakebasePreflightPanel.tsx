import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Alert,
  AlertTitle,
  Button,
  Typography,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  CircularProgress,
  IconButton,
  Tooltip,
} from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import HealthAndSafetyIcon from '@mui/icons-material/HealthAndSafety';
import { apiClient } from '../../config/api/ApiConfig';

interface PreflightCheck { name: string; ok: boolean; detail: string; }
interface PreflightRemediation { summary: string; steps: string[]; commands: string[]; }
interface PreflightReport {
  status: 'healthy' | 'action_required' | 'error';
  current_user?: string | null;
  checks: PreflightCheck[];
  remediation?: PreflightRemediation | null;
}

interface Props {
  /** Instance to diagnose. Falls back to the configured instance server-side. */
  instanceName?: string;
  /** Run the diagnostic automatically on mount (e.g. when Lakebase is enabled). */
  autoRun?: boolean;
  /** A report already returned by test-connection/enable, to render without re-fetching. */
  initialReport?: PreflightReport | null;
}

/**
 * Runs the Lakebase connect preflight and renders the result: a green summary
 * when the app's service principal can operate the schema, or the exact
 * remediation (and copyable SQL) when it cannot. Self-contained so the large
 * DatabaseManagement component only needs a one-line render.
 */
const LakebasePreflightPanel: React.FC<Props> = ({ instanceName, autoRun, initialReport }) => {
  const [report, setReport] = useState<PreflightReport | null>(initialReport ?? null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await apiClient.post<PreflightReport>(
        '/database-management/lakebase/preflight',
        instanceName ? { instance_name: instanceName } : {},
      );
      setReport(resp.data);
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to run diagnostics';
      setError(msg);
    } finally {
      setLoading(false);
    }
  }, [instanceName]);

  useEffect(() => {
    if (autoRun && !initialReport) {
      run();
    }
  }, [autoRun, initialReport, run]);

  const copy = (text: string) => {
    navigator.clipboard?.writeText(text);
  };

  return (
    <Box sx={{ mt: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <Typography variant="subtitle1" sx={{ display: 'flex', alignItems: 'center' }}>
          <HealthAndSafetyIcon sx={{ mr: 1 }} />
          Connection diagnostics
        </Typography>
        <Button size="small" variant="outlined" onClick={run} disabled={loading}>
          {loading ? <CircularProgress size={16} /> : 'Run diagnostics'}
        </Button>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 1 }}>{error}</Alert>
      )}

      {report && report.status === 'healthy' && (
        <Alert severity="success" icon={<CheckCircleIcon />}>
          <AlertTitle>All checks passed</AlertTitle>
          This app&apos;s service principal ({report.current_user}) can create and update the
          schema Kasal needs.
          <List dense sx={{ mt: 0.5 }}>
            {report.checks.map((c) => (
              <ListItem key={c.name} sx={{ py: 0 }}>
                <ListItemIcon sx={{ minWidth: 28 }}>
                  {c.ok ? <CheckCircleIcon color="success" fontSize="small" />
                        : <WarningAmberIcon color="warning" fontSize="small" />}
                </ListItemIcon>
                <ListItemText primary={c.name} secondary={c.detail} />
              </ListItem>
            ))}
          </List>
        </Alert>
      )}

      {report && report.status === 'error' && (
        <Alert severity="error" icon={<ErrorIcon />}>
          <AlertTitle>Diagnostics could not run</AlertTitle>
          {report.checks.map((c) => (
            <div key={c.name}>{c.name}: {c.detail}</div>
          ))}
        </Alert>
      )}

      {report && report.status === 'action_required' && report.remediation && (
        <Alert severity="warning" icon={<WarningAmberIcon />}>
          <AlertTitle>Action required — Kasal cannot update the database schema</AlertTitle>
          <Typography variant="body2" sx={{ mb: 1 }}>{report.remediation.summary}</Typography>

          <Typography variant="subtitle2" sx={{ mt: 1 }}>How to fix</Typography>
          <List dense component="ol" sx={{ listStyle: 'decimal', pl: 3 }}>
            {report.remediation.steps.map((s, i) => (
              <ListItem key={i} sx={{ display: 'list-item', py: 0.25 }}>
                <ListItemText primary={s} />
              </ListItem>
            ))}
          </List>

          {report.remediation.commands.length > 0 && (
            <>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1 }}>
                <Typography variant="subtitle2">
                  Reference SQL (run by a privileged Postgres identity)
                </Typography>
                <Tooltip title="Copy">
                  <IconButton size="small" onClick={() => copy(report.remediation!.commands.join('\n'))}>
                    <ContentCopyIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Box>
              <Box
                component="pre"
                sx={{
                  bgcolor: 'grey.900', color: 'grey.100', p: 1.5, borderRadius: 1,
                  fontSize: '0.8rem', overflowX: 'auto', mt: 0.5,
                }}
              >
                {report.remediation.commands.join('\n')}
              </Box>
            </>
          )}
        </Alert>
      )}
    </Box>
  );
};

export default LakebasePreflightPanel;
