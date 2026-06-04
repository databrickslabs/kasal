/**
 * ConfigEditorPage — standalone /config-editor page.
 *
 * Two-column layout:
 *   Left:  key list sidebar with color-coded status badges
 *   Right: editor panel for the selected key
 *
 * Load from: file upload, paste JSON, or execution history.
 * Save as:  download JSON file.
 */

import React, { useState, useCallback, useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Button,
  Paper,
  Divider,
  Alert,
  Snackbar,
  useTheme,
  alpha,
  Tooltip,
  Chip,
  CircularProgress,
} from '@mui/material';
import {
  FileDownload as DownloadIcon,
  FolderOpen as LoadIcon,
  Undo as UndoIcon,
  Save as SaveIcon,
  CheckCircle as ApproveIcon,
  ArrowBack as BackIcon,
} from '@mui/icons-material';
import type { PipelineConfig } from '../../types/configEditor';
import { getKeyStatus, countTodos } from '../../types/configEditor';
import ConfigSidebar from './ConfigSidebar';
import KeyEditor from './KeyEditor';
import ConfigLoader from './ConfigLoader';
import { runService } from '../../api/ExecutionHistoryService';
import { HITLService } from '../../api/HITLService';

const ConfigEditorPage: React.FC = () => {
  const theme = useTheme();
  const location = useLocation();
  const navigate = useNavigate();

  const [config, setConfig] = useState<PipelineConfig | null>(null);
  const [originalConfig, setOriginalConfig] = useState<PipelineConfig | null>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [source, setSource] = useState<string>('');
  const [showLoader, setShowLoader] = useState(true);
  const [hasChanges, setHasChanges] = useState(false);

  // Execution / HITL state from route
  const [jobId, setJobId] = useState<string | null>(null);
  const [approvalId, setApprovalId] = useState<number | null>(null);
  const [saving, setSaving] = useState(false);
  const [snackbar, setSnackbar] = useState<{ open: boolean; message: string; severity: 'success' | 'error' }>({
    open: false,
    message: '',
    severity: 'success',
  });

  // ── Load config ──
  const handleLoad = useCallback((loaded: PipelineConfig, src: string) => {
    setConfig(loaded);
    setOriginalConfig(JSON.parse(JSON.stringify(loaded)));
    setSource(src);
    setShowLoader(false);
    setHasChanges(false);

    // Auto-select first key
    const keys = Object.keys(loaded);
    if (keys.length > 0) {
      setSelectedKey(keys[0]);
    }
  }, []);

  // ── Auto-load from route state (when navigating from ShowResult or HITL dialog) ──
  useEffect(() => {
    const state = location.state as {
      config?: PipelineConfig;
      source?: string;
      jobId?: string;
      approvalId?: number;
    } | null;
    if (state?.config) {
      handleLoad(state.config, state.source || 'Execution Result');
      if (state.jobId) setJobId(state.jobId);
      if (state.approvalId) setApprovalId(state.approvalId);
      // Clear the state so refreshing doesn't re-load
      window.history.replaceState({}, document.title);
    }
  }, [location.state, handleLoad]);

  // ── Edit a key ──
  const handleKeyChange = useCallback((key: string, newValue: unknown) => {
    if (!config) return;
    setConfig((prev) => {
      if (!prev) return prev;
      return { ...prev, [key]: newValue };
    });
    setHasChanges(true);
  }, [config]);

  // ── Undo all changes ──
  const handleUndo = useCallback(() => {
    if (originalConfig) {
      setConfig(JSON.parse(JSON.stringify(originalConfig)));
      setHasChanges(false);
    }
  }, [originalConfig]);

  // ── Download JSON ──
  const handleDownload = useCallback(() => {
    if (!config) return;
    const json = JSON.stringify(config, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'pipeline_config.json';
    a.click();
    URL.revokeObjectURL(url);
  }, [config]);

  // ── Save to execution DB ──
  const handleSaveToExecution = useCallback(async () => {
    if (!config || !jobId) return;
    setSaving(true);
    try {
      await runService.updateExecutionResult(jobId, config);
      setSnackbar({ open: true, message: 'Config saved to execution', severity: 'success' });
      setHasChanges(false);
      setOriginalConfig(JSON.parse(JSON.stringify(config)));
    } catch {
      setSnackbar({ open: true, message: 'Failed to save config', severity: 'error' });
    } finally {
      setSaving(false);
    }
  }, [config, jobId]);

  // ── Save & Approve HITL flow ──
  const handleSaveAndApprove = useCallback(async () => {
    if (!config || !jobId || !approvalId) return;
    setSaving(true);
    try {
      await runService.updateExecutionResult(jobId, config);
      await HITLService.approveGate(approvalId, {
        comment: 'Config reviewed and approved via Config Editor',
      });
      setSnackbar({ open: true, message: 'Config saved and flow resumed', severity: 'success' });
      setHasChanges(false);
      setOriginalConfig(JSON.parse(JSON.stringify(config)));
      setApprovalId(null); // gate consumed
    } catch {
      setSnackbar({ open: true, message: 'Failed to save or approve', severity: 'error' });
    } finally {
      setSaving(false);
    }
  }, [config, jobId, approvalId]);

  // ── Summary stats ──
  const summaryInfo = config ? (() => {
    const keys = Object.keys(config);
    let auto = 0, todo = 0, empty = 0, nullCount = 0, totalTodos = 0;
    keys.forEach((k) => {
      const s = getKeyStatus(config[k]);
      if (s === 'auto') auto++;
      else if (s === 'todo') todo++;
      else if (s === 'empty') empty++;
      else nullCount++;
      totalTodos += countTodos(config[k]);
    });
    return { total: keys.length, auto, todo, empty, nullCount, totalTodos };
  })() : null;

  return (
    <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      {/* ── Top bar ── */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          px: 3,
          py: 1.5,
          borderBottom: `1px solid ${theme.palette.divider}`,
          backgroundColor: theme.palette.background.paper,
        }}
      >
        <Tooltip title="Back to previous page">
          <Button
            variant="text"
            size="small"
            startIcon={<BackIcon />}
            onClick={() => navigate(-1)}
            sx={{ minWidth: 'auto', mr: 1 }}
          >
            Back
          </Button>
        </Tooltip>

        <Typography variant="h5" sx={{ fontWeight: 600, flex: 1 }}>
          Pipeline Config Editor
        </Typography>

        {summaryInfo && (
          <Box sx={{ display: 'flex', gap: 0.5 }}>
            <Chip label={`${summaryInfo.total} keys`} size="small" variant="outlined" />
            {summaryInfo.totalTodos > 0 && (
              <Chip
                label={`${summaryInfo.totalTodos} TODOs`}
                size="small"
                color="warning"
                variant="outlined"
              />
            )}
          </Box>
        )}

        {hasChanges && (
          <Tooltip title="Undo all changes">
            <Button
              variant="outlined"
              size="small"
              color="warning"
              startIcon={<UndoIcon />}
              onClick={handleUndo}
            >
              Undo All
            </Button>
          </Tooltip>
        )}

        <Button
          variant="outlined"
          size="small"
          startIcon={<LoadIcon />}
          onClick={() => setShowLoader(true)}
        >
          Load
        </Button>

        <Button
          variant="contained"
          size="small"
          startIcon={<DownloadIcon />}
          disabled={!config}
          onClick={handleDownload}
        >
          Save JSON
        </Button>

        {jobId && (
          <Button
            variant="contained"
            size="small"
            color="primary"
            startIcon={saving ? <CircularProgress size={16} color="inherit" /> : <SaveIcon />}
            disabled={!config || saving}
            onClick={handleSaveToExecution}
          >
            Save to Execution
          </Button>
        )}

        {approvalId && jobId && (
          <Button
            variant="contained"
            size="small"
            color="success"
            startIcon={saving ? <CircularProgress size={16} color="inherit" /> : <ApproveIcon />}
            disabled={!config || saving}
            onClick={handleSaveAndApprove}
          >
            Save &amp; Approve Flow
          </Button>
        )}
      </Box>

      {/* ── Loader overlay ── */}
      {showLoader && (
        <Box sx={{ px: 3, py: 2, borderBottom: `1px solid ${theme.palette.divider}` }}>
          <ConfigLoader onLoad={handleLoad} />
          {config && (
            <Button
              variant="text"
              size="small"
              sx={{ mt: 1 }}
              onClick={() => setShowLoader(false)}
            >
              Cancel — keep current config
            </Button>
          )}
        </Box>
      )}

      {/* ── Main content ── */}
      {!config && !showLoader && (
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', flex: 1 }}>
          <Alert severity="info" variant="outlined">
            No config loaded. Click <strong>Load</strong> to get started.
          </Alert>
        </Box>
      )}

      {config && (
        <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          {/* Left sidebar */}
          <Paper
            variant="outlined"
            sx={{
              width: 280,
              minWidth: 280,
              borderRadius: 0,
              borderTop: 'none',
              borderBottom: 'none',
              borderLeft: 'none',
              overflow: 'hidden',
              display: 'flex',
              flexDirection: 'column',
            }}
          >
            {source && (
              <Box sx={{ px: 2, py: 1, borderBottom: `1px solid ${theme.palette.divider}`, backgroundColor: alpha(theme.palette.info.main, 0.04) }}>
                <Typography variant="caption" color="text.secondary" noWrap>
                  Source: {source}
                </Typography>
              </Box>
            )}
            <ConfigSidebar
              config={config}
              selectedKey={selectedKey}
              onSelectKey={setSelectedKey}
            />
          </Paper>

          {/* Divider */}
          <Divider orientation="vertical" flexItem />

          {/* Right editor panel */}
          <Box
            sx={{
              flex: 1,
              overflow: 'auto',
              p: 3,
              backgroundColor: alpha(theme.palette.background.default, 0.3),
            }}
          >
            {selectedKey && selectedKey in config ? (
              <KeyEditor
                configKey={selectedKey}
                value={config[selectedKey]}
                onChange={handleKeyChange}
              />
            ) : (
              <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
                <Typography color="text.secondary">
                  Select a key from the sidebar to edit
                </Typography>
              </Box>
            )}
          </Box>
        </Box>
      )}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={4000}
        onClose={() => setSnackbar((s) => ({ ...s, open: false }))}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert
          severity={snackbar.severity}
          onClose={() => setSnackbar((s) => ({ ...s, open: false }))}
          variant="filled"
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default ConfigEditorPage;
