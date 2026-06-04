/**
 * ConfigLoader — Load config from file upload, paste JSON, or execution history.
 */

import React, { useState, useRef, useCallback } from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  Tabs,
  Tab,
  Alert,
  Paper,
  CircularProgress,
  MenuItem,
  Select,
  FormControl,
  InputLabel,
  useTheme,
  alpha,
} from '@mui/material';
import {
  UploadFile as UploadIcon,
  ContentPaste as PasteIcon,
  History as HistoryIcon,
} from '@mui/icons-material';
import type { PipelineConfig } from '../../types/configEditor';
import { RunService } from '../../api/ExecutionHistoryService';
import type { Run } from '../../types/run';

interface ConfigLoaderProps {
  onLoad: (config: PipelineConfig, source: string) => void;
}

const ConfigLoader: React.FC<ConfigLoaderProps> = ({ onLoad }) => {
  const theme = useTheme();
  const [tab, setTab] = useState(0);
  const [error, setError] = useState<string | null>(null);

  // File upload
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragActive, setDragActive] = useState(false);

  // Paste
  const [pasteText, setPasteText] = useState('');

  // Execution history
  const [runs, setRuns] = useState<Run[]>([]);
  const [selectedRunId, setSelectedRunId] = useState('');
  const [loadingRuns, setLoadingRuns] = useState(false);
  const [loadingResult, setLoadingResult] = useState(false);

  // ── Parse & validate ──
  const parseAndLoad = useCallback(
    (jsonStr: string, source: string) => {
      setError(null);
      try {
        const parsed = JSON.parse(jsonStr);

        // Unwrap Tool 90 output format: { proposed_config: {...}, summary: {...} }
        const config: PipelineConfig = parsed.proposed_config || parsed;

        // Basic validation: check it's an object with at least some expected keys
        if (typeof config !== 'object' || Array.isArray(config)) {
          throw new Error('Expected a JSON object, not an array or primitive');
        }

        const knownKeys = [
          'join_key_map', 'enrichment_joins', 'switch_decompositions',
          'filter_sets', 'measure_resolutions', 'column_metadata',
        ];
        const hasKnown = knownKeys.some((k) => k in config);
        if (!hasKnown && Object.keys(config).length > 0) {
          // Warn but still load — could be partial config
          console.warn('Loaded JSON does not contain typical pipeline_config keys');
        }

        onLoad(config, source);
      } catch (e) {
        setError(`Failed to parse JSON: ${(e as Error).message}`);
      }
    },
    [onLoad],
  );

  // ── File upload handlers ──
  const handleFile = useCallback(
    (file: File) => {
      if (!file.name.endsWith('.json')) {
        setError('Please upload a .json file');
        return;
      }
      const reader = new FileReader();
      reader.onload = () => {
        parseAndLoad(reader.result as string, `File: ${file.name}`);
      };
      reader.onerror = () => setError('Failed to read file');
      reader.readAsText(file);
    },
    [parseAndLoad],
  );

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragActive(false);
      const file = e.dataTransfer.files[0];
      if (file) handleFile(file);
    },
    [handleFile],
  );

  // ── Execution history ──
  const loadRuns = useCallback(async () => {
    setLoadingRuns(true);
    setError(null);
    try {
      const runService = RunService.getInstance();
      const response = await runService.getRuns(50, 0);
      // Filter to completed runs only
      const completedRuns = response.runs.filter(
        (r) => r.status === 'COMPLETED',
      );
      setRuns(completedRuns);
      if (completedRuns.length === 0) {
        setError('No completed runs found');
      }
    } catch (e) {
      setError(`Failed to load runs: ${(e as Error).message}`);
    } finally {
      setLoadingRuns(false);
    }
  }, []);

  const loadRunResult = useCallback(async () => {
    if (!selectedRunId) return;
    setLoadingResult(true);
    setError(null);
    try {
      const runService = RunService.getInstance();
      const run = await runService.getRunByJobId(selectedRunId);
      if (!run?.result) {
        setError('This run has no result data');
        return;
      }

      // Result could be nested — look for proposed_config or pipeline_config
      const resultStr = JSON.stringify(run.result);
      parseAndLoad(resultStr, `Run: ${run.run_name || run.job_id}`);
    } catch (e) {
      setError(`Failed to load result: ${(e as Error).message}`);
    } finally {
      setLoadingResult(false);
    }
  }, [selectedRunId, parseAndLoad]);

  return (
    <Paper
      variant="outlined"
      sx={{
        p: 2,
        backgroundColor: alpha(theme.palette.background.paper, 0.8),
      }}
    >
      <Tabs value={tab} onChange={(_, v) => { setTab(v); setError(null); }} sx={{ mb: 2 }}>
        <Tab icon={<UploadIcon />} label="File Upload" iconPosition="start" sx={{ minHeight: 48 }} />
        <Tab icon={<PasteIcon />} label="Paste JSON" iconPosition="start" sx={{ minHeight: 48 }} />
        <Tab icon={<HistoryIcon />} label="Execution History" iconPosition="start" sx={{ minHeight: 48 }} />
      </Tabs>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {/* Tab 0: File Upload */}
      {tab === 0 && (
        <Box
          onDragOver={(e) => { e.preventDefault(); setDragActive(true); }}
          onDragLeave={() => setDragActive(false)}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
          sx={{
            border: `2px dashed ${dragActive ? theme.palette.primary.main : theme.palette.divider}`,
            borderRadius: 2,
            p: 4,
            textAlign: 'center',
            cursor: 'pointer',
            backgroundColor: dragActive
              ? alpha(theme.palette.primary.main, 0.04)
              : 'transparent',
            transition: 'all 0.2s',
            '&:hover': {
              borderColor: theme.palette.primary.main,
              backgroundColor: alpha(theme.palette.primary.main, 0.02),
            },
          }}
        >
          <UploadIcon sx={{ fontSize: 48, color: 'text.secondary', mb: 1 }} />
          <Typography variant="body1">
            Drop <code>pipeline_config.json</code> here or click to browse
          </Typography>
          <Typography variant="caption" color="text.secondary">
            Accepts the raw config or Tool 90 output (with proposed_config wrapper)
          </Typography>
          <input
            ref={fileInputRef}
            type="file"
            accept=".json"
            hidden
            onChange={(e) => {
              const file = e.target.files?.[0];
              if (file) handleFile(file);
            }}
          />
        </Box>
      )}

      {/* Tab 1: Paste JSON */}
      {tab === 1 && (
        <Box>
          <TextField
            fullWidth
            multiline
            minRows={8}
            maxRows={20}
            placeholder='Paste pipeline_config.json content here...'
            value={pasteText}
            onChange={(e) => setPasteText(e.target.value)}
            sx={{
              '& .MuiInputBase-input': { fontFamily: 'monospace', fontSize: '0.85rem' },
            }}
          />
          <Button
            variant="contained"
            sx={{ mt: 1 }}
            disabled={!pasteText.trim()}
            onClick={() => parseAndLoad(pasteText, 'Pasted JSON')}
          >
            Load Config
          </Button>
        </Box>
      )}

      {/* Tab 2: Execution History */}
      {tab === 2 && (
        <Box>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'flex-end', mb: 2 }}>
            <FormControl fullWidth size="small">
              <InputLabel>Select a completed run</InputLabel>
              <Select
                value={selectedRunId}
                label="Select a completed run"
                onChange={(e) => setSelectedRunId(e.target.value)}
                disabled={loadingRuns}
              >
                {runs.map((run) => (
                  <MenuItem key={run.job_id} value={run.job_id}>
                    {run.run_name || run.job_id} — {run.created_at?.slice(0, 16)}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Button
              variant="outlined"
              onClick={loadRuns}
              disabled={loadingRuns}
              startIcon={loadingRuns ? <CircularProgress size={16} /> : <HistoryIcon />}
              sx={{ whiteSpace: 'nowrap' }}
            >
              {runs.length > 0 ? 'Refresh' : 'Load Runs'}
            </Button>
          </Box>
          <Button
            variant="contained"
            disabled={!selectedRunId || loadingResult}
            onClick={loadRunResult}
            startIcon={loadingResult ? <CircularProgress size={16} /> : undefined}
          >
            Load Result
          </Button>
        </Box>
      )}
    </Paper>
  );
};

export default ConfigLoader;
