/**
 * Learn-from-corrections panel.
 *
 * The flywheel: upload the UC Metric View YAMLs the customer validated & deployed,
 * the backend diffs them against our original output and distils a reusable
 * "domain context" README. One click applies it to the Domain context field so the
 * NEXT generation starts closer to what the customer actually wants.
 */

import React from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  Alert,
  Chip,
  CircularProgress
} from '@mui/material';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import DownloadIcon from '@mui/icons-material/Download';
import { UcmvLearningService, UcmvLearnResponse } from '../../../../api/tools/UcmvLearningService';

interface UCMVLearningPanelProps {
  /** Apply the distilled README into the Domain context field. */
  onApplyDomainContext: (readme: string) => void;
  /** Label of the apply button. */
  applyLabel?: string;
  disabled?: boolean;
}

/** Derive the view key from an uploaded file name, matching how the generator
 *  names its downloads (`<view>_uc_metric_view.yml`). */
function viewNameFromFile(fileName: string): string {
  return fileName
    .replace(/\.ya?ml$/i, '')
    .replace(/_uc_metric_view$/i, '')
    .trim();
}

export const UCMVLearningPanel: React.FC<UCMVLearningPanelProps> = ({
  onApplyDomainContext,
  applyLabel = 'Use as Domain context',
  disabled = false
}) => {
  const [corrected, setCorrected] = React.useState<Record<string, string>>({});
  const [modelHint, setModelHint] = React.useState<string>('');
  const [loading, setLoading] = React.useState<boolean>(false);
  const [result, setResult] = React.useState<UcmvLearnResponse | null>(null);
  const [error, setError] = React.useState<string>('');

  const views = Object.keys(corrected);

  const handleUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files || []);
    if (!files.length) return;
    Promise.all(
      files.map(
        (file) =>
          new Promise<[string, string]>((resolve) => {
            const reader = new FileReader();
            reader.onload = (e) =>
              resolve([viewNameFromFile(file.name), (e.target?.result as string) || '']);
            reader.readAsText(file);
          })
      )
    ).then((pairs) => {
      setCorrected((prev) => {
        const next = { ...prev };
        for (const [view, text] of pairs) {
          if (view && text) next[view] = text;
        }
        return next;
      });
    });
    event.target.value = '';
  };

  const handleAnalyze = async () => {
    setLoading(true);
    setError('');
    setResult(null);
    try {
      const resp = await UcmvLearningService.learn({
        corrected,
        model_hint: modelHint || undefined
      });
      setResult(resp);
      if (resp.error) setError(resp.error);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Analysis failed');
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = () => {
    if (!result?.readme) return;
    const blob = new Blob([result.readme], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'learned_domain_context.md';
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, mt: 1 }}>
      <Typography variant="caption" color="text.secondary">
        Already deployed corrected UCMVs? Upload them and we&apos;ll diff them against our
        original output and draft the domain context for you — the learning flywheel.
      </Typography>

      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
        <Button
          variant="outlined"
          component="label"
          size="small"
          startIcon={<UploadFileIcon />}
          disabled={disabled || loading}
        >
          Upload corrected UCMV YAMLs
          <input
            type="file"
            accept=".yml,.yaml"
            multiple
            hidden
            onChange={handleUpload}
          />
        </Button>
        {views.map((v) => (
          <Chip
            key={v}
            label={v}
            size="small"
            onDelete={() =>
              setCorrected((prev) => {
                const next = { ...prev };
                delete next[v];
                return next;
              })
            }
          />
        ))}
      </Box>

      <TextField
        label="Model / report name (optional)"
        value={modelHint}
        onChange={(e) => setModelHint(e.target.value)}
        disabled={disabled || loading}
        fullWidth
        size="small"
        placeholder="e.g. Total Supply Chain"
      />

      <Box>
        <Button
          variant="contained"
          size="small"
          startIcon={loading ? <CircularProgress size={16} /> : <AutoAwesomeIcon />}
          onClick={handleAnalyze}
          disabled={disabled || loading || views.length === 0}
        >
          {loading ? 'Analysing corrections…' : 'Generate learned context'}
        </Button>
      </Box>

      {error && (
        <Alert severity="error" variant="outlined">
          <Typography variant="caption">{error}</Typography>
        </Alert>
      )}

      {result && !result.readme && !error && (
        <Alert severity="info" variant="outlined">
          <Typography variant="caption">
            {result.note || 'No reusable guidance was produced.'}
            {result.unmatched_views.length > 0 &&
              ` Unmatched views (no original found): ${result.unmatched_views.join(', ')}.`}
          </Typography>
        </Alert>
      )}

      {result?.readme && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          <Typography variant="caption" color="text.secondary">
            Learned from {result.views_with_changes} changed view(s)
            {result.fuzzy_matched && Object.keys(result.fuzzy_matched).length > 0 &&
              ` · similarity-matched: ${Object.entries(result.fuzzy_matched)
                .map(([v, m]) => `${v}→${m.matched_to}`)
                .join(', ')}`}
            {result.unmatched_views.length > 0 &&
              ` · unmatched: ${result.unmatched_views.join(', ')}`}
          </Typography>
          <Box
            component="pre"
            sx={{
              m: 0,
              p: 1,
              fontFamily: 'monospace',
              fontSize: '0.75rem',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              bgcolor: 'action.hover',
              borderRadius: 1,
              maxHeight: 300,
              overflow: 'auto'
            }}
          >
            {result.readme}
          </Box>
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Button
              variant="contained"
              size="small"
              startIcon={<AutoAwesomeIcon />}
              onClick={() => onApplyDomainContext(result.readme as string)}
              disabled={disabled}
            >
              {applyLabel}
            </Button>
            <Button
              variant="outlined"
              size="small"
              startIcon={<DownloadIcon />}
              onClick={handleDownload}
            >
              Download
            </Button>
          </Box>
        </Box>
      )}
    </Box>
  );
};
