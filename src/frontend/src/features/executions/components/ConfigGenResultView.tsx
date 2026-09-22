/**
 * ConfigGenResultView — result view for the Pipeline Config Generator step.
 *
 * The generator's output is a large JSON (proposed_config + the extracted
 * measures_json / mquery_json / relationships_json handed to the UC Metric View
 * Generator). Rendered raw it is an unreadable dump, so this shows a compact
 * summary plus — the point of it — buttons to DOWNLOAD each extracted artifact
 * as its own JSON file (and a full bundle).
 */
import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Box, Button, Chip, Paper, Typography } from '@mui/material';
import DownloadIcon from '@mui/icons-material/Download';
import DataObjectIcon from '@mui/icons-material/DataObject';
import EditNoteIcon from '@mui/icons-material/EditNote';

type Cfg = Record<string, unknown>;

/** Detect the Pipeline Config Generator output shape. */
// eslint-disable-next-line react-refresh/only-export-components
export function isPipelineConfigResult(value: unknown): boolean {
  if (typeof value !== 'object' || value === null) return false;
  const o = value as Cfg;
  // proposed_config is always emitted; measures_json/mquery_json are the
  // extracted handoff arrays. Require proposed_config plus at least one array so
  // this never matches a UCMV/validator result.
  return (
    'proposed_config' in o &&
    ('measures_json' in o || 'mquery_json' in o || 'relationships_json' in o)
  );
}

function downloadJson(data: unknown, filename: string): void {
  const blob = new Blob([JSON.stringify(data, null, 2)], {
    type: 'application/json;charset=utf-8',
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

const ARTIFACTS: Array<{ key: string; file: string; label: string }> = [
  { key: 'measures_json', file: 'measures.json', label: 'Measures JSON' },
  { key: 'mquery_json', file: 'mquery.json', label: 'M-Query JSON' },
  { key: 'relationships_json', file: 'relationships.json', label: 'Relationships JSON' },
  { key: 'proposed_config', file: 'pipeline_config.json', label: 'Pipeline Config' },
];

function count(v: unknown): number | null {
  if (Array.isArray(v)) return v.length;
  if (v && typeof v === 'object') return Object.keys(v as object).length;
  return null;
}

const ConfigGenResultView: React.FC<{ cfg: Record<string, unknown>; jobId?: string }> = ({ cfg, jobId }) => {
  const navigate = useNavigate();
  // The review/edit page (per-key status, TODO triage, edit + save-back) lives at
  // /config-editor; open it with the extracted proposed_config so reviewers can
  // work through it here, not only at the approval gate.
  const configForReview = (cfg.proposed_config ?? cfg) as Record<string, unknown>;
  const openConfigEditor = () =>
    navigate('/config-editor', {
      state: { config: configForReview, source: 'BI migration result', jobId },
    });

  const available = ARTIFACTS.filter((a) => {
    const c = count(cfg[a.key]);
    return cfg[a.key] != null && (c === null || c > 0);
  });
  const measures = count(cfg['measures_json']);
  const mquery = count(cfg['mquery_json']);
  const relationships = count(cfg['relationships_json']);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
      <Box display="flex" alignItems="center" gap={1.5} flexWrap="wrap">
        <DataObjectIcon color="primary" />
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          Pipeline Config — Extracted Artifacts
        </Typography>
        {measures != null && <Chip size="small" label={`${measures} measures`} variant="outlined" />}
        {mquery != null && <Chip size="small" label={`${mquery} M-Query tables`} variant="outlined" />}
        {relationships != null && (
          <Chip size="small" label={`${relationships} relationships`} variant="outlined" />
        )}
        {configForReview && Object.keys(configForReview).length > 0 && (
          <Button
            size="small"
            variant="outlined"
            startIcon={<EditNoteIcon />}
            onClick={openConfigEditor}
            sx={{ ml: 'auto' }}
          >
            Review &amp; edit config
          </Button>
        )}
      </Box>

      {available.length > 0 && (
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', alignItems: 'center' }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600, mr: 0.5 }}>
            Download extracted JSON:
          </Typography>
          {available.map((a) => (
            <Button
              key={a.key}
              size="small"
              variant="outlined"
              startIcon={<DownloadIcon />}
              onClick={() => downloadJson(cfg[a.key], a.file)}
            >
              {a.label}
            </Button>
          ))}
          <Button
            size="small"
            variant="text"
            startIcon={<DownloadIcon />}
            onClick={() => downloadJson(cfg, 'pipeline_config_full.json')}
          >
            All (bundle)
          </Button>
        </Box>
      )}

      {cfg['summary'] != null && (
        <Paper variant="outlined" sx={{ p: 1.5, bgcolor: 'background.default' }}>
          <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600 }}>
            Summary
          </Typography>
          <Box
            component="pre"
            sx={{
              m: 0,
              mt: 0.5,
              fontSize: '0.78rem',
              fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              maxHeight: 260,
              overflow: 'auto',
            }}
          >
            {typeof cfg['summary'] === 'string'
              ? (cfg['summary'] as string)
              : JSON.stringify(cfg['summary'], null, 2)}
          </Box>
        </Paper>
      )}
    </Box>
  );
};

export default ConfigGenResultView;
