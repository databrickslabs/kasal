/**
 * Presentational building blocks for UCMVResultViewer: highlighted SQL/YAML block,
 * stats chips, collapsible section and copy-to-clipboard button.
 */
import React, { useState, useCallback } from 'react';
import {
  Box,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Chip,
  Tooltip,
  IconButton,
  useTheme,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { Highlight, themes } from 'prism-react-renderer';
import type { UCMVStats } from './ucmvResultTypes';

export const SQLBlock: React.FC<{ code: string; language?: string }> = ({ code, language = 'sql' }) => {
  const muiTheme = useTheme();
  const isDark = muiTheme.palette.mode === 'dark';
  const prismTheme = isDark ? themes.oneDark : themes.oneLight;

  return (
    <Highlight theme={prismTheme} code={code.trim()} language={language}>
      {({ style, tokens, getLineProps, getTokenProps }) => (
        <pre
          style={{
            ...style,
            margin: 0,
            padding: 12,
            overflow: 'auto',
            maxHeight: 400,
            fontSize: '0.8rem',
            fontFamily: 'monospace',
            lineHeight: 1.5,
            borderRadius: 4,
          }}
        >
          {tokens.map((line, i) => (
            <div key={i} {...getLineProps({ line })}>
              {line.map((token, key) => (
                <span key={key} {...getTokenProps({ token })} />
              ))}
            </div>
          ))}
        </pre>
      )}
    </Highlight>
  );
};

export const StatsChips: React.FC<{ stats: UCMVStats }> = ({ stats }) => (
  <Box display="flex" gap={1} flexWrap="wrap">
    {stats.total_measures_translated != null && (
      <Chip size="small" label={`${stats.total_measures_translated} translated`} color="success" variant="outlined" />
    )}
    {stats.total_untranslatable != null && (
      <Chip
        size="small"
        label={`${stats.total_untranslatable} untranslatable`}
        color={stats.total_untranslatable > 0 ? 'warning' : 'default'}
        variant="outlined"
      />
    )}
    {stats.total_artifact_measures != null && (
      <Chip
        size="small"
        label={`${stats.total_artifact_measures} artifacts`}
        color={stats.total_artifact_measures > 0 ? 'info' : 'default'}
        variant="outlined"
      />
    )}
    {stats.coverage_pct != null && (
      <Chip
        size="small"
        label={`${Math.round(stats.coverage_pct)}% coverage`}
        color={stats.coverage_pct >= 100 ? 'success' : stats.coverage_pct >= 80 ? 'warning' : 'error'}
        variant="filled"
      />
    )}
  </Box>
);

export const Section: React.FC<{
  title: string;
  icon: React.ReactNode;
  count?: number;
  defaultExpanded?: boolean;
  action?: React.ReactNode;
  children: React.ReactNode;
}> = ({ title, icon, count, defaultExpanded = false, action, children }) => (
  <Accordion defaultExpanded={defaultExpanded} disableGutters variant="outlined" sx={{ '&:before': { display: 'none' } }}>
    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
      <Box display="flex" alignItems="center" gap={1} flexGrow={1}>
        {icon}
        <Typography variant="subtitle2">{title}</Typography>
        {count != null && <Chip size="small" label={count} sx={{ height: 20, fontSize: '0.75rem' }} />}
        {action && <Box sx={{ ml: 'auto', mr: 1 }}>{action}</Box>}
      </Box>
    </AccordionSummary>
    <AccordionDetails sx={{ p: 0, overflow: 'auto' }}>{children}</AccordionDetails>
  </Accordion>
);

export const CopyButton: React.FC<{ content: string }> = ({ content }) => {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // clipboard unavailable (non-https or restricted env)
    }
  }, [content]);
  return (
    <Tooltip title={copied ? 'Copied!' : 'Copy to clipboard'}>
      <IconButton size="small" onClick={handleCopy} sx={{ p: 0.25 }}>
        {copied
          ? <CheckCircleIcon sx={{ fontSize: 14 }} color="success" />
          : <ContentCopyIcon sx={{ fontSize: 14 }} />}
      </IconButton>
    </Tooltip>
  );
};
