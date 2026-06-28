import React from 'react';
import Box from '@mui/material/Box';
import { TRACE_DETAIL_RENDERERS } from './registry';

/**
 * Generic expanded-trace-detail dispatcher. Picks the first registered
 * tool-specific renderer that matches (e.g. Genie → sections + table + chart);
 * otherwise falls back to a plain monospace block. Tool-specific UI lives in
 * the registry, not here — so this stays small and ChatMessage stays lean.
 *
 * `indent` is the left indent in MUI spacing units (8px each): 1.5 = 12px.
 */
export const TraceDetail: React.FC<{ detail: string; label?: string; indent?: number }> = ({
  detail,
  label,
  indent = 1.5,
}) => {
  const renderer = TRACE_DETAIL_RENDERERS.find((r) => r.match(detail, label));
  if (renderer) {
    const Component = renderer.Component;
    return <Component detail={detail} indent={indent} />;
  }
  // Rendered as a <Box> (div), not <pre>, so the chat's global `pre` rule (a
  // dark code-block style) doesn't override it — the monospace / line-height /
  // bottom-margin that the <pre> + that rule used to supply are set here.
  return (
    <Box
      sx={{
        mt: 0.5,
        ml: indent,
        mb: 1.5,
        fontSize: 11,
        fontFamily: 'monospace',
        whiteSpace: 'pre-wrap',
        overflowWrap: 'break-word',
        lineHeight: 1.6,
        borderRadius: '4px',
        p: 1,
        maxWidth: '85%',
        maxHeight: 288,
        overflowY: 'auto',
        color: 'text.primary',
        backgroundColor: 'background.default',
        border: 1,
        borderColor: 'divider',
      }}
    >
      {detail}
    </Box>
  );
};
