import React from 'react';
import Box from '@mui/material/Box';
import MessageContent from '../MessageContent';

/**
 * Renderer for a text-answer tool result (e.g. Perplexity, scraped page) shown
 * INSIDE the collapsible trace pill: the answer as Markdown in an indented,
 * scrollable panel — readable but tucked away (these answers are often long).
 * Returns null when there's no content.
 *
 * Accepts the full inline-trace props (label / sublabel / durationMs) for
 * registry compatibility, but renders only the answer body. `indent` is the left
 * indent in MUI spacing units (8px each): 1.5 = 12px.
 */
export const ToolResultCard: React.FC<{
  detail: string;
  label?: string;
  sublabel?: string;
  durationMs?: number;
  indent?: number;
}> = ({ detail, indent = 1.5 }) => {
  if (!detail || !detail.trim()) return null;
  return (
    <Box
      sx={{
        mt: 0.5,
        ml: indent,
        maxWidth: '85%',
        borderRadius: '4px',
        p: 1.5,
        maxHeight: 384,
        overflowY: 'auto',
        fontSize: 14,
        lineHeight: 1.625,
        color: 'text.primary',
        backgroundColor: 'background.default',
        border: 1,
        borderColor: 'divider',
      }}
    >
      <MessageContent content={detail} />
    </Box>
  );
};
