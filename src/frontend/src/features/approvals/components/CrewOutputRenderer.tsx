/**
 * CrewOutputRenderer
 *
 * Renders a crew's previous output for the HITL approval dialog. Crew output is
 * typically GitHub-flavored Markdown (often tables); some crews emit a full HTML
 * document. This renders Markdown nicely (incl. tables via remark-gfm) and, when
 * the content is an HTML document, shows it in a sandboxed iframe instead of raw
 * text.
 */
import React, { useMemo } from 'react';
import { Box, Paper, useTheme } from '@mui/material';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface CrewOutputRendererProps {
  content: string;
  /** Max height of the scroll/preview area (px number or any CSS length). */
  maxHeight?: number | string;
}

/** Heuristic: is the content a full/standalone HTML document (vs Markdown)? */
export const looksLikeHtmlDocument = (raw: string): boolean => {
  const s = (raw || '').trim();
  if (!s) return false;
  return (
    /<!DOCTYPE\s+html/i.test(s) ||
    /<html[\s>]/i.test(s) ||
    /<body[\s>]/i.test(s)
  );
};

export const CrewOutputRenderer: React.FC<CrewOutputRendererProps> = ({
  content,
  maxHeight = 320,
}) => {
  const theme = useTheme();
  const isHtml = useMemo(() => looksLikeHtmlDocument(content), [content]);

  if (isHtml) {
    // Sandboxed iframe with NO allow-scripts: renders HTML/CSS visually for the
    // reviewer without executing untrusted JavaScript.
    return (
      <Paper
        variant="outlined"
        sx={{ p: 0, overflow: 'hidden', bgcolor: 'background.default' }}
      >
        <iframe
          title="Previous Crew Output"
          srcDoc={content}
          sandbox=""
          style={{
            width: '100%',
            height: maxHeight,
            border: 'none',
            backgroundColor: '#ffffff',
          }}
        />
      </Paper>
    );
  }

  const codeBg =
    theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.04)' : 'rgba(255, 255, 255, 0.1)';
  // Sticky table headers MUST be opaque, otherwise scrolled rows bleed through
  // the (translucent) codeBg and the header text overlaps the data.
  const headerBg =
    theme.palette.mode === 'light' ? theme.palette.grey[100] : theme.palette.grey[900];

  return (
    <Paper
      variant="outlined"
      sx={{ p: 1.5, maxHeight, overflow: 'auto', bgcolor: 'background.default' }}
    >
      <Box
        sx={{
          fontSize: '0.85rem',
          wordBreak: 'break-word',
          '& > :first-of-type': { mt: 0 },
          '& > :last-child': { mb: 0 },
          '& p': { marginBottom: theme.spacing(1) },
          '& h1, & h2, & h3, & h4, & h5, & h6': {
            marginTop: theme.spacing(1.5),
            marginBottom: theme.spacing(1),
            fontWeight: 600,
          },
          '& ul, & ol': { paddingLeft: theme.spacing(2.5), marginBottom: theme.spacing(1) },
          '& li': { marginBottom: theme.spacing(0.25) },
          '& code': {
            backgroundColor: codeBg,
            padding: theme.spacing(0.25, 0.5),
            borderRadius: 4,
            fontSize: '0.85em',
          },
          '& pre': {
            backgroundColor: codeBg,
            padding: theme.spacing(1.5),
            borderRadius: 4,
            overflow: 'auto',
            '& code': { backgroundColor: 'transparent', padding: 0 },
          },
          '& blockquote': {
            borderLeft: `4px solid ${theme.palette.primary.main}`,
            margin: 0,
            padding: theme.spacing(0.5, 2),
            backgroundColor: codeBg,
          },
          '& a': {
            color: theme.palette.primary.main,
            textDecoration: 'none',
            '&:hover': { textDecoration: 'underline' },
          },
          // GitHub-flavored markdown tables. Wide tables (many columns) must
          // scroll horizontally rather than crushing each column down to one
          // letter per line, so the table sizes to its content and the cells
          // get a sensible min/max width.
          '& table': {
            borderCollapse: 'collapse',
            width: 'auto',
            minWidth: '100%',
            marginBottom: theme.spacing(2),
            tableLayout: 'auto',
          },
          '& th, & td': {
            border: `1px solid ${theme.palette.divider}`,
            padding: theme.spacing(0.75, 1),
            textAlign: 'left',
            verticalAlign: 'top',
            minWidth: 140,
            maxWidth: 320,
          },
          '& th': {
            backgroundColor: headerBg,
            fontWeight: 600,
            position: 'sticky',
            top: 0,
            zIndex: 1,
          },
        }}
      >
        <ReactMarkdown
          remarkPlugins={[remarkGfm]}
          components={{
            a: ({ children, href, ...props }) => (
              <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
                {children}
              </a>
            ),
          }}
        >
          {content}
        </ReactMarkdown>
      </Box>
    </Paper>
  );
};

export default CrewOutputRenderer;
