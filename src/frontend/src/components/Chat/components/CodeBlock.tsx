import React, { useState, useCallback, useMemo, memo } from 'react';
import { Box, IconButton, Tooltip, Typography, Button } from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import VisibilityIcon from '@mui/icons-material/Visibility';
import UnfoldMoreIcon from '@mui/icons-material/UnfoldMore';
import { Highlight, themes } from 'prism-react-renderer';
import { useTheme } from '../../../hooks/global/useTheme';
import { isHtmlDocument } from '../utils/textProcessing';

const MAX_LINES = 50;

interface CodeBlockProps {
  language: string;
  code: string;
}

export const CodeBlock: React.FC<CodeBlockProps> = memo(({ language, code }) => {
  const { isDarkMode } = useTheme();
  const [copied, setCopied] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const showPreview = language === 'html' || isHtmlDocument(code);
  const totalLines = useMemo(() => code.split('\n').length, [code]);
  const isTruncated = !expanded && totalLines > MAX_LINES;

  // Truncate to MAX_LINES when collapsed to reduce DOM node count
  const displayCode = useMemo(() => {
    if (!isTruncated) return code;
    return code.split('\n').slice(0, MAX_LINES).join('\n');
  }, [code, isTruncated]);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

  const handlePreview = useCallback(() => {
    window.dispatchEvent(
      new CustomEvent('codeBlockPreview', { detail: { html: code } })
    );
  }, [code]);

  const theme = isDarkMode ? themes.oneDark : themes.oneLight;
  const headerBg = isDarkMode ? '#1e1e1e' : '#f5f5f5';
  const headerBorder = isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)';

  return (
    <Box
      sx={{
        borderRadius: '8px',
        overflow: 'hidden',
        border: `1px solid ${headerBorder}`,
        my: 1,
        maxWidth: '100%',
        contain: 'content',
      }}
    >
      {/* Header bar */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          backgroundColor: headerBg,
          px: 1.5,
          py: 0.5,
          borderBottom: `1px solid ${headerBorder}`,
          minHeight: 32,
        }}
      >
        <Typography
          variant="caption"
          sx={{
            fontFamily: 'monospace',
            color: isDarkMode ? 'rgba(255,255,255,0.5)' : 'rgba(0,0,0,0.5)',
            fontSize: '0.75rem',
            userSelect: 'none',
          }}
        >
          {language || 'code'}
        </Typography>
        <Box sx={{ display: 'flex', gap: 0.5 }}>
          {showPreview && (
            <Tooltip title="Preview HTML">
              <IconButton size="small" onClick={handlePreview} sx={{ p: 0.5 }}>
                <VisibilityIcon sx={{ fontSize: 16, color: isDarkMode ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)' }} />
              </IconButton>
            </Tooltip>
          )}
          <Tooltip title={copied ? 'Copied!' : 'Copy code'}>
            <IconButton size="small" onClick={handleCopy} sx={{ p: 0.5 }}>
              {copied ? (
                <CheckIcon sx={{ fontSize: 16, color: 'success.main' }} />
              ) : (
                <ContentCopyIcon sx={{ fontSize: 16, color: isDarkMode ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)' }} />
              )}
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {/* Syntax-highlighted code */}
      <Highlight theme={theme} code={displayCode} language={language || 'text'}>
        {({ style, tokens, getLineProps, getTokenProps }) => (
          <pre
            style={{
              ...style,
              margin: 0,
              padding: 12,
              overflow: 'auto',
              maxHeight: 400,
              fontSize: '0.875em',
              fontFamily: 'monospace',
              lineHeight: 1.5,
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

      {/* Show more / show less footer */}
      {totalLines > MAX_LINES && (
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            backgroundColor: headerBg,
            borderTop: `1px solid ${headerBorder}`,
            py: 0.25,
          }}
        >
          <Button
            size="small"
            startIcon={<UnfoldMoreIcon sx={{ fontSize: 14 }} />}
            onClick={() => setExpanded(prev => !prev)}
            sx={{
              textTransform: 'none',
              fontSize: '0.75rem',
              color: isDarkMode ? 'rgba(255,255,255,0.5)' : 'rgba(0,0,0,0.5)',
            }}
          >
            {expanded ? 'Show less' : `Show all ${totalLines} lines`}
          </Button>
        </Box>
      )}
    </Box>
  );
});

CodeBlock.displayName = 'CodeBlock';
