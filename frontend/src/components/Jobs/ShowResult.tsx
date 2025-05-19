import React, { useMemo } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Paper,
  Typography,
  Divider,
  Link,
  useTheme,
} from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { ShowResultProps } from '../../types/common';
import { ResultValue } from '../../types/result';

const ShowResult: React.FC<ShowResultProps> = ({ open, onClose, result }) => {
  const theme = useTheme();
  // URL detection regex pattern
  const urlPattern = /(https?:\/\/[^\s]+)/g;

  // Memoize the formatted result to prevent unnecessary re-processing
  const memoizedResult = useMemo(() => 
    result ? result : {}
  , [result]);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const formatValue = (value: unknown): string => {
    if (typeof value === 'object' && value !== null) {
      return JSON.stringify(value, null, 2);
    }
    return String(value ?? '');
  };

  const renderWithLinks = (text: string) => {
    const parts = text.split(urlPattern);
    return parts.map((part, index) => {
      if (part.match(urlPattern)) {
        return (
          <Box 
            key={index} 
            sx={{ 
              display: 'inline-flex', 
              alignItems: 'center',
              gap: 0.5
            }}
          >
            <Link 
              href={part} 
              target="_blank" 
              rel="noopener noreferrer"
              sx={{ 
                display: 'inline-flex',
                alignItems: 'center',
                gap: 0.5,
                color: 'primary.main',
                textDecoration: 'none',
                '&:hover': {
                  textDecoration: 'underline'
                }
              }}
            >
              {part}
              <OpenInNewIcon sx={{ fontSize: 16 }} />
            </Link>
          </Box>
        );
      }
      return part;
    });
  };

  const isMarkdown = (text: string): boolean => {
    // Simple check for common markdown patterns
    const markdownPatterns = [
      /^#+ /m,           // Headers
      /\*\*.+\*\*/,      // Bold
      /_.+_/,            // Italic
      /\[.+\]\(.+\)/,    // Links
      /^\s*[-*+]\s/m,    // Lists
      /^\s*\d+\.\s/m,    // Numbered lists
      /```[\s\S]*```/,   // Code blocks
      /^\s*>/m,          // Blockquotes
    ];
    return markdownPatterns.some(pattern => pattern.test(text));
  };

  const renderContent = (content: Record<string, ResultValue>) => {
    if (typeof content === 'object' && content !== null) {
      return Object.entries(content).map(([key, value], index) => (
        <Box key={key} sx={{ mb: 3 }}>
          <Typography 
            variant="subtitle1" 
            sx={{ 
              color: 'primary.main',
              fontWeight: 700,
              mb: 1,
              letterSpacing: '0.01em'
            }}
          >
            {key}
          </Typography>
          <Paper 
            elevation={0} 
            sx={{ 
              bgcolor: theme.palette.mode === 'light' ? 'grey.50' : 'grey.900',
              p: 2.5,
              borderRadius: 1.5,
              border: '1px solid',
              borderColor: theme.palette.mode === 'light' ? 'grey.200' : 'grey.800',
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                bgcolor: theme.palette.mode === 'light' ? 'grey.100' : 'grey.800',
                borderColor: theme.palette.mode === 'light' ? 'grey.300' : 'grey.700',
              }
            }}
          >
            {typeof value === 'string' && isMarkdown(value) ? (
              <Box sx={{
                '& .markdown-body': {
                  fontFamily: theme.typography.fontFamily,
                  fontSize: '0.9rem',
                  lineHeight: 1.6,
                  color: theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.87)' : 'rgba(255, 255, 255, 0.87)',
                  '& h1, & h2, & h3, & h4, & h5, & h6': {
                    color: theme.palette.primary.main,
                    fontWeight: 600,
                    marginTop: theme.spacing(2),
                    marginBottom: theme.spacing(1),
                  },
                  '& p': {
                    marginBottom: theme.spacing(1.5),
                  },
                  '& ul, & ol': {
                    paddingLeft: theme.spacing(2.5),
                    marginBottom: theme.spacing(1.5),
                  },
                  '& li': {
                    marginBottom: theme.spacing(0.5),
                  },
                  '& code': {
                    backgroundColor: theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.04)' : 'rgba(255, 255, 255, 0.1)',
                    padding: theme.spacing(0.25, 0.5),
                    borderRadius: 4,
                    fontSize: '0.85em',
                  },
                  '& pre': {
                    backgroundColor: theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.04)' : 'rgba(255, 255, 255, 0.1)',
                    padding: theme.spacing(1.5),
                    borderRadius: 4,
                    overflow: 'auto',
                    '& code': {
                      backgroundColor: 'transparent',
                      padding: 0,
                    },
                  },
                  '& blockquote': {
                    borderLeft: `4px solid ${theme.palette.primary.main}`,
                    margin: 0,
                    padding: theme.spacing(0.5, 2),
                    backgroundColor: theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.04)' : 'rgba(255, 255, 255, 0.1)',
                  },
                  '& a': {
                    color: theme.palette.primary.main,
                    textDecoration: 'none',
                    '&:hover': {
                      textDecoration: 'underline',
                    },
                  },
                  '& table': {
                    borderCollapse: 'collapse',
                    width: '100%',
                    marginBottom: theme.spacing(2),
                  },
                  '& th, & td': {
                    border: `1px solid ${theme.palette.divider}`,
                    padding: theme.spacing(0.75, 1),
                  },
                  '& th': {
                    backgroundColor: theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.04)' : 'rgba(255, 255, 255, 0.1)',
                    fontWeight: 600,
                  },
                },
              }}>
                <ReactMarkdown 
                  className="markdown-body"
                  remarkPlugins={[remarkGfm]}
                  components={{
                    a: ({node, children, href, ...props}) => (
                      <a
                        href={href}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: '4px',
                          color: theme.palette.primary.main,
                          textDecoration: 'none',
                        }}
                        onMouseEnter={(e) => {
                          e.currentTarget.style.textDecoration = 'underline';
                        }}
                        onMouseLeave={(e) => {
                          e.currentTarget.style.textDecoration = 'none';
                        }}
                        {...props}
                      >
                        {children}
                        <OpenInNewIcon sx={{ fontSize: 16 }} />
                      </a>
                    ),
                  }}
                >
                  {value}
                </ReactMarkdown>
              </Box>
            ) : (
              <pre style={{
                margin: 0,
                whiteSpace: 'pre-wrap',
                wordWrap: 'break-word',
                fontFamily: '"Roboto Mono", monospace',
                fontSize: '0.9rem',
                lineHeight: 1.6,
                color: theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.87)' : 'rgba(255, 255, 255, 0.87)'
              }}>
                {typeof value === 'string' 
                  ? renderWithLinks(value)
                  : formatValue(value)
                }
              </pre>
            )}
          </Paper>
          {index < Object.entries(content).length - 1 && (
            <Divider sx={{ my: 3 }} />
          )}
        </Box>
      ));
    }
    return (
      <Paper 
        elevation={0} 
        sx={{ 
          bgcolor: theme.palette.mode === 'light' ? 'grey.50' : 'grey.900',
          p: 2.5,
          borderRadius: 1.5,
          border: '1px solid',
          borderColor: theme.palette.mode === 'light' ? 'grey.200' : 'grey.800',
          transition: 'all 0.2s ease-in-out',
          '&:hover': {
            bgcolor: theme.palette.mode === 'light' ? 'grey.100' : 'grey.800',
            borderColor: theme.palette.mode === 'light' ? 'grey.300' : 'grey.700',
          }
        }}
      >
        {typeof content === 'string' && isMarkdown(content) ? (
          <ReactMarkdown 
            className="markdown-body"
            remarkPlugins={[remarkGfm]}
          >
            {content}
          </ReactMarkdown>
        ) : (
          <pre style={{
            margin: 0,
            whiteSpace: 'pre-wrap',
            wordWrap: 'break-word',
            fontFamily: '"Roboto Mono", monospace',
            fontSize: '0.9rem',
            lineHeight: 1.6,
            color: theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.87)' : 'rgba(255, 255, 255, 0.87)'
          }}>
            {typeof content === 'string' 
              ? renderWithLinks(content)
              : formatValue(content)
            }
          </pre>
        )}
      </Paper>
    );
  };

  return (
    <Dialog 
      open={open} 
      onClose={onClose} 
      maxWidth="md" 
      fullWidth
      PaperProps={{
        sx: { 
          maxHeight: '90vh',
          borderRadius: 2
        }
      }}
    >
      <DialogTitle sx={{ px: 3, py: 2.5, display: 'flex', justifyContent: 'flex-end', alignItems: 'center' }}>
        <Typography variant="caption" color="text.secondary">
          Shortcut: sr
        </Typography>
      </DialogTitle>
      <DialogContent sx={{ px: 3, py: 2 }}>
        <Box sx={{ 
          px: 0,
          py: 0
        }}>
          {renderContent(memoizedResult)}
        </Box>
      </DialogContent>
      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button 
          onClick={onClose}
          variant="contained"
          color="primary"
          sx={{
            borderRadius: 1.5,
            textTransform: 'none',
            px: 3,
            py: 1,
            fontWeight: 600
          }}
        >
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ShowResult; 