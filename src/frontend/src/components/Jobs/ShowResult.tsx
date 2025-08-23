import React, { useMemo, useState, useRef, useEffect } from 'react';
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
  IconButton,
  Tooltip,
  ToggleButton,
  ToggleButtonGroup,
} from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import CodeIcon from '@mui/icons-material/Code';
import WebIcon from '@mui/icons-material/Web';
import FullscreenIcon from '@mui/icons-material/Fullscreen';
import FullscreenExitIcon from '@mui/icons-material/FullscreenExit';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { ShowResultProps } from '../../types/common';
import { ResultValue } from '../../types/result';

const ShowResult: React.FC<ShowResultProps> = ({ open, onClose, result }) => {
  const theme = useTheme();
  const [copied, setCopied] = useState(false);
  const [viewMode, setViewMode] = useState<'code' | 'html'>('code');
  const [isFullscreen, setIsFullscreen] = useState(false);
  const dialogRef = useRef<HTMLDivElement>(null);
  // URL detection regex pattern
  const urlPattern = /(https?:\/\/[^\s]+)/g;

  // Memoize the formatted result to prevent unnecessary re-processing
  const memoizedResult = useMemo(() => 
    result ? result : {}
  , [result]);

  // Fullscreen handlers
  const handleFullscreen = async () => {
    if (!document.fullscreenElement) {
      try {
        const element = dialogRef.current?.querySelector('.MuiDialog-paper');
        if (element) {
          await element.requestFullscreen();
          setIsFullscreen(true);
        }
      } catch (err) {
        console.error('Error attempting to enable fullscreen:', err);
      }
    } else {
      try {
        await document.exitFullscreen();
        setIsFullscreen(false);
      } catch (err) {
        console.error('Error attempting to exit fullscreen:', err);
      }
    }
  };

  // Listen for fullscreen changes
  useEffect(() => {
    const handleFullscreenChange = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };

    document.addEventListener('fullscreenchange', handleFullscreenChange);
    document.addEventListener('webkitfullscreenchange', handleFullscreenChange);
    document.addEventListener('mozfullscreenchange', handleFullscreenChange);
    document.addEventListener('MSFullscreenChange', handleFullscreenChange);

    return () => {
      document.removeEventListener('fullscreenchange', handleFullscreenChange);
      document.removeEventListener('webkitfullscreenchange', handleFullscreenChange);
      document.removeEventListener('mozfullscreenchange', handleFullscreenChange);
      document.removeEventListener('MSFullscreenChange', handleFullscreenChange);
    };
  }, []);

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

  const isHTML = (text: string): boolean => {
    // First check if it's HTML wrapped in markdown code block
    if (text.match(/^```(html|HTML)\s*\n[\s\S]*\n```\s*$/)) {
      return true;
    }
    
    // Check if the text contains HTML tags
    const htmlPatterns = [
      /<\/?[a-z][\s\S]*>/i,  // HTML tags
      /<!DOCTYPE\s+html/i,    // DOCTYPE declaration
      /<html[^>]*>/i,          // HTML tag
      /<body[^>]*>/i,          // Body tag
      /<div[^>]*>/i,           // Div tag
      /<p[^>]*>/i,             // Paragraph tag
      /<h[1-6][^>]*>/i,        // Header tags
      /<table[^>]*>/i,         // Table tag
    ];
    return htmlPatterns.some(pattern => pattern.test(text));
  };

  // Sandboxed iframe component for HTML+JS rendering
  const SandboxedHTMLRenderer: React.FC<{ html: string; isFullscreen?: boolean }> = ({ html, isFullscreen = false }) => {
    const iframeRef = useRef<HTMLIFrameElement>(null);
    
    // Clean up the HTML string - remove markdown code block syntax if present
    const cleanupHtml = (htmlContent: string) => {
      let cleanedHtml = htmlContent;
      
      // Remove ```html or ```HTML from the beginning and ``` from the end
      if (cleanedHtml.match(/^```(html|HTML)\s*\n/)) {
        cleanedHtml = cleanedHtml.replace(/^```(html|HTML)\s*\n/, '');
        cleanedHtml = cleanedHtml.replace(/\n```\s*$/, '');
      }
      
      return cleanedHtml;
    };
    
    useEffect(() => {
      if (iframeRef.current) {
        const iframe = iframeRef.current;
        const iframeWindow = iframe.contentWindow;
        const iframeDoc = iframe.contentDocument || iframeWindow?.document;
        
        if (iframeDoc && iframeWindow) {
          let cleanedHtml = cleanupHtml(html);
          
          // Remove fallback messages for presentations when in iframe
          // Remove the fallback-message div that shows "browser not supported"
          cleanedHtml = cleanedHtml.replace(
            /<div[^>]*class="fallback-message"[^>]*>[\s\S]*?<\/div>/gi,
            ''
          );
          
          // Force impress-supported class on body to hide fallback messages
          cleanedHtml = cleanedHtml.replace(
            /class="impress-not-supported"/g,
            'class="impress-supported"'
          );
          
          // Add responsive scaling styles and initialization
          const initScript = `
            <style>
              /* Responsive scaling for iframe content */
              html, body {
                margin: 0 !important;
                padding: 0 !important;
                width: 100% !important;
                height: 100% !important;
                overflow: auto !important;
              }
              
              /* Scale impress.js presentations */
              #impress {
                transform-origin: top left !important;
              }
              
              /* Scale reveal.js presentations */
              .reveal {
                width: 100% !important;
                height: 100% !important;
              }
              
              /* Ensure content fits viewport */
              body > *:first-child:not(script):not(style) {
                max-width: 100% !important;
                max-height: 100vh !important;
              }
            </style>
            <script>
              // Mark as supported environment
              document.body.classList.remove('impress-not-supported');
              document.body.classList.add('impress-supported');
              
              // Hide any fallback messages
              document.querySelectorAll('.fallback-message').forEach(function(el) {
                el.style.display = 'none';
              });
              
              // Ensure feature APIs are available
              if (!window.requestAnimationFrame) {
                window.requestAnimationFrame = function(callback) {
                  return setTimeout(callback, 1000/60);
                };
              }
              if (!window.cancelAnimationFrame) {
                window.cancelAnimationFrame = function(id) {
                  clearTimeout(id);
                };
              }
              
              // Auto-scale content to fit
              function scaleContent() {
                var container = document.body;
                var content = document.getElementById('impress') || 
                             document.querySelector('.reveal') || 
                             document.querySelector('main') || 
                             document.body.firstElementChild;
                
                if (content && content.offsetWidth > 0) {
                  var containerWidth = window.innerWidth;
                  var containerHeight = window.innerHeight;
                  var contentWidth = content.scrollWidth || content.offsetWidth || 1920;
                  var contentHeight = content.scrollHeight || content.offsetHeight || 1080;
                  
                  // Calculate scale to fit
                  var scaleX = containerWidth / contentWidth;
                  var scaleY = containerHeight / contentHeight;
                  var scale = Math.min(scaleX, scaleY, 1);
                  
                  // Apply scale for impress.js
                  if (content.id === 'impress') {
                    content.style.transform = 'scale(' + scale + ')';
                    content.style.transformOrigin = 'top left';
                  }
                }
              }
              
              // Initialize on load
              window.addEventListener('load', function() {
                // Scale content
                scaleContent();
                
                // For impress.js
                if (typeof impress !== 'undefined') {
                  try {
                    var api = impress();
                    api.init();
                    console.log('Impress.js initialized');
                    // Rescale after init
                    setTimeout(scaleContent, 100);
                  } catch(e) {
                    console.log('Impress.js init:', e);
                  }
                }
                
                // For reveal.js
                if (typeof Reveal !== 'undefined') {
                  try {
                    Reveal.initialize({
                      width: '100%',
                      height: '100%',
                      margin: 0,
                      minScale: 0.2,
                      maxScale: 2.0,
                      center: true,
                      embedded: true
                    });
                    console.log('Reveal.js initialized');
                  } catch(e) {
                    console.log('Reveal.js init:', e);
                  }
                }
              });
              
              // Rescale on resize
              window.addEventListener('resize', scaleContent);
            </script>
          `;
          
          // Insert initialization script before closing head or body tag
          if (cleanedHtml.includes('</head>')) {
            cleanedHtml = cleanedHtml.replace('</head>', initScript + '\n</head>');
          } else if (cleanedHtml.includes('</body>')) {
            cleanedHtml = cleanedHtml.replace('</body>', initScript + '\n</body>');
          } else {
            cleanedHtml += initScript;
          }
          
          // Write content to iframe
          iframeDoc.open();
          iframeDoc.write(cleanedHtml);
          iframeDoc.close();
        }
      }
    }, [html]);
    
    return (
      <Box
        sx={{
          width: '100%',
          height: isFullscreen ? 'calc(100vh - 80px)' : 'calc(80vh - 100px)', // Account for dialog header/footer
          minHeight: '400px',
          maxHeight: isFullscreen ? 'calc(100vh - 80px)' : 'calc(95vh - 120px)',
          border: `1px solid ${theme.palette.divider}`,
          borderRadius: 1,
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        <iframe
          ref={iframeRef}
          sandbox="allow-scripts allow-same-origin allow-forms allow-modals allow-popups allow-presentation allow-pointer-lock allow-top-navigation-by-user-activation"
          allow="accelerometer; camera; encrypted-media; fullscreen; gyroscope; magnetometer; microphone; midi; payment; usb; xr-spatial-tracking"
          style={{
            width: '100%',
            height: '100%',
            border: 'none',
            backgroundColor: theme.palette.background.paper,
          }}
          title="Sandboxed HTML/JS Content"
        />
      </Box>
    );
  };

  const renderContent = (content: Record<string, ResultValue>) => {
    if (typeof content === 'object' && content !== null) {
      // If there's only one key called 'Value', render its content directly
      const entries = Object.entries(content);
      if (entries.length === 1 && entries[0][0].toLowerCase() === 'value') {
        const value = entries[0][1];
        return renderSingleValue(value);
      }
      
      return entries.map(([key, value], index) => (
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
            {typeof value === 'string' && isHTML(value) && viewMode === 'html' ? (
              <SandboxedHTMLRenderer html={value} isFullscreen={isFullscreen} />
            ) : typeof value === 'string' && isMarkdown(value) && !isHTML(value) ? (
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
        {typeof content === 'string' && isHTML(content) && viewMode === 'html' ? (
          <SandboxedHTMLRenderer html={content} isFullscreen={isFullscreen} />
        ) : typeof content === 'string' && isMarkdown(content) && !isHTML(content) ? (
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

  const handleCopyToClipboard = async () => {
    try {
      // Extract text content from the result
      let textContent = '';
      
      if (typeof memoizedResult === 'string') {
        textContent = memoizedResult;
      } else if (typeof memoizedResult === 'object' && memoizedResult !== null) {
        // Convert object to formatted string
        const entries = Object.entries(memoizedResult);
        if (entries.length === 1 && entries[0][0].toLowerCase() === 'value') {
          // If there's only a 'Value' key, copy just its content
          textContent = String(entries[0][1]);
        } else {
          // Format as key-value pairs
          textContent = entries.map(([key, value]) => {
            if (typeof value === 'string') {
              return `${key}:\n${value}`;
            } else {
              return `${key}:\n${JSON.stringify(value, null, 2)}`;
            }
          }).join('\n\n');
        }
      } else {
        textContent = String(memoizedResult);
      }
      
      await navigator.clipboard.writeText(textContent);
      setCopied(true);
      
      // Reset the copied state after 2 seconds
      setTimeout(() => {
        setCopied(false);
      }, 2000);
    } catch (error) {
      console.error('Failed to copy to clipboard:', error);
      setCopied(false);
    }
  };

  const renderSingleValue = (value: ResultValue) => {
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
        {typeof value === 'string' && isHTML(value) && viewMode === 'html' ? (
          <SandboxedHTMLRenderer html={value} isFullscreen={isFullscreen} />
        ) : typeof value === 'string' && isMarkdown(value) && !isHTML(value) ? (
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
    );
  };

  return (
    <Dialog 
      ref={dialogRef}
      open={open} 
      onClose={onClose} 
      maxWidth={viewMode === 'html' && Object.values(memoizedResult || {}).some(value => 
        typeof value === 'string' && isHTML(value)
      ) ? "xl" : "lg"} 
      fullWidth
      PaperProps={{
        sx: { 
          maxHeight: isFullscreen ? '100vh' : '95vh',
          borderRadius: isFullscreen ? 0 : 2,
          width: viewMode === 'html' && Object.values(memoizedResult || {}).some(value => 
            typeof value === 'string' && isHTML(value)
          ) ? (isFullscreen ? '100vw' : '95vw') : undefined,
          margin: isFullscreen ? 0 : undefined,
        }
      }}
    >
      <DialogTitle sx={{ px: 3, py: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        {/* View Mode Toggle - only show if HTML content is detected */}
        {(() => {
          const hasHTMLContent = Object.values(memoizedResult || {}).some(value => 
            typeof value === 'string' && isHTML(value)
          );
          return hasHTMLContent ? (
            <ToggleButtonGroup
              value={viewMode}
              exclusive
              onChange={(_, newMode) => newMode && setViewMode(newMode)}
              size="small"
              sx={{ height: 32 }}
            >
              <ToggleButton value="code" aria-label="code view">
                <CodeIcon sx={{ mr: 0.5, fontSize: 18 }} />
                Code
              </ToggleButton>
              <ToggleButton value="html" aria-label="html view">
                <WebIcon sx={{ mr: 0.5, fontSize: 18 }} />
                HTML
              </ToggleButton>
            </ToggleButtonGroup>
          ) : (
            <Box />
          );
        })()}
        
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Tooltip title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}>
            <IconButton
              onClick={handleFullscreen}
              size="small"
              sx={{
                color: 'text.secondary',
                transition: 'all 0.2s',
                '&:hover': {
                  color: 'primary.main',
                  backgroundColor: 'action.hover',
                }
              }}
            >
              {isFullscreen ? <FullscreenExitIcon /> : <FullscreenIcon />}
            </IconButton>
          </Tooltip>
          
          <Tooltip title={copied ? "Copied!" : "Copy to clipboard"}>
            <IconButton
              onClick={handleCopyToClipboard}
              size="small"
              sx={{
                color: copied ? 'success.main' : 'text.secondary',
                transition: 'all 0.2s',
                '&:hover': {
                  color: 'primary.main',
                  backgroundColor: 'action.hover',
                }
              }}
            >
              {copied ? <CheckIcon /> : <ContentCopyIcon />}
            </IconButton>
          </Tooltip>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ 
        px: viewMode === 'html' && Object.values(memoizedResult || {}).some(value => 
          typeof value === 'string' && isHTML(value)
        ) ? 1 : 3, 
        py: viewMode === 'html' && Object.values(memoizedResult || {}).some(value => 
          typeof value === 'string' && isHTML(value)
        ) ? 1 : 2,
        overflow: 'auto',
        display: 'flex',
        flexDirection: 'column',
      }}>
        <Box sx={{ 
          px: 0,
          py: 0,
          flexGrow: 1,
          display: 'flex',
          flexDirection: 'column',
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
      {/* Snackbar removed - clipboard notifications disabled */}
    </Dialog>
  );
};

export default ShowResult; 