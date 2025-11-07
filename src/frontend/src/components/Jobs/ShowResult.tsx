import React, { useMemo, useState, useRef, useEffect, memo, useCallback } from 'react';
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
import PictureAsPdfIcon from '@mui/icons-material/PictureAsPdf';
import HtmlIcon from '@mui/icons-material/Html';
// import CloudIcon from '@mui/icons-material/Cloud';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { ShowResultProps } from '../../types/common';
import { ResultValue } from '../../types/result';
import { generateRunPDF } from '../../utils/pdfGenerator';
import { DatabricksService } from '../../api/DatabricksService';
// import { Run } from '../../api/ExecutionHistoryService';

// eslint-disable-next-line react/prop-types
const ShowResult = memo<ShowResultProps>(({ open, onClose, result, run }) => {
  const theme = useTheme();
  const [copied, setCopied] = useState(false);
  const [viewMode, setViewMode] = useState<'code' | 'html'>('code');
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [databricksVolumeInfo, setDatabricksVolumeInfo] = useState<{ path: string; workspaceUrl?: string } | null>(null);
  const dialogRef = useRef<HTMLDivElement>(null);
  // Track if the dialog has been opened at least once
  const hasOpenedRef = useRef(false);

  // Function to check for Databricks volume information from configuration
  const checkForDatabricksVolumeInfo = useCallback(async (resultData: Record<string, unknown>) => {
    try {
      // Check if we have run information with task configuration
      if (!run) {
        console.log('No run information available');
        return;
      }

      const databricksService = DatabricksService.getInstance();
      const globalConfig = await databricksService.getDatabricksConfig();

      if (!globalConfig) {
        console.log('No Databricks configuration found');
        return;
      }

      // Fetch workspace URL from backend environment
      let workspaceUrl: string | undefined;
      try {
        const envInfo = await databricksService.getDatabricksEnvironment();
        if (envInfo.databricks_host) {
          workspaceUrl = envInfo.databricks_host;
        }
      } catch (error) {
        console.error('Error fetching Databricks environment:', error);
      }

      let volumePath: string | null = null;
      let volumeEnabled = false;

      // First, check task-level configuration
      // eslint-disable-next-line react/prop-types
      if (run?.tasks_yaml) {
        try {
          // Parse the tasks YAML (it's actually stored as a string of the YAML content)
          // Check if any task has volume configuration
          // eslint-disable-next-line react/prop-types
          const tasksContent = run?.tasks_yaml;

          // Look for callback_config in the tasks
          // Pattern to find callback_config with DatabricksVolumeCallback
          const callbackPattern = /callback_config:\s*["']?DatabricksVolumeCallback/;
          const hasVolumeCallback = callbackPattern.test(tasksContent);

          if (hasVolumeCallback) {
            // Extract volume path from callback config if specified
            const volumePathPattern = /volume_path:\s*["']?([^"'\n]+)["']?/;
            const pathMatch = volumePathPattern.exec(tasksContent);

            if (pathMatch && pathMatch[1]) {
              volumePath = pathMatch[1];
              volumeEnabled = true;
            } else if (globalConfig.volume_enabled && globalConfig.volume_path) {
              // Use global config if no specific path in task
              volumePath = globalConfig.volume_path;
              volumeEnabled = true;
            }
          }
        } catch (error) {
          console.error('Error parsing tasks configuration:', error);
        }
      }

      // If no task-level config, check global configuration
      if (!volumeEnabled && globalConfig.volume_enabled && globalConfig.volume_path) {
        volumePath = globalConfig.volume_path;
        volumeEnabled = true;
      }

      // If volume is enabled, construct the full path
      if (volumeEnabled && volumePath) {
        // Get execution name from run
        // eslint-disable-next-line react/prop-types
        const executionName = run?.run_name || run?.job_id || 'unknown';

        // Construct the full volume path
        // Format: /Volumes/catalog/schema/volume/execution_name/[date]/files...
        const fullVolumePath = `/Volumes/${volumePath.replace(/\./g, '/')}/${executionName}`;

        setDatabricksVolumeInfo({
          path: fullVolumePath,
          workspaceUrl: workspaceUrl
        });

        console.log('Databricks volume detected:', {
          path: fullVolumePath,
          workspaceUrl: workspaceUrl
        });
      } else {
        console.log('No volume configuration detected');
      }
    } catch (error) {
      console.error('Error checking for Databricks volume info:', error);
    }
  }, [run]);

  // Update the hasOpened ref when dialog opens and set view mode based on content
  useEffect(() => {
    if (open) {
      hasOpenedRef.current = true;
      // Set view mode to 'html' if HTML content is detected
      if (result) {
        const resultString = JSON.stringify(result);
        if (/<[^>]*>/.test(resultString) || resultString.includes('<!DOCTYPE') || resultString.includes('<html')) {
          setViewMode('html');
        } else {
          setViewMode('code');
        }

        // Check for Databricks volume information from configuration
        checkForDatabricksVolumeInfo(result);
      }
    }
  }, [open, result, checkForDatabricksVolumeInfo]);

  // URL detection regex pattern
  const urlPattern = /(https?:\/\/[^\s]+)/g;

  // Check if result contains HTML content
  const isHtmlContent = useMemo(() => {
    if (!result) return false;
    const resultString = JSON.stringify(result);
    // Check for common HTML tags
    return /<[^>]*>/.test(resultString) || resultString.includes('<!DOCTYPE') || resultString.includes('<html');
  }, [result]);

  // Memoize the formatted result to prevent unnecessary re-processing
  const memoizedResult = useMemo(() => {
    if (!result) return {};
    // Return the result directly if it's already an object
    // Only clone if necessary for stability
    return result;
  }, [result]);

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

  // Download HTML function
  const handleDownloadHtml = () => {
    if (!run) return;

    // Extract the HTML content from the result
    let htmlContent = '';

    // Check if result contains HTML
    if (result && typeof result === 'object') {
      // Look for HTML in the result values
      const resultString = JSON.stringify(result);

      // If the result contains HTML tags, extract the HTML
      if (resultString.includes('<html') || resultString.includes('<!DOCTYPE')) {
        // Find the actual HTML content in the result
        for (const value of Object.values(result)) {
          const valueStr = String(value);
          if (valueStr.includes('<html') || valueStr.includes('<!DOCTYPE')) {
            htmlContent = valueStr;
            break;
          }
        }
      }
    }

    // If no HTML found, create a simple HTML with the result
    if (!htmlContent) {
      htmlContent = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Result</title>
</head>
<body>
    <pre>${JSON.stringify(result, null, 2)}</pre>
</body>
</html>
      `;
    }

    const blob = new Blob([htmlContent], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    // eslint-disable-next-line react/prop-types
    const sanitizedName = run?.run_name ? run.run_name.replace(/[^a-z0-9]/gi, '_').toLowerCase() : 'result';
    const timestamp = new Date().toISOString().split('T')[0];
    // eslint-disable-next-line react/prop-types
    const jobId = run?.job_id || 'unknown';
    link.download = `${sanitizedName}_${timestamp}_${jobId}.html`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
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

  // Create a stable key for the iframe based on the HTML content
  const getHtmlKey = (html: string) => {
    // Create a simple hash of the first 100 chars to identify unique content
    const sample = html.substring(0, 100);
    let hash = 0;
    for (let i = 0; i < sample.length; i++) {
      const char = sample.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return hash.toString();
  };

  // Sandboxed iframe component for HTML+JS rendering - with proper memoization
  const SandboxedHTMLRenderer = memo(
    function SandboxedHTMLRenderer({ html, isFullscreen = false }: { html: string; isFullscreen?: boolean }) {
      const iframeRef = useRef<HTMLIFrameElement>(null);
      const isInitializedRef = useRef<boolean>(false);
      const contentKeyRef = useRef<string>('');
      const lastWrittenContentRef = useRef<string>('');

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

    const cleanedHtml = cleanupHtml(html);
    const currentKey = getHtmlKey(cleanedHtml);

    // Check if this is actually new content
    const isNewContent = contentKeyRef.current !== currentKey;
    if (isNewContent) {
      contentKeyRef.current = currentKey;
      isInitializedRef.current = false;

    }

    // Initialize iframe when content changes
    useEffect(() => {


      if (!iframeRef.current) return;

      const iframe = iframeRef.current;

      // Only write if we have new content
      if (!isNewContent && isInitializedRef.current) {
        return;
      }

      // Double-check we haven't already written this exact content
      if (lastWrittenContentRef.current === cleanedHtml && isInitializedRef.current) {
        return;
      }

      isInitializedRef.current = true;
      lastWrittenContentRef.current = cleanedHtml;

      let processedHtml = cleanedHtml;

      // Add scripts to handle sandboxed environment limitations
      const initScript = `
        <script>
          // Polyfill for History API in sandboxed iframe
          (function() {
            // Store the original methods if they exist
            const originalPushState = window.history.pushState;
            const originalReplaceState = window.history.replaceState;

            // Override history methods to prevent errors in sandboxed context
            try {
              window.history.pushState = function(state, title, url) {
                try {
                  // Try to call the original method
                  return originalPushState.apply(window.history, arguments);
                } catch (e) {
                  // Silently fail if not allowed in sandbox
                  console.debug('pushState blocked in sandbox:', e.message);
                }
              };

              window.history.replaceState = function(state, title, url) {
                try {
                  // Try to call the original method
                  return originalReplaceState.apply(window.history, arguments);
                } catch (e) {
                  // Silently fail if not allowed in sandbox
                  console.debug('replaceState blocked in sandbox:', e.message);
                }
              };

              // Also handle popstate events that might cause issues
              window.addEventListener('error', function(e) {
                if (e.message && e.message.includes('history')) {
                  e.preventDefault();
                  console.debug('History-related error prevented:', e.message);
                }
              }, true);
            } catch (e) {
              // Even the override might fail in some strict contexts
              console.debug('Could not override history methods:', e.message);
            }

            // For libraries that check for history API support
            if (!window.history.state) {
              try {
                Object.defineProperty(window.history, 'state', {
                  get: function() { return null; },
                  configurable: true
                });
              } catch (e) {
                // Ignore if we can't define the property
              }
            }

            // Handle location.hash for navigation-based libraries
            let virtualHash = '';
            try {
              Object.defineProperty(window.location, 'hash', {
                get: function() { return virtualHash; },
                set: function(value) {
                  virtualHash = value;
                  // Trigger hashchange event for libraries that listen to it
                  window.dispatchEvent(new HashChangeEvent('hashchange'));
                },
                configurable: true
              });
            } catch (e) {
              // If we can't override location.hash, that's okay
            }
          })();
        </script>
        <style>
          /* Responsive scaling for iframe content */
          html, body {
            margin: 0 !important;
            padding: 0 !important;
            width: 100% !important;
            height: 100% !important;
            overflow: auto !important;
          }

          /* Ensure content fits viewport */
          body > *:first-child:not(script):not(style) {
            max-width: 100% !important;
            max-height: 100vh !important;
          }
        </style>
        <script>
          // Basic iframe initialization - no library-specific code
          (function() {
            // Ensure iframe content is properly sized
            if (document.readyState === 'complete' || document.readyState === 'interactive') {
              // Content already loaded
              if (document.body) {
                document.body.style.margin = '0';
                document.body.style.padding = '0';
              }
            } else {
              // Wait for content to load
              window.addEventListener('DOMContentLoaded', function() {
                if (document.body) {
                  document.body.style.margin = '0';
                  document.body.style.padding = '0';
                }
              });
            }
          })();
        </script>
      `;

      // Insert initialization script at the very beginning of the document
      // This ensures it runs before any library scripts
      if (processedHtml.includes('<head>')) {
        processedHtml = processedHtml.replace('<head>', '<head>' + initScript);
      } else if (processedHtml.includes('<html>')) {
        processedHtml = processedHtml.replace('<html>', '<html><head>' + initScript + '</head>');
      } else {
        // If no head or html tag, prepend the script
        processedHtml = initScript + processedHtml;
      }

      // Use srcdoc for better security and compatibility
      // This avoids the need for allow-same-origin while still allowing scripts
      // srcdoc content is treated as having a unique origin
      iframe.srcdoc = processedHtml;
    }, [isNewContent, cleanedHtml]); // Re-run when content actually changes
    // Ensure focus enters the iframe so users can interact with rendered HTML via keyboard
    useEffect(() => {
      const iframe = iframeRef.current;
      if (!iframe) return;

      const focusIframe = () => {
        try {
          // Focus the iframe element and its window if possible
          iframe.focus();
          iframe.contentWindow?.focus();
        } catch (_) {
          // Ignore focus errors in strict sandbox contexts
        }
      };

      // Attempt to focus shortly after setting srcdoc
      const t = window.setTimeout(focusIframe, 0);

      // Also focus when iframe reports load
      iframe.addEventListener('load', focusIframe);

      return () => {
        window.clearTimeout(t);
        iframe.removeEventListener('load', focusIframe);
      };
    }, [isNewContent, cleanedHtml]);




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
          tabIndex={-1}
          onLoad={() => {
            try {
              iframeRef.current?.focus();
              iframeRef.current?.contentWindow?.focus();
            } catch (_) {
              // Ignore focus errors in strict sandbox contexts
            }
          }}
          sandbox="allow-scripts allow-forms allow-modals allow-popups allow-presentation allow-pointer-lock allow-downloads"
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
    },
    // Custom comparison function - only re-render if html content actually changes
    (prevProps, nextProps) => {
      // Clean both HTML strings for comparison
      const cleanPrev = prevProps.html.replace(/^```(html|HTML)\s*\n/, '').replace(/\n```\s*$/, '').trim();
      const cleanNext = nextProps.html.replace(/^```(html|HTML)\s*\n/, '').replace(/\n```\s*$/, '').trim();

      // Return true if props are equal (skip re-render), false if different (re-render)
      const isEqual = cleanPrev === cleanNext && prevProps.isFullscreen === nextProps.isFullscreen;



      return isEqual;
    }
  );

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
      keepMounted={hasOpenedRef.current} // Keep the dialog mounted after first open
      maxWidth={viewMode === 'html' && Object.values(memoizedResult || {}).some(value =>
        typeof value === 'string' && isHTML(value)
      ) ? "xl" : "lg"}
      fullWidth
      sx={{
        zIndex: (theme) => theme.zIndex.modal + 100, // Higher z-index to appear above other dialogs
      }}
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
      <DialogActions sx={{ px: 3, py: 2, gap: 1 }}>
        {databricksVolumeInfo && (
          <Tooltip title="Open Databricks Volume Location">
            <Button
              onClick={() => {
                if (databricksVolumeInfo.workspaceUrl) {
                  // Construct the Databricks catalog URL
                  // Format: https://<workspace>/explore/data/volumes/<catalog>/<schema>/<volume>
                  const pathParts = databricksVolumeInfo.path.replace('/Volumes/', '').split('/');
                  if (pathParts.length >= 3) {
                    const catalog = pathParts[0];
                    const schema = pathParts[1];
                    const volume = pathParts[2];

                    // Remove trailing slash from workspace URL if present
                    const workspaceUrl = databricksVolumeInfo.workspaceUrl.replace(/\/$/, '');

                    // Construct the URL to the volume in Databricks catalog
                    const volumeUrl = `${workspaceUrl}/explore/data/volumes/${catalog}/${schema}/${volume}`;

                    // Open in new tab
                    window.open(volumeUrl, '_blank');
                  } else {
                    // Fallback to workspace URL if path parsing fails
                    window.open(databricksVolumeInfo.workspaceUrl, '_blank');
                  }
                } else {
                  // If no workspace URL, show alert
                  alert('Unable to retrieve Databricks workspace URL. Please ensure Databricks is properly configured.');
                }
              }}
              variant="outlined"
              startIcon={<FolderOpenIcon />}
              sx={{
                borderRadius: '8px',
                textTransform: 'none',
                fontWeight: 500,
                borderColor: '#FF5722',
                color: '#FF5722',
                '&:hover': {
                  borderColor: '#F4511E',
                  backgroundColor: 'rgba(255, 87, 34, 0.04)',
                },
              }}
            >
              Open in Databricks Volume
            </Button>
          </Tooltip>
        )}
        {run && (
          <Tooltip title={isHtmlContent ? "Download as HTML" : "Download as PDF"}>
            <Button
              onClick={isHtmlContent ? handleDownloadHtml : () => generateRunPDF(run)}
              variant="outlined"
              startIcon={isHtmlContent ? <HtmlIcon /> : <PictureAsPdfIcon />}
              sx={{
                borderRadius: '8px',
                textTransform: 'none',
                fontWeight: 500,
              }}
            >
              {isHtmlContent ? 'Download HTML' : 'Download PDF'}
            </Button>
          </Tooltip>
        )}
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
}, (prevProps, nextProps) => {
  // Custom comparison to prevent re-renders
  // Only re-render if these specific props change
  return (
    prevProps.open === nextProps.open &&
    prevProps.onClose === nextProps.onClose &&
    JSON.stringify(prevProps.result) === JSON.stringify(nextProps.result)
  );
});

ShowResult.displayName = 'ShowResult';

export default ShowResult;