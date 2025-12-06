import React, { useState, useMemo, useCallback } from 'react';
import {
  Box,
  Typography,
  Button,
  IconButton,
  Chip,
  Tooltip,
  Paper,
  Alert,
  Pagination,
  Divider,
} from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import FirstPageIcon from '@mui/icons-material/FirstPage';
import LastPageIcon from '@mui/icons-material/LastPage';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import CheckIcon from '@mui/icons-material/Check';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

// Size thresholds in characters
const SIZE_THRESHOLDS = {
  SMALL: 10000,      // 10KB - no pagination needed
  MEDIUM: 100000,    // 100KB - paginate with standard controls
  LARGE: 500000,     // 500KB - paginate with warning
  HUGE: 1000000,     // 1MB - strong warning about performance
};

const DEFAULT_PAGE_SIZE = 10000; // 10,000 characters per page

interface PaginatedOutputProps {
  /** The content to display - can be string or object (will be JSON stringified) */
  content: string | Record<string, unknown> | undefined;
  /** Number of characters per page (default: 10000) */
  pageSize?: number;
  /** Enable markdown rendering (default: true) */
  enableMarkdown?: boolean;
  /** Show copy buttons (default: true) */
  showCopyButton?: boolean;
  /** Maximum height of content area (default: '60vh') */
  maxHeight?: string;
  /** Custom styling for the paper container */
  paperSx?: Record<string, unknown>;
}

/**
 * Formats content for display, handling both string and object types
 */
const formatContent = (content: string | Record<string, unknown> | undefined): string => {
  if (!content) return 'No output available';

  if (typeof content === 'string') {
    // Clean up tool results and other formatted strings
    if (content.includes('ToolResult')) {
      const match = content.match(/result="([^"]+)"/);
      if (match) {
        try {
          const parsed = JSON.parse(match[1].replace(/'/g, '"'));
          return JSON.stringify(parsed, null, 2);
        } catch {
          return content;
        }
      }
    }
    return content;
  }

  return JSON.stringify(content, null, 2);
};

/**
 * Splits content into pages, trying to break at natural boundaries (newlines)
 */
const splitContentIntoPages = (content: string, pageSize: number): string[] => {
  if (content.length <= pageSize) return [content];

  const pages: string[] = [];
  let startIndex = 0;

  while (startIndex < content.length) {
    let endIndex = startIndex + pageSize;

    // Try to break at a newline if possible (within 50% of page size from end)
    if (endIndex < content.length) {
      const searchStart = startIndex + Math.floor(pageSize * 0.5);
      const lastNewline = content.lastIndexOf('\n', endIndex);
      if (lastNewline > searchStart) {
        endIndex = lastNewline + 1;
      }
    } else {
      endIndex = content.length;
    }

    pages.push(content.slice(startIndex, endIndex));
    startIndex = endIndex;
  }

  return pages;
};

/**
 * Formats byte size for display
 */
const formatSize = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
};

/**
 * PaginatedOutput Component
 *
 * Displays large content with pagination to prevent browser crashes.
 * Supports markdown rendering, copy functionality, and "show all" option.
 */
const PaginatedOutput: React.FC<PaginatedOutputProps> = ({
  content,
  pageSize = DEFAULT_PAGE_SIZE,
  enableMarkdown = true,
  showCopyButton = true,
  maxHeight = '60vh',
  paperSx = {},
}) => {
  const [currentPage, setCurrentPage] = useState(1);
  const [showAll, setShowAll] = useState(false);
  const [copied, setCopied] = useState<'page' | 'all' | null>(null);

  // Format and memoize the content
  const formattedContent = useMemo(() => formatContent(content), [content]);
  const contentSize = formattedContent.length;

  // Split content into pages
  const pages = useMemo(
    () => splitContentIntoPages(formattedContent, pageSize),
    [formattedContent, pageSize]
  );

  const totalPages = pages.length;
  const needsPagination = totalPages > 1;
  const isLargeContent = contentSize > SIZE_THRESHOLDS.LARGE;
  const isHugeContent = contentSize > SIZE_THRESHOLDS.HUGE;

  // Get content to display
  const displayContent = useMemo(() => {
    if (showAll) return formattedContent;
    return pages[currentPage - 1] || '';
  }, [showAll, formattedContent, pages, currentPage]);

  // Handle page change
  const handlePageChange = useCallback((_event: React.ChangeEvent<unknown>, page: number) => {
    setCurrentPage(page);
    setShowAll(false);
  }, []);

  // Handle copy functionality
  const handleCopy = useCallback(async (type: 'page' | 'all') => {
    const textToCopy = type === 'all' ? formattedContent : displayContent;
    try {
      await navigator.clipboard.writeText(textToCopy);
      setCopied(type);
      setTimeout(() => setCopied(null), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  }, [formattedContent, displayContent]);

  // Handle show all toggle
  const handleShowAll = useCallback(() => {
    setShowAll(true);
    setCurrentPage(1);
  }, []);

  // Handle show paginated
  const handleShowPaginated = useCallback(() => {
    setShowAll(false);
  }, []);

  // Reset pagination when content changes
  React.useEffect(() => {
    setCurrentPage(1);
    setShowAll(false);
  }, [content]);

  // Markdown styles
  const markdownStyles = {
    '& pre': {
      overflowX: 'auto',
      padding: 1,
      borderRadius: 1,
      backgroundColor: (theme: { palette: { mode: string } }) =>
        theme.palette.mode === 'dark' ? 'grey.800' : 'grey.100',
      fontFamily: 'monospace',
      fontSize: '0.875rem',
    },
    '& code': {
      fontFamily: 'monospace',
      fontSize: '0.875rem',
      backgroundColor: (theme: { palette: { mode: string } }) =>
        theme.palette.mode === 'dark' ? 'grey.800' : 'grey.200',
      padding: '2px 4px',
      borderRadius: '3px',
    },
    '& p': { marginTop: 1, marginBottom: 1 },
    '& ul, & ol': { paddingLeft: 3, marginTop: 1, marginBottom: 1 },
    '& li': { marginTop: 0.5, marginBottom: 0.5 },
    '& blockquote': {
      borderLeft: '4px solid',
      borderColor: 'primary.main',
      paddingLeft: 2,
      marginLeft: 0,
      marginTop: 1,
      marginBottom: 1,
      fontStyle: 'italic',
      color: 'text.secondary',
    },
    '& h1, & h2, & h3, & h4, & h5, & h6': {
      marginTop: 2,
      marginBottom: 1,
      fontWeight: 'bold',
    },
    '& h1': { fontSize: '1.5rem' },
    '& h2': { fontSize: '1.3rem' },
    '& h3': { fontSize: '1.1rem' },
    '& h4': { fontSize: '1rem' },
    '& h5': { fontSize: '0.9rem' },
    '& h6': { fontSize: '0.85rem' },
    '& table': {
      width: '100%',
      borderCollapse: 'collapse',
      marginTop: 1,
      marginBottom: 1,
    },
    '& th, & td': {
      border: '1px solid',
      borderColor: 'divider',
      padding: 1,
      textAlign: 'left',
    },
    '& th': {
      backgroundColor: (theme: { palette: { mode: string } }) =>
        theme.palette.mode === 'dark' ? 'grey.800' : 'grey.200',
      fontWeight: 'bold',
    },
    '& hr': {
      marginTop: 2,
      marginBottom: 2,
      border: 'none',
      borderTop: '1px solid',
      borderColor: 'divider',
    },
    '& a': {
      color: 'primary.main',
      textDecoration: 'none',
      '&:hover': { textDecoration: 'underline' },
    },
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
      {/* Size info and controls header */}
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: 1,
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Chip
            size="small"
            label={`Size: ${formatSize(contentSize)}`}
            variant="outlined"
            color={isHugeContent ? 'error' : isLargeContent ? 'warning' : 'default'}
          />
          {needsPagination && !showAll && (
            <Chip
              size="small"
              label={`Page ${currentPage} of ${totalPages}`}
              color="primary"
              variant="outlined"
            />
          )}
          {showAll && needsPagination && (
            <Chip
              size="small"
              label="Showing All"
              color="warning"
              icon={<WarningAmberIcon />}
            />
          )}
        </Box>

        {showCopyButton && (
          <Box sx={{ display: 'flex', gap: 0.5 }}>
            <Tooltip title={copied === 'page' ? 'Copied!' : 'Copy current page'}>
              <Button
                size="small"
                startIcon={copied === 'page' ? <CheckIcon /> : <ContentCopyIcon />}
                onClick={() => handleCopy('page')}
                color={copied === 'page' ? 'success' : 'primary'}
                variant="text"
              >
                Copy Page
              </Button>
            </Tooltip>
            {needsPagination && (
              <Tooltip title={copied === 'all' ? 'Copied!' : 'Copy all content'}>
                <Button
                  size="small"
                  startIcon={copied === 'all' ? <CheckIcon /> : <ContentCopyIcon />}
                  onClick={() => handleCopy('all')}
                  color={copied === 'all' ? 'success' : 'inherit'}
                  variant="text"
                >
                  Copy All
                </Button>
              </Tooltip>
            )}
          </Box>
        )}
      </Box>

      {/* Warning for large content */}
      {showAll && isLargeContent && (
        <Alert
          severity={isHugeContent ? 'error' : 'warning'}
          icon={<WarningAmberIcon />}
          action={
            <Button color="inherit" size="small" onClick={handleShowPaginated}>
              Use Pagination
            </Button>
          }
        >
          {isHugeContent
            ? 'Displaying over 1MB of content may cause browser performance issues.'
            : 'Displaying large content. If you experience lag, switch to pagination.'}
        </Alert>
      )}

      {/* Pagination controls - top */}
      {needsPagination && !showAll && (
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            gap: 1,
            py: 1,
          }}
        >
          <Tooltip title="First page">
            <span>
              <IconButton
                size="small"
                onClick={() => setCurrentPage(1)}
                disabled={currentPage === 1}
              >
                <FirstPageIcon />
              </IconButton>
            </span>
          </Tooltip>

          <Pagination
            count={totalPages}
            page={currentPage}
            onChange={handlePageChange}
            size="small"
            showFirstButton={false}
            showLastButton={false}
            siblingCount={1}
            boundaryCount={1}
          />

          <Tooltip title="Last page">
            <span>
              <IconButton
                size="small"
                onClick={() => setCurrentPage(totalPages)}
                disabled={currentPage === totalPages}
              >
                <LastPageIcon />
              </IconButton>
            </span>
          </Tooltip>

          <Divider orientation="vertical" flexItem sx={{ mx: 1 }} />

          <Tooltip
            title={
              isHugeContent
                ? 'Warning: May cause performance issues with content over 1MB'
                : isLargeContent
                ? 'Warning: May cause lag with large content'
                : 'Show all content without pagination'
            }
          >
            <Button
              size="small"
              variant="text"
              color={isHugeContent ? 'error' : isLargeContent ? 'warning' : 'primary'}
              onClick={handleShowAll}
              startIcon={isLargeContent ? <WarningAmberIcon /> : undefined}
            >
              Show All
            </Button>
          </Tooltip>
        </Box>
      )}

      {/* Content area */}
      <Paper
        sx={{
          p: 2,
          backgroundColor: (theme) =>
            theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
          maxHeight: maxHeight,
          overflow: 'auto',
          ...(enableMarkdown ? markdownStyles : {}),
          ...paperSx,
        }}
      >
        {enableMarkdown ? (
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{displayContent}</ReactMarkdown>
        ) : (
          <Typography
            component="pre"
            sx={{
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              margin: 0,
            }}
          >
            {displayContent}
          </Typography>
        )}
      </Paper>

      {/* Pagination controls - bottom (for long content) */}
      {needsPagination && !showAll && (
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            gap: 1,
            py: 1,
          }}
        >
          <Pagination
            count={totalPages}
            page={currentPage}
            onChange={handlePageChange}
            size="small"
            siblingCount={1}
            boundaryCount={1}
          />
        </Box>
      )}

      {/* Show paginated button when showing all */}
      {showAll && needsPagination && (
        <Box sx={{ display: 'flex', justifyContent: 'center', pt: 1 }}>
          <Button
            size="small"
            variant="outlined"
            onClick={handleShowPaginated}
            color="primary"
          >
            Switch to Paginated View
          </Button>
        </Box>
      )}
    </Box>
  );
};

export default PaginatedOutput;
