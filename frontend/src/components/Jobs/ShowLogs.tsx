import React, { useEffect, useRef, useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  CircularProgress,
  Alert,
  IconButton,
  Tooltip,
  Chip,
  Paper,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import { LogEntry } from '../../api/ExecutionLogs';
import { executionLogService } from '../../api/ExecutionLogs';
import { ShowLogsProps } from '../../types/common';
import { useTranslation } from 'react-i18next';

// Strip ANSI escape sequences (including the ESC character)
const stripAnsiEscapes = (text: string): string => {
  if (!text) return '';

  // Create the escape character as a string
  const ESC = String.fromCharCode(27); // ASCII 27 (ESC)
  const BEL = String.fromCharCode(7);  // ASCII 7 (BEL)

  // Process the text to remove ANSI sequences and leading brackets
  return text
    // Standard ANSI colors and styles 
    .replace(new RegExp(`${ESC}\\[[0-9;]*[ABCDEFGHJKLMSTfminsulh]`, 'g'), '')
    // Control sequences with BEL terminator
    .replace(new RegExp(`${ESC}\\][^${BEL}]*${BEL}`, 'g'), '')
    // Single character controls
    .replace(new RegExp(`${ESC}[NOPGKabcdfghinqrstuABCDHIJMOPRTl]`, 'g'), '')
    // Other formats
    .replace(new RegExp(`${ESC}[\\[\\]\\?\\(\\)#;0-9]*[0-9A-Za-z]`, 'g'), '')
    // Final cleanup of any remaining ESC characters
    .replace(new RegExp(ESC, 'g'), '')
    // Remove leading brackets, parentheses, and vertical bars - fixed regex
    .replace(/^[([{|]+/gm, '');
};

// Function to clean and parse log content
const cleanLogContent = (content: string): { content: string, type: string } => {
  if (!content) return { content: '', type: 'INFO' };
  
  // First strip ANSI escape sequences including ESC character
  let cleanedContent = stripAnsiEscapes(content);
  
  // Remove leading brackets, parentheses, and vertical bars - fixed regex
  cleanedContent = cleanedContent.replace(/^[([{|]+/gm, '').trim();
  
  // Remove redundant prefixes
  
  // Clean CREW-STDOUT prefix
  if (cleanedContent.includes('INFO - CREW-STDOUT:')) {
    cleanedContent = cleanedContent.replace(/INFO - CREW-STDOUT:\s*/, '').trim();
  }
  
  // Clean CREWAI-LOG prefix
  if (cleanedContent.includes('INFO - CREWAI-LOG:')) {
    cleanedContent = cleanedContent.replace(/INFO - CREWAI-LOG:\s*/, '').trim();
  }
  
  // Clean EVENT prefix
  if (cleanedContent.includes('INFO - EVENT-')) {
    cleanedContent = cleanedContent.replace(/INFO - EVENT-/, 'EVENT: ').trim();
  }
  
  // Clean CREW prefix
  if (cleanedContent.includes('[CREW]')) {
    // Extract just the meaningful part of the log after the timestamp
    const match = cleanedContent.match(/\[CREW\]\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}\s*-\s*(.*)/);
    if (match && match[1]) {
      cleanedContent = match[1].trim();
    } else {
      cleanedContent = cleanedContent.replace(/\[CREW\]/g, '').trim();
    }
  }
  
  // Remove ALL tree characters (more aggressive regex)
  cleanedContent = cleanedContent.replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, ' ').replace(/\s{2,}/g, ' ');
  
  // Determine log type
  let type = 'INFO';
  if (content.includes('[LLM-CALL-START]')) type = 'LLM-CALL';
  else if (content.includes('Error') || content.includes('ERROR') || content.includes('Failed') || content.includes('❌')) type = 'ERROR';
  else if (content.includes('STDOUT')) type = 'INFO'; // Change STDOUT to INFO to avoid redundant display
  else if (content.includes('CREWAI-LOG:')) type = 'API';
  else if (content.includes('EVENT-')) type = 'EVENT';
  else if (content.includes('[CREW]')) type = 'CREW';
  else if (content.includes('🚀 Crew:')) type = 'CREW';
  
  return { content: cleanedContent, type };
};

// Function to get log type color
const getLogTypeColor = (type: string): string => {
  switch(type) {
    case 'ERROR': return '#ff5252';
    case 'LLM-CALL': return '#ffab40';
    case 'CREW': return '#42a5f5';
    case 'STDOUT': return '#66bb6a';
    case 'API': return '#7986cb';
    case 'EVENT': return '#ba68c8';
    default: return '#e0e0e0';
  }
};

// Map ANSI color codes to CSS colors
const ansiColorMap: Record<string, string> = {
  '30': '#000000', // Black
  '31': '#ff5252', // Red
  '32': '#4caf50', // Green
  '33': '#ffeb3b', // Yellow
  '34': '#2196f3', // Blue
  '35': '#e040fb', // Magenta
  '36': '#00bcd4', // Cyan
  '37': '#e0e0e0', // White
  '90': '#9e9e9e', // Bright Black (Gray)
  '91': '#ff8a80', // Bright Red
  '92': '#b9f6ca', // Bright Green
  '93': '#ffff8d', // Bright Yellow
  '94': '#82b1ff', // Bright Blue
  '95': '#ea80fc', // Bright Magenta
  '96': '#84ffff', // Bright Cyan
  '97': '#ffffff', // Bright White
};

// Handle ANSI escape sequences
const parseAnsiString = (text: string): React.ReactNode => {
  if (!text || !text.includes('[')) return text;
  
  // Split by ANSI escape sequences
  const parts = text.split(/(\[\d+(?:;\d+)*m|\[0m)/);
  if (parts.length <= 1) return text;
  
  let activeColor = '';
  let isBold = false;
  
  return (
    <>
      {parts.map((part, index) => {
        // If this is an escape sequence, update state and return null
        if (part.match(/\[\d+(?:;\d+)*m/)) {
          // Check for reset
          if (part === '[0m') {
            activeColor = '';
            isBold = false;
            return null;
          }
          
          // Extract the numbers from the sequence
          const codes = part.match(/\[(\d+(?:;\d+)*)m/)?.[1].split(';') || [];
          
          codes.forEach(code => {
            // Handle color codes
            if (ansiColorMap[code]) {
              activeColor = ansiColorMap[code];
            }
            // Handle bold
            else if (code === '1') {
              isBold = true;
            }
          });
          
          return null;
        }
        
        // Render actual content with current styling
        return part ? (
          <span 
            key={index}
            style={{
              color: activeColor || undefined,
              fontWeight: isBold ? 'bold' : undefined,
            }}
          >
            {part}
          </span>
        ) : null;
      })}
    </>
  );
};

// Extract content from box drawing log message
const extractBoxContent = (text: string): { title?: string, lines: string[] } => {
  // First clean all box drawing characters
  text = text.replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, ' ').replace(/\s{2,}/g, ' ');
  
  const lines = text.split('\n').filter(line => line.trim() !== '');
  let title = '';
  
  // Extract title from first line
  if (lines.length > 0) {
    title = lines[0].trim();
  }
  
  // Content lines are all the rest
  const contentLines = lines.slice(1).map(line => line.trim()).filter(Boolean);
  
  return { title, lines: contentLines };
};

// Render box content as a styled component instead of using ASCII characters
const renderStyledBox = (text: string): React.ReactNode => {
  const isError = text.includes('Error') || text.includes('Failure');
  const _isSuccess = text.includes('Completed') || text.includes('Success');
  const isInfo = text.includes('Crew Execution Started');
  
  // Determine the box color based on content
  let boxColor = '#4caf50'; // Default green
  let titleColor = '#66bb6a';
  let titleBgColor = 'rgba(102, 187, 106, 0.1)';
  
  if (isError) {
    boxColor = '#ff5252'; // Red for errors
    titleColor = '#ff5252';
    titleBgColor = 'rgba(255, 82, 82, 0.1)';
  } else if (isInfo) {
    boxColor = '#42a5f5'; // Blue for crew info
    titleColor = '#42a5f5';
    titleBgColor = 'rgba(66, 165, 245, 0.1)';
  }
  
  const { title, lines } = extractBoxContent(text);
  
  const getKeyValue = (line: string): { key: string, value: string } | null => {
    const match = line.match(/([^:]+):\s*(.*)/);
    return match ? { key: match[1].trim(), value: match[2].trim() } : null;
  };
  
  return (
    <Paper
      elevation={0}
      sx={{
        border: `1px solid ${boxColor}`,
        borderRadius: '4px',
        overflow: 'hidden',
        backgroundColor: 'rgba(33, 37, 43, 0.7)',
        mb: 0.5
      }}
    >
      {title && (
        <Box
          sx={{
            backgroundColor: titleBgColor,
            color: titleColor,
            fontWeight: 'bold',
            px: 1.5,
            py: 0.5,
            borderBottom: `1px solid ${boxColor}`,
            fontSize: '0.9rem'
          }}
        >
          {parseAnsiString(title)}
        </Box>
      )}
      <Box sx={{ p: 1.5 }}>
        {lines.map((line, i) => {
          const keyValue = getKeyValue(line);
          if (keyValue) {
            return (
              <Box key={i} sx={{ display: 'flex', mb: 0.25 }}>
                <Box sx={{ color: '#e0e0e0', minWidth: '100px' }}>{keyValue.key}:</Box>
                <Box sx={{ color: boxColor, fontWeight: 'bold', ml: 1 }}>
                  {parseAnsiString(keyValue.value)}
                </Box>
              </Box>
            );
          }
          return (
            <Box 
              key={i} 
              sx={{ 
                color: 'white',
                mb: 0.25,
                fontSize: '0.85rem',
                lineHeight: '1.2'
              }}
            >
              {parseAnsiString(line)}
            </Box>
          );
        })}
      </Box>
    </Paper>
  );
};

// Function to format log content with appropriate styling
const formatLogContent = (content: string): React.ReactNode => {
  if (!content) return null;
  
  // First clean any tree characters from the content with a more comprehensive regex
  const cleanedContent = content.replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, ' ').replace(/\s{2,}/g, ' ');
  
  // Don't detect based on box characters since we're removing them all
  // Instead, detect based on content patterns
  if (cleanedContent.includes('Crew Execution Started') || cleanedContent.includes('Completed') || 
      cleanedContent.includes('Error') || cleanedContent.includes('Failure')) {
    const boxContent = cleanedContent.replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, ' ');
    return renderStyledBox(boxContent);
  }
  
  // Handle tree-style logs with crew/task/agent icons
  if (cleanedContent.includes('🚀 Crew:') || cleanedContent.includes('📋 Task:') || 
      cleanedContent.includes('🤖 Agent:') || cleanedContent.includes('Assigned to:') || 
      cleanedContent.includes('Status:')) {
    return renderCrewTree(cleanedContent);
  }
  
  // Helper to create styled spans for different parts
  const createStyledContent = (text: string) => {
    // Clean any tree characters from this line with a more comprehensive regex
    text = text.replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, ' ').replace(/\s{2,}/g, ' ');

    // If the text contains ANSI color codes, parse them
    if (text.match(/\[\d+(?:;\d+)*m/) || text.includes('[0m')) {
      return parseAnsiString(text);
    }
    
    // Format specific patterns
    if (text.includes('❌')) {
      return <span style={{ color: '#ff5252', fontWeight: 'bold' }}>{text}</span>;
    }
    
    if (text.includes('🚀')) {
      return <span style={{ color: '#42a5f5', fontWeight: 'bold' }}>{text}</span>;
    }
    
    if (text.includes('Agent:')) {
      const match = text.match(/Agent:\s+(.+)$/);
      if (match) {
        return (
          <>
            <span>Agent: </span>
            <span style={{ color: '#4caf50', fontWeight: 'bold' }}>{match[1]}</span>
          </>
        );
      }
      return <span style={{ color: '#66bb6a', fontWeight: 'bold' }}>{text}</span>;
    }
    
    if (text.includes('Task:')) {
      const match = text.match(/Task:\s+(.+)$/);
      if (match) {
        return (
          <>
            <span>Task: </span>
            <span style={{ color: '#ffab40', fontWeight: 'bold' }}>{match[1]}</span>
          </>
        );
      }
      return <span style={{ color: '#ffab40', fontWeight: 'bold' }}>{text}</span>;
    }
    
    if (text.includes('Status:')) {
      const match = text.match(/Status:\s+(.+)$/);
      if (match) {
        return (
          <>
            <span>Status: </span>
            <span style={{ color: '#ba68c8', fontWeight: 'bold' }}>{match[1]}</span>
          </>
        );
      }
      return <span style={{ color: '#ba68c8', fontWeight: 'bold' }}>{text}</span>;
    }
    
    if (text.includes('Assigned to:')) {
      const match = text.match(/Assigned to:\s+(.+)$/);
      if (match) {
        return (
          <>
            <span>Assigned to: </span>
            <span style={{ color: '#4caf50', fontWeight: 'bold' }}>{match[1]}</span>
          </>
        );
      }
    }

    // Format HTTP requests for API logs
    if (text.match(/HTTP Request:/)) {
      return <span style={{ color: '#7986cb' }}>{text}</span>;
    }

    // Apply syntax highlighting for sections with numbered lists and headlines
    if (text.match(/^\d+\.\s+\*\*.+\*\*/)) {
      return <span style={{ color: '#81c784', fontWeight: 'bold' }}>{text}</span>;
    }

    return <span>{text}</span>;
  };

  // Split by lines to process each line
  const lines = cleanedContent.split('\n');
  return (
    <>
      {lines.map((line, i) => (
        <div key={i} style={{ lineHeight: '1.2' }}>
          {createStyledContent(line)}
        </div>
      ))}
    </>
  );
};

// New function to render the crew tree with only one vertical bar
const renderCrewTree = (content: string): React.ReactNode => {
  // Super aggressive cleaning of all possible tree characters
  content = content.replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, ' ').replace(/\s{2,}/g, ' ');
  
  const lines = content.split('\n').filter(line => line.trim() !== '');
  if (lines.length === 0) return null;
  
  // Process tree structure but ignore indentation levels
  const processedLines = lines.map((line, index) => {
    // Determine the type of line and extract the content
    const lineData = {
      raw: line,
      type: 'unknown',
      content: '',
      icon: '',
      label: '',
      value: '',
      isKeyValue: false
    };
    
    // Clean content - remove any remaining tree characters and leading brackets
    const cleanContent = line.trim()
      .replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, '')
      .replace(/^[([{|]+/, '') // Remove leading brackets/parentheses - fixed regex
      .trim();
    
    // Determine the type and parse content
    if (cleanContent.startsWith('🚀 Crew:')) {
      lineData.type = 'crew';
      lineData.icon = '🚀';
      lineData.content = cleanContent.replace('🚀 Crew:', '').trim();
    } 
    else if (cleanContent.startsWith('📋 Task:')) {
      lineData.type = 'task';
      lineData.icon = '📋';
      lineData.content = cleanContent.replace('📋 Task:', '').trim();
    } 
    else if (cleanContent.startsWith('🤖 Agent:')) {
      lineData.type = 'agent';
      lineData.icon = '🤖';
      lineData.content = cleanContent.replace('🤖 Agent:', '').trim();
    } 
    else if (cleanContent.includes('Assigned to:')) {
      lineData.type = 'assigned';
      lineData.isKeyValue = true;
      const parts = cleanContent.split('Assigned to:');
      lineData.label = 'Assigned to';
      lineData.value = parts[1].trim();
    } 
    else if (cleanContent.includes('Status:')) {
      lineData.type = 'status';
      lineData.isKeyValue = true;
      const parts = cleanContent.split('Status:');
      lineData.label = 'Status';
      lineData.value = parts[1].trim().replace(/[│├└─║╔╗╚╝╠╣╦╩╬╭╮╯╰]/g, '').trim();
    } 
    else {
      lineData.type = 'text';
      lineData.content = cleanContent;
    }
    
    return lineData;
  });
  
  // Render the processed lines with a single vertical bar
  return (
    <Box sx={{ 
      fontFamily: 'monospace', 
      my: 0.5,
      pl: 1.5,
      pt: 0.5,
      pb: 0.25,
      borderLeft: '2px solid #42a5f5',
      backgroundColor: 'rgba(66, 165, 245, 0.05)',
      borderRadius: '4px',
      fontSize: '0.85rem',
    }}>
      {processedLines.map((item, index) => {
        // Different styling based on type without indentation
        switch (item.type) {
          case 'crew':
            return (
              <Box 
                key={index} 
                sx={{ 
                  display: 'flex',
                  alignItems: 'center',
                  py: 0.25,
                  color: '#42a5f5',
                  fontWeight: 'bold',
                }}
              >
                <span style={{ marginRight: '4px' }}>{item.icon}</span>
                <span>Crew: {item.content}</span>
              </Box>
            );
            
          case 'task':
            return (
              <Box 
                key={index} 
                sx={{ 
                  display: 'flex',
                  alignItems: 'center',
                  py: 0.25,
                  color: '#ffab40',
                  fontWeight: 'bold',
                }}
              >
                <span style={{ marginRight: '4px' }}>{item.icon}</span>
                <span>Task: {item.content}</span>
              </Box>
            );
            
          case 'agent':
            return (
              <Box 
                key={index} 
                sx={{ 
                  display: 'flex',
                  alignItems: 'center',
                  py: 0.25,
                  color: '#4caf50',
                  fontWeight: 'bold',
                }}
              >
                <span style={{ marginRight: '4px' }}>{item.icon}</span>
                <span>Agent: {item.content}</span>
              </Box>
            );
            
          case 'assigned':
            return (
              <Box 
                key={index} 
                sx={{ 
                  display: 'flex',
                  py: 0.25,
                }}
              >
                <span style={{ color: '#e0e0e0', minWidth: '100px' }}>Assigned to:</span>
                <span style={{ color: '#4caf50', fontWeight: 'bold', marginLeft: '4px' }}>{item.value}</span>
              </Box>
            );
            
          case 'status': {
            // Determine color based on status value
            let statusColor = '#ba68c8'; // Default purple
            if (item.value.includes('✅ Completed')) {
              statusColor = '#66bb6a'; // Green
            } else if (item.value.includes('❌')) {
              statusColor = '#ff5252'; // Red 
            } else if (item.value.includes('Executing')) {
              statusColor = '#ffab40'; // Orange
            }
            
            return (
              <Box 
                key={index} 
                sx={{ 
                  display: 'flex',
                  py: 0.25,
                }}
              >
                <span style={{ color: '#e0e0e0', minWidth: '100px' }}>Status:</span>
                <span style={{ color: statusColor, fontWeight: 'bold', marginLeft: '4px' }}>{item.value}</span>
              </Box>
            );
          }
          
          default:
            return (
              <Box 
                key={index} 
                sx={{ 
                  py: 0.25,
                  color: '#e0e0e0',
                }}
              >
                {item.content}
              </Box>
            );
        }
      })}
    </Box>
  );
};

// Process logs to group related entries and clean content
const processLogs = (logs: LogEntry[]): LogEntry[] => {
  if (!logs || logs.length === 0) return [];
  
  // Clean each log entry
  return logs.map(log => {
    const originalContent = log.output || log.content || '';
    const { content, type } = cleanLogContent(originalContent);
    
    return {
      ...log,
      output: content,
      content,
      logType: type
    };
  });
};

// Consolidate similar consecutive API logs
const consolidateLogs = (logs: LogEntry[]): LogEntry[] => {
  if (logs.length <= 1) return logs;
  
  const consolidated: LogEntry[] = [];
  let index = 0;
  
  while (index < logs.length) {
    const currentLog = logs[index];
    const currentContent = currentLog.content || '';
    
    // Skip timestamp-only logs (often appear between real content)
    if (currentContent.match(/^\d{2}:\d{2}:\d{2}\s+[AP]M$/)) {
      index++;
      continue;
    }
    
    // Process API logs specially
    if (currentLog.logType === 'API' && currentContent.includes('HTTP Request:')) {
      // Look ahead for similar API logs to consolidate
      let consolidatedContent = currentContent;
      let nextIndex = index + 1;
      
      // Skip timestamp logs and collect similar HTTP requests
      while (nextIndex < logs.length) {
        const nextLog = logs[nextIndex];
        const nextContent = nextLog.content || '';
        
        // Skip timestamp-only logs
        if (nextContent.match(/^\d{2}:\d{2}:\d{2}\s+[AP]M$/)) {
          nextIndex++;
          continue;
        }
        
        // If it's a similar API log, consolidate it
        if (nextLog.logType === 'API' && nextContent.includes('HTTP Request:')) {
          consolidatedContent += '\n' + nextContent;
          nextIndex++;
        } else {
          // Not a similar API log, stop consolidating
          break;
        }
      }
      
      // Create a consolidated log entry
      consolidated.push({
        ...currentLog,
        content: consolidatedContent,
        output: consolidatedContent
      });
      
      // Move past all consolidated logs
      index = nextIndex;
    } else {
      // For non-API logs, just add them as is
      consolidated.push(currentLog);
      index++;
    }
  }
  
  return consolidated;
};

// Determine if a type badge should be shown for this log entry
const shouldShowTypeBadge = (logType: string, prevLogType: string | null): boolean => {
  // Don't show badge for regular INFO logs
  if (logType === 'INFO') return false;
  
  // Show badge if this is the first log or if the log type has changed
  return prevLogType === null || logType !== prevLogType;
};

const ShowLogs: React.FC<ShowLogsProps> = ({
  open,
  onClose,
  logs,
  jobId,
  isConnecting,
  connectionError,
}) => {
  const [localLogs, setLocalLogs] = useState<LogEntry[]>(logs);
  const [processedLogs, setProcessedLogs] = useState<LogEntry[]>([]);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const LIMIT = 1000;
  const { t } = useTranslation();

  const filterEmptyLogs = (logs: LogEntry[]): LogEntry[] => {
    return logs.filter(log => {
      // Remove logs that are empty, only whitespace, or just newlines
      const logContent = log.output || log.content;
      if (!logContent) return false;
      const trimmedOutput = logContent.trim();
      return trimmedOutput.length > 0;
    });
  };

  useEffect(() => {
    // Sort logs by timestamp and filter empty ones
    const sortedLogs = [...logs]
      .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
    const filteredLogs = filterEmptyLogs(sortedLogs);
    setLocalLogs(filteredLogs);
    
    // Process logs for display
    const processed = processLogs(filteredLogs);
    const consolidated = consolidateLogs(processed);
    setProcessedLogs(consolidated);
  }, [logs]);

  useEffect(() => {
    if (autoScroll && scrollContainerRef.current) {
      scrollContainerRef.current.scrollTop = scrollContainerRef.current.scrollHeight;
    }
  }, [processedLogs, autoScroll]);

  const loadMoreLogs = async () => {
    if (isLoadingMore || !hasMore) return;
    
    try {
      setIsLoadingMore(true);
      const nextOffset = offset + LIMIT;
      const moreLogs = await executionLogService.getHistoricalLogs(jobId, LIMIT, nextOffset);
      
      if (moreLogs.length === 0) {
        setHasMore(false);
      } else {
        const newLogs = filterEmptyLogs(
          moreLogs
            .map(({ job_id, execution_id, ...rest }) => ({
              ...rest,
              output: rest.output || rest.content
            }))
            .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())
        );
        const updatedLogs = [...localLogs, ...newLogs];
        setLocalLogs(updatedLogs);
        
        const processed = processLogs(updatedLogs);
        const consolidated = consolidateLogs(processed);
        setProcessedLogs(consolidated);
        
        setOffset(nextOffset);
      }
    } catch (error) {
      console.error('Error loading more logs:', error);
    } finally {
      setIsLoadingMore(false);
    }
  };

  const handleScroll = () => {
    if (!scrollContainerRef.current) return;

    const { scrollTop, scrollHeight, clientHeight } = scrollContainerRef.current;
    
    // Check if user has scrolled up
    const isAtBottom = scrollHeight - scrollTop - clientHeight < 50;
    setAutoScroll(isAtBottom);

    // Load more when scrolled near top
    if (scrollTop < 50 && !isLoadingMore && hasMore) {
      loadMoreLogs();
    }
  };

  const handleRefresh = async () => {
    try {
      setIsRefreshing(true);
      const latestLogs = await executionLogService.getHistoricalLogs(jobId, LIMIT, 0);
      const convertedLogs = filterEmptyLogs(
        latestLogs
          .map(({ job_id, execution_id, ...rest }) => ({
            ...rest,
            output: rest.output || rest.content
          }))
          .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())
      );
      
      // Compare if there are actual new logs to avoid unnecessary state updates
      const currentLogIds = new Set(localLogs.map(log => log.id));
      const hasNewLogs = convertedLogs.some(log => !currentLogIds.has(log.id));
      
      if (hasNewLogs || convertedLogs.length !== localLogs.length) {
        setLocalLogs(convertedLogs);
        
        const processed = processLogs(convertedLogs);
        const consolidated = consolidateLogs(processed);
        setProcessedLogs(consolidated);
        
        setOffset(0);
        setHasMore(true);
        setAutoScroll(true);
      }
    } catch (error) {
      console.error('Error refreshing logs:', error);
    } finally {
      setIsRefreshing(false);
    }
  };

  // Render logs with badges only when the log type changes
  const renderLogs = () => {
    let previousLogType: string | null = null;
    
    return processedLogs.map((log, index) => {
      const content = log.output || log.content || '';
      const logType = log.logType || 'INFO';
      
      // Determine if we should show badge for this log
      const showBadge = shouldShowTypeBadge(logType, previousLogType);
      
      // Update the previous log type for the next entry
      previousLogType = logType;
      
      return (
        <Box
          key={`${log.timestamp}-${index}`}
          sx={{
            mb: 0.5,
            py: 0.25,
            px: 0.5,
            borderRadius: '4px',
            backgroundColor: logType === 'ERROR' ? 'rgba(255,82,82,0.1)' : 'transparent',
            borderLeft: `3px solid ${getLogTypeColor(logType)}`,
            '&:hover': { bgcolor: 'rgba(255,255,255,0.05)' }
          }}
        >
          {showBadge && (
            <Chip 
              label={logType}
              size="small"
              sx={{ 
                height: '16px',
                fontSize: '0.7rem',
                backgroundColor: getLogTypeColor(logType),
                color: '#000',
                '& .MuiChip-label': { px: 0.75 },
                mb: 0.25
              }}
            />
          )}
          
          <Box 
            sx={{ 
              color: logType === 'INFO' ? '#e0e0e0' : 
                     logType === 'ERROR' ? '#ff8a80' : 
                     logType === 'STDOUT' ? '#b9f6ca' : 
                     logType === 'API' ? '#c5cae9' :
                     logType === 'EVENT' ? '#e1bee7' :
                     '#e1f5fe',
              fontFamily: 'monospace',
              fontSize: '0.85rem',
              mt: showBadge ? 0.25 : 0,
              lineHeight: '1.2'
            }}
          >
            {formatLogContent(content)}
          </Box>
        </Box>
      );
    });
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: { height: '80vh' }
      }}
    >
      <DialogTitle>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography>{t('logs.executionLogs.title')}</Typography>
          <Box>
            <Tooltip title={autoScroll ? t('logs.executionLogs.autoScrollOn') : t('logs.executionLogs.autoScrollOff')}>
              <Typography variant="caption" sx={{ mr: 2, color: autoScroll ? 'success.main' : 'text.secondary' }}>
                {autoScroll ? t('logs.executionLogs.autoScrollOn') : t('logs.executionLogs.autoScrollOff')}
              </Typography>
            </Tooltip>
            <Tooltip title={t('logs.executionLogs.refreshLogs')}>
              <IconButton 
                onClick={handleRefresh}
                disabled={isRefreshing}
              >
                <RefreshIcon />
              </IconButton>
            </Tooltip>
          </Box>
        </Box>
      </DialogTitle>
      
      <DialogContent>
        {isConnecting && (
          <Box display="flex" justifyContent="center" mb={2}>
            <CircularProgress size={20} />
            <Typography variant="body2" ml={1}>
              {t('logs.executionLogs.connecting')}
            </Typography>
          </Box>
        )}
        
        {connectionError && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {t('logs.executionLogs.connectionError')}
          </Alert>
        )}

        {isLoadingMore && (
          <Box display="flex" justifyContent="center" mt={2}>
            <CircularProgress size={20} />
          </Box>
        )}

        <Box
          ref={scrollContainerRef}
          onScroll={handleScroll}
          sx={{
            height: 'calc(100% - 20px)',
            overflowY: 'auto',
            bgcolor: '#21252b',
            p: 2,
            borderRadius: 1,
            fontFamily: 'monospace',
            fontSize: '0.875rem',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word'
          }}
        >
          {renderLogs()}
        </Box>
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>{t('common.close')}</Button>
      </DialogActions>
    </Dialog>
  );
};

export default ShowLogs; 