import React from 'react';
import {
  Box,
  Typography,
  ListItem,
  ListItemAvatar,
  Avatar,
  Stack,
  Chip,
  IconButton,
  Tooltip,
  Fade,
} from '@mui/material';
import TerminalIcon from '@mui/icons-material/Terminal';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import { ChatMessage } from '../types';
import { stripAnsiEscapes } from '../utils/textProcessing';

interface GroupedTraceMessagesProps {
  messages: ChatMessage[];
  onOpenLogs?: (jobId: string) => void;
}

export const GroupedTraceMessages: React.FC<GroupedTraceMessagesProps> = ({ messages, onOpenLogs }) => {
  if (messages.length === 0) return null;

  // Get the first message for metadata
  const firstMessage = messages[0];
  const jobId = firstMessage.jobId;

  return (
    <Fade in={true} timeout={300}>
      <ListItem 
        alignItems="flex-start"
        sx={{
          flexDirection: 'row',
          gap: 1,
          py: 1.5,
          width: '100%',
          maxWidth: '100%',
          overflow: 'hidden',
          minWidth: 0,
          '& > *': {
            minWidth: 0,
          }
        }}
      >
        <ListItemAvatar sx={{ minWidth: 'auto' }}>
          <Avatar
            sx={{
              bgcolor: 'warning.main',
              width: 32,
              height: 32,
            }}
          >
            <TerminalIcon sx={{ fontSize: 20 }} />
          </Avatar>
        </ListItemAvatar>
        
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Stack direction="column" spacing={1}>
            {/* Header - shown only once */}
            <Stack direction="row" spacing={1} alignItems="center">
              <Typography variant="caption" color="text.secondary">
                Trace Output
              </Typography>
              {firstMessage.eventSource && (
                <Chip
                  label={firstMessage.eventSource}
                  size="small"
                  variant="outlined"
                  color="primary"
                />
              )}
              {firstMessage.eventContext && (
                <Chip
                  label={firstMessage.eventContext}
                  size="small"
                  variant="outlined"
                  color="secondary"
                />
              )}
              {jobId && onOpenLogs && (
                <Tooltip title="View execution logs">
                  <IconButton
                    size="small"
                    onClick={() => onOpenLogs(jobId)}
                    sx={{ ml: 1 }}
                  >
                    <OpenInNewIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              )}
            </Stack>

            {/* Combined content for all trace messages */}
            <Box
              sx={{
                color: 'text.primary',
                wordBreak: 'break-word',
                whiteSpace: 'pre-wrap',
                maxWidth: '100%',
                overflow: 'visible',
                fontFamily: 'monospace',
                fontSize: '0.875rem',
              }}
            >
              {messages.map((message, index) => {
                const processedContent = stripAnsiEscapes(message.content);
                
                // Check if content is wrapped in markdown code block
                const codeBlockMatch = processedContent.match(/^```(?:json)?\s*\n([\s\S]*?)\n```$/);
                let contentToProcess = processedContent;
                if (codeBlockMatch) {
                  contentToProcess = codeBlockMatch[1].trim();
                }
                
                // Format the metadata line
                const metadataLine = [
                  message.eventType || message.eventSource || 'trace',
                  message.eventContext,
                  message.timestamp.toLocaleString('en-GB', { 
                    day: '2-digit',
                    month: '2-digit', 
                    year: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit',
                    hour12: false
                  })
                ].filter(Boolean).join('\n');
                
                // Try to parse as JSON for better formatting
                let contentElement;
                try {
                  const jsonContent = JSON.parse(contentToProcess);
                  
                  // Check if this is a JSON result (has suggestions, quality, or entities)
                  const isJsonResult = jsonContent.suggestions || jsonContent.quality || jsonContent.entities;
                  
                  if (isJsonResult) {
                    // JSON results get a box but still in grey since they're trace messages
                    contentElement = (
                      <Box
                        component="pre"
                        sx={{
                          overflow: 'auto',
                          maxHeight: '400px',
                          backgroundColor: 'rgba(0, 0, 0, 0.05)',
                          p: 1,
                          borderRadius: 1,
                          border: '1px solid rgba(0, 0, 0, 0.1)',
                          mt: 0.5,
                          maxWidth: '100%',
                          width: '100%',
                          boxSizing: 'border-box',
                          minWidth: 0,
                          overflowX: 'auto',
                          overflowY: 'auto',
                          color: 'rgba(0, 0, 0, 0.4)', // Light grey for trace messages
                        }}
                      >
                        <code>{JSON.stringify(jsonContent, null, 2)}</code>
                      </Box>
                    );
                  } else {
                    // Other JSON content without box in light grey
                    contentElement = (
                      <Box sx={{ whiteSpace: 'pre-wrap', color: 'rgba(0, 0, 0, 0.4)' }}>
                        {JSON.stringify(jsonContent, null, 2)}
                      </Box>
                    );
                  }
                } catch (e) {
                  // Not JSON, render as regular content in light grey
                  contentElement = (
                    <Box sx={{ whiteSpace: 'pre-wrap', color: 'rgba(0, 0, 0, 0.4)' }}>
                      {processedContent}
                    </Box>
                  );
                }
                
                return (
                  <Box key={message.id} sx={{ mb: index < messages.length - 1 ? 3 : 0 }}>
                    <Box sx={{ color: 'rgba(0, 0, 0, 0.4)', fontSize: '0.813rem', mb: 0.5 }}>
                      {metadataLine}
                    </Box>
                    {contentElement}
                  </Box>
                );
              })}
            </Box>
          </Stack>
        </Box>
      </ListItem>
    </Fade>
  );
};