import React from 'react';
import {
  ListItem,
  ListItemAvatar,
  Avatar,
  ListItemText,
  Chip,
  Box,
  Typography,
  Fade,
  Stack,
  IconButton,
  Tooltip,
} from '@mui/material';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import PersonIcon from '@mui/icons-material/Person';
import GroupIcon from '@mui/icons-material/Group';
import AssignmentIcon from '@mui/icons-material/Assignment';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import SettingsIcon from '@mui/icons-material/Settings';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import { ChatMessage } from '../types';
import { MessageContent } from './MessageRenderer';
import { stripAnsiEscapes, isMarkdown, isHtmlDocument } from '../utils/textProcessing';
import { GenieSpaceConfigPrompt } from '../GenieSpaceConfigPrompt';
import { UiSurfaceResult } from './UiSurfaceResult';
import { toSurface } from '../../ChatMode/utils/surfaceAdapter';
import type { ToolConfigNeededData } from '../../../hooks/global/useCrewGenerationSSE';

interface ChatMessageItemProps {
  message: ChatMessage;
  onOpenLogs?: (jobId: string) => void;
}

/** Pick the human-readable answer out of a result envelope object: the runner
 *  emits `{text, a2ui}`, older runs stored `{value: "<markdown>"}`. */
const pickEnvelopeText = (obj: Record<string, unknown>): string | null => {
  for (const key of ['text', 'value']) {
    const v = obj[key];
    if (typeof v === 'string' && v.trim()) return v;
  }
  return null;
};

/**
 * Extract the markdown answer text from a result message's content, or null
 * when there is none (plain markdown, raw JSON without a text field, …).
 * Accepts the `{text, a2ui}` envelope, the legacy `{value}` shape, and the
 * same one-level `{result: {…}}` / `{output: "<json>"}` wrappers `toSurface`
 * unwraps — deliberately WITHOUT a deep walk, so a component's own `text`
 * prop inside the a2ui surface can never be mistaken for the answer.
 */
const extractResultText = (raw: string): string | null => {
  const trimmed = raw.trim();
  if (!trimmed.startsWith('{')) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
  const obj = parsed as Record<string, unknown>;
  const direct = pickEnvelopeText(obj);
  if (direct) return direct;
  for (const key of ['result', 'output']) {
    const wrapped = obj[key];
    if (typeof wrapped === 'string') {
      const nested = extractResultText(wrapped);
      if (nested) return nested;
    } else if (wrapped && typeof wrapped === 'object' && !Array.isArray(wrapped)) {
      const nested = pickEnvelopeText(wrapped as Record<string, unknown>);
      if (nested) return nested;
    }
  }
  return null;
};

/** Normalize a result's markdown: trim lines, drop empty ones, and re-insert
 *  single blank lines around headers / after lists / between paragraphs so the
 *  rendered markdown stays compact without excessive line breaks. */
const normalizeResultMarkdown = (content: string): string => {
  const lines = content.split('\n').map((line: string) => line.trim());
  const processedLines: string[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const nextLine = lines[i + 1] || '';

    // Skip empty lines
    if (line === '') continue;

    // Check if this is a list item
    const isListItem = line.match(/^[-*•]\s/) || line.match(/^\d+\.\s/);
    const nextIsListItem = nextLine.match(/^[-*•]\s/) || nextLine.match(/^\d+\.\s/);

    // Check if this is a header
    const isHeader = line.match(/^#+\s/);

    processedLines.push(line);

    // Add spacing logic
    if (isHeader && i < lines.length - 1) {
      // Add blank line after headers
      processedLines.push('');
    } else if (isListItem && !nextIsListItem && nextLine !== '') {
      // Add blank line after list ends
      processedLines.push('');
    } else if (!isListItem && !isHeader && nextLine !== '' && !nextIsListItem) {
      // Add blank line between paragraphs
      processedLines.push('');
    }
  }

  return processedLines.join('\n').trim();
};

/** Styling for the final answer text: plain content on the chat's default
 *  background — no border, no card chrome — with compact markdown typography. */
const resultTextSx = {
  py: 0.5,
  maxWidth: '100%',
  width: '100%',
  boxSizing: 'border-box',
  color: 'text.primary',
  // Override markdown default spacing for compact display
  '& h1, & h2, & h3': {
    marginTop: '0.75em',
    marginBottom: '0.5em',
    fontSize: '1.1em',
    fontWeight: 600,
  },
  '& h4, & h5, & h6': {
    marginTop: '0.5em',
    marginBottom: '0.25em',
    fontSize: '1em',
    fontWeight: 600,
  },
  '& p': {
    margin: '0.4em 0',
    lineHeight: 1.4,
  },
  '& ul, & ol': {
    margin: '0.4em 0',
    paddingLeft: '1.5em',
  },
  '& li': {
    margin: '0.2em 0',
    lineHeight: 1.4,
  },
  '& li p': {
    margin: 0,
    display: 'inline',
  },
  '& > *:first-of-type': {
    marginTop: 0,
  },
  '& > *:last-child': {
    marginBottom: 0,
  },
  // Remove empty paragraphs
  '& p:empty': {
    display: 'none',
  },
  // Compact line height
  lineHeight: 1.5,
} as const;

export const ChatMessageItem: React.FC<ChatMessageItemProps> = ({ message, onOpenLogs }) => {
  const getIntentIcon = (intent?: string) => {
    switch (intent) {
      case 'generate_agent':
        return <SmartToyIcon />;
      case 'generate_task':
        return <AssignmentIcon />;
      case 'generate_crew':
        return <GroupIcon />;
      case 'configure_crew':
        return <SettingsIcon />;
      default:
        return <AccountTreeIcon />;
    }
  };

  const getIntentColor = (intent?: string): 'primary' | 'secondary' | 'success' | 'default' => {
    switch (intent) {
      case 'generate_agent':
        return 'primary';
      case 'generate_task':
        return 'secondary';
      case 'generate_crew':
        return 'success';
      default:
        return 'default';
    }
  };

  const renderMessageContent = () => {
    // Genie space config prompt
    if (message.metadata?.type === 'genie_config_needed' && Array.isArray(message.metadata.configs)) {
      return <GenieSpaceConfigPrompt configs={message.metadata.configs as ToolConfigNeededData[]} />;
    }

    // Process content to remove ANSI codes
    const processedContent = stripAnsiEscapes(message.content);

    // Special handling for result messages
    if (message.type === 'result') {
      // A2UI surfaces render as their designed layout (like Chat mode's
      // preview), not as raw JSON. toSurface accepts the new envelope, a bare
      // surface, or an older legacy document (adapted) — one renderer for all.
      // When the envelope also carries the markdown answer ({text, a2ui}),
      // show the answer FIRST (plain, no card chrome) and the Generated UI
      // card below it.
      const surface = toSurface(processedContent);
      const answerText = extractResultText(processedContent);
      if (surface) {
        return (
          <Box sx={{ width: '100%', maxWidth: '100%' }}>
            {answerText && (
              <Box data-testid="result-text" sx={{ ...resultTextSx, mb: 1.5 }}>
                <MessageContent content={normalizeResultMarkdown(answerText)} />
              </Box>
            )}
            <UiSurfaceResult surface={surface} />
          </Box>
        );
      }

      // Envelope / legacy {value} text without a surface: plain markdown.
      if (answerText) {
        return (
          <Box data-testid="result-text" sx={resultTextSx}>
            <MessageContent content={normalizeResultMarkdown(answerText)} />
          </Box>
        );
      }

      // Try to parse as JSON
      try {
        const jsonContent = JSON.parse(processedContent);
        // Regular JSON, display formatted
        return (
          <Box
            component="pre"
            sx={{
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              overflow: 'auto',
              maxHeight: '600px',
              backgroundColor: 'rgba(0, 0, 0, 0.05)',
              p: 2,
              borderRadius: 1,
              border: '1px solid rgba(0, 0, 0, 0.1)',
              m: 0,
              maxWidth: '100%',
              width: '100%',
              boxSizing: 'border-box',
              minWidth: 0,
              overflowX: 'auto',
              overflowY: 'auto',
              '& code': {
                display: 'block',
                whiteSpace: 'pre',
                wordBreak: 'normal',
                overflowWrap: 'normal',
              }
            }}
          >
            <code>{JSON.stringify(jsonContent, null, 2)}</code>
          </Box>
        );
      } catch (e) {
        // Not JSON: the final answer markdown renders as plain content on the
        // chat's default background — no border, no card chrome.
        return (
          <Box data-testid="result-text" sx={resultTextSx}>
            <MessageContent content={normalizeResultMarkdown(processedContent)} />
          </Box>
        );
      }
    }
    
    // Special handling for trace messages with JSON content
    if (message.type === 'trace') {
      let contentToProcess = processedContent;
      
      // Check if content is wrapped in markdown code block
      const codeBlockMatch = processedContent.match(/^```(?:json)?\s*\n([\s\S]*?)\n```$/);
      if (codeBlockMatch) {
        contentToProcess = codeBlockMatch[1].trim();
      }
      
      // Try to parse as JSON for better formatting
      try {
        const jsonContent = JSON.parse(contentToProcess);
        return (
          <Box
            component="pre"
            sx={{
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              overflow: 'auto',
              maxHeight: '400px',
              backgroundColor: 'rgba(0, 0, 0, 0.05)',
              p: 1,
              borderRadius: 1,
              m: 0,
              maxWidth: '100%',
              width: '100%',
              boxSizing: 'border-box',
              minWidth: 0, // Critical for flex containers
              overflowX: 'auto', // Allow horizontal scrolling for wide content
              overflowY: 'auto',
              '& code': {
                display: 'block',
                whiteSpace: 'pre',
                wordBreak: 'normal', // Don't break words in JSON
                overflowWrap: 'normal', // Don't wrap in JSON
                minWidth: 'max-content', // Allow code to be as wide as needed
              }
            }}
          >
            <code>{JSON.stringify(jsonContent, null, 2)}</code>
          </Box>
        );
      } catch (e) {
        // Not JSON, render as regular content
      }
    }
    
    if (message.type === 'assistant' && message.result) {
      const result = message.result as Record<string, unknown>;
      
      // Handle agent generation
      if (message.intent === 'generate_agent' && result.agent) {
        const agent = result.agent as { name: string; role: string };
        return (
          <Box>
            <Typography variant="body1" sx={{ mb: 1 }}>
              I&apos;ve created an agent for you:
            </Typography>
            <Box sx={{ bgcolor: 'background.paper', p: 2, borderRadius: 1 }}>
              <Typography variant="h6" sx={{ mb: 1 }}>{agent.name}</Typography>
              <Typography variant="body2" color="text.secondary">
                Role: {agent.role}
              </Typography>
            </Box>
          </Box>
        );
      }
      
      // Handle task generation
      if (message.intent === 'generate_task' && result.task) {
        const task = result.task as { name: string; description: string };
        return (
          <Box>
            <Typography variant="body1" sx={{ mb: 1 }}>
              I&apos;ve created a task for you:
            </Typography>
            <Box sx={{ bgcolor: 'background.paper', p: 2, borderRadius: 1 }}>
              <Typography variant="h6" sx={{ mb: 1 }}>{task.name}</Typography>
              <Typography variant="body2" color="text.secondary">
                {task.description}
              </Typography>
            </Box>
          </Box>
        );
      }
      
      // Handle crew generation
      if (message.intent === 'generate_crew' && result.crew) {
        const crew = result.crew as { agents?: Array<{ name: string }>; tasks?: Array<{ name: string }> };
        return (
          <Box>
            <Typography variant="body1" sx={{ mb: 1 }}>
              I&apos;ve created a complete plan for you:
            </Typography>
            <Box sx={{ bgcolor: 'background.paper', p: 2, borderRadius: 1 }}>
              {crew.agents && crew.agents.length > 0 && (
                <Box sx={{ mb: 2 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 'bold', mb: 1 }}>
                    Agents:
                  </Typography>
                  {crew.agents.map((agent, idx) => (
                    <Typography key={idx} variant="body2" sx={{ ml: 2 }}>
                      • {agent.name}
                    </Typography>
                  ))}
                </Box>
              )}
              {crew.tasks && crew.tasks.length > 0 && (
                <Box>
                  <Typography variant="subtitle1" sx={{ fontWeight: 'bold', mb: 1 }}>
                    Tasks:
                  </Typography>
                  {crew.tasks.map((task, idx) => (
                    <Typography key={idx} variant="body2" sx={{ ml: 2 }}>
                      • {task.name}
                    </Typography>
                  ))}
                </Box>
              )}
            </Box>
          </Box>
        );
      }
    }
    
    // Wrap raw HTML documents in a code fence so they get syntax highlighting + Preview button
    if (isHtmlDocument(processedContent)) {
      return <MessageContent content={'```html\n' + processedContent + '\n```'} />;
    }

    // Default content rendering
    return <MessageContent content={processedContent} />;
  };


  return (
    <Fade in={true} timeout={300}>
      <ListItem
        alignItems="flex-start"
        sx={{
          flexDirection: 'row', // Always left-aligned
          gap: 1,
          py: 1.5,
          width: '100%',
          maxWidth: '100%',
          overflow: 'hidden',
          minWidth: 0, // Critical for flex containers
          '& > *': {
            minWidth: 0, // Ensure all children respect container width
          }
        }}
      >
        <ListItemAvatar sx={{ minWidth: 'auto' }}>
          <Avatar
            sx={{
              bgcolor: message.type === 'user' 
                ? 'primary.main' 
                : message.type === 'execution' || message.type === 'trace'
                ? 'warning.main'
                : message.type === 'result'
                ? 'success.main'
                : 'secondary.main',
              width: 32,
              height: 32,
            }}
          >
            {message.type === 'user' ? (
              <PersonIcon sx={{ fontSize: 20 }} />
            ) : message.type === 'execution' || message.type === 'trace' ? (
              <SmartToyIcon sx={{ fontSize: 20 }} />
            ) : message.type === 'result' ? (
              <SmartToyIcon sx={{ fontSize: 20 }} />
            ) : (
              <SmartToyIcon sx={{ fontSize: 20 }} />
            )}
          </Avatar>
        </ListItemAvatar>
        <ListItemText
          sx={{
            flex: 1, // Take all available space
            maxWidth: 'calc(100% - 40px)', // Full width minus avatar
            mx: 0,
            overflow: 'hidden',
          }}
          primary={
            <Stack direction="column" spacing={1}>
              {message.intent && message.type === 'assistant' && (
                <Stack direction="row" spacing={1} alignItems="center">
                  <Chip
                    icon={getIntentIcon(message.intent)}
                    label={message.intent.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase())}
                    size="small"
                    color={getIntentColor(message.intent)}
                    variant="outlined"
                  />
                  {message.confidence !== undefined && (
                    <Typography variant="caption" color="text.secondary">
                      {Math.round(message.confidence * 100)}% confidence
                    </Typography>
                  )}
                </Stack>
              )}
              {(message.type === 'execution' || message.type === 'trace') && (
                <Stack direction="row" spacing={1} alignItems="center">
                  <Typography variant="caption" color="text.secondary">
                    {message.type === 'execution' ? 'Execution' : 'Trace'} Output
                  </Typography>
                  {message.eventSource && (
                    <Chip
                      label={message.eventSource}
                      size="small"
                      variant="outlined"
                      color="primary"
                    />
                  )}
                  {message.eventContext && (
                    <Chip
                      label={message.eventContext}
                      size="small"
                      variant="outlined"
                      color="secondary"
                    />
                  )}
                  {message.jobId && onOpenLogs && (
                    <Tooltip title="View execution logs">
                      <IconButton
                        size="small"
                        onClick={() => message.jobId && onOpenLogs(message.jobId)}
                        sx={{ ml: 1 }}
                      >
                        <OpenInNewIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  )}
                </Stack>
              )}
              <Box
                sx={{
                  color: message.type === 'user'
                    ? 'primary.main'
                    : message.type === 'result'
                    ? 'text.primary' // Final results in black
                    : 'text.primary',
                  wordBreak: 'break-word',
                  whiteSpace: isMarkdown(stripAnsiEscapes(message.content)) ? 'normal' : 'pre-wrap',
                  maxWidth: '100%',
                  overflow: 'visible',
                  fontWeight: message.type === 'result' ? 500 : 'normal', // Make result text slightly bolder
                }}
              >
                {renderMessageContent()}
              </Box>
              <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
                {message.timestamp.toLocaleTimeString()}
              </Typography>
            </Stack>
          }
        />
      </ListItem>
    </Fade>
  );
};