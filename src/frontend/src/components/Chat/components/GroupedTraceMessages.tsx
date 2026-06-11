import React, { useState } from 'react';
import {
  Box,
  Typography,
  ListItem,
  ButtonBase,
  IconButton,
  Tooltip,
  Fade,
} from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import CheckIcon from '@mui/icons-material/Check';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import { ChatMessage } from '../types';
import { stripAnsiEscapes } from '../utils/textProcessing';

interface GroupedTraceMessagesProps {
  messages: ChatMessage[];
  /** True while the run that produced these traces is still executing. */
  running?: boolean;
  onOpenLogs?: (jobId: string) => void;
}

const SUMMARY_MAX_CHARS = 140;

/** "agent_execution" → "Agent Execution" */
const humanizeEventType = (value?: string): string =>
  value ? value.replace(/[_-]+/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()) : '';

/** Derive the display pieces for one trace step: event name (agent/task/tool),
 *  the task context, a one-line summary, and the full output (only when it
 *  adds something beyond the summary). Shared by the timeline steps and the
 *  live header line. */
function describeTrace(message: ChatMessage): {
  name: string;
  meta: string;
  summary: string;
  context: string;
} {
  const name = humanizeEventType(message.eventType) || message.eventSource || 'Step';
  const meta = message.eventContext || '';
  const content = stripAnsiEscapes(message.content).trim();
  const firstLine = content.split('\n').find((l) => l.trim() !== '') || '';
  const summary =
    firstLine.length > SUMMARY_MAX_CHARS
      ? firstLine.substring(0, SUMMARY_MAX_CHARS) + '…'
      : firstLine;
  const context = content !== summary ? content : '';
  return { name, meta, summary, context };
}

/** One step on the run-activity timeline: a dot on the left rail, the event
 *  name in bold, its timestamp, a short summary line, and — behind a per-step
 *  toggle — the full trace output (kept hidden so the timeline stays
 *  scannable, matching the Chat-mode run-activity container). */
const TimelineStep: React.FC<{ message: ChatMessage; last: boolean }> = ({ message, last }) => {
  const [open, setOpen] = useState(false);
  const { name, meta, summary, context } = describeTrace(message);

  return (
    <Box component="li" sx={{ position: 'relative', pl: 2.5, pb: last ? 0 : 1.5, listStyle: 'none' }}>
      <Box
        aria-hidden="true"
        sx={{
          position: 'absolute',
          left: 0,
          top: 5,
          width: 8,
          height: 8,
          borderRadius: '50%',
          bgcolor: 'primary.main',
        }}
      />
      {!last && (
        <Box
          aria-hidden="true"
          sx={{
            position: 'absolute',
            left: '3.5px',
            top: 14,
            bottom: 0,
            width: '1px',
            bgcolor: 'divider',
          }}
        />
      )}
      <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 1, minWidth: 0 }}>
        <Typography variant="caption" sx={{ fontWeight: 600, color: 'text.primary' }}>
          {name}
        </Typography>
        {meta && (
          <Typography
            variant="caption"
            sx={{
              color: 'text.secondary',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              minWidth: 0,
            }}
          >
            {meta}
          </Typography>
        )}
        <Typography
          variant="caption"
          sx={{ fontFamily: 'monospace', fontSize: '0.625rem', color: 'text.disabled', flexShrink: 0 }}
        >
          {message.timestamp.toLocaleTimeString()}
        </Typography>
      </Box>
      {summary && (
        <Typography
          variant="caption"
          component="div"
          sx={{ mt: 0.25, color: 'text.secondary', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}
        >
          {summary}
        </Typography>
      )}
      {context && (
        <Box sx={{ mt: 0.5 }}>
          <ButtonBase
            onClick={() => setOpen((v) => !v)}
            aria-label={`${open ? 'Hide' : 'Show'} context for ${name}`}
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 0.25,
              borderRadius: 0.5,
              color: 'text.disabled',
              fontSize: '0.625rem',
              fontWeight: 500,
              '&:hover': { color: 'text.secondary' },
            }}
          >
            <ChevronRightIcon
              sx={{
                fontSize: 12,
                transition: 'transform 0.15s',
                transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
              }}
            />
            {open ? 'Hide context' : 'Show context'}
          </ButtonBase>
          {open && (
            <Box
              sx={{
                mt: 0.5,
                p: 1,
                maxHeight: 160,
                overflowY: 'auto',
                borderRadius: 1,
                border: 1,
                borderColor: 'divider',
                bgcolor: 'action.hover',
                color: 'text.secondary',
                fontSize: '0.75rem',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
              }}
            >
              {context}
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
};

/**
 * The run-activity container for the Agent Builder chat — visually identical
 * to Chat mode's RunProgress: a status row (pulsing dot + "Working…" while the
 * run executes, a check + "Run activity" once done) with a chevron that
 * expands into the timeline of trace steps. The full trace output lives
 * behind per-step "Show context" toggles instead of being dumped into the
 * conversation, so the chat stays readable while the crew runs.
 */
export const GroupedTraceMessages: React.FC<GroupedTraceMessagesProps> = ({
  messages,
  running = false,
  onOpenLogs,
}) => {
  const [open, setOpen] = useState(false);
  const hasTimeline = messages.length > 0;

  // Nothing to show: no traces yet and the run is not live.
  if (!hasTimeline && !running) return null;

  const jobId = messages[0]?.jobId;
  const label = running ? 'Working…' : 'Run activity';
  // While the run is live, the header shows the LATEST step as a one-liner
  // (agent/task/tool name + first line of its output) so the box visibly
  // progresses; "Working…" only fills the gap before the first trace arrives.
  const live = running && hasTimeline ? describeTrace(messages[messages.length - 1]) : null;

  return (
    <Fade in={true} timeout={300}>
      <ListItem sx={{ py: 1, px: 1, width: '100%', maxWidth: '100%', minWidth: 0 }}>
        <Box
          sx={{
            width: '100%',
            minWidth: 0,
            borderRadius: 3,
            border: 1,
            borderColor: 'divider',
            bgcolor: 'background.paper',
            overflow: 'hidden',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 1.5, py: 1 }}>
            {running ? (
              <Box sx={{ position: 'relative', display: 'flex', width: 8, height: 8, flexShrink: 0 }} aria-hidden="true">
                <Box
                  sx={{
                    position: 'absolute',
                    width: '100%',
                    height: '100%',
                    borderRadius: '50%',
                    bgcolor: 'primary.main',
                    opacity: 0.6,
                    animation: 'kasalRunPing 1.2s cubic-bezier(0, 0, 0.2, 1) infinite',
                    '@keyframes kasalRunPing': {
                      '75%, 100%': { transform: 'scale(2.5)', opacity: 0 },
                    },
                  }}
                />
                <Box sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: 'primary.main' }} />
              </Box>
            ) : (
              <CheckIcon sx={{ fontSize: 14, color: 'primary.main', flexShrink: 0 }} />
            )}
            <ButtonBase
              onClick={() => setOpen((v) => !v)}
              disabled={!hasTimeline}
              aria-label={hasTimeline ? (open ? 'Collapse run activity' : 'Expand run activity') : undefined}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 0.75,
                flex: 1,
                minWidth: 0,
                justifyContent: 'flex-start',
                cursor: hasTimeline ? 'pointer' : 'default',
              }}
            >
              <Typography
                variant="caption"
                noWrap
                title={live && live.summary ? `${live.name} — ${live.summary}` : undefined}
                sx={{
                  fontWeight: 500,
                  color: 'text.secondary',
                  minWidth: 0,
                  ...(running && {
                    animation: 'kasalRunPulse 2s ease-in-out infinite',
                    '@keyframes kasalRunPulse': {
                      '0%, 100%': { opacity: 1 },
                      '50%': { opacity: 0.5 },
                    },
                  }),
                }}
              >
                {live ? (
                  <>
                    <Box component="span" sx={{ fontWeight: 600, color: 'text.primary' }}>
                      {live.name}
                    </Box>
                    {live.summary ? ` — ${live.summary}` : ''}
                  </>
                ) : (
                  label
                )}
              </Typography>
              {hasTimeline && (
                <ChevronRightIcon
                  sx={{
                    fontSize: 16,
                    color: 'text.disabled',
                    transition: 'transform 0.15s',
                    transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
                  }}
                />
              )}
            </ButtonBase>
            {jobId && onOpenLogs && (
              <Tooltip title="View execution logs">
                <IconButton
                  size="small"
                  aria-label="View execution logs"
                  onClick={() => onOpenLogs(jobId)}
                  sx={{ ml: 'auto', flexShrink: 0 }}
                >
                  <OpenInNewIcon sx={{ fontSize: 16 }} />
                </IconButton>
              </Tooltip>
            )}
          </Box>
          {open && hasTimeline && (
            <Box
              component="ol"
              sx={{ m: 0, px: 2, py: 1.5, borderTop: 1, borderColor: 'divider' }}
            >
              {messages.map((message, i) => (
                <TimelineStep key={message.id} message={message} last={i === messages.length - 1} />
              ))}
            </Box>
          )}
        </Box>
      </ListItem>
    </Fade>
  );
};
