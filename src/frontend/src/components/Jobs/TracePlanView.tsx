import type { PlanItem } from '../../features/executions/trace/lib/plan';
import React from 'react';
import { Box, Chip, LinearProgress, Stack, Typography } from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import RadioButtonUncheckedIcon from '@mui/icons-material/RadioButtonUnchecked';
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutline';
import CancelIcon from '@mui/icons-material/Cancel';

const STATUS_META: Record<string, { icon: JSX.Element; color: string; label: string }> = {
  completed: {
    icon: <CheckCircleIcon fontSize="small" color="success" />,
    color: 'success.main',
    label: 'done',
  },
  in_progress: {
    icon: <PlayCircleOutlineIcon fontSize="small" color="primary" />,
    color: 'primary.main',
    label: 'in progress',
  },
  cancelled: {
    icon: <CancelIcon fontSize="small" color="disabled" />,
    color: 'text.disabled',
    label: 'cancelled',
  },
  pending: {
    icon: <RadioButtonUncheckedIcon fontSize="small" color="disabled" />,
    color: 'text.primary',
    label: 'pending',
  },
};

/**
 * The agent's plan for the task, as a checklist.
 *
 * This is the inner plan — how the agent decided to do *this* task — as opposed
 * to the crew's task graph. Shown as a list because the question a reader has
 * is "how far along is it and what is it doing now", which a wall of JSON
 * cannot answer at a glance.
 */
const TracePlanView: React.FC<{ items: PlanItem[] }> = ({ items }) => {
  const completed = items.filter((i) => i.status === 'completed').length;
  const cancelled = items.filter((i) => i.status === 'cancelled').length;
  const active = items.find((i) => i.status === 'in_progress');
  const total = items.length;
  const percent = total ? Math.round((completed / total) * 100) : 0;

  return (
    <Box sx={{ mb: 2 }}>
      <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1, flexWrap: 'wrap', gap: 1 }}>
        <Typography variant="subtitle2">Task plan</Typography>
        <Chip size="small" label={`${completed}/${total} done`} color={completed === total ? 'success' : 'default'} variant="outlined" />
        {cancelled > 0 && <Chip size="small" label={`${cancelled} cancelled`} variant="outlined" />}
      </Stack>

      <LinearProgress
        variant="determinate"
        value={percent}
        sx={{ height: 6, borderRadius: 3, mb: 1.5 }}
      />

      {active && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1.5 }}>
          Currently: {active.label || active.content}
        </Typography>
      )}

      <Stack spacing={0.75}>
        {items.map((item) => {
          const meta = STATUS_META[item.status] ?? STATUS_META.pending;
          // When the model wrote a short label, that is the headline and the
          // full sentence goes underneath — a checklist is scanned, not read.
          const headline = item.label || item.content;
          const detail = item.label && item.label !== item.content ? item.content : '';
          return (
            <Stack key={`${item.id}-${item.content}`} direction="row" spacing={1} alignItems="flex-start">
              <Box sx={{ mt: '2px', flexShrink: 0 }}>{meta.icon}</Box>
              <Box sx={{ minWidth: 0 }}>
                <Typography
                  variant="body2"
                  sx={{
                    color: meta.color,
                    textDecoration: item.status === 'cancelled' ? 'line-through' : 'none',
                    fontWeight: item.status === 'in_progress' ? 600 : 400,
                  }}
                >
                  {headline}
                </Typography>
                {detail && (
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    sx={{
                      display: 'block',
                      textDecoration: item.status === 'cancelled' ? 'line-through' : 'none',
                    }}
                  >
                    {detail}
                  </Typography>
                )}
              </Box>
            </Stack>
          );
        })}
      </Stack>
    </Box>
  );
};

export default TracePlanView;
