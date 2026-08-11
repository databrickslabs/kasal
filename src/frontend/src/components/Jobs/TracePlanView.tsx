import React from 'react';
import { Box, Chip, LinearProgress, Stack, Typography } from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import RadioButtonUncheckedIcon from '@mui/icons-material/RadioButtonUnchecked';
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutline';
import CancelIcon from '@mui/icons-material/Cancel';

export interface PlanItem {
  id: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | string;
}

/**
 * Read a plan out of a trace event, whichever shape it arrived in.
 *
 * Two events carry a plan and they are NOT the same shape:
 *  - `plan_updated` — the engine's own event, clean JSON in
 *    `extra_data.plan_items`.
 *  - the `todo` tool_usage span — `extra_data.tool_args` is a Python **repr**
 *    string (single quotes, `True`/`None`), because that is how tool arguments
 *    are stringified on their way to the trace.
 *
 * The repr is parsed on a best-effort basis: it is display-only, so a failure
 * costs a nicer rendering, never correctness. Everything falls back to the raw
 * JSON view the dialog already has.
 */
export function extractPlanItems(output: unknown): PlanItem[] | null {
  if (typeof output !== 'object' || output === null) return null;
  const extra = (output as Record<string, unknown>).extra_data as
    | Record<string, unknown>
    | undefined;
  if (!extra) return null;

  // Preferred: the engine's structured payload.
  const structured = extra.plan_items;
  if (typeof structured === 'string' && structured.trim()) {
    const parsed = tryParse(structured);
    if (parsed) return parsed;
  }
  if (Array.isArray(structured)) return normalise(structured);

  // Fallback: the todo tool call's arguments, as a Python repr.
  const args = extra.tool_args;
  if (typeof args === 'string' && args.includes('todos')) {
    const parsed = tryParse(pythonReprToJson(args));
    if (parsed) return parsed;
    const wrapped = tryParseObject(pythonReprToJson(args));
    if (wrapped && Array.isArray(wrapped.todos)) return normalise(wrapped.todos);
  }
  return null;
}

/** Python repr → JSON. Display-only, and deliberately conservative. */
function pythonReprToJson(input: string): string {
  return input
    .replace(/\bNone\b/g, 'null')
    .replace(/\bTrue\b/g, 'true')
    .replace(/\bFalse\b/g, 'false')
    // Single-quoted strings → double-quoted, preserving any embedded doubles.
    .replace(/'((?:[^'\\]|\\.)*)'/g, (_m, body: string) => `"${body.replace(/"/g, '\\"')}"`);
}

function tryParse(text: string): PlanItem[] | null {
  try {
    const value = JSON.parse(text);
    if (Array.isArray(value)) return normalise(value);
    if (value && Array.isArray(value.todos)) return normalise(value.todos);
  } catch {
    /* display-only */
  }
  return null;
}

function tryParseObject(text: string): Record<string, unknown> | null {
  try {
    const value = JSON.parse(text);
    return typeof value === 'object' && value !== null ? value : null;
  } catch {
    return null;
  }
}

function normalise(rows: unknown[]): PlanItem[] | null {
  const items = rows
    .filter((row): row is Record<string, unknown> => typeof row === 'object' && row !== null)
    .map((row) => ({
      id: String(row.id ?? ''),
      content: String(row.content ?? ''),
      status: String(row.status ?? 'pending'),
    }))
    .filter((item) => item.content);
  return items.length ? items : null;
}

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
          Currently: {active.content}
        </Typography>
      )}

      <Stack spacing={0.75}>
        {items.map((item) => {
          const meta = STATUS_META[item.status] ?? STATUS_META.pending;
          return (
            <Stack key={`${item.id}-${item.content}`} direction="row" spacing={1} alignItems="flex-start">
              <Box sx={{ mt: '2px', flexShrink: 0 }}>{meta.icon}</Box>
              <Typography
                variant="body2"
                sx={{
                  color: meta.color,
                  textDecoration: item.status === 'cancelled' ? 'line-through' : 'none',
                  fontWeight: item.status === 'in_progress' ? 600 : 400,
                }}
              >
                {item.content}
              </Typography>
            </Stack>
          );
        })}
      </Stack>
    </Box>
  );
};

export default TracePlanView;
