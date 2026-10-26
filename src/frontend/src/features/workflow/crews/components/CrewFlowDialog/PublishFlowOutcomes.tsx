import React from 'react';
import { Alert, Box, Stack, TextField, Typography } from '@mui/material';

interface PublishFlowOutcomesProps {
  /** The crews in this flow, in graph order where known. */
  crews: string[];
  /** crew name -> what it delivers, as written here. */
  outcomes: Record<string, string>;
  onChange: (outcomes: Record<string, string>) => void;
  /** Crews nothing else listens to — the ones a turn usually asks for. */
  terminal?: string[];
}

/**
 * What each crew in this flow DELIVERS, written per flow.
 *
 * A conversational flow is asked for different things on different turns, and
 * it picks which crew to run by matching the turn against these lines. Without
 * them the choice falls back to the crews' task text, which is written to
 * instruct an agent — "Research and compile a comprehensive database of…" — and
 * says almost nothing about what comes out.
 *
 * Why this lives HERE, on the flow, rather than on the crew:
 *
 * - **A crew delivers something narrower as a step than it does alone.** The
 *   same gathering crew feeding a comparison is not the same offer as that crew
 *   published on its own, so one description cannot serve both honestly.
 * - **It must not require publishing the crews.** Reading these from each
 *   crew's publication would mean a flow could only describe itself if every
 *   step were separately published — filling the routing catalogue with steps
 *   nobody should call directly, and making "documented" mean "exposed".
 *
 * Optional throughout. A flow with none of these still runs; it just re-runs
 * everything each turn instead of the one crew the turn asked for.
 */
const PublishFlowOutcomes: React.FC<PublishFlowOutcomesProps> = ({
  crews,
  outcomes,
  onChange,
  terminal = [],
}) => {
  if (crews.length === 0) return null;

  const set = (crew: string, value: string) =>
    onChange({ ...outcomes, [crew]: value });

  const described = crews.filter((c) => (outcomes[c] || '').trim()).length;

  return (
    <Stack spacing={1.5}>
      <Box>
        <Typography variant="body2" sx={{ fontWeight: 600 }}>
          What each step delivers
        </Typography>
        <Typography variant="caption" color="text.secondary">
          Used to decide which step a follow-up question needs, so a later turn
          runs one crew instead of all of them. Describe what comes OUT, not how
          the work is done.
        </Typography>
      </Box>

      {described === 0 && (
        <Alert severity="info" variant="outlined">
          Without these, every turn re-runs the whole flow — the choice has
          nothing to match a question against.
        </Alert>
      )}

      {crews.map((crew) => (
        <TextField
          key={crew}
          size="small"
          fullWidth
          label={crew}
          placeholder={
            terminal.includes(crew)
              ? 'e.g. a ranked comparison table of the frameworks'
              : 'e.g. the raw material the later steps compare'
          }
          value={outcomes[crew] || ''}
          onChange={(e) => set(crew, e.target.value)}
          helperText={
            terminal.includes(crew)
              ? 'Nothing else in the flow uses this — a turn can ask for it directly'
              : undefined
          }
        />
      ))}
    </Stack>
  );
};

export default PublishFlowOutcomes;
