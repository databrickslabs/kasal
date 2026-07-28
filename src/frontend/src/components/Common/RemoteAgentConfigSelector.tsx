import React, { useEffect, useState } from 'react';
import {
  Alert,
  Autocomplete,
  Box,
  Chip,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { A2AAgent, A2AAgentService } from '../../api/tools/A2AAgentService';

export interface RemoteAgentToolConfig {
  /** Which remotes this agent may delegate to. Empty means every enabled one. */
  agent_names?: string[];
}

interface Props {
  value: RemoteAgentToolConfig;
  onChange: (config: RemoteAgentToolConfig) => void;
}

/**
 * Choose which remote agents the Remote Agent tool exposes.
 *
 * One tool is built PER remote, so its description can name that remote's
 * actual skills — which is what lets the calling model pick correctly. The cost
 * of that design is that leaving this empty in a workspace with twenty remotes
 * hands the agent twenty delegation tools, which is worse than none: a long
 * tool list degrades selection for every other tool too.
 *
 * So: empty is fine while there are a handful, and the component says so once
 * there are not.
 */
const CROWDED = 5;

const RemoteAgentConfigSelector: React.FC<Props> = ({ value, onChange }) => {
  const [agents, setAgents] = useState<A2AAgent[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    A2AAgentService.list()
      .then((rows) => {
        if (!cancelled) setAgents(rows.filter((a) => a.enabled));
      })
      .catch(() => {
        if (!cancelled) setError('Could not load remote agents.');
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const selected = value.agent_names ?? [];
  const crowded = agents.length > CROWDED && selected.length === 0;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
      <Autocomplete
        multiple
        options={agents.map((a) => a.name)}
        value={selected}
        onChange={(_, names) => onChange({ agent_names: names })}
        renderOption={(props, name) => {
          const agent = agents.find((a) => a.name === name);
          return (
            <li {...props} key={name}>
              <Box>
                <Typography variant="body2">{name}</Typography>
                <Typography variant="caption" color="text.secondary">
                  {agent?.skills.length
                    ? agent.skills.map((s) => s.name).join(', ')
                    : agent?.description || 'No skills advertised'}
                </Typography>
              </Box>
            </li>
          );
        }}
        renderTags={(names, getTagProps) =>
          names.map((name, index) => (
            <Tooltip
              key={name}
              title={
                agents.find((a) => a.name === name)?.skills.map((s) => s.name).join(', ') ||
                ''
              }
            >
              <Chip size="small" label={name} {...getTagProps({ index })} />
            </Tooltip>
          ))
        }
        renderInput={(params) => (
          <TextField
            {...params}
            label="Remote agents this agent may delegate to"
            placeholder={selected.length ? '' : 'All enabled remote agents'}
            error={Boolean(error)}
            helperText={
              error ??
              'One delegation tool is created per remote, named after it. Leave empty to expose all of them.'
            }
          />
        )}
      />

      {crowded && (
        <Alert severity="warning">
          {agents.length} remote agents are enabled here. Leaving this empty gives
          the agent {agents.length} delegation tools, which makes it worse at
          choosing any tool. Pick the one or two it actually needs.
        </Alert>
      )}
    </Box>
  );
};

export default RemoteAgentConfigSelector;
