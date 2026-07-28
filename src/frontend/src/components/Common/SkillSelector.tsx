import React, { useEffect, useMemo, useState } from 'react';
import {
  Autocomplete,
  Box,
  Chip,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { Skill, SkillService } from '../../api/tools/SkillService';

interface Props {
  value: string[];
  onChange: (names: string[]) => void;
  disabled?: boolean;
}

/**
 * Pick the skills an agent should have.
 *
 * Skills attach to AGENTS, not tasks: a skill is expertise, and in Kasal
 * expertise already lives on the agent alongside role, goal and backstory.
 *
 * Stored by NAME rather than by id, deliberately. A skill's name is its
 * identity in the format — it must match the folder it exports to — so a name
 * survives an export/import round trip and an id does not. It also means a
 * workspace skill that overrides a builtin resolves correctly without the agent
 * config having to be rewritten.
 */
const SkillSelector: React.FC<Props> = ({ value, onChange, disabled }) => {
  const [skills, setSkills] = useState<Skill[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    SkillService.list()
      .then((rows) => {
        if (!cancelled) setSkills(rows.filter((s) => s.enabled));
      })
      .catch(() => {
        if (!cancelled) setError('Could not load skills.');
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const byName = useMemo(
    () => new Map(skills.map((s) => [s.name, s])),
    [skills],
  );

  const options = useMemo(() => {
    // A skill the agent references but that no longer exists still shows, so a
    // deleted or disabled skill is visible as a stale chip rather than silently
    // vanishing from the form.
    const names = skills.map((s) => s.name);
    return [...names, ...value.filter((n) => !names.includes(n))];
  }, [skills, value]);

  return (
    <Autocomplete
      multiple
      disabled={disabled}
      options={options}
      value={value}
      onChange={(_, next) => onChange(next)}
      renderOption={(props, name) => {
        const skill = byName.get(name);
        return (
          <li {...props} key={name}>
            <Box>
              <Typography variant="body2">{name}</Typography>
              <Typography variant="caption" color="text.secondary">
                {skill?.description ?? 'No longer available'}
              </Typography>
            </Box>
          </li>
        );
      }}
      renderTags={(names, getTagProps) =>
        names.map((name, index) => {
          const skill = byName.get(name);
          return (
            <Tooltip key={name} title={skill?.description ?? 'No longer available'}>
              <Chip
                size="small"
                label={name}
                color={skill ? 'default' : 'warning'}
                {...getTagProps({ index })}
              />
            </Tooltip>
          );
        })
      }
      renderInput={(params) => (
        <TextField
          {...params}
          label="Skills"
          placeholder={value.length ? '' : 'None'}
          error={Boolean(error)}
          helperText={
            error ??
            'Procedures this agent can follow. Only their names and descriptions sit in the prompt — the instructions load when the agent decides one applies.'
          }
        />
      )}
    />
  );
};

export default SkillSelector;
