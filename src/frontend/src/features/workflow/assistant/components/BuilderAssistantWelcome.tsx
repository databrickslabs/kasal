import React from 'react';
import { Box, Button, Typography } from '@mui/material';
import { ArrowUpRight, FileText, Users, UserRound } from 'lucide-react';

interface Props {
  dark: boolean;
  disabled?: boolean;
  hasNodes?: boolean;
  onPrompt?: (prompt: string) => void;
}

export function BuilderAssistantWelcome({ dark, hasNodes }: Props) {
  return (
    <Box sx={{ px: 3.5, pt: 3, pb: 2.5 }}>
      <Typography sx={{ fontSize: '1.5rem', lineHeight: 1.3, letterSpacing: '-0.035em', fontWeight: 500, color: dark ? '#E8ECEF' : '#1B1F23', maxWidth: 300, mb: 1.25 }}>
        What would you like to build?
      </Typography>
      <Typography sx={{ fontSize: '0.875rem', lineHeight: 1.7, color: dark ? '#A0AAB4' : '#66717D', maxWidth: 330 }}>
        {hasNodes ? 'Refine your crew, add a new capability, or ask what to do next.' : 'Describe the work. I’ll help you shape the agents and tasks.'}
      </Typography>
    </Box>
  );
}

export function BuilderAssistantStarters({ dark, disabled, onPrompt }: Props) {
  const ink = dark ? '#E8ECEF' : '#1B1F23';
  const muted = dark ? '#A0AAB4' : '#66717D';
  return (
    <Box sx={{ px: 3, pt: 2.25, pb: 3 }}>
      <Typography sx={{ fontSize: '0.6875rem', color: muted, mb: 1, px: 0.75 }}>A place to start</Typography>
      {[
        { label: 'Create an agent', prompt: 'Create an agent that can analyze financial data', Icon: UserRound },
        { label: 'Summarize documents', prompt: 'I need a task to summarize documents', Icon: FileText },
        { label: 'Build a research team', prompt: 'Build a research team with a researcher and writer', Icon: Users },
      ].map(({ label, prompt, Icon }) => (
        <Button
          key={label}
          disabled={disabled}
          onClick={() => onPrompt?.(prompt)}
          sx={{ width: '100%', justifyContent: 'flex-start', gap: 1.25, py: 1, px: 1, borderRadius: '12px', fontSize: '0.8125rem', fontWeight: 400, textTransform: 'none', color: muted, '&:hover': { color: ink, bgcolor: dark ? 'rgba(255,255,255,0.05)' : 'rgba(27,31,35,0.035)' }, '&.Mui-focusVisible': { outline: `2px solid ${muted}`, outlineOffset: 1 } }}
        >
          <Icon size={16} strokeWidth={1.5} /><span>{label}</span><ArrowUpRight size={14} strokeWidth={1.5} style={{ marginLeft: 'auto', opacity: 0.6 }} />
        </Button>
      ))}
      <Box component="details" sx={{ color: muted, mt: 1.5, px: 0.75, fontSize: '0.75rem' }}>
        <Box component="summary" sx={{ cursor: 'pointer', py: 1, '&:focus-visible': { outline: `2px solid ${muted}`, outlineOffset: 2 } }}>Commands and help</Box>
        <Box sx={{ display: 'grid', gridTemplateColumns: 'auto 1fr', gap: 1, py: 1, lineHeight: 1.6 }}>
          {[
            ['/list crews', 'Browse saved crews'], ['/load crew <name>', 'Open a saved crew'], ['/run crew', 'Run the current crew'],
            ['/list flows', 'Browse saved flows'], ['/run flow', 'Run the current flow'], ['/help', 'All commands'],
          ].map(([command, text]) => <React.Fragment key={command}><Box component="code" sx={{ color: ink }}>{command}</Box><span>{text}</span></React.Fragment>)}
        </Box>
      </Box>
    </Box>
  );
}
