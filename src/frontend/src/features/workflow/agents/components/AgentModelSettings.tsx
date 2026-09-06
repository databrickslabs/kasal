import React, { useState } from 'react';
import { Box, Button, Grid, Menu, MenuItem, Typography } from '@mui/material';
import { ExpandMore } from '@mui/icons-material';
import type { Agent } from '../../../../types/workflow/agent';
import type { Models } from '../../../../types/config/models';
import { DEFAULT_EFFORT, effortLabel } from '../../../../types/workflow/effort';
import EffortPicker from '../../../../shared/components/EffortPicker';
import ModelOverrideFields from './ModelOverrideFields';

/** Saved agent settings, shared by UI runs and API callers. */
export default function AgentModelSettings({ agent, model, acceptsTemperature, onPatch }: {
  agent: Partial<Agent>; model?: Models[string]; acceptsTemperature: boolean;
  onPatch: (patch: Partial<Agent>) => void;
}) {
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  const effort = agent.execution_effort;
  return <>
    <Grid item xs={12}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 2 }}>
        <Box>
          <Typography sx={{ fontSize: 14, fontWeight: 500 }}>Effort</Typography>
          <Typography sx={{ fontSize: 12, color: 'text.secondary', mt: 0.5 }}>
            Saved with this agent. Sets reasoning and execution limits for every launch.
          </Typography>
        </Box>
        <Button endIcon={<ExpandMore />} onClick={e => setAnchor(e.currentTarget)}
          sx={{ color: 'text.primary', textTransform: 'none', flexShrink: 0 }}>
          {effort ? effortLabel(effort) : 'Use model settings'}
        </Button>
      </Box>
      {effort && <Typography sx={{ fontSize: 12, color: 'text.secondary', mt: 1 }}>
        Individual overrides below take precedence. Fine-tune rounds and time in Execution Settings.
      </Typography>}
      <Menu anchorEl={anchor} open={Boolean(anchor)} onClose={() => setAnchor(null)}
        slotProps={{ paper: { sx: { width: 352, maxHeight: 'min(600px, 80vh)', borderRadius: 3, p: 0.5 } } }}>
        <MenuItem selected={!effort} onClick={() => { onPatch({ execution_effort: null }); setAnchor(null); }}>
          Use model settings
        </MenuItem>
        <EffortPicker scope="agent" value={effort ?? DEFAULT_EFFORT} inherited={!effort}
          models={[model ?? {}]} onChange={(settings, profile) => {
            if (!profile) return;
            onPatch({ execution_effort: settings, max_iter: profile.max_iter,
              max_execution_time: profile.max_execution_time, max_tokens: null,
              thinking_effort: undefined, reasoning_effort: null, thinking_budget_tokens: undefined });
            setAnchor(null);
          }} />
      </Menu>
    </Grid>
    <ModelOverrideFields acceptsTemperature={acceptsTemperature}
      temperature={agent.temperature} maxTokens={agent.max_tokens}
      modelMaxOutputTokens={model?.max_output_tokens} thinkingMode={model?.thinking_mode}
      allowedEfforts={model?.allowed_efforts} returnsThinkingText={model?.returns_thinking_text}
      thinkingBudgetTokens={agent.thinking_budget_tokens} thinkingEffort={agent.thinking_effort}
      onChange={(field, value) => onPatch({ [field]: value })} />
  </>;
}
