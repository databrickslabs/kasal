import React, { useEffect, useState } from 'react';
import { Box, Button, CircularProgress, TextField, Typography } from '@mui/material';
import { Check } from '@mui/icons-material';
import { apiClient } from '../api/client';
import { EFFORT_LABELS, EffortProfile, EffortSettings, EffortTier } from '../../types/workflow/effort';

export interface EffortModel {
  name?: string;
  allowed_efforts?: string[];
  thinking_mode?: 'manual' | 'adaptive' | null;
}
const HINTS: Record<EffortTier, string> = {
  none: 'No native reasoning · shortest runs', minimal: 'Small, tightly scoped tasks',
  low: 'Quick, focused work', medium: 'A balance of depth and speed',
  high: 'Thorough research and deliverables', xhigh: 'Longer, complex work',
  max: 'The most demanding tasks',
};

export function effortChoices(models: EffortModel[]): EffortTier[] {
  const supported = new Set(models.flatMap(m => m.allowed_efforts ?? []));
  // Manual thinking and models without native depth still have runtime tiers.
  if (!supported.size || models.some(m => m.thinking_mode === 'manual')) {
    ['low', 'medium', 'high'].forEach(t => supported.add(t));
  }
  return (Object.keys(EFFORT_LABELS) as EffortTier[]).filter(t => supported.has(t));
}

/** Shared by both input menus. Limits come from the same backend as execution. */
export default function EffortPicker({ value, onChange, models, onPicked, inherited = false, scope = 'run' }: {
  value: EffortSettings; onChange: (value: EffortSettings, profile?: EffortProfile) => void;
  models: EffortModel[]; onPicked?: () => void;
  inherited?: boolean; scope?: 'run' | 'agent';
}) {
  const [profiles, setProfiles] = useState<Partial<Record<EffortTier, EffortProfile>>>();
  const [error, setError] = useState(false);
  const [retry, setRetry] = useState(0);
  const [advanced, setAdvanced] = useState(false);
  const [draft, setDraft] = useState({ minutes: '', rounds: '' });
  useEffect(() => {
    let live = true;
    setError(false);
    apiClient.get<{ effort_profiles: Partial<Record<EffortTier, EffortProfile>> }>('/models/enabled')
      .then(({ data }) => {
        if (!data.effort_profiles) throw new Error('Missing effort profiles');
        if (live) setProfiles(data.effort_profiles);
      }).catch(() => { if (live) setError(true); });
    return () => { live = false; };
  }, [retry]);
  const options = effortChoices(models);
  const profile = profiles?.[value.tier];
  const native = models.some(m => (m.allowed_efforts?.length ?? 0) > 0 || m.thinking_mode === 'manual');
  const minutes = Number(draft.minutes), rounds = Number(draft.rounds);
  const valid = Number.isInteger(minutes) && minutes >= 1 && minutes <= 240
    && Number.isInteger(rounds) && rounds >= 1 && rounds <= 1000;
  if (error) return <Box sx={{ p: 1.5 }}><Typography variant="body2">Couldn’t load effort limits.</Typography>
    <Button onClick={() => setRetry(r => r + 1)}>Retry</Button></Box>;
  if (!profiles) return <Box sx={{ p: 2, display: 'flex', gap: 1 }}><CircularProgress size={16} /><Typography variant="caption">Loading effort limits…</Typography></Box>;
  const rowStyle = { width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    textAlign: 'left', textTransform: 'none', color: 'text.primary', border: 0, px: '12px', py: '10px',
    borderRadius: '10px', gap: 1.5, cursor: 'pointer', fontFamily: 'inherit',
    '&:hover': { bgcolor: 'action.hover' }, '&:focus-visible': { outline: '2px solid', outlineColor: 'text.secondary', outlineOffset: -2 } } as const;
  return <Box onKeyDown={e => { if (e.target instanceof HTMLInputElement) e.stopPropagation(); }}>
    {!advanced ? <>
      <Box role="menu" aria-label="Effort levels" sx={{ display: 'grid', gap: '2px' }}>
        {options.map(tier => <Box component="button" type="button" key={tier}
          role="menuitemradio" aria-checked={!inherited && value.tier === tier} disabled={!profiles[tier]}
          className="!px-3 !py-2.5" onClick={() => { onChange({ tier }, profiles[tier]); onPicked?.(); }}
          sx={{ ...rowStyle, bgcolor: !inherited && value.tier === tier ? 'action.selected' : 'transparent' }}>
          <Box component="span" sx={{ display: 'grid', gap: '3px', minWidth: 0 }}>
            <Box component="span" sx={{ fontSize: '13px', fontWeight: 500, lineHeight: '20px' }}>{EFFORT_LABELS[tier]}</Box>
            <Box component="span" sx={{ fontSize: '12px', fontWeight: 400, lineHeight: '18px', color: 'text.secondary' }}>{HINTS[tier]}</Box>
          </Box>{!inherited && value.tier === tier && <Check sx={{ fontSize: 16, flexShrink: 0 }} />}
        </Box>)}
      </Box>
      {!inherited && <Box sx={{ px: '12px', pt: '14px', pb: '6px' }}>
        <Box sx={{ fontSize: '12px', lineHeight: '19px', color: 'text.secondary' }}>
          {EFFORT_LABELS[value.tier]} · up to {(scope === 'agent'
            ? value.max_execution_time ?? profile?.max_execution_time ?? 300
            : value.run_max_seconds ?? profile?.run_max_seconds ?? 600) / 60} min{scope === 'agent' ? ' per turn' : ''}
          {' · '}{value.max_iter ?? profile?.max_iter ?? 15} rounds per agent turn
        </Box>
        {!native && <Box sx={{ fontSize: '12px', lineHeight: '19px', color: 'text.secondary', mt: '6px' }}>
          This model has no native effort control. Only runtime limits change.
        </Box>}
        {!options.includes(value.tier) && <Box sx={{ fontSize: '12px', lineHeight: '19px', color: 'text.secondary', mt: '6px' }}>
          Native depth will use the nearest supported level.
        </Box>}
      </Box>}
      {scope === 'run' && <Box component="button" type="button" className="!px-3 !py-2.5" style={{ fontSize: 13, lineHeight: '20px' }} sx={{ ...rowStyle, bgcolor: 'transparent' }}
        onClick={() => {
          setDraft({ minutes: String((value.run_max_seconds ?? profile?.run_max_seconds ?? 600) / 60),
            rounds: String(value.max_iter ?? profile?.max_iter ?? 15) }); setAdvanced(true);
        }}>Adjust limits <span aria-hidden="true">›</span></Box>}
    </> : <Box sx={{ px: '12px', pt: '12px', pb: '8px', display: 'grid', gap: '18px' }}>
      <Box sx={{ fontSize: '13px', lineHeight: '20px', color: 'text.secondary' }}>
        Keep {EFFORT_LABELS[value.tier].toLowerCase()} reasoning and set your own runtime allowance.
      </Box>
      <TextField size="small" type="number" label="Whole run · minutes" value={draft.minutes}
        inputProps={{ min: 1, max: 240, step: 1 }} onChange={e => setDraft(d => ({ ...d, minutes: e.target.value }))} />
      <TextField size="small" type="number" label="Rounds per agent turn" value={draft.rounds}
        inputProps={{ min: 1, max: 1000, step: 1 }} onChange={e => setDraft(d => ({ ...d, rounds: e.target.value }))} />
      <Box sx={{ fontSize: '12px', lineHeight: '19px', color: 'text.secondary' }}>
        Time is shared across agents and retries. A round may call several tools. Runs finish when the work is done.
      </Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
        <Button size="small" onClick={() => setAdvanced(false)} sx={{ color: 'text.secondary', textTransform: 'none' }}>Back to levels</Button>
        <Button disabled={!valid} size="small" onClick={() => {
          onChange({ tier: value.tier, run_max_seconds: minutes * 60, max_execution_time: minutes * 60, max_iter: rounds }); onPicked?.();
        }} sx={{ color: 'text.primary', textTransform: 'none' }}>Apply custom limits</Button>
      </Box>
    </Box>}
  </Box>;
}
