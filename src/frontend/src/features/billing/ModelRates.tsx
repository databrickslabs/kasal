import { useState } from 'react';
import { Alert, Box, Button, MenuItem, TextField, Typography } from '@mui/material';
import { BillingService } from './BillingService';
import type { BillingSummary } from './types';

/** Pricing is explicit: unknown/self-hosted endpoints have no invented price. */
export default function ModelRates({ summary, onSaved }: { summary: BillingSummary; onSaved: () => void }) {
  const names = [...new Set([...summary.models.map(row => row.id), ...summary.rates.map(rate => rate.model)])].sort();
  const [model, setModel] = useState(names[0] || '');
  const initial = summary.rates.find(rate => rate.model === model);
  const [input, setInput] = useState(String(initial?.input_per_million ?? ''));
  const [output, setOutput] = useState(String(initial?.output_per_million ?? ''));
  const [cached, setCached] = useState(String(initial?.cached_input_per_million ?? ''));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);
  const choose = (name: string) => {
    setModel(name); setSaved(false); setError('');
    const rate = summary.rates.find(item => item.model === name);
    setInput(String(rate?.input_per_million ?? '')); setOutput(String(rate?.output_per_million ?? '')); setCached(String(rate?.cached_input_per_million ?? ''));
  };
  const valid = model.trim() && input.trim() && output.trim() && [input, output, ...(cached.trim() ? [cached] : [])].every(value => Number.isFinite(Number(value)) && Number(value) >= 0 && Number(value) <= 1000000);
  const save = async () => {
    if (!valid) return;
    setBusy(true); setError(''); setSaved(false);
    try {
      await BillingService.saveRate({ model: model.trim(), input_per_million: input, output_per_million: output, cached_input_per_million: cached.trim() || null });
      setSaved(true); onSaved();
    } catch { setError('The model rate could not be saved. Please try again.'); }
    finally { setBusy(false); }
  };
  return <Box component="form" onSubmit={event => { event.preventDefault(); void save(); }} sx={{ maxWidth: 760, py: 1 }}>
    <Typography sx={{ fontSize: 16, fontWeight: 600 }}>Model rates</Typography>
    <Typography sx={{ fontSize: 13, color: 'text.secondary', mt: 0.75, mb: 3 }}>Enter your provider’s USD price per million tokens. Updating a rate recalculates estimates for retained calls; it does not change provider charges.</Typography>
    {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
    <TextField select fullWidth size="small" label="Model" value={model} onChange={event => choose(event.target.value)} disabled={busy || !names.length} sx={{ mb: 3 }}>
      {names.map(name => <MenuItem key={name} value={name}>{name}</MenuItem>)}
    </TextField>
    {!names.length && <Typography sx={{ mb: 2, fontSize: 13, color: 'text.secondary' }}>Run a model first to configure its recorded model name.</Typography>}
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(3, 1fr)' }, gap: 2 }}>
      {([{ label: 'Input / 1M tokens', value: input, set: setInput }, { label: 'Output / 1M tokens', value: output, set: setOutput }, { label: 'Cached input / 1M', value: cached, set: setCached }]).map(field =>
        <TextField key={field.label} size="small" label={field.label} type="number" value={field.value} disabled={busy} onChange={event => { field.set(event.target.value); setSaved(false); }} inputProps={{ min: 0, max: 1000000, step: 'any' }} helperText={field.set === setCached ? 'Optional; needed for cache hits' : 'USD'} />)}
    </Box>
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mt: 2 }}>
      <Button type="submit" color="inherit" disabled={!valid || busy} sx={{ bgcolor: 'action.selected', borderRadius: 3, px: 2 }}>{busy ? 'Saving…' : 'Save rate'}</Button>
      {saved && <Typography role="status" sx={{ color: 'text.secondary', fontSize: 13 }}>Rate saved</Typography>}
    </Box>
  </Box>;
}

