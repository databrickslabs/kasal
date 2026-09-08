import { useEffect, useState } from 'react';
import { Alert, Box, Button, CircularProgress, FormControl, IconButton, InputLabel, MenuItem, Select, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Typography } from '@mui/material';
import { RefreshCw } from 'lucide-react';
import { BillingService } from './BillingService';
import ModelRates from './ModelRates';
import { costLabel, type BillingSummary } from './types';

type Period = 'month' | '7' | '30' | '90';
type View = 'models' | 'runs' | 'days' | 'rates';
const modes: Record<string, string> = { agent: 'Chat', crew: 'Agent Builder', flow: 'Flow Builder' };

export default function BillingActivity({ executionIds }: { executionIds?: string[] }) {
  const [period, setPeriod] = useState<Period>('month');
  const [view, setView] = useState<View>('models');
  const [summary, setSummary] = useState<BillingSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  const [page, setPage] = useState(0);
  const idsKey = JSON.stringify(executionIds);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError(''); setSummary(null); setPage(0);
    const end = new Date();
    const start = period === 'month' ? new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), 1)) : new Date(end.getTime() - Number(period) * 86400000);
    const ids: string[] | undefined = idsKey ? JSON.parse(idsKey) : undefined;
    BillingService.summary(start.toISOString(), end.toISOString(), ids, controller.signal)
      .then(data => { if (!controller.signal.aborted) setSummary(data); })
      .catch(() => { if (!controller.signal.aborted) setError('Billing could not be loaded. Please try again.'); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [period, idsKey, revision]);
  const refresh = () => setRevision(value => value + 1);
  const totals = summary?.totals;
  const rows = summary && view !== 'rates' ? summary[view] : [];
  const visibleRows = rows.slice(page * 20, (page + 1) * 20);
  return <Box sx={{ p: { xs: 2, sm: 3 }, pt: 2, flex: 1, minHeight: 0, overflow: 'auto', '& .MuiOutlinedInput-root': { borderRadius: 3 }, '& .MuiButton-root': { textTransform: 'none' } }}>
    <Box sx={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 1.5, mb: 2.5 }}>
      <Box sx={{ flex: 1, minWidth: 180 }}><Typography component="h3" sx={{ fontWeight: 600, fontSize: 18 }}>Usage & cost</Typography><Typography sx={{ color: 'text.secondary', fontSize: 12, mt: 0.5 }}>Model usage for {executionIds ? 'this session' : 'this teamspace'} · USD estimates</Typography></Box>
      <FormControl size="small" sx={{ minWidth: 155 }}><InputLabel id="billing-period">Period</InputLabel><Select labelId="billing-period" label="Period" value={period} onChange={event => setPeriod(event.target.value as Period)} sx={{ fontSize: 13 }}>
        <MenuItem value="month">This month (UTC)</MenuItem><MenuItem value="7">Last 7 days</MenuItem><MenuItem value="30">Last 30 days</MenuItem><MenuItem value="90">Last 90 days</MenuItem>
      </Select></FormControl>
      <IconButton aria-label="Refresh billing" disabled={loading} onClick={refresh}>{loading ? <CircularProgress size={18} color="inherit" /> : <RefreshCw size={18} />}</IconButton>
    </Box>
    {error && <Alert severity="error" action={<Button color="inherit" onClick={refresh}>Retry</Button>}>{error}</Alert>}
    {summary && totals && <>
      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'repeat(2, 1fr)', sm: 'repeat(4, 1fr)' }, gap: 3, py: 1, mb: 2.5 }}>
        {[
          { label: 'Estimated cost', value: costLabel(totals.estimated_cost_usd), detail: `${totals.priced_calls.toLocaleString()} of ${totals.calls.toLocaleString()} calls priced` },
          { label: 'Tokens', value: totals.total_tokens.toLocaleString(), detail: `${totals.cached_input_tokens.toLocaleString()} cached input` },
          { label: 'Model calls', value: totals.calls.toLocaleString(), detail: `${totals.measured_calls.toLocaleString()} with reported usage` },
          { label: 'Runs', value: summary.runs.length.toLocaleString(), detail: 'With recorded model calls' },
        ].map(item => <Box key={item.label}><Typography sx={{ fontSize: 12, color: 'text.secondary' }}>{item.label}</Typography><Typography sx={{ fontSize: 26, fontWeight: 600, letterSpacing: '-0.03em', mt: 0.75 }}>{item.value}</Typography><Typography sx={{ fontSize: 11, color: 'text.secondary', mt: 0.5 }}>{item.detail}</Typography></Box>)}
      </Box>
      <Typography sx={{ fontSize: 12, color: 'text.secondary', lineHeight: 1.7, mb: 2 }}>{summary.basis}</Typography>
      {totals.calls > totals.priced_calls && <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap', mb: 2 }}>
        <Typography sx={{ fontSize: 12 }}>{(totals.calls - totals.priced_calls).toLocaleString()} calls are unpriced. Missing usage or rates are excluded from the estimate.</Typography>
        {summary.can_manage_rates && <Button color="inherit" size="small" onClick={() => setView('rates')} sx={{ borderRadius: 2.5 }}>Set model rates</Button>}
      </Box>}
      <Box component="nav" aria-label="Billing breakdown" sx={{ display: 'flex', gap: 0.5, mb: 2 }}>
        {([{ id: 'models', label: 'By model' }, { id: 'runs', label: 'By run' }, { id: 'days', label: 'By day' }, ...(summary.can_manage_rates ? [{ id: 'rates', label: 'Model rates' }] : [])] as { id: View; label: string }[]).map(item =>
          <Button key={item.id} color="inherit" size="small" aria-pressed={view === item.id} onClick={() => { setView(item.id); setPage(0); }} sx={{ fontSize: 12, borderRadius: 2.5, px: 1.5, bgcolor: view === item.id ? 'action.selected' : 'transparent' }}>{item.label}</Button>)}
      </Box>
      {view === 'rates' && summary.can_manage_rates ? <ModelRates summary={summary} onSaved={refresh} /> : !rows.length ? <Box sx={{ textAlign: 'center', py: 6 }}>
        <Typography sx={{ fontSize: 15, fontWeight: 500 }}>No recorded usage in this period</Typography><Typography sx={{ fontSize: 13, color: 'text.secondary', mt: 1 }}>Try a different period{executionIds ? ' or switch to all teamspace usage' : ''}.</Typography>
      </Box> : <>
        <TableContainer><Table size="small" aria-label="Billing usage" sx={{ '& th': { fontSize: 11, color: 'text.secondary' }, '& td': { fontSize: 12, py: 1.75 }, '& td, & th': { borderColor: 'divider', px: 1.5 } }}>
          <TableHead><TableRow><TableCell>{view === 'models' ? 'Model' : view === 'runs' ? 'Run' : 'Day (UTC)'}</TableCell><TableCell align="right">Calls</TableCell><TableCell align="right">Input tokens</TableCell><TableCell align="right">Output tokens</TableCell><TableCell align="right">Estimated cost</TableCell></TableRow></TableHead>
          <TableBody>{visibleRows.map(row => <TableRow key={row.id}>
            <TableCell sx={{ maxWidth: 320, overflowWrap: 'anywhere' }}><Typography sx={{ fontSize: 13, fontWeight: 500 }}>{row.label}</Typography>{row.execution_type && <Typography sx={{ fontSize: 11, color: 'text.secondary', mt: 0.5 }}>{modes[row.execution_type] || row.execution_type}</Typography>}</TableCell>
            <TableCell align="right">{row.calls.toLocaleString()}</TableCell><TableCell align="right">{row.input_tokens.toLocaleString()}</TableCell><TableCell align="right">{row.output_tokens.toLocaleString()}</TableCell>
            <TableCell align="right"><Typography sx={{ fontSize: 13, fontWeight: 500 }}>{costLabel(row.estimated_cost_usd)}</Typography>{row.priced_calls < row.calls && <Typography sx={{ fontSize: 11, color: 'text.secondary' }}>{row.priced_calls}/{row.calls} priced</Typography>}</TableCell>
          </TableRow>)}</TableBody>
        </Table></TableContainer>
        {rows.length > 20 && <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 1, mt: 1 }}><Typography sx={{ fontSize: 12, color: 'text.secondary' }}>{page * 20 + 1}–{Math.min((page + 1) * 20, rows.length)} of {rows.length}</Typography><Button color="inherit" disabled={page === 0} onClick={() => setPage(value => value - 1)}>Previous</Button><Button color="inherit" disabled={(page + 1) * 20 >= rows.length} onClick={() => setPage(value => value + 1)}>Next</Button></Box>}
      </>}
    </>}
  </Box>;
}
