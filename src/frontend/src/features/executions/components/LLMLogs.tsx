import { useState, useEffect, useCallback, useRef } from 'react';
import { Alert, Box, Button, CircularProgress, FormControl, IconButton, InputLabel, List, ListItemButton, MenuItem, Select, TablePagination, Typography } from '@mui/material';
import { Check, CircleAlert, RefreshCw } from 'lucide-react';
import type { LLMLog } from '../../../types/common';
import logService from '../../../api/execution/LogService';

const operations: Record<string, string> = {
  'generate-crew': 'Create crew', 'generate-crew-plan': 'Create crew plan', 'generate-flow': 'Create flow',
  'generate-agent': 'Create agent', 'generate-task': 'Create task', 'generate-connections': 'Connect tasks',
  'process-connections': 'Process connections', 'improve-prompt': 'Improve prompt', 'generate-templates': 'Create templates',
};
const operationName = (endpoint: string) => operations[endpoint] || endpoint.replace(/[-_]/g, ' ');
const duration = (ms: number) => ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;
type Detail = 'response' | 'request' | 'metadata';

/** Read-only model call inspector, shared by Activity and standalone log views. */
export default function LLMLogs({ embedded = false }: { embedded?: boolean }) {
  const [logs, setLogs] = useState<LLMLog[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [endpoint, setEndpoint] = useState('all');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<Detail>('response');
  const requestId = useRef(0);
  const fetchLogs = useCallback(async () => {
    const id = ++requestId.current;
    setLoading(true);
    setError('');
    try {
      const data = await logService.getLLMLogs({ page, per_page: rowsPerPage, endpoint: endpoint === 'all' ? undefined : endpoint });
      if (id !== requestId.current) return;
      setLogs(data);
      setSelectedId(previous => data.some(log => log.id === previous) ? previous : data[0]?.id || null);
    } catch (cause) {
      if (id !== requestId.current) return;
      console.error('Error fetching logs:', cause);
      setError('Model calls could not be loaded. Try refreshing.');
    } finally {
      if (id === requestId.current) setLoading(false);
    }
  }, [page, rowsPerPage, endpoint]);
  useEffect(() => {
    void fetchLogs();
    return () => { requestId.current += 1; };
  }, [fetchLogs]);
  const selected = logs.find(log => log.id === selectedId);
  const tokens = logs.reduce((total, log) => total + (log.tokens_used || 0), 0);
  const meanDuration = logs.length ? logs.reduce((total, log) => total + log.duration_ms, 0) / logs.length : 0;
  const endpoints = [...new Set([...Object.keys(operations), ...logs.map(log => log.endpoint), ...(endpoint === 'all' ? [] : [endpoint])])];
  const body = selected && (detail === 'request' ? selected.prompt : detail === 'response' ? selected.response
    : JSON.stringify({ endpoint: selected.endpoint, model: selected.model, status: selected.status,
      created_at: selected.created_at, duration_ms: selected.duration_ms, tokens_used: selected.tokens_used,
      ...(selected.extra_data ? { extra_data: selected.extra_data } : {}) }, null, 2));

  return <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, height: embedded ? '100%' : 'min(720px, 80dvh)', minHeight: 0, color: 'text.primary' }}>
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, flexWrap: 'wrap', pb: 2, flexShrink: 0 }}>
      <Box sx={{ flex: 1, minWidth: 160 }}>
        <Typography component="h3" sx={{ fontSize: 18, fontWeight: 600 }}>Model calls</Typography>
        <Typography sx={{ fontSize: 12, color: 'text.secondary', mt: 0.5 }}>Requests and responses across this teamspace.</Typography>
      </Box>
      <FormControl size="small" sx={{ minWidth: 170 }}>
        <InputLabel id="model-calls-operation">Operation</InputLabel>
        <Select labelId="model-calls-operation" label="Operation" value={endpoint} onChange={event => { setEndpoint(event.target.value); setPage(0); setLogs([]); }} sx={{ fontSize: 13, borderRadius: 3 }}>
          <MenuItem value="all">All operations</MenuItem>
          {endpoints.map(value => <MenuItem key={value} value={value}>{operationName(value)}</MenuItem>)}
        </Select>
      </FormControl>
      <IconButton aria-label="Refresh" disabled={loading} onClick={() => void fetchLogs()} sx={{ borderRadius: 3 }}>
        {loading ? <CircularProgress size={18} color="inherit" /> : <RefreshCw size={18} />}
      </IconButton>
    </Box>
    {error && <Alert severity="error" sx={{ mb: 2 }} action={<Button color="inherit" size="small" onClick={() => void fetchLogs()}>Retry</Button>}>{error}</Alert>}
    {!!logs.length && <Typography sx={{ fontSize: 12, color: 'text.secondary', pb: 1.5 }}>
      {logs.length} calls on this page · {tokens.toLocaleString()} reported tokens · {duration(meanDuration)} average
    </Typography>}
    {!loading && !error && !logs.length && <Box role="status" sx={{ display: 'grid', placeContent: 'center', flex: 1, textAlign: 'center', gap: 1 }}>
      <Typography sx={{ fontSize: 15, fontWeight: 500 }}>No model calls yet</Typography>
      <Typography sx={{ fontSize: 13, color: 'text.secondary' }}>Calls will appear here as this teamspace uses its models.</Typography>
    </Box>}
    {!!logs.length && <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: 'row' }, flex: 1, minHeight: 0, gap: 2 }}>
      <List aria-label="Model calls" disablePadding sx={{ width: { xs: '100%', md: 300 }, flexShrink: 0, minHeight: 0, maxHeight: { xs: '28vh', md: 'none' }, overflowY: 'auto', pr: 0.5 }}>
        {logs.map(log => <ListItemButton key={log.id} selected={selectedId === log.id} aria-label={`${operationName(log.endpoint)} · ${log.model}`}
          onClick={() => { setSelectedId(log.id); setDetail('response'); }} sx={{ display: 'block', borderRadius: 3, mb: 0.5, p: 1.5, '&.Mui-selected': { bgcolor: 'action.selected' } }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            {log.status === 'success' ? <Check size={14} /> : <CircleAlert size={14} />}
            <Typography noWrap sx={{ fontSize: 13, fontWeight: 550, flex: 1 }}>{operationName(log.endpoint)}</Typography>
            <Typography sx={{ fontSize: 11, color: 'text.secondary' }}>{duration(log.duration_ms)}</Typography>
          </Box>
          <Typography noWrap sx={{ fontSize: 12, mt: 0.75, color: 'text.secondary' }}>{log.model}</Typography>
          <Typography sx={{ fontSize: 11, mt: 0.5, color: 'text.secondary' }}>
            {new Date(log.created_at).toLocaleString()} · {typeof log.tokens_used !== 'number' ? 'Tokens unavailable' : `${log.tokens_used.toLocaleString()} tokens`}
          </Typography>
        </ListItemButton>)}
      </List>
      {selected && <Box role="region" aria-label="Model call details" sx={{ display: 'flex', flexDirection: 'column', flex: 1, minWidth: 0, minHeight: 0 }}>
        <Box sx={{ pb: 1.5 }}>
          <Typography sx={{ fontWeight: 600, fontSize: 15 }}>{operationName(selected.endpoint)}</Typography>
          <Typography sx={{ color: 'text.secondary', fontSize: 12, mt: 0.5 }}>{selected.model} · {selected.status === 'success' ? 'Completed' : 'Failed'}</Typography>
        </Box>
        <Box component="nav" aria-label="Model call content" sx={{ display: 'flex', gap: 0.5, pb: 1.5 }}>
          {(['response', 'request', 'metadata'] as const).map(value => <Button key={value} size="small" color="inherit" aria-pressed={detail === value} onClick={() => setDetail(value)}
            sx={{ borderRadius: 2.5, fontSize: 12, textTransform: 'none', bgcolor: detail === value ? 'action.selected' : 'transparent' }}>{value[0].toUpperCase() + value.slice(1)}</Button>)}
        </Box>
        {selected.error_message && <Alert severity="error" sx={{ mb: 1.5, fontSize: 12 }}>{selected.error_message}</Alert>}
        <Box sx={{ overflow: 'auto', flex: 1, minHeight: 0, pr: 1, pb: 2 }}>
          <Typography component="pre" sx={{ m: 0, fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: 12.5, lineHeight: 1.75, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>
            {body || (detail === 'response' ? 'No response was recorded for this call.' : 'No request was recorded for this call.')}
          </Typography>
        </Box>
      </Box>}
    </Box>}
    <TablePagination component="div" count={-1} rowsPerPage={rowsPerPage} page={page} rowsPerPageOptions={[10, 25, 50, 100]}
      labelRowsPerPage="Calls per page" labelDisplayedRows={({ from, to }) => logs.length ? `${from}–${Math.min(to, page * rowsPerPage + logs.length)}` : '0 calls'}
      backIconButtonProps={{ disabled: loading || page === 0 }} nextIconButtonProps={{ disabled: loading || logs.length < rowsPerPage }}
      onPageChange={(_, next) => { setPage(next); setLogs([]); }} onRowsPerPageChange={event => { setRowsPerPage(Number(event.target.value)); setPage(0); setLogs([]); }}
      sx={{ flexShrink: 0, '& .MuiTablePagination-toolbar': { px: 0 }, '& .MuiTablePagination-selectLabel, & .MuiTablePagination-displayedRows': { fontSize: 12 } }} />
  </Box>;
}
