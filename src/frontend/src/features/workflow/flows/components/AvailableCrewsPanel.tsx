import React, { useState } from 'react';
import { Box, ButtonBase, CircularProgress, IconButton, InputAdornment, TextField, Tooltip, Typography } from '@mui/material';
import { ArrowUpRight, RefreshCw, Search, Users } from 'lucide-react';
import type { CrewResponse } from '../../../../types/workflow/crew';

interface Props {
  crews: CrewResponse[];
  loading: boolean;
  onRefresh: () => void;
  onAdd: (crew: CrewResponse) => Promise<void>;
}

/** Crew catalog shares the conversation pane, leaving the whole canvas usable. */
export default function AvailableCrewsPanel({ crews, loading, onRefresh, onAdd }: Props) {
  const [query, setQuery] = useState('');
  const [adding, setAdding] = useState<string | null>(null);
  const filtered = crews.filter(crew => crew.name.toLowerCase().includes(query.toLowerCase()));
  return <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, px: 2.5, pt: 2, pb: 1 }}>
    <Box sx={{ display: 'flex', alignItems: 'center', mb: 0.75 }}>
      <Typography sx={{ flex: 1, fontWeight: 600, fontSize: 18 }}>Build with your crews</Typography>
      <Tooltip title="Refresh crews"><IconButton aria-label="Refresh crews" disabled={loading} onClick={onRefresh} size="small"><RefreshCw size={16} /></IconButton></Tooltip>
    </Box>
    <Typography sx={{ color: 'text.secondary', fontSize: 13, lineHeight: 1.65, mb: 2 }}>Describe the outcome below and Kasal will connect the right crews. You can also add them to the canvas yourself.</Typography>
    <TextField size="small" placeholder="Search crews" value={query} onChange={e => setQuery(e.target.value)} inputProps={{ 'aria-label': 'Search available crews' }}
      InputProps={{ startAdornment: <InputAdornment position="start"><Search size={16} /></InputAdornment> }} sx={{ mb: 1.5, '& .MuiOutlinedInput-root': { borderRadius: 3, fontSize: 13 }, '& fieldset': { border: 0 }, bgcolor: 'action.hover', borderRadius: 3 }} />
    <Box sx={{ overflowY: 'auto', flex: 1, minHeight: 0 }}>
      {loading ? <Box sx={{ p: 4, textAlign: 'center' }}><CircularProgress size={22} /></Box> : filtered.length ? filtered.map(crew => <ButtonBase key={crew.id} disabled={!!adding}
        aria-label={`Add ${crew.name} to flow`} onClick={async () => { setAdding(crew.id); try { await onAdd(crew); } finally { setAdding(null); } }}
        sx={{ display: 'flex', gap: 1.5, width: '100%', textAlign: 'left', px: 1.5, py: 1.75, mb: 1, borderRadius: 3, bgcolor: 'action.hover', '&:hover': { bgcolor: 'action.selected' } }}>
        <Box sx={{ color: 'text.secondary', display: 'flex', flexShrink: 0 }}><Users size={20} strokeWidth={1.6} /></Box>
        <Box sx={{ minWidth: 0, flex: 1 }}><Typography sx={{ fontSize: 14, fontWeight: 500 }}>{crew.name}</Typography>
          <Typography sx={{ fontSize: 12, color: 'text.secondary', mt: 0.4 }}>{crew.agent_ids?.length || 0} agents · {crew.task_ids?.length || 0} tasks</Typography></Box>
        {adding === crew.id ? <CircularProgress size={15} /> : <ArrowUpRight size={15} />}
      </ButtonBase>) : <Typography sx={{ py: 3, fontSize: 13, color: 'text.secondary' }}>{crews.length ? 'No matching crews.' : 'No saved crews yet. Create and save a crew in Agent Builder to use it here.'}</Typography>}
    </Box>
  </Box>;
}
