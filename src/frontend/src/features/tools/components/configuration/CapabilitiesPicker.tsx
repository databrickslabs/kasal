import React, { useState } from 'react';
import { Box, Divider, InputAdornment, TextField } from '@mui/material';
import { Search } from 'lucide-react';
import { useGroupStore } from '../../../../store/groups';
import ToolConnectionPicker from './ToolConnectionPicker';
import McpConnectionPicker from './McpConnectionPicker';
import A2AConnectionPicker from './A2AConnectionPicker';

export default function CapabilitiesPicker({ selectedTools, onToolsChange, selectedMcpServers, onMcpServersChange, disabled, reconcileSelection }: {
  selectedTools?: string[]; onToolsChange?: (ids: string[]) => void;
  selectedMcpServers?: string[]; onMcpServersChange?: (names: string[]) => void;
  disabled?: boolean; reconcileSelection?: boolean;
}) {
  const [query, setQuery] = useState('');
  const groupId = useGroupStore(state => state.currentGroupId);
  return <Box sx={{ minWidth: 280, maxWidth: '100%', p: 0.5 }}>
    <TextField fullWidth size="small" placeholder="Search tools and agents…" value={query} onChange={e => setQuery(e.target.value)}
      inputProps={{ 'aria-label': 'Search capabilities' }} InputProps={{ startAdornment: <InputAdornment position="start"><Search size={16} /></InputAdornment> }} />
    <Box sx={{ maxHeight: 'min(55vh, 520px)', overflowY: 'auto', mt: 1 }}>
      <ToolConnectionPicker key={`tools:${groupId}`} searchQuery={query} selectedIds={selectedTools} onChange={onToolsChange} disabled={disabled} />
      <Divider sx={{ my: 1 }} />
      <McpConnectionPicker key={`mcp:${groupId}`} searchQuery={query} selectedNames={selectedMcpServers} onChange={onMcpServersChange} disabled={disabled} reconcileSelection={reconcileSelection} />
      <Divider sx={{ my: 1 }} />
      <A2AConnectionPicker key={`a2a:${groupId}`} searchQuery={query} disabled={disabled} />
    </Box>
  </Box>;
}
