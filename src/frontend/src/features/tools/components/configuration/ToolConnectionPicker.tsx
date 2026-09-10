import React, { useEffect, useRef, useState } from 'react';
import { Box, Button, CircularProgress, TextField, Typography } from '@mui/material';
import CapabilitySection from './CapabilitySection';
import CapabilityRow from './CapabilityRow';
import { ToolService, type Tool } from '../../../../api/tools/ToolService';
import { GroupToolService } from '../../../../api/groups/GroupToolService';
import { usePermissionStore } from '../../../../store/permissions';

/** Enabled built-in tools and inline teamspace opt-in, shared by both builders. */
export default function ToolConnectionPicker({ selectedIds = [], onChange, disabled = false, searchQuery }: {
  selectedIds?: string[]; onChange?: (ids: string[]) => void; disabled?: boolean; searchQuery?: string;
}) {
  const admin = usePermissionStore(s => s.isWorkspaceAdmin());
  const [tools, setTools] = useState<Tool[]>([]);
  const [available, setAvailable] = useState<Tool[]>([]);
  const [browse, setBrowse] = useState(false);
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  const owner = useRef(localStorage.getItem('selectedGroupId'));
  const mounted = useRef(true);
  const operating = useRef(false);
  const latest = useRef({ selectedIds, onChange });
  latest.current = { selectedIds, onChange };
  const active = () => mounted.current && owner.current === localStorage.getItem('selectedGroupId');
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    Promise.all([ToolService.listEnabledTools(), browse && admin ? GroupToolService.listAvailable() : Promise.resolve([])])
      .then(([enabled, catalog]) => { if (!cancelled && active()) { setTools(enabled); setAvailable(catalog); setError(''); } })
      .catch(() => { if (!cancelled && active()) setError('Could not load tools. Retry to refresh the list.'); })
      .finally(() => { if (!cancelled && active()) setLoading(false); });
    return () => { cancelled = true; };
  }, [browse, admin, revision]);
  const add = async (tool: Tool) => {
    if (!active() || !owner.current || operating.current) return;
    operating.current = true; setBusy(true); setError('');
    try {
      const mapping = await GroupToolService.addTool(tool.id, owner.current);
      if (!active()) return;
      if (!mapping.enabled) await GroupToolService.setEnabled(tool.id, true, owner.current);
      if (!active()) return;
      const enabled = await ToolService.listEnabledTools();
      if (!active()) return;
      if (!enabled.some(t => t.id === tool.id)) throw new Error('The tool could not be enabled. Check its configuration and retry.');
      setTools(enabled);
      const current = latest.current;
      if (!current.selectedIds.includes(String(tool.id))) current.onChange?.([...current.selectedIds, String(tool.id)]);
      window.dispatchEvent(new Event('tools-changed'));
    } catch (e) { if (active()) setError(e instanceof Error ? e.message : 'Could not add this tool.'); }
    finally { operating.current = false; if (mounted.current) setBusy(false); }
  };
  const rows = [...tools, ...(browse ? available.filter(t => !tools.some(enabled => enabled.id === t.id)) : [])]
    .filter(tool => `${tool.title} ${tool.description}`.toLowerCase().includes((searchQuery ?? query).toLowerCase()));
  return <CapabilitySection title="Tools">
    {searchQuery === undefined && <TextField size="small" fullWidth placeholder="Search tools…" inputProps={{ 'aria-label': 'Search tools' }} value={query} onChange={e => setQuery(e.target.value)} />}
    {loading ? <CircularProgress size={20} aria-label="Loading tools" /> : <Box>
      {rows.map(tool => {
        const enabled = tools.some(t => t.id === tool.id);
        const selected = selectedIds.includes(String(tool.id));
        return <CapabilityRow key={tool.id} name={tool.title} description={tool.description}
          selected={onChange ? selected : enabled} selectable={!!onChange && enabled}
          disabled={disabled || busy} status={!enabled ? 'Add' : onChange ? selected ? 'Selected' : 'Select' : 'Enabled'}
          onClick={onChange || !enabled ? () => {
            if (!active()) return;
            if (!enabled) { void add(tool); return; }
            onChange?.(selected ? selectedIds.filter(id => id !== String(tool.id)) : [...selectedIds, String(tool.id)]);
          } : undefined} />;
      })}
      {!rows.length && !error && <Typography variant="body2">{query ? 'No matching tools.' : 'No tools enabled in this teamspace.'}</Typography>}
    </Box>}
    {admin && <Button size="small" disabled={disabled || busy} onClick={() => setBrowse(!browse)}>{browse ? 'Show enabled tools' : 'Add tools to teamspace'}</Button>}
    {error && <Box role="alert"><Typography color="error" variant="body2">{error}</Typography><Button onClick={() => setRevision(n => n + 1)}>Retry tools</Button></Box>}
  </CapabilitySection>;
}
