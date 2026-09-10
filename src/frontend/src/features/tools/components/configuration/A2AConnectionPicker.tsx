import React, { useEffect, useRef, useState } from 'react';
import { Box, Button, Typography } from '@mui/material';
import { A2AAgentService, type A2AAgent } from '../../../../api/tools/A2AAgentService';
import { usePermissionStore } from '../../../../store/permissions';
import CapabilitySection from './CapabilitySection';
import CapabilityRow from './CapabilityRow';

export default function A2AConnectionPicker({ searchQuery, disabled }: { searchQuery: string; disabled?: boolean }) {
  const admin = usePermissionStore(s => s.isWorkspaceAdmin());
  const [agents, setAgents] = useState<A2AAgent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [browse, setBrowse] = useState(false);
  const [revision, setRevision] = useState(0);
  const owner = useRef(localStorage.getItem('selectedGroupId'));
  const mounted = useRef(true);
  const operating = useRef(false);
  const active = () => mounted.current && owner.current === localStorage.getItem('selectedGroupId');
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    A2AAgentService.list().then(rows => { if (!cancelled && active()) { setAgents(rows.map(agent => ({ ...agent, enabled: agent.enabled && agent.group_id === owner.current }))); setError(''); } })
      .catch(() => { if (!cancelled && active()) setError('Could not load A2A agents.'); })
      .finally(() => { if (!cancelled && active()) setLoading(false); });
    return () => { cancelled = true; };
  }, [revision]);
  const rows = agents.filter(a => (a.enabled || admin && browse) && `${a.name} ${a.description || ''} ${a.skills.map(s => s.name).join(' ')}`.toLowerCase().includes(searchQuery.toLowerCase()));
  const enable = async (agent: A2AAgent) => {
    if (!active() || !owner.current || operating.current) return;
    operating.current = true; setBusy(true); setError('');
    try {
      const result = await A2AAgentService.setWorkspaceEnabled(agent.id, true, owner.current);
      if (!result.enabled) throw new Error('The agent could not be enabled.');
      if (active()) setAgents(current => [...current.filter(a => a.name !== result.name), result]);
    } catch (e) { if (active()) setError(e instanceof Error ? e.message : 'Could not enable this agent.'); }
    finally { operating.current = false; if (mounted.current) setBusy(false); }
  };
  return <CapabilitySection title="A2A agents">
    {loading ? <Typography variant="body2">Loading agents…</Typography> : rows.map(agent => <CapabilityRow key={agent.id}
      name={agent.name} description={agent.skills.map(s => s.name).join(', ') || agent.description || undefined}
      selected={agent.enabled} status={agent.enabled ? 'Enabled' : 'Add'} disabled={disabled || busy}
      onClick={agent.enabled ? undefined : () => { void enable(agent); }} />)}
    {!loading && !rows.length && !error && <Typography variant="body2">{searchQuery ? 'No matching agents.' : 'No A2A agents enabled in this teamspace.'}</Typography>}
    {admin && <Button size="small" disabled={disabled || busy} onClick={() => setBrowse(!browse)}>{browse ? 'Show enabled agents' : 'Add agents to teamspace'}</Button>}
    {error && <Box role="alert"><Typography color="error" variant="body2">{error}</Typography><Button onClick={() => setRevision(n => n + 1)}>Retry agents</Button></Box>}
  </CapabilitySection>;
}
