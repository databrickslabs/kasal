import React, { useEffect, useRef, useState } from 'react';
import { Box, Button } from '@mui/material';
import { MCPService, type DatabricksMcpOption } from '../../../../api/tools/MCPService';
import type { MCPServerConfig } from '../../../../types/config/mcp';
import { usePermissionStore } from '../../../../store/permissions';
import CapabilityRow from './CapabilityRow';
import DatabricksMcpPicker from './DatabricksMcpPicker';

export interface McpConnectionPickerProps {
  /** Omit onChange for workspace connection management without a run selection. */
  selectedNames?: string[];
  onChange?: (names: string[]) => void;
  disabled?: boolean;
  reconcileSelection?: boolean;
  searchQuery?: string;
}

/** The same inline connection surface for Chat and both builders. */
export default function McpConnectionPicker({ selectedNames = [], onChange, disabled = false, reconcileSelection = false, searchQuery }: McpConnectionPickerProps) {
  const systemAdmin = usePermissionStore((s) => s.isSystemAdmin);
  const canConfigure = usePermissionStore((s) => s.isWorkspaceAdmin());
  const [servers, setServers] = useState<MCPServerConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<'available' | 'databricks' | 'custom'>('available');
  const [query, setQuery] = useState('');
  const [busy, setBusy] = useState(false);
  const [reload, setReload] = useState(0);
  const [form, setForm] = useState({ name: '', url: '', key: '', transport: 'streamable' });
  const mounted = useRef(true);
  const latest = useRef({ selectedNames, onChange });
  latest.current = { selectedNames, onChange };
  const operating = useRef(false);
  const ownerGroup = useRef(localStorage.getItem('selectedGroupId'));
  const active = () => mounted.current && ownerGroup.current === localStorage.getItem('selectedGroupId');

  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    MCPService.getInstance().getMcpServers().then(({ servers: rows }) => {
      if (!cancelled && active()) {
        setServers(rows); setError(null);
        if (reconcileSelection) {
          const names = new Set(rows.filter((s) => s.enabled).map((s) => s.name));
          const kept = latest.current.selectedNames.filter((n) => names.has(n));
          if (kept.length !== latest.current.selectedNames.length) latest.current.onChange?.(kept);
        }
      }
    }).catch((e: unknown) => {
      if (!cancelled && active()) setError(e instanceof Error ? e.message : 'Could not load connected tools.');
    }).finally(() => { if (!cancelled && active()) setLoading(false); });
    return () => { cancelled = true; };
  }, [reload, reconcileSelection]);

  const select = (name: string) => {
    if (!active()) return;
    const current = latest.current;
    if (!current.selectedNames.includes(name)) current.onChange?.([...current.selectedNames, name]);
  };
  const enable = async (server: MCPServerConfig) => {
    if (!active()) throw new Error('Teamspace changed. Reopen the tool picker.');
    const svc = MCPService.getInstance();
    const enabled = server.enabled && server.group_id
      ? server
      : ownerGroup.current ? await svc.enableForWorkspace(server.id, ownerGroup.current) : await svc.enableForWorkspace(server.id);
    if (!enabled.enabled || !enabled.name) throw new Error('The server could not be enabled. Try connecting again.');
    if (active()) {
      setServers((rows) => [...rows.filter((r) => r.name.toLowerCase() !== enabled.name.toLowerCase()), enabled]);
      select(enabled.name);
    }
    return enabled.name;
  };
  const operate = async (action: () => Promise<string>) => {
    if (operating.current || disabled) throw new Error('A connection is already in progress.');
    operating.current = true; setBusy(true); setError(null);
    try { return await action(); }
    finally { operating.current = false; if (mounted.current) setBusy(false); }
  };
  const connect = (option: DatabricksMcpOption) => operate(async () => {
    const svc = MCPService.getInstance();
    const name = await svc.ensureDatabricksServer(option, 'global');
    const { servers: base } = await svc.getBaseServers();
    const server = base.find((s) => s.name.toLowerCase() === name.toLowerCase());
    if (!server) throw new Error('The registered server could not be found. Try connecting again.');
    return enable(server);
  });
  const report = (e: unknown) => { if (active()) setError(e instanceof Error ? e.message : 'Could not connect the server.'); };
  const input: React.CSSProperties = { width: '100%', padding: '8px 10px', fontSize: 13, borderRadius: 8, border: '1px solid var(--border-color)', background: 'var(--bg-input)', color: 'var(--text-primary)' };
  const button: React.CSSProperties = { ...input, width: 'auto', cursor: 'pointer' };
  const selected = onChange ? selectedNames : servers.filter((s) => s.enabled).map((s) => s.name);
  const visible = servers.filter((s) => (canConfigure || s.enabled) && s.name.toLowerCase().includes((searchQuery ?? query).toLowerCase()));

  return <Box data-testid="mcp-connection-picker" sx={(theme) => ({
    '--bg-primary': theme.palette.background.paper, '--bg-secondary': theme.palette.action.hover,
    '--bg-input': theme.palette.background.paper, '--bg-active-chip': theme.palette.action.selected,
    '--bg-rail-hover': theme.palette.action.hover, '--border-color': theme.palette.divider,
    '--text-primary': theme.palette.text.primary, '--text-secondary': theme.palette.text.secondary,
    '--text-muted': theme.palette.text.secondary, '--accent': theme.palette.error.main,
    color: 'text.primary', p: 1, minWidth: 0,
  })}>
    {view !== 'available' && <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
      <button type="button" style={button} onClick={() => { setView('available'); setError(null); }}>‹ Your tools</button>
      {systemAdmin && <button type="button" style={button} onClick={() => { setView(view === 'custom' ? 'databricks' : 'custom'); setError(null); }}>
        {view === 'custom' ? 'Browse Databricks' : 'Custom URL'}
      </button>}
    </div>}
    {view === 'available' ? <>
      {searchQuery === undefined && <input aria-label="Search connected tools" placeholder="Search connected tools…" value={query} onChange={(e) => setQuery(e.target.value)} style={input} />}
      {loading ? <p role="status">Loading tools…</p> : visible.map((server) => <CapabilityRow key={server.id} name={server.name}
        selected={selected.includes(server.name)} selectable={!!onChange}
        disabled={disabled || busy}
        status={selected.includes(server.name) ? onChange ? 'Selected' : 'Connected' : server.enabled ? 'Select' : 'Connect'}
        onClick={() => {
          if (!active()) return;
          if (server.enabled && onChange) {
            onChange(selected.includes(server.name) ? selected.filter((n) => n !== server.name) : [...selected, server.name]);
          } else if (!server.enabled && canConfigure) {
            void operate(() => enable(server)).catch(report);
          }
        }} />)}
      {!loading && !visible.length && !error && <p style={{ fontSize: 12 }}>{query ? 'No matching tools.' : 'No tools connected to this teamspace yet.'}</p>}
      {canConfigure && systemAdmin && <Button size="small" disabled={disabled || busy} onClick={() => setView('databricks')}>Connect a tool…</Button>}
      {error && <button type="button" style={button} onClick={() => setReload((n) => n + 1)}>Retry</button>}
    </> : view === 'databricks' ? <DatabricksMcpPicker selectedNames={selected} onConnect={connect} selectedLabel={onChange ? 'Selected' : 'Connected'}
      registeredUrls={new Set(servers.map((s) => s.server_url?.replace(/\/+$/, '') || ''))} /> : <form onSubmit={(event) => {
      event.preventDefault();
      void operate(async () => {
        const svc = MCPService.getInstance();
        const { servers: base } = await svc.getBaseServers();
        const server = base.find((s) => s.server_url === form.url.trim()) ?? await svc.createGlobalServer({
          name: form.name.trim(), server_url: form.url.trim(), server_type: form.transport, auth_type: 'api_key', api_key: form.key,
          enabled: true, global_enabled: false, timeout_seconds: 30, max_retries: 3, rate_limit: 60,
        });
        if (!server.enabled) await svc.setGlobalAvailability(server.id, true);
        const name = await enable({ ...server, enabled: false });
        if (active()) { setForm({ name: '', url: '', key: '', transport: 'streamable' }); setView('available'); }
        return name;
      }).catch(report);
    }} style={{ display: 'grid', gap: 8 }}>
      <input required aria-label="Server name" placeholder="Server name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} style={input} />
      <input required type="url" aria-label="Server URL" placeholder="https://…/mcp" value={form.url} onChange={(e) => setForm({ ...form, url: e.target.value })} style={input} />
      <input type="password" aria-label="API key" placeholder="API key (optional)" value={form.key} onChange={(e) => setForm({ ...form, key: e.target.value })} style={input} />
      <select aria-label="Transport" value={form.transport} onChange={(e) => setForm({ ...form, transport: e.target.value })} style={input}><option value="streamable">Streamable HTTP</option><option value="sse">SSE</option></select>
      <button type="submit" disabled={disabled || busy} style={input}>{busy ? 'Connecting…' : 'Connect'}</button>
    </form>}
    {error && <p role="alert" style={{ color: 'var(--accent)', fontSize: 12 }}>{error}</p>}
  </Box>;
}
