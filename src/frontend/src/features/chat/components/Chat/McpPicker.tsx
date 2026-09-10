import React, { useEffect, useRef, useState } from 'react';
import CapabilitiesPicker from '../../../tools/components/configuration/CapabilitiesPicker';
import { useExecutionStore } from '../../store/executionStore';
import { useAppStore } from '../../store/appStore';
import { AgentBricksService, AgentBricksEndpoint } from '../../../../api/databricks/AgentBricksService';

/** Shared inline MCP connections plus Chat's Agent Bricks selection. */
const McpPicker: React.FC<{
  disabled?: boolean;
  menuPlacement?: 'up' | 'down';
  /** 'button' (default): the composer's icon trigger + its own popover.
   *  'inline': render ONLY the list content, always open, position: static —
   *  for embedding inside another menu (the composer "+" accordion), where an
   *  absolutely-positioned popover would be clipped by overflow-hidden. */
  variant?: 'button' | 'inline';
}> = ({ disabled, menuPlacement = 'up', variant = 'button' }) => {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState('');
  const rootRef = useRef<HTMLDivElement>(null);

  const selected = useExecutionStore((s) => s.selectedMcpServers);
  const setSelected = useExecutionStore((s) => s.setSelectedMcpServers);
  // Default to [] so a persisted store snapshot predating this field (or a
  // partial test store) never crashes the picker on `.length`/`.includes`.
  const selectedAgentBricks = useExecutionStore((s) => s.selectedAgentBricksEndpoints) ?? [];
  const toggleAgentBricks = useExecutionStore((s) => s.toggleAgentBricksEndpoint);
  const [agentBricks, setAgentBricks] = useState<AgentBricksEndpoint[] | null>(null);
  // The "Agents" section only appears when the AgentBricksTool is enabled in the
  // workspace's tool catalog — without that tool, picking an endpoint can't equip it.
  const toolNameMap = useAppStore((s) => s.toolNameMap);
  const agentBricksToolEnabled = Object.values(toolNameMap).includes('AgentBricksTool');

  // Close on outside click.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [open]);

  // Content is visible when the popover is open — or always, in the inline
  // variant (embedded in the composer "+" menu, which mounts it only while
  // that menu is open). Both fetches key off THIS, not `open`: the inline
  // variant never sets `open`, and gating on it left the list on "Loading…"
  // forever.
  const contentVisible = variant === 'inline' || open;

  // Agent Bricks endpoints (loaded once when the content shows; the section is
  // hidden entirely when the workspace has none — i.e. the feature isn't in use).
  useEffect(() => {
    if (!contentVisible || agentBricks !== null || !agentBricksToolEnabled) return;
    let cancelled = false;
    AgentBricksService.getEndpoints(true)
      .then((res) => {
        if (cancelled) return;
        const endpoints = res?.endpoints ?? [];
        setAgentBricks(endpoints);
        // Same reconciliation as MCP: prune selected endpoints that no longer exist.
        const store = useExecutionStore.getState();
        const available = new Set(endpoints.map((e) => e.name));
        const kept = store.selectedAgentBricksEndpoints.filter((n) => available.has(n));
        if (kept.length !== store.selectedAgentBricksEndpoints.length) {
          store.setSelectedAgentBricksEndpoints(kept);
        }
      })
      .catch(() => {
        if (!cancelled) setAgentBricks([]);
      });
    return () => {
      cancelled = true;
    };
  }, [contentVisible, agentBricks, agentBricksToolEnabled]);

  const check = (isSelected: boolean) => (
    <span
      aria-hidden="true"
      className="w-3.5 h-3.5 rounded flex-shrink-0 flex items-center justify-center"
      style={{
        border: `1px solid ${isSelected ? 'var(--text-secondary)' : 'var(--border-color)'}`,
        backgroundColor: isSelected ? 'var(--bg-active-chip)' : 'transparent',
        color: 'var(--text-primary)',
      }}
    >
      {isSelected && (
        <svg className="w-2.5 h-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
        </svg>
      )}
    </span>
  );

  // The configured-server list can be long, so it's searchable by name.
  const query = filter.trim().toLowerCase();
  const nameMatches = (name: string) => !query || name.toLowerCase().includes(query);

  // Agent Bricks rows (filtered by the same top search box as MCP, matched on
  // the friendly agent name).
  const visibleAgentBricks = (agentBricks ?? []).filter((e) => nameMatches(e.display_name || e.name));
  const totalSelected = selected.length + selectedAgentBricks.length;

  const contentShell = (children: React.ReactNode) =>
    variant === 'inline' ? (
      <div role="menu" aria-label="MCP picker" className="w-full">
        {children}
      </div>
    ) : (
      <div
        role="menu"
        aria-label="MCP picker"
        className={`absolute right-0 ${menuPlacement === 'down' ? 'top-full mt-2' : 'bottom-full mb-2'} w-80 rounded-xl overflow-hidden z-20`}
        style={{
          backgroundColor: 'var(--bg-primary)',
          border: '1px solid var(--border-color)',
          boxShadow: 'var(--shadow-popover)',
          maxHeight: '70vh', overflowY: 'auto',
        }}
      >
        {children}
      </div>
    );

  return (
    <div ref={rootRef} className={variant === 'inline' ? '' : 'relative flex-shrink-0'}>
      {variant !== 'inline' && (
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        disabled={disabled}
        className="relative w-8 h-8 rounded-xl flex items-center justify-center transition-colors hover:opacity-80 disabled:opacity-40 disabled:cursor-not-allowed"
        style={{
          color: 'var(--text-secondary)',
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
        }}
        title="MCP servers for the next run"
        aria-label="MCP servers"
        aria-expanded={open}
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
        </svg>
        {totalSelected > 0 && (
          <span
            className="absolute -top-1 -right-1 text-[9px] tabular-nums rounded-full min-w-[14px] h-[14px] flex items-center justify-center px-0.5"
            style={{ backgroundColor: 'var(--bg-active-chip)', color: 'var(--text-primary)' }}
          >
            {totalSelected}
          </span>
        )}
      </button>
      )}

      {contentVisible && contentShell(
        <>
          <div
            className="px-3 pt-2.5 pb-1 text-[10px] font-semibold uppercase tracking-wide"
            style={{ color: 'var(--text-muted)' }}
          >
            Tools and agents
          </div>

          <CapabilitiesPicker selectedMcpServers={selected} onMcpServersChange={setSelected} disabled={disabled} reconcileSelection />

          {/* Agent Bricks section — pick a Databricks Agent Bricks agent to equip
              the crew with (via AgentBricksTool). Hidden entirely when the
              workspace has no Agent Bricks agents (i.e. the feature isn't in use). */}
          {agentBricksToolEnabled && agentBricks && agentBricks.length > 0 && (
            <>
              <div
                className="px-3 pt-2.5 pb-1 text-[10px] font-semibold uppercase tracking-wide"
                style={{ color: 'var(--text-muted)', borderTop: '1px solid var(--border-color)' }}
              >
                Agents
              </div>
              <input aria-label="Search agents" placeholder="Search agents…" value={filter} onChange={(e) => setFilter(e.target.value)} style={{ padding: 8, width: '100%' }} />
              <div className="max-h-48 overflow-y-auto px-1.5 pb-1.5">
                {visibleAgentBricks.length === 0 ? (
                  <div className="px-3 py-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>
                    No matching agents
                  </div>
                ) : (
                  visibleAgentBricks.map((ep) => {
                    const isSelected = selectedAgentBricks.includes(ep.name);
                    return (
                      <button
                        key={ep.id || ep.name}
                        type="button"
                        role="menuitemcheckbox"
                        aria-checked={isSelected}
                        onClick={() => toggleAgentBricks(ep.name)}
                        title={ep.name}
                        className="w-full flex items-center gap-2 !px-2.5 !py-1.5 my-0.5 rounded-lg text-left text-xs transition-colors hover:bg-[var(--bg-rail-hover)]"
                        style={{ color: 'var(--text-primary)' }}
                      >
                        {check(isSelected)}
                        <span className="truncate flex-1">{ep.display_name || ep.name}</span>
                        <span className="text-[10px] uppercase flex-shrink-0" style={{ color: 'var(--text-muted)' }}>
                          agent
                        </span>
                      </button>
                    );
                  })
                )}
              </div>
            </>
          )}

          {/* The managed SQL MCP executes arbitrary statements with the
              caller's warehouse permissions — selecting it deserves an
              explicit, plain-language heads-up, not silent power. */}
          {selected.some((n) => n.toLowerCase() === 'databricks sql') && (
            <div
              className="px-3 py-2 text-[11px]"
              style={{ color: '#d97706', borderTop: '1px solid var(--border-color)' }}
            >
              ⚠ Databricks SQL lets the agent change your data, not just read it —
              it can add, update or permanently delete records using your access.
              Be careful what you ask for and check what it did before trusting it.
            </div>
          )}

        </>,
      )}
    </div>
  );
};

export default McpPicker;
