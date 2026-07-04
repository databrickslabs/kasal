import React, { useEffect, useRef, useState } from 'react';
import { KasalMcpServer, listKasalMcpServers } from '../../api/mcp';
import { useExecutionStore } from '../../store/executionStore';
import { useAppStore } from '../../store/appStore';
import { usePermissionStore } from '../../../../store/permissions';
import { AgentBricksService, AgentBricksEndpoint } from '../../../../api/AgentBricksService';

/**
 * The chat input's "+" control (left of Send): pick the MCP servers (and Agent
 * Bricks endpoints) the next crew should be equipped with.
 *
 * It lists ONLY the MCP servers ENABLED for this workspace — the curated,
 * group-scoped allow-list returned by /mcp/servers, filtered to enabled servers
 * (disabled ones aren't usable in a run, so they're omitted). Browsing and
 * registering the full
 * Databricks catalog (external connections, Databricks SQL, Unity Catalog
 * Functions, Genie spaces, AI Search indexes) now lives in
 * Configuration → MCP, so this picker never enumerates the whole workspace.
 *
 * Selections live in the execution store and are injected into every generated
 * agent's tool_configs.MCP_SERVERS.
 */
const McpPicker: React.FC<{
  disabled?: boolean;
  menuPlacement?: 'up' | 'down';
  /** Open the MCP configuration dialog — shown as a "Connect a tool" footer action
   *  for workspace/system admins so first-run users can register/enable servers
   *  right from the picker. Members (who can't configure MCP) don't see it. */
  onOpenMcpConfig?: () => void;
}> = ({ disabled, menuPlacement = 'up', onOpenMcpConfig }) => {
  const [open, setOpen] = useState(false);
  const canConfigureMcp = usePermissionStore((s) => s.isWorkspaceAdmin());
  const [filter, setFilter] = useState('');
  const [kasalServers, setKasalServers] = useState<KasalMcpServer[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  const selected = useExecutionStore((s) => s.selectedMcpServers);
  const toggle = useExecutionStore((s) => s.toggleMcpServer);
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

  // Load the workspace's configured MCP servers when the popover opens.
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setError(null);
    listKasalMcpServers()
      .then((servers) => {
        if (cancelled) return;
        setKasalServers(servers);
        // Reconcile the persisted selection against reality: servers removed or
        // disabled in Configuration leave stale names in the store, which show as
        // a phantom "+" count. Drop any selected name no longer available so the
        // badge reflects what's actually equipped.
        const store = useExecutionStore.getState();
        const available = new Set(servers.map((s) => s.name));
        const kept = store.selectedMcpServers.filter((n) => available.has(n));
        if (kept.length !== store.selectedMcpServers.length) {
          store.setSelectedMcpServers(kept);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setKasalServers([]);
          setError('Could not load MCP servers');
        }
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  // Agent Bricks endpoints (loaded once when the popover opens; the section is
  // hidden entirely when the workspace has none — i.e. the feature isn't in use).
  useEffect(() => {
    if (!open || agentBricks !== null || !agentBricksToolEnabled) return;
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
  }, [open, agentBricks, agentBricksToolEnabled]);

  const check = (isSelected: boolean) => (
    <span
      aria-hidden="true"
      className="w-3.5 h-3.5 rounded flex-shrink-0 flex items-center justify-center"
      style={{
        border: `1px solid ${isSelected ? 'var(--accent)' : 'var(--border-color)'}`,
        backgroundColor: isSelected ? 'var(--accent)' : 'transparent',
      }}
    >
      {isSelected && (
        <svg className="w-2.5 h-2.5" fill="none" viewBox="0 0 24 24" stroke="#fff" strokeWidth={3}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
        </svg>
      )}
    </span>
  );

  // The configured-server list can be long, so it's searchable by name.
  const query = filter.trim().toLowerCase();
  const nameMatches = (name: string) => !query || name.toLowerCase().includes(query);
  const kasalList = kasalServers ?? [];
  const visibleKasal = kasalList.filter((s) => nameMatches(s.name));

  // Agent Bricks rows (filtered by the same top search box as MCP, matched on
  // the friendly agent name).
  const visibleAgentBricks = (agentBricks ?? []).filter((e) => nameMatches(e.display_name || e.name));
  const totalSelected = selected.length + selectedAgentBricks.length;

  return (
    <div ref={rootRef} className="relative flex-shrink-0">
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
            style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
          >
            {totalSelected}
          </span>
        )}
      </button>

      {open && (
        <div
          role="menu"
          aria-label="MCP picker"
          className={`absolute right-0 ${menuPlacement === 'down' ? 'top-full mt-2' : 'bottom-full mb-2'} w-80 rounded-xl overflow-hidden z-20`}
          style={{
            backgroundColor: 'var(--bg-primary)',
            border: '1px solid var(--border-color)',
            boxShadow: 'var(--shadow-popover)',
          }}
        >
          <div
            className="px-3 pt-2.5 pb-1 text-[10px] font-semibold uppercase tracking-wide"
            style={{ color: 'var(--text-muted)' }}
          >
            MCP
          </div>

          <div className="px-3 pb-1.5">
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Search MCP servers…"
              aria-label="Search MCP servers"
              className="w-full rounded-md px-2 py-1 text-xs outline-none"
              style={{
                backgroundColor: 'var(--bg-input)',
                color: 'var(--text-primary)',
                border: '1px solid var(--border-color)',
              }}
            />
          </div>

          <div className="max-h-80 overflow-y-auto px-1.5 pb-1.5">
            {kasalServers === null ? (
              <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
            ) : kasalList.length === 0 ? (
              <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>
                No MCP servers available
              </div>
            ) : visibleKasal.length === 0 ? (
              <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>
                No matching MCP servers
              </div>
            ) : (
              visibleKasal.map((server) => {
                const isSelected = selected.includes(server.name);
                return (
                  <button
                    key={String(server.id)}
                    type="button"
                    role="menuitemcheckbox"
                    aria-checked={isSelected}
                    disabled={!server.enabled && !isSelected}
                    onClick={() => toggle(server.name)}
                    title={!server.enabled ? 'Disabled — enable it in Configuration → MCP' : server.server_url}
                    className="w-full flex items-center gap-2 !px-2.5 !py-1.5 my-0.5 rounded-lg text-left text-xs transition-colors hover:bg-[var(--bg-rail-hover)] disabled:opacity-40 disabled:cursor-not-allowed"
                    style={{ color: 'var(--text-primary)' }}
                  >
                    {check(isSelected)}
                    <span className="truncate flex-1">{server.name}</span>
                    {!server.enabled && (
                      <span className="text-[10px] flex-shrink-0" style={{ color: 'var(--text-muted)' }}>disabled</span>
                    )}
                  </button>
                );
              })
            )}
          </div>

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
          {error && (
            <div className="px-3 py-2 text-[11px]" style={{ color: 'var(--accent)', borderTop: '1px solid var(--border-color)' }}>
              {error}
            </div>
          )}

          {/* Connect-a-tool action — registering/enabling MCP servers is admin-only,
              so this footer appears for workspace/system admins. It opens the MCP
              config dialog; on close the list refetches so a newly enabled server
              shows up without reopening the picker. */}
          {canConfigureMcp && onOpenMcpConfig && (
            <div className="p-1.5" style={{ borderTop: '1px solid var(--border-color)' }}>
              <button
                type="button"
                onClick={() => {
                  setOpen(false);
                  onOpenMcpConfig();
                }}
                className="w-full flex items-center gap-2 !px-2.5 !py-1.5 rounded-lg text-left text-xs transition-colors hover:bg-[var(--bg-rail-hover)]"
                style={{ color: 'var(--text-primary)' }}
              >
                <svg className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--text-secondary)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
                </svg>
                Connect a tool…
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default McpPicker;
