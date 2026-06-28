import React, { useEffect, useRef, useState } from 'react';
import Box from '@mui/material/Box';
import type { Theme } from '@mui/material/styles';
import type { SystemStyleObject } from '@mui/system';
import { KasalMcpServer, listKasalMcpServers } from '../../api/mcp';
import { useExecutionStore } from '../../store/executionStore';
import { useAppStore } from '../../store/appStore';
import { AgentBricksService, AgentBricksEndpoint } from '../../../../api/AgentBricksService';
import { buttonResetSx, inputResetSx } from '../../chatSx';

/**
 * The chat input's "+" control (left of Send): pick the MCP servers (and Agent
 * Bricks endpoints) the next crew should be equipped with.
 *
 * It lists ONLY the MCP servers configured for this workspace — the curated,
 * group-scoped allow-list returned by /mcp/servers (admins additionally see
 * any they've disabled, shown greyed out). Browsing and registering the full
 * Databricks catalog (external connections, Databricks SQL, Unity Catalog
 * Functions, Genie spaces, AI Search indexes) now lives in
 * Configuration → MCP, so this picker never enumerates the whole workspace.
 *
 * Selections live in the execution store and are injected into every generated
 * agent's tool_configs.MCP_SERVERS.
 */
const McpPicker: React.FC<{ disabled?: boolean; menuPlacement?: 'up' | 'down' }> = ({
  disabled,
  menuPlacement = 'up',
}) => {
  // Open down only when the composer is centered (landing screen); otherwise up
  // (it's pinned to the bottom, where a downward menu would render off-screen).
  const menuPosSx = menuPlacement === 'down' ? { top: '100%', mt: 1 } : { bottom: '100%', mb: 1 };
  const [open, setOpen] = useState(false);
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
        if (!cancelled) setKasalServers(servers);
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
        if (!cancelled) setAgentBricks(res?.endpoints ?? []);
      })
      .catch(() => {
        if (!cancelled) setAgentBricks([]);
      });
    return () => {
      cancelled = true;
    };
  }, [open, agentBricks, agentBricksToolEnabled]);

  const check = (isSelected: boolean) => (
    <Box
      component="span"
      aria-hidden="true"
      sx={{
        width: 14,
        height: 14,
        borderRadius: '4px',
        flexShrink: 0,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        border: 1,
        borderColor: isSelected ? 'primary.main' : 'divider',
        backgroundColor: isSelected ? 'primary.main' : 'transparent',
      }}
    >
      {isSelected && (
        <Box component="svg" sx={{ width: 10, height: 10 }} fill="none" viewBox="0 0 24 24" stroke="#fff" strokeWidth={3}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
        </Box>
      )}
    </Box>
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

  const sectionHeaderSx = {
    px: 1.5,
    pt: 1.25,
    pb: 0.5,
    fontSize: 10,
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.025em',
    color: 'text.disabled',
  } as const;
  const rowBtnSx: SystemStyleObject<Theme> = {
    ...buttonResetSx,
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    gap: 1,
    px: 1.5,
    py: 0.75,
    textAlign: 'left',
    fontSize: 12,
    transition: 'opacity 0.15s',
    color: 'text.primary',
    '&:hover': { opacity: 0.8 },
    '&:disabled': { opacity: 0.4, cursor: 'not-allowed' },
  };
  const emptyMsgSx = { px: 1.5, py: 1, fontSize: 12, color: 'text.disabled' } as const;

  return (
    <Box ref={rootRef} sx={{ position: 'relative', flexShrink: 0 }}>
      <Box
        component="button"
        type="button"
        onClick={() => setOpen((v) => !v)}
        disabled={disabled}
        title="MCP servers for the next run"
        aria-label="MCP servers"
        aria-expanded={open}
        sx={{
          ...buttonResetSx,
          position: 'relative',
          width: 32,
          height: 32,
          borderRadius: '12px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          transition: 'color 0.15s, background-color 0.15s',
          color: open || totalSelected > 0 ? 'primary.main' : 'text.secondary',
          backgroundColor: (t) => t.chat.bgSecondary,
          border: 1,
          borderColor: 'divider',
          '&:hover': { opacity: 0.8 },
          '&:disabled': { opacity: 0.4, cursor: 'not-allowed' },
        }}
      >
        <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
        </Box>
        {totalSelected > 0 && (
          <Box
            component="span"
            sx={{
              position: 'absolute',
              top: -4,
              right: -4,
              fontSize: 9,
              fontVariantNumeric: 'tabular-nums',
              borderRadius: '9999px',
              minWidth: 14,
              height: 14,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              px: 0.25,
              backgroundColor: 'primary.main',
              color: '#fff',
            }}
          >
            {totalSelected}
          </Box>
        )}
      </Box>

      {open && (
        <Box
          role="menu"
          aria-label="MCP picker"
          sx={{
            position: 'absolute',
            ...menuPosSx,
            right: 0,
            width: 320,
            borderRadius: '12px',
            overflow: 'hidden',
            zIndex: 50,
            backgroundColor: 'background.default',
            border: 1,
            borderColor: 'divider',
            boxShadow: (t) => t.chat.shadowPopover,
          }}
        >
          <Box sx={sectionHeaderSx}>MCP</Box>

          <Box sx={{ px: 1.5, pb: 0.75 }}>
            <Box
              component="input"
              value={filter}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFilter(e.target.value)}
              placeholder="Search MCP servers…"
              aria-label="Search MCP servers"
              sx={{
                ...inputResetSx,
                width: '100%',
                borderRadius: '6px',
                px: 1,
                py: 0.5,
                fontSize: 12,
                backgroundColor: 'background.paper',
                color: 'text.primary',
                border: 1,
                borderColor: 'divider',
              }}
            />
          </Box>

          <Box sx={{ maxHeight: 320, overflowY: 'auto', pb: 0.5 }}>
            {kasalServers === null ? (
              <Box sx={emptyMsgSx}>Loading…</Box>
            ) : kasalList.length === 0 ? (
              <Box sx={emptyMsgSx}>No MCP servers available</Box>
            ) : visibleKasal.length === 0 ? (
              <Box sx={emptyMsgSx}>No matching MCP servers</Box>
            ) : (
              visibleKasal.map((server) => {
                const isSelected = selected.includes(server.name);
                return (
                  <Box
                    component="button"
                    key={String(server.id)}
                    type="button"
                    role="menuitemcheckbox"
                    aria-checked={isSelected}
                    disabled={!server.enabled && !isSelected}
                    onClick={() => toggle(server.name)}
                    title={!server.enabled ? 'Disabled — enable it in Configuration → MCP' : server.server_url}
                    sx={rowBtnSx}
                  >
                    {check(isSelected)}
                    <Box component="span" sx={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>{server.name}</Box>
                    {!server.enabled && (
                      <Box component="span" sx={{ fontSize: 10, flexShrink: 0, color: 'text.disabled' }}>disabled</Box>
                    )}
                  </Box>
                );
              })
            )}
          </Box>

          {/* Agent Bricks section — pick a Databricks Agent Bricks agent to equip
              the crew with (via AgentBricksTool). Hidden entirely when the
              workspace has no Agent Bricks agents (i.e. the feature isn't in use). */}
          {agentBricksToolEnabled && agentBricks && agentBricks.length > 0 && (
            <>
              <Box sx={{ ...sectionHeaderSx, borderTop: 1, borderColor: 'divider' }}>Agents</Box>
              <Box sx={{ maxHeight: 192, overflowY: 'auto', pb: 0.5 }}>
                {visibleAgentBricks.length === 0 ? (
                  <Box sx={{ px: 1.5, py: 0.75, fontSize: 12, color: 'text.disabled' }}>
                    No matching agents
                  </Box>
                ) : (
                  visibleAgentBricks.map((ep) => {
                    const isSelected = selectedAgentBricks.includes(ep.name);
                    return (
                      <Box
                        component="button"
                        key={ep.id || ep.name}
                        type="button"
                        role="menuitemcheckbox"
                        aria-checked={isSelected}
                        onClick={() => toggleAgentBricks(ep.name)}
                        title={ep.name}
                        sx={rowBtnSx}
                      >
                        {check(isSelected)}
                        <Box component="span" sx={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>{ep.display_name || ep.name}</Box>
                        <Box component="span" sx={{ fontSize: 10, textTransform: 'uppercase', flexShrink: 0, color: 'text.disabled' }}>
                          agent
                        </Box>
                      </Box>
                    );
                  })
                )}
              </Box>
            </>
          )}

          {/* The managed SQL MCP executes arbitrary statements with the
              caller's warehouse permissions — selecting it deserves an
              explicit, plain-language heads-up, not silent power. */}
          {selected.some((n) => n.toLowerCase() === 'databricks sql') && (
            <Box sx={{ px: 1.5, py: 1, fontSize: 11, color: '#d97706', borderTop: 1, borderColor: 'divider' }}>
              ⚠ Databricks SQL lets the agent change your data, not just read it —
              it can add, update or permanently delete records using your access.
              Be careful what you ask for and check what it did before trusting it.
            </Box>
          )}
          {error && (
            <Box sx={{ px: 1.5, py: 1, fontSize: 11, color: 'primary.main', borderTop: 1, borderColor: 'divider' }}>
              {error}
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
};

export default McpPicker;
