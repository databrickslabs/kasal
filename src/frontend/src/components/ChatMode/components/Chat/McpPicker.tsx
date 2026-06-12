import React, { useEffect, useRef, useState } from 'react';
import {
  DatabricksMcpCatalog,
  DatabricksMcpOption,
  DatabricksManagedMcpType,
  KasalMcpServer,
  databricksMcpServerName,
  ensureDatabricksMcpServer,
  getDatabricksMcpCatalog,
  listAiSearchMcpIndexes,
  listGenieMcpSpaces,
  listKasalMcpServers,
} from '../../api/mcp';
import { useExecutionStore } from '../../store/executionStore';

/**
 * The chat input's "+" control (left of Send): pick the MCP servers the next
 * crew should be equipped with. ONE flat list under a single "MCP" header —
 * Kasal-configured servers first, then the workspace's Databricks MCPs:
 * external (UC connection-based) ones, directly selectable managed leaves
 * (Databricks SQL, Unity Catalog Functions from the configured
 * catalog.schema), and two-step Genie / AI Search drill-downs (type first,
 * then a searchable space / index list — a workspace can have thousands of
 * Genie spaces, so they are never listed up front).
 * Selecting a Databricks entry registers it as a Kasal MCP server
 * (streamable + databricks_spn); entries already registered in Kasal are
 * hidden from the Databricks portion so each server appears exactly once.
 * Selections live in the execution store and are injected into every
 * generated agent's tool_configs.MCP_SERVERS.
 */

const isRegisteredInKasal = (
  option: DatabricksMcpOption,
  servers: KasalMcpServer[],
): boolean =>
  servers.some(
    (s) =>
      // Registered names are lowercase, but legacy rows may be mixed-case.
      s.name.toLowerCase() === databricksMcpServerName(option) ||
      Boolean(s.server_url && s.server_url === option.server_url),
  );

const managedLeafOption = (t: DatabricksManagedMcpType): DatabricksMcpOption => ({
  id: t.id,
  kind: t.kind,
  name: t.name,
  description: t.description,
  server_url: t.server_url || '',
});

const McpPicker: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState('');
  const [kasalServers, setKasalServers] = useState<KasalMcpServer[] | null>(null);
  const [catalog, setCatalog] = useState<DatabricksMcpCatalog | null>(null);
  const [expandedType, setExpandedType] = useState<'genie' | 'ai-search' | null>(null);
  const [genieSearch, setGenieSearch] = useState('');
  const [genieOptions, setGenieOptions] = useState<DatabricksMcpOption[]>([]);
  const [genieLoaded, setGenieLoaded] = useState(false);
  const [genieNextToken, setGenieNextToken] = useState<string | null>(null);
  const [aiSearchOptions, setAiSearchOptions] = useState<DatabricksMcpOption[]>([]);
  const [aiSearchLoaded, setAiSearchLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busyOption, setBusyOption] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  const selected = useExecutionStore((s) => s.selectedMcpServers);
  const toggle = useExecutionStore((s) => s.toggleMcpServer);

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

  // Load the Kasal-configured servers when the popover opens.
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

  // Load the Databricks catalog when the popover opens (cached afterwards).
  useEffect(() => {
    if (!open || catalog !== null) return;
    let cancelled = false;
    getDatabricksMcpCatalog()
      .then((c) => {
        if (!cancelled) setCatalog(c);
      })
      .catch(() => {
        if (!cancelled) {
          setCatalog({ workspace_url: '', external: [], managed: [] });
          setError('Could not load Databricks MCPs');
        }
      });
    return () => {
      cancelled = true;
    };
  }, [open, catalog]);

  // Step two: Genie spaces (searchable, paginated; debounced on the search box).
  useEffect(() => {
    if (!open || expandedType !== 'genie') return;
    let cancelled = false;
    const timer = window.setTimeout(() => {
      listGenieMcpSpaces(genieSearch || undefined)
        .then(({ options, next_page_token }) => {
          if (cancelled) return;
          setGenieOptions(options);
          setGenieNextToken(next_page_token);
          setGenieLoaded(true);
        })
        .catch(() => {
          if (cancelled) return;
          setGenieOptions([]);
          setGenieNextToken(null);
          setGenieLoaded(true);
          setError('Could not load Genie spaces');
        });
    }, genieLoaded ? 250 : 0);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, expandedType, genieSearch]);

  // Step two: AI Search indexes.
  useEffect(() => {
    if (!open || expandedType !== 'ai-search' || aiSearchLoaded) return;
    let cancelled = false;
    listAiSearchMcpIndexes()
      .then((options) => {
        if (cancelled) return;
        setAiSearchOptions(options);
        setAiSearchLoaded(true);
      })
      .catch(() => {
        if (cancelled) return;
        setAiSearchOptions([]);
        setAiSearchLoaded(true);
        setError('Could not load AI Search indexes');
      });
    return () => {
      cancelled = true;
    };
  }, [open, expandedType, aiSearchLoaded]);

  const loadMoreGenie = async (token: string) => {
    try {
      const { options, next_page_token } = await listGenieMcpSpaces(
        genieSearch || undefined,
        token,
      );
      setGenieOptions((prev) => [...prev, ...options]);
      setGenieNextToken(next_page_token);
    } catch {
      setError('Could not load more Genie spaces');
    }
  };

  const selectDatabricksOption = async (option: DatabricksMcpOption) => {
    const name = databricksMcpServerName(option);
    if (selected.includes(name)) {
      toggle(name); // deselect — no registration round-trip needed
      return;
    }
    setBusyOption(option.id);
    setError(null);
    try {
      const serverName = await ensureDatabricksMcpServer(option);
      toggle(serverName);
      // The registration may have created a new Kasal server — refresh the list.
      setKasalServers(await listKasalMcpServers());
    } catch (e) {
      const response = (e as { response?: { status?: number } }).response;
      setError(
        response && response.status === 403
          ? 'Only admins can register Databricks MCP servers'
          : 'Could not register the Databricks MCP server',
      );
    } finally {
      setBusyOption(null);
    }
  };

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

  const chevron = (expanded: boolean) => (
    <svg
      className={`w-3 h-3 flex-shrink-0 transition-transform ${expanded ? 'rotate-90' : ''}`}
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={2}
    >
      <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
    </svg>
  );

  const optionRow = (option: DatabricksMcpOption, indent = false) => {
    const name = databricksMcpServerName(option);
    const isSelected = selected.includes(name);
    const busy = busyOption === option.id;
    return (
      <button
        key={option.id}
        type="button"
        role="menuitemcheckbox"
        aria-checked={isSelected}
        disabled={busy}
        onClick={() => void selectDatabricksOption(option)}
        title={option.description || option.server_url}
        className={`w-full flex items-center gap-2 ${indent ? 'pl-9' : 'pl-3'} pr-3 py-1.5 text-left text-xs transition-colors hover:opacity-80 disabled:opacity-60`}
        style={{ color: 'var(--text-primary)' }}
      >
        {check(isSelected)}
        <span className="truncate flex-1">{option.name}</span>
        <span className="text-[10px] uppercase flex-shrink-0" style={{ color: 'var(--text-muted)' }}>
          {busy ? '…' : option.kind}
        </span>
      </button>
    );
  };

  // Top-level filter: the combined list (Kasal + external + managed types) can
  // be long, so it's searchable by name. Genie spaces / AI Search indexes keep
  // their own (server-side) search inside the drill-down.
  const query = filter.trim().toLowerCase();
  const nameMatches = (name: string) => !query || name.toLowerCase().includes(query);
  const kasalList = kasalServers ?? [];
  const visibleKasal = kasalList.filter((s) => nameMatches(s.name));
  const visibleExternal = (catalog?.external ?? []).filter(
    (o) => !isRegisteredInKasal(o, kasalList) && nameMatches(o.name),
  );
  const visibleManaged = (catalog?.managed ?? []).filter((t) => nameMatches(t.name));

  const drillRow = (kind: 'genie' | 'ai-search', label: string) => (
    <button
      type="button"
      onClick={() => setExpandedType((t) => (t === kind ? null : kind))}
      aria-expanded={expandedType === kind}
      className="w-full flex items-center gap-2 px-3 py-1.5 text-left text-xs font-medium transition-colors hover:opacity-80"
      style={{ color: 'var(--text-secondary)' }}
    >
      {chevron(expandedType === kind)}
      {label}
    </button>
  );

  return (
    <div ref={rootRef} className="relative flex-shrink-0">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        disabled={disabled}
        className="relative w-8 h-8 rounded-xl flex items-center justify-center transition-colors hover:opacity-80 disabled:opacity-40 disabled:cursor-not-allowed"
        style={{
          color: open || selected.length > 0 ? 'var(--accent)' : 'var(--text-secondary)',
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
        {selected.length > 0 && (
          <span
            className="absolute -top-1 -right-1 text-[9px] tabular-nums rounded-full min-w-[14px] h-[14px] flex items-center justify-center px-0.5"
            style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
          >
            {selected.length}
          </span>
        )}
      </button>

      {open && (
        <div
          role="menu"
          aria-label="MCP picker"
          className="absolute bottom-full right-0 mb-2 w-80 rounded-xl overflow-hidden z-20"
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

          <div className="max-h-80 overflow-y-auto pb-1">
            {kasalServers === null || catalog === null ? (
              <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
            ) : kasalServers.length === 0 &&
              catalog.external.length === 0 &&
              catalog.managed.length === 0 ? (
              <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>
                No MCP servers available
              </div>
            ) : visibleKasal.length === 0 &&
              visibleExternal.length === 0 &&
              visibleManaged.length === 0 ? (
              <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>
                No matching MCP servers
              </div>
            ) : (
              <>
                {/* Kasal-configured servers */}
                {visibleKasal.map((server) => {
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
                      className="w-full flex items-center gap-2 px-3 py-1.5 text-left text-xs transition-colors hover:opacity-80 disabled:opacity-40 disabled:cursor-not-allowed"
                      style={{ color: 'var(--text-primary)' }}
                    >
                      {check(isSelected)}
                      <span className="truncate flex-1">{server.name}</span>
                      {!server.enabled && (
                        <span className="text-[10px] flex-shrink-0" style={{ color: 'var(--text-muted)' }}>disabled</span>
                      )}
                    </button>
                  );
                })}

                {/* Databricks external (UC connection) servers, minus the ones
                    already registered in Kasal above. */}
                {visibleExternal.map((option) => optionRow(option))}

                {/* Databricks managed types: leaves select directly, Genie and
                    AI Search drill into their instances. */}
                {visibleManaged.map((managedType) =>
                  managedType.expandable ? (
                    <React.Fragment key={managedType.id}>
                      {drillRow(
                        managedType.kind as 'genie' | 'ai-search',
                        managedType.name,
                      )}
                      {expandedType === managedType.kind && managedType.kind === 'genie' && (
                        <>
                          <div className="pl-9 pr-3 py-1">
                            <input
                              value={genieSearch}
                              onChange={(e) => setGenieSearch(e.target.value)}
                              placeholder="Search Genie spaces…"
                              aria-label="Search Genie spaces"
                              className="w-full rounded-md px-2 py-1 text-xs outline-none"
                              style={{
                                backgroundColor: 'var(--bg-input)',
                                color: 'var(--text-primary)',
                                border: '1px solid var(--border-color)',
                              }}
                            />
                          </div>
                          {!genieLoaded ? (
                            <div className="pl-9 py-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
                          ) : genieOptions.length === 0 ? (
                            <div className="pl-9 py-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>No spaces found</div>
                          ) : (
                            <>
                              {genieOptions.map((option) => optionRow(option, true))}
                              {genieNextToken && (
                                <button
                                  type="button"
                                  onClick={() => void loadMoreGenie(genieNextToken)}
                                  className="w-full pl-9 pr-3 py-1.5 text-left text-xs font-medium transition-colors hover:opacity-80"
                                  style={{ color: 'var(--accent)' }}
                                >
                                  Load more…
                                </button>
                              )}
                            </>
                          )}
                        </>
                      )}
                      {expandedType === managedType.kind && managedType.kind === 'ai-search' && (
                        !aiSearchLoaded ? (
                          <div className="pl-9 py-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
                        ) : aiSearchOptions.length === 0 ? (
                          <div className="pl-9 py-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>No indexes found</div>
                        ) : (
                          aiSearchOptions.map((option) => optionRow(option, true))
                        )
                      )}
                    </React.Fragment>
                  ) : (
                    [managedLeafOption(managedType)]
                      .filter((option) => !isRegisteredInKasal(option, kasalServers))
                      .map((option) => optionRow(option))
                  ),
                )}
              </>
            )}
          </div>

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
        </div>
      )}
    </div>
  );
};

export default McpPicker;
