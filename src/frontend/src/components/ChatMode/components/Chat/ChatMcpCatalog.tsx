import React, { useEffect, useState } from 'react';
import {
  MCPService,
  type DatabricksMcpOption,
  type DatabricksManagedMcpType,
  type DatabricksMcpCatalog as Catalog,
} from '../../../../api/MCPService';

/**
 * Chat-native Databricks MCP catalog picker (the "Add server" → Databricks tab in
 * ChatMcpDialog). Lists what the workspace exposes — external UC-connection MCPs,
 * managed leaves (e.g. Databricks SQL, Unity Catalog functions), and expandable
 * types (Genie spaces, AI Search indexes) that drill into a searchable list.
 * Registering an option calls MCPService.ensureDatabricksServer at the given scope
 * (idempotent), then asks the parent to reload so the new server shows in the list.
 *
 * Styled with chat tokens; buttons set padding inline (the #kasal-chat-root reset
 * zeroes Tailwind px/py).
 */
export interface ChatMcpCatalogProps {
  /** 'global' registers a base server (system admin); 'workspace' a scoped one. */
  scope: 'global' | 'workspace';
  /** Reload the parent server list after a successful registration. */
  onRegistered: () => Promise<void> | void;
}

const managedLeafOption = (t: DatabricksManagedMcpType): DatabricksMcpOption => ({
  id: t.id,
  kind: t.kind,
  name: t.name,
  description: t.description,
  server_url: t.server_url || '',
});

const ChatMcpCatalog: React.FC<ChatMcpCatalogProps> = ({ scope, onRegistered }) => {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [done, setDone] = useState<Set<string>>(new Set());
  const [expanded, setExpanded] = useState<'genie' | 'ai-search' | null>(null);
  const [genieSearch, setGenieSearch] = useState('');
  const [genieOptions, setGenieOptions] = useState<DatabricksMcpOption[] | null>(null);
  const [aiSearchOptions, setAiSearchOptions] = useState<DatabricksMcpOption[] | null>(null);

  useEffect(() => {
    let cancelled = false;
    MCPService.getInstance()
      .getDatabricksCatalog()
      .then((c) => { if (!cancelled) setCatalog(c); })
      .catch((e: unknown) => {
        if (!cancelled) {
          setCatalog({ workspace_url: '', external: [], managed: [] });
          setError(e instanceof Error ? e.message : 'Could not load Databricks MCPs');
        }
      });
    return () => { cancelled = true; };
  }, []);

  // Genie spaces (searchable, first page).
  useEffect(() => {
    if (expanded !== 'genie') return;
    let cancelled = false;
    setGenieOptions(null);
    MCPService.getInstance()
      .listGenieSpaces(genieSearch || undefined)
      .then(({ options }) => { if (!cancelled) setGenieOptions(options); })
      .catch(() => { if (!cancelled) setGenieOptions([]); });
    return () => { cancelled = true; };
  }, [expanded, genieSearch]);

  // AI Search indexes (loaded once on expand).
  useEffect(() => {
    if (expanded !== 'ai-search' || aiSearchOptions !== null) return;
    let cancelled = false;
    MCPService.getInstance()
      .listAiSearchIndexes()
      .then((options) => { if (!cancelled) setAiSearchOptions(options); })
      .catch(() => { if (!cancelled) setAiSearchOptions([]); });
    return () => { cancelled = true; };
  }, [expanded, aiSearchOptions]);

  const register = async (option: DatabricksMcpOption) => {
    setBusyId(option.id);
    setError(null);
    try {
      await MCPService.getInstance().ensureDatabricksServer(option, scope);
      setDone((d) => new Set(d).add(option.id));
      await onRegistered();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to add server');
    } finally {
      setBusyId(null);
    }
  };

  const optionRow = (option: DatabricksMcpOption, kindLabel?: string) => {
    const added = done.has(option.id);
    return (
      <div
        key={option.id}
        className="flex items-center gap-2 rounded-lg mb-1.5"
        style={{ padding: '8px 10px', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-[13px] font-medium truncate" style={{ color: 'var(--text-primary)' }}>{option.name}</span>
            {kindLabel && (
              <span className="text-[9px] uppercase tracking-wide flex-shrink-0 rounded" style={{ padding: '1px 5px', color: 'var(--text-muted)', border: '1px solid var(--border-color)' }}>
                {kindLabel}
              </span>
            )}
          </div>
          {option.description && (
            <div className="text-[11px] truncate mt-0.5" style={{ color: 'var(--text-muted)' }}>{option.description}</div>
          )}
        </div>
        <button
          type="button"
          onClick={() => register(option)}
          disabled={busyId === option.id || added}
          className="text-xs font-medium rounded-lg flex-shrink-0 transition-colors disabled:opacity-60"
          style={{
            padding: '6px 10px',
            color: added ? 'var(--text-muted)' : 'var(--text-primary)',
            border: '1px solid var(--border-color)',
            backgroundColor: added ? 'transparent' : 'var(--bg-primary)',
          }}
        >
          {added ? 'Added' : busyId === option.id ? 'Adding…' : 'Add'}
        </button>
      </div>
    );
  };

  const drillRow = (kind: 'genie' | 'ai-search', label: string, count?: number) => (
    <button
      key={kind}
      type="button"
      onClick={() => setExpanded(kind)}
      className="w-full flex items-center gap-2 rounded-lg mb-1.5 text-left transition-colors hover:bg-[var(--bg-rail-hover)]"
      style={{ padding: '8px 10px', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
    >
      <span className="flex-1 text-[13px] font-medium" style={{ color: 'var(--text-primary)' }}>{label}</span>
      {typeof count === 'number' && count > 0 && (
        <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>{count}</span>
      )}
      <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--text-muted)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
      </svg>
    </button>
  );

  // Drill-in view (Genie spaces / AI Search indexes).
  if (expanded) {
    const isGenie = expanded === 'genie';
    const opts = isGenie ? genieOptions : aiSearchOptions;
    return (
      <div>
        <div className="flex items-center gap-2 mb-2">
          <button
            type="button"
            onClick={() => setExpanded(null)}
            className="flex items-center gap-1 text-xs font-medium rounded-lg transition-colors hover:bg-[var(--bg-rail-hover)]"
            style={{ padding: '5px 8px', color: 'var(--text-secondary)' }}
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
            </svg>
            Back
          </button>
          <span className="text-xs font-semibold" style={{ color: 'var(--text-primary)' }}>
            {isGenie ? 'Genie spaces' : 'AI Search indexes'}
          </span>
        </div>
        {isGenie && (
          <input
            value={genieSearch}
            onChange={(e) => setGenieSearch(e.target.value)}
            placeholder="Search Genie spaces…"
            className="mb-2"
            style={{ padding: '7px 10px', backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)', borderRadius: 8, fontSize: 13, width: '100%', outline: 'none' }}
          />
        )}
        {opts === null ? (
          <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
        ) : opts.length === 0 ? (
          <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>Nothing found.</div>
        ) : (
          opts.map((o) => optionRow(o))
        )}
        {error && <div className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
      </div>
    );
  }

  const external = catalog?.external ?? [];
  const managed = catalog?.managed ?? [];

  return (
    <div>
      {catalog === null ? (
        <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>Loading catalog…</div>
      ) : external.length === 0 && managed.length === 0 ? (
        <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>
          No Databricks MCP servers found in this workspace.
        </div>
      ) : (
        <>
          {external.map((o) => optionRow(o, 'external'))}
          {managed.map((mt) =>
            mt.expandable && (mt.kind === 'genie' || mt.kind === 'ai-search')
              ? drillRow(mt.kind, mt.name)
              : optionRow(managedLeafOption(mt), mt.kind),
          )}
        </>
      )}
      {error && <div className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
    </div>
  );
};

export default ChatMcpCatalog;
