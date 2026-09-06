import React, { useEffect, useState } from 'react';
import {
  MCPService,
  type DatabricksMcpOption,
  type DatabricksManagedMcpType,
  type DatabricksMcpCatalog as Catalog,
} from '../../../../api/tools/MCPService';

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
  /** Server URLs already registered (trailing slash stripped). Their catalog
   *  rows show "Added" and disable the button instead of offering "Add". */
  registeredUrls?: Set<string>;
}

const stripTrailingSlash = (u?: string): string => (u || '').replace(/\/+$/, '');

// Connections can number in the dozens — page them so the list length (and the
// dialog) stays bounded. Managed types are few and always shown in full.
const EXTERNAL_PAGE_SIZE = 5;

const managedLeafOption = (t: DatabricksManagedMcpType): DatabricksMcpOption => ({
  id: t.id,
  kind: t.kind,
  name: t.name,
  description: t.description,
  server_url: t.server_url || '',
});

const ChatMcpCatalog: React.FC<ChatMcpCatalogProps> = ({ scope, onRegistered, registeredUrls }) => {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [done, setDone] = useState<Set<string>>(new Set());
  const [catalogSearch, setCatalogSearch] = useState('');
  const [extPage, setExtPage] = useState(0);
  const [expanded, setExpanded] = useState<'genie' | 'ai-search' | 'functions' | null>(null);
  const [genieSearch, setGenieSearch] = useState('');
  const [genieOptions, setGenieOptions] = useState<DatabricksMcpOption[] | null>(null);
  const [aiSearchOptions, setAiSearchOptions] = useState<DatabricksMcpOption[] | null>(null);
  const [functionsSearch, setFunctionsSearch] = useState('');
  const [functionsOptions, setFunctionsOptions] = useState<DatabricksMcpOption[] | null>(null);
  const [functionsCatalogs, setFunctionsCatalogs] = useState<string[]>([]);
  const [functionsCatalog, setFunctionsCatalog] = useState<string | undefined>(undefined);
  const [functionsSchema, setFunctionsSchema] = useState<DatabricksMcpOption | null>(null);
  const [functionNameSearch, setFunctionNameSearch] = useState('');
  const [schemaFunctions, setSchemaFunctions] = useState<
    Array<{ name: string; comment: string | null }> | null
  >(null);

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

  // Function schemas (searchable; reloads on catalog switch).
  useEffect(() => {
    if (expanded !== 'functions') return;
    let cancelled = false;
    setFunctionsOptions(null);
    MCPService.getInstance()
      .listFunctionSchemas(functionsCatalog, functionsSearch || undefined)
      .then(({ options, catalogs, selected_catalog }) => {
        if (cancelled) return;
        setFunctionsOptions(options);
        setFunctionsCatalogs(catalogs);
        if (functionsCatalog === undefined && selected_catalog) {
          setFunctionsCatalog(selected_catalog);
        }
      })
      .catch(() => { if (!cancelled) setFunctionsOptions([]); });
    return () => { cancelled = true; };
  }, [expanded, functionsSearch, functionsCatalog]);

  // A chosen schema's individual functions (visibility only).
  useEffect(() => {
    if (!functionsSchema) return;
    const m = (functionsSchema.server_url || '').match(
      /\/api\/2\.0\/mcp\/functions\/([^/]+)\/([^/?]+)/,
    );
    if (!m) { setSchemaFunctions([]); return; }
    let cancelled = false;
    setSchemaFunctions(null);
    MCPService.getInstance()
      .listSchemaFunctions(m[1], m[2], functionNameSearch || undefined)
      .then((fns) => { if (!cancelled) setSchemaFunctions(fns); })
      .catch(() => { if (!cancelled) setSchemaFunctions([]); });
    return () => { cancelled = true; };
  }, [functionsSchema, functionNameSearch]);

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

  const optionRow = (option: DatabricksMcpOption, kindLabel?: string, onView?: () => void) => {
    // "Added" if we registered it this session OR it's already a registered
    // server (matched by URL) — so e.g. Genie One shows Added, not Add.
    const added =
      done.has(option.id) ||
      (!!option.server_url && !!registeredUrls?.has(stripTrailingSlash(option.server_url)));
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
        {onView && (
          <button
            type="button"
            onClick={onView}
            className="text-xs font-medium rounded-lg flex-shrink-0 transition-colors"
            style={{ padding: '6px 10px', color: 'var(--text-secondary)', border: '1px solid var(--border-color)', backgroundColor: 'transparent' }}
          >
            Functions ›
          </button>
        )}
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

  const drillRow = (kind: 'genie' | 'ai-search' | 'functions', label: string, count?: number) => (
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

  const fnInputStyle = { padding: '7px 10px', backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)', borderRadius: 8, fontSize: 13, width: '100%', outline: 'none' } as const;

  // Second-level view: the individual functions of a chosen schema (visibility
  // only — enabling the schema server exposes all of them).
  if (expanded === 'functions' && functionsSchema) {
    return (
      <div>
        <div className="flex items-center gap-2 mb-2">
          <button
            type="button"
            onClick={() => { setFunctionsSchema(null); setFunctionNameSearch(''); setSchemaFunctions(null); }}
            className="flex items-center gap-1 text-xs font-medium rounded-lg transition-colors hover:bg-[var(--bg-rail-hover)]"
            style={{ padding: '5px 8px', color: 'var(--text-secondary)' }}
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
            </svg>
            Schemas
          </button>
          <span className="text-xs font-semibold truncate" style={{ color: 'var(--text-primary)' }}>
            {functionsSchema.name}
          </span>
        </div>
        {/* Enabling still registers the whole schema server. */}
        {optionRow(functionsSchema)}
        <div className="text-[11px] mb-2 mt-0.5" style={{ color: 'var(--text-muted)' }}>
          Enabling the server above adds all of these functions — they aren&apos;t selected individually.
        </div>
        <input
          value={functionNameSearch}
          onChange={(e) => setFunctionNameSearch(e.target.value)}
          placeholder="Search functions…"
          className="mb-2"
          style={fnInputStyle}
        />
        {schemaFunctions === null ? (
          <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
        ) : schemaFunctions.length === 0 ? (
          <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>No functions found.</div>
        ) : (
          schemaFunctions.map((fn) => (
            <div key={fn.name} className="rounded-lg mb-1" style={{ padding: '6px 10px', border: '1px solid var(--border-color)' }}>
              <div className="text-[13px] truncate" style={{ color: 'var(--text-primary)' }}>{fn.name}</div>
              {fn.comment && (
                <div className="text-[11px] truncate mt-0.5" style={{ color: 'var(--text-muted)' }}>{fn.comment}</div>
              )}
            </div>
          ))
        )}
        {error && <div className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
      </div>
    );
  }

  // Drill-in view (Genie spaces / AI Search indexes / UC Function schemas).
  if (expanded) {
    const opts =
      expanded === 'genie'
        ? genieOptions
        : expanded === 'functions'
          ? functionsOptions
          : aiSearchOptions;
    const title =
      expanded === 'genie'
        ? 'Genie spaces'
        : expanded === 'functions'
          ? 'UC Function schemas'
          : 'AI Search indexes';
    const inputStyle = { padding: '7px 10px', backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)', borderRadius: 8, fontSize: 13, width: '100%', outline: 'none' } as const;
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
            {title}
          </span>
        </div>
        {expanded === 'genie' && (
          <input
            value={genieSearch}
            onChange={(e) => setGenieSearch(e.target.value)}
            placeholder="Search Genie spaces…"
            className="mb-2"
            style={inputStyle}
          />
        )}
        {expanded === 'functions' && (
          <>
            {functionsCatalogs.length > 0 && (
              <select
                value={functionsCatalog ?? ''}
                onChange={(e) => setFunctionsCatalog(e.target.value || undefined)}
                aria-label="Select catalog"
                className="mb-2"
                style={inputStyle}
              >
                {functionsCatalogs.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            )}
            <input
              value={functionsSearch}
              onChange={(e) => setFunctionsSearch(e.target.value)}
              placeholder="Search catalog.schema…"
              className="mb-2"
              style={inputStyle}
            />
          </>
        )}
        {opts === null ? (
          <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
        ) : opts.length === 0 ? (
          <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>Nothing found.</div>
        ) : (
          opts.map((o) =>
            optionRow(
              o,
              undefined,
              expanded === 'functions'
                ? () => { setFunctionsSchema(o); setFunctionNameSearch(''); setSchemaFunctions(null); }
                : undefined,
            ),
          )
        )}
        {error && <div className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
      </div>
    );
  }

  const allExternal = catalog?.external ?? [];
  const allManaged = catalog?.managed ?? [];
  const q = catalogSearch.trim().toLowerCase();
  // Managed types are a short, fixed set — always shown in full. The search box
  // (placed above Connections) filters only the long connections list.
  // Directly-selectable leaves (SQL, Genie One — the "Add" rows) first, then the
  // expandable drill categories (Functions, Genie, AI Search). Stable sort keeps
  // each group in its catalog order.
  const managed = allManaged
    .slice()
    .sort((a, b) => Number(Boolean(a.expandable)) - Number(Boolean(b.expandable)));
  const external = allExternal.filter((o) => !q || o.name.toLowerCase().includes(q));
  const sectionLabelCls = 'text-[10px] font-semibold uppercase tracking-wide mt-1 mb-1.5';
  const extPages = Math.max(1, Math.ceil(external.length / EXTERNAL_PAGE_SIZE));
  const extPageSafe = Math.min(extPage, extPages - 1);
  const pagedExternal = external.slice(
    extPageSafe * EXTERNAL_PAGE_SIZE,
    extPageSafe * EXTERNAL_PAGE_SIZE + EXTERNAL_PAGE_SIZE,
  );
  const pagerBtnCls = 'text-xs font-medium rounded-lg transition-colors hover:bg-[var(--bg-rail-hover)] disabled:opacity-40 disabled:cursor-not-allowed';

  return (
    <div>
      {catalog === null ? (
        <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>Loading catalog…</div>
      ) : allExternal.length === 0 && allManaged.length === 0 ? (
        <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>
          No Databricks MCP servers found in this workspace.
        </div>
      ) : (
        <>
          {/* Databricks-managed types first — the common picks (no search: it's
              a short, fixed set). */}
          {managed.length > 0 && (
            <div className={sectionLabelCls} style={{ color: 'var(--text-muted)' }}>Databricks managed</div>
          )}
          {managed.map((mt) =>
            mt.expandable &&
            (mt.kind === 'genie' || mt.kind === 'ai-search' || mt.kind === 'functions')
              ? drillRow(mt.kind, mt.name)
              : optionRow(managedLeafOption(mt), mt.kind),
          )}
          {/* Then connection-based (external) servers — searchable + paged. */}
          {allExternal.length > 0 && (
            <>
              <div className={sectionLabelCls} style={{ color: 'var(--text-muted)' }}>Connections</div>
              <input
                value={catalogSearch}
                onChange={(e) => { setCatalogSearch(e.target.value); setExtPage(0); }}
                placeholder="Search connections…"
                aria-label="Search connections"
                className="mb-2"
                style={fnInputStyle}
              />
            </>
          )}
          {pagedExternal.map((o) => optionRow(o, 'external'))}
          {external.length > EXTERNAL_PAGE_SIZE && (
            <div
              className="flex items-center justify-between"
              style={{
                color: 'var(--text-muted)',
                // Pinned to the bottom of the scroll area so Prev/Next are always
                // reachable without scrolling to the end of the list.
                position: 'sticky',
                bottom: 0,
                marginTop: 4,
                padding: '6px 2px',
                backgroundColor: 'var(--bg-primary)',
                borderTop: '1px solid var(--border-color)',
              }}
            >
              <button
                type="button"
                onClick={() => setExtPage((p) => Math.max(0, p - 1))}
                disabled={extPageSafe === 0}
                className={pagerBtnCls}
                style={{ padding: '4px 8px', color: 'var(--text-secondary)' }}
              >
                ‹ Prev
              </button>
              <span className="text-[11px]">
                {extPageSafe * EXTERNAL_PAGE_SIZE + 1}–{extPageSafe * EXTERNAL_PAGE_SIZE + pagedExternal.length} of {external.length}
              </span>
              <button
                type="button"
                onClick={() => setExtPage((p) => Math.min(extPages - 1, p + 1))}
                disabled={extPageSafe >= extPages - 1}
                className={pagerBtnCls}
                style={{ padding: '4px 8px', color: 'var(--text-secondary)' }}
              >
                Next ›
              </button>
            </div>
          )}
          {q && external.length === 0 && (
            <div className="py-4 text-center text-xs" style={{ color: 'var(--text-muted)' }}>
              No connections match “{catalogSearch}”.
            </div>
          )}
        </>
      )}
      {error && <div className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
    </div>
  );
};

export default ChatMcpCatalog;
