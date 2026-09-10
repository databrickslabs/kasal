import React, { useEffect, useRef, useState } from 'react';
import {
  MCPService,
  databricksMcpServerName,
  type DatabricksMcpOption,
  type DatabricksManagedMcpType,
  type DatabricksMcpCatalog as Catalog,
} from '../../../../api/tools/MCPService';


/** Databricks discovery shared by Chat and builder tool pickers. */
export interface DatabricksMcpPickerProps {
  /** The owning picker registers, enables and applies its own selection. */
  onConnect: (option: DatabricksMcpOption) => Promise<string>;
  selectedNames: string[];
  selectedLabel?: string;
  /** Server URLs already registered (trailing slash stripped). Their catalog
   *  rows offer "Use in chat" so registration is not mistaken for selection. */
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

const DatabricksMcpPicker: React.FC<DatabricksMcpPickerProps> = ({ onConnect, selectedNames, registeredUrls, selectedLabel = 'Selected' }) => {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  const [reload, setReload] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [done, setDone] = useState<Record<string, string>>({});
  const selected = selectedNames;
  const connecting = useRef(false);
  const [catalogSearch, setCatalogSearch] = useState('');
  const [extPage, setExtPage] = useState(0);
  const [expanded, setExpanded] = useState<'genie' | 'ai-search' | 'functions' | null>(null);
  const [genieSearch, setGenieSearch] = useState('');
  const [genieNextToken, setGenieNextToken] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const genieRequest = useRef(0);
  const [genieOptions, setGenieOptions] = useState<DatabricksMcpOption[] | null>(null);
  const [aiSearchOptions, setAiSearchOptions] = useState<DatabricksMcpOption[] | null>(null);
  const [functionsSearch, setFunctionsSearch] = useState('');
  const [functionsOptions, setFunctionsOptions] = useState<DatabricksMcpOption[] | null>(null);
  const [functionsCatalogs, setFunctionsCatalogs] = useState<string[]>([]);
  const [functionsCatalog, setFunctionsCatalog] = useState<string | undefined>(undefined);
  const [defaultCatalog, setDefaultCatalog] = useState('');
  const [functionsSchema, setFunctionsSchema] = useState<DatabricksMcpOption | null>(null);
  const [functionNameSearch, setFunctionNameSearch] = useState('');
  const [schemaFunctions, setSchemaFunctions] = useState<
    Array<{ name: string; comment: string | null }> | null
  >(null);

  useEffect(() => {
    let cancelled = false;
    setCatalog(null);
    setLoadFailed(false);
    setError(null);
    MCPService.getInstance()
      .getDatabricksCatalog()
      .then((c) => { if (!cancelled) setCatalog(c); })
      .catch((e: unknown) => {
        if (!cancelled) {
          setLoadFailed(true);
          setCatalog({ workspace_url: '', external: [], managed: [] });
          setError(e instanceof Error ? e.message : 'Could not load Databricks MCPs');
        }
      });
    return () => { cancelled = true; };
  }, [reload]);

  // Search on demand; debounce typing and retain access to every result page.
  useEffect(() => {
    if (expanded !== 'genie') return;
    const version = ++genieRequest.current;
    setGenieOptions(null);
    setGenieNextToken(null);
    setLoadingMore(false);
    const timer = window.setTimeout(() => {
      MCPService.getInstance().listGenieSpaces(genieSearch || undefined)
        .then(({ options, next_page_token }) => {
          if (version !== genieRequest.current) return;
          setGenieOptions(options);
          setGenieNextToken(next_page_token);
        })
        .catch(() => {
          if (version !== genieRequest.current) return;
          setGenieOptions([]);
          setError('Could not load Genie spaces. Try your search again.');
        });
    }, genieSearch ? 250 : 0);
    return () => { genieRequest.current = version + 1; window.clearTimeout(timer); };
  }, [expanded, genieSearch]);

  const loadMoreGenie = async () => {
    if (!genieNextToken || loadingMore) return;
    const version = genieRequest.current;
    setLoadingMore(true);
    try {
      const { options, next_page_token } = await MCPService.getInstance().listGenieSpaces(genieSearch || undefined, genieNextToken);
      if (version !== genieRequest.current) return;
      setGenieOptions((previous) => [...new Map([...(previous ?? []), ...options].map((o) => [o.id, o])).values()]);
      setGenieNextToken(next_page_token);
    } catch {
      if (version === genieRequest.current) setError('Could not load more Genie spaces. Try again.');
    } finally {
      if (version === genieRequest.current) setLoadingMore(false);
    }
  };

  // AI Search indexes (loaded once on expand).
  useEffect(() => {
    if (expanded !== 'ai-search' || aiSearchOptions !== null) return;
    let cancelled = false;
    MCPService.getInstance()
      .listAiSearchIndexes()
      .then((options) => { if (!cancelled) setAiSearchOptions(options); })
      .catch(() => { if (!cancelled) { setAiSearchOptions([]); setError('Could not load AI Search indexes.'); } });
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
          setDefaultCatalog(selected_catalog);
        }
      })
      .catch(() => { if (!cancelled) { setFunctionsOptions([]); setError('Could not load function schemas. Try another catalog or search.'); } });
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
      .catch(() => { if (!cancelled) { setSchemaFunctions([]); setError('Could not load functions.'); } });
    return () => { cancelled = true; };
  }, [functionsSchema, functionNameSearch]);

  const register = async (option: DatabricksMcpOption) => {
    if (connecting.current) return;
    connecting.current = true;
    setBusyId(option.id);
    setError(null);
    try {
      const connectedName = await onConnect(option);
      setDone((d) => ({ ...d, [option.id]: connectedName }));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to add server');
    } finally {
      connecting.current = false;
      setBusyId(null);
    }
  };

  const optionRow = (option: DatabricksMcpOption, kindLabel?: string, onView?: () => void) => {
    const added = selected.includes(done[option.id] || databricksMcpServerName(option));
    const registered = !!option.server_url && !!registeredUrls?.has(stripTrailingSlash(option.server_url));
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
          disabled={busyId !== null || added}
          className="text-xs font-medium rounded-lg flex-shrink-0 transition-colors disabled:opacity-60"
          style={{
            padding: '6px 10px',
            color: added ? 'var(--text-muted)' : 'var(--text-primary)',
            border: '1px solid var(--border-color)',
            backgroundColor: added ? 'transparent' : 'var(--bg-primary)',
          }}
        >
          {added ? selectedLabel : busyId === option.id ? 'Connecting…' : registered ? 'Use' : 'Connect'}
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

  const categoryNavigation = (
    <div className="flex flex-wrap gap-1 mb-3" aria-label="Databricks categories">
      <button type="button" onClick={() => { setExpanded(null); setFunctionsSchema(null); setError(null); }}
        style={{ ...fnInputStyle, width: 'auto', fontSize: 12 }}>All servers</button>
      {(catalog?.managed ?? []).filter((m) => m.expandable).map((m) => (
        <button key={m.id} type="button" aria-pressed={expanded === m.kind}
          onClick={() => { setExpanded(m.kind as 'genie' | 'ai-search' | 'functions'); setFunctionsSchema(null); setError(null); }}
          style={{ ...fnInputStyle, width: 'auto', fontSize: 12, backgroundColor: expanded === m.kind ? 'var(--bg-active-chip)' : 'var(--bg-input)' }}>
          {m.name}
        </button>
      ))}
    </div>
  );

  // Second-level view: the individual functions of a chosen schema (visibility
  // only — enabling the schema server exposes all of them).
  if (expanded === 'functions' && functionsSchema) {
    return (
      <div>
        {categoryNavigation}
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
        {error && <div role="alert" className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
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
        {categoryNavigation}
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
                value={functionsCatalog ?? defaultCatalog}
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
        {expanded === 'genie' && genieNextToken && (
          <button type="button" onClick={() => void loadMoreGenie()} disabled={loadingMore}
            style={{ ...fnInputStyle, marginTop: 8 }}>
            {loadingMore ? 'Loading…' : 'Load more Genie spaces'}
          </button>
        )}
        {error && <div role="alert" className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
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
      ) : loadFailed ? (
        <button type="button" onClick={() => setReload((n) => n + 1)} style={fnInputStyle}>Retry Databricks discovery</button>
      ) : allExternal.length === 0 && allManaged.length === 0 ? (
        <div className="py-6 text-center text-xs" style={{ color: 'var(--text-muted)' }}>
          No Databricks servers were returned. Check your workspace connection or retry discovery.
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
      {error && <div role="alert" className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
    </div>
  );
};

export default DatabricksMcpPicker;
