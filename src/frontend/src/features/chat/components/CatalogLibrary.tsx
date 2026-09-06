import React, { useState } from 'react';
import { CatalogItem } from '../api/crews';
import { useAppStore } from '../store/appStore';

interface CatalogLibraryProps {
  crews: CatalogItem[];
  flows: CatalogItem[];
  onLoadCrew: (name: string) => void;
  onLoadFlow: (name: string) => void;
}

type LibraryEntry = CatalogItem & { kind: 'crew' | 'flow' };

/** Unsearched rows rendered at most — past this, the footer says to search. */
const RENDER_CAP = 50;

/**
 * The collapsible "Catalog" section in the chat rail: the crews and flows
 * PUBLISHED TO CHAT, each tagged with its own icon, so they can be browsed and
 * opened with a click (replaces /list crews & flows).
 *
 * Published-only, not every saved crew. This rail sits beside a composer whose
 * "Use existing" control routes to exactly this set, and a list that showed
 * more than the router can reach would advertise things a prompt can never
 * select. Publishing to chat is what puts something here.
 */
const Chevron: React.FC<{ open: boolean }> = ({ open }) => (
  <svg
    className="w-3 h-3 flex-shrink-0 transition-transform"
    style={{ transform: open ? 'rotate(180deg)' : 'none' }}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2.5}
  >
    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
  </svg>
);

// Crew = a small team (people); Flow = connected nodes (a pipeline).
const CrewIcon: React.FC = () => (
  <svg className="w-3.5 h-3.5 flex-shrink-0 opacity-80" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
    <circle cx="9" cy="8" r="3" />
    <path strokeLinecap="round" strokeLinejoin="round" d="M3.5 19a5.5 5.5 0 0111 0M16 11a3 3 0 100-6M19.5 19a5.5 5.5 0 00-3.5-5.1" />
  </svg>
);

const FlowIcon: React.FC = () => (
  <svg className="w-3.5 h-3.5 flex-shrink-0 opacity-80" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
    <rect x="3" y="4" width="6" height="5" rx="1.5" />
    <rect x="15" y="15" width="6" height="5" rx="1.5" />
    <path strokeLinecap="round" strokeLinejoin="round" d="M6 9v4a3 3 0 003 3h6" />
  </svg>
);

const CatalogLibrary: React.FC<CatalogLibraryProps> = ({ crews, flows, onLoadCrew, onLoadFlow }) => {
  // Expansion lives in the app store so the collapsed rail's catalog icon can
  // open the sidebar with this section already expanded.
  const open = useAppStore((s) => s.catalogOpen);
  const setOpen = useAppStore((s) => s.setCatalogOpen);
  const [query, setQuery] = useState('');

  const entries: LibraryEntry[] = [
    ...crews.map((c) => ({ ...c, kind: 'crew' as const })),
    ...flows.map((f) => ({ ...f, kind: 'flow' as const })),
  ];
  if (entries.length === 0) return null;

  const q = query.trim().toLowerCase();
  const filtered = q ? entries.filter((e) => e.name.toLowerCase().includes(q)) : entries;
  const showSearch = entries.length > 6;
  // The backend list is unpaginated (every published crew/flow arrives), but a
  // rail with hundreds of DOM rows helps no one scan — cap the unsearched view
  // and SAY so, pointing at the search box. A query lifts the cap: filtered
  // results are what the user asked for.
  const capped = !q && filtered.length > RENDER_CAP;
  const visibleEntries = capped ? filtered.slice(0, RENDER_CAP) : filtered;

  // Flat, flush with the rail — a header row styled exactly like RECENT below
  // it, no card chrome. (A bordered grey card was tried and read as heavier
  // than the content it holds.)
  return (
    <div className="pt-4">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-1.5 !px-3 !py-1.5 text-left transition-colors hover:bg-[var(--bg-rail-hover)]"
        style={{ color: 'var(--text-muted)' }}
        aria-expanded={open}
      >
        <span className="text-[11px] font-semibold uppercase tracking-[0.08em] flex-1">
          Catalog
        </span>
        <span className="text-[10px] tabular-nums">{entries.length}</span>
        <Chevron open={open} />
      </button>
      {open && (
        <div className="px-2 pt-1">
          {showSearch && (
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search catalog…"
              className="w-[calc(100%-0.5rem)] mx-1 mb-1 px-2 py-1.5 rounded-md text-[12px] outline-none"
              style={{
                backgroundColor: 'var(--bg-input)',
                color: 'var(--text-primary)',
                border: '1px solid var(--border-color)',
              }}
            />
          )}
          {/* Capped + scrollable so the catalog never pushes the chat session
              list (Recent) off-screen. */}
          <div className="max-h-52 overflow-y-auto">
            {visibleEntries.map((item) => (
              <button
                key={`${item.kind}-${item.id}`}
                onClick={() => (item.kind === 'crew' ? onLoadCrew(item.name) : onLoadFlow(item.name))}
                className="w-full flex items-center gap-2.5 !px-3 !py-1.5 my-0.5 rounded-lg text-left transition-colors hover:bg-[var(--bg-rail-hover)]"
                style={{ color: 'var(--text-secondary)' }}
              >
                {item.kind === 'crew' ? <CrewIcon /> : <FlowIcon />}
                <span className="truncate flex-1 text-[13px]" style={{ color: 'var(--text-primary)' }}>
                  {item.name}
                </span>
              </button>
            ))}
            {filtered.length === 0 && (
              <div className="px-2.5 py-1.5 text-[12px]" style={{ color: 'var(--text-muted)' }}>
                No matches
              </div>
            )}
            {capped && (
              <div className="px-3 py-1.5 text-[11px]" style={{ color: 'var(--text-muted)' }}>
                Showing {RENDER_CAP} of {filtered.length} — search to narrow
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default CatalogLibrary;
