import React, { useState } from 'react';
import { CatalogItem } from '../api/crews';

interface CatalogLibraryProps {
  crews: CatalogItem[];
  flows: CatalogItem[];
  onLoadCrew: (name: string) => void;
  onLoadFlow: (name: string) => void;
}

type LibraryEntry = CatalogItem & { kind: 'crew' | 'flow' };

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
    style={{ transform: open ? 'rotate(90deg)' : 'none' }}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2.5}
  >
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
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
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');

  const entries: LibraryEntry[] = [
    ...crews.map((c) => ({ ...c, kind: 'crew' as const })),
    ...flows.map((f) => ({ ...f, kind: 'flow' as const })),
  ];
  if (entries.length === 0) return null;

  const q = query.trim().toLowerCase();
  const filtered = q ? entries.filter((e) => e.name.toLowerCase().includes(q)) : entries;
  const showSearch = entries.length > 6;

  return (
    <div className="px-2 pt-2">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-1.5 px-3 py-1.5 rounded-lg transition-colors hover:bg-[var(--bg-rail-hover)]"
        style={{ color: 'var(--text-muted)' }}
        aria-expanded={open}
      >
        <Chevron open={open} />
        <span className="text-[10px] font-semibold uppercase tracking-wider flex-1 text-left">
          Catalog
        </span>
        <span className="text-[10px] tabular-nums" style={{ color: 'var(--text-muted)' }}>
          {entries.length}
        </span>
      </button>
      {open && (
        <div className="mt-0.5">
          {showSearch && (
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search catalog…"
              className="w-[calc(100%-1.5rem)] mx-3 mb-1 px-2 py-1 rounded-md text-[12px] outline-none"
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
            {filtered.map((item) => (
              <button
                key={`${item.kind}-${item.id}`}
                onClick={() => (item.kind === 'crew' ? onLoadCrew(item.name) : onLoadFlow(item.name))}
                title={`Open ${item.kind} “${item.name}”`}
                className="w-full flex items-center gap-2 pl-7 pr-3 py-1.5 my-0.5 rounded-lg text-left transition-colors hover:bg-[var(--bg-rail-hover)]"
                style={{ color: 'var(--text-secondary)' }}
              >
                {item.kind === 'crew' ? <CrewIcon /> : <FlowIcon />}
                <span className="truncate text-[13px]">{item.name}</span>
              </button>
            ))}
            {filtered.length === 0 && (
              <div className="pl-7 pr-3 py-1.5 text-[12px]" style={{ color: 'var(--text-muted)' }}>
                No matches
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default CatalogLibrary;
