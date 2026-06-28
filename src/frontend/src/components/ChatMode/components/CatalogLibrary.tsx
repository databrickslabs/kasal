import React, { useState } from 'react';
import Box from '@mui/material/Box';
import { CatalogItem } from '../api/crews';
import { buttonResetSx, inputResetSx } from '../chatSx';

interface CatalogLibraryProps {
  crews: CatalogItem[];
  flows: CatalogItem[];
  onLoadCrew: (name: string) => void;
  onLoadFlow: (name: string) => void;
}

type LibraryEntry = CatalogItem & { kind: 'crew' | 'flow' };

/**
 * A single collapsible "Catalog" section in the chat rail listing saved crews
 * and flows together — each tagged with its own icon — so business users can
 * browse and open their saved work with a click (replaces /list crews & flows).
 */
const Chevron: React.FC<{ open: boolean }> = ({ open }) => (
  <Box
    component="svg"
    sx={{ width: 12, height: 12, flexShrink: 0, transition: 'transform 0.15s', transform: open ? 'rotate(90deg)' : 'none' }}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2.5}
  >
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
  </Box>
);

// Crew = a small team (people); Flow = connected nodes (a pipeline).
const CrewIcon: React.FC = () => (
  <Box component="svg" sx={{ width: 14, height: 14, flexShrink: 0, opacity: 0.8 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
    <circle cx="9" cy="8" r="3" />
    <path strokeLinecap="round" strokeLinejoin="round" d="M3.5 19a5.5 5.5 0 0111 0M16 11a3 3 0 100-6M19.5 19a5.5 5.5 0 00-3.5-5.1" />
  </Box>
);

const FlowIcon: React.FC = () => (
  <Box component="svg" sx={{ width: 14, height: 14, flexShrink: 0, opacity: 0.8 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
    <rect x="3" y="4" width="6" height="5" rx="1.5" />
    <rect x="15" y="15" width="6" height="5" rx="1.5" />
    <path strokeLinecap="round" strokeLinejoin="round" d="M6 9v4a3 3 0 003 3h6" />
  </Box>
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
    <Box sx={{ px: 1, pt: 1 }}>
      <Box
        component="button"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        sx={{
          ...buttonResetSx,
          width: '100%',
          display: 'flex',
          alignItems: 'center',
          gap: 0.75,
          px: 1.5,
          py: 0.75,
          borderRadius: '8px',
          transition: 'background-color 0.15s',
          color: 'text.disabled',
          '&:hover': { backgroundColor: (t) => t.chat.bgRailHover },
        }}
      >
        <Chevron open={open} />
        <Box component="span" sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', flex: 1, textAlign: 'left' }}>
          Agents Catalog
        </Box>
        <Box component="span" sx={{ fontSize: 10, fontVariantNumeric: 'tabular-nums', color: 'text.disabled' }}>
          {entries.length}
        </Box>
      </Box>
      {open && (
        <Box sx={{ mt: 0.25 }}>
          {showSearch && (
            <Box
              component="input"
              value={query}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setQuery(e.target.value)}
              placeholder="Search agents catalog…"
              sx={{
                ...inputResetSx,
                width: 'calc(100% - 1.5rem)',
                mx: 1.5,
                mb: 0.5,
                px: 1,
                py: 0.5,
                borderRadius: '6px',
                fontSize: 12,
                backgroundColor: 'background.paper',
                color: 'text.primary',
                border: 1,
                borderColor: 'divider',
              }}
            />
          )}
          {/* Capped + scrollable so the catalog never pushes the chat session
              list (Recent) off-screen. */}
          <Box sx={{ maxHeight: 208, overflowY: 'auto' }}>
            {filtered.map((item) => (
              <Box
                component="button"
                key={`${item.kind}-${item.id}`}
                onClick={() => (item.kind === 'crew' ? onLoadCrew(item.name) : onLoadFlow(item.name))}
                title={`Open ${item.kind} “${item.name}”`}
                sx={{
                  ...buttonResetSx,
                  width: '100%',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  pl: 3.5,
                  pr: 1.5,
                  py: 0.75,
                  my: 0.25,
                  borderRadius: '8px',
                  textAlign: 'left',
                  transition: 'background-color 0.15s',
                  color: 'text.secondary',
                  '&:hover': { backgroundColor: (t) => t.chat.bgRailHover },
                }}
              >
                {item.kind === 'crew' ? <CrewIcon /> : <FlowIcon />}
                <Box component="span" sx={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 13 }}>{item.name}</Box>
              </Box>
            ))}
            {filtered.length === 0 && (
              <Box sx={{ pl: 3.5, pr: 1.5, py: 0.75, fontSize: 12, color: 'text.disabled' }}>
                No matches
              </Box>
            )}
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default CatalogLibrary;
