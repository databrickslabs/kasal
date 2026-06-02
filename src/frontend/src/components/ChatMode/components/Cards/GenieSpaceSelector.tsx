import React, { useState, useEffect, useRef, useCallback } from 'react';
import { searchGenieSpaces, GenieSpace } from '../../api/genie';

interface GenieSpaceSelectorProps {
  value: string;
  onChange: (spaceId: string) => void;
}

const GenieSpaceSelector: React.FC<GenieSpaceSelectorProps> = ({ value, onChange }) => {
  const [spaces, setSpaces] = useState<GenieSpace[]>([]);
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const fetchedRef = useRef(false);

  const fetchSpaces = useCallback(async (search?: string) => {
    setLoading(true);
    setError(null);
    try {
      const result = await searchGenieSpaces(search);
      setSpaces(result);
      fetchedRef.current = true;
    } catch {
      setError('Failed to load spaces');
      setSpaces([]);
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch on first open
  useEffect(() => {
    if (open && !fetchedRef.current) {
      fetchSpaces();
    }
  }, [open, fetchSpaces]);

  // Debounced search
  useEffect(() => {
    if (!open) return;
    const timer = setTimeout(() => {
      fetchSpaces(query);
    }, 300);
    return () => clearTimeout(timer);
  }, [query, open, fetchSpaces]);

  // Close on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  const selectedSpace = spaces.find((s) => s.id === value);
  const displayText = selectedSpace ? selectedSpace.name : value || '';

  return (
    <div ref={wrapperRef} className="relative">
      <label
        className="block text-[10px] font-semibold uppercase tracking-wider mb-1"
        style={{ color: 'var(--text-muted)' }}
      >
        Genie Space
      </label>
      <button
        type="button"
        onClick={() => {
          setOpen(!open);
          if (!open) {
            setTimeout(() => inputRef.current?.focus(), 50);
          }
        }}
        className="w-full flex items-center gap-2 rounded-lg px-3 py-2 text-sm text-left transition-colors"
        style={{
          backgroundColor: 'var(--bg-primary)',
          color: value ? 'var(--text-primary)' : 'var(--text-muted)',
          border: `1px solid ${open ? 'var(--accent)' : 'var(--border-color)'}`,
        }}
      >
        <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75m16.5 0c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" />
        </svg>
        <span className="flex-1 truncate">
          {displayText || 'Select a space...'}
        </span>
        <svg
          className={`w-3.5 h-3.5 flex-shrink-0 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
          style={{ color: 'var(--text-muted)' }}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
        </svg>
      </button>

      {open && (
        <div
          className="absolute z-50 mt-1 w-full rounded-lg shadow-lg overflow-hidden"
          style={{
            backgroundColor: 'var(--bg-input)',
            border: '1px solid var(--border-color)',
          }}
        >
          {/* Search input */}
          <div className="p-2" style={{ borderBottom: '1px solid var(--border-color)' }}>
            <input
              ref={inputRef}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search spaces..."
              className="w-full px-2.5 py-1.5 rounded text-sm outline-none"
              style={{
                backgroundColor: 'var(--bg-primary)',
                color: 'var(--text-primary)',
                border: '1px solid var(--border-color)',
              }}
            />
          </div>

          {/* Space list */}
          <div className="max-h-48 overflow-y-auto">
            {loading && (
              <div className="px-3 py-4 text-center text-xs" style={{ color: 'var(--text-muted)' }}>
                Loading spaces...
              </div>
            )}
            {error && (
              <div className="px-3 py-4 text-center text-xs" style={{ color: '#ef4444' }}>
                {error}
              </div>
            )}
            {!loading && !error && spaces.length === 0 && (
              <div className="px-3 py-4 text-center text-xs" style={{ color: 'var(--text-muted)' }}>
                No spaces found
              </div>
            )}
            {!loading && spaces.map((space) => (
              <button
                key={space.id}
                onClick={() => {
                  onChange(space.id);
                  setOpen(false);
                }}
                className="w-full text-left px-3 py-2 text-sm transition-colors hover:opacity-80 flex items-center gap-2"
                style={{
                  backgroundColor: space.id === value ? 'var(--bg-secondary)' : 'transparent',
                  color: 'var(--text-primary)',
                }}
              >
                <svg className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375" />
                </svg>
                <div className="flex-1 min-w-0">
                  <div className="truncate">{space.name}</div>
                  {space.description && (
                    <div className="text-[10px] truncate" style={{ color: 'var(--text-muted)' }}>
                      {space.description}
                    </div>
                  )}
                </div>
                {space.id === value && (
                  <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                  </svg>
                )}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default GenieSpaceSelector;
