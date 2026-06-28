import React, { useState, useEffect, useRef, useCallback } from 'react';
import Box from '@mui/material/Box';
import { searchGenieSpaces, getGenieSpace, GenieSpace } from '../../api/genie';
import { buttonResetSx, inputResetSx } from '../../chatSx';

interface GenieSpaceSelectorProps {
  value: string;
  onChange: (spaceId: string) => void;
}

const SHADOW_LG = '0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1)';

const GenieSpaceSelector: React.FC<GenieSpaceSelectorProps> = ({ value, onChange }) => {
  const [spaces, setSpaces] = useState<GenieSpace[]>([]);
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const fetchedRef = useRef(false);
  // Deep link for the CURRENTLY SELECTED space, shown on the closed picker so
  // the user can open it to validate — resolved even when the dropdown list
  // hasn't been loaded (e.g. a restored session).
  const [selectedUrl, setSelectedUrl] = useState<string | null>(null);

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

  // Resolve the selected space's deep link: prefer the already-loaded list,
  // otherwise fetch the single space so the "open in Databricks" link works
  // without opening the dropdown.
  useEffect(() => {
    if (!value) {
      setSelectedUrl(null);
      return;
    }
    const fromList = spaces.find((s) => s.id === value)?.url;
    if (fromList) {
      setSelectedUrl(fromList);
      return;
    }
    let cancelled = false;
    getGenieSpace(value).then((space) => {
      if (!cancelled) setSelectedUrl(space?.url || null);
    });
    return () => {
      cancelled = true;
    };
  }, [value, spaces]);

  return (
    <Box ref={wrapperRef} sx={{ position: 'relative' }}>
      <Box
        component="label"
        sx={{ display: 'block', fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', mb: 0.5, color: 'text.disabled' }}
      >
        Genie Space
      </Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
        <Box
          component="button"
          type="button"
          onClick={() => {
            setOpen(!open);
            if (!open) {
              setTimeout(() => inputRef.current?.focus(), 50);
            }
          }}
          sx={{
            ...buttonResetSx,
            flex: 1,
            minWidth: 0,
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            borderRadius: '8px',
            px: 1.5,
            py: 1,
            fontSize: 14,
            textAlign: 'left',
            transition: 'border-color 0.15s, background-color 0.15s',
            backgroundColor: 'background.default',
            color: value ? 'text.primary' : 'text.disabled',
            border: 1,
            borderColor: open ? 'primary.main' : 'divider',
          }}
        >
          <Box component="svg" sx={{ width: 16, height: 16, flexShrink: 0, color: 'primary.main' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75m16.5 0c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" />
          </Box>
          <Box component="span" sx={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {displayText || 'Select a space...'}
          </Box>
          <Box
            component="svg"
            sx={{ width: 14, height: 14, flexShrink: 0, transition: 'transform 0.15s', transform: open ? 'rotate(180deg)' : 'none', color: 'text.disabled' }}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
          </Box>
        </Box>
        {value && selectedUrl && (
          <Box
            component="a"
            href={selectedUrl}
            target="_blank"
            rel="noopener noreferrer"
            title="Open the selected space in Databricks"
            aria-label="Open the selected space in Databricks"
            sx={{
              flexShrink: 0,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 36,
              height: 36,
              borderRadius: '8px',
              transition: 'opacity 0.15s',
              color: 'text.disabled',
              border: 1,
              borderColor: 'divider',
              '&:hover': { opacity: 0.7 },
            }}
          >
            <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 6H5.25A2.25 2.25 0 003 8.25v10.5A2.25 2.25 0 005.25 21h10.5A2.25 2.25 0 0018 18.75V10.5m-10.5 6L21 3m0 0h-5.25M21 3v5.25" />
            </Box>
          </Box>
        )}
      </Box>

      {open && (
        <Box
          sx={{
            position: 'absolute',
            zIndex: 50,
            mt: 0.5,
            width: '100%',
            borderRadius: '8px',
            boxShadow: SHADOW_LG,
            overflow: 'hidden',
            backgroundColor: 'background.paper',
            border: 1,
            borderColor: 'divider',
          }}
        >
          {/* Search input */}
          <Box sx={{ p: 1, borderBottom: 1, borderColor: 'divider' }}>
            <Box
              component="input"
              ref={inputRef}
              value={query}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setQuery(e.target.value)}
              placeholder="Search spaces..."
              sx={{
                ...inputResetSx,
                width: '100%',
                px: 1.25,
                py: 0.75,
                borderRadius: '4px',
                fontSize: 14,
                backgroundColor: 'background.default',
                color: 'text.primary',
                border: 1,
                borderColor: 'divider',
              }}
            />
          </Box>

          {/* Space list */}
          <Box sx={{ maxHeight: 192, overflowY: 'auto' }}>
            {loading && (
              <Box sx={{ px: 1.5, py: 2, textAlign: 'center', fontSize: 12, color: 'text.disabled' }}>
                Loading spaces...
              </Box>
            )}
            {error && (
              <Box sx={{ px: 1.5, py: 2, textAlign: 'center', fontSize: 12, color: '#ef4444' }}>
                {error}
              </Box>
            )}
            {!loading && !error && spaces.length === 0 && (
              <Box sx={{ px: 1.5, py: 2, textAlign: 'center', fontSize: 12, color: 'text.disabled' }}>
                No spaces found
              </Box>
            )}
            {!loading && spaces.map((space) => (
              // Row = the select button + a separate external link. They're
              // siblings (not nested) so the link doesn't sit inside the button
              // (invalid HTML) and clicking it opens Databricks without selecting.
              <Box
                key={space.id}
                sx={{
                  display: 'flex',
                  alignItems: 'stretch',
                  transition: 'background-color 0.15s',
                  '&:hover': { opacity: 0.8 },
                  backgroundColor: space.id === value ? (t) => t.chat.bgSecondary : 'transparent',
                }}
              >
                <Box
                  component="button"
                  onClick={() => {
                    onChange(space.id);
                    setOpen(false);
                  }}
                  sx={{
                    ...buttonResetSx,
                    flex: 1,
                    minWidth: 0,
                    textAlign: 'left',
                    px: 1.5,
                    py: 1,
                    fontSize: 14,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                    color: 'text.primary',
                  }}
                >
                  <Box component="svg" sx={{ width: 14, height: 14, flexShrink: 0, color: 'primary.main' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375" />
                  </Box>
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Box sx={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{space.name}</Box>
                    {space.description && (
                      <Box sx={{ fontSize: 10, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'text.disabled' }}>
                        {space.description}
                      </Box>
                    )}
                  </Box>
                  {space.id === value && (
                    <Box component="svg" sx={{ width: 16, height: 16, flexShrink: 0, color: 'primary.main' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                    </Box>
                  )}
                </Box>
                {space.url && (
                  <Box
                    component="a"
                    href={space.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    onClick={(e: React.MouseEvent) => e.stopPropagation()}
                    title="Open this space in Databricks"
                    aria-label={`Open ${space.name} in Databricks`}
                    sx={{ display: 'flex', alignItems: 'center', px: 1.25, flexShrink: 0, transition: 'opacity 0.15s', color: 'text.disabled', '&:hover': { opacity: 0.7 } }}
                  >
                    <Box component="svg" sx={{ width: 14, height: 14 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 6H5.25A2.25 2.25 0 003 8.25v10.5A2.25 2.25 0 005.25 21h10.5A2.25 2.25 0 0018 18.75V10.5m-10.5 6L21 3m0 0h-5.25M21 3v5.25" />
                    </Box>
                  </Box>
                )}
              </Box>
            ))}
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default GenieSpaceSelector;
