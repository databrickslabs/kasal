import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { Box, Button, Typography } from '@mui/material';
import { ArrowLeft } from 'lucide-react';

interface CatalogAction {
  label: string;
  host: HTMLElement;
  onClose: () => void;
}
const CatalogNavigationContext = createContext<{
  action: CatalogAction | null;
  openAction: (action: CatalogAction) => void;
  releaseAction: (host: HTMLElement) => void;
} | null>(null);

export const useCatalogNavigation = () => useContext(CatalogNavigationContext);

export function CatalogNavigation({ open, children }: { open: boolean; children: ReactNode }) {
  const [action, setAction] = useState<CatalogAction | null>(null);
  const openAction = useCallback((entry: CatalogAction) => setAction(entry), []);
  const releaseAction = useCallback((host: HTMLElement) => setAction(current => current?.host === host ? null : current), []);
  useEffect(() => {
    if (!open && action) { action.onClose(); setAction(null); }
  }, [open, action]);
  return <CatalogNavigationContext.Provider value={{ action, openAction, releaseAction }}>{children}</CatalogNavigationContext.Provider>;
}

/** A Catalog action replaces the list; Back closes it and restores the list. */
export function CatalogNavigationContent({ children }: { children: ReactNode }) {
  const navigation = useCatalogNavigation();
  const action = navigation?.action;
  return <>
    <Box className="catalog-list" hidden={Boolean(action)} sx={{ display: action ? 'none' : 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      {children}
    </Box>
    {action && <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, overflow: 'hidden' }}>
      <Box component="nav" aria-label="Catalog navigation" sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 2, py: 1, borderBottom: 1, borderColor: 'divider' }}>
        <Button color="inherit" size="small" startIcon={<ArrowLeft size={16} />} onClick={action.onClose}
          sx={{ textTransform: 'none', flexShrink: 0, borderRadius: 2 }}>Back to Catalog</Button>
        <Typography aria-hidden sx={{ color: 'text.disabled' }}>/</Typography>
        <Typography noWrap sx={{ fontSize: 12, color: 'text.secondary' }}>{action.label}</Typography>
      </Box>
      <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}
        ref={(element: HTMLDivElement | null) => { if (element && action.host.parentElement !== element) element.appendChild(action.host); }} />
    </Box>}
  </>;
}
