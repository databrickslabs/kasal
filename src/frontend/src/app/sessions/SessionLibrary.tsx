import React from 'react';
import { Alert, Box, Button } from '@mui/material';
import { useUILayoutStore } from '../../store/uiLayout';
import { useThemeStore } from '../../store/theme';
import { useAppStore } from '../../features/chat/store/appStore';
import CatalogLibrary from '../../features/chat/components/CatalogLibrary';

export default function SessionLibrary() {
  const mode = useUILayoutStore(state => state.appMode);
  const dark = useThemeStore(state => state.isDarkMode);
  const crews = useAppStore(state => state.savedCrews);
  const flows = useAppStore(state => state.savedFlows);
  const error = useAppStore(state => state.catalogError);
  const retry = useAppStore(state => state.loadCatalog);
  const load = (kind: 'crew' | 'flow', name: string) => window.dispatchEvent(new CustomEvent('sessionCatalogLoad', { detail: { kind, name } }));
  if (mode !== 'chat') return null;
  return <Box className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'} sx={{ flexShrink: 0, maxHeight: '28vh', overflowY: 'auto', background: 'transparent !important', '& button': { border: 0, backgroundColor: 'transparent', cursor: 'pointer' } }}>
    {error && <Alert severity="warning" sx={{ mx: 1, mb: 1, fontSize: 12 }} action={<Button color="inherit" size="small" onClick={() => void retry()}>Retry</Button>}>{error}</Alert>}
    <CatalogLibrary crews={crews} flows={flows} onLoadCrew={name => load('crew', name)} onLoadFlow={name => load('flow', name)} />
  </Box>;
}
