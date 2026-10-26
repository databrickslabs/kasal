import React, { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { Box, IconButton, Typography } from '@mui/material';
import { Minimize2 } from 'lucide-react';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { kasalStageSurface } from '../../../../theme/kasalSurfaces';

interface Props {
  dark: boolean;
  landing: boolean;
  response: React.ReactNode;
  preview: React.ReactNode;
  composerHost: HTMLElement;
  onClose: () => void;
}

/** Chat's 768px column and landing layout, while preserving builder execution and draft state. */
export default function ExpandedBuilderConversation({ dark, landing, response, preview, composerHost, onClose }: Props) {
  const expanded = useUILayoutStore(state => state.leftSidebarExpanded);
  const leftWidth = useUILayoutStore(state => state.leftSidebarExpandedWidth);
  const closeRef = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    closeRef.current?.focus();
    // The canvas stays mounted behind this view, but its hidden controls must not receive focus.
    const covered = [...document.querySelectorAll<HTMLElement>('[data-testid="workspace-conversation-pane"], [data-crew-container], [data-flow-container]')];
    const previous = covered.map(element => element.inert);
    covered.forEach(element => { element.inert = true; });
    return () => covered.forEach((element, index) => { element.inert = previous[index]; });
  }, []);
  const input = <Box sx={{ width: '100%', maxWidth: 768, mx: 'auto', px: 2, pb: 2.5, pt: 1, flexShrink: 0, boxSizing: 'border-box' }}
    ref={(node: HTMLDivElement | null) => { if (node && composerHost.parentElement !== node) node.appendChild(composerHost); }} />;
  return createPortal(<Box role="region" aria-label="Expanded conversation" className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}
    onKeyDown={event => { if (event.key === 'Escape' && !event.defaultPrevented) { event.stopPropagation(); onClose(); } }}
    sx={{ position: 'fixed', top: 0, bottom: 0, left: expanded ? leftWidth : 48, right: 0, zIndex: 1202,
      display: 'flex', minWidth: 0, overflow: 'hidden', ...kasalStageSurface(dark),
      '& [data-testid="builder-composer"]': { bgcolor: 'var(--bg-input)', border: '1px solid var(--border-color)', boxShadow: 'var(--shadow-input)',
        '&:focus-within': { boxShadow: 'var(--shadow-input-focus)' } },
      '& [data-testid="builder-conversation-scroll"]': { pt: 3, pb: 3 },
    }}>
    <Box component="main" sx={{ position: 'relative', display: 'flex', flexDirection: 'column', flex: 1, minWidth: 0, minHeight: 0 }}>
      <IconButton ref={closeRef} aria-label="Back to canvas" onClick={onClose} size="small"
        sx={{ position: 'absolute', top: 8, right: 10, zIndex: 1 }}><Minimize2 size={16} /></IconButton>
      {landing ? <Box className="kasal-landing-hero" sx={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', height: '100%', px: 3 }}>
        <Typography component="h1" sx={{ textAlign: 'center', mb: 3, fontSize: 24, fontWeight: 600, color: 'var(--text-primary)' }}>What can I help you with?</Typography>
        {input}
      </Box> : <>
        <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', width: '100%', maxWidth: 768, mx: 'auto' }}>{response}</Box>
        {input}
      </>}
    </Box>
    {preview && <Box sx={{ flex: 1, minWidth: 0, minHeight: 0 }}>{preview}</Box>}
  </Box>, document.body);
}
