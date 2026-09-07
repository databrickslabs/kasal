import React, { useContext, useState } from 'react';
import { Box, Button, Dialog, DialogContent, IconButton } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import A2uiSurface from '../../../chat/components/Chat/A2uiSurface';
import { downloadSurfacePdf } from '../../../chat/utils/surfacePdf';
import { BuilderPreviewContext } from './BuilderPreviewContext';
import { useThemeStore } from '../../../../store/theme';
import type { Surface } from '../../../../shared/a2ui/index';
import '../../../chat/chat.css';

// Match Chat's native-control reset without introducing a second chat root ID.
const scopeSx = { background: 'transparent', '& button': { border: 0, padding: 0, backgroundColor: 'transparent', color: 'inherit', cursor: 'pointer' } };

export const UiSurfaceView: React.FC<{ surface: Surface }> = ({ surface }) => {
  const dark = useThemeStore(state => state.isDarkMode);
  return <Box className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'} sx={scopeSx}>
    <A2uiSurface blendWithHost surface={surface} />
  </Box>;
};

/** Render at the conversation's width, using Chat's surface controls and theme. */
export const UiSurfaceResult: React.FC<{ surface: Surface; messageId?: string }> = ({ surface, messageId }) => {
  const preview = useContext(BuilderPreviewContext);
  const dark = useThemeStore(state => state.isDarkMode);
  const [dialogOpen, setDialogOpen] = useState(false);
  const inPane = Boolean(messageId && preview?.previewMessageId === messageId);
  const expand = () => {
    if (preview?.openResult) preview.openResult({ type: 'ui', data: JSON.stringify(surface), sourceMessageId: messageId });
    else setDialogOpen(true);
  };
  return <Box sx={{ width: '100%', minWidth: 0, whiteSpace: 'normal' }}>
    {inPane ? <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, fontSize: 12, color: 'text.secondary' }}>
      Opened in the side panel
      <Button size="small" color="inherit" onClick={preview?.closePreview}>Show here</Button>
    </Box> : <Box className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'} sx={scopeSx}>
      <A2uiSurface blendWithHost surface={surface} onExpand={expand}
        onDownloadPdf={() => downloadSurfacePdf(surface, surface.surfaceKind || 'kasal-app')} />
    </Box>}
    <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} fullWidth maxWidth="lg">
      <IconButton aria-label="Close full view" onClick={() => setDialogOpen(false)}
        sx={{ position: 'absolute', top: 8, right: 8, zIndex: 1 }}><CloseIcon /></IconButton>
      <DialogContent><UiSurfaceView surface={surface} /></DialogContent>
    </Dialog>
  </Box>;
};
