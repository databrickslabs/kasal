import React from 'react';
import { Box, IconButton, Tooltip } from '@mui/material';
import { Eraser, Maximize, ZoomIn, ZoomOut, ArrowLeftRight } from 'lucide-react';
import { useUILayoutStore } from '../../store/uiLayout';
import { useThemeStore } from '../../store/theme';

interface Props { onClear: () => void; onFit: () => void; onZoomIn: () => void; onZoomOut: () => void }
export default function CanvasTools({ onClear, onFit, onZoomIn, onZoomOut }: Props) {
  const dark = useThemeStore(state => state.isDarkMode);
  const layout = useUILayoutStore(state => state.layoutOrientation);
  const toggle = () => {
    useUILayoutStore.getState().setLayoutOrientation(layout === 'horizontal' ? 'vertical' : 'horizontal');
    window.dispatchEvent(new CustomEvent('recalculateNodePositions', { detail: { reason: 'layout-orientation-toggle' } }));
  };
  return <Box data-tour="canvas-tools" aria-label="Canvas tools" role="toolbar" sx={{ position: 'absolute', bottom: 18, right: 18, zIndex: 15, display: 'flex', alignItems: 'center', p: 0.5, borderRadius: 3, bgcolor: dark ? '#232930' : '#FFFFFF', boxShadow: dark ? '0 2px 12px rgba(0,0,0,.2)' : '0 2px 12px rgba(27,31,35,.08)' }}>
    {([{ title: 'Fit view', Icon: Maximize, action: onFit }, { title: 'Zoom in', Icon: ZoomIn, action: onZoomIn }, { title: 'Zoom out', Icon: ZoomOut, action: onZoomOut }, { title: 'Change canvas orientation', Icon: ArrowLeftRight, action: toggle }, { title: 'Clear canvas', Icon: Eraser, action: onClear }]).map(({ title, Icon, action }) =>
      <Tooltip key={title} title={title}><IconButton aria-label={title} onClick={action} sx={{ width: 40, height: 36, borderRadius: 2 }}><Icon size={16} /></IconButton></Tooltip>)}
  </Box>;
}
