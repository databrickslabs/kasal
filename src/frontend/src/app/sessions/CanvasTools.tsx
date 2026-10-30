import React from 'react';
import { Box, IconButton, Tooltip } from '@mui/material';
import { Eraser, Maximize, ZoomIn, ZoomOut, ArrowLeftRight, CopyPlus } from 'lucide-react';
import { useUILayoutStore } from '../../store/uiLayout';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useAppStore as useChatAppStore } from '../../features/chat/store/appStore';
import SaveAsNewCrewButton from '../../features/workflow/crews/components/SaveAsNewCrewButton';

interface Props { runControl?: React.ReactNode; onClear: () => void; onFit: () => void; onZoomIn: () => void; onZoomOut: () => void }
export default function CanvasTools({ runControl, onClear, onFit, onZoomIn, onZoomOut }: Props) {
  const layout = useUILayoutStore(state => state.layoutOrientation);
  const activeCanvasId = useBuilderCanvasStore(state => state.activeCanvasId);
  const activeTab = useBuilderCanvasStore(state => state.canvases.find(c => c.id === state.activeCanvasId));
  const toggle = () => {
    useUILayoutStore.getState().setLayoutOrientation(layout === 'horizontal' ? 'vertical' : 'horizontal');
    window.dispatchEvent(new CustomEvent('recalculateNodePositions', { detail: { reason: 'layout-orientation-toggle' } }));
  };
  // Refresh the catalog so the clone shows up immediately in the crew library.
  const handleCloned = (crew: { id: string; name: string }) => {
    void useChatAppStore.getState().loadCatalog();
    window.dispatchEvent(new CustomEvent('crewCloned', { detail: crew }));
  };
  return <Box sx={{ position: 'absolute', bottom: 18, right: 56, zIndex: 15, display: 'flex', alignItems: 'center', p: 0.5, borderRadius: 3, bgcolor: 'transparent', boxShadow: 'none' }}>
    <Box data-tour="canvas-tools" aria-label="Canvas tools" role="toolbar" sx={{ display: 'flex', alignItems: 'center' }}>
    {([{ title: 'Fit view', Icon: Maximize, action: onFit }, { title: 'Zoom in', Icon: ZoomIn, action: onZoomIn }, { title: 'Zoom out', Icon: ZoomOut, action: onZoomOut }, { title: 'Change canvas orientation', Icon: ArrowLeftRight, action: toggle }, { title: 'Clear canvas', Icon: Eraser, action: onClear }]).map(({ title, Icon, action }) =>
      <Tooltip key={title} title={title}><IconButton aria-label={title} onClick={action} sx={{ width: 40, height: 36, borderRadius: 2 }}><Icon size={16} /></IconButton></Tooltip>)}
    <SaveAsNewCrewButton
      key={activeCanvasId}
      crewId={activeTab?.savedCrewId}
      crewName={activeTab?.savedCrewName}
      onCloned={handleCloned}
      renderTrigger={(open, disabled) => (
        <Tooltip title={disabled ? 'Save the crew first to clone it' : 'Save as new crew'}>
          <span>
            <IconButton aria-label="Save as new crew" onClick={open} disabled={disabled} sx={{ width: 40, height: 36, borderRadius: 2 }}><CopyPlus size={16} /></IconButton>
          </span>
        </Tooltip>
      )}
    />
    </Box>
    {runControl && <Box sx={{ display: 'flex', ml: 2, flexShrink: 0 }}>{runControl}</Box>}
  </Box>;
}
