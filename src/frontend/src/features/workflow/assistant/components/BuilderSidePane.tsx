import { useLayoutEffect } from 'react';
import { Box, Button, IconButton, Tab, Tabs, Typography } from '@mui/material';
import { ArrowLeft, X } from 'lucide-react';
import PreviewPanel from '../../../chat/components/Preview/PreviewPanel';
import ScheduleRunDialog from '../../../chat/components/Chat/ScheduleRunDialog';
import CrewOptimizeDialog from '../../crews/components/CrewOptimizeDialog';
import HITLApprovalDialog from '../../../approvals/components/HITLApprovalDialog';
import CheckpointDialog from '../../../executions/components/CheckpointDialog';
import type { PreviewContent } from '../../../chat/types/preview';
import type { RunStep } from '../../../chat/components/Preview/traceEventStep';
import { isCanvasEditorId, type BuilderNodeEditorEntry } from '../store/builderNodeEditorBridge';
import { kasalStageSurface } from '../../../../theme/kasalSurfaces';

export type BuilderPaneTab =
  | { id: 'activity' | 'memory' | 'result'; content: PreviewContent; step?: RunStep }
  | { id: 'schedule'; executionId: string; defaultName: string; onCreated: (name: string) => void }
  | { id: 'optimize'; crewId: string; crewName: string }
  | { id: 'approval'; jobId: string; approvalId: number; onDecision: () => void }
  | { id: 'checkpoints'; jobId: string; onResumed: (newJobId: string) => void }
  | { id: BuilderNodeEditorEntry['id']; editor: BuilderNodeEditorEntry };
export type BuilderPaneId = BuilderPaneTab['id'] | 'canvas';
const labels: Partial<Record<BuilderPaneId, string>> = {
  approval: 'Review approval', canvas: 'Canvas', activity: 'Run activity', result: 'Result', memory: 'Memory graph', optimize: 'Optimize crew', schedule: 'Schedule', checkpoints: 'Checkpoints',
};

/** One canvas-side destination, with views opened only from conversation actions. */
export default function BuilderSidePane({ tabs, active, onSelect, onClose, dark, canvasHost }: {
  tabs: BuilderPaneTab[]; active: BuilderPaneId; onSelect: (id: BuilderPaneId) => void;
  onClose: (id: BuilderPaneId) => void; dark: boolean; canvasHost?: HTMLElement | null;
}) {
  const editingCanvas = isCanvasEditorId(active);
  const visibleTabs = tabs.filter(tab => !isCanvasEditorId(tab.id));
  const labelFor = (id: BuilderPaneId) => {
    const tab = tabs.find(item => item.id === id);
    if (tab && 'editor' in tab && id.startsWith('catalog:')) return tab.editor.label;
    return tab && 'editor' in tab ? `${id.startsWith('agent:') ? 'Agent' : id.startsWith('task:') ? 'Task' : 'Connection'} · ${tab.editor.label}` : labels[id];
  };
  // The graph remains mounted with its viewport and selection intact. A hidden
  // graph must not receive keyboard shortcuts or focus through the visible pane.
  useLayoutEffect(() => {
    const parent = canvasHost?.parentElement;
    if (!parent) return;
    const siblings = Array.from(parent.children).filter(element => element !== canvasHost) as HTMLElement[];
    const previous = siblings.map(element => ({ inert: element.inert, visibility: element.style.visibility }));
    const padding = parent.style.paddingTop;
    parent.style.paddingTop = '44px';
    if (active !== 'canvas') siblings.forEach(element => { element.inert = true; element.style.visibility = 'hidden'; });
    return () => {
      parent.style.paddingTop = padding;
      siblings.forEach((element, index) => {
        element.inert = previous[index].inert;
        element.style.visibility = previous[index].visibility;
      });
    };
  }, [canvasHost, active]);

  return <Box className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}
    sx={{ height: '100%', width: '100%', display: 'flex', flexDirection: 'column', minHeight: 0, pointerEvents: 'none', color: 'text.primary' }}>
    <Box data-builder-pane-tabs sx={{ display: 'flex', alignItems: 'center', height: 44, flexShrink: 0, px: 1, pointerEvents: 'auto', ...kasalStageSurface(dark) }}>
      <Tabs value={editingCanvas ? 'canvas' : active} onChange={(_, id: BuilderPaneId) => onSelect(id)} variant="scrollable" scrollButtons="auto" allowScrollButtonsMobile
        aria-label="Canvas and run views" sx={{ flex: 1, minWidth: 0, minHeight: 36,
          '& .MuiTabs-indicator': { display: 'none' },
          '& .MuiTab-root': { minHeight: 32, minWidth: 0, maxWidth: 210, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', px: 1.5, py: 0.5, mr: 0.5, borderRadius: '12px', textTransform: 'none', fontSize: 13, color: 'text.secondary' },
          '& .MuiTab-root.Mui-selected': { bgcolor: 'action.selected', color: 'text.primary' } }}>
        {(['canvas', ...visibleTabs.map(tab => tab.id)] as BuilderPaneId[]).map(id => <Tab key={id} value={id} label={labelFor(id)}
          onClick={id === 'canvas' && editingCanvas ? () => onClose(active) : undefined}
          sx={id === 'canvas' ? { position: 'sticky', left: 0, zIndex: 1, ...kasalStageSurface(dark), '&.Mui-selected': { backgroundImage: 'none' } } : undefined}
          id={`builder-view-tab-${id}`} aria-controls={id === 'canvas' ? editingCanvas ? `builder-view-panel-${active}` : undefined : `builder-view-panel-${id}`} />)}
      </Tabs>
      {active !== 'canvas' && !editingCanvas && <IconButton size="small" aria-label={`Close ${labelFor(active)}`} onClick={() => onClose(active)}><X size={16} /></IconButton>}
    </Box>
    {/* Node forms are children of Canvas; other workspace views retain their tabs. */}
    {tabs.map(tab => <Box key={tab.id} role="tabpanel" id={`builder-view-panel-${tab.id}`} aria-labelledby={`builder-view-tab-${isCanvasEditorId(tab.id) ? 'canvas' : tab.id}`} hidden={active !== tab.id}
      sx={{ display: active === tab.id ? 'flex' : 'none', flexDirection: 'column', flex: 1, minHeight: 0, minWidth: 0, overflow: 'hidden', pointerEvents: 'auto', ...kasalStageSurface(dark) }}>
      {isCanvasEditorId(tab.id) && <Box component="nav" aria-label="Canvas navigation"
        sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 2, py: 1, flexShrink: 0, borderBottom: 1, borderColor: 'divider' }}>
        <Button color="inherit" size="small" startIcon={<ArrowLeft size={16} />} onClick={() => onClose(tab.id)}
          sx={{ textTransform: 'none', flexShrink: 0, borderRadius: 2 }}>Back to Canvas</Button>
        <Typography aria-hidden sx={{ color: 'text.disabled' }}>/</Typography>
        <Typography noWrap sx={{ fontSize: 12, color: 'text.secondary' }}>{labelFor(tab.id)}</Typography>
      </Box>}
      {'editor' in tab ? <Box sx={{ width: '100%', flex: 1, minHeight: 0, overflow: 'hidden' }}
        ref={(element: HTMLDivElement | null) => { if (element && tab.editor.host.parentElement !== element) element.appendChild(tab.editor.host); }} />
        : tab.id === 'schedule' ? <ScheduleRunDialog key={tab.executionId} embedded executionId={tab.executionId} defaultName={tab.defaultName}
        onClose={() => onClose(tab.id)} onCreated={name => { tab.onCreated(name); onClose(tab.id); }} />
        : tab.id === 'optimize' ? <CrewOptimizeDialog key={tab.crewId} embedded open={active === tab.id} crewId={tab.crewId} crewName={tab.crewName} onClose={() => onClose(tab.id)} />
        : tab.id === 'approval' ? <HITLApprovalDialog key={tab.approvalId} embedded open executionId={tab.jobId} approvalId={tab.approvalId} onClose={() => onClose(tab.id)} onActionComplete={() => tab.onDecision()} />
        : tab.id === 'checkpoints' ? <CheckpointDialog key={tab.jobId} embedded open jobId={tab.jobId} onResumed={tab.onResumed} onClose={() => onClose(tab.id)} />
        : <Box role="region" aria-label={tab.id === 'memory' ? 'Run memory preview' : tab.id === 'result' ? 'Result preview' : 'Run activity preview'}
          sx={{ display: 'flex', height: '100%', width: '100%', minWidth: 0, '& > aside': { minWidth: 0 }, '& button': { border: 0, backgroundColor: 'transparent', color: 'inherit', cursor: 'pointer' } }}>
          <PreviewPanel embedded content={tab.content} focusStep={tab.step} chatCollapsed={false} onClose={() => onClose(tab.id)} />
        </Box>}
    </Box>)}
  </Box>;
}
