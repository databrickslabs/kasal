import React, { lazy, Suspense, useEffect, useState } from 'react';
import { Box, Button, CircularProgress, Dialog, IconButton, Typography } from '@mui/material';
import { Activity, X } from 'lucide-react';
import { useSessionRunIds } from './useSessionRunIds';
import { useThemeStore } from '../../store/theme';
import { kasalStageSurface } from '../../theme/kasalSurfaces';
import { useGroupStore } from '../../store/groups';
import { usePermissionStore } from '../../store/permissions';
import SidebarAction from '../../components/SidebarAction';

const ExecutionHistory = lazy(() => import('../../features/executions/components/ExecutionHistory'));
const Schedules = lazy(() => import('../../features/workflow/scheduling/components/ScheduleDialog'));
const ModelCalls = lazy(() => import('../../features/executions/components/LLMLogs'));
type Section = 'executions' | 'schedules' | 'logs';
const sections: { id: Section; label: string }[] = [
  { id: 'executions', label: 'Executions' },
  { id: 'schedules', label: 'Schedules' },
  { id: 'logs', label: 'Model calls' },
];

export function WorkspaceActivity({ expanded }: { expanded: boolean }) {
  const dark = useThemeStore(state => state.isDarkMode);
  const [open, setOpen] = useState(false);
  const [section, setSection] = useState<Section>('executions');
  const [scope, setScope] = useState<'session' | 'teamspace'>('session');
  const { ids, sessionKey } = useSessionRunIds();
  const groupId = useGroupStore(state => state.currentGroupId);
  const canUseBuilders = usePermissionStore(state => state.allowAgentBuilder || state.allowFlowBuilder);
  const activeSection = canUseBuilders ? section : 'executions';
  useEffect(() => { setOpen(false); }, [groupId]);
  useEffect(() => {
    const show = () => { setScope('teamspace'); setSection('executions'); setOpen(true); };
    window.addEventListener('openWorkspaceActivity', show);
    return () => window.removeEventListener('openWorkspaceActivity', show);
  }, []);
  return <>
    <SidebarAction label="Activity" icon={<Activity size={18} />} expanded={expanded}
      onClick={() => setOpen(true)} data-tour="workspace-activity" />
    <Dialog key={`${groupId}:${sessionKey}`} open={open} onClose={() => setOpen(false)} maxWidth="lg" fullWidth aria-label="Activity"
      PaperProps={{ sx: { height: 'min(780px, 85dvh)', borderRadius: 4, ...kasalStageSurface(dark) } }}>
      <Box sx={{ display: 'flex', alignItems: 'center', px: 2.5, pt: 2, pb: 1, gap: 1 }}>
        <Activity size={19} />
        <Typography component="h2" sx={{ fontWeight: 600, fontSize: 16, flex: 1 }}>Activity</Typography>
        <IconButton size="small" aria-label="Close activity" onClick={() => setOpen(false)}><X size={18} /></IconButton>
      </Box>
      <Box component="nav" aria-label="Activity sections" sx={{ display: 'flex', px: 2, pb: 1, gap: 0.5 }}>
        {sections.filter(item => item.id === 'executions' || canUseBuilders).map(item => <Button key={item.id} size="small" color="inherit" aria-pressed={activeSection === item.id}
          onClick={() => setSection(item.id)} sx={{ borderRadius: 2, px: 1.5, fontSize: 13, bgcolor: activeSection === item.id ? 'action.selected' : 'transparent' }}>{item.label}</Button>)}
      </Box>
      {activeSection === 'executions' && <Box sx={{ display: 'flex', px: { xs: 2, sm: 3 }, pt: 1.5, gap: 0.5 }}>
        {(['session', 'teamspace'] as const).map(value => <Button key={value} size="small" color="inherit" aria-pressed={scope === value}
          onClick={() => setScope(value)} sx={{ borderRadius: 2, fontSize: 12, color: scope === value ? 'text.primary' : 'text.secondary', bgcolor: scope === value ? 'action.hover' : 'transparent' }}>
          {value === 'session' ? 'This session' : 'All teamspace runs'}
        </Button>)}
      </Box>}
      {open && <Box key={`${groupId}:${activeSection}`} sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
        <Suspense fallback={<Box sx={{ p: 4, textAlign: 'center' }}><CircularProgress size={24} color="inherit" aria-label="Loading activity" /></Box>}>
          {activeSection === 'executions' && <ExecutionHistory key={scope} embedded jobIds={scope === 'session' ? ids : undefined} title="Executions" />}
          {activeSection === 'schedules' && <Schedules embedded open onClose={() => setOpen(false)} nodes={[]} edges={[]} selectedModel="" />}
          {activeSection === 'logs' && <Box sx={{ p: 2.5, pt: 1, flex: 1, minHeight: 0, overflow: 'hidden', display: 'flex' }}>
            <ModelCalls embedded />
          </Box>}
        </Suspense>
      </Box>}
    </Dialog>
  </>;
}
