import SidebarToggle from '../../features/chat/SidebarToggle';
import { WorkspaceActivity } from './WorkspaceActivity';
import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert, Box, Button, CircularProgress, IconButton, InputBase,
  Menu, MenuItem, Tooltip, Typography,
} from '@mui/material';
import { Archive, ArrowLeft, BookOpen, MessageSquare, MoreHorizontal, Network, Pin, Search, Workflow } from 'lucide-react';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useUILayoutStore } from '../../store/uiLayout';
import { useFlowConfigStore } from '../../store/flowConfig';
import { usePermissionStore } from '../../store/permissions';
import { useThemeStore } from '../../store/theme';
import { useAppStore } from '../../features/chat/store/appStore';
import { useSessionStore } from './sessionStore';
import { useExecutionStore } from '../../features/chat/store/executionStore';
import SidebarAccountActions from '../../components/SidebarAccountActions';
import SidebarAction from '../../components/SidebarAction';
import { kasalStageSurface } from '../../theme/kasalSurfaces';
import { builderSessionKey, collectSessions, type WorkspaceSession } from './sessionIndex';
import { modeLabels, openWorkspaceSession } from './sessionNavigation';
import { useSessionPreferences } from './sessionPreferences';
import NewSessionButton from './NewSessionButton';
import { useWorkspaceSessions } from './useWorkspaceSessions';
import { deleteArchivedSession } from './sessionDeletion';

const modeIcons = { chat: MessageSquare, crew: Network, flow: Workflow };
interface Props { onOpenSettings: () => void; onOpenCatalog?: () => void; library?: React.ReactNode }

export default function SessionSidebar({ onOpenSettings, onOpenCatalog, library }: Props) {
  const { groupId, loadError, loading, retry } = useWorkspaceSessions();
  const tabs = useBuilderCanvasStore(state => state.canvases);
  const unavailableSessionIds = useBuilderCanvasStore(state => state.unavailableSessionIds);
  const activeCanvasId = useBuilderCanvasStore(state => state.activeCanvasId);
  const chats = useSessionStore(state => state.sessions);
  const currentChatId = useSessionStore(state => state.currentSessionId);
  const mode = useUILayoutStore(state => state.appMode);
  const open = useAppStore(state => state.sidebarOpen);
  useEffect(() => {
    useUILayoutStore.setState({ leftSidebarExpanded: open, leftSidebarExpandedWidth: 256 });
  }, [open]);
  const dark = useThemeStore(state => state.isDarkMode);
  const allowCrew = usePermissionStore(state => state.allowAgentBuilder);
  const flowEnabled = useFlowConfigStore(state => state.kasalFlowEnabled);
  const allowFlow = usePermissionStore(state => state.allowFlowBuilder) && flowEnabled;
  const preferences = useSessionPreferences(state => state.entries);
  const updatePreference = useSessionPreferences(state => state.update);
  const [search, setSearch] = useState('');
  const [archived, setArchived] = useState(false);
  const [menu, setMenu] = useState<{ session: WorkspaceSession; anchor: HTMLElement } | null>(null);
  const [renameKey, setRenameKey] = useState<string | null>(null);
  const [rename, setRename] = useState('');
  const [error, setError] = useState('');
  const [opening, setOpening] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const sessions = useMemo(
    () => collectSessions(chats, tabs, groupId, { crew: allowCrew, flow: allowFlow }, unavailableSessionIds),
    [chats, tabs, groupId, allowCrew, allowFlow, unavailableSessionIds],
  );
  const visible = sessions.filter(session => Boolean(preferences[session.key]?.archived) === archived
    && `${session.title} ${modeLabels[session.mode]}`.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => Number(Boolean(preferences[b.key]?.pinned)) - Number(Boolean(preferences[a.key]?.pinned)));
  const activeTab = tabs.find(tab => tab.id === activeCanvasId);
  const activeKey = mode === 'chat' ? `chat:${currentChatId}` : activeTab ? builderSessionKey(activeTab) : null;
  const select = async (session: WorkspaceSession) => {
    setOpening(session.key); setError('');
    try { await openWorkspaceSession(session); }
    catch { setError('Could not open this session. Please try again.'); }
    finally { setOpening(null); }
  };
  const finishRename = async (session: WorkspaceSession) => {
    setRenameKey(null);
    if (!rename.trim() || rename.trim() === session.title) return;
    try {
      if (session.mode === 'chat') await useSessionStore.getState().renameSession(session.id, rename.trim());
      else useBuilderCanvasStore.getState().updateCanvasName(session.id, rename.trim());
    } catch { setError('Could not rename this session. Please try again.'); }
  };
  const remove = async (session: WorkspaceSession) => {
    if (deleting) return;
    setMenu(null); setDeleting(session.key); setError('');
    try { await deleteArchivedSession(session, groupId); }
    catch (cause) {
      setError(cause instanceof Error && !('isAxiosError' in cause)
        ? cause.message : 'Could not delete this session. Please try again.');
    } finally { setDeleting(null); }
  };

  return (
    <Box component="aside" aria-label="Sessions" data-tour="session-sidebar" sx={{
      position: 'absolute', top: 0, bottom: 0, left: 0, zIndex: 10,
      width: open ? 256 : 48, display: 'flex', flexDirection: 'column',
      ...kasalStageSurface(dark), overflow: 'hidden',
    }}>
      <Box sx={{ display: 'flex', alignItems: 'center', flexDirection: open ? 'row' : 'column', px: open ? 1 : 0.5, py: 0.5, minHeight: 48, boxSizing: 'border-box' }}>
        <SidebarToggle />
        <NewSessionButton expanded={open} onCreated={() => { setArchived(false); setSearch(''); }} />
      </Box>
      {open && (
        <>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, px: 1.5, pb: 1.5 }}>
            <Box sx={{ display: 'flex', flex: 1, minWidth: 0, alignItems: 'center', gap: 1, px: 1, py: 0.5, borderRadius: 2, color: 'text.secondary', '&:focus-within': { bgcolor: 'action.hover' } }}>
              <Search size={15} style={{ flexShrink: 0 }} />
              <InputBase value={search} onChange={event => setSearch(event.target.value)} placeholder="Search sessions"
                inputProps={{ 'aria-label': 'Search sessions' }} sx={{ fontSize: 12, width: '100%' }} />
            </Box>
            <Tooltip title={archived ? 'Back to sessions' : 'Archived sessions'}>
              <IconButton data-tour="session-archive" size="small" aria-label={archived ? 'Back to sessions' : 'Archived sessions'} onClick={() => setArchived(!archived)}>
                {archived ? <ArrowLeft size={15} /> : <Archive size={15} />}
              </IconButton>
            </Tooltip>
          </Box>
          {archived && <Typography sx={{ px: 2.5, pb: 1, fontSize: 11, color: 'text.secondary' }}>Archived</Typography>}
          {(error || loadError) && <Alert severity="warning" sx={{ mx: 1, fontSize: 12 }}
            action={loadError && <Button color="inherit" size="small" disabled={loading} onClick={retry}>Retry</Button>}>
            {error || loadError}
          </Alert>}
          <Box data-tour="session-list" sx={{ flex: 1, minHeight: 0, overflowY: 'auto', px: 1, pb: 2 }}>
            {!visible.length && <Typography sx={{ px: 1.5, pt: 2, fontSize: 12, color: 'text.secondary' }}>
              {search ? 'No matching sessions' : archived ? 'No archived sessions' : 'Start a conversation or add to your canvas.'}
            </Typography>}
            {visible.map(session => {
              const Icon = modeIcons[session.mode];
              const selected = session.key === activeKey;
              return (
                <Box key={session.key} sx={{
                  display: 'flex', alignItems: 'center', mb: 0.25, borderRadius: 2,
                  bgcolor: selected ? 'action.selected' : 'transparent',
                  '&:hover': { bgcolor: 'action.hover' },
                  '&:hover .session-options, &:focus-within .session-options': { opacity: 1 },
                }}>
                  {renameKey === session.key ? (
                    <InputBase autoFocus value={rename} onChange={event => setRename(event.target.value)}
                      onBlur={() => void finishRename(session)}
                      onKeyDown={event => {
                        if (event.key === 'Enter') void finishRename(session);
                        if (event.key === 'Escape') setRenameKey(null);
                      }}
                      inputProps={{ 'aria-label': 'Session name', maxLength: 200 }} sx={{ flex: 1, px: 1.5, py: 1, fontSize: 13 }} />
                  ) : (
                    <Button disableRipple color="inherit" aria-current={selected ? 'page' : undefined}
                      disabled={deleting === session.key || session.canvasPending} onClick={() => void select(session)} title={`${session.title} · ${modeLabels[session.mode]}`}
                      sx={{ minWidth: 0, flex: 1, textAlign: 'left', justifyContent: 'flex-start', gap: 1.25, px: 1.5, py: 1.1, textTransform: 'none', color: selected ? 'text.primary' : 'text.secondary' }}>
                      <Icon size={16} style={{ flexShrink: 0 }} />
                      <Typography noWrap sx={{ flex: 1, fontSize: 13, fontWeight: selected ? 600 : 400 }}>{session.title}</Typography>
                      {preferences[session.key]?.pinned && <Pin size={12} style={{ flexShrink: 0 }} />}
                      <SessionActivity session={session} opening={opening === session.key || Boolean(loading && session.canvasPending)} />
                    </Button>
                  )}
                  <IconButton className="session-options" aria-label={`Options for ${session.title}`} size="small" disabled={deleting === session.key || session.canvasPending}
                    onClick={event => setMenu({ session, anchor: event.currentTarget })} sx={{ opacity: selected ? 1 : 0, mr: 0.5 }}>
                    <MoreHorizontal size={16} />
                  </IconButton>
                </Box>
              );
            })}
          </Box>
          {library}
        </>
      )}
      {!open && <Box sx={{ flex: 1 }} />}
      {onOpenCatalog && ((mode === 'crew' && allowCrew) || (mode === 'flow' && allowFlow)) && <SidebarAction
        label={mode === 'flow' ? 'Flow catalog' : 'Crew catalog'} expanded={open}
        icon={<BookOpen size={18} strokeWidth={2} aria-hidden="true" />}
        data-tour="builder-catalog" onClick={onOpenCatalog} />}
      <WorkspaceActivity expanded={open} />
      <SidebarAccountActions onOpenSettings={onOpenSettings} showLabel={open} />
      <Menu disableEnforceFocus disableRestoreFocus anchorEl={menu?.anchor} open={Boolean(menu)} onClose={() => setMenu(null)}
        slotProps={{ paper: { sx: { borderRadius: 3, minWidth: 180 } } }}>
        <MenuItem onClick={() => { if (menu) { setRenameKey(menu.session.key); setRename(menu.session.title); } setMenu(null); }}>Rename</MenuItem>
        <MenuItem onClick={() => { if (menu) updatePreference(menu.session.key, { pinned: !preferences[menu.session.key]?.pinned }); setMenu(null); }}>
          {menu && preferences[menu.session.key]?.pinned ? 'Unpin' : 'Pin'}
        </MenuItem>
        <MenuItem onClick={() => { if (menu) updatePreference(menu.session.key, { archived: !archived }); setMenu(null); }}>
          {archived ? 'Restore session' : 'Archive'}
        </MenuItem>
        {archived && <MenuItem disabled={Boolean(deleting)} sx={{ color: 'error.main' }} onClick={() => {
          if (menu) void remove(menu.session);
        }}>Delete session</MenuItem>}
      </Menu>
    </Box>
  );
}

function SessionActivity({ session, opening }: { session: WorkspaceSession; opening: boolean }) {
  const chatRunning = useExecutionStore(state => session.mode === 'chat' && state.hasActiveExecution(session.id));
  return opening || session.running || chatRunning ? <CircularProgress size={12} color="inherit" aria-label={opening ? 'Opening session' : 'Running'} /> : null;
}
