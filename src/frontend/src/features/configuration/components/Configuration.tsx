import React, { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Box, Button, CircularProgress, FormControl, IconButton, InputAdornment, InputLabel, List, ListItemButton, ListItemIcon, ListItemText, MenuItem, Select, TextField, Typography } from '@mui/material';
import { Zap, Search, X, Settings2, SlidersHorizontal, Bot, Wrench, BookOpen, MessageSquare, Brain, LayoutTemplate, Cloud, Plug, Network, Activity, KeyRound, Users, Cpu, Database, Boxes, Building2, UserRound, ShieldCheck } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { usePermissionStore } from '../../../store/permissions';
import { useUserStore } from '../../../store/user';
import { useGroupStore } from '../../../store/groups';
import { useThemeStore } from '../../../store/theme';
import { useEventTriggersStore } from '../../../store/eventTriggers';
import { kasalStageSurface } from '../../../theme/kasalSurfaces';
import { getSettingsSections, SettingsGroup, SettingsScope, SettingsSection, SettingsSectionId } from './settingsSections';
import GeneralSettings from './GeneralSettings';
import SettingsContent from './SettingsContent';
import { readSettingsNavigation, clearSettingsNavigation, switchSettingsTeamspace } from '../lib/settingsNavigation';

const ModelConfiguration = lazy(() => import('./Models'));
const APIKeys = lazy(() => import('./APIKeys/APIKeys'));
const ObjectManagement = lazy(() => import('./ObjectManagement'));
const ToolsConfiguration = lazy(() => import('./Tools/ToolsConfiguration'));
const Prompts = lazy(() => import('./Prompts'));
const DatabricksConfiguration = lazy(() => import('./DatabricksConfiguration'));
const MLflowConfiguration = lazy(() => import('./MLflowConfiguration'));
const MCPConfiguration = lazy(() => import('./MCP/MCPConfiguration'));
const RemoteAgents = lazy(() => import('./RemoteAgents/RemoteAgents'));
const SkillsConfiguration = lazy(() => import('./Skills/SkillsConfiguration'));
const EnginesConfiguration = lazy(() => import('./Engines'));
const MemoryConfiguration = lazy(() => import('../../memory/components').then(module => ({ default: module.MemoryConfiguration })));
const DatabaseManagement = lazy(() => import('./DatabaseManagement'));
const AccessManagement = lazy(() => import('./AccessManagement'));
const UIConfigurator = lazy(() => import('./UIConfigurator'));
const TriggersPanel = lazy(() => import('../../triggers/components/TriggersPanel'));
const WorkspaceOverview = lazy(() => import('./WorkspaceOverview'));

const icons = { 'event-triggers': Zap, general: SlidersHorizontal, overview: Building2, models: Bot, tools: Wrench, skills: BookOpen, prompts: MessageSquare, memory: Brain, ui: LayoutTemplate, databricks: Cloud, mcp: Plug, 'remote-agents': Network, mlflow: Activity, 'api-keys': KeyRound, access: Users, engines: Cpu, database: Database, objects: Boxes };
const groups: { id: SettingsGroup; label: string }[] = [
  { id: 'general', label: 'General' }, { id: 'ai', label: 'AI capabilities' }, { id: 'connections', label: 'Connections' }, { id: 'administration', label: 'Administration' },
];

function SectionContent({ id, scope, onNavigate }: { id: SettingsSectionId; scope: SettingsScope; onNavigate: (id: SettingsSectionId) => void }) {
  const mode = scope === 'system' ? 'system' : 'workspace';
  switch (id) {
    case 'event-triggers': return <TriggersPanel embedded />;
    case 'general': return <GeneralSettings />;
    case 'overview': return <WorkspaceOverview embedded onConfigureSection={section => { if (section === 'mcp') onNavigate('mcp'); }} />;
    case 'models': return <ModelConfiguration mode={mode} />;
    case 'tools': return <ToolsConfiguration mode={mode} />;
    case 'mcp': return <MCPConfiguration mode={mode} />;
    case 'remote-agents': return <RemoteAgents mode={mode} />;
    case 'skills': return <SkillsConfiguration />;
    case 'prompts': return <Prompts />;
    case 'memory': return <MemoryConfiguration />;
    case 'ui': return <UIConfigurator />;
    case 'databricks': return <DatabricksConfiguration />;
    case 'mlflow': return <MLflowConfiguration />;
    case 'api-keys': return <APIKeys />;
    case 'access': return <AccessManagement />;
    case 'engines': return <EnginesConfiguration />;
    case 'database': return <DatabaseManagement />;
    case 'objects': return <ObjectManagement />;
  }
}

export default function Configuration({ onClose }: { onClose?: () => void }) {
  const { t } = useTranslation();
  const eventTriggersEnabled = useEventTriggersStore(state => state.enabled);
  const loadEventTriggers = useEventTriggersStore(state => state.load);
  useEffect(() => { void loadEventTriggers(); }, [loadEventTriggers]);
  const dark = useThemeStore(state => state.isDarkMode);
  const { userRole, isLoading, isSystemAdmin, isPersonalWorkspaceManager, loadPermissions } = usePermissionStore();
  const email = useUserStore(state => state.currentUser?.email);
  const groupId = useGroupStore(state => state.currentGroupId);
  const teamspaces = useGroupStore(state => state.groups);
  const fetchMyGroups = useGroupStore(state => state.fetchMyGroups);
  const currentGroup = useGroupStore(state => state.getCurrentGroup());
  const permissionsReady = !isLoading && (isSystemAdmin || isPersonalWorkspaceManager || userRole !== null);
  const isWorkspaceAdmin = isSystemAdmin || userRole === 'admin' || (!!groupId?.startsWith('user_') && isPersonalWorkspaceManager);
  const access = useMemo(() => ({ isSystemAdmin, isWorkspaceAdmin, isEditor: userRole === 'editor' }), [isSystemAdmin, isWorkspaceAdmin, userRole]);
  const [scope, setScope] = useState<SettingsScope>('workspace');
  const [selected, setSelected] = useState<SettingsSectionId>(() => readSettingsNavigation()?.section || 'overview');
  const [switchingTeamspace, setSwitchingTeamspace] = useState(false);
  const [switchError, setSwitchError] = useState('');
  const [query, setQuery] = useState('');
  const contentRef = useRef<HTMLDivElement>(null);
  const workspaceName = currentGroup?.name || 'Current teamspace';
  const canConfigureCurrent = getSettingsSections('workspace', access).length > 0;
  const administeredTeamspaces = teamspaces.filter(group => group.status === 'active' && (
    (group.id === groupId && canConfigureCurrent) || isSystemAdmin || group.user_role === 'ADMIN' ||
    (group.id.startsWith('user_') && isPersonalWorkspaceManager)
  ));
  const scopeOptions = [
    { id: 'personal', scope: 'personal' as const, label: 'My preferences', icon: UserRound },
    ...administeredTeamspaces.map(group => ({ id: `workspace:${group.id}`, scope: 'workspace' as const, label: group.name, icon: Building2 })),
    ...(canConfigureCurrent && !administeredTeamspaces.some(group => group.id === groupId) ? [{ id: `workspace:${groupId || ''}`, scope: 'workspace' as const, label: workspaceName, icon: Building2 }] : []),
    ...(isSystemAdmin ? [{ id: 'system', scope: 'system' as const, label: 'System administration', icon: ShieldCheck }] : []),
  ];
  const activeScope = (scope === 'workspace' && !canConfigureCurrent) || (scope === 'system' && !isSystemAdmin) ? 'personal' : scope;
  const scopeValue = activeScope === 'workspace' ? `workspace:${groupId || ''}` : activeScope;
  const sections = useMemo(() => getSettingsSections(activeScope, access).filter(section => section.id !== 'event-triggers' || eventTriggersEnabled), [activeScope, access, eventTriggersEnabled]);
  const active = sections.find(section => section.id === selected) || sections[0];
  const label = (section: SettingsSection) => t(`configuration.${section.key}`, { defaultValue: section.title });
  const normalizedQuery = query.trim().toLocaleLowerCase();
  const filtered = sections.filter(section => `${label(section)} ${section.title} ${section.description} ${section.group}`.toLocaleLowerCase().includes(normalizedQuery));
  const scopeLabel = scopeOptions.find(option => option.id === scopeValue)?.label;

  useEffect(() => { clearSettingsNavigation(); }, []);
  useEffect(() => { if (email) void fetchMyGroups(); }, [email, fetchMyGroups]);
  useEffect(() => { void loadPermissions(); }, [email, groupId, loadPermissions]);
  useEffect(() => { contentRef.current?.scrollTo?.({ top: 0 }); }, [active?.id, activeScope, groupId]);
  useEffect(() => {
    const handler = (event: Event) => {
      const section = (event as CustomEvent<{ section?: string }>).detail?.section;
      // Retain the existing tools → API Keys shortcut, with stable IDs for new callers.
      const id = section === 'API Keys' ? 'api-keys' : section;
      const target = getSettingsSections('workspace', access).find(item => item.id === id);
      if (target) { setScope('workspace'); setSelected(target.id); setQuery(''); }
    };
    window.addEventListener('kasal:navigate-config', handler);
    return () => window.removeEventListener('kasal:navigate-config', handler);
  }, [access]);
  const changeScope = (value: string) => {
    const option = scopeOptions.find(item => item.id === value);
    if (!option) return;
    setQuery(''); setSwitchError('');
    if (option.scope !== 'workspace') { setScope(option.scope); return; }
    const targetId = value.slice('workspace:'.length);
    if (targetId === groupId) { setScope('workspace'); return; }
    setSwitchingTeamspace(true);
    try { switchSettingsTeamspace(targetId, active.id); }
    catch (error) { setSwitchError(error instanceof Error ? error.message : 'Could not switch teamspace.'); setSwitchingTeamspace(false); }
  };

  return <Box data-testid="settings-workspace" sx={{ height: '100%', minHeight: 0, display: 'flex', flexDirection: 'column', ...kasalStageSurface(dark), color: 'text.primary' }}>
    <Box component="header" sx={{ display: 'flex', alignItems: 'center', gap: 1.5, px: { xs: 2, md: 4 }, py: 2.5, flexShrink: 0 }}>
      <Box sx={{ display: 'grid', placeItems: 'center', width: 38, height: 38, borderRadius: 3, bgcolor: 'action.hover' }}><Settings2 size={20} /></Box>
      <Box sx={{ flex: 1 }}><Typography component="h1" sx={{ fontSize: 20, fontWeight: 650 }}>Settings</Typography><Typography sx={{ fontSize: 12, color: 'text.secondary' }}>Your preferences, teamspace, and connected services</Typography></Box>
      {onClose && <IconButton aria-label="Close settings" onClick={onClose}><X size={20} /></IconButton>}
    </Box>
    {switchError && <Alert severity="error" sx={{ mx: 4, mb: 2 }}>{switchError}</Alert>}
    {!permissionsReady || switchingTeamspace ? <Box role="status" sx={{ flex: 1, display: 'flex', gap: 1.5, alignItems: 'center', justifyContent: 'center' }}><CircularProgress size={20} color="inherit" /><Typography color="text.secondary">{switchingTeamspace ? 'Switching teamspace…' : 'Loading settings…'}</Typography></Box> :
    <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: 'row' }, flex: 1, minHeight: 0, gap: { xs: 1, md: 4 }, px: { xs: 2, md: 4 }, pb: { xs: 2, md: 4 } }}>
      <Box component="nav" aria-label="Settings sections" sx={{ width: { xs: '100%', md: 240 }, flexShrink: 0, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        <FormControl fullWidth size="small" sx={{ mt: 1, mb: 2 }}>
          <InputLabel id="settings-scope-label">Settings for</InputLabel>
          <Select labelId="settings-scope-label" label="Settings for" value={scopeValue} onChange={event => changeScope(event.target.value)} sx={{ borderRadius: 3, fontSize: 13, '& fieldset': { border: 0 }, bgcolor: 'action.hover' }}>
            {scopeOptions.map(option => <MenuItem key={option.id} value={option.id}><Box sx={{ display: 'flex', alignItems: 'center', gap: 1, minWidth: 0 }}><option.icon size={16} style={{ flexShrink: 0 }} /><Typography noWrap sx={{ fontSize: 13 }}>{option.label}</Typography></Box></MenuItem>)}
          </Select>
        </FormControl>
        <Box sx={{ display: { xs: 'none', md: 'flex' }, flexDirection: 'column', flex: 1, minHeight: 0 }}>
          <TextField size="small" placeholder="Search settings" value={query} onChange={event => setQuery(event.target.value)} inputProps={{ 'aria-label': 'Search settings' }}
            InputProps={{ startAdornment: <InputAdornment position="start"><Search size={15} /></InputAdornment>, endAdornment: query ? <IconButton size="small" aria-label="Clear settings search" onClick={() => setQuery('')}><X size={14} /></IconButton> : undefined }}
            sx={{ mb: 1, '& .MuiOutlinedInput-root': { borderRadius: 3, fontSize: 13, '& fieldset': { border: 0 }, bgcolor: 'action.hover' } }} />
          <Box sx={{ flex: 1, overflowY: 'auto', minHeight: 0, pr: 0.5 }}>
            {groups.map(group => {
              const items = filtered.filter(section => section.group === group.id);
              if (!items.length) return null;
              return <Box key={group.id} sx={{ mt: 2 }}><Typography sx={{ px: 1.5, mb: 0.75, fontSize: 11, fontWeight: 600, color: 'text.secondary' }}>{group.label}</Typography>
                <List disablePadding>{items.map(section => {
                  const Icon = icons[section.id];
                  return <ListItemButton key={section.id} id={`settings-nav-${section.id}`} selected={active.id === section.id} aria-current={active.id === section.id ? 'page' : undefined} onClick={() => setSelected(section.id)} sx={{ py: 0.8, px: 1.5, mb: 0.25, borderRadius: 2.5, '&.Mui-selected': { bgcolor: 'action.selected' } }}>
                    <ListItemIcon sx={{ minWidth: 30, color: active.id === section.id ? 'text.primary' : 'text.secondary' }}><Icon size={17} strokeWidth={1.6} /></ListItemIcon>
                    <ListItemText primary={label(section)} primaryTypographyProps={{ fontSize: 13, fontWeight: active.id === section.id ? 600 : 400 }} />
                  </ListItemButton>;
                })}</List>
              </Box>;
            })}
            {!filtered.length && <Box role="status" sx={{ p: 2 }}><Typography variant="body2" color="text.secondary">No settings found in {scopeLabel}.</Typography><Button size="small" color="inherit" onClick={() => setQuery('')}>Clear search</Button></Box>}
          </Box>
        </Box>
        <FormControl size="small" fullWidth sx={{ display: { xs: 'flex', md: 'none' } }}><InputLabel id="settings-section-label">Section</InputLabel><Select labelId="settings-section-label" label="Section" value={active.id} onChange={event => setSelected(event.target.value as SettingsSectionId)}>{sections.map(section => <MenuItem key={section.id} value={section.id}>{label(section)}</MenuItem>)}</Select></FormControl>
      </Box>
      <Box component="main" ref={contentRef} aria-label={`${label(active)} settings`} sx={{ flex: 1, minWidth: 0, minHeight: 0, overflow: 'auto', px: { xs: 0, md: 2 }, pt: { xs: 2, md: 0.5 } }}>
        <Box sx={{ maxWidth: ['general', 'databricks', 'mlflow', 'memory', 'engines', 'ui'].includes(active.id) ? 880 : 1180, mx: 'auto', pb: 3 }}>
          <Typography sx={{ fontSize: 11, color: 'text.secondary', mb: 0.75 }}>{scopeLabel}</Typography>
          <Typography component="h2" sx={{ fontSize: 26, fontWeight: 650, letterSpacing: '-0.025em' }}>{label(active)}</Typography>
          <Typography sx={{ fontSize: 14, color: 'text.secondary', mt: 0.75, mb: 4 }}>{active.description}</Typography>
          <SettingsContent>
            <Suspense fallback={<Box role="status" sx={{ py: 5, display: 'flex', alignItems: 'center', gap: 1.5 }}><CircularProgress size={18} color="inherit" /><Typography variant="body2">Loading {label(active).toLocaleLowerCase()}…</Typography></Box>}>
              <SectionContent key={`${groupId}:${activeScope}:${active.id}`} id={active.id} scope={activeScope} onNavigate={setSelected} />
            </Suspense>
          </SettingsContent>
        </Box>
      </Box>
    </Box>}
  </Box>;
}
