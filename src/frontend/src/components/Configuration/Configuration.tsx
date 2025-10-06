import React, { useState, useEffect } from 'react';
import {
  Typography,
  Box,
  Alert,
  Snackbar,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  SelectChangeEvent,
  List,
  ListItemIcon,
  ListItemText,
  Paper,
  ListItemButton,
  IconButton,
} from '@mui/material';
import SettingsIcon from '@mui/icons-material/Settings';
import TranslateIcon from '@mui/icons-material/Translate';
import DarkModeIcon from '@mui/icons-material/DarkMode';
import ModelIcon from '@mui/icons-material/Psychology';
import KeyIcon from '@mui/icons-material/Key';
import BuildIcon from '@mui/icons-material/Build';
import CodeIcon from '@mui/icons-material/Code';
import TextFormatIcon from '@mui/icons-material/TextFormat';
import CloudIcon from '@mui/icons-material/Cloud';
import EngineeringIcon from '@mui/icons-material/Engineering';
import CloseIcon from '@mui/icons-material/Close';
import MemoryIcon from '@mui/icons-material/Memory';
import StorageIcon from '@mui/icons-material/Storage';
import WorkspacesIcon from '@mui/icons-material/Workspaces';
import BarChartIcon from '@mui/icons-material/BarChart';
// import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import { useTranslation } from 'react-i18next';
import { LanguageService } from '../../api/LanguageService';
import { ThemeConfig as _ThemeConfig } from '../../api/ThemeService';
import { useThemeStore } from '../../store/theme';
import { usePermissionStore } from '../../store/permissions';
import { useUserStore } from '../../store/user';
import ModelConfiguration from './Models';
import APIKeys from './APIKeys/APIKeys';
import ObjectManagement from './ObjectManagement';
import ToolsConfiguration from './Tools/ToolsConfiguration';
import PromptConfiguration from './PromptConfiguration';
import DatabricksConfiguration from './DatabricksConfiguration';
import PowerBIConfiguration from './PowerBIConfiguration';
import MCPConfiguration from './MCP/MCPConfiguration';
import EnginesConfiguration from './Engines';
import { DatabricksOneClickSetup } from '../MemoryBackend';
import DatabaseManagement from './DatabaseManagement';
import GroupManagement from './GroupManagement';
import WorkspaceOverview from './WorkspaceOverview';
import UserPermissionManagement from './UserPermissionManagement';
// import DSPyConfiguration from './DSPyConfiguration'; // Temporarily disabled
import { LANGUAGES } from '../../config/i18n/config';

interface ConfigurationProps {
  onClose?: () => void;
}

interface ContentPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function ContentPanel(props: ContentPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`config-panel-${index}`}
      aria-labelledby={`config-nav-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ py: 2 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

// Navigation item interface
interface NavItem {
  label: string;
  icon: JSX.Element;
  index: number;
  group: 'general' | 'system' | 'workspace';
}

function Configuration({ onClose }: ConfigurationProps): JSX.Element {
  const { t } = useTranslation();
  const [currentLanguage, setCurrentLanguage] = useState<string>('en');
  const { currentTheme, changeTheme } = useThemeStore();

  // Get permission state from store and selected group ID
  const {
    userRole,
    isLoading: permissionsLoading,
    loadPermissions,
    isSystemAdmin: storeIsSystemAdmin,
    isPersonalWorkspaceManager
  } = usePermissionStore(state => ({
    userRole: state.userRole,
    isLoading: state.isLoading,
    loadPermissions: state.loadPermissions,
    isSystemAdmin: state.isSystemAdmin,
    isPersonalWorkspaceManager: state.isPersonalWorkspaceManager
  }));

  // Get current user to watch for changes
  const currentUser = useUserStore(state => state.currentUser);

  // Get selected group ID to determine workspace context
  const selectedGroupId = localStorage.getItem('selectedGroupId');
  const isPersonalWorkspace = selectedGroupId && selectedGroupId.startsWith('user_');

  // Check if permissions have been loaded at least once
  // System admins should always have access, others need their role loaded
  const permissionsReady = !permissionsLoading && (
    storeIsSystemAdmin || // System admins always ready
    isPersonalWorkspaceManager || // Personal workspace managers always ready
    userRole !== null // Others need group role loaded
  );

  // Check if user has admin permissions
  // Workspace admin: Either admin role in current workspace OR owner of personal workspace OR personal workspace manager in personal workspace
  const isWorkspaceAdmin = storeIsSystemAdmin || (permissionsReady && (
    userRole === 'admin' ||
    (isPersonalWorkspace && isPersonalWorkspaceManager)
  ));
  const isSystemAdmin = storeIsSystemAdmin; // System admin status is not dependent on group permissions
  const isAdmin = permissionsReady && userRole === 'admin';
  const isOperator = permissionsReady && userRole === 'operator';
  const isEditor = permissionsReady && userRole === 'editor';

  // Debug logging
  useEffect(() => {
    console.log('Configuration - Permissions loading:', permissionsLoading);
    console.log('Configuration - Permissions ready:', permissionsReady);
    console.log('Configuration - User role:', userRole);
    console.log('Configuration - Store isSystemAdmin:', storeIsSystemAdmin);
    console.log('Configuration - Store isPersonalWorkspaceManager:', isPersonalWorkspaceManager);
    console.log('Configuration - Selected group ID:', selectedGroupId);
    console.log('Configuration - Is personal workspace:', isPersonalWorkspace);
    console.log('Configuration - Is workspace admin:', isWorkspaceAdmin);
    console.log('Configuration - Is system admin (computed):', isSystemAdmin);
    console.log('Configuration - Is admin:', isAdmin);
    console.log('Configuration - Is operator:', isOperator);
    console.log('Configuration - Is editor:', isEditor);
  }, [permissionsReady, userRole, isAdmin, isOperator, isEditor, isWorkspaceAdmin, isSystemAdmin, isPersonalWorkspace, selectedGroupId, permissionsLoading, storeIsSystemAdmin, isPersonalWorkspaceManager]);

  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error',
  });
  const [activeSection, setActiveSection] = useState(0);
  // Always show Database Management now (no permission check needed)
  // Future: Will check for admin group membership
  const [showDatabaseManagement, setShowDatabaseManagement] = useState(true);

  // Build navigation items dynamically based on permissions
  const navItems: NavItem[] = React.useMemo(() => {
    let currentIndex = 0;
    const baseNavItems: NavItem[] = [];

    // General section - always visible
    baseNavItems.push({
      label: t('configuration.general.title', { defaultValue: 'General' }),
      icon: <TranslateIcon fontSize="small" />,
      index: currentIndex++,
      group: 'general'
    });

    // System admin-only sections (manage entire system)
    if (isSystemAdmin) {
      baseNavItems.push({
        label: t('configuration.workspaces.tab', { defaultValue: 'Workspaces' }),
        icon: <WorkspacesIcon fontSize="small" />,
        index: currentIndex++,
        group: 'system'
      });
      baseNavItems.push({
        label: t('configuration.engines.tab', { defaultValue: 'Engines' }),
        icon: <EngineeringIcon fontSize="small" />,
        index: currentIndex++,
        group: 'system'
      });
      baseNavItems.push({
        label: t('configuration.models.global', { defaultValue: 'Models (Global)' }),
        icon: <ModelIcon fontSize="small" />,
        index: currentIndex++,
        group: 'system'
      });
      // Tools (Global) - system-wide tools
      baseNavItems.push({
        label: t('configuration.tools.global', { defaultValue: 'Tools (Global)' }),
        icon: <BuildIcon fontSize="small" />,
        index: currentIndex++,
        group: 'system'
      });
      baseNavItems.push({
        label: t('configuration.userPermissions.tab', { defaultValue: 'User Permissions' }),
        icon: <SettingsIcon fontSize="small" />,
        index: currentIndex++,
        group: 'system'
      });
      if (showDatabaseManagement) {
        baseNavItems.push({
          label: t('configuration.database.tab', { defaultValue: 'Database Management' }),
          icon: <StorageIcon fontSize="small" />,
          index: currentIndex++,
          group: 'system'
        });
      }
    }

    // Workspace admin sections (configure workspace-specific settings)
    if (isWorkspaceAdmin) {
      baseNavItems.push({
        label: t('configuration.workspaceOverview.tab', { defaultValue: 'Workspace Overview' }),
        icon: <WorkspacesIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      baseNavItems.push({
        label: t('configuration.databricks.tab', { defaultValue: 'Databricks' }),
        icon: <CloudIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      baseNavItems.push({
        label: t('configuration.powerbi.tab', { defaultValue: 'Power BI' }),
        icon: <BarChartIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      baseNavItems.push({
        label: t('configuration.memoryBackend.tab', { defaultValue: 'Memory Backend' }),
        icon: <MemoryIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      baseNavItems.push({
        label: t('configuration.mcpServers.tab', { defaultValue: 'MCP Servers' }),
        icon: <CloudIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      // Models (Workspace) - workspace models management
      baseNavItems.push({
        label: t('configuration.models.workspace', { defaultValue: 'Models (Workspace)' }),
        icon: <ModelIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      // Tools (Workspace) - workspace tools management
      baseNavItems.push({
        label: t('configuration.tools.workspace', { defaultValue: 'Tools (Workspace)' }),
        icon: <BuildIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      // DSPy Optimization (Workspace) - temporarily disabled
      // baseNavItems.push({
      //   label: t('configuration.dspy.tab', { defaultValue: 'DSPy Optimization' }),
      //   icon: <AutoFixHighIcon fontSize="small" />,
      //   index: currentIndex++,
      //   group: 'workspace'
      // });
    }

    // Sections visible to editors and admins (but not operators) — workspace-relevant
    if (!isOperator) {
      baseNavItems.push({
        label: t('configuration.apiKeys.tab', { defaultValue: 'API Keys' }),
        icon: <KeyIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      baseNavItems.push({
        label: t('configuration.objects.tab', { defaultValue: 'Object Management' }),
        icon: <CodeIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
      baseNavItems.push({
        label: t('configuration.prompts.tab', { defaultValue: 'Prompt Instructions' }),
        icon: <TextFormatIcon fontSize="small" />,
        index: currentIndex++,
        group: 'workspace'
      });
    }

    return baseNavItems;
  }, [showDatabaseManagement, t, isSystemAdmin, isWorkspaceAdmin, isOperator]);

  // Ensure permissions are loaded on mount
  useEffect(() => {
    // Load permissions immediately if not ready
    if (!permissionsReady && !permissionsLoading) {
      console.log('Configuration - Loading permissions');
      loadPermissions();
    }
  }, [permissionsReady, permissionsLoading, loadPermissions]);

  // Reload permissions when user changes (e.g., user fetch completes)
  useEffect(() => {
    if (currentUser && currentUser.email) {
      console.log('Configuration - User changed, reloading permissions for:', currentUser.email);
      loadPermissions();
    }
  }, [currentUser, loadPermissions]);

  useEffect(() => {
    const loadConfig = async () => {
      try {
        // Load language configuration
        const languageService = LanguageService.getInstance();
        const currentLang = await languageService.getCurrentLanguage();
        setCurrentLanguage(currentLang);


        // Database Management is now always visible
        // Future: Will check for admin group membership
        setShowDatabaseManagement(true);
        console.log('Database Management enabled for all users');
      } catch (error) {
        console.error('Error loading configuration:', error);
      }
    };

    loadConfig();
  }, []);

  const handleNavChange = (index: number) => {
    setActiveSection(index);
  };

  const handleLanguageChange = async (event: SelectChangeEvent<string>) => {
    const newLanguage = event.target.value;
    try {
      const languageService = LanguageService.getInstance();
      await languageService.setLanguage(newLanguage);
      setCurrentLanguage(newLanguage);
      setNotification({
        open: true,
        message: t('configuration.language.saved', { defaultValue: 'Language changed successfully' }),
        severity: 'success',
      });
    } catch (error) {
      console.error('Error changing language:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to change language',
        severity: 'error',
      });
    }
  };

  const handleThemeChange = (event: SelectChangeEvent<string>) => {
    const newTheme = event.target.value;
    changeTheme(newTheme);
    setNotification({
      open: true,
      message: t('configuration.theme.saved', { defaultValue: 'Theme changed successfully' }),
      severity: 'success',
    });
  };

  const handleCloseNotification = () => {
    setNotification({ ...notification, open: false });
  };

  useEffect(() => {
    // Allow deep-link navigation into specific configuration sections (e.g., API Keys)
    const handler = (_evt: Event) => {
      try {


        const target = navItems.find(i => i.label === t('configuration.apiKeys.tab', { defaultValue: 'API Keys' }));
        if (target) {
          setActiveSection(target.index);
        }
      } catch (e) {
        // no-op
      }
    };
    window.addEventListener('kasal:navigate-config', handler as EventListener);
    return () => window.removeEventListener('kasal:navigate-config', handler as EventListener);
  }, [navItems, t]);
  // Group nav items for sidebar sections (for clearer separation in sidebar)
  const generalItems = React.useMemo(() => navItems.filter(i => i.group === 'general'), [navItems]);
  const systemItems = React.useMemo(() => navItems.filter(i => i.group === 'system'), [navItems]);
  const workspaceItems = React.useMemo(() => navItems.filter(i => i.group === 'workspace'), [navItems]);


  return (
    <Box sx={{
      width: '80vw',
      height: '80vh',
      mx: 'auto',
      px: 2,
      py: 1.5
    }}>

      <Box sx={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        mb: 3,
        pb: 1.5,
        borderBottom: '1px solid',
        borderColor: 'divider'
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <SettingsIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: '1.4rem' }} />
          <Typography variant="h5">{t('configuration.title')}</Typography>
        </Box>
        {onClose && (
          <IconButton
            onClick={onClose}
            size="small"
            sx={{
              color: 'text.secondary',
              '&:hover': {
                color: 'text.primary',
              }
            }}
          >
            <CloseIcon />
          </IconButton>
        )}
      </Box>

      {/* Show loading state while permissions are being determined */}
      {!permissionsReady ? (
        <Box sx={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          height: 'calc(100% - 60px)'
        }}>
          <Typography variant="body1" color="text.secondary">
            Loading permissions...
          </Typography>
        </Box>
      ) : (
        <Box sx={{
          display: 'flex',
          flexDirection: 'row',
          gap: 2,
          height: 'calc(100% - 60px)',
          overflow: 'hidden'
        }}>
        {/* Left Sidebar Navigation */}
        <Paper
          sx={{
            width: 240,
            flexShrink: 0,
            borderRadius: 1,
            height: '100%',
            overflow: 'auto'
          }}
          elevation={1}
        >
          <List sx={{ py: 1 }}>
            {/* General */}
            {generalItems.map((item) => (
              <ListItemButton
                key={item.index}
                selected={activeSection === item.index}
                onClick={() => handleNavChange(item.index)}
                sx={{
                  mb: 0.5,
                  borderRadius: 1,
                  mx: 0.5,
                  '&.Mui-selected': {
                    backgroundColor: 'action.selected',
                    '&:hover': {
                      backgroundColor: 'action.hover',
                    },
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 40 }}>
                  {item.icon}
                </ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>
            ))}

            {/* System Administration */}
            {systemItems.length > 0 && (
              <Box sx={{ px: 2, pt: 1, pb: 0.5 }}>
                <Typography variant="overline" color="text.secondary">System Administration</Typography>
              </Box>
            )}
            {systemItems.map((item) => (
              <ListItemButton
                key={item.index}
                selected={activeSection === item.index}
                onClick={() => handleNavChange(item.index)}
                sx={{
                  mb: 0.5,
                  borderRadius: 1,
                  mx: 0.5,
                  '&.Mui-selected': {
                    backgroundColor: 'action.selected',
                    '&:hover': {
                      backgroundColor: 'action.hover',
                    },
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 40 }}>
                  {item.icon}
                </ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>
            ))}

            {/* Workspace Settings */}
            {workspaceItems.length > 0 && (
              <Box sx={{ px: 2, pt: 1, pb: 0.5 }}>
                <Typography variant="overline" color="text.secondary">Workspace Settings</Typography>
              </Box>
            )}
            {workspaceItems.map((item) => (
              <ListItemButton
                key={item.index}
                selected={activeSection === item.index}
                onClick={() => handleNavChange(item.index)}
                sx={{
                  mb: 0.5,
                  borderRadius: 1,
                  mx: 0.5,
                  '&.Mui-selected': {
                    backgroundColor: 'action.selected',
                    '&:hover': {
                      backgroundColor: 'action.hover',
                    },
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 40 }}>
                  {item.icon}
                </ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>
            ))}
          </List>
        </Paper>

        {/* Content Area */}
        <Box sx={{
          flex: 1,
          bgcolor: 'background.paper',
          borderRadius: 1,
          p: 2,
          overflow: 'auto',
          height: '100%'
        }}>
          {/* Render content panels dynamically based on navigation items */}
          {navItems.map((item) => {
            // General Settings
            if (item.label === t('configuration.general.title', { defaultValue: 'General' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  {/* Language Settings */}
                  <Box sx={{ mb: 3 }}>
                    <Box sx={{
                      display: 'flex',
                      alignItems: 'center',
                      mb: 1.5
                    }}>
                      <TranslateIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.2rem' }} />
                      <Typography variant="subtitle1" fontWeight="medium">{t('configuration.language.title')}</Typography>
                    </Box>

                    <FormControl fullWidth size="small">
                      <InputLabel id="language-select-label">
                        {t('configuration.language.select')}
                      </InputLabel>
                      <Select
                        labelId="language-select-label"
                        value={currentLanguage}
                        onChange={handleLanguageChange}
                        label={t('configuration.language.select')}
                        size="small"
                      >
                        {Object.entries(LANGUAGES).map(([code, { nativeName }]) => (
                          <MenuItem key={code} value={code}>
                            {nativeName}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  </Box>

                  {/* Theme Settings */}
                  <Box sx={{ mb: 3 }}>
                    <Box sx={{
                      display: 'flex',
                      alignItems: 'center',
                      mb: 1.5
                    }}>
                      <DarkModeIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.2rem' }} />
                      <Typography variant="subtitle1" fontWeight="medium">{t('configuration.theme.title')}</Typography>
                    </Box>

                    <FormControl fullWidth size="small">
                      <InputLabel>{t('configuration.theme.select')}</InputLabel>
                      <Select
                        value={currentTheme}
                        onChange={handleThemeChange}
                        label={t('configuration.theme.select')}
                      >
                        <MenuItem value="professional">Professional (Blue)</MenuItem>
                        <MenuItem value="calmEarth">Calm Earth (Green)</MenuItem>
                        <MenuItem value="deepOcean">Deep Ocean (Dark)</MenuItem>
                        <MenuItem value="vibrantCreative">Vibrant Creative (Purple)</MenuItem>
                      </Select>
                    </FormControl>
                  </Box>
                </ContentPanel>
              );
            }

            // Workspace Overview (Workspace Admins)
            if (item.label === t('configuration.workspaceOverview.tab', { defaultValue: 'Workspace Overview' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <WorkspaceOverview />
                </ContentPanel>
              );
            }

            // Workspaces (System Admin only)
            if (item.label === t('configuration.workspaces.tab', { defaultValue: 'Workspaces' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <GroupManagement />
                </ContentPanel>
              );
            }

            // Engines (Admin only)
            if (item.label === t('configuration.engines.tab', { defaultValue: 'Engines' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <EnginesConfiguration />
                </ContentPanel>
              );
            }

            // Models (Global) - System Administration
            if (item.label === t('configuration.models.global', { defaultValue: 'Models (Global)' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <ModelConfiguration mode="system" />
                </ContentPanel>
              );
            }

            // Models (Workspace) - Workspace Settings
            if (item.label === t('configuration.models.workspace', { defaultValue: 'Models (Workspace)' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <ModelConfiguration mode="workspace" />
                </ContentPanel>
              );
            }

            // Tools (Global) - System Administration
            if (item.label === t('configuration.tools.global', { defaultValue: 'Tools (Global)' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <ToolsConfiguration mode="system" />
                </ContentPanel>
              );
            }

            // Tools (Workspace) - Workspace Settings
            if (item.label === t('configuration.tools.workspace', { defaultValue: 'Tools (Workspace)' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <ToolsConfiguration mode="workspace" />
                </ContentPanel>
              );
            }

            // DSPy Optimization - Workspace Settings (temporarily disabled)
            // if (item.label === t('configuration.dspy.tab', { defaultValue: 'DSPy Optimization' })) {
            //   return (
            //     <ContentPanel key={item.index} value={activeSection} index={item.index}>
            //       <DSPyConfiguration />
            //     </ContentPanel>
            //   );
            // }

            // User Permission Management (System Admin only)
            if (item.label === t('configuration.userPermissions.tab', { defaultValue: 'User Permissions' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <UserPermissionManagement />
                </ContentPanel>
              );
            }

            // Database Management (Admin only)
            if (item.label === t('configuration.database.tab', { defaultValue: 'Database Management' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <DatabaseManagement />
                </ContentPanel>
              );
            }

            // MCP Servers
            if (item.label === t('configuration.mcpServers.tab', { defaultValue: 'MCP Servers' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <MCPConfiguration />
                </ContentPanel>
              );
            }

            // Memory Backend
            if (item.label === t('configuration.memoryBackend.tab', { defaultValue: 'Memory Backend' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <DatabricksOneClickSetup />
                </ContentPanel>
              );
            }

            // Databricks
            if (item.label === t('configuration.databricks.tab', { defaultValue: 'Databricks' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <DatabricksConfiguration onSaved={onClose} />
                </ContentPanel>
              );
            }

            // Power BI
            if (item.label === t('configuration.powerbi.tab', { defaultValue: 'Power BI' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <PowerBIConfiguration onSaved={onClose} />
                </ContentPanel>
              );
            }

            // API Keys
            if (item.label === t('configuration.apiKeys.tab', { defaultValue: 'API Keys' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <APIKeys />
                </ContentPanel>
              );
            }

            // Object Management
            if (item.label === t('configuration.objects.tab', { defaultValue: 'Object Management' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <ObjectManagement />
                </ContentPanel>
              );
            }

            // Prompt Instructions
            if (item.label === t('configuration.prompts.tab', { defaultValue: 'Prompt Instructions' })) {
              return (
                <ContentPanel key={item.index} value={activeSection} index={item.index}>
                  <PromptConfiguration />
                </ContentPanel>
              );
            }

            return null;
          })}

        </Box>
      </Box>
      )}

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={handleCloseNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert
          onClose={handleCloseNotification}
          severity={notification.severity}
          sx={{ width: '100%' }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
}

export default Configuration;