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
import StorageIcon from '@mui/icons-material/Storage';
import MemoryIcon from '@mui/icons-material/MemoryRounded';
import CloudIcon from '@mui/icons-material/Cloud';
import EngineeringIcon from '@mui/icons-material/Engineering';
import CloseIcon from '@mui/icons-material/Close';
import GroupIcon from '@mui/icons-material/Group';
import { useTranslation } from 'react-i18next';
import { LanguageService } from '../../api/LanguageService';
import { ThemeConfig as _ThemeConfig } from '../../api/ThemeService';
import { useThemeStore } from '../../store/theme';
import ModelConfiguration from './Models';
import APIKeys from './APIKeys/APIKeys';
import ObjectManagement from './ObjectManagement';
import ToolForm from '../Tools/ToolForm';
import PromptConfiguration from './PromptConfiguration';
import DatabricksConfiguration from './DatabricksConfiguration';
import DatabaseConfiguration from './Database';
import MemoryManagement from './Memory/MemoryManagement';
import MCPConfiguration from './MCP/MCPConfiguration';
import EnginesConfiguration from './Engines';
import TenantManagement from './TenantManagement';
import DeveloperMode from './DeveloperMode';
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
}

function Configuration({ onClose }: ConfigurationProps): JSX.Element {
  const { t } = useTranslation();
  const [currentLanguage, setCurrentLanguage] = useState<string>('en');
  const { currentTheme, changeTheme } = useThemeStore();
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error',
  });
  const [activeSection, setActiveSection] = useState(0);

  // Navigation items
  const navItems: NavItem[] = [
    {
      label: t('configuration.general.title', { defaultValue: 'General' }),
      icon: <TranslateIcon fontSize="small" />,
      index: 0
    },
    {
      label: t('configuration.tenants.tab', { defaultValue: 'Tenants' }),
      icon: <GroupIcon fontSize="small" />,
      index: 1
    },
    {
      label: t('configuration.engines.tab', { defaultValue: 'Engines' }),
      icon: <EngineeringIcon fontSize="small" />,
      index: 2
    },
    {
      label: t('configuration.mcpServers.tab', { defaultValue: 'MCP Servers' }),
      icon: <CloudIcon fontSize="small" />,
      index: 3
    },
    {
      label: t('configuration.models.tab', { defaultValue: 'Models' }),
      icon: <ModelIcon fontSize="small" />,
      index: 4
    },
    {
      label: t('configuration.databricks.tab', { defaultValue: 'Databricks' }),
      icon: <KeyIcon fontSize="small" />,
      index: 5
    },
    {
      label: t('configuration.apiKeys.tab', { defaultValue: 'API Keys' }),
      icon: <KeyIcon fontSize="small" />,
      index: 6
    },
    {
      label: t('configuration.tools.tab', { defaultValue: 'Tools' }),
      icon: <BuildIcon fontSize="small" />,
      index: 7
    },
    {
      label: t('configuration.objects.tab', { defaultValue: 'Object Management' }),
      icon: <CodeIcon fontSize="small" />,
      index: 8
    },
    {
      label: t('configuration.prompts.tab', { defaultValue: 'Prompts' }),
      icon: <TextFormatIcon fontSize="small" />,
      index: 9
    },
    {
      label: t('configuration.database.tab', { defaultValue: 'Database' }),
      icon: <StorageIcon fontSize="small" />,
      index: 10
    },
    {
      label: t('configuration.memory.tab', { defaultValue: 'Memory' }),
      icon: <MemoryIcon fontSize="small" />,
      index: 11
    }
  ];

  useEffect(() => {
    const loadConfig = async () => {
      try {
        const languageService = LanguageService.getInstance();
        const currentLang = await languageService.getCurrentLanguage();
        setCurrentLanguage(currentLang);
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
            {navItems.map((item) => (
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
          <ContentPanel value={activeSection} index={0}>
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

          <ContentPanel value={activeSection} index={1}>
            <DeveloperMode />
            <TenantManagement />
          </ContentPanel>

          <ContentPanel value={activeSection} index={2}>
            <EnginesConfiguration />
          </ContentPanel>

          <ContentPanel value={activeSection} index={3}>
            <MCPConfiguration />
          </ContentPanel>
          
          <ContentPanel value={activeSection} index={4}>
            <ModelConfiguration />
          </ContentPanel>

          <ContentPanel value={activeSection} index={5}>
            <DatabricksConfiguration onSaved={onClose} />
          </ContentPanel>

          <ContentPanel value={activeSection} index={6}>
            <APIKeys />
          </ContentPanel>

          <ContentPanel value={activeSection} index={7}>
            <ToolForm />
          </ContentPanel>

          <ContentPanel value={activeSection} index={8}>
            <ObjectManagement />
          </ContentPanel>

          <ContentPanel value={activeSection} index={9}>
            <PromptConfiguration />
          </ContentPanel>

          <ContentPanel value={activeSection} index={10}>
            <DatabaseConfiguration />
          </ContentPanel>

          <ContentPanel value={activeSection} index={11}>
            <MemoryManagement />
          </ContentPanel>
        </Box>
      </Box>

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