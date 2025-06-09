import React, { useState, useEffect } from 'react';
import { 
  Box, 
  Typography, 
  CircularProgress, 
  Drawer, 
  List, 
  ListItem, 
  ListItemButton, 
  ListItemText,
  Collapse,
  AppBar,
  Toolbar,
  IconButton,
  useTheme,
  useMediaQuery
} from '@mui/material';
import { 
  ExpandLess, 
  ExpandMore, 
  Menu as MenuIcon,
  GitHub as GitHubIcon,
  Home as HomeIcon
} from '@mui/icons-material';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { useNavigate } from 'react-router-dom';

interface DocSection {
  label: string;
  items: { label: string; file: string }[];
}

const docSections: DocSection[] = [
  {
    label: 'Getting Started',
    items: [
      { label: 'Installation', file: 'GETTING_STARTED' },
      { label: 'Best Practices', file: 'BEST_PRACTICES' },
    ],
  },
  {
    label: 'Architecture',
    items: [
      { label: 'Overview', file: 'ARCHITECTURE' },
      { label: 'Authorization', file: 'AUTHORIZATION' },
      { label: 'Security Model', file: 'SECURITY_MODEL' },
      { label: 'Database Migrations', file: 'DATABASE_MIGRATIONS' },
      { label: 'Database Seeding', file: 'DATABASE_SEEDING' },
      { label: 'Models', file: 'MODELS' },
      { label: 'Schemas', file: 'SCHEMAS' },
      { label: 'Schema Structure', file: 'SCHEMAS_STRUCTURE' },
    ],
  },
  {
    label: 'Backend Features',
    items: [
      { label: 'CrewAI Engine', file: 'CREWAI_ENGINE' },
      { label: 'LLM Manager', file: 'LLM_MANAGER' },
      { label: 'Logging', file: 'LOGGING' },
      { label: 'Tasks', file: 'TASKS' },
      { label: 'Agents', file: 'AGENTS' },
    ],
  },
  {
    label: 'API & Usage',
    items: [
      { label: 'REST API', file: 'API' },
      { label: 'Shortcuts', file: 'SHORTCUTS' },
    ],
  },
  {
    label: 'Deployment',
    items: [
      { label: 'Deployment Guide', file: 'DEPLOYMENT_GUIDE' },
    ],
  },
];

const Documentation: React.FC = () => {
  const [currentDoc, setCurrentDoc] = useState<string>('index');
  const [docContent, setDocContent] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(true);
  const [openSections, setOpenSections] = useState<{ [key: string]: boolean }>({});
  const [mobileOpen, setMobileOpen] = useState(false);
  
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));
  const navigate = useNavigate();

  const drawerWidth = 280;

  useEffect(() => {
    loadDocument(currentDoc);
  }, [currentDoc]);

  const loadDocument = async (filename: string) => {
    setLoading(true);
    try {
      // Import the markdown file dynamically
      const response = await fetch(`/docs/${filename}.md`);
      if (response.ok) {
        const content = await response.text();
        setDocContent(content);
      } else {
        setDocContent(`# Document Not Found\n\nThe document "${filename}.md" could not be loaded.`);
      }
    } catch (error) {
      setDocContent(`# Error Loading Document\n\nFailed to load "${filename}.md": ${error}`);
    } finally {
      setLoading(false);
    }
  };

  const handleSectionToggle = (section: string) => {
    setOpenSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };

  const handleDocSelect = (filename: string) => {
    setCurrentDoc(filename);
    if (isMobile) {
      setMobileOpen(false);
    }
  };

  const handleDrawerToggle = () => {
    setMobileOpen(!mobileOpen);
  };

  const drawer = (
    <Box sx={{ overflow: 'auto' }}>
      <Toolbar>
        <Typography variant="h6" noWrap component="div">
          Kasal Docs
        </Typography>
      </Toolbar>
      <List>
        <ListItem disablePadding>
          <ListItemButton onClick={() => handleDocSelect('index')}>
            <HomeIcon sx={{ mr: 1 }} />
            <ListItemText primary="Home" />
          </ListItemButton>
        </ListItem>
        
        {docSections.map((section) => (
          <Box key={section.label}>
            <ListItemButton onClick={() => handleSectionToggle(section.label)}>
              <ListItemText primary={section.label} />
              {openSections[section.label] ? <ExpandLess /> : <ExpandMore />}
            </ListItemButton>
            <Collapse in={openSections[section.label]} timeout="auto" unmountOnExit>
              <List component="div" disablePadding>
                {section.items.map((item) => (
                  <ListItemButton
                    key={item.file}
                    sx={{ pl: 4 }}
                    onClick={() => handleDocSelect(item.file)}
                    selected={currentDoc === item.file}
                  >
                    <ListItemText primary={item.label} />
                  </ListItemButton>
                ))}
              </List>
            </Collapse>
          </Box>
        ))}
      </List>
    </Box>
  );

  return (
    <Box sx={{ display: 'flex', height: '100vh' }}>
      <AppBar
        position="fixed"
        sx={{
          width: { md: `calc(100% - ${drawerWidth}px)` },
          ml: { md: `${drawerWidth}px` },
        }}
      >
        <Toolbar>
          <IconButton
            color="inherit"
            aria-label="open drawer"
            edge="start"
            onClick={handleDrawerToggle}
            sx={{ mr: 2, display: { md: 'none' } }}
          >
            <MenuIcon />
          </IconButton>
          <Typography variant="h6" noWrap component="div" sx={{ flexGrow: 1 }}>
            Kasal Documentation
          </Typography>
          <IconButton color="inherit" onClick={() => navigate('/workflow')}>
            <HomeIcon />
          </IconButton>
          <IconButton 
            color="inherit"
            component="a"
            href="https://github.com/yourusername/kasal"
            target="_blank"
          >
            <GitHubIcon />
          </IconButton>
        </Toolbar>
      </AppBar>
      
      <Box
        component="nav"
        sx={{ width: { md: drawerWidth }, flexShrink: { md: 0 } }}
      >
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={handleDrawerToggle}
          ModalProps={{
            keepMounted: true,
          }}
          sx={{
            display: { xs: 'block', md: 'none' },
            '& .MuiDrawer-paper': { boxSizing: 'border-box', width: drawerWidth },
          }}
        >
          {drawer}
        </Drawer>
        <Drawer
          variant="permanent"
          sx={{
            display: { xs: 'none', md: 'block' },
            '& .MuiDrawer-paper': { boxSizing: 'border-box', width: drawerWidth },
          }}
          open
        >
          {drawer}
        </Drawer>
      </Box>
      
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          width: { md: `calc(100% - ${drawerWidth}px)` },
          mt: '64px',
          overflow: 'auto',
        }}
      >
        {loading ? (
          <Box display="flex" justifyContent="center" alignItems="center" height="50vh">
            <CircularProgress />
            <Typography sx={{ ml: 2 }}>Loading Documentation...</Typography>
          </Box>
        ) : (
          <Box sx={{ maxWidth: '800px', mx: 'auto' }}>
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                h1: ({ children }) => (
                  <Typography variant="h3" component="h1" gutterBottom sx={{ color: 'primary.main', fontWeight: 700 }}>
                    {children}
                  </Typography>
                ),
                h2: ({ children }) => (
                  <Typography variant="h4" component="h2" gutterBottom sx={{ color: 'primary.dark', fontWeight: 600, mt: 3 }}>
                    {children}
                  </Typography>
                ),
                h3: ({ children }) => (
                  <Typography variant="h5" component="h3" gutterBottom sx={{ fontWeight: 600, mt: 2 }}>
                    {children}
                  </Typography>
                ),
                p: ({ children }) => (
                  <Typography variant="body1" paragraph sx={{ lineHeight: 1.7 }}>
                    {children}
                  </Typography>
                ),
                code: ({ children, ...props }: any) => (
                  props.inline ? (
                    <Box
                      component="code"
                      sx={{
                        backgroundColor: 'grey.100',
                        border: '1px solid',
                        borderColor: 'grey.300',
                        borderRadius: 1,
                        px: 0.5,
                        py: 0.25,
                        fontSize: '0.875rem',
                        fontFamily: 'monospace',
                      }}
                    >
                      {children}
                    </Box>
                  ) : (
                    <Box
                      component="pre"
                      sx={{
                        backgroundColor: 'grey.100',
                        border: '1px solid',
                        borderColor: 'grey.300',
                        borderRadius: 1,
                        p: 2,
                        overflow: 'auto',
                        fontSize: '0.875rem',
                        fontFamily: 'monospace',
                      }}
                    >
                      <code>{children}</code>
                    </Box>
                  )
                ),
              }}
            >
              {docContent}
            </ReactMarkdown>
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default Documentation;