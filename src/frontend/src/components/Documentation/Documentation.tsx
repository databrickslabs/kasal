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
  Home as HomeIcon
} from '@mui/icons-material';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { useNavigate } from 'react-router-dom';
import mermaid from 'mermaid';

interface DocSection {
  label: string;
  items: { label: string; file: string }[];
}

const docSections: DocSection[] = [
 
  {
    label: 'Architecture',
    items: [
      { label: 'Solution Architecture', file: 'ARCHITECTURE_GUIDE' },
    ],
  },
  {
    label: 'Development',
    items: [
      { label: 'Developer Guide', file: 'DEVELOPER_GUIDE' },
      { label: 'Code structure', file: 'CODE_STRUCTURE_GUIDE' },
      { label: 'API Reference', file: 'API_REFERENCE' },
    ],
  },
  
];

const Documentation: React.FC = () => {
  const [currentDoc, setCurrentDoc] = useState<string>('README');
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

  // Initialize Mermaid diagrams whenever the markdown content changes
  useEffect(() => {
    if (!loading && docContent) {
      try {
        mermaid.initialize({ startOnLoad: false, securityLevel: 'loose' });
        // Run after the next paint to ensure markdown is in the DOM
        window.requestAnimationFrame(() => {
          mermaid.run();
        });
      } catch (e) {
        // no-op: if mermaid fails, the raw code block will still be shown
      }
    }
  }, [docContent, loading]);

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
          <ListItemButton onClick={() => handleDocSelect('README')}>
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
                img: (props) => (
                  <img
                    {...(props as any)}
                    style={{
                      maxWidth: '100%',
                      height: 'auto',
                      display: 'block',
                      marginTop: 16,
                      marginBottom: 16,
                    }}
                  />
                ),
                a: ({ href, children, ...props }: { href?: string; children?: React.ReactNode }) => {
                  if (!href) return <a {...(props as any)}>{children}</a>;

                  // Intra-page anchors (e.g., #section)
                  if (href.startsWith('#')) {
                    return (
                      <Box
                        component="a"
                        href={href}
                        sx={{
                          color: theme.palette.primary.main,
                          textDecoration: 'underline',
                          '&:hover': { color: theme.palette.primary.dark },
                        }}
                        {...(props as any)}
                      >
                        {children}
                      </Box>
                    );
                  }

                  const isExternal = /^https?:\/\//i.test(href);
                  const isMd = href.toLowerCase().endsWith('.md');

                  // Normalize internal doc links to our loader (expects filename without .md)
                  if (!isExternal && isMd) {
                    // Strip common prefixes like /docs/ or src/docs/
                    const normalized = href
                      .replace(/^\/?src\/docs\//i, '')
                      .replace(/^\/?docs\//i, '');
                    const base = (normalized.split('/').pop() || normalized).replace(/\.md$/i, '');
                    return (
                      <Box
                        component="a"
                        href="#"
                        onClick={(e: React.MouseEvent) => {
                          e.preventDefault();
                          handleDocSelect(base);
                        }}
                        sx={{
                          color: theme.palette.primary.main,
                          textDecoration: 'underline',
                          cursor: 'pointer',
                          '&:hover': { color: theme.palette.primary.dark },
                        }}
                      >
                        {children}
                      </Box>
                    );
                  }

                  // External links open in new tab
                  return (
                    <Box
                      component="a"
                      href={href}
                      target="_blank"
                      rel="noopener noreferrer"
                      sx={{
                        color: theme.palette.primary.main,
                        textDecoration: 'underline',
                        '&:hover': { color: theme.palette.primary.dark },
                      }}
                      {...(props as any)}
                    >
                      {children}
                    </Box>
                  );
                },
                code: ({ children, inline, className, ...props }: { children?: React.ReactNode; inline?: boolean; className?: string }) => {
                  const languageMatch = /language-(\w+)/.exec(className || '');
                  const language = languageMatch ? languageMatch[1] : undefined;

                  // Render Mermaid diagrams as <div class="mermaid"> blocks
                  if (!inline && language === 'mermaid') {
                    const diagram = String(children || '').replace(/\n$/, '');
                    return (
                      <Box component="div" className="mermaid" sx={{ my: 2 }}>
                        {diagram}
                      </Box>
                    );
                  }

                  // Default code rendering (inline vs block)
                  if (inline) {
                    return (
                      <Box
                        component="code"
                        sx={{
                          backgroundColor: theme.palette.background.subtle,
                          border: '1px solid',
                          borderColor: theme.palette.divider,
                          borderRadius: 1,
                          px: 0.5,
                          py: 0.25,
                          fontSize: '0.875rem',
                          fontFamily: 'monospace',
                        }}
                      >
                        {children}
                      </Box>
                    );
                  }

                  return (
                    <Box
                      component="pre"
                      sx={{
                        backgroundColor: theme.palette.background.subtle,
                        border: '1px solid',
                        borderColor: theme.palette.divider,
                        borderRadius: 1,
                        p: 2,
                        overflow: 'auto',
                        fontSize: '0.875rem',
                        fontFamily: 'monospace',
                      }}
                    >
                      <code>{children}</code>
                    </Box>
                  );
                },
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