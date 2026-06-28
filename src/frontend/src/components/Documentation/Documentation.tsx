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
    label: 'Overview',
    items: [
      { label: 'Why Kasal', file: 'WHY_KASAL' },
      { label: 'Solution Architecture', file: 'ARCHITECTURE_GUIDE' },
    ],
  },
  {
    label: 'Observability / Tracing',
    items: [
      { label: '📊 MLflow Tracing — Setup & Requirements', file: 'mlflow-tracing-setup' },
      { label: 'Lakebase Setup (Persistence)', file: 'lakebase-deployment' },
    ],
  },
  {
    label: 'Security',
    items: [
      { label: 'Security Compliance', file: 'README_SECURITY_COMPLIANCE' },
      { label: 'Security Test Guide', file: 'README_SECURITY_GUARDRAILS_TESTGUIDE' },
      { label: 'Supply Chain Security', file: 'README_SECURITY_SUPPLY_CHAIN' },
    ],
  },
  {
    label: 'Development',
    items: [
      { label: 'Developer Guide', file: 'DEVELOPER_GUIDE' },
      { label: 'Code Structure', file: 'CODE_STRUCTURE_GUIDE' },
    ],
  },
  {
    label: 'API',
    items: [
      { label: 'API Endpoints', file: 'api_endpoints' },
    ],
  },
  {
    label: 'Power BI - Overview',
    items: [
      { label: '📋 Overview & Navigation', file: 'powerbi/README' },
      { label: '🔐 Authentication & SP Setup', file: 'powerbi/01-authentication-setup' },
      { label: '📖 Simple Migration Story', file: 'powerbi/02-simple-migration-story' },
    ],
  },
  {
    label: 'Power BI - Analytics / Q&A',
    items: [
      { label: '⭐ Analytics Q&A — Case Study', file: 'powerbi/powerbi-analytics-qa-case-study' },
      { label: 'Tool 72 — Comprehensive Analysis', file: 'powerbi/tool-72-comprehensive-analysis' },
      { label: 'Tool 79 — Semantic Model Fetcher', file: 'powerbi/tool-79-semantic-model-fetcher' },
      { label: 'Tool 80 — DAX Generator', file: 'powerbi/tool-80-dax-generator' },
      { label: 'Tool 81 — Metadata Reducer', file: 'powerbi/tool-81-metadata-reducer' },
      { label: 'Tool 82 — DAX Executor', file: 'powerbi/tool-82-dax-executor' },
    ],
  },
  {
    label: 'Power BI - Migration (Extraction)',
    items: [
      { label: 'Tool 73 — Measure Conversion', file: 'powerbi/tool-73-measure-conversion' },
      { label: 'Tool 74 — M-Query Conversion', file: 'powerbi/tool-74-mquery-conversion' },
      { label: 'Tool 75 — Relationships', file: 'powerbi/tool-75-relationships' },
      { label: 'Tool 76 — Hierarchies (Fabric)', file: 'powerbi/tool-76-hierarchies' },
      { label: 'Tool 77 — Field Parameters (Fabric)', file: 'powerbi/tool-77-field-parameters' },
      { label: 'Tool 78 — Report References (Fabric)', file: 'powerbi/tool-78-report-references' },
    ],
  },
  {
    label: 'Power BI - UC Metric Views',
    items: [
      { label: '🚀 End-to-End Migration Guide', file: 'powerbi/ucmv-migration-guide' },
      { label: 'Tool 85 — DAX to SQL Translator', file: 'powerbi/tool-85-dax-to-sql-translator' },
      { label: 'Tool 86 — UC Metric View Generator', file: 'powerbi/tool-86-uc-metric-view-generator' },
      { label: 'Tool 87 — Measure Allocator', file: 'powerbi/tool-87-measure-allocator' },
      { label: 'Tool 88 — Metric View Deployer', file: 'powerbi/tool-88-metric-view-deployer' },
      { label: 'Tool 89 — Config Generator', file: 'powerbi/tool-89-config-generator' },
      { label: 'Tool 90 — Pipeline Config Generator', file: 'powerbi/tool-90-pipeline-config-generator' },
      { label: 'Pipeline Config Reference', file: 'UCMV_PIPELINE_CONFIG_GUIDE' },
    ],
  },
];

const Documentation: React.FC = () => {
  const [currentDoc, setCurrentDoc] = useState<string>('README');
  const [docContent, setDocContent] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(true);
  const [openSections, setOpenSections] = useState<{ [key: string]: boolean }>({
    'Power BI - Overview': true,
    'Power BI - UC Metric Views': true,
  });
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
        // A dev/SPA server answers an unknown /docs/*.md path with the app's
        // index.html shell (HTTP 200), not a 404 — so a missing doc would
        // otherwise render that raw HTML as if it were the document. Treat an
        // HTML shell as "not found" instead.
        const isHtmlShell =
          /^\s*<!doctype html>/i.test(content) || /<div id="root">/i.test(content);
        if (isHtmlShell) {
          setDocContent(`# Document Not Found\n\nThe document "${filename}.md" could not be loaded.`);
        } else {
          setDocContent(content);
        }
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
                img: (props: React.ImgHTMLAttributes<HTMLImageElement>) => (
                  <img
                    {...props}
                    style={{
                      maxWidth: '100%',
                      height: 'auto',
                      display: 'block',
                      marginTop: 16,
                      marginBottom: 16,
                    }}
                  />
                ),
                a: ({ href, children, ...props }: React.AnchorHTMLAttributes<HTMLAnchorElement>) => {
                  if (!href) return <a {...props}>{children}</a>;

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
                        {...props}
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
                    // Resolve relative links (./something.md) against current doc's directory
                    let base = normalized.replace(/\.md$/i, '');
                    if (base.startsWith('./') || (!base.startsWith('/') && !base.includes(':'))) {
                      const currentDir = currentDoc.includes('/') ? currentDoc.substring(0, currentDoc.lastIndexOf('/')) : '';
                      const relativePart = base.replace(/^\.\//, '');
                      base = currentDir ? `${currentDir}/${relativePart}` : relativePart;
                    }
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
                      {...props}
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