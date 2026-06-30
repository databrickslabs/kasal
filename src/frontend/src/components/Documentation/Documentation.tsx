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
  Toolbar,
  IconButton,
  Tooltip,
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
import { useNavigate, useLocation } from 'react-router-dom';
import mermaid from 'mermaid';
import { CodeBlock } from '../Chat/components/CodeBlock';
import './Documentation.css';

interface DocSection {
  label: string;
  items: { label: string; file: string }[];
}

// Primary navigation mirrors the canonical doc set published on kasal.io so the
// in-app docs and the public site stay aligned. The "Additional guides" group
// keeps repo-specific deep dives (observability, Lakebase, Power BI) that the
// public site folds into other pages, so no content is lost.
const docSections: DocSection[] = [
  {
    label: 'Get started',
    items: [
      { label: 'Quick Start', file: 'QUICK_START' },
      { label: 'Why Kasal', file: 'WHY_KASAL' },
    ],
  },
  {
    label: 'Platform',
    items: [
      { label: 'Architecture Guide', file: 'ARCHITECTURE_GUIDE' },
      { label: 'Code Structure', file: 'CODE_STRUCTURE_GUIDE' },
      { label: 'Developer Guide', file: 'DEVELOPER_GUIDE' },
    ],
  },
  {
    label: 'Reference',
    items: [
      { label: 'API Reference', file: 'api_endpoints' },
      { label: 'Tools', file: 'TOOLS' },
      { label: 'MCP Servers', file: 'MCP' },
      { label: 'Models', file: 'MODELS' },
      { label: 'Memory', file: 'MEMORY' },
      { label: 'Security', file: 'SECURITY' },
    ],
  },
  {
    label: 'Tutorials',
    items: [
      { label: 'End-User Tutorials', file: 'END_USER_TUTORIAL_CATALOG' },
    ],
  },
  {
    label: 'Additional guides',
    items: [
      { label: 'Flow Routing and Output Schemas', file: 'flow-routing' },
      { label: 'MLflow Tracing: Setup and Requirements', file: 'mlflow-tracing-setup' },
      { label: 'Lakebase Setup (Persistence)', file: 'lakebase-deployment' },
      { label: 'Security: Compliance', file: 'README_SECURITY_COMPLIANCE' },
      { label: 'Security: Test Guide', file: 'README_SECURITY_GUARDRAILS_TESTGUIDE' },
      { label: 'Security: Supply Chain', file: 'README_SECURITY_SUPPLY_CHAIN' },
    ],
  },
  {
    label: 'Power BI migration',
    items: [
      { label: 'Overview and Navigation', file: 'powerbi/README' },
      { label: 'Authentication and SP Setup', file: 'powerbi/01-authentication-setup' },
      { label: 'Simple Migration Story', file: 'powerbi/02-simple-migration-story' },
      { label: 'Analytics Q&A Case Study', file: 'powerbi/powerbi-analytics-qa-case-study' },
      { label: 'End-to-End Migration Guide', file: 'powerbi/ucmv-migration-guide' },
      { label: 'Pipeline Config Reference', file: 'UCMV_PIPELINE_CONFIG_GUIDE' },
      { label: 'Tool 72: Comprehensive Analysis', file: 'powerbi/tool-72-comprehensive-analysis' },
      { label: 'Tool 73: Measure Conversion', file: 'powerbi/tool-73-measure-conversion' },
      { label: 'Tool 74: M-Query Conversion', file: 'powerbi/tool-74-mquery-conversion' },
      { label: 'Tool 75: Relationships', file: 'powerbi/tool-75-relationships' },
      { label: 'Tool 76: Hierarchies (Fabric)', file: 'powerbi/tool-76-hierarchies' },
      { label: 'Tool 77: Field Parameters (Fabric)', file: 'powerbi/tool-77-field-parameters' },
      { label: 'Tool 78: Report References (Fabric)', file: 'powerbi/tool-78-report-references' },
      { label: 'Tool 79: Semantic Model Fetcher', file: 'powerbi/tool-79-semantic-model-fetcher' },
      { label: 'Tool 80: DAX Generator', file: 'powerbi/tool-80-dax-generator' },
      { label: 'Tool 81: Metadata Reducer', file: 'powerbi/tool-81-metadata-reducer' },
      { label: 'Tool 82: DAX Executor', file: 'powerbi/tool-82-dax-executor' },
      { label: 'Tool 85: DAX to SQL Translator', file: 'powerbi/tool-85-dax-to-sql-translator' },
      { label: 'Tool 86: UC Metric View Generator', file: 'powerbi/tool-86-uc-metric-view-generator' },
      { label: 'Tool 87: Measure Allocator', file: 'powerbi/tool-87-measure-allocator' },
      { label: 'Tool 88: Metric View Deployer', file: 'powerbi/tool-88-metric-view-deployer' },
      { label: 'Tool 89: Config Generator', file: 'powerbi/tool-89-config-generator' },
      { label: 'Tool 90: Pipeline Config Generator', file: 'powerbi/tool-90-pipeline-config-generator' },
    ],
  },
];

const Documentation: React.FC = () => {
  const location = useLocation();
  // Allow deep-linking to a specific doc via /docs/<file> (e.g. /docs/flow-routing).
  const docFromPath = location.pathname.replace(/^\/docs\/?/, '');
  const [currentDoc, setCurrentDoc] = useState<string>(docFromPath || 'README');
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

  // Deep-link support: when the URL path changes (e.g. opened from a "Learn more"
  // link), select that doc. Sidebar clicks don't change the path, so they're safe.
  useEffect(() => {
    const d = location.pathname.replace(/^\/docs\/?/, '');
    if (d && d !== currentDoc) {
      setCurrentDoc(d);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.pathname]);

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
        // index.html shell (HTTP 200), not a 404, so a missing doc would
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
      <Toolbar sx={{ justifyContent: 'space-between' }}>
        <Typography variant="h6" noWrap component="div">
          Kasal Docs
        </Typography>
        <Tooltip title="Back to app">
          <IconButton size="small" edge="end" onClick={() => navigate('/workflow')}>
            <HomeIcon />
          </IconButton>
        </Tooltip>
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
      {/* Mobile-only menu toggle (no full-width app bar). */}
      <IconButton
        aria-label="open drawer"
        onClick={handleDrawerToggle}
        sx={{
          display: { xs: 'flex', md: 'none' },
          position: 'fixed',
          top: 8,
          left: 8,
          zIndex: (t) => t.zIndex.drawer + 1,
          bgcolor: 'background.paper',
          boxShadow: 1,
          '&:hover': { bgcolor: 'background.paper' },
        }}
      >
        <MenuIcon />
      </IconButton>

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
          overflow: 'auto',
        }}
      >
        {loading ? (
          <Box display="flex" justifyContent="center" alignItems="center" height="50vh">
            <CircularProgress />
            <Typography sx={{ ml: 2 }}>Loading Documentation...</Typography>
          </Box>
        ) : (
          <Box
            className="markdown-body"
            data-theme={theme.palette.mode === 'dark' ? 'dark' : 'light'}
            sx={{ maxWidth: '860px', mx: 'auto' }}
          >
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                // github-markdown-css styles every standard element; we only
                // override the ones that carry app-specific behavior.
                img: ({ src, ...props }: React.ImgHTMLAttributes<HTMLImageElement>) => {
                  // Markdown uses paths relative to the doc (e.g. images/x.png).
                  // The browser would otherwise resolve them against the current
                  // page URL (or the SPA serves index.html for them), so rewrite
                  // relative srcs to an absolute /docs/<dir>/... path that maps to
                  // the served public/docs (and frontend_static/docs) tree.
                  let resolved = src || '';
                  const isExternal = /^(https?:)?\/\//i.test(resolved) || resolved.startsWith('data:');
                  if (resolved && !isExternal && !resolved.startsWith('/')) {
                    const relativePart = resolved.replace(/^\.\//, '');
                    const currentDir = currentDoc.includes('/')
                      ? currentDoc.substring(0, currentDoc.lastIndexOf('/'))
                      : '';
                    resolved = currentDir
                      ? `/docs/${currentDir}/${relativePart}`
                      : `/docs/${relativePart}`;
                  }
                  return <img src={resolved} {...props} />;
                },
                a: ({ href, children, ...props }: React.AnchorHTMLAttributes<HTMLAnchorElement>) => {
                  if (!href) return <a {...props}>{children}</a>;

                  // Intra-page anchors (e.g., #section): leave to the browser.
                  if (href.startsWith('#')) {
                    return <a href={href} {...props}>{children}</a>;
                  }

                  const isExternal = /^https?:\/\//i.test(href);
                  const isMd = href.toLowerCase().endsWith('.md');

                  // Internal doc links: route through our loader (no full reload).
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
                      <a
                        href="#"
                        onClick={(e: React.MouseEvent) => {
                          e.preventDefault();
                          handleDocSelect(base);
                        }}
                      >
                        {children}
                      </a>
                    );
                  }

                  // External links open in new tab
                  return (
                    <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
                      {children}
                    </a>
                  );
                },
                // Fenced code blocks arrive as <pre><code class="language-x">…>.
                // We intercept at the <pre> level so the highlighted component
                // (and Mermaid <div>) is never nested inside a <pre>. Inline
                // code is NOT wrapped in <pre>, so it falls through to
                // github-markdown-css's default styling.
                pre: ({ children }) => {
                  const codeEl = React.Children.toArray(children).find(
                    (c): c is React.ReactElement => React.isValidElement(c)
                  );
                  const codeProps = (codeEl?.props ?? {}) as { className?: string; children?: React.ReactNode };
                  const languageMatch = /language-(\w+)/.exec(codeProps.className || '');
                  const language = languageMatch ? languageMatch[1] : undefined;
                  const codeText = String(codeProps.children ?? '').replace(/\n$/, '');

                  // Render Mermaid diagrams as <div class="mermaid"> blocks
                  if (language === 'mermaid') {
                    return <div className="mermaid">{codeText}</div>;
                  }

                  // Syntax-highlighted block with copy button + language label,
                  // reusing the same component the chat workspace uses.
                  return <CodeBlock language={language || 'text'} code={codeText} />;
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