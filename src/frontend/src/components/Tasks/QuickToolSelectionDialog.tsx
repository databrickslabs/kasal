import React, { useState, useCallback, useEffect, useRef } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  IconButton,
  Typography,
  Box,
  CircularProgress,
  FormControl,
  TextField,
  InputAdornment,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Checkbox,
  Tabs,
  Tab,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import SearchIcon from '@mui/icons-material/Search';
import StorageIcon from '@mui/icons-material/Storage';
import { ToolService, Tool } from '../../api/ToolService';
import { MCPService } from '../../api/MCPService';
import { MCPServerConfig } from '../Configuration/MCP/MCPConfiguration';

export interface QuickToolSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onApply: (tools: string[], mcpServers: string[]) => void;
  currentTools?: string[];
  currentMcpServers?: string[];
  isUpdating?: boolean;
  initialTab?: number;
}

const QuickToolSelectionDialog: React.FC<QuickToolSelectionDialogProps> = ({
  open,
  onClose,
  onApply,
  currentTools = [],
  currentMcpServers = [],
  isUpdating = false,
  initialTab = 0
}) => {
  // Tab state
  const [activeTab, setActiveTab] = useState(initialTab);

  // Tools state
  const [tools, setTools] = useState<Tool[]>([]);
  const [selectedTools, setSelectedTools] = useState<string[]>([]);
  const [isLoadingTools, setIsLoadingTools] = useState<boolean>(false);
  const [toolSearchQuery, setToolSearchQuery] = useState<string>('');
  const [toolFocusedIndex, setToolFocusedIndex] = useState<number>(-1);

  // MCP state
  const [mcpServers, setMcpServers] = useState<MCPServerConfig[]>([]);
  const [selectedMcpServers, setSelectedMcpServers] = useState<string[]>([]);
  const [isLoadingMcp, setIsLoadingMcp] = useState<boolean>(false);
  const [mcpSearchQuery, setMcpSearchQuery] = useState<string>('');
  const [mcpFocusedIndex, setMcpFocusedIndex] = useState<number>(-1);

  const searchInputRef = useRef<HTMLInputElement>(null);
  const mcpSearchInputRef = useRef<HTMLInputElement>(null);

  // Filter tools based on search query
  const filteredTools = tools.filter(tool =>
    tool.title.toLowerCase().includes(toolSearchQuery.toLowerCase()) ||
    tool.description.toLowerCase().includes(toolSearchQuery.toLowerCase())
  );

  // Filter MCP servers based on search query
  const filteredMcpServers = mcpServers.filter(server =>
    server.name.toLowerCase().includes(mcpSearchQuery.toLowerCase()) ||
    server.server_type.toLowerCase().includes(mcpSearchQuery.toLowerCase())
  );

  // Sync initialTab when dialog opens
  useEffect(() => {
    if (open) {
      setActiveTab(initialTab);
    }
  }, [open, initialTab]);

  // Fetch tools and set current selection when dialog opens
  useEffect(() => {
    if (open) {
      fetchTools();
      setSelectedTools(currentTools.map(t => String(t)));
      setToolSearchQuery('');
      setToolFocusedIndex(-1);
    }
  }, [open, currentTools]);

  // Fetch MCP servers when dialog opens
  useEffect(() => {
    if (open) {
      fetchMcpServers();
      setSelectedMcpServers([...currentMcpServers]);
      setMcpSearchQuery('');
      setMcpFocusedIndex(-1);
    }
  }, [open, currentMcpServers]);

  // Focus search input when tab changes
  useEffect(() => {
    if (open) {
      setTimeout(() => {
        if (activeTab === 0) {
          searchInputRef.current?.focus();
        } else {
          mcpSearchInputRef.current?.focus();
        }
      }, 100);
    }
  }, [open, activeTab]);

  const fetchTools = async () => {
    setIsLoadingTools(true);
    try {
      const toolsList = await ToolService.listEnabledTools();
      setTools(toolsList);
    } catch (error) {
      console.error('Error fetching tools:', error);
    } finally {
      setIsLoadingTools(false);
    }
  };

  const fetchMcpServers = async () => {
    setIsLoadingMcp(true);
    try {
      const mcpService = MCPService.getInstance();
      const response = await mcpService.getMcpServers();
      const enabledServers = response.servers.filter(server => server.enabled);
      setMcpServers(enabledServers);
    } catch (error) {
      console.error('Error fetching MCP servers:', error);
    } finally {
      setIsLoadingMcp(false);
    }
  };

  const handleToolToggle = (tool: Tool) => {
    const toolId = String(tool.id);
    setSelectedTools(prev =>
      prev.includes(toolId)
        ? prev.filter(id => id !== toolId)
        : [...prev, toolId]
    );
  };

  const handleMcpToggle = (server: MCPServerConfig) => {
    setSelectedMcpServers(prev =>
      prev.includes(server.name)
        ? prev.filter(name => name !== server.name)
        : [...prev, server.name]
    );
  };

  const handleClose = useCallback(() => {
    setSelectedTools([]);
    setSelectedMcpServers([]);
    onClose();
  }, [onClose]);

  const handleApply = useCallback(() => {
    onApply(selectedTools, selectedMcpServers);
    onClose();
  }, [selectedTools, selectedMcpServers, onApply, onClose]);

  const handleToolSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setToolSearchQuery(event.target.value);
    setToolFocusedIndex(0);
  };

  const handleMcpSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setMcpSearchQuery(event.target.value);
    setMcpFocusedIndex(0);
  };

  const handleToolKeyDown = (event: React.KeyboardEvent) => {
    const toolCount = filteredTools.length;

    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        setToolFocusedIndex(prev => (prev + 1) % toolCount);
        break;
      case 'ArrowUp':
        event.preventDefault();
        setToolFocusedIndex(prev => (prev - 1 + toolCount) % toolCount);
        break;
      case ' ':
        event.preventDefault();
        if (toolFocusedIndex >= 0 && toolFocusedIndex < toolCount) {
          handleToolToggle(filteredTools[toolFocusedIndex]);
        }
        break;
      case 'Enter':
        event.preventDefault();
        handleApply();
        break;
      default:
        break;
    }
  };

  const handleMcpKeyDown = (event: React.KeyboardEvent) => {
    const serverCount = filteredMcpServers.length;

    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        setMcpFocusedIndex(prev => (prev + 1) % serverCount);
        break;
      case 'ArrowUp':
        event.preventDefault();
        setMcpFocusedIndex(prev => (prev - 1 + serverCount) % serverCount);
        break;
      case ' ':
        event.preventDefault();
        if (mcpFocusedIndex >= 0 && mcpFocusedIndex < serverCount) {
          handleMcpToggle(filteredMcpServers[mcpFocusedIndex]);
        }
        break;
      case 'Enter':
        event.preventDefault();
        handleApply();
        break;
      default:
        break;
    }
  };

  // Helper to check if a tool is selected (handles both string and number IDs)
  const isToolSelected = (tool: Tool) => {
    const toolIdStr = String(tool.id);
    return selectedTools.includes(toolIdStr) || selectedTools.includes(tool.title);
  };

  const isMcpSelected = (server: MCPServerConfig) => {
    return selectedMcpServers.includes(server.name);
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle sx={{ pb: 1 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Configure Capabilities</Typography>
          <IconButton onClick={handleClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
        <Typography variant="caption" color="text.secondary">
          {selectedTools.length} tool{selectedTools.length !== 1 ? 's' : ''}, {selectedMcpServers.length} MCP server{selectedMcpServers.length !== 1 ? 's' : ''}
        </Typography>
      </DialogTitle>

      <Tabs
        value={activeTab}
        onChange={(_, newValue) => setActiveTab(newValue)}
        sx={{ px: 3, borderBottom: 1, borderColor: 'divider' }}
      >
        <Tab
          label={`Tools (${selectedTools.length})`}
          sx={{ textTransform: 'none', fontWeight: 500 }}
        />
        <Tab
          label={`MCP Servers (${selectedMcpServers.length})`}
          icon={<StorageIcon sx={{ fontSize: 16 }} />}
          iconPosition="start"
          sx={{ textTransform: 'none', fontWeight: 500 }}
        />
      </Tabs>

      <DialogContent sx={{ pb: 1, minHeight: 400 }}>
        {/* Tools Tab */}
        {activeTab === 0 && (
          <>
            <TextField
              inputRef={searchInputRef}
              fullWidth
              placeholder="Search tools... (↑↓ navigate, Space toggle, Enter apply)"
              value={toolSearchQuery}
              onChange={handleToolSearchChange}
              onKeyDown={handleToolKeyDown}
              variant="outlined"
              size="small"
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon />
                  </InputAdornment>
                ),
              }}
              sx={{ mb: 2, mt: 1 }}
            />

            {isLoadingTools ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
                <CircularProgress size={24} />
              </Box>
            ) : (
              <FormControl component="fieldset" fullWidth>
                <List
                  dense
                  sx={{
                    maxHeight: '350px',
                    overflow: 'auto',
                  }}
                >
                  {filteredTools.map((tool, index) => (
                    <ListItem key={tool.id} disablePadding dense>
                      <ListItemButton
                        selected={index === toolFocusedIndex}
                        onClick={() => handleToolToggle(tool)}
                        sx={{
                          borderRadius: 1,
                          mb: 0.5,
                        }}
                      >
                        <ListItemIcon>
                          <Checkbox
                            edge="start"
                            checked={isToolSelected(tool)}
                            tabIndex={-1}
                            disableRipple
                          />
                        </ListItemIcon>
                        <ListItemText
                          primary={tool.title}
                          secondary={tool.description}
                          primaryTypographyProps={{
                            variant: 'body2',
                            fontWeight: isToolSelected(tool) ? 'bold' : 'normal',
                          }}
                          secondaryTypographyProps={{
                            variant: 'caption',
                            color: 'textSecondary',
                            sx: {
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              display: '-webkit-box',
                              WebkitLineClamp: 2,
                              WebkitBoxOrient: 'vertical',
                            }
                          }}
                        />
                      </ListItemButton>
                    </ListItem>
                  ))}
                  {filteredTools.length === 0 && !isLoadingTools && (
                    <ListItem>
                      <ListItemText
                        primary="No tools found"
                        secondary="Try a different search term"
                      />
                    </ListItem>
                  )}
                </List>
              </FormControl>
            )}
          </>
        )}

        {/* MCP Servers Tab */}
        {activeTab === 1 && (
          <>
            <TextField
              inputRef={mcpSearchInputRef}
              fullWidth
              placeholder="Search MCP servers... (↑↓ navigate, Space toggle, Enter apply)"
              value={mcpSearchQuery}
              onChange={handleMcpSearchChange}
              onKeyDown={handleMcpKeyDown}
              variant="outlined"
              size="small"
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon />
                  </InputAdornment>
                ),
              }}
              sx={{ mb: 2, mt: 1 }}
            />

            {isLoadingMcp ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
                <CircularProgress size={24} />
              </Box>
            ) : (
              <FormControl component="fieldset" fullWidth>
                <List
                  dense
                  sx={{
                    maxHeight: '350px',
                    overflow: 'auto',
                  }}
                >
                  {filteredMcpServers.map((server, index) => (
                    <ListItem key={server.id} disablePadding dense>
                      <ListItemButton
                        selected={index === mcpFocusedIndex}
                        onClick={() => handleMcpToggle(server)}
                        sx={{
                          borderRadius: 1,
                          mb: 0.5,
                        }}
                      >
                        <ListItemIcon>
                          <Checkbox
                            edge="start"
                            checked={isMcpSelected(server)}
                            tabIndex={-1}
                            disableRipple
                          />
                        </ListItemIcon>
                        <ListItemText
                          primary={server.name}
                          secondary={`${server.server_type}${server.server_url ? ` · ${server.server_url}` : ''}`}
                          primaryTypographyProps={{
                            variant: 'body2',
                            fontWeight: isMcpSelected(server) ? 'bold' : 'normal',
                          }}
                          secondaryTypographyProps={{
                            variant: 'caption',
                            color: 'textSecondary',
                          }}
                        />
                      </ListItemButton>
                    </ListItem>
                  ))}
                  {filteredMcpServers.length === 0 && !isLoadingMcp && (
                    <ListItem>
                      <ListItemText
                        primary="No MCP servers found"
                        secondary="Configure MCP servers in Settings → Configuration → MCP"
                      />
                    </ListItem>
                  )}
                </List>
              </FormControl>
            )}
          </>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={handleClose}>Cancel</Button>
        <Button
          variant="contained"
          onClick={handleApply}
          disabled={isUpdating}
        >
          {isUpdating ? <CircularProgress size={20} /> : 'Apply'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default QuickToolSelectionDialog;
