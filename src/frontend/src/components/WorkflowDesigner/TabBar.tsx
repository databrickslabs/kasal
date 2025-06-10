import React, { useState } from 'react';
import {
  Box,
  Tab,
  Tabs,
  IconButton,
  Tooltip,
  Menu,
  MenuItem,
  TextField,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Chip,
  ListItemIcon,
  ListItemText
} from '@mui/material';
import {
  Add as AddIcon,
  Close as CloseIcon,
  ContentCopy as DuplicateIcon,
  Edit as EditIcon,
  PlayArrow as RunIcon,
  Save as SaveIcon,
  Warning as WarningIcon,
  NoteAdd as NewCanvasIcon,
  FolderOpen as LoadCrewIcon,
  ArrowDropDown as ArrowDropDownIcon
} from '@mui/icons-material';
import { useTabManagerStore } from '../../store/tabManager';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';

interface TabBarProps {
  onRunTab?: (tabId: string) => void;
  isRunning?: boolean;
  runningTabId?: string | null;
  onLoadCrew?: () => void;
}

const TabBar: React.FC<TabBarProps> = ({ 
  onRunTab, 
  isRunning = false, 
  runningTabId = null,
  onLoadCrew
}) => {
  const { isDarkMode } = useThemeManager();
  const {
    tabs,
    activeTabId,
    createTab,
    closeTab,
    setActiveTab,
    updateTabName,
    duplicateTab
  } = useTabManagerStore();

  const [contextMenu, setContextMenu] = useState<{
    mouseX: number;
    mouseY: number;
    tabId: string;
  } | null>(null);

  const [newTabMenu, setNewTabMenu] = useState<{
    anchorEl: HTMLElement | null;
  }>({
    anchorEl: null
  });
  
  const [renameDialog, setRenameDialog] = useState<{
    open: boolean;
    tabId: string;
    currentName: string;
  }>({
    open: false,
    tabId: '',
    currentName: ''
  });

  const [closeConfirmDialog, setCloseConfirmDialog] = useState<{
    open: boolean;
    tabId: string;
    tabName: string;
  }>({
    open: false,
    tabId: '',
    tabName: ''
  });

  const handleTabChange = (_event: React.SyntheticEvent, newValue: string) => {
    setActiveTab(newValue);
  };

  const handleNewTabMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setNewTabMenu({ anchorEl: event.currentTarget });
  };

  const handleNewTabMenuClose = () => {
    setNewTabMenu({ anchorEl: null });
  };

  const handleNewEmptyCanvas = () => {
    createTab();
    handleNewTabMenuClose();
  };

  const handleLoadExistingCrew = () => {
    if (onLoadCrew) {
      onLoadCrew();
    }
    handleNewTabMenuClose();
  };

  const handleCloseTab = (tabId: string, event: React.MouseEvent) => {
    event.stopPropagation();
    
    const tab = tabs.find(t => t.id === tabId);
    if (tab && tab.isDirty) {
      // Show confirmation dialog for unsaved changes
      setCloseConfirmDialog({
        open: true,
        tabId,
        tabName: tab.name
      });
    } else {
      // Close directly if no unsaved changes
      closeTab(tabId);
    }
  };

  const handleConfirmClose = (action: 'save' | 'discard' | 'cancel') => {
    console.log('TabBar: handleConfirmClose called with action:', action, 'tabId:', closeConfirmDialog.tabId);
    
    if (action === 'save') {
      const tab = tabs.find(t => t.id === closeConfirmDialog.tabId);
      console.log('TabBar: Found tab for close:', tab ? { id: tab.id, name: tab.name, savedCrewId: tab.savedCrewId, isDirty: tab.isDirty } : 'NOT FOUND');
      
      if (tab && tab.savedCrewId) {
        // Check if this is a legacy tab with 'loaded' placeholder
        if (tab.savedCrewId === 'loaded') {
          console.log('TabBar: Found legacy tab with "loaded" savedCrewId, attempting to find actual crew ID');
          
          // Look for agent nodes with agentId
          const agentNode = tab.nodes.find(node => node.type === 'agentNode' && node.data?.agentId);
          if (agentNode?.data?.agentId) {
            console.log('TabBar: Found agent ID in tab:', agentNode.data.agentId);
          }
          
          // Look for task nodes with taskId  
          const taskNode = tab.nodes.find(node => node.type === 'taskNode' && node.data?.taskId);
          if (taskNode?.data?.taskId) {
            console.log('TabBar: Found task ID in tab:', taskNode.data.taskId);
          }
          
          // If we have a tab name that matches a crew name, we can try to update by name
          if (tab.name && (agentNode || taskNode)) {
            console.log('TabBar: Attempting to update crew by name for legacy tab:', tab.name);
            // Trigger update with special flag indicating this is a legacy tab update
            const event = new CustomEvent('updateExistingCrewByName', {
              detail: { tabId: closeConfirmDialog.tabId, crewName: tab.name }
            });
            window.dispatchEvent(event);
            
            // Listen for update completion
            const handleUpdateComplete = () => {
              console.log('TabBar: Legacy update complete, closing tab:', closeConfirmDialog.tabId);
              closeTab(closeConfirmDialog.tabId);
              window.removeEventListener('updateCrewComplete', handleUpdateComplete);
            };
            window.addEventListener('updateCrewComplete', handleUpdateComplete);
          } else {
            // Fallback to save dialog if we can't determine the crew
            console.log('TabBar: Cannot determine crew ID, falling back to save dialog');
            const event = new CustomEvent('openSaveCrewDialog');
            window.dispatchEvent(event);
            
            // Listen for save completion
            const handleSaveComplete = () => {
              console.log('TabBar: Save complete, closing tab:', closeConfirmDialog.tabId);
              closeTab(closeConfirmDialog.tabId);
              window.removeEventListener('saveCrewComplete', handleSaveComplete);
            };
            window.addEventListener('saveCrewComplete', handleSaveComplete);
          }
        } else {
          // This is an existing crew with valid ID - trigger update
          console.log('TabBar: Triggering update for existing crew:', tab.savedCrewId);
          const event = new CustomEvent('updateExistingCrew', {
            detail: { tabId: closeConfirmDialog.tabId, crewId: tab.savedCrewId }
          });
          window.dispatchEvent(event);
          
          // Listen for update completion
          const handleUpdateComplete = () => {
            console.log('TabBar: Update complete, closing tab:', closeConfirmDialog.tabId);
            closeTab(closeConfirmDialog.tabId);
            window.removeEventListener('updateCrewComplete', handleUpdateComplete);
          };
          window.addEventListener('updateCrewComplete', handleUpdateComplete);
        }
      } else {
        // This is a new crew - trigger save crew dialog
        console.log('TabBar: Triggering save dialog for new crew');
        const event = new CustomEvent('openSaveCrewDialog');
        window.dispatchEvent(event);
        
        // Listen for save completion
        const handleSaveComplete = () => {
          console.log('TabBar: Save complete, closing tab:', closeConfirmDialog.tabId);
          closeTab(closeConfirmDialog.tabId);
          window.removeEventListener('saveCrewComplete', handleSaveComplete);
        };
        window.addEventListener('saveCrewComplete', handleSaveComplete);
      }
    } else if (action === 'discard') {
      closeTab(closeConfirmDialog.tabId);
    }
    
    setCloseConfirmDialog({ open: false, tabId: '', tabName: '' });
  };

  const handleContextMenu = (event: React.MouseEvent, tabId: string) => {
    event.preventDefault();
    setContextMenu({
      mouseX: event.clientX - 2,
      mouseY: event.clientY - 4,
      tabId
    });
  };

  const handleContextMenuClose = () => {
    setContextMenu(null);
  };

  const handleRename = (tabId: string) => {
    const tab = tabs.find(t => t.id === tabId);
    if (tab) {
      setRenameDialog({
        open: true,
        tabId,
        currentName: tab.name
      });
    }
    handleContextMenuClose();
  };

  const handleRenameSubmit = () => {
    if (renameDialog.currentName.trim()) {
      updateTabName(renameDialog.tabId, renameDialog.currentName.trim());
    }
    setRenameDialog({ open: false, tabId: '', currentName: '' });
  };

  const handleDuplicate = (tabId: string) => {
    duplicateTab(tabId);
    handleContextMenuClose();
  };

  const handleRunTab = (tabId: string) => {
    if (onRunTab) {
      onRunTab(tabId);
    }
    handleContextMenuClose();
  };

  const handleSaveTab = (tabId: string) => {
    // Set the active tab before saving
    setActiveTab(tabId);
    
    // Trigger save crew dialog
    setTimeout(() => {
      const event = new CustomEvent('openSaveCrewDialog');
      window.dispatchEvent(event);
    }, 100);
    
    handleContextMenuClose();
  };

  // Create a default tab if none exist
  React.useEffect(() => {
    if (tabs.length === 0) {
      createTab('Main Workflow');
    }
  }, [tabs.length, createTab]);

  return (
    <>
      <Box
        sx={{
          borderBottom: 1,
          borderColor: 'divider',
          backgroundColor: isDarkMode ? '#1a1a1a' : '#ffffff',
          display: 'flex',
          alignItems: 'center',
          minHeight: '48px',
          paddingLeft: 1,
          paddingRight: 1,
          position: 'relative',
          zIndex: 1001 // Above the toolbar
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', flex: 1 }}>
          <Tabs
            value={activeTabId || false}
            onChange={handleTabChange}
            variant="scrollable"
            scrollButtons="auto"
            sx={{
              minWidth: 0, // Allow tabs to shrink
              '& .MuiTab-root': {
                minHeight: '40px',
                textTransform: 'none',
                fontSize: '0.875rem',
                fontWeight: 500,
                padding: '8px 12px',
                minWidth: 'auto',
                maxWidth: '200px',
                '&.Mui-selected': {
                  color: isDarkMode ? '#90caf9' : '#1976d2',
                }
              },
              '& .MuiTabs-indicator': {
                backgroundColor: isDarkMode ? '#90caf9' : '#1976d2',
              }
            }}
          >
            {tabs.map((tab) => (
              <Tab
                key={tab.id}
                value={tab.id}
                onContextMenu={(e) => handleContextMenu(e, tab.id)}
                label={
                  <Box sx={{ 
                    display: 'flex', 
                    alignItems: 'center', 
                    gap: 1,
                    maxWidth: '180px'
                  }}>
                    <Typography
                      variant="body2"
                      sx={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        flex: 1
                      }}
                    >
                      {tab.name}
                    </Typography>
                    
                    {/* Show indicators */}
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      {tab.isDirty && (
                        <Tooltip title="Unsaved changes">
                          <Box
                            sx={{
                              width: 6,
                              height: 6,
                              borderRadius: '50%',
                              backgroundColor: isDarkMode ? '#ff9800' : '#f57c00'
                            }}
                          />
                        </Tooltip>
                      )}
                      
                      {tab.savedCrewId && !tab.isDirty && (
                        <Tooltip title={`Saved as: ${tab.savedCrewName}`}>
                          <SaveIcon sx={{ fontSize: 12, color: 'success.main' }} />
                        </Tooltip>
                      )}
                      
                      {runningTabId === tab.id && isRunning && (
                        <Chip
                          size="small"
                          label="Running"
                          color="success"
                          sx={{ 
                            height: 16, 
                            fontSize: '0.6rem',
                            '& .MuiChip-label': { px: 0.5 }
                          }}
                        />
                      )}
                      
                      {tabs.length > 1 && (
                        <IconButton
                          size="small"
                          onClick={(e) => handleCloseTab(tab.id, e)}
                          sx={{
                            width: 16,
                            height: 16,
                            padding: 0,
                            '&:hover': {
                              backgroundColor: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)'
                            }
                          }}
                        >
                          <CloseIcon sx={{ fontSize: 12 }} />
                        </IconButton>
                      )}
                    </Box>
                  </Box>
                }
              />
            ))}
          </Tabs>

          <Tooltip title="New Tab Options">
            <IconButton
              onClick={handleNewTabMenuOpen}
              size="small"
              sx={{
                marginLeft: 1,
                padding: '6px',
                color: isDarkMode ? 'rgba(255,255,255,0.7)' : 'rgba(0,0,0,0.6)',
                '&:hover': {
                  backgroundColor: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)',
                  color: isDarkMode ? 'rgba(255,255,255,0.9)' : 'rgba(0,0,0,0.8)'
                }
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                <AddIcon sx={{ fontSize: 18 }} />
                <ArrowDropDownIcon sx={{ fontSize: 14 }} />
              </Box>
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {/* New Tab Menu */}
      <Menu
        open={Boolean(newTabMenu.anchorEl)}
        onClose={handleNewTabMenuClose}
        anchorEl={newTabMenu.anchorEl}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
      >
        <MenuItem onClick={handleNewEmptyCanvas}>
          <ListItemIcon>
            <NewCanvasIcon sx={{ fontSize: 18 }} />
          </ListItemIcon>
          <ListItemText 
            primary="New Empty Canvas"
            secondary="Start with a blank workflow"
          />
        </MenuItem>
        <MenuItem onClick={handleLoadExistingCrew}>
          <ListItemIcon>
            <LoadCrewIcon sx={{ fontSize: 18 }} />
          </ListItemIcon>
          <ListItemText 
            primary="Load Existing Crew"
            secondary="Open a saved workflow"
          />
        </MenuItem>
      </Menu>

      {/* Context Menu */}
      <Menu
        open={contextMenu !== null}
        onClose={handleContextMenuClose}
        anchorReference="anchorPosition"
        anchorPosition={
          contextMenu !== null
            ? { top: contextMenu.mouseY, left: contextMenu.mouseX }
            : undefined
        }
      >
        <MenuItem onClick={() => handleRename(contextMenu?.tabId || '')}>
          <EditIcon sx={{ mr: 1, fontSize: 18 }} />
          Rename
        </MenuItem>
        <MenuItem onClick={() => handleDuplicate(contextMenu?.tabId || '')}>
          <DuplicateIcon sx={{ mr: 1, fontSize: 18 }} />
          Duplicate
        </MenuItem>
        <MenuItem onClick={() => handleSaveTab(contextMenu?.tabId || '')}>
          <SaveIcon sx={{ mr: 1, fontSize: 18 }} />
          Save Crew
        </MenuItem>
        <MenuItem 
          onClick={() => handleRunTab(contextMenu?.tabId || '')}
          disabled={isRunning}
        >
          <RunIcon sx={{ mr: 1, fontSize: 18 }} />
          Run This Tab
        </MenuItem>
        {tabs.length > 1 && (
          <MenuItem onClick={() => {
            const tab = tabs.find(t => t.id === contextMenu?.tabId);
            if (tab && tab.isDirty) {
              setCloseConfirmDialog({
                open: true,
                tabId: contextMenu?.tabId || '',
                tabName: tab.name
              });
            } else {
              closeTab(contextMenu?.tabId || '');
            }
            handleContextMenuClose();
          }}>
            <CloseIcon sx={{ mr: 1, fontSize: 18 }} />
            Close Tab
          </MenuItem>
        )}
      </Menu>

      {/* Rename Dialog */}
      <Dialog
        open={renameDialog.open}
        onClose={() => setRenameDialog({ open: false, tabId: '', currentName: '' })}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Rename Tab</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            margin="dense"
            label="Tab Name"
            fullWidth
            variant="outlined"
            value={renameDialog.currentName}
            onChange={(e) => setRenameDialog(prev => ({ ...prev, currentName: e.target.value }))}
            onKeyPress={(e) => {
              if (e.key === 'Enter') {
                handleRenameSubmit();
              }
            }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setRenameDialog({ open: false, tabId: '', currentName: '' })}>
            Cancel
          </Button>
          <Button onClick={handleRenameSubmit} variant="contained">
            Rename
          </Button>
        </DialogActions>
      </Dialog>

      {/* Close Confirmation Dialog */}
      <Dialog
        open={closeConfirmDialog.open}
        onClose={() => setCloseConfirmDialog({ open: false, tabId: '', tabName: '' })}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <WarningIcon color="warning" />
          Unsaved Changes
        </DialogTitle>
        <DialogContent>
          <Typography>
            The tab &quot;{closeConfirmDialog.tabName}&quot; has unsaved changes. 
            Do you want to save your changes before closing?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => handleConfirmClose('cancel')} color="inherit">
            Cancel
          </Button>
          <Button onClick={() => handleConfirmClose('discard')} color="error">
            Discard Changes
          </Button>
          <Button onClick={() => handleConfirmClose('save')} variant="contained" color="primary">
            Save & Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default TabBar; 