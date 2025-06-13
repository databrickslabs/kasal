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
  ListItemText,
  Divider
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
  ArrowDropDown as ArrowDropDownIcon,
  Clear as ClearAllIcon
} from '@mui/icons-material';
import { useTabManagerStore } from '../../store/tabManager';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';

interface TabBarProps {
  onRunTab?: (tabId: string) => void;
  isRunning?: boolean;
  runningTabId?: string | null;
  onLoadCrew?: () => void;
  disabled?: boolean;
}

const TabBar: React.FC<TabBarProps> = ({ 
  onRunTab, 
  isRunning = false, 
  runningTabId = null,
  onLoadCrew,
  disabled = false
}) => {
  const { isDarkMode } = useThemeManager();
  const {
    tabs,
    activeTabId,
    createTab,
    closeTab,
    setActiveTab,
    updateTabName,
    duplicateTab,
    clearAllTabs,
    clearTabExecutionStatus
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

  const [closeAllConfirmDialog, setCloseAllConfirmDialog] = useState(false);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: string) => {
    if (disabled) {
      return; // Prevent tab switching when disabled
    }
    setActiveTab(newValue);
  };

  const handleNewTabMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setNewTabMenu({ anchorEl: event.currentTarget });
  };

  const handleNewTabMenuClose = () => {
    setNewTabMenu({ anchorEl: null });
  };

  const handleNewEmptyCanvas = () => {
    if (disabled) {
      return; // Prevent creating new tabs when disabled
    }
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
    if (disabled) {
      return; // Prevent closing tabs when disabled
    }
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
    if (disabled) {
      return; // Prevent context menu when disabled
    }
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

  // Auto-clear execution status after 5 minutes
  React.useEffect(() => {
    const timers: NodeJS.Timeout[] = [];
    
    tabs.forEach(tab => {
      if (tab.executionStatus && tab.executionStatus !== 'running' && tab.lastExecutionTime) {
        const timeSinceExecution = Date.now() - new Date(tab.lastExecutionTime).getTime();
        const timeRemaining = 5 * 60 * 1000 - timeSinceExecution; // 5 minutes
        
        if (timeRemaining > 0) {
          const timer = setTimeout(() => {
            clearTabExecutionStatus(tab.id);
          }, timeRemaining);
          timers.push(timer);
        } else {
          // Clear immediately if more than 5 minutes have passed
          clearTabExecutionStatus(tab.id);
        }
      }
    });
    
    return () => {
      timers.forEach(timer => clearTimeout(timer));
    };
  }, [tabs, clearTabExecutionStatus]);

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
              opacity: disabled ? 0.6 : 1,
              pointerEvents: disabled ? 'none' : 'auto',
              '& .MuiTab-root': {
                minHeight: '40px',
                textTransform: 'none',
                fontSize: '0.875rem',
                fontWeight: 500,
                padding: '8px 12px',
                minWidth: 'auto',
                maxWidth: '200px',
                cursor: disabled ? 'not-allowed' : 'pointer',
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
                      
                      {(runningTabId === tab.id && isRunning) || tab.executionStatus === 'running' ? (
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
                      ) : null}
                      
                      {tab.executionStatus === 'completed' && (
                        <Tooltip title={tab.lastExecutionTime ? `Completed at ${new Date(tab.lastExecutionTime).toLocaleTimeString()}` : 'Completed'}>
                          <Chip
                            size="small"
                            label="Completed"
                            color="success"
                            variant="outlined"
                            sx={{ 
                              height: 16, 
                              fontSize: '0.6rem',
                              '& .MuiChip-label': { px: 0.5 }
                            }}
                          />
                        </Tooltip>
                      )}
                      
                      {tab.executionStatus === 'failed' && (
                        <Tooltip title={tab.lastExecutionTime ? `Failed at ${new Date(tab.lastExecutionTime).toLocaleTimeString()}` : 'Failed'}>
                          <Chip
                            size="small"
                            label="Failed"
                            color="error"
                            variant="outlined"
                            sx={{ 
                              height: 16, 
                              fontSize: '0.6rem',
                              '& .MuiChip-label': { px: 0.5 }
                            }}
                          />
                        </Tooltip>
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

          <Tooltip title={disabled ? "Tab operations disabled during processing" : "New Tab Options"}>
            <IconButton
              onClick={handleNewTabMenuOpen}
              disabled={disabled}
              size="small"
              sx={{
                marginLeft: 1,
                padding: '6px',
                color: isDarkMode ? 'rgba(255,255,255,0.7)' : 'rgba(0,0,0,0.6)',
                '&:hover': {
                  backgroundColor: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)',
                  color: isDarkMode ? 'rgba(255,255,255,0.9)' : 'rgba(0,0,0,0.8)'
                },
                '&.Mui-disabled': {
                  color: isDarkMode ? 'rgba(255,255,255,0.3)' : 'rgba(0,0,0,0.3)',
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
        {(() => {
          const tab = tabs.find(t => t.id === contextMenu?.tabId);
          return tab?.executionStatus && tab.executionStatus !== 'running' ? (
            <MenuItem onClick={() => {
              clearTabExecutionStatus(contextMenu?.tabId || '');
              handleContextMenuClose();
            }}>
              <CloseIcon sx={{ mr: 1, fontSize: 18 }} />
              Clear Execution Status
            </MenuItem>
          ) : null;
        })()}
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
        {tabs.length > 0 && (
          <>
            <Divider />
            <MenuItem onClick={() => {
              // Check if any tabs have unsaved changes
              const hasUnsavedChanges = tabs.some(tab => tab.isDirty);
              if (hasUnsavedChanges) {
                setCloseAllConfirmDialog(true);
              } else {
                clearAllTabs();
                // Create a new empty tab after clearing all
                createTab();
              }
              handleContextMenuClose();
            }}>
              <ClearAllIcon sx={{ mr: 1, fontSize: 18 }} />
              Close All Tabs
            </MenuItem>
          </>
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

      {/* Close All Confirmation Dialog */}
      <Dialog
        open={closeAllConfirmDialog}
        onClose={() => setCloseAllConfirmDialog(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <WarningIcon color="warning" />
            Close All Tabs
          </Box>
        </DialogTitle>
        <DialogContent>
          <Typography>
            Some tabs have unsaved changes. Do you want to save all changes before closing?
          </Typography>
          {tabs.filter(tab => tab.isDirty).length > 0 && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="body2" color="text.secondary">
                Tabs with unsaved changes:
              </Typography>
              <Box sx={{ mt: 1 }}>
                {tabs.filter(tab => tab.isDirty).map(tab => (
                  <Chip 
                    key={tab.id}
                    label={tab.name}
                    size="small"
                    sx={{ mr: 1, mb: 1 }}
                  />
                ))}
              </Box>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCloseAllConfirmDialog(false)} color="inherit">
            Cancel
          </Button>
          <Button 
            onClick={() => {
              clearAllTabs();
              createTab(); // Create a new empty tab
              setCloseAllConfirmDialog(false);
            }} 
            color="error"
          >
            Discard All Changes
          </Button>
          <Button 
            onClick={() => {
              // For now, just close without saving
              // In future, could implement a save all functionality
              setCloseAllConfirmDialog(false);
            }} 
            variant="contained" 
            color="primary"
          >
            Save All & Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default TabBar; 