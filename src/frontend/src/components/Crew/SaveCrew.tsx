import React, { useState, useRef, useEffect } from 'react';
import { Dialog, DialogTitle, DialogContent, DialogActions, Button, TextField } from '@mui/material';
import { CrewService } from '../../api/CrewService';
import axios from 'axios';
import { SaveCrewProps } from '../../types/crews';
import { Edge } from 'reactflow';
import { useTabManagerStore } from '../../store/tabManager';

interface SaveCrewComponentProps extends SaveCrewProps {
  disabled?: boolean;
}

const SaveCrew: React.FC<SaveCrewComponentProps> = ({ nodes, edges, trigger, disabled = false }) => {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const [isSaving, setIsSaving] = useState(false);
  
  const { activeTabId, updateTabCrewInfo } = useTabManagerStore();

  // Listen for the custom event to open the save crew dialog
  useEffect(() => {
    const handleOpenSaveCrewDialog = () => {
      if (!disabled) {
        setOpen(true);
      }
    };
    
    const handleUpdateExistingCrew = async (event: Event) => {
      if (disabled) return;
      
      const customEvent = event as CustomEvent;
      const { tabId, crewId } = customEvent.detail;
      
      // Perform the update directly without showing the dialog
      try {
        console.log('SaveCrew: Updating existing crew', { tabId, crewId });
        
        const tab = useTabManagerStore.getState().getTab(tabId);
        if (!tab) {
          console.error('SaveCrew: Tab not found for update', tabId);
          return;
        }
        
        setIsSaving(true);
        
        // Remove duplicate edges before saving - use tab edges not component edges
        const uniqueEdges = tab.edges.reduce((acc: Edge[], edge) => {
          const edgeKey = `${edge.source}-${edge.target}`;
          if (!acc.some(e => `${e.source}-${e.target}` === edgeKey)) {
            acc.push(edge);
          }
          return acc;
        }, []);

        console.log('SaveCrew: About to update crew with data:', {
          crewId,
          name: tab.savedCrewName || tab.name,
          nodes: tab.nodes.length,
          edges: uniqueEdges.length
        });

        // Use the current tab's nodes and edges for update
        const updatedCrew = await CrewService.updateCrew(crewId, {
          name: tab.savedCrewName || tab.name,
          agent_ids: [], // Will be calculated in the service
          task_ids: [], // Will be calculated in the service
          nodes: tab.nodes,
          edges: uniqueEdges
        });
        
        console.log('SaveCrew: Update successful', updatedCrew);
        
        // Update the tab's crew info and mark as clean
        const { updateTabCrewInfo, markTabClean } = useTabManagerStore.getState();
        updateTabCrewInfo(tabId, updatedCrew.id, updatedCrew.name);
        markTabClean(tabId);
        
        // Dispatch completion event
        setTimeout(() => {
          const completeEvent = new CustomEvent('updateCrewComplete', {
            detail: { crewId: updatedCrew.id, crewName: updatedCrew.name }
          });
          window.dispatchEvent(completeEvent);
        }, 100);
        
      } catch (error) {
        console.error('SaveCrew: Update failed', error);
        // Could show an error notification here
      } finally {
        setIsSaving(false);
      }
    };

    const handleUpdateExistingCrewByName = async (event: Event) => {
      if (disabled) return;
      
      const customEvent = event as CustomEvent;
      const { tabId, crewName } = customEvent.detail;
      
      try {
        console.log('SaveCrew: Updating crew by name:', { tabId, crewName });
        
        const tab = useTabManagerStore.getState().getTab(tabId);
        if (!tab) {
          console.error('SaveCrew: Tab not found for update by name', tabId);
          return;
        }
        
        setIsSaving(true);
        
        // First, get all crews to find the one with matching name
        const allCrews = await CrewService.getCrews();
        const matchingCrew = allCrews.find(crew => crew.name === crewName);
        
        if (!matchingCrew) {
          console.error('SaveCrew: No crew found with name:', crewName);
          // Fallback to showing save dialog if crew not found
          setOpen(true);
          return;
        }
        
        console.log('SaveCrew: Found matching crew:', matchingCrew.id, 'for name:', crewName);
        
        // Remove duplicate edges before saving
        const uniqueEdges = tab.edges.reduce((acc: Edge[], edge) => {
          const edgeKey = `${edge.source}-${edge.target}`;
          if (!acc.some(e => `${e.source}-${e.target}` === edgeKey)) {
            acc.push(edge);
          }
          return acc;
        }, []);

        // Use the found crew ID to update
        const updatedCrew = await CrewService.updateCrew(matchingCrew.id.toString(), {
          name: crewName,
          agent_ids: [], // Will be calculated in the service
          task_ids: [], // Will be calculated in the service
          nodes: tab.nodes,
          edges: uniqueEdges
        });
        
        console.log('SaveCrew: Update by name successful', updatedCrew);
        
        // Update the tab's crew info and mark as clean
        const { updateTabCrewInfo, markTabClean } = useTabManagerStore.getState();
        updateTabCrewInfo(tabId, updatedCrew.id, updatedCrew.name);
        markTabClean(tabId);
        
        // Dispatch completion event
        setTimeout(() => {
          const completeEvent = new CustomEvent('updateCrewComplete', {
            detail: { crewId: updatedCrew.id, crewName: updatedCrew.name }
          });
          window.dispatchEvent(completeEvent);
        }, 100);
        
      } catch (error) {
        console.error('SaveCrew: Update by name failed', error);
        // Fallback to showing save dialog on error
        setOpen(true);
      } finally {
        setIsSaving(false);
      }
    };
    
    window.addEventListener('openSaveCrewDialog', handleOpenSaveCrewDialog);
    window.addEventListener('updateExistingCrew', handleUpdateExistingCrew);
    window.addEventListener('updateExistingCrewByName', handleUpdateExistingCrewByName);
    
    return () => {
      window.removeEventListener('openSaveCrewDialog', handleOpenSaveCrewDialog);
      window.removeEventListener('updateExistingCrew', handleUpdateExistingCrew);
      window.removeEventListener('updateExistingCrewByName', handleUpdateExistingCrewByName);
    };
  }, [disabled]);

  const handleClickOpen = () => {
    if (disabled) return;
    setOpen(true);
  };

  const handleClose = () => {
    console.log('SaveCrew: handleClose called', {
      currentOpen: open,
      crewName: name,
      hasError: !!error
    });
    setOpen(false);
    setName('');
    setError('');
  };

  // Focus management with Dialog's callback
  const _handleDialogEntered = () => {
    setTimeout(() => {
      if (nameInputRef.current) {
        nameInputRef.current.focus();
      }
    }, 150); // Increased delay to ensure dialog is fully rendered
  };

  // Handle Enter key press in the name input
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      e.stopPropagation();
      handleSave();
    }
  };

  const handleSave = async (e?: React.FormEvent) => {
    console.log('SaveCrew: handleSave called', {
      event: e?.type,
      dialogOpen: open,
      crewName: name,
      hasError: !!error
    });

    if (e) {
      e.preventDefault();
    }

    if (!name.trim()) {
      setError('Crew name is required');
      return;
    }

    // Prevent multiple saves while one is in progress
    if (isSaving) {
      console.log('SaveCrew: Save already in progress, ignoring request');
      return;
    }

    setIsSaving(true);

    try {
      console.log('SaveCrew: Attempting to save crew', {
        name: name,
        nodes: nodes.length,
        edges: edges.length
      });

      // Remove duplicate edges before saving
      const uniqueEdges = edges.reduce((acc: Edge[], edge) => {
        const edgeKey = `${edge.source}-${edge.target}`;
        if (!acc.some(e => `${e.source}-${e.target}` === edgeKey)) {
          acc.push(edge);
        }
        return acc;
      }, []);

      // Filter agent nodes and ensure agentId is a valid number
      const agentNodes = nodes.filter(node => node.type === 'agentNode');
      const agent_ids = agentNodes
        .filter(node => {
          // First try to get ID directly from data.agentId
          const agentIdFromData = node.data?.agentId;
          if (agentIdFromData !== undefined && agentIdFromData !== null && !isNaN(Number(agentIdFromData))) {
            return true;
          }
          
          // Try to extract from node ID (agent-123-uuid format)
          const match = node.id.match(/^agent-(\d+)/);
          return match && match[1];
        })
        .map(node => {
          // Extract ID from the most appropriate source
          const agentIdFromData = node.data?.agentId;
          if (agentIdFromData !== undefined && agentIdFromData !== null && !isNaN(Number(agentIdFromData))) {
            return String(Number(agentIdFromData));
          }
          
          const match = node.id.match(/^agent-(\d+)/);
          return match ? match[1] : null;
        })
        .filter(Boolean) as string[];

      // Filter task nodes and ensure taskId is a valid number
      const taskNodes = nodes.filter(node => node.type === 'taskNode');
      const task_ids = taskNodes
        .filter(node => {
          // First try to get ID directly from data.taskId
          const taskIdFromData = node.data?.taskId;
          if (taskIdFromData !== undefined && taskIdFromData !== null && !isNaN(Number(taskIdFromData))) {
            return true;
          }
          
          // Try to extract from node ID (task-123-uuid format)
          const match = node.id.match(/^task-(\d+)/);
          return match && match[1];
        })
        .map(node => {
          // Extract ID from the most appropriate source
          const taskIdFromData = node.data?.taskId;
          if (taskIdFromData !== undefined && taskIdFromData !== null && !isNaN(Number(taskIdFromData))) {
            return String(Number(taskIdFromData));
          }
          
          const match = node.id.match(/^task-(\d+)/);
          return match ? match[1] : null;
        })
        .filter(Boolean) as string[];

      console.log('SaveCrew: Processed IDs', { agent_ids, task_ids });

      // Ensure task nodes have complete config with markdown field
      const processedNodes = nodes.map(node => {
        if (node.type === 'taskNode') {
          // Ensure we have a config object, create one if it doesn't exist
          const existingConfig = node.data?.config || {};
          
          // Debug logging
          console.log(`SaveCrew: Processing task node ${node.id}`, {
            topLevelMarkdown: node.data?.markdown,
            configMarkdown: existingConfig.markdown,
            hasConfig: !!node.data?.config
          });
          
          const processedNode = {
            ...node,
            data: {
              ...node.data,
              config: {
                ...existingConfig,
                // Ensure markdown is included in config, prioritize top-level markdown
                markdown: node.data?.markdown !== undefined ? node.data.markdown : (existingConfig.markdown || false)
              }
            }
          };
          
          console.log(`SaveCrew: Processed task node ${node.id}`, {
            resultMarkdown: processedNode.data.config.markdown
          });
          
          return processedNode;
        }
        return node;
      });

      const savedCrew = await CrewService.saveCrew({
        name,
        agent_ids,
        task_ids,
        nodes: processedNodes,
        edges: uniqueEdges
      });
      
      console.log('SaveCrew: Save successful, closing dialog', savedCrew);
      
      // Update the tab's crew info
      if (activeTabId && savedCrew.id) {
        updateTabCrewInfo(activeTabId, savedCrew.id, name);
      }
      
      // Close dialog and reset state
      handleClose();
      
      // Wait for dialog to fully close before dispatching event
      setTimeout(() => {
        console.log('SaveCrew: Dispatching saveCrewComplete event', {
          dialogOpen: document.querySelector('.MuiDialog-root') !== null
        });
        const event = new CustomEvent('saveCrewComplete', {
          detail: { crewId: savedCrew.id, crewName: name }
        });
        window.dispatchEvent(event);
      }, 100);
    } catch (error) {
      console.error('SaveCrew: Save failed', error);
      if (axios.isAxiosError(error) && error.response?.data) {
        const errorData = error.response.data;
        let errorMessage = 'Failed to save crew';
        
        if (typeof errorData === 'string') {
          errorMessage = errorData;
        } else if (errorData.detail && Array.isArray(errorData.detail)) {
          errorMessage = errorData.detail[0]?.msg || errorData.detail[0] || 'Validation error';
        } else if (errorData.detail) {
          errorMessage = errorData.detail;
        } else if (errorData.message) {
          errorMessage = errorData.message;
        }
        
        setError(errorMessage);
      } else {
        setError(error instanceof Error ? error.message : 'Failed to save crew');
      }
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <>
      {React.cloneElement(trigger, { onClick: handleClickOpen, disabled })}
      <Dialog 
        open={open} 
        onClose={handleClose} 
        maxWidth="sm" 
        fullWidth
        component="form"
        onSubmit={handleSave}
      >
        <DialogTitle>Save Crew</DialogTitle>
        <DialogContent>
          <TextField
            inputRef={nameInputRef}
            margin="dense"
            label="Crew Name"
            type="text"
            fullWidth
            value={name}
            onChange={(e) => setName(e.target.value)}
            error={!!error}
            helperText={error}
            onKeyDown={handleKeyDown}
            autoFocus
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose}>Cancel</Button>
          <Button 
            type="submit"
            variant="contained" 
            color="primary"
          >
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default SaveCrew; 