import React, { useState, useRef, useEffect } from 'react';
import { Dialog, DialogTitle, DialogContent, DialogActions, Button, TextField } from '@mui/material';
import { CrewService } from '../../api/CrewService';
import axios from 'axios';
import { SaveCrewProps } from '../../types/crews';
import { Edge } from 'reactflow';

interface SaveCrewComponentProps extends SaveCrewProps {
  disabled?: boolean;
}

const SaveCrew: React.FC<SaveCrewComponentProps> = ({ nodes, edges, trigger, disabled = false }) => {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const [isSaving, setIsSaving] = useState(false);

  // Listen for the custom event to open the save crew dialog
  useEffect(() => {
    const handleOpenSaveCrewDialog = () => {
      if (!disabled) {
        setOpen(true);
      }
    };
    
    window.addEventListener('openSaveCrewDialog', handleOpenSaveCrewDialog);
    
    return () => {
      window.removeEventListener('openSaveCrewDialog', handleOpenSaveCrewDialog);
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

      await CrewService.saveCrew({
        name,
        agent_ids,
        task_ids,
        nodes,
        edges: uniqueEdges
      });
      
      console.log('SaveCrew: Save successful, closing dialog');
      
      // Close dialog and reset state
      handleClose();
      
      // Wait for dialog to fully close before dispatching event
      setTimeout(() => {
        console.log('SaveCrew: Dispatching saveCrewComplete event', {
          dialogOpen: document.querySelector('.MuiDialog-root') !== null
        });
        const event = new CustomEvent('saveCrewComplete');
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