import React, { useState, useRef, useEffect } from 'react';
import { Dialog, DialogTitle, DialogContent, DialogActions, Button, TextField } from '@mui/material';
import { FlowService } from '../../api/FlowService';
import axios from 'axios';
import { Edge, Node } from 'reactflow';
import { useTabManagerStore } from '../../store/tabManager';
import { buildFlowConfiguration } from '../../utils/flowConfigBuilder';

interface SaveFlowProps {
  nodes: Node[];
  edges: Edge[];
  trigger: React.ReactElement;
  disabled?: boolean;
}

const SaveFlow: React.FC<SaveFlowProps> = ({ nodes, edges, trigger, disabled = false }) => {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const [isSaving, setIsSaving] = useState(false);

  const { activeTabId, updateTabFlowInfo } = useTabManagerStore();

  // Listen for the custom event to open the save flow dialog
  useEffect(() => {
    const handleOpenSaveFlowDialog = () => {
      if (!disabled) {
        setOpen(true);
      }
    };

    const handleUpdateExistingFlow = async (event: Event) => {
      if (disabled) return;

      const customEvent = event as CustomEvent;
      const { tabId, flowId } = customEvent.detail;

      // Perform the update directly without showing the dialog
      try {
        console.log('SaveFlow: Updating existing flow', { tabId, flowId });

        const tab = useTabManagerStore.getState().getTab(tabId);
        if (!tab) {
          console.error('SaveFlow: Tab not found for update', tabId);
          return;
        }

        setIsSaving(true);

        // Remove duplicate nodes before processing
        const uniqueNodes = tab.nodes.reduce((acc: typeof tab.nodes, node) => {
          if (!acc.some(n => n.id === node.id)) {
            acc.push(node);
          }
          return acc;
        }, []);

        console.log('SaveFlow: Deduplicated nodes for update:', {
          originalCount: tab.nodes.length,
          uniqueCount: uniqueNodes.length,
          duplicatesRemoved: tab.nodes.length - uniqueNodes.length
        });

        // Remove duplicate edges before saving
        const uniqueEdges = tab.edges.reduce((acc: Edge[], edge) => {
          const edgeKey = `${edge.source}-${edge.target}`;
          if (!acc.some(e => `${e.source}-${e.target}` === edgeKey)) {
            acc.push(edge);
          }
          return acc;
        }, []);

        // Filter edges to only include those that reference existing nodes
        const nodeIds = new Set(uniqueNodes.map(n => n.id));
        const validEdges = uniqueEdges.filter(edge =>
          nodeIds.has(edge.source) && nodeIds.has(edge.target)
        );

        // Build flowConfig from nodes and edges
        const flowConfig = buildFlowConfiguration(uniqueNodes, validEdges, tab.savedFlowName || tab.name);

        console.log('SaveFlow: About to update flow with data:', {
          flowId,
          name: tab.savedFlowName || tab.name,
          nodes: uniqueNodes.length,
          totalEdges: uniqueEdges.length,
          validEdges: validEdges.length,
          removedEdges: uniqueEdges.length - validEdges.length,
          listenersCount: flowConfig.listeners.length,
          actionsCount: flowConfig.actions.length
        });

        // Use the current tab's nodes and edges for update
        const updatedFlow = await FlowService.updateFlow(flowId, {
          name: tab.savedFlowName || tab.name,
          crew_id: tab.savedCrewId || '', // Use the saved crew ID from the tab
          nodes: uniqueNodes,
          edges: validEdges,
          flowConfig  // Include flowConfig with listeners, actions, and tasks
        });

        console.log('SaveFlow: Update successful', updatedFlow);

        // Update the tab's flow info and mark as clean
        const { updateTabFlowInfo, markTabClean } = useTabManagerStore.getState();
        updateTabFlowInfo(tabId, updatedFlow.id, updatedFlow.name);
        markTabClean(tabId);

        // Dispatch completion event
        setTimeout(() => {
          const completeEvent = new CustomEvent('updateFlowComplete', {
            detail: { flowId: updatedFlow.id, flowName: updatedFlow.name }
          });
          window.dispatchEvent(completeEvent);
        }, 100);

      } catch (error) {
        console.error('SaveFlow: Update failed', error);
        // Could show an error notification here
      } finally {
        setIsSaving(false);
      }
    };

    window.addEventListener('openSaveFlowDialog', handleOpenSaveFlowDialog);
    window.addEventListener('updateExistingFlow', handleUpdateExistingFlow);

    return () => {
      window.removeEventListener('openSaveFlowDialog', handleOpenSaveFlowDialog);
      window.removeEventListener('updateExistingFlow', handleUpdateExistingFlow);
    };
  }, [disabled]);

  const handleClickOpen = () => {
    if (disabled) return;
    setOpen(true);
  };

  const handleClose = () => {
    console.log('SaveFlow: handleClose called', {
      currentOpen: open,
      flowName: name,
      hasError: !!error
    });
    setOpen(false);
    setName('');
    setError('');
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
    console.log('SaveFlow: handleSave called', {
      event: e?.type,
      dialogOpen: open,
      flowName: name,
      hasError: !!error
    });

    if (e) {
      e.preventDefault();
    }

    if (!name.trim()) {
      setError('Flow name is required');
      return;
    }

    // Prevent multiple saves while one is in progress
    if (isSaving) {
      console.log('SaveFlow: Save already in progress, ignoring request');
      return;
    }

    setIsSaving(true);

    try {
      console.log('SaveFlow: Attempting to save flow', {
        name: name,
        nodes: nodes.length,
        edges: edges.length
      });

      // Remove duplicate nodes before processing
      const uniqueNodes = nodes.reduce((acc: typeof nodes, node) => {
        if (!acc.some(n => n.id === node.id)) {
          acc.push(node);
        }
        return acc;
      }, []);

      console.log('SaveFlow: Deduplicated nodes:', {
        originalCount: nodes.length,
        uniqueCount: uniqueNodes.length,
        duplicatesRemoved: nodes.length - uniqueNodes.length
      });

      // Remove duplicate edges before saving
      const uniqueEdges = edges.reduce((acc: Edge[], edge) => {
        const edgeKey = `${edge.source}-${edge.target}`;
        if (!acc.some(e => `${e.source}-${e.target}` === edgeKey)) {
          acc.push(edge);
        }
        return acc;
      }, []);

      // Filter edges to only include those that reference existing nodes
      const nodeIds = new Set(uniqueNodes.map(n => n.id));
      const validEdges = uniqueEdges.filter(edge =>
        nodeIds.has(edge.source) && nodeIds.has(edge.target)
      );

      console.log('SaveFlow: Filtered edges', {
        totalEdges: uniqueEdges.length,
        validEdges: validEdges.length,
        removedEdges: uniqueEdges.length - validEdges.length
      });

      // Get active tab to get crew_id
      const activeTab = useTabManagerStore.getState().getTab(activeTabId || '');
      const crew_id = activeTab?.savedCrewId || '';

      if (!crew_id) {
        setError('Please save the crew first before saving the flow');
        setIsSaving(false);
        return;
      }

      // Build flowConfig from nodes and edges
      const flowConfig = buildFlowConfiguration(uniqueNodes, validEdges, name);

      console.log('SaveFlow: Built flowConfig:', {
        listenersCount: flowConfig.listeners.length,
        actionsCount: flowConfig.actions.length,
        startingPointsCount: flowConfig.startingPoints.length
      });

      const savedFlow = await FlowService.saveFlow({
        name,
        crew_id,
        nodes: uniqueNodes,
        edges: validEdges,
        flowConfig  // Include flowConfig with listeners, actions, and tasks
      });

      console.log('SaveFlow: Save successful, closing dialog', savedFlow);

      // Update the tab's flow info
      if (activeTabId && savedFlow.id) {
        updateTabFlowInfo(activeTabId, savedFlow.id, name);
      }

      // Close dialog and reset state
      handleClose();

      // Wait for dialog to fully close before dispatching event
      setTimeout(() => {
        console.log('SaveFlow: Dispatching saveFlowComplete event');
        const event = new CustomEvent('saveFlowComplete', {
          detail: { flowId: savedFlow.id, flowName: name }
        });
        window.dispatchEvent(event);
      }, 100);
    } catch (error) {
      console.error('SaveFlow: Save failed', error);
      if (axios.isAxiosError(error) && error.response?.data) {
        const errorData = error.response.data;
        let errorMessage = 'Failed to save flow';

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
        setError(error instanceof Error ? error.message : 'Failed to save flow');
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
        <DialogTitle>Save Flow</DialogTitle>
        <DialogContent>
          <TextField
            inputRef={nameInputRef}
            margin="dense"
            label="Flow Name"
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
            disabled={isSaving}
          >
            {isSaving ? 'Saving...' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default SaveFlow;
