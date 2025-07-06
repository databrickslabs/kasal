import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  IconButton,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Divider,
  Box,
  Tooltip,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import PersonIcon from '@mui/icons-material/Person';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import { Agent, Tool, AgentDialogProps } from '../../types/agent';
import { AgentService } from '../../api/AgentService';
import { ToolService } from '../../api/ToolService';
import AgentForm from './AgentForm';

const AgentDialog: React.FC<AgentDialogProps> = ({
  open,
  onClose,
  onAgentSelect,
  agents,
  onShowAgentForm,
  fetchAgents,
  showErrorMessage,
}) => {
  const [isDeleting, setIsDeleting] = useState(false);
  const [showAgentForm, setShowAgentForm] = useState(false);
  const [selectedAgents, setSelectedAgents] = useState<Agent[]>([]);
  const [tools, setTools] = useState<Tool[]>([]);
  const [isInitialized, setIsInitialized] = useState(false);

  const loadTools = useCallback(async () => {
    try {
      const toolsList = await ToolService.listTools();
      setTools(toolsList.map(tool => ({
        ...tool,
        id: String(tool.id)
      })));
    } catch (error) {
      console.error('Error loading tools:', error);
    }
  }, []);

  useEffect(() => {
    if (open && !isInitialized) {
      void Promise.all([
        fetchAgents(),
        loadTools()
      ]);
      setIsInitialized(true);
    }
  }, [open, isInitialized, fetchAgents, loadTools]);

  useEffect(() => {
    if (!open) {
      setIsInitialized(false);
      setSelectedAgents([]);
    }
  }, [open]);

  const handleDeleteAgent = async (agent: Agent) => {
    if (!agent.id) return;
    
    try {
      setIsDeleting(true);
      const success = await AgentService.deleteAgent(agent.id);
      if (success) {
        await fetchAgents();
      }
    } catch (error) {
      console.error('Error deleting agent:', error);
    } finally {
      setIsDeleting(false);
    }
  };

  const handleShowAgentForm = () => {
    setShowAgentForm(true);
  };

  const handleAgentSaved = async () => {
    setShowAgentForm(false);
    await fetchAgents();
  };

  const handleAgentToggle = (agent: Agent) => {
    setSelectedAgents(prev => {
      const isSelected = prev.some(a => a.id === agent.id);
      if (isSelected) {
        return prev.filter(a => a.id !== agent.id);
      }
      return [...prev, agent];
    });
  };

  const handlePlaceAgents = () => {
    onAgentSelect(selectedAgents);
    setSelectedAgents([]);
    onClose();
  };

  const handleDeleteAllAgents = async () => {
    try {
      await AgentService.deleteAllAgents();
      setSelectedAgents([]);
      await fetchAgents();
    } catch (error) {
      console.error('Error deleting all agents:', error);
      if (error && typeof error === 'object' && 'response' in error) {
        const axiosError = error as { response?: { status: number; data: { detail: string } } };
        if (axiosError.response?.status === 409) {
          showErrorMessage(axiosError.response.data.detail || 'Cannot delete agents due to dependencies.', 'warning');
        } else {
          const detail = axiosError.response?.data?.detail || 'An unknown error occurred';
          showErrorMessage(`Error deleting agents: ${detail}`, 'error');
        }
      } else {
        showErrorMessage('An unknown error occurred while deleting agents.', 'error');
      }
    }
  };

  const handleSelectAll = () => {
    if (selectedAgents.length === agents.length) {
      setSelectedAgents([]);
    } else {
      setSelectedAgents([...agents]);
    }
  };

  return (
    <>
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
        <DialogTitle>
          Manage Agents
          <IconButton
            aria-label="close"
            onClick={onClose}
            sx={{ position: 'absolute', right: 8, top: 8 }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent sx={{ pb: 1 }}>
          <Box sx={{ mb: 2, display: 'flex', gap: 1 }}>
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={handleShowAgentForm}
            >
              Create Agent
            </Button>
            <Button
              variant="outlined"
              onClick={handleSelectAll}
            >
              {selectedAgents.length === agents.length ? 'Deselect All' : 'Select All'}
            </Button>
            <Button
              variant="outlined"
              color="error"
              startIcon={<DeleteIcon />}
              onClick={handleDeleteAllAgents}
            >
              Delete All
            </Button>
          </Box>

          <Divider sx={{ my: 2 }} />
          
          <List sx={{ maxHeight: '50vh', overflow: 'auto' }}>
            {agents.map((agent) => (
              <ListItemButton 
                key={agent.id}
                onClick={() => handleAgentToggle(agent)}
                selected={selectedAgents.some(a => a.id === agent.id)}
              >
                <ListItemIcon>
                  <PersonIcon />
                </ListItemIcon>
                <ListItemText
                  primary={agent.name}
                  secondary={agent.role}
                />
                <Tooltip title="Delete Agent">
                  <IconButton
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteAgent(agent);
                    }}
                    size="small"
                    color="error"
                    disabled={isDeleting}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </ListItemButton>
            ))}
          </List>
        </DialogContent>
        <DialogActions sx={{ px: 3, py: 2 }}>
          <Button
            variant="contained"
            onClick={handlePlaceAgents}
            disabled={selectedAgents.length === 0}
          >
            Place Selected ({selectedAgents.length})
          </Button>
        </DialogActions>
      </Dialog>

      {showAgentForm && (
        <Dialog open={showAgentForm} onClose={() => setShowAgentForm(false)} maxWidth="md" fullWidth>
          <DialogContent>
            <AgentForm
              tools={tools}
              onCancel={() => setShowAgentForm(false)}
              onAgentSaved={handleAgentSaved}
            />
          </DialogContent>
        </Dialog>
      )}
    </>
  );
};

export default AgentDialog;