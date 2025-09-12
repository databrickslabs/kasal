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
  Typography,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import PersonIcon from '@mui/icons-material/Person';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import { Agent, Tool, AgentDialogProps } from '../../types/agent';
import { AgentService } from '../../api/AgentService';
import { ToolService } from '../../api/ToolService';
import AgentForm from './AgentForm';
import AgentBestPractices from '../BestPractices/AgentBestPractices';

const AgentDialog: React.FC<AgentDialogProps> = ({
  open,
  onClose,
  onAgentSelect,
  agents,
  onShowAgentForm,
  fetchAgents,
  showErrorMessage,
  openInCreateMode = false,
}) => {
  const [isDeleting, setIsDeleting] = useState(false);
  const [showAgentForm, setShowAgentForm] = useState(openInCreateMode);
  const [selectedAgents, setSelectedAgents] = useState<Agent[]>([]);
  const [tools, setTools] = useState<Tool[]>([]);
  const [showBestPractices, setShowBestPractices] = useState(false);
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
      // If opening in create mode, show the form immediately
      if (openInCreateMode) {
        setShowAgentForm(true);
      }
    }
  }, [open, isInitialized, fetchAgents, loadTools, openInCreateMode]);

  useEffect(() => {
    if (!open) {
      setIsInitialized(false);
      setSelectedAgents([]);
      setShowAgentForm(false);
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

  const handleAgentSaved = async (agent?: Agent) => {
    setShowAgentForm(false);
    await fetchAgents();
    
    // If in create mode and agent was saved, place it on canvas and close
    if (openInCreateMode && agent) {
      onAgentSelect([agent]);
      onClose();
    }
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

  // If in create mode, show only the form
  if (openInCreateMode) {
    return (
      <>
        <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', pr: 5 }}>
            <Typography variant="h6">
              Create Agent
            </Typography>
            <Button
              startIcon={<HelpOutlineIcon />}
              onClick={() => setShowBestPractices(true)}
              variant="outlined"
              size="small"
              sx={{ ml: 2 }}
            >
              Best Practices
            </Button>
          </Box>
          <IconButton
            aria-label="close"
            onClick={onClose}
            sx={{ position: 'absolute', right: 8, top: 8 }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <AgentForm
            tools={tools}
            onCancel={onClose}
            onAgentSaved={handleAgentSaved}
            isCreateMode={true}
          />
        </DialogContent>
      </Dialog>
      
      {/* Best Practices Dialog */}
      <AgentBestPractices
        open={showBestPractices}
        onClose={() => setShowBestPractices(false)}
      />
    </>
    );
  }

  // Show the manage interface
  return (
    <>
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', pr: 5 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              Manage Agents
              <Tooltip title="Learn about agent best practices">
                <IconButton
                  size="small"
                  onClick={() => window.open('https://docs.crewai.com/core-concepts/Agents/', '_blank')}
                  sx={{ ml: 1 }}
                >
                  <HelpOutlineIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>
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