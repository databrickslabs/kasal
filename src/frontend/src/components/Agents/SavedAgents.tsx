import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Tooltip
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import MemoryIcon from '@mui/icons-material/Memory';
import axios from 'axios';
import { Agent, SavedAgentsProps } from '../../types/agent';
import { AgentService } from '../../api/AgentService';
import { ToolService } from '../../api/ToolService';

const SavedAgents: React.FC<SavedAgentsProps> = ({ refreshTrigger }) => {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [editedAgent, setEditedAgent] = useState<Agent | null>(null);
  const [tools, setTools] = useState<string[]>([]);

  useEffect(() => {
    const fetchAgents = async () => {
      const fetchedAgents = await AgentService.listAgents();
      setAgents(fetchedAgents);
    };

    const fetchTools = async () => {
      const fetchedTools = await ToolService.listTools();
      setTools(fetchedTools.map(tool => tool.title));
    };

    fetchAgents();
    fetchTools();
  }, [refreshTrigger]);

  const handleEdit = (agent: Agent) => {
    setEditedAgent(agent);
    setEditDialogOpen(true);
  };

  const handleDelete = async (agentId: string | undefined) => {
    if (!agentId) return;
    const success = await AgentService.deleteAgent(agentId);
    if (success) {
      setAgents(agents.filter(agent => agent.id !== agentId));
    }
  };

  const handleSave = async () => {
    if (!editedAgent || !editedAgent.id) return;

    const updatedAgent = await AgentService.updateAgent(editedAgent.id, {
      name: editedAgent.name,
      role: editedAgent.role,
      goal: editedAgent.goal,
      backstory: editedAgent.backstory
    });

    if (updatedAgent) {
      setAgents(agents.map(agent => 
        agent.id === editedAgent.id ? updatedAgent : agent
      ));
      setEditDialogOpen(false);
      setEditedAgent(null);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement> | SelectChangeEvent<string | string[]>) => {
    if (!editedAgent) return;

    const { name, value } = e.target;
    setEditedAgent(prev => {
      if (!prev) return prev;
      return {
        ...prev,
        [name]: value
      };
    });
  };

  const getMemoryDetails = (agent: Agent) => {
    if (!agent.memory) return null;
    
    const embedderDetails = agent.embedder_config 
      ? `${agent.embedder_config.provider || 'openai'} embeddings` 
      : 'default OpenAI embeddings';
      
    return (
      <Tooltip title={`Uses ${embedderDetails}`}>
        <Chip 
          icon={<MemoryIcon fontSize="small" />} 
          label="Memory Enabled" 
          size="small" 
          color="primary" 
          variant="outlined"
          sx={{ mr: 1 }}
        />
      </Tooltip>
    );
  };

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Role</TableCell>
              <TableCell>Goal</TableCell>
              <TableCell>Backstory</TableCell>
              <TableCell>Tools</TableCell>
              <TableCell>Features</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {agents.map((agent) => (
              <TableRow key={agent.id}>
                <TableCell>{agent.name}</TableCell>
                <TableCell>{agent.role}</TableCell>
                <TableCell>{agent.goal}</TableCell>
                <TableCell>{agent.backstory}</TableCell>
                <TableCell>{agent.tools.join(', ')}</TableCell>
                <TableCell>
                  {agent.memory && getMemoryDetails(agent)}
                </TableCell>
                <TableCell align="right">
                  <IconButton onClick={() => handleEdit(agent)}>
                    <EditIcon />
                  </IconButton>
                  <IconButton 
                    onClick={() => agent.id && handleDelete(agent.id)}
                    disabled={!agent.id}
                  >
                    <DeleteIcon />
                  </IconButton>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog 
        open={editDialogOpen} 
        onClose={() => setEditDialogOpen(false)} 
        maxWidth="md" 
        fullWidth
        PaperProps={{
          sx: { 
            display: 'flex', 
            flexDirection: 'column',
            height: '85vh', 
            maxHeight: '85vh' 
          }
        }}
      >
        <DialogTitle>Edit Agent</DialogTitle>
        <DialogContent sx={{ flex: 1, overflow: 'auto', p: 2 }}>
          {editedAgent && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 2 }}>
              <TextField
                fullWidth
                label="Name"
                name="name"
                value={editedAgent.name}
                onChange={handleChange}
              />
              <TextField
                fullWidth
                label="Role"
                name="role"
                value={editedAgent.role}
                onChange={handleChange}
                multiline
                rows={2}
              />
              <TextField
                fullWidth
                label="Goal"
                name="goal"
                value={editedAgent.goal}
                onChange={handleChange}
                multiline
                rows={2}
              />
              <TextField
                fullWidth
                label="Backstory"
                name="backstory"
                value={editedAgent.backstory}
                onChange={handleChange}
                multiline
                rows={3}
              />
              <TextField
                fullWidth
                label="LLM"
                name="llm"
                value={editedAgent.llm}
                onChange={handleChange}
              />
              <FormControl fullWidth>
                <InputLabel>Tools</InputLabel>
                <Select
                  multiple
                  name="tools"
                  value={editedAgent.tools}
                  onChange={handleChange}
                  label="Tools"
                >
                  {tools.map((tool: string) => (
                    <MenuItem key={tool} value={tool}>
                      {tool}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Box>
          )}
        </DialogContent>
        <DialogActions sx={{ 
          p: 2, 
          borderTop: '1px solid rgba(0, 0, 0, 0.12)', 
          backgroundColor: 'white' 
        }}>
          <Button onClick={() => setEditDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleSave} variant="contained" color="primary">
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default SavedAgents; 