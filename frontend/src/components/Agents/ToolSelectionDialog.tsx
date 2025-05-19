/* eslint-disable react/prop-types */
import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
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
  Paper,
  TextField,
  InputAdornment,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Checkbox,
  Select,
  MenuItem,
  SelectChangeEvent
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import SearchIcon from '@mui/icons-material/Search';
import { ToolService, Tool } from '../../api/ToolService';
import { AgentService } from '../../api/AgentService';
import { TaskService } from '../../api/TaskService';
import { Node } from 'reactflow';

// Define a more specific type for our nodes
interface CanvasNode extends Node {
  data: {
    label: string;
    tools?: string[];
    role?: string;
    goal?: string;
    backstory?: string;
    llm?: string;
    max_rpm?: number;
  };
}

interface NodeTarget {
  id: string;
  label: string;
  type: 'agent' | 'task';
}

export interface ToolSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onSelectTools: (tools: string[], targetIds?: string[]) => void;
  isUpdating?: boolean;
  selectedNodes?: CanvasNode[];
}

const ToolSelectionDialog: React.FC<ToolSelectionDialogProps> = ({
  open,
  onClose,
  onSelectTools,
  isUpdating = false,
  selectedNodes = []
}) => {
  const [tools, setTools] = useState<Tool[]>([]);
  const [selectedTools, setSelectedTools] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [focusedIndex, setFocusedIndex] = useState<number>(-1);
  const [targetSelection, setTargetSelection] = useState<'all' | 'all_agents' | 'all_tasks' | 'selected' | string[]>('all');
  const [selectedTargets, setSelectedTargets] = useState<NodeTarget[]>([]);
  const [allAgents, setAllAgents] = useState<NodeTarget[]>([]);
  const [allTasks, setAllTasks] = useState<NodeTarget[]>([]);
  
  const searchInputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLUListElement>(null);

  // Group selected nodes by type
  const groupedNodes = useMemo(() => {
    const agents = selectedNodes.filter(node => node.type === 'agentNode')
      .map(node => ({
        id: node.id,
        label: node.data.label,
        type: 'agent' as const
      }));
    const tasks = selectedNodes.filter(node => node.type === 'taskNode')
      .map(node => ({
        id: node.id,
        label: node.data.label,
        type: 'task' as const
      }));
    return { agents, tasks };
  }, [selectedNodes]);

  // Filter tools based on search query
  const filteredTools = tools
    .filter(tool => tool.enabled !== false) // Only show enabled tools
    .filter(tool => 
      tool.title.toLowerCase().includes(searchQuery.toLowerCase()) || 
      tool.description.toLowerCase().includes(searchQuery.toLowerCase())
    );

  // Fetch all agents and tasks when dialog opens
  useEffect(() => {
    if (open) {
      fetchTools();
      fetchAllAgentsAndTasks();
      setSearchQuery('');
      setFocusedIndex(-1);
      setTargetSelection('all');
      setSelectedTargets([]);
      setTimeout(() => searchInputRef.current?.focus(), 100);
    }
  }, [open]);

  const fetchTools = async () => {
    setIsLoading(true);
    try {
      const toolsList = await ToolService.listTools();
      setTools(toolsList);
    } catch (error) {
      console.error('Error fetching tools:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const fetchAllAgentsAndTasks = async () => {
    try {
      const [agents, tasks] = await Promise.all([
        AgentService.listAgents(),
        TaskService.listTasks()
      ]);

      setAllAgents(agents.map(agent => ({
        id: agent.id?.toString() || '',
        label: agent.name,
        type: 'agent' as const
      })));

      setAllTasks(tasks.map(task => ({
        id: task.id?.toString() || '',
        label: task.name,
        type: 'task' as const
      })));
    } catch (error) {
      console.error('Error fetching agents and tasks:', error);
    }
  };

  const handleToolToggle = (tool: Tool) => {
    const toolId = String(tool.id); // Ensure it's a string for consistency
    setSelectedTools(prev => 
      prev.includes(toolId) 
        ? prev.filter(id => id !== toolId)
        : [...prev, toolId]
    );
  };

  const handleClose = useCallback(() => {
    setSelectedTools([]);
    onClose();
  }, [onClose]);

  const handleTargetChange = (event: SelectChangeEvent<string>) => {
    const value = event.target.value;
    if (value === 'all') {
      setTargetSelection('all');
      setSelectedTargets([]);
    } else if (value === 'all_agents') {
      setTargetSelection('all_agents');
      setSelectedTargets(groupedNodes.agents);
    } else if (value === 'all_tasks') {
      setTargetSelection('all_tasks');
      setSelectedTargets(groupedNodes.tasks);
    } else if (value === 'selected') {
      setTargetSelection('selected');
      setSelectedTargets([]);
    } else if (value.startsWith('agent_')) {
      const agentId = value.replace('agent_', '');
      const agent = groupedNodes.agents.find(a => a.id === agentId);
      if (agent) {
        setTargetSelection([agentId]);
        setSelectedTargets([agent]);
      }
    } else if (value.startsWith('task_')) {
      const taskId = value.replace('task_', '');
      const task = groupedNodes.tasks.find(t => t.id === taskId);
      if (task) {
        setTargetSelection([taskId]);
        setSelectedTargets([task]);
      }
    }
  };

  const handleTargetSelect = (target: { id: string; label: string; type: 'agent' | 'task' }) => {
    setSelectedTargets(prev => {
      const exists = prev.some(t => t.id === target.id);
      if (exists) {
        return prev.filter(t => t.id !== target.id);
      }
      return [...prev, target];
    });
  };

  const handleApply = useCallback(() => {
    if (selectedTools.length > 0) {
      let targetIds: string[] | undefined;
      
      if (targetSelection === 'all') {
        targetIds = [...allAgents.map(a => a.id), ...allTasks.map(t => t.id)];
      } else if (targetSelection === 'all_agents') {
        targetIds = allAgents.map(a => a.id);
      } else if (targetSelection === 'all_tasks') {
        targetIds = allTasks.map(t => t.id);
      } else {
        targetIds = selectedTargets.map(t => t.id);
      }
      
      onSelectTools(selectedTools, targetIds);
      handleClose();
    }
  }, [selectedTools, onSelectTools, handleClose, targetSelection, selectedTargets, allAgents, allTasks]);

  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(event.target.value);
    setFocusedIndex(0);
  };

  const handleKeyDown = (event: React.KeyboardEvent) => {
    const toolCount = filteredTools.length;
    
    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        setFocusedIndex(prev => (prev + 1) % toolCount);
        break;
      case 'ArrowUp':
        event.preventDefault();
        setFocusedIndex(prev => (prev - 1 + toolCount) % toolCount);
        break;
      case ' ':
      case 'Enter':
        event.preventDefault();
        if (focusedIndex >= 0 && focusedIndex < toolCount) {
          handleToolToggle(filteredTools[focusedIndex]);
        }
        break;
      default:
        break;
    }
  };

  return (
    <Dialog 
      open={open} 
      onClose={handleClose} 
      maxWidth="sm" 
      fullWidth
      PaperComponent={Paper}
    >
      <DialogTitle sx={{ pb: 1 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Select Tools</Typography>
          <IconButton onClick={handleClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ pb: 1 }}>
        <FormControl fullWidth sx={{ mb: 2 }}>
          <Select
            value={Array.isArray(targetSelection) ? 
                   targetSelection[0] : 
                   targetSelection}
            onChange={handleTargetChange}
            size="small"
            displayEmpty
            sx={{ 
              '& .MuiSelect-select': {
                display: 'flex',
                alignItems: 'center'
              }
            }}
          >
            <MenuItem value="all">All Agents & Tasks</MenuItem>
            <MenuItem value="all_agents" disabled={groupedNodes.agents.length === 0}>
              All Agents ({groupedNodes.agents.length})
            </MenuItem>
            <MenuItem value="all_tasks" disabled={groupedNodes.tasks.length === 0}>
              All Tasks ({groupedNodes.tasks.length})
            </MenuItem>
            <MenuItem value="selected" disabled={selectedNodes.length === 0}>
              Selected ({selectedNodes.length} {selectedNodes.length === 1 ? 'Node' : 'Nodes'})
            </MenuItem>
            {groupedNodes.agents.length > 0 && (
              <MenuItem value="individual_agents" disabled>
                <Typography variant="caption" color="text.secondary">Individual Agents:</Typography>
              </MenuItem>
            )}
            {groupedNodes.agents.map(agent => (
              <MenuItem 
                key={`agent_${agent.id}`} 
                value={`agent_${agent.id}`}
                sx={{ pl: 3 }}
              >
                {agent.label}
              </MenuItem>
            ))}
            {groupedNodes.tasks.length > 0 && (
              <MenuItem value="individual_tasks" disabled>
                <Typography variant="caption" color="text.secondary">Individual Tasks:</Typography>
              </MenuItem>
            )}
            {groupedNodes.tasks.map(task => (
              <MenuItem 
                key={`task_${task.id}`} 
                value={`task_${task.id}`}
                sx={{ pl: 3 }}
              >
                {task.label}
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        {targetSelection === 'selected' && (
          <Box sx={{ mb: 2, maxHeight: '150px', overflow: 'auto' }}>
            <Typography variant="subtitle2" sx={{ mb: 1 }}>Select Nodes:</Typography>
            {groupedNodes.agents.length > 0 && (
              <Box sx={{ mb: 1 }}>
                <Typography variant="caption" color="text.secondary">Agents:</Typography>
                {groupedNodes.agents.map(agent => (
                  <Box
                    key={agent.id}
                    onClick={() => handleTargetSelect(agent)}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      p: 1,
                      cursor: 'pointer',
                      borderRadius: 1,
                      bgcolor: selectedTargets.some(t => t.id === agent.id) ? 'action.selected' : 'transparent',
                      '&:hover': { bgcolor: 'action.hover' }
                    }}
                  >
                    <Checkbox
                      size="small"
                      checked={selectedTargets.some(t => t.id === agent.id)}
                      onChange={() => handleTargetSelect(agent)}
                    />
                    <Typography variant="body2">{agent.label}</Typography>
                  </Box>
                ))}
              </Box>
            )}
            {groupedNodes.tasks.length > 0 && (
              <Box>
                <Typography variant="caption" color="text.secondary">Tasks:</Typography>
                {groupedNodes.tasks.map(task => (
                  <Box
                    key={task.id}
                    onClick={() => handleTargetSelect(task)}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      p: 1,
                      cursor: 'pointer',
                      borderRadius: 1,
                      bgcolor: selectedTargets.some(t => t.id === task.id) ? 'action.selected' : 'transparent',
                      '&:hover': { bgcolor: 'action.hover' }
                    }}
                  >
                    <Checkbox
                      size="small"
                      checked={selectedTargets.some(t => t.id === task.id)}
                      onChange={() => handleTargetSelect(task)}
                    />
                    <Typography variant="body2">{task.label}</Typography>
                  </Box>
                ))}
              </Box>
            )}
          </Box>
        )}

        <TextField
          inputRef={searchInputRef}
          fullWidth
          placeholder="Search tools... (↑↓ to navigate, Space/Enter to select)"
          value={searchQuery}
          onChange={handleSearchChange}
          onKeyDown={handleKeyDown}
          margin="normal"
          variant="outlined"
          size="small"
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon />
              </InputAdornment>
            ),
          }}
          sx={{ mb: 2 }}
        />

        {isLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
            <CircularProgress size={24} />
          </Box>
        ) : (
          <FormControl component="fieldset" fullWidth>
            <List 
              ref={listRef}
              dense
              sx={{ 
                maxHeight: '300px', 
                overflow: 'auto',
                '& .Mui-selected': {
                  backgroundColor: theme => `rgba(${theme.palette.primary.main}, 0.1)`,
                }
              }}
            >
              {filteredTools.map((tool, index) => (
                <ListItem key={tool.id} disablePadding dense>
                  <ListItemButton
                    selected={index === focusedIndex}
                    onClick={() => handleToolToggle(tool)}
                    sx={{
                      borderRadius: 1,
                      mb: 0.5,
                      '&.Mui-selected': {
                        bgcolor: theme => `rgba(${theme.palette.primary.main}, 0.1)`,
                      }
                    }}
                  >
                    <ListItemIcon>
                      <Checkbox
                        edge="start"
                        checked={selectedTools.includes(String(tool.id))}
                        tabIndex={-1}
                        disableRipple
                      />
                    </ListItemIcon>
                    <ListItemText
                      primary={tool.title}
                      secondary={tool.description}
                      primaryTypographyProps={{
                        variant: 'body2',
                        fontWeight: selectedTools.includes(String(tool.id)) ? 'bold' : 'normal',
                      }}
                      secondaryTypographyProps={{
                        variant: 'caption',
                        color: 'textSecondary',
                      }}
                    />
                  </ListItemButton>
                </ListItem>
              ))}
            </List>
          </FormControl>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={handleClose}>Cancel</Button>
        <Box sx={{ position: 'relative' }}>
          <Button
            variant="contained"
            onClick={handleApply}
            disabled={selectedTools.length === 0 || isUpdating}
          >
            Apply Tools
          </Button>
          {isUpdating && (
            <CircularProgress
              size={24}
              sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                marginTop: '-12px',
                marginLeft: '-12px',
              }}
            />
          )}
        </Box>
      </DialogActions>
    </Dialog>
  );
};

export default ToolSelectionDialog; 