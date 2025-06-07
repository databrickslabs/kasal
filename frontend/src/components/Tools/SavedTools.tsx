import React, { useState, useEffect } from 'react';
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  IconButton,
  Stack,
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
  Tabs,
  Tab,
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import { SavedToolsProps, ToolIcon } from '../../types/tool';
import { Tool, ToolService } from '../../api/ToolService';

const toolIcons: ToolIcon[] = [
  { value: 'screwdriver-wrench', label: 'Screwdriver Wrench' },
  { value: 'search', label: 'Search' },
  { value: 'code', label: 'Code' },
  { value: 'database', label: 'Database' },
  { value: 'file', label: 'File' },
  { value: 'globe', label: 'Web' },
  { value: 'robot', label: 'Robot' },
  { value: 'cogs', label: 'Settings' },
];

function SavedTools({ refreshTrigger }: SavedToolsProps): JSX.Element {
  const [tools, setTools] = useState<Tool[]>([]);
  const [editDialog, setEditDialog] = useState<boolean>(false);
  const [editedTool, setEditedTool] = useState<Tool>({} as Tool);


  useEffect(() => {
    loadTools();
  }, [refreshTrigger]);


  const loadTools = async () => {
    try {
      const toolsList = await ToolService.listTools();
      setTools(toolsList);
    } catch (error) {
      console.error('Error loading tools:', error);
    }
  };



  const handleEdit = (tool: Tool) => {
    setEditedTool(tool);
    setEditDialog(true);
  };

  const handleSaveEdit = async () => {
    try {
      if (editedTool.id) {
        await ToolService.updateTool(editedTool.id, editedTool);
        setEditDialog(false);
        loadTools();
      }
    } catch (error) {
      console.error('Error updating tool:', error);
    }
  };

  const handleDelete = async (toolId: number) => {
    if (window.confirm('Are you sure you want to delete this tool?')) {
      try {
        await ToolService.deleteTool(toolId);
        loadTools();
      } catch (error) {
        console.error('Error deleting tool:', error);
      }
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement> | SelectChangeEvent) => {
    const { name, value } = e.target;
    setEditedTool((prev: Tool) => ({
      ...prev,
      [name]: value
    }));
  };

  return (
    <>
      <Tabs value={0} sx={{ mb: 2 }}>
        <Tab label="Tools" />
      </Tabs>

      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Title</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Icon</TableCell>
              <TableCell>Category</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {tools.map((tool) => (
              <TableRow key={tool.id}>
                <TableCell>{tool.title}</TableCell>
                <TableCell>
                  {tool.description.length > 100 
                    ? `${tool.description.substring(0, 100)}...` 
                    : tool.description}
                </TableCell>
                <TableCell>{tool.icon}</TableCell>
                <TableCell>{tool.category || 'PreBuilt'}</TableCell>
                <TableCell>
                  <Stack direction="row" spacing={1}>
                    <IconButton 
                      size="small" 
                      onClick={() => handleEdit(tool)}
                      color="primary"
                    >
                      <EditIcon />
                    </IconButton>
                    <IconButton 
                      size="small" 
                      onClick={() => tool.id ? handleDelete(tool.id) : undefined}
                      color="error"
                    >
                      <DeleteIcon />
                    </IconButton>
                  </Stack>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>



      <Dialog open={editDialog} onClose={() => setEditDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Edit Tool</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              fullWidth
              label="Title"
              name="title"
              value={editedTool.title || ''}
              onChange={handleChange}
            />
            <TextField
              fullWidth
              label="Description"
              name="description"
              value={editedTool.description || ''}
              onChange={handleChange}
              multiline
              rows={3}
            />
            <FormControl fullWidth>
              <InputLabel>Icon</InputLabel>
              <Select
                name="icon"
                value={editedTool.icon || ''}
                onChange={handleChange}
                label="Icon"
              >
                {toolIcons.map((icon) => (
                  <MenuItem key={icon.value} value={icon.value}>
                    {icon.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditDialog(false)}>Cancel</Button>
          <Button onClick={handleSaveEdit} variant="contained">Save</Button>
        </DialogActions>
      </Dialog>
    </>
  );
}

export default SavedTools; 