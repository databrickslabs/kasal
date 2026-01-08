import React, { useState, useCallback, useEffect, useRef } from 'react';
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
  TextField,
  InputAdornment,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Checkbox,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import SearchIcon from '@mui/icons-material/Search';
import { ToolService, Tool } from '../../api/ToolService';

export interface QuickToolSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onSelectTools: (tools: string[]) => void;
  currentTools?: string[];
  isUpdating?: boolean;
}

const QuickToolSelectionDialog: React.FC<QuickToolSelectionDialogProps> = ({
  open,
  onClose,
  onSelectTools,
  currentTools = [],
  isUpdating = false
}) => {
  const [tools, setTools] = useState<Tool[]>([]);
  const [selectedTools, setSelectedTools] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [focusedIndex, setFocusedIndex] = useState<number>(-1);

  const searchInputRef = useRef<HTMLInputElement>(null);

  // Filter tools based on search query
  const filteredTools = tools.filter(tool =>
    tool.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
    tool.description.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Fetch tools and set current selection when dialog opens
  useEffect(() => {
    if (open) {
      fetchTools();
      // Initialize with current tools - convert to strings for consistency
      setSelectedTools(currentTools.map(t => String(t)));
      setSearchQuery('');
      setFocusedIndex(-1);
      setTimeout(() => searchInputRef.current?.focus(), 100);
    }
  }, [open, currentTools]);

  const fetchTools = async () => {
    setIsLoading(true);
    try {
      const toolsList = await ToolService.listEnabledTools();
      setTools(toolsList);
    } catch (error) {
      console.error('Error fetching tools:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleToolToggle = (tool: Tool) => {
    const toolId = String(tool.id);
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

  const handleApply = useCallback(() => {
    onSelectTools(selectedTools);
    onClose();
  }, [selectedTools, onSelectTools, onClose]);

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
        event.preventDefault();
        if (focusedIndex >= 0 && focusedIndex < toolCount) {
          handleToolToggle(filteredTools[focusedIndex]);
        }
        break;
      case 'Enter':
        event.preventDefault();
        if (selectedTools.length > 0 || currentTools.length > 0) {
          handleApply();
        }
        break;
      default:
        break;
    }
  };

  // Helper to check if a tool is selected (handles both string and number IDs)
  const isToolSelected = (tool: Tool) => {
    const toolIdStr = String(tool.id);
    return selectedTools.includes(toolIdStr) || selectedTools.includes(tool.title);
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle sx={{ pb: 1 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Select Tools</Typography>
          <IconButton onClick={handleClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
        <Typography variant="caption" color="text.secondary">
          {selectedTools.length} tool{selectedTools.length !== 1 ? 's' : ''} selected
        </Typography>
      </DialogTitle>

      <DialogContent sx={{ pb: 1 }}>
        <TextField
          inputRef={searchInputRef}
          fullWidth
          placeholder="Search tools... (↑↓ navigate, Space toggle, Enter apply)"
          value={searchQuery}
          onChange={handleSearchChange}
          onKeyDown={handleKeyDown}
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
              dense
              sx={{
                maxHeight: '350px',
                overflow: 'auto',
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
                    }}
                  >
                    <ListItemIcon>
                      <Checkbox
                        edge="start"
                        checked={isToolSelected(tool)}
                        tabIndex={-1}
                        disableRipple
                      />
                    </ListItemIcon>
                    <ListItemText
                      primary={tool.title}
                      secondary={tool.description}
                      primaryTypographyProps={{
                        variant: 'body2',
                        fontWeight: isToolSelected(tool) ? 'bold' : 'normal',
                      }}
                      secondaryTypographyProps={{
                        variant: 'caption',
                        color: 'textSecondary',
                        sx: {
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          display: '-webkit-box',
                          WebkitLineClamp: 2,
                          WebkitBoxOrient: 'vertical',
                        }
                      }}
                    />
                  </ListItemButton>
                </ListItem>
              ))}
              {filteredTools.length === 0 && !isLoading && (
                <ListItem>
                  <ListItemText
                    primary="No tools found"
                    secondary="Try a different search term"
                  />
                </ListItem>
              )}
            </List>
          </FormControl>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={handleClose}>Cancel</Button>
        <Button
          variant="contained"
          onClick={handleApply}
          disabled={isUpdating}
        >
          {isUpdating ? <CircularProgress size={20} /> : 'Apply'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default QuickToolSelectionDialog;
