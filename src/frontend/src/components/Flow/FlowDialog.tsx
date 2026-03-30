import React, { useState, useEffect, useRef } from 'react';
import { 
  Dialog, 
  DialogTitle, 
  DialogContent, 
  DialogActions, 
  Button, 
  Box, 
  Grid,
  Card,
  CardContent,
  Typography,
  IconButton,
  Tooltip,
  CircularProgress,
  Alert,
  TextField,
  InputAdornment,
  useTheme
} from '@mui/material';
import { FlowService } from '../../api/FlowService';
import { FlowResponse, FlowSelectionDialogProps } from '../../types/flow';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import DeleteIcon from '@mui/icons-material/Delete';
import CalendarTodayIcon from '@mui/icons-material/CalendarToday';
import SearchIcon from '@mui/icons-material/Search';
import DownloadIcon from '@mui/icons-material/Download';

const FlowDialog: React.FC<FlowSelectionDialogProps> = ({ open, onClose, onFlowSelect }): JSX.Element => {
  const [flows, setFlows] = useState<FlowResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const _theme = useTheme();
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (open) {
      loadFlows();
    }
  }, [open]);

  // Focus management when dialog opens
  const handleDialogEntered = () => {
    setTimeout(() => {
      if (searchInputRef.current) {
        searchInputRef.current.focus();
      }
    }, 150); // Increased delay for reliable focus
  };

  const loadFlows = async () => {
    setLoading(true);
    try {
      const fetchedFlows = await FlowService.getFlows();
      setFlows(fetchedFlows);
      setError(null);
    } catch (error) {
      console.error('Error loading flows:', error);
      setError('Failed to load flows');
    } finally {
      setLoading(false);
    }
  };

  const handleFlowSelect = async (flowId: string) => {
    try {
      if (!flowId) {
        throw new Error('Invalid flow ID');
      }
      
      // Fetch the flow using the string ID
      const selectedFlow = await FlowService.getFlow(flowId);
      if (selectedFlow) {
        onFlowSelect(selectedFlow.nodes, selectedFlow.edges, selectedFlow.flowConfig);
        onClose();
      }
    } catch (error) {
      console.error('Error selecting flow:', error);
      setError('Failed to select flow');
    }
  };

  const handleExportFlow = async (event: React.MouseEvent, flow: FlowResponse) => {
    event.stopPropagation();
    try {
      const exportData = JSON.stringify(flow, null, 2);
      const blob = new Blob([exportData], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `flow_${flow.name.replace(/\s+/g, '_').toLowerCase()}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting flow:', error);
      setError('Failed to export flow');
    }
  };

  const handleDeleteFlow = async (event: React.MouseEvent, flowId: string) => {
    event.stopPropagation();
    try {
      // Use the string ID directly
      await FlowService.deleteFlow(flowId);
      loadFlows();
    } catch (error) {
      console.error('Error deleting flow:', error);
      setError('Failed to delete flow');
    }
  };

  const filteredFlows = flows.filter(flow => 
    flow.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <Dialog 
      open={open} 
      onClose={onClose} 
      maxWidth="md" 
      fullWidth
      TransitionProps={{
        onEntered: handleDialogEntered
      }}
    >
      <DialogTitle>Open Flow</DialogTitle>
      <DialogContent>
        <Box sx={{ mb: 2, mt: 1 }}>
          <TextField
            fullWidth
            placeholder="Search flows..."
            inputRef={searchInputRef}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
              )
            }}
          />
        </Box>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
            <CircularProgress />
          </Box>
        ) : flows.length === 0 ? (
          <Typography variant="body1" align="center" sx={{ py: 4 }}>
            No flows available.
          </Typography>
        ) : filteredFlows.length === 0 ? (
          <Typography variant="body1" align="center" sx={{ py: 4 }}>
            No flows match your search.
          </Typography>
        ) : (
          <Grid container spacing={2}>
            {filteredFlows.map((flow) => (
              <Grid item xs={12} sm={6} md={4} key={flow.id}>
                <Card 
                  sx={{ 
                    cursor: 'pointer',
                    transition: 'all 0.2s',
                    '&:hover': {
                      boxShadow: 6,
                      transform: 'translateY(-4px)'
                    },
                    position: 'relative'
                  }}
                  onClick={() => handleFlowSelect(flow.id.toString())}
                >
                  <CardContent>
                    <Box sx={{ 
                      display: 'flex', 
                      alignItems: 'center', 
                      mb: 1.5,
                      justifyContent: 'space-between' 
                    }}>
                      <Typography variant="h6" noWrap sx={{ maxWidth: '80%' }}>
                        {flow.name}
                      </Typography>
                      <Box>
                        <Tooltip title="Export Flow">
                          <IconButton 
                            size="small" 
                            onClick={(e) => handleExportFlow(e, flow)}
                            sx={{ mr: 0.5 }}
                          >
                            <DownloadIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Delete Flow">
                          <IconButton 
                            size="small" 
                            onClick={(e) => handleDeleteFlow(e, flow.id.toString())}
                            color="error"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </Box>
                    </Box>

                    <Box 
                      sx={{ 
                        display: 'flex', 
                        alignItems: 'center', 
                        mb: 1.5,
                        gap: 0.5,
                        color: 'text.secondary'
                      }}
                    >
                      <AccountTreeIcon fontSize="small" />
                      <Typography variant="body2">
                        {flow.nodes.length} nodes
                      </Typography>
                    </Box>

                    <Box 
                      sx={{ 
                        display: 'flex', 
                        alignItems: 'center',
                        color: 'text.secondary'
                      }}
                    >
                      <CalendarTodayIcon fontSize="small" sx={{ mr: 0.5 }} />
                      <Typography variant="body2">
                        {new Date(flow.created_at).toLocaleDateString()}
                      </Typography>
                    </Box>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
      </DialogActions>
    </Dialog>
  );
};

export default FlowDialog; 