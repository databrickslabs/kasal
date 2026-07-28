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
import { FlowService } from '../../api/workflow/FlowService';
import { FlowResponse, FlowSelectionDialogProps } from '../../types/workflow/flow';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import DeleteIcon from '@mui/icons-material/Delete';
import CalendarTodayIcon from '@mui/icons-material/CalendarToday';
import SearchIcon from '@mui/icons-material/Search';
import DownloadIcon from '@mui/icons-material/Download';
import PublishButton from '../Crew/CrewFlowDialog/PublishButton';
import { usePublicationStore } from '../../store/publication';
import { usePermissions } from '../../hooks/usePermissions';

const FlowDialog: React.FC<FlowSelectionDialogProps> = ({ open, onClose, onFlowSelect }): JSX.Element => {
  // Operators run flows; they do not publish or delete them. Same rule as the
  // crew/flow catalog in CrewFlowDialog, enforced again in flows_router.
  const { canEdit, canDelete } = usePermissions();
  const [flows, setFlows] = useState<FlowResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const _theme = useTheme();
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (open) {
      loadFlows();
      refreshPublications();
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

  // Shared with the catalogue in CrewFlowDialog — a flow published there must
  // not still look unpublished here.
  const publishedFlowIds = usePublicationStore((s) => s.publishedFlowIds);
  const setPublished = usePublicationStore((s) => s.setPublished);
  const refreshPublications = usePublicationStore((s) => s.refresh);

  const handleDeleteFlow = async (event: React.MouseEvent, flowId: string) => {
    event.stopPropagation();
    try {
      // Use the string ID directly
      await FlowService.deleteFlow(flowId);
      // Drop the row locally rather than re-fetching: loadFlows() sets `loading`,
      // which swaps the whole grid for a spinner, so deleting one flow read as a
      // full-screen refresh.
      setFlows((current) => current.filter((f) => String(f.id) !== String(flowId)));
      setError(null);
    } catch (error) {
      console.error('Error deleting flow:', error);
      setError('Failed to delete flow');
      // The delete failed, so the row is still on the server — resync.
      loadFlows();
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
                    <Typography
                      variant="h6"
                      title={flow.name}
                      sx={{
                        // The name gets the full card width; the actions moved
                        // to a footer below. Sharing the row with icons clipped
                        // longer names to an unreadable stub.
                        display: '-webkit-box',
                        WebkitLineClamp: 2,
                        WebkitBoxOrient: 'vertical',
                        overflow: 'hidden',
                        lineHeight: 1.3,
                        mb: 1.5,
                      }}
                    >
                      {flow.name}
                    </Typography>

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

                    <Box
                      sx={{
                        display: 'flex',
                        justifyContent: 'flex-end',
                        alignItems: 'center',
                        gap: 0.25,
                        mt: 1.5,
                        pt: 1,
                        borderTop: 1,
                        borderColor: 'divider',
                      }}
                    >
                      {canEdit && (
                        <PublishButton
                          entityType="flow"
                          entityId={String(flow.id)}
                          entityName={flow.name}
                          published={publishedFlowIds.has(String(flow.id))}
                          onChanged={(isPublished) =>
                            setPublished('flow', String(flow.id), isPublished)
                          }
                        />
                      )}
                      <Tooltip title="Export Flow">
                        <IconButton
                          size="small"
                          onClick={(e) => handleExportFlow(e, flow)}
                        >
                          <DownloadIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      {canDelete && (
                        <Tooltip title="Delete Flow">
                          <IconButton
                            size="small"
                            onClick={(e) => handleDeleteFlow(e, flow.id.toString())}
                            color="error"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      )}
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