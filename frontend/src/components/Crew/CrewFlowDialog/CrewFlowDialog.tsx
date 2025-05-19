import React, { useState, useEffect, useRef, ChangeEvent, KeyboardEvent } from 'react';
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
  Tabs,
  Tab
} from '@mui/material';
import { CrewService } from '../../../api/CrewService';
import { FlowService } from '../../../api/FlowService';
import { CrewResponse } from '../../../types/crews';
import { FlowResponse } from '../../../types/flow';
import { CrewFlowSelectionDialogProps } from '../../../types/crewflowDialog';
import { Node as _Node, Edge as _Edge } from 'reactflow';
import DeleteIcon from '@mui/icons-material/Delete';
import DownloadIcon from '@mui/icons-material/Download';
import SearchIcon from '@mui/icons-material/Search';
import CloseIcon from '@mui/icons-material/Close';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import PersonIcon from '@mui/icons-material/Person';
import EditIcon from '@mui/icons-material/Edit';
import UploadIcon from '@mui/icons-material/Upload';
import FileUploadIcon from '@mui/icons-material/FileUpload';
import EditFlowForm from '../../Flow/EditFlowForm';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`dialog-tabpanel-${index}`}
      aria-labelledby={`dialog-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ pt: 2 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

const CrewFlowSelectionDialog: React.FC<CrewFlowSelectionDialogProps> = ({
  open,
  onClose,
  onCrewSelect,
  onFlowSelect,
  initialTab = 0,
}): JSX.Element => {
  const [tabValue, setTabValue] = useState(initialTab);
  const [crews, setCrews] = useState<CrewResponse[]>([]);
  const [flows, setFlows] = useState<FlowResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [importSuccess, setImportSuccess] = useState<string | null>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [editFlowDialogOpen, setEditFlowDialogOpen] = useState(false);
  const [selectedFlowId, setSelectedFlowId] = useState<number | string | null>(null);
  const [_focusedCardIndex, _setFocusedCardIndex] = useState<number>(0);
  const firstCrewCardRef = useRef<HTMLDivElement>(null);
  const firstFlowCardRef = useRef<HTMLDivElement>(null);
  
  // Refs for file inputs
  const crewFileInputRef = useRef<HTMLInputElement>(null);
  const flowFileInputRef = useRef<HTMLInputElement>(null);
  const bulkFileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      // Load both crews and flows when dialog opens
      loadCrews();
      loadFlows();
    }
  }, [open]);

  // Focus management when dialog opens
  const handleDialogEntered = () => {
    // Focus only the search box when dialog opens
    setTimeout(() => {
      if (searchInputRef.current) {
        searchInputRef.current.focus();
      }
    }, 150);
  };

  const loadCrews = async () => {
    setLoading(true);
    try {
      const fetchedCrews = await CrewService.getCrews();
      setCrews(fetchedCrews);
      setError(null);
    } catch (error) {
      console.error('Error loading crews:', error);
      setError('Failed to load crews');
    } finally {
      setLoading(false);
    }
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

  const handleCrewSelect = async (crewId: string) => {
    try {
      setLoading(true);
      setError(null);
      
      const selectedCrew = await CrewService.getCrew(crewId);
      console.log('Selected crew data:', selectedCrew);
      
      if (!selectedCrew?.nodes || !selectedCrew?.edges) {
        throw new Error('Invalid crew data');
      }
      
      // Extract and validate nodes and edges
      const validatedNodes = selectedCrew.nodes || [];
      const validatedEdges = selectedCrew.edges || [];
      
      console.log('Passing to onCrewSelect - Nodes:', validatedNodes);
      console.log('Passing to onCrewSelect - Edges:', validatedEdges);
      
      // Make sure to pass both validatedNodes and validatedEdges to match the expected signature
      onCrewSelect(validatedNodes, validatedEdges);
      onClose();
    } catch (error) {
      console.error('Error selecting crew:', error);
      setError('Failed to load crew. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleFlowSelect = async (flowId: string) => {
    try {
      setLoading(true);
      setError(null);
      
      console.log(`Selecting flow with ID: ${flowId}`);
      const selectedFlow = await FlowService.getFlow(flowId);
      
      // Better error handling for null or missing data
      if (!selectedFlow) {
        throw new Error('Failed to load flow data from server');
      }
      
      if (!Array.isArray(selectedFlow.nodes) || !Array.isArray(selectedFlow.edges)) {
        console.error('Invalid flow structure:', selectedFlow);
        throw new Error('Invalid flow data structure');
      }

      // Extract any flow configuration from the response
      // The FlowService should map flow_config to flowConfig
      let flowConfig = selectedFlow.flowConfig;
      
      // If still no explicit flowConfig but we have nodes with listener data, rebuild the config
      if (!flowConfig && selectedFlow.nodes.some(node => node.data?.listener)) {
        const listeners = selectedFlow.nodes
          .filter(node => node.data?.listener)
          .map(node => ({
            id: `listener-${node.id}`,
            name: node.data.label || `Listener ${node.id}`,
            crewId: String(node.data.crewRef || ''),
            crewName: node.data.crewName || node.data.label || 'Unknown',
            tasks: node.data.listener.tasks || [],
            listenToTaskIds: node.data.listener.listenToTaskIds || [],
            listenToTaskNames: node.data.listener.listenToTaskNames || [],
            conditionType: node.data.listener.conditionType || 'NONE',
            state: node.data.listener.state || {
              stateType: 'unstructured',
              stateDefinition: '',
              stateData: {}
            }
          }));
        
        flowConfig = {
          id: `flow-${Date.now()}`,
          name: selectedFlow.name,
          listeners,
          actions: [],
          startingPoints: []
        };
      }

      // Ensure flowConfig is properly structured
      if (flowConfig) {
        flowConfig = {
          id: flowConfig.id || `flow-${Date.now()}`,
          name: flowConfig.name || selectedFlow.name,
          listeners: flowConfig.listeners || [],
          actions: flowConfig.actions || [],
          startingPoints: flowConfig.startingPoints || []
        };
      }

      onFlowSelect(selectedFlow.nodes, selectedFlow.edges, flowConfig);
      onClose();
      
      // Dispatch event to fit view after nodes are rendered
      setTimeout(() => {
        if (typeof window !== 'undefined') {
          const event = new CustomEvent('fitViewToNodes', { bubbles: true });
          window.dispatchEvent(event);
        }
      }, 100);
    } catch (err) {
      const error = err as Error;
      console.error('Error loading flow:', error);
      setError(error.message || 'Failed to load flow');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteFlow = async (event: React.MouseEvent, flowId: string) => {
    event.stopPropagation();
    try {
      setLoading(true);
      
      await FlowService.deleteFlow(flowId);
      loadFlows();
    } catch (error) {
      console.error('Error deleting flow:', error);
      setError('Failed to delete flow');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteCrew = async (event: React.MouseEvent, crewId: string) => {
    event.stopPropagation();
    try {
      await CrewService.deleteCrew(crewId);
      loadCrews();
    } catch (error) {
      console.error('Error deleting crew:', error);
      setError('Failed to delete crew');
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

  const handleExportCrew = async (event: React.MouseEvent, crew: CrewResponse) => {
    event.stopPropagation();
    try {
      const exportData = JSON.stringify(crew, null, 2);
      const blob = new Blob([exportData], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `crew_${crew.name.replace(/\s+/g, '_').toLowerCase()}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting crew:', error);
      setError('Failed to export crew');
    }
  };

  const handleEditFlow = async (event: React.MouseEvent, flowId: string) => {
    event.stopPropagation();
    try {
      setSelectedFlowId(flowId);
      setEditFlowDialogOpen(true);
    } catch (error) {
      console.error('Error editing flow:', error);
      setError('Failed to edit flow');
    }
  };
  
  const handleEditFlowDialogClose = () => {
    setEditFlowDialogOpen(false);
    setSelectedFlowId(null);
  };
  
  const handleFlowUpdated = () => {
    loadFlows();
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
    // Reset search when changing tabs
    setSearchQuery('');
  };

  // Import functions
  const handleImportCrewClick = () => {
    if (crewFileInputRef.current) {
      crewFileInputRef.current.click();
    }
  };

  const handleImportFlowClick = () => {
    if (flowFileInputRef.current) {
      flowFileInputRef.current.click();
    }
  };

  const handleBulkImportClick = () => {
    if (bulkFileInputRef.current) {
      bulkFileInputRef.current.click();
    }
  };

  const handleImportCrew = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || event.target.files.length === 0) return;
    
    try {
      setLoading(true);
      setError(null);
      setImportSuccess(null);
      
      const file = event.target.files[0];
      const fileContents = await file.text();
      const crewData = JSON.parse(fileContents);
      
      // Validate crew data
      if (!crewData.name) {
        throw new Error('Invalid crew data: missing name');
      }
      
      // Save crew
      await CrewService.saveCrew({
        name: crewData.name,
        nodes: crewData.nodes || [],
        edges: crewData.edges || [],
        agent_ids: crewData.agent_ids || [],
        task_ids: crewData.task_ids || []
      });
      
      await loadCrews();
      setImportSuccess('Crew imported successfully');
      
      // Reset file input
      event.target.value = '';
    } catch (err) {
      const error = err as Error;
      console.error('Error importing crew:', error);
      setError(`Failed to import crew: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleImportFlow = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || event.target.files.length === 0) return;
    
    try {
      setLoading(true);
      setError(null);
      setImportSuccess(null);
      
      const file = event.target.files[0];
      const fileContents = await file.text();
      const flowData = JSON.parse(fileContents);
      
      // Validate flow data
      if (!flowData.name) {
        throw new Error('Invalid flow data: missing name');
      }
      
      // Save flow
      await FlowService.saveFlow({
        name: flowData.name,
        crew_id: flowData.crew_id || 0,
        nodes: flowData.nodes || [],
        edges: flowData.edges || [],
        flowConfig: flowData.flowConfig || flowData.flow_config
      });
      
      await loadFlows();
      setImportSuccess('Flow imported successfully');
      
      // Reset file input
      event.target.value = '';
    } catch (err) {
      const error = err as Error;
      console.error('Error importing flow:', error);
      setError(`Failed to import flow: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleBulkImport = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || event.target.files.length === 0) return;
    
    try {
      setLoading(true);
      setError(null);
      setImportSuccess(null);
      
      const file = event.target.files[0];
      const fileContents = await file.text();
      const bulkData = JSON.parse(fileContents);
      
      // Validate bulk data
      if (!bulkData.crews && !bulkData.flows) {
        throw new Error('Invalid import data: missing crews or flows array');
      }
      
      let importedCrews = 0;
      let importedFlows = 0;
      
      // Import crews
      if (Array.isArray(bulkData.crews)) {
        for (const crew of bulkData.crews) {
          if (crew.name) {
            await CrewService.saveCrew({
              name: crew.name,
              nodes: crew.nodes || [],
              edges: crew.edges || [],
              agent_ids: crew.agent_ids || [],
              task_ids: crew.task_ids || []
            });
            importedCrews++;
          }
        }
      }
      
      // Import flows
      if (Array.isArray(bulkData.flows)) {
        for (const flow of bulkData.flows) {
          if (flow.name) {
            await FlowService.saveFlow({
              name: flow.name,
              crew_id: flow.crew_id || 0,
              nodes: flow.nodes || [],
              edges: flow.edges || [],
              flowConfig: flow.flowConfig || flow.flow_config
            });
            importedFlows++;
          }
        }
      }
      
      // Reload data
      if (importedCrews > 0) {
        await loadCrews();
      }
      
      if (importedFlows > 0) {
        await loadFlows();
      }
      
      setImportSuccess(`Import successful: ${importedCrews} crews and ${importedFlows} flows imported.`);
      
      // Reset file input
      event.target.value = '';
    } catch (err) {
      const error = err as Error;
      console.error('Error bulk importing:', error);
      setError(`Failed to import: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Export all
  const handleExportAllCrews = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Get all crews
      const allCrews = await CrewService.getCrews();
      
      // Package in a format for export
      const exportData = {
        crews: allCrews,
        exportDate: new Date().toISOString()
      };
      
      // Export to file
      const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `all_crews_export_${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting all crews:', error);
      setError('Failed to export all crews');
    } finally {
      setLoading(false);
    }
  };

  const handleExportAllFlows = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Get all flows
      const allFlows = await FlowService.getFlows();
      
      // Package in a format for export
      const exportData = {
        flows: allFlows,
        exportDate: new Date().toISOString()
      };
      
      // Export to file
      const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `all_flows_export_${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting all flows:', error);
      setError('Failed to export all flows');
    } finally {
      setLoading(false);
    }
  };

  const handleExportEverything = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Get all data
      const allCrews = await CrewService.getCrews();
      const allFlows = await FlowService.getFlows();
      
      // Package in a format for export
      const exportData = {
        crews: allCrews,
        flows: allFlows,
        exportDate: new Date().toISOString()
      };
      
      // Export to file
      const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `complete_export_${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting all data:', error);
      setError('Failed to export all data');
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent, id: string, type: 'crew' | 'flow', index: number) => {
    // Get the right array based on the type
    const itemsArray = type === 'crew' ? crews : flows;
    
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      if (type === 'crew') {
        handleCrewSelect(id);
      } else {
        handleFlowSelect(id.toString());
      }
    } else if (e.key === 'ArrowRight') {
      e.preventDefault();
      const nextIndex = (index + 1) % itemsArray.length;
      const nextElement = document.querySelector(`[data-card-index="${type}-${nextIndex}"]`) as HTMLElement;
      if (nextElement) nextElement.focus();
    } else if (e.key === 'ArrowLeft') {
      e.preventDefault();
      const prevIndex = (index - 1 + itemsArray.length) % itemsArray.length;
      const prevElement = document.querySelector(`[data-card-index="${type}-${prevIndex}"]`) as HTMLElement;
      if (prevElement) prevElement.focus();
    } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      // Calculate number of cards per row based on current viewport
      // Assuming 3 cards per row on desktop, 2 on tablet, 1 on mobile
      // This is a rough estimate that matches the Grid item sizing
      const width = window.innerWidth;
      let cardsPerRow = 1;
      if (width >= 960) cardsPerRow = 3; // md breakpoint
      else if (width >= 600) cardsPerRow = 2; // sm breakpoint
      
      // Calculate the vertical navigation
      const currentRow = Math.floor(index / cardsPerRow);
      const currentCol = index % cardsPerRow;
      let targetIndex;
      
      if (e.key === 'ArrowDown') {
        const nextRow = (currentRow + 1);
        targetIndex = nextRow * cardsPerRow + currentCol;
        // If we would go beyond the last row, wrap to the first
        if (targetIndex >= itemsArray.length) {
          // Go to same column in first row
          targetIndex = currentCol;
        }
      } else { // ArrowUp
        const prevRow = (currentRow - 1);
        if (prevRow < 0) {
          // Go to the same column in last row
          const lastRowIndex = Math.floor((itemsArray.length - 1) / cardsPerRow);
          targetIndex = lastRowIndex * cardsPerRow + currentCol;
          // If the last row doesn't have this column, go to the last item
          if (targetIndex >= itemsArray.length) {
            targetIndex = itemsArray.length - 1;
          }
        } else {
          targetIndex = prevRow * cardsPerRow + currentCol;
        }
      }
      
      // Focus the target element if it exists
      if (targetIndex >= 0 && targetIndex < itemsArray.length) {
        const targetElement = document.querySelector(`[data-card-index="${type}-${targetIndex}"]`) as HTMLElement;
        if (targetElement) targetElement.focus();
      }
    }
  };

  // Handle keyboard shortcuts at the dialog level
  const handleDialogKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    // Skip shortcuts if an input field is active
    if (
      document.activeElement instanceof HTMLInputElement ||
      document.activeElement instanceof HTMLTextAreaElement
    ) {
      return;
    }
    
    // When user presses '/' (forward slash) focus the search input
    if (event.key === '/' && searchInputRef.current) {
      event.preventDefault();
      searchInputRef.current.focus();
    }
    
    // When user presses 'f', focus the first card
    if (event.key === 'f') {
      event.preventDefault();
      if (tabValue === 0 && firstCrewCardRef.current) {
        firstCrewCardRef.current.focus();
      } else if (tabValue === 1 && firstFlowCardRef.current) {
        firstFlowCardRef.current.focus();
      }
    }
  };

  return (
    <>
      <Dialog
        open={open}
        onClose={onClose}
        maxWidth="md"
        fullWidth
        TransitionProps={{
          onEntered: handleDialogEntered,
        }}
        PaperProps={{
          component: "div", // This allows the dialog to receive focus
          role: "dialog",
          tabIndex: -1, // This allows the dialog to be part of the tab sequence
        }}
      >
        <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Box>Open Crew or Flow</Box>
          <IconButton onClick={onClose}>
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent onKeyDown={handleDialogKeyDown}>
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          {importSuccess && (
            <Alert severity="success" sx={{ mb: 2 }}>
              {importSuccess}
            </Alert>
          )}
          
          <Box sx={{ width: '100%' }}>
            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
              <Tabs value={tabValue} onChange={handleTabChange} aria-label="crew and flow tabs">
                <Tab 
                  icon={<PersonIcon />} 
                  iconPosition="start" 
                  label="Crews" 
                  id="crew-tab-0" 
                  aria-controls="tabpanel-0"
                  sx={{ textTransform: 'none' }}
                />
                <Tab 
                  icon={<AccountTreeIcon />} 
                  iconPosition="start" 
                  label="Flows" 
                  id="flow-tab-1" 
                  aria-controls="tabpanel-1" 
                  sx={{ textTransform: 'none' }}
                />
              </Tabs>
            </Box>

            {/* Search and action buttons */}
            <Box sx={{ py: 1, display: 'flex', justifyContent: 'space-between' }}>
              <Box sx={{ flex: 1 }}>
                <TextField
                  placeholder="Search..."
                  size="small"
                  fullWidth
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  inputRef={searchInputRef}
                  autoComplete="off"
                  onKeyDown={(e) => {
                    // Only handle Tab and Escape, let all other typing happen normally
                    if (e.key === 'Tab' && !e.shiftKey) {
                      // When Tab is pressed in the search box, move focus to the first card
                      e.preventDefault();
                      // Check which tab is active and focus the appropriate first card
                      if (tabValue === 0 && firstCrewCardRef.current) {
                        firstCrewCardRef.current.focus();
                      } else if (tabValue === 1 && firstFlowCardRef.current) {
                        firstFlowCardRef.current.focus();
                      }
                    } else if (e.key === 'Escape') {
                      setSearchQuery('');
                    }
                    // All other keys should work normally for typing
                  }}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <SearchIcon fontSize="small" />
                      </InputAdornment>
                    ),
                  }}
                />
              </Box>
              <Box sx={{ ml: 2, display: 'flex', gap: 1 }}>
                {tabValue === 0 ? (
                  <>
                    <Button
                      startIcon={<UploadIcon />}
                      variant="outlined"
                      size="small"
                      onClick={handleImportCrewClick}
                    >
                      Import
                    </Button>
                    <Button
                      startIcon={<DownloadIcon />}
                      variant="outlined"
                      size="small"
                      onClick={handleExportAllCrews}
                      disabled={crews.length === 0}
                    >
                      Export All
                    </Button>
                  </>
                ) : (
                  <>
                    <Button
                      startIcon={<UploadIcon />}
                      variant="outlined"
                      size="small"
                      onClick={handleImportFlowClick}
                    >
                      Import
                    </Button>
                    <Button
                      startIcon={<DownloadIcon />}
                      variant="outlined"
                      size="small"
                      onClick={handleExportAllFlows}
                      disabled={flows.length === 0}
                    >
                      Export All
                    </Button>
                  </>
                )}
                <Button
                  startIcon={<FileUploadIcon />}
                  variant="outlined"
                  size="small"
                  onClick={handleBulkImportClick}
                >
                  Bulk Import
                </Button>
                <Button
                  startIcon={<DownloadIcon />}
                  variant="outlined"
                  size="small"
                  onClick={handleExportEverything}
                  disabled={crews.length === 0 && flows.length === 0}
                >
                  Export All
                </Button>
              </Box>
            </Box>
            
            <TabPanel value={tabValue} index={0}>
              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                  <CircularProgress />
                </Box>
              ) : crews.length === 0 ? (
                <Alert severity="info">
                  No crews found. Create a crew by adding agents and tasks, then click Save Crew.
                </Alert>
              ) : (
                <Grid container spacing={2}>
                  {crews
                    .filter(crew => 
                      searchQuery === '' ||
                      crew.name.toLowerCase().includes(searchQuery.toLowerCase())
                    )
                    .map((crew, index) => (
                      <Grid item xs={12} sm={6} md={4} key={crew.id}>
                        <Card 
                          sx={{ 
                            height: '100%',
                            cursor: 'pointer',
                            '&:hover': {
                              boxShadow: 3,
                              bgcolor: 'action.hover'
                            },
                            '&:focus': {
                              outline: '2px solid',
                              outlineColor: 'primary.main',
                              boxShadow: 6,
                              bgcolor: 'action.hover'
                            },
                            opacity: 1,
                            filter: 'none',
                            transition: 'all 0.2s'
                          }}
                          onClick={() => handleCrewSelect(crew.id)}
                          onKeyDown={(e) => handleKeyDown(e, crew.id, 'crew', index)}
                          tabIndex={0}
                          ref={index === 0 ? firstCrewCardRef : undefined}
                          data-card-index={`crew-${index}`}
                        >
                          <CardContent>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                              <Typography variant="h6" component="h2" gutterBottom noWrap>
                                {crew.name}
                              </Typography>
                              <Box>
                                <Tooltip title="Export Crew">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleExportCrew(e, crew)}
                                  >
                                    <DownloadIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                                <Tooltip title="Delete Crew">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleDeleteCrew(e, crew.id)}
                                  >
                                    <DeleteIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                              </Box>
                            </Box>
                            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                              No description
                            </Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              Created: {new Date(crew.created_at).toLocaleString()}
                            </Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              Agents: {(() => {
                                // Count agents from nodes
                                const nodesCount = crew.nodes?.filter(n => 
                                  n.type === 'agent' || 
                                  n.type === 'agentNode' || 
                                  (n.data && (
                                    n.data.type === 'agent' || 
                                    n.data.agentId || 
                                    n.data.agentRef
                                  )) ||
                                  (typeof n.id === 'string' && (
                                    n.id.startsWith('agent-') || 
                                    n.id.startsWith('agent:')
                                  ))
                                ).length || 0;
                                
                                // Add agents directly from crew.agents if it exists
                                const agentsCount = (crew.agents && Array.isArray(crew.agents)) ? crew.agents.length : 0;
                                
                                // Add from agent_ids if it exists
                                const agentIdsCount = (crew.agent_ids && Array.isArray(crew.agent_ids)) ? crew.agent_ids.length : 0;
                                
                                // Return the largest count
                                return Math.max(nodesCount, agentsCount, agentIdsCount);
                              })()} / 
                              Tasks: {(() => {
                                // Count tasks from nodes
                                const nodesCount = crew.nodes?.filter(n => 
                                  n.type === 'task' || 
                                  n.type === 'taskNode' || 
                                  (n.data && (
                                    n.data.type === 'task' || 
                                    n.data.taskId || 
                                    n.data.taskRef
                                  )) ||
                                  (typeof n.id === 'string' && (
                                    n.id.includes('task') || 
                                    n.id.includes('Task')
                                  ))
                                ).length || 0;
                                
                                // Add tasks directly from crew.tasks if it exists
                                const tasksCount = (crew.tasks && Array.isArray(crew.tasks)) ? crew.tasks.length : 0;
                                
                                // Add from task_ids if it exists
                                const taskIdsCount = (crew.task_ids && Array.isArray(crew.task_ids)) ? crew.task_ids.length : 0;
                                
                                // Return the largest count
                                return Math.max(nodesCount, tasksCount, taskIdsCount);
                              })()}
                            </Typography>
                          </CardContent>
                        </Card>
                      </Grid>
                    ))}
                </Grid>
              )}
            </TabPanel>
            <TabPanel value={tabValue} index={1}>
              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                  <CircularProgress />
                </Box>
              ) : flows.length === 0 ? (
                <Alert severity="info">
                  No flows found. Create a flow by adding flow components, then save it.
                </Alert>
              ) : (
                <Grid container spacing={2}>
                  {flows
                    .filter(flow => 
                      searchQuery === '' ||
                      flow.name.toLowerCase().includes(searchQuery.toLowerCase())
                    )
                    .map((flow, index) => (
                      <Grid item xs={12} sm={6} md={4} key={flow.id}>
                        <Card 
                          sx={{ 
                            height: '100%',
                            cursor: 'pointer',
                            '&:hover': {
                              boxShadow: 3,
                              bgcolor: 'action.hover'
                            },
                            '&:focus': {
                              outline: '2px solid',
                              outlineColor: 'primary.main',
                              boxShadow: 6,
                              bgcolor: 'action.hover'
                            },
                            opacity: 1,
                            filter: 'none',
                            transition: 'all 0.2s'
                          }}
                          onClick={() => handleFlowSelect(flow.id.toString())}
                          onKeyDown={(e) => handleKeyDown(e, flow.id, 'flow', index)}
                          tabIndex={0}
                          ref={index === 0 ? firstFlowCardRef : undefined}
                          data-card-index={`flow-${index}`}
                        >
                          <CardContent>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                              <Typography variant="h6" component="h2" gutterBottom noWrap>
                                {flow.name}
                              </Typography>
                              <Box>
                                <Tooltip title="Edit Flow">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleEditFlow(e, flow.id.toString())}
                                  >
                                    <EditIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                                <Tooltip title="Export Flow">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleExportFlow(e, flow)}
                                  >
                                    <DownloadIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                                <Tooltip title="Delete Flow">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleDeleteFlow(e, flow.id.toString())}
                                  >
                                    <DeleteIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                              </Box>
                            </Box>
                            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                              No description
                            </Typography>
                            
                            {/* Crew list section */}
                            <Typography variant="subtitle2" color="text.primary" sx={{ mt: 1 }}>
                              Crews:
                            </Typography>
                            <Box
                              sx={{
                                mt: 1,
                                maxHeight: '80px',
                                overflowY: 'auto',
                                border: '1px solid',
                                borderColor: 'divider',
                                borderRadius: 1,
                                p: 1,
                                bgcolor: 'background.default',
                                '&::-webkit-scrollbar': {
                                  width: '8px',
                                },
                                '&::-webkit-scrollbar-track': {
                                  backgroundColor: 'background.paper',
                                },
                                '&::-webkit-scrollbar-thumb': {
                                  backgroundColor: 'primary.light',
                                  borderRadius: '4px',
                                }
                              }}
                            >
                              {flow.nodes && Array.isArray(flow.nodes) && flow.nodes
                                .filter(node => node.type === 'crewNode' || node.data?.crewName)
                                .map((node, index) => {
                                  const crewName = node.data?.crewName || node.data?.label || `Crew ${index + 1}`;
                                  return (
                                    <Typography 
                                      key={node.id} 
                                      variant="body2" 
                                      sx={{ 
                                        py: 0.5,
                                        borderBottom: index < flow.nodes.filter(n => n.type === 'crewNode' || n.data?.crewName).length - 1 ? 
                                          '1px solid' : 'none',
                                        borderColor: 'divider'
                                      }}
                                    >
                                      • {crewName}
                                    </Typography>
                                  );
                                })}
                              {(!flow.nodes || !Array.isArray(flow.nodes) || 
                                !flow.nodes.some(node => node.type === 'crewNode' || node.data?.crewName)) && (
                                <Typography variant="body2" color="text.secondary">
                                  No crews found
                                </Typography>
                              )}
                            </Box>
                            
                            <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1 }}>
                              Created: {new Date(flow.created_at).toLocaleString()}
                            </Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              Components: {flow.nodes?.length || 0} / 
                              Connections: {flow.edges?.length || 0}
                            </Typography>
                          </CardContent>
                        </Card>
                      </Grid>
                    ))}
                </Grid>
              )}
            </TabPanel>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose} color="primary">
            Cancel
          </Button>
        </DialogActions>
      </Dialog>

      {/* Edit Flow Dialog */}
      {selectedFlowId && (
        <EditFlowForm
          open={editFlowDialogOpen}
          onClose={handleEditFlowDialogClose}
          flowId={selectedFlowId}
          onSave={handleFlowUpdated}
        />
      )}
      
      {/* Hidden file inputs */}
      <input 
        type="file" 
        ref={crewFileInputRef} 
        style={{ display: 'none' }} 
        accept=".json"
        onChange={handleImportCrew}
      />
      <input 
        type="file" 
        ref={flowFileInputRef} 
        style={{ display: 'none' }} 
        accept=".json"
        onChange={handleImportFlow}
      />
      <input 
        type="file" 
        ref={bulkFileInputRef} 
        style={{ display: 'none' }} 
        accept=".json"
        onChange={handleBulkImport}
      />
    </>
  );
};

export default CrewFlowSelectionDialog; 