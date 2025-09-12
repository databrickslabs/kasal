import React, { useState, useRef } from 'react';
import {
  Box,
  Button,
  IconButton,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  Paper,
  LinearProgress,
  Alert,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tooltip,
  Tabs,
  Tab,
  Breadcrumbs,
  Link,
  CircularProgress,
  ListItemIcon,
  ListItemButton,
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  AttachFile as AttachFileIcon,
  Delete as DeleteIcon,
  InsertDriveFile as FileIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  Info as InfoIcon,
  Folder as FolderIcon,
  Storage as StorageIcon,
  ArrowBack as ArrowBackIcon,
} from '@mui/icons-material';
import { DatabricksService, DatabricksConfig } from '../../api/DatabricksService';
import { apiClient } from '../../config/api/ApiConfig';
import { AxiosProgressEvent } from 'axios';

interface UploadedFile {
  id: string;
  filename: string;
  path: string;
  size: number;
  status: 'uploading' | 'success' | 'error';
  progress?: number;
  error?: string;
  source?: 'upload' | 'volume';
}

interface VolumeFile {
  name: string;
  path: string;
  is_directory: boolean;
  size?: number;
  modified_at?: string;
  type?: string;
}

interface KnowledgeFileUploadProps {
  executionId: string;
  groupId: string;
  onFilesUploaded?: (files: UploadedFile[]) => void;
  onAgentsUpdated?: (updatedAgents: any[]) => void; // Callback to update canvas nodes
  disabled?: boolean;
  compact?: boolean;
  availableAgents?: any[]; // Agents currently on the canvas
}

export const KnowledgeFileUpload: React.FC<KnowledgeFileUploadProps> = ({
  executionId,
  groupId,
  onFilesUploaded,
  onAgentsUpdated,
  disabled = false,
  compact = false,
  availableAgents: providedAgents,
}) => {
  const [files, setFiles] = useState<UploadedFile[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [showDialog, setShowDialog] = useState(false);
  const [databricksConfig, setDatabricksConfig] = useState<DatabricksConfig | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tabValue, setTabValue] = useState(0); // 0 = Upload, 1 = Browse Volume
  const [volumeFiles, setVolumeFiles] = useState<VolumeFile[]>([]);
  const [currentPath, setCurrentPath] = useState<string>('');
  const [pathHistory, setPathHistory] = useState<string[]>([]);
  const [isLoadingVolume, setIsLoadingVolume] = useState(false);
  const [selectedVolumeFiles, setSelectedVolumeFiles] = useState<Set<string>>(new Set());
  interface AgentOption {
    id?: string;  // Make id optional to match Agent type
    name: string;
    role: string;
    goal: string;
    backstory: string;
    llm: string;
    tools: string[];
    max_iter: number;
    verbose: boolean;
    allow_delegation: boolean;
    cache: boolean;
    knowledge_sources?: any[];
  }
  const [availableAgents, setAvailableAgents] = useState<AgentOption[]>(providedAgents || []);
  const [selectedAgents, setSelectedAgents] = useState<string[]>([]);
  const [isLoadingAgents, setIsLoadingAgents] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Update available agents when providedAgents changes
  React.useEffect(() => {
    if (providedAgents) {
      setAvailableAgents(providedAgents);
      setIsLoadingAgents(false);
    } else if (showDialog && availableAgents.length === 0) {
      // If no agents provided and dialog is open, load all agents as fallback
      setIsLoadingAgents(true);
      const loadAgents = async () => {
        try {
          // Dynamic import to avoid circular dependencies
          const { AgentService } = await import('../../api/AgentService');
          const agents = await AgentService.listAgents();
          setAvailableAgents(agents);
        } catch (err) {
          console.error('Failed to load agents:', err);
        } finally {
          setIsLoadingAgents(false);
        }
      };
      loadAgents();
    }
  }, [showDialog, providedAgents]);

  // Load Databricks configuration
  React.useEffect(() => {
    const loadConfig = async () => {
      try {
        const service = DatabricksService.getInstance();
        const config = await service.getDatabricksConfig();
        setDatabricksConfig(config);
        
        if (config && !config.knowledge_volume_enabled) {
          setError('Knowledge volume is not enabled in Databricks configuration');
        }
      } catch (err) {
        console.error('Failed to load Databricks config:', err);
        setError('Failed to load Databricks configuration');
      }
    };
    loadConfig();
  }, []);

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFiles = event.target.files;
    if (!selectedFiles || selectedFiles.length === 0) return;

    // Convert FileList to array and create upload entries
    const newFiles: UploadedFile[] = Array.from(selectedFiles).map((file) => ({
      id: `${Date.now()}-${Math.random()}`,
      filename: file.name,
      path: '',
      size: file.size,
      status: 'uploading' as const,
      progress: 0,
    }));

    setFiles((prev) => [...prev, ...newFiles]);
    uploadFiles(Array.from(selectedFiles), newFiles);
    
    // Reset input
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const uploadFiles = async (fileList: File[], fileEntries: UploadedFile[]) => {
    if (!databricksConfig || !databricksConfig.knowledge_volume_enabled) {
      setError('Knowledge volume is not configured');
      return;
    }

    setIsUploading(true);
    setError(null);

    const volumeConfig = {
      volume_path: databricksConfig.knowledge_volume_path || 'main.default.knowledge',
      workspace_url: databricksConfig.workspace_url,
      token: undefined, // Will use environment variable
      file_format: 'auto' as const,
      chunk_size: databricksConfig.knowledge_chunk_size || 1000,
      chunk_overlap: databricksConfig.knowledge_chunk_overlap || 200,
      create_date_dirs: true,
      max_file_size_mb: 50,
      selected_agents: selectedAgents, // Add selected agents to volume config
    };

    // Upload each file
    for (let i = 0; i < fileList.length; i++) {
      const file = fileList[i];
      const entry = fileEntries[i];

      try {
        // Create FormData
        const formData = new FormData();
        formData.append('file', file);
        formData.append('volume_config', JSON.stringify(volumeConfig));

        // Upload file
        const response = await apiClient.post(
          `/databricks/knowledge/upload/${executionId}`,
          formData,
          {
            headers: {
              'Content-Type': 'multipart/form-data',
            },
            onUploadProgress: (progressEvent: AxiosProgressEvent) => {
              const progress = progressEvent.total
                ? Math.round((progressEvent.loaded * 100) / progressEvent.total)
                : 0;
              
              setFiles((prev) =>
                prev.map((f) =>
                  f.id === entry.id
                    ? { ...f, progress }
                    : f
                )
              );
            },
          }
        );

        console.log('Upload response:', response.data);

        // If agents are selected, update their knowledge sources
        if (selectedAgents.length > 0 && response.data.path) {
          await updateAgentKnowledgeSources(response.data.path, file.name);
        }

        // Update file status to success
        setFiles((prev) =>
          prev.map((f) =>
            f.id === entry.id
              ? {
                  ...f,
                  status: 'success' as const,
                  path: response.data.path,
                  progress: 100,
                }
              : f
          )
        );
      } catch (err) {
        console.error(`Failed to upload ${file.name}:`, err);
        
        // Update file status with error
        setFiles((prev) =>
          prev.map((f) =>
            f.id === entry.id
              ? {
                  ...f,
                  status: 'error',
                  error: err instanceof Error ? err.message : 'Upload failed',
                }
              : f
          )
        );
      }
    }

    setIsUploading(false);

    // Notify parent component
    if (onFilesUploaded) {
      onFilesUploaded(files);
    }
  };

  const updateAgentKnowledgeSources = async (filePath: string, fileName: string) => {
    try {
      // Dynamic import to avoid circular dependencies
      const { AgentService } = await import('../../api/AgentService');
      
      console.log('[DEBUG] updateAgentKnowledgeSources called:', {
        selectedAgents,
        availableAgents: availableAgents.map(a => ({ id: a.id, name: a.name })),
        filePath,
        fileName
      });
      
      // Collect updated agents to pass back to parent
      const updatedAgents: any[] = [];
      
      // Update each selected agent with the new knowledge source
      for (const agentId of selectedAgents) {
        console.log(`[DEBUG] Processing agent ID: ${agentId}`);
        const agent = availableAgents.find(a => (a.id || `agent-${a.name}`) === agentId);
        if (agent) {
          console.log(`[DEBUG] Found agent:`, { id: agent.id, name: agent.name, existingKnowledgeSources: agent.knowledge_sources });
          
          const knowledgeSource = {
            type: 'databricks_volume',
            source: filePath,
            metadata: {
              filename: fileName,
              execution_id: executionId,
              group_id: groupId,
              uploaded_at: new Date().toISOString(),
            }
          };
          
          const updatedAgent = {
            ...agent,
            knowledge_sources: [...(agent.knowledge_sources || []), knowledgeSource]
          };
          
          console.log(`[DEBUG] Updated agent with knowledge source:`, { 
            agentName: updatedAgent.name, 
            knowledgeSourcesCount: updatedAgent.knowledge_sources?.length 
          });
          
          // Add to collection of updated agents - ALWAYS add, even if no ID
          updatedAgents.push(updatedAgent);
          
          // Remove id and created_at for update
          const { id, created_at, ...agentData } = updatedAgent as any;
          // Only update if we have a valid agent ID
          if (agent.id) {
            const savedAgent = await AgentService.updateAgentFull(agent.id, agentData as any);
            if (savedAgent) {
              // Update the agent in the collection with the saved version
              updatedAgents[updatedAgents.length - 1] = savedAgent;
              console.log(`[DEBUG] Successfully persisted agent ${agent.name} with knowledge source to backend`);
            }
          } else {
            console.warn(`[DEBUG] Agent ${agent.name} has no ID, knowledge sources will be in memory only until agent is saved`);
          }
        }
      }
      
      // Notify parent component about updated agents
      if (onAgentsUpdated && updatedAgents.length > 0) {
        onAgentsUpdated(updatedAgents);
      }
    } catch (err) {
      console.error('Failed to update agent knowledge sources:', err);
    }
  };

  const handleRemoveFile = (fileId: string) => {
    setFiles((prev) => prev.filter((f) => f.id !== fileId));
  };

  const handleOpenDialog = () => {
    setShowDialog(true);
  };

  const handleCloseDialog = () => {
    setShowDialog(false);
  };

  const toggleAgentSelection = (agentId: string) => {
    setSelectedAgents(prev => {
      if (prev.includes(agentId)) {
        return prev.filter(id => id !== agentId);
      } else {
        return [...prev, agentId];
      }
    });
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  const browseVolumeFiles = async (path = '') => {
    setIsLoadingVolume(true);
    setError(null);
    
    try {
      // Use default volume path if not specified
      const volumePath = path || databricksConfig?.knowledge_volume_path || 'main.default.knowledge';
      
      const response = await apiClient.get<VolumeFile[]>(
        `/databricks/knowledge/browse/${encodeURIComponent(volumePath)}`
      );
      
      setVolumeFiles(response.data);
      setCurrentPath(volumePath);
    } catch (err) {
      console.error('Failed to browse volume:', err);
      setError('Failed to browse Databricks Volume');
      setVolumeFiles([]);
    } finally {
      setIsLoadingVolume(false);
    }
  };

  const navigateToFolder = (folderPath: string) => {
    setPathHistory([...pathHistory, currentPath]);
    browseVolumeFiles(folderPath);
  };

  const navigateBack = () => {
    if (pathHistory.length > 0) {
      const previousPath = pathHistory[pathHistory.length - 1];
      setPathHistory(pathHistory.slice(0, -1));
      browseVolumeFiles(previousPath);
    }
  };

  const toggleFileSelection = (filePath: string) => {
    const newSelection = new Set(selectedVolumeFiles);
    if (newSelection.has(filePath)) {
      newSelection.delete(filePath);
    } else {
      newSelection.add(filePath);
    }
    setSelectedVolumeFiles(newSelection);
  };

  const selectVolumeFiles = async () => {
    if (selectedVolumeFiles.size === 0) return;
    
    setIsUploading(true);
    setError(null);
    
    const selectedFilesList = Array.from(selectedVolumeFiles);
    const newFiles: UploadedFile[] = [];
    
    for (const filePath of selectedFilesList) {
      const filename = filePath.split('/').pop() || 'unknown';
      const fileEntry: UploadedFile = {
        id: `${Date.now()}-${Math.random()}`,
        filename,
        path: filePath,
        size: 0,
        status: 'uploading' as const,
        source: 'volume',
      };
      
      newFiles.push(fileEntry);
      
      try {
        // Register the file from volume
        const formData = new FormData();
        formData.append('file_path', filePath);
        
        const response = await apiClient.post(
          `/databricks/knowledge/select-from-volume/${executionId}`,
          formData
        );
        
        // Update file status
        setFiles((prev) =>
          prev.map((f) =>
            f.id === fileEntry.id
              ? {
                  ...f,
                  status: 'success',
                  path: response.data.path,
                }
              : f
          )
        );

        // If agents are selected, update their knowledge sources
        if (selectedAgents.length > 0 && response.data.path) {
          await updateAgentKnowledgeSources(response.data.path, filename);
        }
      } catch (err) {
        console.error(`Failed to select ${filename}:`, err);
        
        // Update file status with error
        setFiles((prev) =>
          prev.map((f) =>
            f.id === fileEntry.id
              ? {
                  ...f,
                  status: 'error',
                  error: err instanceof Error ? err.message : 'Selection failed',
                }
              : f
          )
        );
      }
    }
    
    setFiles((prev) => [...prev, ...newFiles]);
    setIsUploading(false);
    setSelectedVolumeFiles(new Set());
    
    // Notify parent component
    if (onFilesUploaded) {
      onFilesUploaded(newFiles);
    }
  };

  // Load volume files when switching to browse tab
  React.useEffect(() => {
    if (tabValue === 1 && showDialog && databricksConfig?.knowledge_volume_enabled && volumeFiles.length === 0) {
      browseVolumeFiles();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tabValue, showDialog, databricksConfig]);

  const isKnowledgeEnabled = databricksConfig?.enabled && databricksConfig?.knowledge_volume_enabled;

  return (
    <>
      {/* Upload Button */}
      <Tooltip
        title={
          !isKnowledgeEnabled
            ? 'Knowledge volume is not enabled in Databricks configuration'
            : 'Upload knowledge files for RAG'
        }
      >
        <span>
          <IconButton
            onClick={handleOpenDialog}
            disabled={disabled || !isKnowledgeEnabled}
            color="primary"
            size={compact ? "small" : "medium"}
            sx={compact ? {
              padding: '4px',
              color: 'text.secondary',
              '&:hover': {
                backgroundColor: 'action.hover',
                color: 'primary.main',
              },
            } : {}}
          >
            <AttachFileIcon sx={compact ? { fontSize: 18 } : {}} />
          </IconButton>
        </span>
      </Tooltip>

      {/* Upload Dialog */}
      <Dialog
        open={showDialog}
        onClose={handleCloseDialog}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <UploadIcon />
            <Typography variant="h6">Upload Knowledge Files</Typography>
          </Box>
        </DialogTitle>

        <DialogContent>
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}

          {!isKnowledgeEnabled && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              Knowledge volume is not enabled. Please configure it in the Databricks settings.
            </Alert>
          )}

          {isKnowledgeEnabled && (
            <>
              {/* Agent Selection Section */}
              <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
                <Typography variant="subtitle1" sx={{ mb: 1 }}>
                  Select Agents for Knowledge Access
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  Choose which agents from the canvas should have access to these knowledge files
                </Typography>
                
                {isLoadingAgents ? (
                  <Box sx={{ display: 'flex', justifyContent: 'center', p: 2 }}>
                    <CircularProgress size={24} />
                  </Box>
                ) : availableAgents.length > 0 ? (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                    {availableAgents.map((agent) => {
                      const agentId = agent.id || `agent-${agent.name}`; // Fallback if id is undefined
                      return (
                        <Chip
                          key={agentId}
                          label={agent.name}
                          onClick={() => toggleAgentSelection(agentId)}
                          color={selectedAgents.includes(agentId) ? 'primary' : 'default'}
                          variant={selectedAgents.includes(agentId) ? 'filled' : 'outlined'}
                        />
                      );
                    })}
                  </Box>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No agents on canvas. Add agents to the workflow canvas to enable knowledge assignment.
                  </Typography>
                )}

                {selectedAgents.length > 0 && (
                  <Alert severity="info" sx={{ mt: 2 }}>
                    Selected {selectedAgents.length} agent{selectedAgents.length > 1 ? 's' : ''} will have access to these knowledge files
                  </Alert>
                )}
              </Paper>

              {/* Tabs for Upload and Browse */}
              <Tabs value={tabValue} onChange={(e, v) => setTabValue(v)} sx={{ mb: 2 }}>
                <Tab label="Upload Files" icon={<UploadIcon />} iconPosition="start" />
                <Tab label="Browse Volume" icon={<StorageIcon />} iconPosition="start" />
              </Tabs>

              {/* Tab Panel: Upload */}
              {tabValue === 0 && (
                <>
                  <Alert severity="info" sx={{ mb: 2 }}>
                    Upload files to use as knowledge sources for this execution. Supported formats: PDF, TXT, JSON, CSV, DOC, DOCX
                  </Alert>

                  {/* File Input */}
                  <input
                    ref={fileInputRef}
                    type="file"
                    multiple
                    onChange={handleFileSelect}
                    style={{ display: 'none' }}
                    accept=".pdf,.txt,.json,.csv,.doc,.docx,.md"
                  />

                  <Button
                    variant="contained"
                    startIcon={<UploadIcon />}
                    onClick={() => fileInputRef.current?.click()}
                    disabled={isUploading}
                    fullWidth
                    sx={{ mb: 2 }}
                  >
                    Select Files to Upload
                  </Button>

              {/* File List */}
              {files.length > 0 && (
                <Paper variant="outlined" sx={{ p: 1 }}>
                  <List dense>
                    {files.map((file) => (
                      <ListItem key={file.id}>
                        <ListItemText
                          primary={
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <FileIcon fontSize="small" />
                              <Typography variant="body2">{file.filename}</Typography>
                              <Chip
                                label={formatFileSize(file.size)}
                                size="small"
                                variant="outlined"
                              />
                            </Box>
                          }
                          secondary={
                            file.status === 'uploading' ? (
                              <LinearProgress
                                variant="determinate"
                                value={file.progress}
                                sx={{ mt: 1 }}
                              />
                            ) : file.status === 'success' ? (
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
                                <CheckCircleIcon color="success" fontSize="small" />
                                <Typography variant="caption" color="success.main">
                                  Uploaded successfully
                                </Typography>
                              </Box>
                            ) : file.status === 'error' ? (
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
                                <ErrorIcon color="error" fontSize="small" />
                                <Typography variant="caption" color="error">
                                  {file.error || 'Upload failed'}
                                </Typography>
                              </Box>
                            ) : null
                          }
                        />
                        <ListItemSecondaryAction>
                          <IconButton
                            edge="end"
                            aria-label="delete"
                            onClick={() => handleRemoveFile(file.id)}
                            disabled={file.status === 'uploading'}
                            size="small"
                          >
                            <DeleteIcon />
                          </IconButton>
                        </ListItemSecondaryAction>
                      </ListItem>
                    ))}
                  </List>
                </Paper>
              )}

              {/* Volume Path Info */}
              <Box sx={{ mt: 2, p: 1.5, bgcolor: 'grey.50', borderRadius: 1 }}>
                <Typography variant="caption" color="text.secondary">
                  <InfoIcon fontSize="small" sx={{ verticalAlign: 'middle', mr: 0.5 }} />
                  Files will be uploaded to:
                </Typography>
                <Typography
                  variant="caption"
                  component="code"
                  sx={{
                    display: 'block',
                    mt: 0.5,
                    p: 0.5,
                    bgcolor: 'white',
                    border: '1px solid',
                    borderColor: 'grey.300',
                    borderRadius: 0.5,
                    fontFamily: 'monospace',
                  }}
                >
                  /Volumes/{databricksConfig.knowledge_volume_path?.replace(/\./g, '/')}/{groupId}/{executionId}/
                </Typography>
              </Box>
                </>
              )}

              {/* Tab Panel: Browse Volume */}
              {tabValue === 1 && (
                <>
                  <Alert severity="info" sx={{ mb: 2 }}>
                    Browse and select existing files from Databricks Volume
                  </Alert>

                  {/* Breadcrumb Navigation */}
                  {currentPath && (
                    <Breadcrumbs sx={{ mb: 2 }}>
                      <Link
                        component="button"
                        variant="body2"
                        onClick={() => browseVolumeFiles('')}
                        underline="hover"
                        sx={{ cursor: 'pointer' }}
                      >
                        Root
                      </Link>
                      {currentPath.split('/').filter(p => p).map((part, index, arr) => (
                        <Typography key={index} variant="body2" color={index === arr.length - 1 ? 'text.primary' : 'text.secondary'}>
                          {part}
                        </Typography>
                      ))}
                    </Breadcrumbs>
                  )}

                  {/* Back Button */}
                  {pathHistory.length > 0 && (
                    <Button
                      startIcon={<ArrowBackIcon />}
                      onClick={navigateBack}
                      sx={{ mb: 2 }}
                    >
                      Back
                    </Button>
                  )}

                  {/* Volume Files List */}
                  {isLoadingVolume ? (
                    <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
                      <CircularProgress />
                    </Box>
                  ) : volumeFiles.length > 0 ? (
                    <Paper variant="outlined" sx={{ mb: 2 }}>
                      <List>
                        {volumeFiles.map((item) => (
                          <ListItemButton
                            key={item.path}
                            onClick={() => {
                              if (item.is_directory) {
                                navigateToFolder(item.path);
                              } else {
                                toggleFileSelection(item.path);
                              }
                            }}
                            selected={selectedVolumeFiles.has(item.path)}
                          >
                            <ListItemIcon>
                              {item.is_directory ? (
                                <FolderIcon />
                              ) : (
                                <FileIcon />
                              )}
                            </ListItemIcon>
                            <ListItemText
                              primary={item.name}
                              secondary={
                                item.is_directory
                                  ? 'Folder'
                                  : `${formatFileSize(item.size || 0)} • ${item.type}`
                              }
                            />
                            {!item.is_directory && (
                              <Chip
                                label={selectedVolumeFiles.has(item.path) ? 'Selected' : 'Select'}
                                size="small"
                                color={selectedVolumeFiles.has(item.path) ? 'primary' : 'default'}
                                variant={selectedVolumeFiles.has(item.path) ? 'filled' : 'outlined'}
                              />
                            )}
                          </ListItemButton>
                        ))}
                      </List>
                    </Paper>
                  ) : (
                    <Paper variant="outlined" sx={{ p: 3, textAlign: 'center', mb: 2 }}>
                      <Typography color="text.secondary">
                        No files found in this directory
                      </Typography>
                    </Paper>
                  )}

                  {/* Select Files Button */}
                  {selectedVolumeFiles.size > 0 && (
                    <Button
                      variant="contained"
                      onClick={selectVolumeFiles}
                      disabled={isUploading}
                      fullWidth
                    >
                      Use Selected Files ({selectedVolumeFiles.size})
                    </Button>
                  )}
                </>
              )}
            </>
          )}
        </DialogContent>

        <DialogActions>
          <Button onClick={handleCloseDialog} disabled={isUploading}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};