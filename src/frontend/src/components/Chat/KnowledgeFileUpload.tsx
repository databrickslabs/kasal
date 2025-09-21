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
  CircularProgress,
  ListItemIcon,
  ListItemButton,
  Fade,
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  AttachFile as AttachFileIcon,
  Delete as DeleteIcon,
  InsertDriveFile as FileIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  Folder as FolderIcon,
} from '@mui/icons-material';
import { DatabricksService, DatabricksConfig } from '../../api/DatabricksService';
import { AgentService, Agent } from '../../api/AgentService';
import { apiClient } from '../../config/api/ApiConfig';
import { AxiosProgressEvent } from 'axios';
import { useKnowledgeConfigStore } from '../../store/knowledgeConfigStore';
import { useAgentStore } from '../../store/agent';

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
    // knowledge_sources removed - using DatabricksKnowledgeSearchTool instead
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
        formData.append('agent_ids', JSON.stringify(selectedAgents)); // Send agent IDs as separate field

        console.log('🚀 [UPLOAD DEBUG] Preparing upload:', {
          filename: file.name,
          selectedAgents,
          selectedAgentsJson: JSON.stringify(selectedAgents),
          volumeConfig,
          executionId
        });

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

        console.log('✅ [UPLOAD DEBUG] Upload response:', {
          responseData: response.data,
          sentAgentIds: JSON.stringify(selectedAgents),
          filename: file.name
        });

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

  const { updateAgent } = useAgentStore();

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
          console.log(`[DEBUG] Found agent:`, { id: agent.id, name: agent.name, existingTools: agent.tools });

          // Add DatabricksKnowledgeSearchTool to the agent's tools if not already present
          const toolName = 'DatabricksKnowledgeSearchTool';
          const currentTools = agent.tools || [];

          let updatedAgent = agent;
          if (!currentTools.includes(toolName)) {
            updatedAgent = {
              ...agent,
              tools: [...currentTools, toolName]
            };
            console.log(`[DEBUG] Added ${toolName} to agent ${agent.name}'s tools`);
          } else {
            console.log(`[DEBUG] Agent ${agent.name} already has ${toolName} in tools`);
            updatedAgent = agent; // Keep the agent unchanged
          }

          console.log(`[DEBUG] Updated agent with knowledge search tool:`, {
            agentName: updatedAgent.name,
            toolsCount: updatedAgent.tools?.length,
            tools: updatedAgent.tools
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

              // Update the Zustand store to ensure consistency
              if (savedAgent.id) {
                updateAgent(savedAgent.id, savedAgent);
              }
            }
          } else {
            console.warn(`[DEBUG] Agent ${agent.name} has no ID, tool configuration will be in memory only until agent is saved`);
            // Still update the local availableAgents array for in-memory agents
            const agentIndex = availableAgents.findIndex(a => a.name === agent.name);
            if (agentIndex !== -1) {
              availableAgents[agentIndex] = updatedAgent;
            }
          }
        }
      }

      // Notify parent component about updated agents
      if (onAgentsUpdated && updatedAgents.length > 0) {
        onAgentsUpdated(updatedAgents);
      }
    } catch (err) {
      console.error('Failed to update agent tools:', err);
    }
  };

  const handleRemoveFile = async (fileId: string) => {
    // Remove file from local state
    setFiles((prev) => {
      const newFiles = prev.filter((f) => f.id !== fileId);

      // If no files left, remove DatabricksKnowledgeSearchTool from selected agents
      if (newFiles.length === 0) {
        console.log('[Knowledge] No files left, removing DatabricksKnowledgeSearchTool from agents');

        // Remove tool from all selected agents
        selectedAgents.forEach(async (agentId) => {
          const agent = availableAgents.find(a => (a.id || `agent-${a.name}`) === agentId);
          if (agent) {
            // Remove both ID 36 and 'DatabricksKnowledgeSearchTool' from tools
            const updatedTools = (agent.tools || []).filter(
              toolId => String(toolId) !== '36' && String(toolId) !== 'DatabricksKnowledgeSearchTool'
            );

            // Update agent if it has an ID
            if (agent.id) {
              const updatedAgent = { ...agent, tools: updatedTools };
              try {
                const savedAgent = await AgentService.updateAgentFull(agent.id, updatedAgent as Agent);
                console.log(`[Knowledge] Removed DatabricksKnowledgeSearchTool from agent ${agent.name}`);

                // Update the Zustand store
                if (savedAgent && savedAgent.id) {
                  updateAgent(savedAgent.id, savedAgent);
                }
              } catch (err) {
                console.error(`Failed to remove tool from agent ${agent.name}:`, err);
              }
            }
          }
        });
      }

      return newFiles;
    });
  };

  const handleOpenDialog = () => {
    setShowDialog(true);
  };

  const handleCloseDialog = () => {
    setShowDialog(false);
  };

  const toggleAgentSelection = async (agentId: string) => {
    const agent = availableAgents.find(a => (a.id || `agent-${a.name}`) === agentId);

    console.log('🎯 [AGENT DEBUG] Agent selection toggled:', {
      agentId,
      agentName: agent?.name,
      currentSelectedAgents: selectedAgents
    });

    setSelectedAgents(prev => {
      const isCurrentlySelected = prev.includes(agentId);

      if (isCurrentlySelected) {
        console.log('➖ [AGENT DEBUG] Deselecting agent:', agentId);
        // Agent is being deselected - remove knowledge sources from this execution
        if (agent) {
          removeKnowledgeSourcesFromAgent(agent);
        }
        const newSelection = prev.filter(id => id !== agentId);
        console.log('🔄 [AGENT DEBUG] New agent selection after deselect:', newSelection);
        return newSelection;
      } else {
        console.log('➕ [AGENT DEBUG] Selecting agent:', agentId);
        const newSelection = [...prev, agentId];
        console.log('🔄 [AGENT DEBUG] New agent selection after select:', newSelection);
        return newSelection;
      }
    });
  };

  const removeKnowledgeSourcesFromAgent = async (agent: any) => {
    console.log('[DEBUG] Removing DatabricksKnowledgeSearchTool from deselected agent:', agent.name);

    // Remove the tool from the agent's tools array
    const toolName = 'DatabricksKnowledgeSearchTool';
    const currentTools = agent.tools || [];

    if (currentTools.includes(toolName)) {
      const updatedTools = currentTools.filter((t: string) => t !== toolName);
      const updatedAgent = {
        ...agent,
        tools: updatedTools
      };

      try {
        if (agent.id) {
          const { AgentService } = await import('../../api/AgentService');
          const { id, created_at, ...agentData } = updatedAgent as any;
          const savedAgent = await AgentService.updateAgentFull(agent.id, agentData as any);

          if (savedAgent) {
            // Update Zustand store
            if (savedAgent.id) {
              updateAgent(savedAgent.id, savedAgent);
            }

            if (onAgentsUpdated) {
              onAgentsUpdated([savedAgent]);
            }
            console.log(`[DEBUG] Successfully removed ${toolName} from agent ${agent.name}`);
          }
        } else {
          // For in-memory agents, update the local availableAgents array
          const agentIndex = availableAgents.findIndex(a => a.name === agent.name);
          if (agentIndex !== -1) {
            availableAgents[agentIndex] = updatedAgent;
            if (onAgentsUpdated) {
              onAgentsUpdated([updatedAgent]);
            }
          }
        }
      } catch (err) {
        console.error('Failed to update agent:', err);
      }
    } else {
      console.log(`[DEBUG] Agent ${agent.name} does not have ${toolName} in tools`);
    }
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

      console.log('[DEBUG] About to browse volume:', {
        requestedPath: path,
        databricksConfig: databricksConfig,
        configKnowledgeVolumePath: databricksConfig?.knowledge_volume_path,
        finalVolumePath: volumePath,
        encodedPath: encodeURIComponent(volumePath),
        fullUrl: `/databricks/knowledge/browse/${encodeURIComponent(volumePath)}`
      });

      const response = await apiClient.get<VolumeFile[]>(
        `/databricks/knowledge/browse/${encodeURIComponent(volumePath)}`
      );

      console.log('[DEBUG] Volume files received:', {
        path: volumePath,
        responseData: response.data,
        responseLength: response.data?.length || 0
      });

      // More lenient filtering - only exclude obviously invalid entries
      const validFiles = response.data.filter(file => {
        // Must have a name that's not empty, '.', or '..'
        const hasValidName = file.name &&
                           file.name.trim() !== '' &&
                           file.name !== '.' &&
                           file.name !== '..';

        // Path can be empty for root level items, just needs to exist
        const hasPath = file.path !== undefined && file.path !== null;

        const isValid = hasValidName && hasPath;

        if (!isValid) {
          console.log('[DEBUG] Filtering out invalid file:', {
            name: file.name,
            path: file.path,
            hasValidName,
            hasPath,
            file
          });
        }

        return isValid;
      });

      console.log('[DEBUG] Valid files after filtering:', {
        validFiles,
        validCount: validFiles.length,
        originalCount: response.data?.length || 0
      });

      setVolumeFiles(validFiles);
      setCurrentPath(volumePath);
    } catch (err) {
      console.error('Failed to browse volume:', {
        error: err,
        volumePath: path || databricksConfig?.knowledge_volume_path || 'main.default.knowledge',
        errorMessage: err instanceof Error ? err.message : String(err),
        errorResponse: (err as any)?.response?.data,
        errorStatus: (err as any)?.response?.status
      });
      setError(`Failed to browse Databricks Volume: ${err instanceof Error ? err.message : String(err)}`);
      setVolumeFiles([]);
    } finally {
      setIsLoadingVolume(false);
    }
  };

  const navigateToFolder = (folderPath: string) => {
    browseVolumeFiles(folderPath);
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
        formData.append('selected_agents', JSON.stringify(selectedAgents)); // Send agent IDs for volume selection

        console.log('📁 [VOLUME DEBUG] Selecting volume file:', {
          filePath,
          selectedAgents,
          selectedAgentsJson: JSON.stringify(selectedAgents),
          executionId
        });
        
        const response = await apiClient.post(
          `/databricks/knowledge/select-from-volume/${executionId}`,
          formData
        );

        console.log('✅ [VOLUME DEBUG] Volume selection response:', {
          responseData: response.data,
          sentSelectedAgents: JSON.stringify(selectedAgents),
          filePath
        });

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
    if (tabValue === 1 && showDialog && databricksConfig?.knowledge_volume_enabled) {
      // Only load if we don't have a current path or if we're at the root and have no files
      if (!currentPath || (currentPath === (databricksConfig.knowledge_volume_path || 'main.default.knowledge') && volumeFiles.length === 0)) {
        browseVolumeFiles();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tabValue, showDialog, databricksConfig]);

  // Use global knowledge configuration store
  const { isMemoryBackendConfigured, isKnowledgeSourceEnabled, refreshConfiguration } = useKnowledgeConfigStore();
  const isKnowledgeEnabled = isMemoryBackendConfigured && isKnowledgeSourceEnabled;

  // Debug current state
  React.useEffect(() => {
    console.log('[KnowledgeFileUpload] Configuration state:', {
      isMemoryBackendConfigured,
      isKnowledgeSourceEnabled,
      isKnowledgeEnabled
    });
  }, [isMemoryBackendConfigured, isKnowledgeSourceEnabled, isKnowledgeEnabled]);

  // Force refresh configuration when component mounts
  React.useEffect(() => {
    refreshConfiguration();
  }, [refreshConfiguration]);

  return (
    <>
      {/* Upload Button */}
      <Tooltip
        title={
          !isKnowledgeEnabled
            ? !isMemoryBackendConfigured
              ? 'Knowledge sources require Databricks Vector Search memory backend configuration'
              : 'Knowledge volume is not enabled in Databricks configuration'
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
              {!isMemoryBackendConfigured
                ? 'Knowledge sources require Databricks Vector Search memory backend configuration. Please configure memory backend in Settings.'
                : 'Knowledge volume is not enabled. Please configure it in the Databricks settings.'}
            </Alert>
          )}

          {isKnowledgeEnabled && (
            <>
              {/* Agent Selection Section */}
              <Paper
                elevation={0}
                sx={{
                  p: 3,
                  mb: 3,
                  bgcolor: 'grey.50',
                  borderRadius: 2,
                  border: '1px solid',
                  borderColor: 'grey.200'
                }}
              >
                <Typography
                  variant="h6"
                  sx={{
                    mb: 1,
                    fontWeight: 600,
                    color: 'text.primary'
                  }}
                >
                  Select Agents
                </Typography>
                <Typography
                  variant="body2"
                  sx={{
                    mb: 3,
                    color: 'text.secondary',
                    lineHeight: 1.5
                  }}
                >
                  Choose which agents will have access to these knowledge files
                </Typography>

                {isLoadingAgents ? (
                  <Box sx={{ display: 'flex', justifyContent: 'center', py: 3 }}>
                    <CircularProgress size={28} />
                  </Box>
                ) : availableAgents.length > 0 ? (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1.5 }}>
                    {availableAgents.map((agent) => {
                      const agentId = agent.id || `agent-${agent.name}`;
                      const isSelected = selectedAgents.includes(agentId);
                      const hasKnowledgeTool = agent.tools?.includes('DatabricksKnowledgeSearchTool');

                      return (
                        <Chip
                          key={agentId}
                          label={
                            <span>
                              {agent.name}
                              {hasKnowledgeTool && (
                                <span style={{ marginLeft: '4px', fontSize: '0.9em' }}>
                                  📚
                                </span>
                              )}
                            </span>
                          }
                          onClick={() => toggleAgentSelection(agentId)}
                          color={isSelected ? 'primary' : 'default'}
                          variant={isSelected ? 'filled' : 'outlined'}
                          clickable
                          title={hasKnowledgeTool ?
                            `${agent.name} - Has Knowledge Search capability` :
                            `${agent.name} - Click to add Knowledge Search capability`}
                          sx={{
                            py: 1,
                            px: 0.5,
                            fontSize: '0.875rem',
                            fontWeight: 500,
                            transition: 'all 0.2s ease',
                            '&:hover': {
                              transform: 'translateY(-1px)',
                              boxShadow: 1
                            },
                            ...(isSelected && {
                              boxShadow: 2
                            })
                          }}
                        />
                      );
                    })}
                  </Box>
                ) : (
                  <Alert severity="info" sx={{ mt: 1 }}>
                    No agents available. Add agents to the canvas first.
                  </Alert>
                )}
              </Paper>

              {/* File Upload Section - Appears with smooth transition */}
              {selectedAgents.length > 0 && (
                <Fade in={selectedAgents.length > 0} timeout={300}>
                  <Paper
                    elevation={0}
                    sx={{
                      p: 3,
                      borderRadius: 2,
                      border: '1px solid',
                      borderColor: 'grey.200'
                    }}
                  >
                    <Typography
                      variant="h6"
                      sx={{
                        mb: 2,
                        fontWeight: 600,
                        color: 'text.primary'
                      }}
                    >
                      Upload Knowledge Files
                    </Typography>

                    <Tabs
                      value={tabValue}
                      onChange={(_, v) => setTabValue(v)}
                      sx={{
                        mb: 3,
                        '& .MuiTab-root': {
                          fontWeight: 500,
                          textTransform: 'none',
                          fontSize: '0.9rem'
                        }
                      }}
                    >
                      <Tab label="Upload Files" icon={<UploadIcon />} iconPosition="start" />
                      <Tab label="Browse Volume" icon={<FolderIcon />} iconPosition="start" />
                    </Tabs>

              {/* Tab Panel: Upload */}
              {tabValue === 0 && (
                <>
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
                    sx={{
                      mb: 3,
                      py: 2,
                      borderRadius: 2,
                      textTransform: 'none',
                      fontSize: '1rem',
                      fontWeight: 500,
                      boxShadow: 'none',
                      '&:hover': {
                        boxShadow: 2,
                        transform: 'translateY(-1px)'
                      },
                      transition: 'all 0.2s ease'
                    }}
                  >
                    Choose Files to Upload
                  </Button>

              {/* File List */}
              {files.length > 0 && (
                <Paper
                  variant="outlined"
                  sx={{
                    borderRadius: 2,
                    overflow: 'hidden',
                    border: '1px solid',
                    borderColor: 'grey.200'
                  }}
                >
                  <List disablePadding>
                    {files.map((file) => (
                      <ListItem
                        key={file.id}
                        sx={{
                          borderBottom: '1px solid',
                          borderColor: 'grey.100',
                          '&:last-child': {
                            borderBottom: 'none'
                          }
                        }}
                      >
                        <ListItemText
                          primary={
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                              <FileIcon fontSize="small" color="primary" />
                              <Typography variant="body2" fontWeight={500}>
                                {file.filename}
                              </Typography>
                              <Chip
                                label={formatFileSize(file.size)}
                                size="small"
                                variant="outlined"
                                sx={{ fontSize: '0.75rem' }}
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

                </>
              )}

              {/* Tab Panel: Browse Volume */}
              {tabValue === 1 && (
                <>
                  {/* Volume Browser Header */}
                  <Box sx={{ mb: 2, p: 2, bgcolor: 'grey.50', borderRadius: 1, border: '1px solid', borderColor: 'grey.200' }}>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                      Browsing: {currentPath || 'Default volume'}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: 'block' }}>
                      Config volume path: {databricksConfig?.knowledge_volume_path || 'Not set'}
                    </Typography>
                    <Button
                      variant="text"
                      size="small"
                      onClick={() => {
                        console.log('[DEBUG] Manual refresh clicked');
                        setVolumeFiles([]); // Clear current files to force reload
                        browseVolumeFiles();
                      }}
                      disabled={isLoadingVolume}
                      sx={{ textTransform: 'none', fontSize: '0.8rem' }}
                    >
                      {isLoadingVolume ? 'Loading...' : 'Refresh'}
                    </Button>
                  </Box>

                  {/* Simple File Browser */}
                  {isLoadingVolume ? (
                    <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                      <CircularProgress />
                    </Box>
                  ) : volumeFiles.length > 0 ? (
                    <List>
                      {volumeFiles
                        .filter((item) => item.name && item.name.trim() !== '')
                        .map((item) => (
                          <ListItem
                            key={item.path}
                            disablePadding
                            sx={{ mb: 1 }}
                          >
                            <ListItemButton
                              onClick={() => {
                                if (item.is_directory) {
                                  navigateToFolder(item.path);
                                } else {
                                  toggleFileSelection(item.path);
                                }
                              }}
                              selected={!item.is_directory && selectedVolumeFiles.has(item.path)}
                              sx={{
                                borderRadius: 1,
                                '&:hover': {
                                  bgcolor: 'action.hover'
                                }
                              }}
                            >
                              <ListItemIcon>
                                {item.is_directory ? (
                                  <FolderIcon color="primary" />
                                ) : (
                                  <FileIcon />
                                )}
                              </ListItemIcon>
                              <ListItemText
                                primary={item.name}
                                secondary={
                                  !item.is_directory
                                    ? `${formatFileSize(item.size || 0)}`
                                    : undefined
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
                          </ListItem>
                        ))}
                    </List>
                  ) : (
                    <Box sx={{ textAlign: 'center', py: 4 }}>
                      <Alert severity="info" sx={{ mb: 2 }}>
                        No files or folders found in this location.
                        <br />
                        <Typography variant="caption" sx={{ mt: 1, display: 'block' }}>
                          Current path: {currentPath || 'Not set'}
                        </Typography>
                      </Alert>
                      <Button
                        variant="outlined"
                        onClick={() => browseVolumeFiles()}
                        disabled={isLoadingVolume}
                        sx={{
                          textTransform: 'none',
                          borderRadius: 2
                        }}
                      >
                        {isLoadingVolume ? 'Loading...' : 'Refresh'}
                      </Button>
                    </Box>
                  )}

                  {/* Select Files Button */}
                  {selectedVolumeFiles.size > 0 && (
                    <Button
                      variant="contained"
                      onClick={selectVolumeFiles}
                      disabled={isUploading}
                      fullWidth
                      sx={{
                        py: 1.5,
                        borderRadius: 2,
                        textTransform: 'none',
                        fontWeight: 500,
                        transition: 'all 0.2s ease',
                        '&:hover': {
                          transform: 'translateY(-1px)',
                          boxShadow: 2
                        }
                      }}
                    >
                      Use Selected Files ({selectedVolumeFiles.size})
                    </Button>
                  )}
                </>
              )}
                  </Paper>
                </Fade>
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