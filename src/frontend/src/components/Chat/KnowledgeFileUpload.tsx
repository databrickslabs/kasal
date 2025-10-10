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
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  AttachFile as AttachFileIcon,
  Delete as DeleteIcon,
  InsertDriveFile as FileIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
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



interface KnowledgeFileUploadProps {
  executionId: string;
  groupId: string;
  onFilesUploaded?: (files: UploadedFile[]) => void;
  onAgentsUpdated?: (updatedAgents: Agent[]) => void; // Callback to update canvas nodes
  onTasksUpdated?: (uploadedFilePath: string) => void; // Callback to update task nodes with file path
  disabled?: boolean;
  compact?: boolean;
  availableAgents?: Agent[]; // Agents currently on the canvas
  hasAgents?: boolean; // Whether there are agents on canvas
  hasTasks?: boolean; // Whether there are tasks on canvas
}

export const KnowledgeFileUpload: React.FC<KnowledgeFileUploadProps> = ({
  executionId,
  groupId: _groupId,
  onFilesUploaded,
  onAgentsUpdated,
  onTasksUpdated,
  disabled = false,
  compact = false,
  availableAgents: providedAgents,
  hasAgents = false,
  hasTasks = false,
}) => {
  const [files, setFiles] = useState<UploadedFile[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [showDialog, setShowDialog] = useState(false);
  const [databricksConfig, setDatabricksConfig] = useState<DatabricksConfig | null>(null);
  const [error, setError] = useState<string | null>(null);

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
    allow_code_execution: boolean;
    code_execution_mode: 'safe' | 'unsafe';
    // knowledge_sources removed - using DatabricksKnowledgeSearchTool instead
  }
  const [availableAgents, setAvailableAgents] = useState<AgentOption[]>(providedAgents || []);
  const [selectedAgents, setSelectedAgents] = useState<string[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Update available agents when providedAgents changes and auto-select first agent
  React.useEffect(() => {
    if (providedAgents && providedAgents.length > 0) {
      setAvailableAgents(providedAgents);
      if (selectedAgents.length === 0) {
        const first = providedAgents[0];
        const firstId = first.id || `agent-${first.name}`;
        setSelectedAgents([firstId]);
      }
    } else if (showDialog && availableAgents.length === 0) {
      // If no agents provided and dialog is open, load all agents as fallback
      const loadAgents = async () => {
        try {
          // Dynamic import to avoid circular dependencies
          const { AgentService } = await import('../../api/AgentService');
          const agents = await AgentService.listAgents();
          setAvailableAgents(agents);
          if (selectedAgents.length === 0 && agents.length > 0) {
            const first = agents[0];
            const firstId = first.id || `agent-${first.name}`;
            setSelectedAgents([firstId]);
          }
        } catch (err) {
          console.error('Failed to load agents:', err);
        }
      };
      loadAgents();
    }
  }, [showDialog, providedAgents, availableAgents.length, selectedAgents.length]);

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

        // NEW BEHAVIOR: Update task nodes with the uploaded file path
        // This allows tasks to use DatabricksKnowledgeSearchTool with the file
        if (response.data && response.data.path && onTasksUpdated) {
          console.log('[DEBUG] Notifying parent to update task nodes with file path:', response.data.path);
          onTasksUpdated(response.data.path);
        }

        // LEGACY: If agents are selected, update their knowledge sources and tools
        if (selectedAgents.length > 0 && response.data) {
          await updateAgentKnowledgeSources(response.data);
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

  // Accept full upload response so we can persist metadata alongside the source path
  const updateAgentKnowledgeSources = async (uploadResp: {
    path: string;
    filename: string;
    size: number;
    execution_id?: string;
    uploaded_at?: string;
    volume_info?: { full_path?: string; [key: string]: unknown };
    upload_method?: string;
    simulated?: boolean;
  }) => {
    try {
      const { path: filePath, filename: fileName, size, execution_id, uploaded_at, volume_info, upload_method, simulated } = uploadResp || {};
      // Dynamic import to avoid circular dependencies
      const { AgentService } = await import('../../api/AgentService');

      console.log('[DEBUG] updateAgentKnowledgeSources called:', {
        selectedAgents,
        availableAgents: availableAgents.map(a => ({ id: a.id, name: a.name })),
        filePath,
        fileName,
        size,
        execution_id,
        uploaded_at
      });

      // Build knowledge source entry to append
      const newSource = {
        type: 'databricks_volume',
        source: filePath,
        metadata: {
          filename: fileName,
          uploaded_at: uploaded_at || new Date().toISOString(),
          execution_id,
          upload_method,
          simulated: Boolean(simulated),
        },
        fileInfo: {
          filename: fileName,
          path: filePath,
          full_path: (volume_info && volume_info.full_path) || filePath,
          file_size_bytes: size,
          is_uploaded: true,
          exists: true,
          success: true,
        }
      } as unknown as import('../../types/agent').KnowledgeSource;

      // Collect updated agents to pass back to parent
      const updatedAgents: Agent[] = [];

      // Update each selected agent with the new knowledge source
      for (const agentId of selectedAgents) {
        console.log(`[DEBUG] Processing agent ID: ${agentId}`);
        const agent = availableAgents.find(a => (a.id || `agent-${a.name}`) === agentId);
        if (agent) {
          console.log(`[DEBUG] Found agent:`, { id: agent.id, name: agent.name, existingTools: agent.tools });

          // NOTE: We no longer add DatabricksKnowledgeSearchTool to agent's tools
          // The tool should be added to the task's tools list instead

          // Start updatedAgent; we'll merge knowledge_sources from backend
          let updatedAgent: Agent = { ...(agent as unknown as Agent) };

          // Only persist to backend if we have a real agent ID
          if (agent.id) {
            // Fetch latest agent to merge existing knowledge_sources safely
            const current = await AgentService.getAgent(agent.id);
            const existingSources = current?.knowledge_sources || [];
            const alreadyHas = existingSources.some(s => s.source === filePath);
            const mergedSources = alreadyHas ? existingSources : [...existingSources, newSource];

            const payload: Omit<Agent, 'id' | 'created_at'> = {
              ...current,
              // Explicitly set fields we may not have on the lightweight AgentOption
              name: current?.name || agent.name,
              role: current?.role || agent.role,
              goal: current?.goal || agent.goal,
              backstory: current?.backstory || agent.backstory,
              llm: current?.llm || agent.llm,
              tools: current?.tools || agent.tools || [],
              max_iter: current?.max_iter ?? agent.max_iter ?? 25,
              verbose: current?.verbose ?? agent.verbose ?? false,
              allow_delegation: current?.allow_delegation ?? agent.allow_delegation ?? false,
              cache: current?.cache ?? agent.cache ?? true,
              allow_code_execution: current?.allow_code_execution ?? agent.allow_code_execution ?? false,
              code_execution_mode: (current?.code_execution_mode || agent.code_execution_mode || 'safe') as 'safe' | 'unsafe',
              knowledge_sources: mergedSources,
              // Pass through other optional fields if present
              function_calling_llm: current?.function_calling_llm,
              max_rpm: current?.max_rpm,
              max_execution_time: current?.max_execution_time,
              memory: current?.memory,
              embedder_config: current?.embedder_config,
              tool_configs: current?.tool_configs,
            };

            const savedAgent = await AgentService.updateAgentFull(agent.id, payload);
            if (savedAgent) {
              updatedAgent = savedAgent;
              console.log(`[DEBUG] Persisted knowledge source to agent ${agent.name}`);
              if (savedAgent.id) {
                updateAgent(savedAgent.id, savedAgent);
              }
            }
          } else {
            // Unsaved agent: keep in-memory representation so canvas can reflect
            const inMemSources = ((updatedAgent as Agent).knowledge_sources || []);
            const alreadyHas = inMemSources.some(s => s.source === filePath);
            (updatedAgent as Agent).knowledge_sources = alreadyHas ? inMemSources : [...inMemSources, newSource];
          }

          updatedAgents.push(updatedAgent);
        }
      }

      // Notify parent so canvas nodes update immediately
      if (onAgentsUpdated && updatedAgents.length > 0) {
        onAgentsUpdated(updatedAgents);
      }
    } catch (err) {
      console.error('Failed to update agent tools/knowledge sources:', err);
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
    // Auto-select first agent when opening
    if (selectedAgents.length === 0 && availableAgents.length > 0) {
      const first = availableAgents[0];
      const firstId = first.id || `agent-${first.name}`;
      setSelectedAgents([firstId]);
    }
  };

  const handleCloseDialog = () => {
    setShowDialog(false);
  };





  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };





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
            : !hasAgents
            ? 'Add at least one agent to the canvas before uploading knowledge files'
            : !hasTasks
            ? 'Add at least one task to the canvas before uploading knowledge files'
            : 'Upload knowledge files for tasks to search and reference'
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
        <DialogTitle sx={{ pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <UploadIcon color="primary" />
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  Upload Knowledge Files
                </Typography>
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
                  Add files for tasks to search and reference
                </Typography>
              </Box>
            </Box>
          </Box>
        </DialogTitle>

        <DialogContent sx={{ pt: 2 }}>
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
              {/* File Upload Section */}
              {availableAgents.length > 0 ? (
                <Box>
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
                    size="large"
                    sx={{
                      mb: 2,
                      py: 1.5,
                      borderRadius: 2,
                      textTransform: 'none',
                      fontWeight: 500
                    }}
                  >
                    Choose Files
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
            </Box>
              ) : (
                <Alert severity="info">
                  No agents available on the canvas. Please add at least one agent before uploading knowledge files.
                </Alert>
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