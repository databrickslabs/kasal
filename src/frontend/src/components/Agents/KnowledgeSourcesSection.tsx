import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  IconButton,
  Typography,
  Stack,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  CircularProgress,
  Alert,
  Snackbar,
  Chip,
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import FileIcon from '@mui/icons-material/InsertDriveFile';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';

import { uploadService, UploadedFileInfo } from '../../api/UploadService';
import { KnowledgeSourcesSectionProps } from '../../types/agent';

type KnowledgeSourceMetadata = {
  chunk_size?: number;
  chunk_overlap?: number;
};

type KnowledgeSourceField = {
  type: string;
  source: string;
  metadata: KnowledgeSourceMetadata;
  fileInfo?: UploadedFileInfo;
};

const sourceTypes = [
  { value: 'text', label: 'Text Content', description: 'Raw text content or string data' },
  { value: 'file', label: 'Text File', description: 'Plain text files (.txt)' },
  { value: 'pdf', label: 'PDF Document', description: 'PDF documents' },
  { value: 'csv', label: 'CSV File', description: 'Comma-separated values files' },
  { value: 'excel', label: 'Excel File', description: 'Excel spreadsheets (.xlsx)' },
  { value: 'json', label: 'JSON File', description: 'JSON documents' },
  { value: 'url', label: 'URL', description: 'Web URLs or online resources' },
  { value: 'docling', label: 'CrewDocling', description: 'Multiple file formats including TXT, PDF, DOCX, HTML' },
];

export const KnowledgeSourcesSection: React.FC<KnowledgeSourcesSectionProps> = ({
  knowledgeSources,
  onChange,
}) => {
  const [uploading, setUploading] = useState<{ [key: number]: boolean }>({});
  const [checking, setChecking] = useState<{ [key: number]: boolean }>({});
  const [notification, setNotification] = useState<{ open: boolean, message: string, severity: 'success' | 'error' }>({
    open: false,
    message: '',
    severity: 'success'
  });

  // Memoize the checkExistingFiles function to avoid recreating it on every render
  const checkExistingFiles = useCallback(async () => {
    // Only check file-based sources (not text or URLs)
    const fileBasedSources = knowledgeSources.filter(source => 
      source.type !== 'text' && source.type !== 'url' && source.source
    );
    
    if (fileBasedSources.length === 0) return;
    
    // Set checking state for each source that needs checking
    fileBasedSources.forEach((_, index) => {
      setChecking(prev => ({ ...prev, [index]: true }));
    });
    
    // Check each file
    const updatedSources = [...knowledgeSources];
    for (let i = 0; i < fileBasedSources.length; i++) {
      const source = fileBasedSources[i];
      const index = knowledgeSources.findIndex(s => s === source);
      
      try {
        // Always verify file existence to ensure fileInfo is current
        const fileInfo = await uploadService.checkKnowledgeFile(source.source);
        
        // Update source with file info
        updatedSources[index] = {
          ...updatedSources[index],
          fileInfo
        };
      } catch (error) {
        console.error(`Error checking file for source ${index}:`, error);
      } finally {
        // Clear checking state
        setChecking(prev => ({ ...prev, [index]: false }));
      }
    }
    
    // Always update sources to ensure fileInfo is current
    onChange(updatedSources);
  }, [knowledgeSources, onChange]);

  // Force a check when the component is first mounted (to handle reopening the form)
  useEffect(() => {
    const timer = setTimeout(() => {
      checkExistingFiles();
    }, 500);
    
    return () => clearTimeout(timer);
  }, [checkExistingFiles]);

  // Check for existing files when knowledge sources change
  useEffect(() => {
    const timer = setTimeout(() => {
      checkExistingFiles();
    }, 1000);
    
    return () => clearTimeout(timer);
  }, [checkExistingFiles]);

  const handleAddSource = () => {
    onChange([
      ...knowledgeSources,
      { 
        type: 'text',
        source: '',
        metadata: {
          chunk_size: 4000,
          chunk_overlap: 200
        }
      },
    ]);
  };

  const handleRemoveSource = (index: number) => {
    const newSources = knowledgeSources.filter((_, i) => i !== index);
    onChange(newSources);
  };

  const handleSourceChange = (
    index: number,
    field: keyof KnowledgeSourceField,
    value: string | number | KnowledgeSourceMetadata
  ) => {
    const newSources = [...knowledgeSources];
    
    if (field === 'metadata') {
      newSources[index] = {
        ...newSources[index],
        metadata: {
          ...newSources[index].metadata,
          ...(value as KnowledgeSourceMetadata)
        }
      };
    } else if (field === 'source' && newSources[index].type !== 'text' && newSources[index].type !== 'url') {
      // If changing a file source, reset the fileInfo
      newSources[index] = {
        ...newSources[index],
        [field]: value as string,
        fileInfo: undefined
      };
      
      // Trigger a check for the new source if it's not empty
      if (value) {
        checkFileSource(index, value as string);
      }
    } else {
      // For other fields, just update the value
      newSources[index] = {
        ...newSources[index],
        [field]: value,
      };
      
      // If type changes from text/url to a file type, or between file types, reset source and fileInfo
      if (field === 'type') {
        const oldType = knowledgeSources[index].type;
        const newType = value as string;
        
        const wasFileType = oldType !== 'text' && oldType !== 'url';
        const isFileType = newType !== 'text' && newType !== 'url';
        
        if ((wasFileType !== isFileType) || (wasFileType && isFileType && oldType !== newType)) {
          newSources[index].source = '';
          newSources[index].fileInfo = undefined;
        }
      }
    }
    
    onChange(newSources);
  };

  const checkFileSource = async (index: number, filename: string) => {
    // Skip empty filenames
    if (!filename) return;
    
    // Set checking state for this source
    setChecking(prev => ({ ...prev, [index]: true }));
    
    try {
      // Check if file exists on server
      const fileInfo = await uploadService.checkKnowledgeFile(filename);
      
      // Update source with file info
      const newSources = [...knowledgeSources];
      newSources[index] = {
        ...newSources[index],
        fileInfo
      };
      
      onChange(newSources);
      
      // Show notification if file doesn't exist
      if (!fileInfo.exists) {
        setNotification({
          open: true,
          message: `File "${filename}" doesn't exist in the uploads directory`,
          severity: 'error'
        });
      }
    } catch (error) {
      console.error(`Error checking file for source ${index}:`, error);
    } finally {
      // Clear checking state
      setChecking(prev => ({ ...prev, [index]: false }));
    }
  };

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>, index: number) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Update uploading state for this index
    setUploading(prev => ({ ...prev, [index]: true }));

    try {
      // Upload file using the UploadService
      const response = await uploadService.uploadKnowledgeFile(file);

      // Update the knowledge source with the file path returned from the server
      if (response.success) {
        const newSources = [...knowledgeSources];
        newSources[index] = {
          ...newSources[index],
          source: response.path,
          fileInfo: response
        };
        onChange(newSources);

        // Show success notification
        setNotification({
          open: true,
          message: `Successfully uploaded ${file.name}`,
          severity: 'success'
        });
        
        // Add a small delay before checking if needed
        setTimeout(() => {
          // Only check if the fileInfo isn't already marked as existing
          if (!response.exists) {
            checkFileSource(index, response.path);
          }
        }, 2000);
      }
    } catch (error) {
      console.error('Error uploading file:', error);
      setNotification({
        open: true,
        message: `Failed to upload ${file.name}`,
        severity: 'error'
      });
    } finally {
      // Reset uploading state
      setUploading(prev => ({ ...prev, [index]: false }));
    }
  };

  const handleCloseNotification = () => {
    setNotification(prev => ({ ...prev, open: false }));
  };

  const getFileAcceptTypes = (sourceType: string): string => {
    switch (sourceType) {
      case 'pdf': return '.pdf';
      case 'csv': return '.csv';
      case 'excel': return '.xlsx,.xls';
      case 'json': return '.json';
      case 'file': return '.txt';
      case 'docling': return '.txt,.pdf,.docx,.html';
      default: return '*';
    }
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="subtitle1">Knowledge Sources</Typography>
        <Button
          startIcon={<AddIcon />}
          onClick={handleAddSource}
          variant="outlined"
          size="small"
        >
          Add Source
        </Button>
      </Box>
      
      <Snackbar 
        open={notification.open} 
        autoHideDuration={6000}
        onClose={handleCloseNotification}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert onClose={handleCloseNotification} severity={notification.severity}>
          {notification.message}
        </Alert>
      </Snackbar>
      
      <Stack spacing={2}>
        {knowledgeSources.map((source, index) => (
          <Box
            key={index}
            sx={{
              border: '1px solid',
              borderColor: 'divider',
              borderRadius: 1,
              overflow: 'hidden',
            }}
          >
            <Box
              sx={{
                display: 'flex',
                gap: 2,
                alignItems: 'flex-start',
                p: 2,
                backgroundColor: 'background.paper',
              }}
            >
              <FormControl sx={{ width: 200 }}>
                <InputLabel>Type</InputLabel>
                <Select
                  value={source.type}
                  onChange={(e) => handleSourceChange(index, 'type', e.target.value)}
                  label="Type"
                  size="small"
                >
                  {sourceTypes.map((type) => (
                    <MenuItem key={type.value} value={type.value}>
                      <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                        <Typography variant="body2">{type.label}</Typography>
                        <Typography variant="caption" color="text.secondary">
                          {type.description}
                        </Typography>
                      </Box>
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              {source.type === 'text' ? (
                <TextField
                  fullWidth
                  label="Source"
                  value={source.source}
                  onChange={(e) => handleSourceChange(index, 'source', e.target.value)}
                  size="small"
                  placeholder="Enter text content..."
                  multiline
                  rows={3}
                />
              ) : (
                <Box display="flex" flexDirection="column" flexGrow={1} gap={1}>
                  <Box display="flex" gap={2} alignItems="center">
                    <TextField
                      fullWidth
                      label="Source"
                      value={source.source}
                      onChange={(e) => handleSourceChange(index, 'source', e.target.value)}
                      size="small"
                      placeholder={
                        source.type === 'url' ? 'Enter URL...' :
                        source.type === 'docling' ? 'Enter file path or URL...' :
                        'Enter path or upload a file...'
                      }
                      InputProps={{
                        readOnly: source.type !== 'url' && source.fileInfo?.exists,
                        sx: {
                          bgcolor: source.fileInfo?.exists ? 'action.selected' : 'inherit',
                          '&:hover': {
                            bgcolor: source.fileInfo?.exists ? 'action.selected' : 'inherit',
                          }
                        },
                        startAdornment: source.fileInfo?.exists ? (
                          <FileIcon color="primary" sx={{ mr: 1 }} />
                        ) : undefined
                      }}
                    />
                    
                    {source.type !== 'url' && source.type !== 'text' && (
                      <>
                        {!source.fileInfo?.exists ? (
                          <Button
                            component="label"
                            variant="outlined"
                            startIcon={uploading[index] ? <CircularProgress size={20} /> : <UploadFileIcon />}
                            size="small"
                            disabled={uploading[index] || checking[index]}
                          >
                            Upload
                            <input
                              type="file"
                              hidden
                              onChange={(e) => handleFileUpload(e, index)}
                              accept={getFileAcceptTypes(source.type)}
                            />
                          </Button>
                        ) : (
                          <Button
                            variant="outlined"
                            color="success"
                            size="small"
                            startIcon={<CheckCircleIcon />}
                            sx={{ pointerEvents: 'none' }}
                          >
                            Uploaded
                          </Button>
                        )}
                        
                        {source.source && !source.fileInfo && !checking[index] && (
                          <Button
                            variant="outlined"
                            size="small"
                            onClick={() => checkFileSource(index, source.source)}
                          >
                            Check
                          </Button>
                        )}
                        
                        {checking[index] && (
                          <CircularProgress size={24} />
                        )}
                      </>
                    )}
                  </Box>
                  
                  {/* File status indicator */}
                  {source.type !== 'text' && source.type !== 'url' && source.source && (
                    <Box sx={{ mt: 1 }}>
                      {source.fileInfo ? (
                        source.fileInfo.exists ? (
                          <Chip
                            icon={<FileIcon />}
                            color="success"
                            size="small"
                            label={
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                <span>{source.source.split('/').pop()}</span>
                                <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                                  ({uploadService.formatFileSize(source.fileInfo.file_size_bytes)})
                                </Typography>
                                <CheckCircleIcon fontSize="small" color="success" />
                              </Box>
                            }
                          />
                        ) : (
                          <Chip
                            icon={<ErrorIcon />}
                            color="error"
                            size="small"
                            label={`File not found: ${source.source}`}
                          />
                        )
                      ) : checking[index] ? (
                        <Chip
                          icon={<CircularProgress size={16} />}
                          color="default"
                          size="small"
                          label="Checking file..."
                        />
                      ) : null}
                    </Box>
                  )}
                </Box>
              )}

              <IconButton
                onClick={() => handleRemoveSource(index)}
                color="error"
                size="small"
              >
                <DeleteIcon />
              </IconButton>
            </Box>

            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography variant="body2">Advanced Configuration</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Stack spacing={2}>
                  <TextField
                    fullWidth
                    type="number"
                    label="Chunk Size"
                    value={source.metadata?.chunk_size || 4000}
                    onChange={(e) => handleSourceChange(index, 'metadata', { chunk_size: parseInt(e.target.value) })}
                    size="small"
                    helperText="Maximum size of each content chunk (default: 4000)"
                  />
                  <TextField
                    fullWidth
                    type="number"
                    label="Chunk Overlap"
                    value={source.metadata?.chunk_overlap || 200}
                    onChange={(e) => handleSourceChange(index, 'metadata', { chunk_overlap: parseInt(e.target.value) })}
                    size="small"
                    helperText="Overlap between chunks to maintain context (default: 200)"
                  />
                </Stack>
              </AccordionDetails>
            </Accordion>
          </Box>
        ))}
      </Stack>
    </Box>
  );
}; 