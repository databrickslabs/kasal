/**
 * Dialog component for viewing documents from a Databricks Vector Search index
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  CircularProgress,
  Alert,
  Paper,
  Divider,
  IconButton,
  Tooltip,
  Chip,
  TextField,
  InputAdornment,
} from '@mui/material';
import {
  Close as CloseIcon,
  Refresh as RefreshIcon,
  Search as SearchIcon,
  ContentCopy as CopyIcon,
} from '@mui/icons-material';
import { apiClient } from '../../config/api/ApiConfig';

interface IndexDocumentsDialogProps {
  open: boolean;
  onClose: () => void;
  indexName: string;
  indexType: 'short_term' | 'long_term' | 'entity' | 'document';
  workspaceUrl: string;
  endpointName: string;
  backendId?: string;
}

interface Document {
  id: string;
  text: string;
  metadata?: Record<string, unknown>;
}

export const IndexDocumentsDialog: React.FC<IndexDocumentsDialogProps> = ({
  open,
  onClose,
  indexName,
  indexType,
  workspaceUrl,
  endpointName,
  backendId,
}) => {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [filteredDocuments, setFilteredDocuments] = useState<Document[]>([]);

  const fetchDocuments = useCallback(async () => {
    setLoading(true);
    setError('');
    
    try {
      const params: Record<string, string | number> = {
        index_name: indexName,
        workspace_url: workspaceUrl,
        endpoint_name: endpointName,
        index_type: indexType,
        limit: 30
      };
      
      // Add backend_id if available
      if (backendId) {
        params.backend_id = backendId;
      }
      
      const response = await apiClient.get('/memory-backend/databricks/index-documents', { params });
      
      if (response.data.success) {
        setDocuments(response.data.documents || []);
      } else {
        setError(response.data.message || 'Failed to fetch documents');
      }
    } catch (err) {
      console.error('Error fetching index documents:', err);
      setError('Failed to fetch documents. Please try again.');
    } finally {
      setLoading(false);
    }
  }, [indexName, workspaceUrl, endpointName, indexType, backendId]);

  useEffect(() => {
    if (open && indexName && workspaceUrl && endpointName) {
      fetchDocuments();
    }
  }, [open, indexName, workspaceUrl, endpointName, fetchDocuments]);

  useEffect(() => {
    // Filter documents based on search query
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      const filtered = documents.filter(doc => 
        doc.text?.toLowerCase().includes(query) ||
        doc.id?.toLowerCase().includes(query) ||
        JSON.stringify(doc.metadata)?.toLowerCase().includes(query)
      );
      setFilteredDocuments(filtered);
    } else {
      setFilteredDocuments(documents);
    }
  }, [searchQuery, documents]);

  const handleCopyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const getIndexTypeLabel = () => {
    const labels = {
      short_term: 'Short-term Memory',
      long_term: 'Long-term Memory',
      entity: 'Entity Memory',
      document: 'Document Embeddings'
    };
    return labels[indexType] || indexType;
  };

  const getIndexTypeColor = () => {
    const colors = {
      short_term: 'primary',
      long_term: 'secondary',
      entity: 'success',
      document: 'info'
    } as const;
    return colors[indexType] || 'default';
  };

  const formatMetadata = (metadata: unknown) => {
    if (!metadata) return null;
    
    try {
      const formatted = typeof metadata === 'string' 
        ? JSON.stringify(JSON.parse(metadata), null, 2)
        : JSON.stringify(metadata, null, 2);
      return formatted;
    } catch {
      return String(metadata);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: {
          height: '80vh',
          display: 'flex',
          flexDirection: 'column'
        }
      }}
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="h6">Index Documents</Typography>
            <Chip 
              label={getIndexTypeLabel()} 
              color={getIndexTypeColor()}
              size="small"
            />
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Tooltip title="Refresh">
              <IconButton onClick={fetchDocuments} disabled={loading}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
            <IconButton onClick={onClose}>
              <CloseIcon />
            </IconButton>
          </Box>
        </Box>
        <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
          {indexName} ({filteredDocuments.length} of {documents.length} documents)
        </Typography>
      </DialogTitle>
      
      <DialogContent dividers sx={{ flex: 1, overflow: 'auto' }}>
        <Box sx={{ mb: 2 }}>
          <TextField
            fullWidth
            size="small"
            placeholder="Search documents..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
              ),
            }}
          />
        </Box>

        {loading && (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
            <CircularProgress />
          </Box>
        )}
        
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        
        {!loading && !error && filteredDocuments.length === 0 && (
          <Alert severity="info">
            {searchQuery ? 'No documents match your search.' : 'No documents found in this index.'}
          </Alert>
        )}
        
        {!loading && !error && filteredDocuments.length > 0 && (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {filteredDocuments.map((doc, index) => (
              <Paper 
                key={doc.id || index} 
                variant="outlined" 
                sx={{ p: 2 }}
              >
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1 }}>
                  <Typography variant="subtitle2" color="primary">
                    Document #{index + 1}
                  </Typography>
                  <Tooltip title="Copy document text">
                    <IconButton 
                      size="small" 
                      onClick={() => handleCopyToClipboard(doc.text)}
                    >
                      <CopyIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>
                
                {doc.id && (
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                    ID: {doc.id}
                  </Typography>
                )}
                
                
                <Divider sx={{ my: 1 }} />
                
                <Typography 
                  variant="body2" 
                  sx={{ 
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                    mb: doc.metadata ? 1 : 0
                  }}
                >
                  {doc.text}
                </Typography>
                
                {doc.metadata && Object.keys(doc.metadata).length > 0 && (
                  <>
                    <Divider sx={{ my: 1 }} />
                    <Box>
                      <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'bold' }}>
                        Metadata:
                      </Typography>
                      <Box 
                        component="pre" 
                        sx={{ 
                          backgroundColor: 'grey.50',
                          p: 1,
                          borderRadius: 1,
                          overflow: 'auto',
                          fontSize: '0.75rem',
                          mt: 0.5,
                          fontFamily: 'monospace'
                        }}
                      >
                        {formatMetadata(doc.metadata)}
                      </Box>
                    </Box>
                  </>
                )}
              </Paper>
            ))}
          </Box>
        )}
      </DialogContent>
      
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
};