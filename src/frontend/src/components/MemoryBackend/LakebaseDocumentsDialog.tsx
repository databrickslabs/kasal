/**
 * Dialog component for viewing documents from a Lakebase pgvector memory table.
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
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
import { MemoryBackendService, LakebaseDocument } from '../../api/MemoryBackendService';

interface LakebaseDocumentsDialogProps {
  open: boolean;
  onClose: () => void;
  tableName: string;
  memoryType: 'short_term' | 'long_term' | 'entity';
  instanceName?: string;
}

const TYPE_LABELS: Record<string, string> = {
  short_term: 'Short-term Memory',
  long_term: 'Long-term Memory',
  entity: 'Entity Memory',
};

const TYPE_COLORS: Record<string, 'primary' | 'secondary' | 'success'> = {
  short_term: 'primary',
  long_term: 'secondary',
  entity: 'success',
};

const LakebaseDocumentsDialog: React.FC<LakebaseDocumentsDialogProps> = ({
  open,
  onClose,
  tableName,
  memoryType,
  instanceName,
}) => {
  const [documents, setDocuments] = useState<LakebaseDocument[]>([]);
  const [filteredDocuments, setFilteredDocuments] = useState<LakebaseDocument[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [total, setTotal] = useState(0);

  const fetchDocuments = useCallback(async () => {
    setLoading(true);
    setError('');

    try {
      const result = await MemoryBackendService.getLakebaseTableData(tableName, 50, instanceName);
      if (result.success) {
        setDocuments(result.documents || []);
        setTotal(result.total || result.documents?.length || 0);
      } else {
        setError(result.message || 'Failed to fetch documents');
      }
    } catch {
      setError('Failed to fetch documents. Please try again.');
    } finally {
      setLoading(false);
    }
  }, [tableName, instanceName]);

  useEffect(() => {
    if (open && tableName) {
      fetchDocuments();
    }
  }, [open, tableName, fetchDocuments]);

  useEffect(() => {
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      setFilteredDocuments(
        documents.filter(
          (doc) =>
            doc.text?.toLowerCase().includes(query) ||
            doc.id?.toLowerCase().includes(query) ||
            doc.agent?.toLowerCase().includes(query) ||
            doc.crew_id?.toLowerCase().includes(query) ||
            JSON.stringify(doc.metadata)?.toLowerCase().includes(query)
        )
      );
    } else {
      setFilteredDocuments(documents);
    }
  }, [searchQuery, documents]);

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const formatMetadata = (metadata: unknown) => {
    if (!metadata) return null;
    try {
      const formatted =
        typeof metadata === 'string'
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
      PaperProps={{ sx: { height: '80vh', display: 'flex', flexDirection: 'column' } }}
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="h6">Table Data</Typography>
            <Chip
              label={TYPE_LABELS[memoryType] || memoryType}
              color={TYPE_COLORS[memoryType] || 'default'}
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
          {tableName} ({filteredDocuments.length} of {total} records)
        </Typography>
      </DialogTitle>

      <DialogContent dividers sx={{ flex: 1, overflow: 'auto' }}>
        <Box sx={{ mb: 2 }}>
          <TextField
            fullWidth
            size="small"
            placeholder="Search records..."
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
            {searchQuery ? 'No records match your search.' : 'No records found in this table.'}
          </Alert>
        )}

        {!loading && !error && filteredDocuments.length > 0 && (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {filteredDocuments.map((doc, index) => (
              <Paper key={doc.id || index} variant="outlined" sx={{ p: 2 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
                    <Typography variant="subtitle2" color="primary">
                      Record #{index + 1}
                    </Typography>
                    {doc.agent && (
                      <Chip label={doc.agent} size="small" variant="outlined" />
                    )}
                    {doc.score != null && (
                      <Chip label={`Score: ${doc.score.toFixed(2)}`} size="small" variant="outlined" color="info" />
                    )}
                  </Box>
                  <Tooltip title="Copy content">
                    <IconButton size="small" onClick={() => handleCopy(doc.text)}>
                      <CopyIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>

                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5, fontFamily: 'monospace', fontSize: '0.7rem' }}>
                  ID: {doc.id}
                </Typography>
                {doc.crew_id && (
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5, fontFamily: 'monospace', fontSize: '0.7rem' }}>
                    Crew: {doc.crew_id}
                  </Typography>
                )}
                {doc.created_at && (
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1, fontSize: '0.7rem' }}>
                    Created: {new Date(doc.created_at).toLocaleString()}
                  </Typography>
                )}

                <Divider sx={{ my: 1 }} />

                <Typography
                  variant="body2"
                  sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', mb: doc.metadata && Object.keys(doc.metadata).length > 0 ? 1 : 0 }}
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
                          bgcolor: 'action.hover',
                          p: 1,
                          borderRadius: 1,
                          overflow: 'auto',
                          fontSize: '0.75rem',
                          mt: 0.5,
                          fontFamily: 'monospace',
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
    </Dialog>
  );
};

export default LakebaseDocumentsDialog;
