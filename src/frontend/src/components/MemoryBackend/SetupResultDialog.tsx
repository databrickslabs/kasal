/**
 * Dialog component for displaying Vector Search setup results
 */

import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Box,
  Alert,
  Typography,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Link,
  Button,
} from '@mui/material';
import {
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
} from '@mui/icons-material';
import { SetupResult } from '../../types/memoryBackend';
import { buildVectorSearchEndpointUrl, buildVectorSearchIndexUrl, getVectorSearchSetupStatus } from './databricksVectorSearchUtils';

interface SetupResultDialogProps {
  open: boolean;
  onClose: () => void;
  setupResult: SetupResult | null;
  workspaceUrl?: string;
  savedConfigWorkspaceUrl?: string;
}

export const SetupResultDialog: React.FC<SetupResultDialogProps> = ({
  open,
  onClose,
  setupResult,
  workspaceUrl,
  savedConfigWorkspaceUrl,
}) => {
  const renderSetupStatusIcon = (status?: string) => {
    const iconStatus = getVectorSearchSetupStatus(status);
    if (iconStatus === 'success') {
      return <CheckCircleIcon color="success" />;
    }
    return <ErrorIcon color="error" />;
  };

  const getWorkspaceUrl = () => workspaceUrl || savedConfigWorkspaceUrl || '';

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle>
        {setupResult?.success ? 'Setup Complete!' : 'Setup Failed'}
      </DialogTitle>
      <DialogContent>
        {setupResult && (
          <Box>
            <Alert 
              severity={setupResult.success ? 'success' : 'error'} 
              sx={{ mb: 2 }}
            >
              {setupResult.message}
            </Alert>

            {setupResult.warning && (
              <Alert severity="warning" sx={{ mb: 2 }}>
                {setupResult.warning}
              </Alert>
            )}

            {setupResult.info && (
              <Alert severity="info" sx={{ mb: 2 }}>
                {setupResult.info}
              </Alert>
            )}

            {setupResult.endpoints && (
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  Endpoints Created:
                </Typography>
                <List dense>
                  {setupResult.endpoints.memory && (
                    <ListItem>
                      <ListItemIcon>
                        {renderSetupStatusIcon(setupResult.endpoints.memory.status)}
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Link 
                            href={buildVectorSearchEndpointUrl(getWorkspaceUrl(), setupResult.endpoints.memory.name)}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {setupResult.endpoints.memory.name}
                          </Link>
                        }
                        secondary="Memory Endpoint (Direct Access)"
                      />
                    </ListItem>
                  )}
                  {setupResult.endpoints.document && (
                    <ListItem>
                      <ListItemIcon>
                        {renderSetupStatusIcon(setupResult.endpoints.document.status)}
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Link 
                            href={buildVectorSearchEndpointUrl(getWorkspaceUrl(), setupResult.endpoints.document.name)}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {setupResult.endpoints.document.name}
                          </Link>
                        }
                        secondary="Document Endpoint (Direct Access)"
                      />
                    </ListItem>
                  )}
                </List>
              </Box>
            )}

            {setupResult.indexes && (
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  Indexes Created:
                </Typography>
                <List dense>
                  {setupResult.indexes.short_term && (
                    <ListItem>
                      <ListItemIcon>
                        {renderSetupStatusIcon(setupResult.indexes.short_term.status)}
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Link 
                            href={buildVectorSearchIndexUrl(getWorkspaceUrl(), setupResult.indexes.short_term.name)}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {setupResult.indexes.short_term.name}
                          </Link>
                        }
                        secondary="Short-term Memory Index"
                      />
                    </ListItem>
                  )}
                  {setupResult.indexes.long_term && (
                    <ListItem>
                      <ListItemIcon>
                        {renderSetupStatusIcon(setupResult.indexes.long_term.status)}
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Link 
                            href={buildVectorSearchIndexUrl(getWorkspaceUrl(), setupResult.indexes.long_term.name)}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {setupResult.indexes.long_term.name}
                          </Link>
                        }
                        secondary="Long-term Memory Index"
                      />
                    </ListItem>
                  )}
                  {setupResult.indexes.entity && (
                    <ListItem>
                      <ListItemIcon>
                        {renderSetupStatusIcon(setupResult.indexes.entity.status)}
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Link 
                            href={buildVectorSearchIndexUrl(getWorkspaceUrl(), setupResult.indexes.entity.name)}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {setupResult.indexes.entity.name}
                          </Link>
                        }
                        secondary="Entity Memory Index"
                      />
                    </ListItem>
                  )}
                  {setupResult.indexes.document && (
                    <ListItem>
                      <ListItemIcon>
                        {renderSetupStatusIcon(setupResult.indexes.document.status)}
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Link 
                            href={buildVectorSearchIndexUrl(getWorkspaceUrl(), setupResult.indexes.document.name)}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {setupResult.indexes.document.name}
                          </Link>
                        }
                        secondary="Document Embeddings Index"
                      />
                    </ListItem>
                  )}
                </List>
              </Box>
            )}
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
};