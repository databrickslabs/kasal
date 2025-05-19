import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  CircularProgress,
  Paper,
  List,
  ListItem,
  ListItemText,
  TextField,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Snackbar,
  Alert,
  IconButton,
  Divider
} from '@mui/material';
import { useTranslation } from 'react-i18next';
import EditIcon from '@mui/icons-material/Edit';
import RestoreIcon from '@mui/icons-material/RestoreOutlined';
import { PromptService, PromptTemplate } from '../../api/PromptService';

const PromptConfiguration: React.FC = () => {
  const { t } = useTranslation();
  const [prompts, setPrompts] = useState<PromptTemplate[]>([]);
  const [loading, setLoading] = useState(true);
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [currentPrompt, setCurrentPrompt] = useState<PromptTemplate | null>(null);
  const [editedTemplate, setEditedTemplate] = useState('');
  const [resetConfirmOpen, setResetConfirmOpen] = useState(false);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error',
  });

  useEffect(() => {
    loadPrompts();
  }, []);

  const loadPrompts = async () => {
    setLoading(true);
    try {
      const promptService = PromptService.getInstance();
      const fetchedPrompts = await promptService.getAllPrompts();
      setPrompts(fetchedPrompts);
    } catch (error) {
      console.error('Error loading prompts:', error);
      setNotification({
        open: true,
        message: 'Failed to load prompt templates',
        severity: 'error',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleEditClick = (prompt: PromptTemplate) => {
    setCurrentPrompt(prompt);
    setEditedTemplate(prompt.template);
    setEditDialogOpen(true);
  };

  const handleCloseEditDialog = () => {
    setEditDialogOpen(false);
    setCurrentPrompt(null);
    setEditedTemplate('');
  };

  const handleSavePrompt = async () => {
    if (!currentPrompt) return;
    
    try {
      const promptService = PromptService.getInstance();
      await promptService.updatePrompt(currentPrompt.id, {
        ...currentPrompt,
        template: editedTemplate,
      });
      
      // Update local state
      setPrompts(prompts.map(p => 
        p.id === currentPrompt.id 
          ? { ...p, template: editedTemplate, updated_at: new Date().toISOString() } 
          : p
      ));
      
      setNotification({
        open: true,
        message: 'Prompt template updated successfully',
        severity: 'success',
      });
      
      handleCloseEditDialog();
    } catch (error) {
      console.error('Error updating prompt:', error);
      setNotification({
        open: true,
        message: 'Failed to update prompt template',
        severity: 'error',
      });
    }
  };

  const handleCloseNotification = () => {
    setNotification({
      ...notification,
      open: false,
    });
  };

  const handleResetPrompts = async () => {
    setResetConfirmOpen(false);
    setLoading(true);
    try {
      const promptService = PromptService.getInstance();
      const result = await promptService.resetPromptTemplates();
      
      setNotification({
        open: true,
        message: `Successfully reset ${result.reset_count} prompt templates to default values`,
        severity: 'success',
      });
      
      // Reload the prompts
      await loadPrompts();
    } catch (error) {
      console.error('Error resetting prompt templates:', error);
      setNotification({
        open: true,
        message: 'Failed to reset prompt templates',
        severity: 'error',
      });
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="300px">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        {t('configuration.prompts.title', { defaultValue: 'Prompt Templates' })}
      </Typography>
      <Typography variant="body2" color="textSecondary" paragraph>
        {t('configuration.prompts.description', { defaultValue: 'Edit the system prompt templates used by Kasal agents.' })}
      </Typography>

      <Box display="flex" justifyContent="flex-end" mb={2}>
        <Button
          startIcon={<RestoreIcon />}
          variant="outlined"
          color="primary"
          onClick={() => setResetConfirmOpen(true)}
        >
          {t('configuration.prompts.resetToDefault', { defaultValue: 'Reset to Default' })}
        </Button>
      </Box>

      <Paper elevation={2} sx={{ mt: 2 }}>
        <List>
          {prompts.map((prompt) => (
            <React.Fragment key={prompt.id}>
              <ListItem
                secondaryAction={
                  <IconButton edge="end" onClick={() => handleEditClick(prompt)}>
                    <EditIcon />
                  </IconButton>
                }
              >
                <ListItemText
                  primary={prompt.name}
                  secondary={prompt.description || 'No description'}
                />
              </ListItem>
              <Divider />
            </React.Fragment>
          ))}
          {prompts.length === 0 && (
            <ListItem>
              <ListItemText primary="No prompt templates found" />
            </ListItem>
          )}
        </List>
      </Paper>

      <Dialog
        open={editDialogOpen}
        onClose={handleCloseEditDialog}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>
          {t('configuration.prompts.editTitle', { defaultValue: 'Edit Prompt Template' })}
        </DialogTitle>
        <DialogContent>
          {currentPrompt && (
            <>
              <Box sx={{ mb: 2, mt: 1 }}>
                <Typography variant="subtitle1" fontWeight="bold">
                  {currentPrompt.name}
                </Typography>
                {currentPrompt.description && (
                  <Typography variant="body2" color="textSecondary">
                    {currentPrompt.description}
                  </Typography>
                )}
              </Box>
              <TextField
                label={t('configuration.prompts.template', { defaultValue: 'Template' })}
                multiline
                rows={15}
                fullWidth
                value={editedTemplate}
                onChange={(e) => setEditedTemplate(e.target.value)}
                variant="outlined"
              />
            </>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseEditDialog}>
            {t('common.cancel', { defaultValue: 'Cancel' })}
          </Button>
          <Button onClick={handleSavePrompt} variant="contained" color="primary">
            {t('common.save', { defaultValue: 'Save' })}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={handleCloseNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert 
          onClose={handleCloseNotification} 
          severity={notification.severity}
          variant="filled"
        >
          {notification.message}
        </Alert>
      </Snackbar>

      {/* Reset Confirmation Dialog */}
      <Dialog
        open={resetConfirmOpen}
        onClose={() => setResetConfirmOpen(false)}
      >
        <DialogTitle>
          {t('configuration.prompts.resetConfirmTitle', { defaultValue: 'Reset Prompt Templates' })}
        </DialogTitle>
        <DialogContent>
          <Typography>
            {t('configuration.prompts.resetConfirmMessage', { 
              defaultValue: 'Are you sure you want to reset all prompt templates to their default values? This action cannot be undone.' 
            })}
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setResetConfirmOpen(false)}>
            {t('common.cancel', { defaultValue: 'Cancel' })}
          </Button>
          <Button onClick={handleResetPrompts} variant="contained" color="primary" autoFocus>
            {t('configuration.prompts.confirmReset', { defaultValue: 'Reset' })}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default PromptConfiguration; 