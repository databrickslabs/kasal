import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  FormControlLabel,
  Switch,
  Card,
  CardContent,
  Grid,
} from '@mui/material';
import { useUserPreferencesStore } from '../../store/userPreferencesStore';
import ViewInArIcon from '@mui/icons-material/ViewInAr';
import DashboardIcon from '@mui/icons-material/Dashboard';

interface UIPreferenceDialogProps {
  open: boolean;
  onClose: () => void;
}

const UIPreferenceDialog: React.FC<UIPreferenceDialogProps> = ({ open, onClose }) => {
  const { useNewExecutionUI, setUseNewExecutionUI, setHasSeenUIPreferenceDialog } = useUserPreferencesStore();
  const [selectedMode, setSelectedMode] = useState(useNewExecutionUI);

  const handleSave = () => {
    setUseNewExecutionUI(selectedMode);
    setHasSeenUIPreferenceDialog(true);
    onClose();
  };

  const handleCancel = () => {
    setSelectedMode(useNewExecutionUI);
    onClose();
  };

  return (
    <Dialog open={open} onClose={handleCancel} maxWidth="md" fullWidth>
      <DialogTitle>Choose Your Execution View Preference</DialogTitle>
      <DialogContent>
        <Box sx={{ mt: 2 }}>
          <Typography variant="body1" gutterBottom>
            Select how you&apos;d like to view execution actions and details:
          </Typography>
          
          <Grid container spacing={3} sx={{ mt: 2 }}>
            <Grid item xs={12} md={6}>
              <Card 
                variant="outlined" 
                sx={{ 
                  cursor: 'pointer',
                  border: !selectedMode ? '2px solid primary.main' : '1px solid',
                  borderColor: !selectedMode ? 'primary.main' : 'divider',
                  transition: 'all 0.3s',
                }}
                onClick={() => setSelectedMode(false)}
              >
                <CardContent>
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    <DashboardIcon color={!selectedMode ? 'primary' : 'action'} sx={{ mr: 1 }} />
                    <Typography variant="h6" color={!selectedMode ? 'primary' : 'text.primary'}>
                      Traditional View
                    </Typography>
                  </Box>
                  <Typography variant="body2" color="text.secondary">
                    All action buttons (View Result, Download PDF, View Logs, Schedule, View Trace) 
                    are displayed directly in the execution history table.
                  </Typography>
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="caption" color="text.secondary">
                      ✓ Quick access to all actions<br />
                      ✓ Everything visible at a glance<br />
                      ✓ Traditional layout
                    </Typography>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
            
            <Grid item xs={12} md={6}>
              <Card 
                variant="outlined"
                sx={{ 
                  cursor: 'pointer',
                  border: selectedMode ? '2px solid primary.main' : '1px solid',
                  borderColor: selectedMode ? 'primary.main' : 'divider',
                  transition: 'all 0.3s',
                }}
                onClick={() => setSelectedMode(true)}
              >
                <CardContent>
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    <ViewInArIcon color={selectedMode ? 'primary' : 'action'} sx={{ mr: 1 }} />
                    <Typography variant="h6" color={selectedMode ? 'primary' : 'text.primary'}>
                      Streamlined View
                    </Typography>
                  </Box>
                  <Typography variant="body2" color="text.secondary">
                    Only &quot;View Execution Trace&quot; button in the table. All other actions 
                    are available inside the Execution Trace Timeline dialog.
                  </Typography>
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="caption" color="text.secondary">
                      ✓ Cleaner interface<br />
                      ✓ Focused workflow<br />
                      ✓ All details in one place
                    </Typography>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          </Grid>
          
          <Box sx={{ mt: 3, p: 2, bgcolor: 'background.paper', borderRadius: 1 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={selectedMode}
                  onChange={(e) => setSelectedMode(e.target.checked)}
                  color="primary"
                />
              }
              label={
                <Typography variant="body2">
                  Use streamlined execution view (you can change this later in settings)
                </Typography>
              }
            />
          </Box>
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleCancel}>Cancel</Button>
        <Button onClick={handleSave} variant="contained" color="primary">
          Save Preference
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default UIPreferenceDialog;