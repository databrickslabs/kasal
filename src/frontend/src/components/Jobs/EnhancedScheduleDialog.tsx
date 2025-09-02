import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Typography,
  Box,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  ToggleButton,
  ToggleButtonGroup,
  Checkbox,
  FormControlLabel,
  FormGroup,
  Grid,
  Snackbar,
  Alert,
  IconButton,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import { useTranslation } from 'react-i18next';

interface EnhancedScheduleDialogProps {
  open: boolean;
  onClose: () => void;
  scheduleName: string;
  cronExpression: string;
  scheduleNameInputRef: React.RefObject<HTMLInputElement>;
  onScheduleNameChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onCronExpressionChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onScheduleJob: () => Promise<void>;
}

type CronMode = 'manual' | 'visual';

const EnhancedScheduleDialog: React.FC<EnhancedScheduleDialogProps> = ({
  open,
  onClose,
  scheduleName,
  cronExpression,
  scheduleNameInputRef,
  onScheduleNameChange,
  onCronExpressionChange,
  onScheduleJob,
}) => {
  const { t: _t } = useTranslation();
  const [cronMode, setCronMode] = useState<CronMode>('visual');
  const [frequency, setFrequency] = useState('daily');
  const [selectedTime, setSelectedTime] = useState({ hour: '0', minute: '0' });
  const [selectedDays, setSelectedDays] = useState<string[]>([]);
  const [selectedMonths, setSelectedMonths] = useState<string[]>([]);
  const [selectedDaysOfMonth, setSelectedDaysOfMonth] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // Parse existing cron expression when dialog opens or cron expression changes externally
  const parseCronExpression = useCallback((expression: string) => {
    const parts = expression.split(' ');
    if (parts.length !== 5) return;
    
    const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

    // Set time
    setSelectedTime({
      minute: minute === '*' ? '0' : minute,
      hour: hour === '*' ? '0' : hour,
    });

    // Determine frequency and set corresponding values
    if (hour === '*') {
      setFrequency('hourly');
    } else if (dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
      setFrequency('daily');
    } else if (dayOfMonth === '*' && month === '*' && dayOfWeek !== '*') {
      setFrequency('weekly');
      setSelectedDays(dayOfWeek.split(','));
    } else if (dayOfMonth !== '*' && month === '*') {
      setFrequency('monthly');
      setSelectedDaysOfMonth(dayOfMonth.split(','));
    } else if (month !== '*') {
      setFrequency('yearly');
      setSelectedMonths(month.split(','));
    }
  }, []);

  // Update cron expression based on visual controls
  const updateCronFromVisual = useCallback(() => {
    const { hour, minute } = selectedTime;
    let newCronExpression = '';

    const days = selectedDays.length > 0 ? selectedDays.join(',') : '*';
    const daysOfMonth = selectedDaysOfMonth.length > 0 ? selectedDaysOfMonth.join(',') : '*';
    const months = selectedMonths.length > 0 ? selectedMonths.join(',') : '*';

    switch (frequency) {
      case 'hourly':
        newCronExpression = `${minute} * * * *`;
        break;
      case 'daily':
        newCronExpression = `${minute} ${hour} * * *`;
        break;
      case 'weekly':
        newCronExpression = `${minute} ${hour} * * ${days}`;
        break;
      case 'monthly':
        newCronExpression = `${minute} ${hour} ${daysOfMonth} * *`;
        break;
      case 'yearly':
        newCronExpression = `${minute} ${hour} * ${months} *`;
        break;
      default:
        newCronExpression = '0 0 * * *';
    }

    // Create a synthetic event to update the cron expression
    const syntheticEvent = {
      target: { value: newCronExpression }
    } as React.ChangeEvent<HTMLInputElement>;
    onCronExpressionChange(syntheticEvent);
  }, [frequency, selectedTime, selectedDays, selectedMonths, selectedDaysOfMonth, onCronExpressionChange]);

  // Track if we're initializing to prevent update loops
  const [isInitializing, setIsInitializing] = useState(false);

  // Initialize values when dialog opens
  useEffect(() => {
    if (open) {
      setIsInitializing(true);
      if (cronExpression && cronExpression !== '0 0 * * *') {
        parseCronExpression(cronExpression);
      } else {
        // Reset to defaults
        setFrequency('daily');
        setSelectedTime({ hour: '0', minute: '0' });
        setSelectedDays([]);
        setSelectedMonths([]);
        setSelectedDaysOfMonth([]);
      }
      setError(null);
      // Allow updates after a short delay
      setTimeout(() => setIsInitializing(false), 100);
    }
  }, [open, cronExpression, parseCronExpression]); // Keep dependencies but prevent loops with isInitializing flag

  // Update cron expression when visual controls change
  useEffect(() => {
    if (cronMode === 'visual' && open && !isInitializing) {
      updateCronFromVisual();
    }
  }, [cronMode, frequency, selectedTime, selectedDays, selectedMonths, selectedDaysOfMonth, open, isInitializing, updateCronFromVisual]); // Keep all dependencies

  const handleScheduleJob = async () => {
    if (!scheduleName || !cronExpression) {
      setError('Please fill in all required fields');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      await onScheduleJob();
    } catch (error) {
      console.error('Error scheduling job:', error);
      setError('Failed to schedule job');
    } finally {
      setLoading(false);
    }
  };

  const handleCloseError = () => {
    setError(null);
  };

  const renderVisualCronBuilder = () => {
    const hours = Array.from({ length: 24 }, (_, i) => i.toString());
    const minutes = Array.from({ length: 60 }, (_, i) => i.toString());
    const daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    const daysInMonth = Array.from({ length: 31 }, (_, i) => (i + 1).toString());

    return (
      <Box sx={{ mt: 2 }}>
        <FormControl fullWidth sx={{ mb: 2 }}>
          <InputLabel>Frequency</InputLabel>
          <Select
            value={frequency}
            onChange={(e) => setFrequency(e.target.value)}
            label="Frequency"
          >
            <MenuItem value="hourly">Hourly</MenuItem>
            <MenuItem value="daily">Daily</MenuItem>
            <MenuItem value="weekly">Weekly</MenuItem>
            <MenuItem value="monthly">Monthly</MenuItem>
            <MenuItem value="yearly">Yearly</MenuItem>
          </Select>
        </FormControl>

        <Grid container spacing={2} sx={{ mb: 2 }}>
          <Grid item xs={6}>
            <FormControl fullWidth>
              <InputLabel>Hour</InputLabel>
              <Select
                value={selectedTime.hour}
                onChange={(e) => setSelectedTime(prev => ({ ...prev, hour: e.target.value }))}
                label="Hour"
                disabled={frequency === 'hourly'}
              >
                {hours.map(hour => (
                  <MenuItem key={hour} value={hour}>{hour.padStart(2, '0')}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={6}>
            <FormControl fullWidth>
              <InputLabel>Minute</InputLabel>
              <Select
                value={selectedTime.minute}
                onChange={(e) => setSelectedTime(prev => ({ ...prev, minute: e.target.value }))}
                label="Minute"
              >
                {minutes.map(minute => (
                  <MenuItem key={minute} value={minute}>{minute.padStart(2, '0')}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        </Grid>

        {frequency === 'weekly' && (
          <FormControl component="fieldset" fullWidth sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom>Select Days</Typography>
            <FormGroup row>
              {daysOfWeek.map((day, index) => (
                <FormControlLabel
                  key={day}
                  control={
                    <Checkbox
                      checked={selectedDays.includes(index.toString())}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedDays([...selectedDays, index.toString()]);
                        } else {
                          setSelectedDays(selectedDays.filter(d => d !== index.toString()));
                        }
                      }}
                    />
                  }
                  label={day}
                />
              ))}
            </FormGroup>
          </FormControl>
        )}

        {frequency === 'monthly' && (
          <FormControl component="fieldset" fullWidth sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom>Select Days of Month</Typography>
            <FormGroup row>
              {daysInMonth.map((day) => (
                <FormControlLabel
                  key={day}
                  control={
                    <Checkbox
                      checked={selectedDaysOfMonth.includes(day)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedDaysOfMonth([...selectedDaysOfMonth, day]);
                        } else {
                          setSelectedDaysOfMonth(selectedDaysOfMonth.filter(d => d !== day));
                        }
                      }}
                    />
                  }
                  label={day}
                  sx={{ width: '70px' }}
                />
              ))}
            </FormGroup>
          </FormControl>
        )}

        {frequency === 'yearly' && (
          <FormControl component="fieldset" fullWidth sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom>Select Months</Typography>
            <FormGroup row>
              {monthNames.map((month, index) => (
                <FormControlLabel
                  key={month}
                  control={
                    <Checkbox
                      checked={selectedMonths.includes((index + 1).toString())}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedMonths([...selectedMonths, (index + 1).toString()]);
                        } else {
                          setSelectedMonths(selectedMonths.filter(m => m !== (index + 1).toString()));
                        }
                      }}
                    />
                  }
                  label={month}
                />
              ))}
            </FormGroup>
          </FormControl>
        )}
      </Box>
    );
  };

  return (
    <>
      <Dialog
        open={open}
        onClose={onClose}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between',
          pb: 1.5,
          borderBottom: '1px solid',
          borderColor: 'divider'
        }}>
          <Typography variant="h6">Schedule Job</Typography>
          <IconButton 
            onClick={onClose}
            size="small"
            sx={{ 
              color: 'text.secondary',
              '&:hover': {
                color: 'text.primary',
              }
            }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Schedule Name"
            value={scheduleName}
            onChange={onScheduleNameChange}
            margin="normal"
            inputRef={scheduleNameInputRef}
          />

          <Box sx={{ mb: 2, mt: 2 }}>
            <ToggleButtonGroup
              value={cronMode}
              exclusive
              onChange={(e, newMode) => newMode && setCronMode(newMode)}
              sx={{ mb: 2 }}
            >
              <ToggleButton value="visual">Simple Schedule</ToggleButton>
              <ToggleButton value="manual">Advanced (Cron)</ToggleButton>
            </ToggleButtonGroup>

            {cronMode === 'manual' ? (
              <TextField
                fullWidth
                label="Cron Expression"
                value={cronExpression}
                onChange={onCronExpressionChange}
                helperText="Example: '0 0 * * *' for daily at midnight"
              />
            ) : (
              renderVisualCronBuilder()
            )}
          </Box>

          {cronMode === 'visual' && (
            <Box sx={{ mt: 2, p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
              <Typography variant="body2" color="text.secondary">
                <strong>Generated Cron Expression:</strong> {cronExpression}
              </Typography>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose} disabled={loading}>
            Cancel
          </Button>
          <Button 
            onClick={handleScheduleJob} 
            variant="contained" 
            color="primary"
            disabled={loading || !scheduleName || !cronExpression}
          >
            {loading ? 'Scheduling...' : 'Schedule Job'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={!!error}
        autoHideDuration={6000}
        onClose={handleCloseError}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert
          onClose={handleCloseError}
          severity="error"
          variant="filled"
          sx={{ width: '100%', whiteSpace: 'pre-line' }}
        >
          {error}
        </Alert>
      </Snackbar>
    </>
  );
};

export default EnhancedScheduleDialog;