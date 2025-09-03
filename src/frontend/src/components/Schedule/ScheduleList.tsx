import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Switch,
  Tooltip,
  CircularProgress,
  useTheme,
} from '@mui/material';
import {
  Delete as DeleteIcon,
} from '@mui/icons-material';
import { ScheduleService } from '../../api/ScheduleService';
import { toast } from 'react-hot-toast';
import { Schedule } from '../../types/schedule';

const ScheduleList: React.FC = () => {
  const theme = useTheme();
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [loading, setLoading] = useState(false);

  const loadSchedules = useCallback(async () => {
    try {
      setLoading(true);
      const fetchedSchedules = await ScheduleService.listSchedules();
      setSchedules(fetchedSchedules);
    } catch (error) {
      console.error('Error loading schedules:', error);
      toast.error('Failed to load schedules');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSchedules();
  }, [loadSchedules]);

  const handleToggleSchedule = async (id: number) => {
    try {
      await ScheduleService.toggleSchedule(id);
      toast.success('Schedule toggled successfully');
      loadSchedules();
    } catch (error) {
      console.error('Error toggling schedule:', error);
      toast.error('Failed to toggle schedule');
    }
  };

  const handleDeleteSchedule = async (id: number) => {
    if (!window.confirm('Are you sure you want to delete this schedule?')) {
      return;
    }
    
    try {
      await ScheduleService.deleteSchedule(id);
      toast.success('Schedule deleted successfully');
      loadSchedules();
    } catch (error) {
      console.error('Error deleting schedule:', error);
      toast.error('Failed to delete schedule');
    }
  };


  if (loading && schedules.length === 0) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 2 }}>
        <CircularProgress size={24} />
      </Box>
    );
  }

  return (
    <Box sx={{ width: '100%' }}>
      <Typography 
        variant="subtitle2" 
        sx={{ 
          color: theme.palette.primary.main, 
          mb: 1,
          fontWeight: 600,
          textTransform: 'uppercase',
          letterSpacing: '0.5px',
          fontSize: '0.7rem',
          px: 1
        }}
      >
        Schedules ({schedules.length})
      </Typography>
      
      {schedules.length === 0 ? (
        <Box sx={{ px: 1 }}>
          <Typography 
            variant="body2" 
            color="text.secondary" 
            sx={{ fontSize: '0.8rem', fontStyle: 'italic' }}
          >
            No schedules found
          </Typography>
        </Box>
      ) : (
        <List dense sx={{ py: 0 }}>
          {schedules.map((schedule) => (
            <ListItem
              key={schedule.id}
              sx={{
                px: 1,
                py: 0.5,
                borderRadius: 1,
                mb: 0.5,
                border: `1px solid ${theme.palette.divider}`,
                backgroundColor: 'background.paper',
                '&:hover': {
                  backgroundColor: 'action.hover',
                },
              }}
            >
              <ListItemText
                primary={
                  <Typography 
                    variant="body2" 
                    sx={{ 
                      fontWeight: 500,
                      fontSize: '0.85rem',
                      color: 'text.primary'
                    }}
                  >
                    {schedule.name}
                  </Typography>
                }
                secondary={
                  <Typography 
                    variant="caption" 
                    sx={{ 
                      color: 'text.secondary',
                      fontSize: '0.7rem'
                    }}
                  >
                    {schedule.cron_expression}
                  </Typography>
                }
                sx={{ mr: 10 }} // Much larger margin to make room for buttons
              />
              <ListItemSecondaryAction sx={{ right: 60 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                  <Tooltip title={schedule.is_active ? 'Disable' : 'Enable'}>
                    <Switch
                      size="small"
                      checked={schedule.is_active}
                      onChange={() => handleToggleSchedule(schedule.id)}
                      sx={{ 
                        mr: 0.5,
                        '& .MuiSwitch-switchBase': {
                          padding: '4px',
                        },
                        '& .MuiSwitch-thumb': {
                          width: 16,
                          height: 16,
                        },
                        '& .MuiSwitch-track': {
                          borderRadius: 10,
                        }
                      }}
                    />
                  </Tooltip>
                  <Tooltip title="Delete">
                    <IconButton
                      size="small"
                      onClick={() => handleDeleteSchedule(schedule.id)}
                      sx={{ 
                        p: 0.5,
                        '&:hover': {
                          color: theme.palette.error.main,
                        }
                      }}
                    >
                      <DeleteIcon sx={{ fontSize: '1rem' }} />
                    </IconButton>
                  </Tooltip>
                </Box>
              </ListItemSecondaryAction>
            </ListItem>
          ))}
        </List>
      )}
    </Box>
  );
};

export default ScheduleList;