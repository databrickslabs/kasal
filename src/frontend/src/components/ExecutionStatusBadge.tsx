import React from 'react';
import { Chip, CircularProgress, Box } from '@mui/material';
import {
  CheckCircle as CompletedIcon,
  Error as FailedIcon,
  Cancel as CancelledIcon,
  Stop as StoppedIcon,
  PlayArrow as RunningIcon,
  HourglassEmpty as PendingIcon,
  Build as PreparingIcon,
} from '@mui/icons-material';

interface ExecutionStatusBadgeProps {
  status: string;
  size?: 'small' | 'medium';
  showIcon?: boolean;
}

const ExecutionStatusBadge: React.FC<ExecutionStatusBadgeProps> = ({
  status,
  size = 'small',
  showIcon = true,
}) => {
  const getStatusConfig = () => {
    const normalizedStatus = status?.toUpperCase();
    switch (normalizedStatus) {
      case 'PENDING':
        return {
          label: 'Pending',
          color: 'warning' as const,
          icon: <PendingIcon />,
        };
      case 'QUEUED':
        return {
          label: 'Queued',
          color: 'warning' as const,
          icon: <PendingIcon />,
        };
      case 'PREPARING':
        return {
          label: 'Preparing',
          color: 'info' as const,
          icon: <PreparingIcon />,
        };
      case 'RUNNING':
        return {
          label: 'Running',
          color: 'primary' as const,
          icon: <RunningIcon />,
        };
      case 'STOPPING':
        return {
          label: 'Stopping',
          color: 'warning' as const,
          icon: (
            <Box display="flex" alignItems="center">
              <CircularProgress size={16} color="inherit" />
            </Box>
          ),
        };
      case 'STOPPED':
        return {
          label: 'Stopped',
          color: 'warning' as const,
          icon: <StoppedIcon />,
        };
      case 'COMPLETED':
        return {
          label: 'Completed',
          color: 'success' as const,
          icon: <CompletedIcon />,
        };
      case 'FAILED':
        return {
          label: 'Failed',
          color: 'error' as const,
          icon: <FailedIcon />,
        };
      case 'CANCELLED':
        return {
          label: 'Cancelled',
          color: 'default' as const,
          icon: <CancelledIcon />,
        };
      default:
        // Return the original status with proper casing
        return {
          label: status ? status.charAt(0).toUpperCase() + status.slice(1).toLowerCase() : 'Unknown',
          color: 'default' as const,
          icon: null,
        };
    }
  };

  const config = getStatusConfig();

  return (
    <Chip
      label={config.label}
      color={config.color}
      size={size}
      icon={showIcon && config.icon ? config.icon : undefined}
      variant={status?.toUpperCase() === 'STOPPING' ? 'filled' : 'outlined'}
      sx={{
        animation: status?.toUpperCase() === 'STOPPING' 
          ? 'pulse 2s infinite' 
          : 'none',
        '@keyframes pulse': {
          '0%': { opacity: 1 },
          '50%': { opacity: 0.6 },
          '100%': { opacity: 1 },
        },
      }}
    />
  );
};

export default ExecutionStatusBadge;