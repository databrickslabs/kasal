import React, { useState } from 'react';
import { Chip, CircularProgress, Box, Tooltip } from '@mui/material';
import {
  CheckCircle as CompletedIcon,
  Error as FailedIcon,
  Cancel as CancelledIcon,
  Stop as StoppedIcon,
  PlayArrow as RunningIcon,
  HourglassEmpty as PendingIcon,
  Build as PreparingIcon,
  PanTool as WaitingApprovalIcon,
  ThumbDown as RejectedIcon,
} from '@mui/icons-material';
import { HITLApprovalDialog } from './HITL';

interface ExecutionStatusBadgeProps {
  status: string;
  size?: 'small' | 'medium';
  showIcon?: boolean;
  /** Execution ID - required for HITL approval dialog */
  executionId?: string;
  /** Callback when approval action is completed */
  onApprovalComplete?: (action: 'approve' | 'reject') => void;
}

const ExecutionStatusBadge: React.FC<ExecutionStatusBadgeProps> = ({
  status,
  size = 'small',
  showIcon = true,
  executionId,
  onApprovalComplete,
}) => {
  const [approvalDialogOpen, setApprovalDialogOpen] = useState(false);

  const isWaitingForApproval = status?.toUpperCase() === 'WAITING_FOR_APPROVAL';
  const isClickable = isWaitingForApproval && !!executionId;

  const handleClick = () => {
    if (isClickable) {
      setApprovalDialogOpen(true);
    }
  };

  const handleApprovalComplete = (action: 'approve' | 'reject') => {
    setApprovalDialogOpen(false);
    onApprovalComplete?.(action);
  };
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
      case 'WAITING_FOR_APPROVAL':
        return {
          label: 'Awaiting Approval',
          color: 'warning' as const,
          icon: <WaitingApprovalIcon />,
        };
      case 'REJECTED':
        return {
          label: 'Rejected',
          color: 'error' as const,
          icon: <RejectedIcon />,
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

  const chip = (
    <Chip
      label={config.label}
      color={config.color}
      size={size}
      icon={showIcon && config.icon ? config.icon : undefined}
      variant={['STOPPING', 'WAITING_FOR_APPROVAL'].includes(status?.toUpperCase()) ? 'filled' : 'outlined'}
      onClick={isClickable ? handleClick : undefined}
      sx={{
        animation: ['STOPPING', 'WAITING_FOR_APPROVAL'].includes(status?.toUpperCase())
          ? 'pulse 2s infinite'
          : 'none',
        '@keyframes pulse': {
          '0%': { opacity: 1 },
          '50%': { opacity: 0.6 },
          '100%': { opacity: 1 },
        },
        ...(isClickable && {
          cursor: 'pointer',
          '&:hover': {
            transform: 'scale(1.05)',
            boxShadow: 1,
          },
          transition: 'transform 0.2s, box-shadow 0.2s',
        }),
      }}
    />
  );

  return (
    <>
      {isClickable ? (
        <Tooltip title="Click to review and approve" arrow>
          {chip}
        </Tooltip>
      ) : (
        chip
      )}

      {/* HITL Approval Dialog */}
      {executionId && (
        <HITLApprovalDialog
          open={approvalDialogOpen}
          executionId={executionId}
          onClose={() => setApprovalDialogOpen(false)}
          onActionComplete={handleApprovalComplete}
        />
      )}
    </>
  );
};

export default ExecutionStatusBadge;