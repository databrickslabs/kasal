/**
 * HITL Approval Dialog Component
 *
 * A dialog that appears when clicking on an "Awaiting Approval" status badge.
 * Allows users to quickly approve or reject a pending HITL gate.
 */

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
  CircularProgress,
  Alert,
  Chip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Paper,
  Divider,
  IconButton,
} from '@mui/material';
import {
  CheckCircle as ApproveIcon,
  Cancel as RejectIcon,
  Close as CloseIcon,
  AccessTime as TimeIcon,
  Description as DescriptionIcon,
  Refresh as RefreshIcon,
} from '@mui/icons-material';
import {
  HITLService,
  HITLApprovalResponse,
  HITLRejectionAction,
} from '../../api/HITLService';

interface HITLApprovalDialogProps {
  /** Whether the dialog is open */
  open: boolean;
  /** The execution ID to fetch approval for */
  executionId: string;
  /** Callback when dialog is closed */
  onClose: () => void;
  /** Callback when an approval action is completed */
  onActionComplete?: (action: 'approve' | 'reject') => void;
}

const HITLApprovalDialog: React.FC<HITLApprovalDialogProps> = ({
  open,
  executionId,
  onClose,
  onActionComplete,
}) => {
  const [approval, setApproval] = useState<HITLApprovalResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionType, setActionType] = useState<'approve' | 'reject' | null>(null);
  const [comment, setComment] = useState('');
  const [rejectionReason, setRejectionReason] = useState('');
  const [rejectionAction, setRejectionAction] = useState<HITLRejectionAction>(
    HITLRejectionAction.REJECT
  );
  const [actionLoading, setActionLoading] = useState(false);

  // Fetch approval for the execution
  const fetchApproval = useCallback(async () => {
    if (!executionId) return;

    setLoading(true);
    setError(null);

    try {
      const status = await HITLService.getExecutionHITLStatus(executionId);
      if (status.pending_approval) {
        setApproval(status.pending_approval);
      } else {
        setApproval(null);
        setError('No pending approval found for this execution');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch approval');
    } finally {
      setLoading(false);
    }
  }, [executionId]);

  // Fetch on open
  useEffect(() => {
    if (open && executionId) {
      fetchApproval();
      // Reset form state
      setActionType(null);
      setComment('');
      setRejectionReason('');
      setRejectionAction(HITLRejectionAction.REJECT);
    }
  }, [open, executionId, fetchApproval]);

  // Handle approve action
  const handleApprove = async () => {
    if (!approval) return;

    setActionLoading(true);
    try {
      await HITLService.approveGate(approval.id, {
        comment: comment || undefined,
      });
      onActionComplete?.('approve');
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to approve');
    } finally {
      setActionLoading(false);
    }
  };

  // Handle reject action
  const handleReject = async () => {
    if (!approval || !rejectionReason) return;

    setActionLoading(true);
    try {
      await HITLService.rejectGate(approval.id, {
        reason: rejectionReason,
        action: rejectionAction,
      });
      onActionComplete?.('reject');
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to reject');
    } finally {
      setActionLoading(false);
    }
  };

  // Format time remaining
  const formatTimeRemaining = (expiresAt: string | null | undefined) => {
    if (!expiresAt) return 'No expiry';

    const now = new Date();
    const expires = new Date(expiresAt);
    const diff = expires.getTime() - now.getTime();

    if (diff <= 0) return 'Expired';

    const hours = Math.floor(diff / (1000 * 60 * 60));
    const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));

    if (hours > 24) {
      const days = Math.floor(hours / 24);
      return `${days}d ${hours % 24}h remaining`;
    }
    if (hours > 0) {
      return `${hours}h ${minutes}m remaining`;
    }
    return `${minutes}m remaining`;
  };

  const renderContent = () => {
    if (loading) {
      return (
        <Box display="flex" justifyContent="center" alignItems="center" p={4}>
          <CircularProgress size={32} />
          <Typography variant="body2" sx={{ ml: 2 }}>
            Loading approval details...
          </Typography>
        </Box>
      );
    }

    if (error && !approval) {
      return (
        <Alert
          severity="error"
          action={
            <Button color="inherit" size="small" onClick={fetchApproval}>
              Retry
            </Button>
          }
        >
          {error}
        </Alert>
      );
    }

    if (!approval) {
      return (
        <Alert severity="info">
          No pending approval found for this execution.
        </Alert>
      );
    }

    // Show approval form based on action type
    if (actionType === 'approve') {
      return (
        <Box>
          <Typography variant="body1" gutterBottom>
            Approving this gate will resume the flow execution.
          </Typography>
          <TextField
            label="Comment (optional)"
            fullWidth
            multiline
            rows={3}
            value={comment}
            onChange={(e) => setComment(e.target.value)}
            placeholder="Add an optional comment..."
            sx={{ mt: 2 }}
          />
          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          )}
        </Box>
      );
    }

    if (actionType === 'reject') {
      return (
        <Box>
          <Typography variant="body1" gutterBottom>
            Please provide a reason for rejection.
          </Typography>
          <TextField
            label="Rejection Reason"
            fullWidth
            required
            multiline
            rows={3}
            value={rejectionReason}
            onChange={(e) => setRejectionReason(e.target.value)}
            placeholder="Enter reason for rejection..."
            sx={{ mt: 2, mb: 2 }}
          />
          <FormControl fullWidth>
            <InputLabel>Action</InputLabel>
            <Select
              value={rejectionAction}
              label="Action"
              onChange={(e) => setRejectionAction(e.target.value as HITLRejectionAction)}
            >
              <MenuItem value={HITLRejectionAction.REJECT}>
                Reject - Fail the flow execution
              </MenuItem>
              <MenuItem value={HITLRejectionAction.RETRY}>
                Retry - Re-run the previous crew
              </MenuItem>
            </Select>
          </FormControl>
          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          )}
        </Box>
      );
    }

    // Default: show approval details
    return (
      <Box>
        {/* Status and Time */}
        <Box display="flex" alignItems="center" gap={1} mb={2}>
          <Chip
            label={approval.status}
            size="small"
            color="warning"
          />
          <Chip
            icon={<TimeIcon />}
            label={formatTimeRemaining(approval.expires_at)}
            size="small"
            variant="outlined"
            color={approval.is_expired ? 'error' : 'default'}
          />
        </Box>

        {/* Message */}
        <Typography variant="h6" gutterBottom>
          {(approval.gate_config as { message?: string })?.message || 'Approval Required'}
        </Typography>

        {/* Previous Crew Info */}
        {approval.previous_crew_name && (
          <Typography variant="body2" color="text.secondary" gutterBottom>
            Waiting after: <strong>{approval.previous_crew_name}</strong>
          </Typography>
        )}

        <Divider sx={{ my: 2 }} />

        {/* Previous Output */}
        {approval.previous_crew_output && (
          <Box mb={2}>
            <Typography
              variant="body2"
              fontWeight="medium"
              display="flex"
              alignItems="center"
              gap={0.5}
              gutterBottom
            >
              <DescriptionIcon fontSize="small" />
              Previous Crew Output:
            </Typography>
            <Paper
              variant="outlined"
              sx={{
                p: 1.5,
                maxHeight: 200,
                overflow: 'auto',
                bgcolor: 'background.default',
              }}
            >
              <Typography
                variant="body2"
                sx={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: '0.85rem' }}
              >
                {approval.previous_crew_output}
              </Typography>
            </Paper>
          </Box>
        )}

        {/* Metadata */}
        <Box display="flex" flexWrap="wrap" gap={2} sx={{ fontSize: '0.85rem' }}>
          <Box>
            <Typography variant="caption" color="text.secondary" display="block">
              Gate
            </Typography>
            <Typography variant="body2" fontFamily="monospace">
              {approval.gate_node_id?.split('-').slice(-2).join('-') || 'N/A'}
            </Typography>
          </Box>
          <Box>
            <Typography variant="caption" color="text.secondary" display="block">
              Created
            </Typography>
            <Typography variant="body2">
              {new Date(approval.created_at).toLocaleString()}
            </Typography>
          </Box>
        </Box>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </Box>
    );
  };

  const renderActions = () => {
    if (loading || (!approval && !error)) {
      return (
        <Button onClick={onClose}>Close</Button>
      );
    }

    if (!approval) {
      return (
        <>
          <Button startIcon={<RefreshIcon />} onClick={fetchApproval}>
            Refresh
          </Button>
          <Button onClick={onClose}>Close</Button>
        </>
      );
    }

    if (actionType === 'approve') {
      return (
        <>
          <Button onClick={() => setActionType(null)} disabled={actionLoading}>
            Back
          </Button>
          <Button
            variant="contained"
            color="success"
            onClick={handleApprove}
            disabled={actionLoading}
            startIcon={actionLoading ? <CircularProgress size={16} /> : <ApproveIcon />}
          >
            Confirm Approval
          </Button>
        </>
      );
    }

    if (actionType === 'reject') {
      return (
        <>
          <Button onClick={() => setActionType(null)} disabled={actionLoading}>
            Back
          </Button>
          <Button
            variant="contained"
            color="error"
            onClick={handleReject}
            disabled={actionLoading || !rejectionReason}
            startIcon={actionLoading ? <CircularProgress size={16} /> : <RejectIcon />}
          >
            Confirm Rejection
          </Button>
        </>
      );
    }

    // Default: show approve/reject buttons
    return (
      <>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="outlined"
          color="error"
          startIcon={<RejectIcon />}
          onClick={() => setActionType('reject')}
          disabled={approval.is_expired}
        >
          Reject
        </Button>
        <Button
          variant="contained"
          color="success"
          startIcon={<ApproveIcon />}
          onClick={() => setActionType('approve')}
          disabled={approval.is_expired}
        >
          Approve
        </Button>
      </>
    );
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      PaperProps={{
        sx: { minHeight: 300 },
      }}
    >
      <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Box>
          {actionType === 'approve' && 'Approve Gate'}
          {actionType === 'reject' && 'Reject Gate'}
          {!actionType && 'Human Approval Required'}
        </Box>
        <IconButton size="small" onClick={onClose}>
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent dividers>
        {renderContent()}
      </DialogContent>
      <DialogActions>
        {renderActions()}
      </DialogActions>
    </Dialog>
  );
};

export default HITLApprovalDialog;
